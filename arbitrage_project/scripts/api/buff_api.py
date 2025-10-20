import requests
import time
import random
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_buff_prices():
    base_url = "https://buff.163.com/api/market/goods"
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"game": "csgo", "page_num": 1}

    all_items = []
    print("Fetching Buff163 market data...")

    while True:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=10)

            # Check response status
            if r.status_code != 200:
                print(f"Error {r.status_code} on page {params['page_num']}")
                break

            resp_json = r.json()

            # Handle error messages like rate limits
            if "data" not in resp_json:
                print(f"Unexpected response on page {params['page_num']}: {resp_json.get('msg', 'no data')}")
                # Wait longer and retry once
                time.sleep(random.uniform(5, 8))
                continue

            data = resp_json["data"]
            items = data.get("items", [])
            total_pages = data.get("total_page", params["page_num"])
            current_page = data.get("page_num", params["page_num"])

            print(f"Fetched page {current_page}/{total_pages} → {len(items)} items")
            all_items.extend(items)

            if current_page >= total_pages:
                break

            params["page_num"] += 1
            time.sleep(random.uniform(1.5, 3.0))  # more human-like delay

        except Exception as e:
            print(f"Error on page {params['page_num']}: {e}")
            break

    print(f"Total collected items: {len(all_items)}")

    if not all_items:
        print("⚠️ No data collected, exiting.")
        return

    # --- Write to database ---
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO markets (name, fee_percent)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
    """, ("Buff163", 0.02))
    conn.commit()

    inserted_count = 0
    for d in all_items:
        item_name = d.get("market_hash_name") or d.get("name")
        if not item_name:
            continue

        sell_price = float(d.get("sell_min_price") or 0)
        volume = int(d.get("sell_num") or 0)

        cur.execute("""
            INSERT INTO items (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (item_name,))

        cur.execute("""
            INSERT INTO prices (item_id, market_id, price, volume)
            VALUES (
                (SELECT id FROM items WHERE name=%s),
                (SELECT id FROM markets WHERE name='Buff163'),
                %s,
                %s
            )
        """, (item_name, sell_price, volume))

        inserted_count += 1
        if inserted_count % 100 == 0:
            conn.commit()
            print(f"Inserted {inserted_count} items so far...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Done. Inserted {inserted_count} Buff163 prices into Postgres.")
