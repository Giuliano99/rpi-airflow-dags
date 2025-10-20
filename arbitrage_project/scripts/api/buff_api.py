import requests
from arbitrage_project.scripts.db.config_db import get_connection
import time

def fetch_buff_prices():
    """
    Fetch all CS:GO item prices from Buff163 API (all pages)
    and write them to Postgres. Uses market_hash_name for mapping.
    """

    base_url = "https://buff.163.com/api/market/goods"
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"game": "csgo", "page_num": 1}

    all_items = []
    print("Fetching Buff163 market data...")

    # 1️⃣ Loop through all pages
    while True:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()["data"]

            items = data["items"]
            total_pages = data["total_page"]
            current_page = data["page_num"]

            print(f"Fetched page {current_page}/{total_pages} → {len(items)} items")
            all_items.extend(items)

            if current_page >= total_pages:
                break

            params["page_num"] += 1
            time.sleep(1.0)  # be nice to their API

        except Exception as e:
            print(f"Error on page {params['page_num']}: {e}")
            break

    print(f"Total collected items: {len(all_items)}")

    # 2️⃣ Connect to Postgres
    conn = get_connection()
    cur = conn.cursor()

    # Ensure the market exists
    cur.execute("""
        INSERT INTO markets (name, fee_percent)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
    """, ("Buff163", 0.02))
    conn.commit()

    # 3️⃣ Insert or update items and prices
    inserted_count = 0
    for d in all_items:
        item_name = d.get("market_hash_name") or d.get("name")
        if not item_name:
            continue

        sell_price = float(d.get("sell_min_price") or 0)
        volume = int(d.get("sell_num") or 0)

        # Ensure item exists
        cur.execute("""
            INSERT INTO items (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (item_name,))

        # Insert price
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