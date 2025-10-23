import requests
import time
import random
from arbitrage_project.scripts.db.config_db import get_connection


def fetch_buff_prices():
    base_url = "https://buff.163.com/api/market/goods"
    headers = {
        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      f"(KHTML, like Gecko) Chrome/{random.randint(100,120)}.0.{random.randint(1000,9999)}.100 "
                      f"Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://buff.163.com/market/csgo",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    params = {"game": "csgo", "page_num": 1}

    all_items = []
    print("🚀 Fetching Buff163 market data...")

    while True:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=10)

            # --- Handle rate limit and forbidden errors ---
            if r.status_code == 429:
                wait_time = random.uniform(45, 90)
                print(f"⚠️ Rate limit (429) auf Seite {params['page_num']}. Warten {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            if r.status_code == 403:
                print("🚫 403 Forbidden – IP temporär geblockt. Task endet frühzeitig.")
                break

            if r.status_code != 200:
                print(f"❌ HTTP {r.status_code} auf Seite {params['page_num']}")
                break

            resp_json = r.json()

            # --- Handle malformed responses ---
            if "data" not in resp_json:
                print(f"⚠️ Unerwartete Antwort auf Seite {params['page_num']}: {resp_json.get('msg', 'no data')}")
                time.sleep(random.uniform(5, 8))
                continue

            data = resp_json["data"]
            items = data.get("items", [])
            total_pages = data.get("total_page", params["page_num"])
            current_page = data.get("page_num", params["page_num"])

            print(f"✅ Seite {current_page}/{total_pages} → {len(items)} Items")
            all_items.extend(items)

            # --- Safety: Limit pages to 100 per run ---
            if current_page >= 100:
                print("⏹️ Seitenlimit (100) erreicht – Stoppe frühzeitig für POC.")
                break

            if current_page >= total_pages:
                break

            params["page_num"] += 1
            time.sleep(random.uniform(1.5, 3.0))  # human-like delay

        except Exception as e:
            print(f"⚠️ Fehler auf Seite {params['page_num']}: {e}")
            break

    print(f"📦 Total gesammelt: {len(all_items)} Items")

    if not all_items:
        print("⚠️ Keine Daten gesammelt, Task endet.")
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
            print(f"💾 {inserted_count} Items gespeichert...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Fertig! {inserted_count} Buff163-Preise erfolgreich in Postgres geschrieben.")
