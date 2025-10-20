import requests
import time
import random
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_skinport_prices():
    base_url = "https://api.skinport.com/v1/items"
    params = {"app_id": 730, "currency": "EUR", "tradable": "true"}
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; ArbitrageBot/1.0)"
    }

    print("Fetching Skinport market data...")
    all_items = []

    try:
        r = requests.get(base_url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            print(f"❌ Error fetching data: {r.status_code} → {r.text}")
            return

        data = r.json()
        if not isinstance(data, list):
            print("❌ Unexpected API format:", data)
            return

        print(f"Fetched {len(data)} Skinport items.")
        all_items.extend(data)

    except Exception as e:
        print(f"❌ Exception fetching Skinport data: {e}")
        return

    print(f"Total collected items: {len(all_items)}")
    if not all_items:
        print("⚠️ No data collected, exiting.")
        return

    # --- Write to database ---
    conn = get_connection()
    cur = conn.cursor()

    # Register Skinport market if not existing
    cur.execute("""
        INSERT INTO markets (name, fee_percent)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
    """, ("Skinport", 0.12))
    conn.commit()

    inserted_count = 0

    for item in all_items:
        item_name = item.get("market_hash_name")
        if not item_name:
            continue

        min_price = item.get("min_price") or 0
        suggested_price = item.get("suggested_price") or 0
        volume = item.get("volume") or 0

        try:
            min_price = float(min_price)
            suggested_price = float(suggested_price)
            volume = int(volume)
        except Exception:
            continue

        cur.execute("""
            INSERT INTO items (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (item_name,))

        cur.execute("""
            INSERT INTO prices (item_id, market_id, price, volume)
            VALUES (
                (SELECT id FROM items WHERE name=%s),
                (SELECT id FROM markets WHERE name='Skinport'),
                %s,
                %s
            )
        """, (item_name, min_price, volume))

        inserted_count += 1
        if inserted_count % 100 == 0:
            conn.commit()
            print(f"Inserted {inserted_count} Skinport items so far...")

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Done. Inserted {inserted_count} Skinport prices into Postgres.")