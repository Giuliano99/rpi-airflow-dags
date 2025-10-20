import requests
import time
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_skinport_prices():
    api_key = Variable.get("SKINPORT_API_KEY")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "ArbitrageBot/1.0"
    }

    base_url = "https://api.skinport.com/v1/items"
    params = {
        "app_id": 730,        # CS:GO
        "currency": "EUR",
        "page": 1
    }

    all_items = []
    print("Fetching Skinport market data...")

    while True:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=10)
            if r.status_code != 200:
                print(f"❌ Error fetching data: {r.status_code} → {r.text}")
                break

            resp_json = r.json()
            items = resp_json.get("items", [])
            total_pages = resp_json.get("total_pages", params["page"])
            current_page = resp_json.get("current_page", params["page"])

            print(f"Fetched page {current_page}/{total_pages} → {len(items)} items")
            all_items.extend(items)

            if current_page >= total_pages:
                break

            params["page"] += 1
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            print(f"Error on page {params['page']}: {e}")
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
    """, ("Skinport", 0.12))  # example 12% fee
    conn.commit()

    inserted_count = 0
    for d in all_items:
        item_name = d.get("market_hash_name") or d.get("name")
        if not item_name:
            continue

        sell_price = float(d.get("lowest_price") or 0)
        volume = int(d.get("volume") or 0)

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
        """, (item_name, sell_price, volume))

        inserted_count += 1
        if inserted_count % 100 == 0:
            conn.commit()
            print(f"Inserted {inserted_count} items so far...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Done. Inserted {inserted_count} Skinport prices into Postgres.")