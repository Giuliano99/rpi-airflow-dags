from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import time
import random
from datetime import datetime
import requests
import brotli
import urllib3
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_skinport_prices():
    api_key = Variable.get("SKINPORT_API_KEY")
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "ArbitrageBot/1.0"
    }

    base_url = "https://api.skinport.com/v1/items"
    params = {
        "app_id": 730,        # CS2 / CS:GO
        "currency": "EUR",
        "tradable": "true"
    }

    print("Fetching Skinport market data...")
    try:
        r = requests.get(base_url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"❌ Error fetching data: {r.status_code} → {r.text}")
            return

        items = r.json()
        print(f"✅ Received {len(items)} items from Skinport.")
        for item in items[:3]:
            print(item)

    except Exception as e:
        print(f"Error fetching Skinport data: {e}")
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
    for d in items:
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