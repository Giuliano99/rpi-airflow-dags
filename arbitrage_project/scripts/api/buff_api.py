import requests
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_buff_prices():
    """
    Fetches CS:GO item prices from Buff163 API and writes them to Postgres.
    Ensures items and market exist before inserting prices.
    """
    url = "https://buff.163.com/api/market/goods?game=csgo&page_num=1"
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1️⃣ Fetch data from Buff163 API
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()["data"]["items"]
    except Exception as e:
        print("Error fetching Buff163 API:", e)
        return

    print(f"Received {len(data)} items from Buff163 API")

    # 2️⃣ Connect to Postgres
    conn = get_connection()
    cur = conn.cursor()

    # 3️⃣ Ensure the market exists
    cur.execute("""
        INSERT INTO markets (name, fee_percent)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
    """, ("Buff163", 0.02))
    conn.commit()

    # 4️⃣ Loop through API items and insert into DB
    for d in data:
        item_name = d["name"]
        sell_price = float(d["sell_min_price"])
        volume = int(d["sell_num"])

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

        print(f"Inserted price for {item_name}: price={sell_price}, volume={volume}")

    # 5️⃣ Commit all changes and close connection
    conn.commit()
    cur.close()
    conn.close()
    print("Buff163 prices successfully written to Postgres")

