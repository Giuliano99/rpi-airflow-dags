import requests
from arbitrage_project.scripts.db.config_db import get_connection

def fetch_buff_prices():
    url = "https://buff.163.com/api/market/goods?game=csgo&page_num=1"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    data = r.json()["data"]["items"]

    # ✅ Debug print to see what the API returned
    print("Received items:", data)



    conn = get_connection()
    cur = conn.cursor()
    for d in data:
        cur.execute("""
            INSERT INTO prices (item_id, market_id, price, volume)
            SELECT i.id, m.id, %s, %s
            FROM items i, markets m
            WHERE i.name=%s AND m.name='Buff163'
        """, (float(d["sell_min_price"]), int(d["sell_num"]), d["name"]))
    conn.commit()
    cur.close()
    conn.close()
