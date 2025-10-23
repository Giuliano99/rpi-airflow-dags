import requests
import time
import random
from arbitrage_project.scripts.db.config_db import get_connection


def get_cny_to_eur_rate():
    """Fetch current exchange rate from CNY to EUR."""
    try:
        url = "https://api.exchangerate.host/latest?base=CNY&symbols=EUR"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        rate = resp.json()["rates"]["EUR"]
        print(f"💱 Aktueller Wechselkurs CNY→EUR: {rate:.4f}")
        return rate
    except Exception as e:
        print(f"⚠️ Fehler beim Abrufen des Wechselkurses: {e}")
        # Fallback-Wert (ungefährer Durchschnitt)
        return 0.13


def fetch_buff_prices():
    base_url = "https://buff.163.com/api/market/goods"
    headers = {
        "User-Agent": f"Mozilla/5.0 (ArbBot/{random.randint(100,999)})",
        "Accept": "application/json",
        "Referer": "https://buff.163.com/",
    }
    params = {"game": "csgo", "page_num": 1}

    exchange_rate = get_cny_to_eur_rate()
    all_items = []

    print("🚀 Fetching Buff163 market data...")
    retry_count = 0
    max_retries = 5

    while True:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=15)

            # --- Handle Rate Limits (HTTP 429) ---
            if r.status_code == 429:
                wait_time = random.uniform(30, 60) * (retry_count + 1)
                print(f"⚠️ Rate limit (429) auf Seite {params['page_num']}. Warten {wait_time:.1f}s...")
                time.sleep(wait_time)
                retry_count += 1
                if retry_count > max_retries:
                    print("❌ Zu viele 429-Fehler. Abbruch.")
                    break
                continue

            # --- Handle Non-OK Responses ---
            if r.status_code != 200:
                print(f"❌ HTTP {r.status_code} auf Seite {params['page_num']}")
                break

            retry_count = 0  # Reset auf Erfolg
            resp_json = r.json()

            if "data" not in resp_json:
                print(f"⚠️ Keine 'data'-Struktur auf Seite {params['page_num']}: {resp_json.get('msg', 'no data')}")
                time.sleep(random.uniform(5, 10))
                continue

            data = resp_json["data"]
            items = data.get("items", [])
            total_pages = data.get("total_page", params["page_num"])
            current_page = data.get("page_num", params["page_num"])

            print(f"✅ Seite {current_page}/{total_pages} → {len(items)} Items")
            all_items.extend(items)

            if current_page >= total_pages:
                break

            params["page_num"] += 1
            time.sleep(random.uniform(1.5, 3.5))  # menschlichere Pausen

        except Exception as e:
            print(f"⚠️ Fehler auf Seite {params['page_num']}: {e}")
            time.sleep(random.uniform(10, 20))
            continue

    print(f"📦 Total gesammelt: {len(all_items)} Items")

    if not all_items:
        print("⚠️ Keine Daten gesammelt, Task endet.")
        return

    # --- Write to PostgreSQL ---
    conn = get_connection()
    cur = conn.cursor()

    # Markt sicherstellen
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

        sell_price_cny = float(d.get("sell_min_price") or 0)
        sell_price_eur = sell_price_cny * exchange_rate
        volume = int(d.get("sell_num") or 0)

        # Item einfügen
        cur.execute("""
            INSERT INTO items (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (item_name,))

        # Preis speichern (in EUR)
        cur.execute("""
            INSERT INTO prices (item_id, market_id, price, volume, currency)
            VALUES (
                (SELECT id FROM items WHERE name=%s),
                (SELECT id FROM markets WHERE name='Buff163'),
                %s,
                %s,
                'EUR'
            )
        """, (item_name, sell_price_eur, volume))

        inserted_count += 1
        if inserted_count % 100 == 0:
            conn.commit()
            print(f"💾 {inserted_count} Items gespeichert...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Fertig! {inserted_count} Buff163-Preise in EUR in Postgres gespeichert.")
