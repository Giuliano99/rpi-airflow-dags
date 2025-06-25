import os
import pandas as pd
import psycopg2

CSV_FOLDER = "/home/pi/airflow/darts_results"
DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def safe_int(val):
    """Converts value to int, returning 0 if NaN or invalid."""
    try:
        return 0 if pd.isna(val) else int(val)
    except Exception as e:
        print(f"[⚠️] Could not convert value to int: {val} ({e})")
        return 0

def load_raw_results():
    print("[ℹ️] Starting to load raw results...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("[ℹ️] Ensuring staging table exists and is clean...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dart_matches_staging (
        id SERIAL PRIMARY KEY,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        player1score INT,
        player2score INT,
        winner VARCHAR(100)
    );
    TRUNCATE dart_matches_staging;
    """)
    conn.commit()

    total_inserted = 0
    total_skipped = 0

    for file in os.listdir(CSV_FOLDER):
        if file.endswith(".csv"):
            file_path = os.path.join(CSV_FOLDER, file)
            print(f"[📄] Processing file: {file_path}")
            df = pd.read_csv(file_path)

            if df.empty:
                print(f"[⚠️] Skipping empty file: {file}")
                continue

            for i, row in df.iterrows():
                try:
                    match_date = row.get('Date')
                    player1 = str(row.get('Player 1', '')).strip()
                    player2 = str(row.get('Player 2', '')).strip()
                    player1_score = safe_int(row.get('Player 1 Score'))
                    player2_score = safe_int(row.get('Player 2 Score'))
                    winner = str(row.get('Winner', '')).strip()

                    if not match_date or not player1 or not player2 or not winner:
                        print(f"[⚠️] Skipping incomplete row {i}: {row.to_dict()}")
                        total_skipped += 1
                        continue

                    cursor.execute("""
                        INSERT INTO dart_matches_staging (matchdate, player1, player2, player1score, player2score, winner)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (
                        match_date, player1, player2, player1_score, player2_score, winner
                    ))
                    total_inserted += 1

                except Exception as e:
                    print(f"[❌] Failed to insert row {i} from {file}: {e}")
                    total_skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[✅] Finished loading results. Inserted: {total_inserted}, Skipped: {total_skipped}")