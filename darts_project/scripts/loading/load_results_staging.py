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

def load_raw_results():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

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

    for file in os.listdir(CSV_FOLDER):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(CSV_FOLDER, file))
            if df.empty:
                continue

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dart_matches_staging (matchdate, player1, player2, player1score, player2score, winner)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    row.get('Date'),
                    str(row.get('Player 1', '')).strip(),
                    str(row.get('Player 2', '')).strip(),
                    int(row.get('Player 1 Score', 0) or 0),
                    int(row.get('Player 2 Score', 0) or 0),
                    str(row.get('Winner', '')).strip()
                ))

    conn.commit()
    cursor.close()
    conn.close()