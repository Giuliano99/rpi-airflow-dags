import os
import pandas as pd
import psycopg2
import psycopg2.extras

CSV_FOLDER = "/home/pi/airflow/darts_upcoming"
DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def load_raw_upcoming():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS upcoming_matches_staging (
        id SERIAL PRIMARY KEY,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        odds JSONB
    );
    TRUNCATE upcoming_matches_staging;
    """)
    conn.commit()

    for file in os.listdir(CSV_FOLDER):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(CSV_FOLDER, file))
            if df.empty:
                continue
            df.fillna('', inplace=True)

            for _, row in df.iterrows():
                odds = {col: row[col] for col in df.columns if col not in ['Date', 'Player 1', 'Player 2'] and row[col] != ''}
                cursor.execute("""
                    INSERT INTO upcoming_matches_staging (matchdate, player1, player2, odds)
                    VALUES (%s, %s, %s, %s);
                """, (
                    row.get('Date'),
                    str(row.get('Player 1', '')).strip(),
                    str(row.get('Player 2', '')).strip(),
                    psycopg2.extras.Json(odds)
                ))

    conn.commit()
    cursor.close()
    conn.close()