from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os
from datetime import datetime

# PostgreSQL Connection
DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

# CSV paths
CSV_RESULTS_FOLDER = "/home/pi/airflow/darts_results"
CSV_UPCOMING_FOLDER = "/home/pi/airflow/darts_upcoming"

# === FUNCTION 1: Load Past Results ===
def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dart_matches (
        match_id SERIAL PRIMARY KEY,
        matchdate DATE, 
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        player1score INT,
        player2score INT,
        winner VARCHAR(100),
        UNIQUE (player1, player2, matchdate)
    );
    """)
    conn.commit()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS new_matches_log (
        match_id INT PRIMARY KEY REFERENCES dart_matches(match_id) ON DELETE CASCADE,
        processed BOOLEAN DEFAULT FALSE
    );
    """)
    conn.commit()

    for filename in os.listdir(CSV_RESULTS_FOLDER):
        if not filename.endswith(".csv"):
            continue

        file_path = os.path.join(CSV_RESULTS_FOLDER, filename)
        if os.path.getsize(file_path) == 0:
            print(f"Skipping empty file: {filename}")
            continue

        df = pd.read_csv(file_path)
        if df.empty:
            continue

        expected_columns = {'Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'}
        if not expected_columns.issubset(df.columns):
            print(f"⚠️ Skipping file with missing columns: {filename}")
            continue

        df = df.fillna({'Date': "1970-01-01", 'Player 1': "Unknown", 'Player 2': "Unknown"})

        for _, row in df.iterrows():
            try:
                matchdate = str(row['Date'])
                p1 = str(row['Player 1']).strip()
                p2 = str(row['Player 2']).strip()
                s1 = int(row['Player 1 Score']) if not pd.isna(row['Player 1 Score']) else 0
                s2 = int(row['Player 2 Score']) if not pd.isna(row['Player 2 Score']) else 0
                winner = str(row['Winner'])

                cursor.execute("""
                    SELECT match_id FROM dart_matches 
                    WHERE player1 = %s AND player2 = %s AND matchdate = %s;
                """, (p1, p2, matchdate))

                if cursor.fetchone() is None:
                    cursor.execute("""
                        INSERT INTO dart_matches (matchdate, player1, player2, player1score, player2score, winner)
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING match_id;
                    """, (matchdate, p1, p2, s1, s2, winner))
            except:
                continue

    cursor.execute("""
    INSERT INTO new_matches_log (match_id, processed)
    SELECT match_id, FALSE FROM dart_matches
    ON CONFLICT (match_id) DO NOTHING;
    """)
    conn.commit()

    cursor.close()
    conn.close()

# === FUNCTION 2: Load Upcoming Matches ===
def load_upcoming_matches():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS upcoming_matches (
        id SERIAL PRIMARY KEY,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        odds JSONB,
        best_odd_player1 FLOAT,
        win_prob_player1 FLOAT,
        best_odd_player2 FLOAT,
        win_prob_player2 FLOAT,
        implied_margin FLOAT,
        elo_prob_player1 FLOAT,
        elo_prob_player2 FLOAT
    );
    """)
    conn.commit()

    for filename in os.listdir(CSV_UPCOMING_FOLDER):
        if not filename.endswith(".csv"):
            continue

        path = os.path.join(CSV_UPCOMING_FOLDER, filename)
        if os.path.getsize(path) == 0:
            print(f"Skipping empty upcoming file: {filename}")
            continue

        df = pd.read_csv(path)
        if df.empty:
            continue

        df.fillna('', inplace=True)

        for _, row in df.iterrows():
            matchdate = row.get('Date', '1970-01-01')
            player1 = str(row.get('Player 1', 'Unknown')).strip()
            player2 = str(row.get('Player 2', 'Unknown')).strip()

            odds = {col: row[col] for col in df.columns if col not in ['Date', 'Player 1', 'Player 2'] and row[col] != ''}

            cursor.execute("""
                INSERT INTO upcoming_matches (matchdate, player1, player2, odds)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (matchdate, player1, player2, psycopg2.extras.Json(odds)))

    conn.commit()
    cursor.close()
    conn.close()

# === DAG 1: Load Results ===
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 2),
    "catchup": False
}

with DAG(
    "load_darts_results",
    default_args=default_args,
    schedule_interval="10 21 * * *",  # 21:10 daily
) as dag_results:

    task_load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    task_load_csv

# === DAG 2: Load Upcoming Matches ===
with DAG(
    "load_upcoming_matches",
    default_args=default_args,
    schedule_interval= None,
) as dag_upcoming:

    task_load_upcoming = PythonOperator(
        task_id="load_upcoming_csv",
        python_callable=load_upcoming_matches
    )


    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_upcoming",
        trigger_dag_id="transform_upcoming",  
    )

    task_load_upcoming >> trigger_transform_dag

