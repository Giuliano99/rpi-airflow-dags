from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
import great_expectations as ge
import psycopg2.extras
from great_expectations.dataset import PandasDataset

# PostgreSQL Config
DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

CSV_RESULTS_FOLDER = "/home/pi/airflow/darts_results"
CSV_UPCOMING_FOLDER = "/home/pi/airflow/darts_upcoming"

# === Load Results CSV ===
def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create Table
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

        ge_df = ge.from_pandas(df)

        # Great Expectations checks
        ge_df.expect_table_columns_to_match_ordered_list(['Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'])
        ge_df.expect_compound_columns_to_be_unique(['Date', 'Player 1', 'Player 2'])
        ge_df.expect_column_values_to_not_be_null('Date')
        ge_df.expect_column_values_to_not_be_null('Player 1')
        ge_df.expect_column_values_to_not_be_null('Player 2')
        ge_df.expect_column_values_to_not_be_null('Winner')

        if not ge_df.validate().success:
            print(f"❌ Validation failed for file {filename}. Skipping.")
            continue

        for _, row in df.iterrows():
            try:
                matchdate = str(row['Date'])
                p1 = str(row['Player 1']).strip()
                p2 = str(row['Player 2']).strip()
                s1 = int(row['Player 1 Score']) if pd.notna(row['Player 1 Score']) else 0
                s2 = int(row['Player 2 Score']) if pd.notna(row['Player 2 Score']) else 0
                winner = str(row['Winner'])

                cursor.execute("""
                    INSERT INTO dart_matches (matchdate, player1, player2, player1score, player2score, winner)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player1, player2, matchdate) DO NOTHING;
                """, (matchdate, p1, p2, s1, s2, winner))

            except Exception as e:
                print(f"Failed row in {filename}: {e}")
                continue

    conn.commit()
    cursor.close()
    conn.close()

# === Load Upcoming Matches CSV ===
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
        elo_prob_player2 FLOAT,
        norm_prob_player1 FLOAT,
        norm_prob_player2 FLOAT,
        brier_elo FLOAT,
        brier_bookmaker FLOAT,
        logloss_elo FLOAT,
        logloss_bookmaker FLOAT,
        winner VARCHAR(100),
        UNIQUE (matchdate, player1, player2)
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

        ge_df = ge.from_pandas(df)

        # Great Expectations checks
        ge_df.expect_table_columns_to_contain(['Date', 'Player 1', 'Player 2'])
        ge_df.expect_column_values_to_not_be_null('Date')
        ge_df.expect_column_values_to_not_be_null('Player 1')
        ge_df.expect_column_values_to_not_be_null('Player 2')
        ge_df.expect_compound_columns_to_be_unique(['Date', 'Player 1', 'Player 2'])

        if not ge_df.validate().success:
            print(f"❌ Validation failed for file {filename}. Skipping.")
            continue

        for _, row in df.iterrows():
            matchdate = row.get('Date', '1970-01-01')
            player1 = str(row.get('Player 1', 'Unknown')).strip()
            player2 = str(row.get('Player 2', 'Unknown')).strip()

            odds = {col: row[col] for col in df.columns if col not in ['Date', 'Player 1', 'Player 2'] and row[col] != ''}

            try:
                cursor.execute("""
                    INSERT INTO upcoming_matches (matchdate, player1, player2, odds)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (matchdate, player1, player2) DO NOTHING;
                """, (matchdate, player1, player2, psycopg2.extras.Json(odds)))
            except Exception as e:
                print(f"Insert error in upcoming match {filename}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

# === DAG Definitions ===
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 2),
    "catchup": False
}

with DAG(
    "load_darts_results",
    default_args=default_args,
    schedule_interval="10 21 * * *",
) as dag_results:

    task_load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    task_load_csv

with DAG(
    "load_upcoming_matches",
    default_args=default_args,
    schedule_interval=None,
) as dag_upcoming:

    task_load_upcoming = PythonOperator(
        task_id="load_upcoming_csv",
        python_callable=load_upcoming_matches
    )

    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_upcoming",
        trigger_dag_id="transform_upcoming"
    )

    task_load_upcoming >> trigger_transform_dag