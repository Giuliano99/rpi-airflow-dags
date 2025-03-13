from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "172.17.0.2",  # Or the IP of your Raspberry Pi if accessing remotely
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"  # Replace with your actual password
}

def calculate_elo():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Fetch unprocessed matches
    cursor.execute("""
        SELECT match_id, player1, player2, player1score, player2score, winner
        FROM dart_matches
        WHERE match_id IN (SELECT match_id FROM new_matches_log WHERE processed = FALSE)
        ORDER BY matchdate ASC
    """)
    matches = cursor.fetchall()

    if not matches:
        print("No new matches to process.")
        return

    # Fetch current Elo ratings
    cursor.execute("SELECT player, elo FROM elo_rankings")
    elo_dict = {row[0]: row[1] for row in cursor.fetchall()}

    # Elo calculation function
    def update_elo(winner, loser, k=32):
        Ra = elo_dict.get(winner, 1500)
        Rb = elo_dict.get(loser, 1500)

        Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))
        Eb = 1 / (1 + 10 ** ((Ra - Rb) / 400))

        elo_dict[winner] = Ra + k * (1 - Ea)
        elo_dict[loser] = Rb + k * (0 - Eb)

    # Process matches
    for match in matches:
        match_id, p1, p2, p1_score, p2_score, winner = match
        loser = p1 if winner == p2 else p2

        update_elo(winner, loser)

        # Mark match as processed
        cursor.execute("UPDATE new_matches_log SET processed = TRUE WHERE match_id = %s", (match_id,))

    # Update Elo rankings in database
    for player, elo in elo_dict.items():
        cursor.execute("""
            INSERT INTO elo_rankings (player, elo)
            VALUES (%s, %s)
            ON CONFLICT (player) DO UPDATE SET elo = EXCLUDED.elo
        """, (player, elo))

    conn.commit()
    cursor.close()
    conn.close()
    print("Elo ratings updated successfully.")

# Airflow DAG setup
default_args = {"owner": "airflow", "start_date": datetime(2025, 2, 2), "catchup": False}

with DAG("update_elo_ratings", default_args=default_args, schedule_interval="@hourly") as dag:
    task_update_elo = PythonOperator(
        task_id="calculate_elo",
        python_callable=calculate_elo
    )

    task_update_elo
