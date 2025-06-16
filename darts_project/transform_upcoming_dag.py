from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import psycopg2

# Make sure local files can be imported
sys.path.append(os.path.dirname(__file__))

from transform_upcoming import transform_upcoming_odds_and_update_elo

# === New: Function to update winner ===
def update_winners_in_upcoming_matches():
    conn = psycopg2.connect(
        host="172.17.0.2",
        port="5432",
        database="darts_project",
        user="postgres",
        password="5ads15"
    )
    cursor = conn.cursor()

    # Ensure the column exists (already done, but for safety)
    cursor.execute("""
        ALTER TABLE upcoming_matches
        ADD COLUMN IF NOT EXISTS winner VARCHAR(100);
    """)
    conn.commit()

    # Match by date and player names
    cursor.execute("""
        UPDATE upcoming_matches AS u
        SET winner = d.winner
        FROM dart_matches AS d
        WHERE u.winner IS NULL
          AND u.matchdate = d.matchdate
          AND (
            (u.player1 = d.player1 AND u.player2 = d.player2)
             OR
            (u.player1 = d.player2 AND u.player2 = d.player1)
          );
    """)
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ Upcoming matches updated with winners.")

# === DAG ===
with DAG(
    dag_id="transform_upcoming",
    start_date=datetime(2025, 2, 2),
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id="calculate_best_odds_and_probs",
        python_callable=transform_upcoming_odds_and_update_elo
    )

    update_winners_task = PythonOperator(
        task_id="update_actual_results",
        python_callable=update_winners_in_upcoming_matches
    )

    transform_task >> update_winners_task
