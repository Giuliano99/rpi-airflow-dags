from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def add_new_players():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_new_players_query = """
        INSERT INTO elo_rankings (player, elo)
        SELECT DISTINCT player1, 1500 FROM dart_matches
        WHERE player1 NOT IN (SELECT player FROM elo_rankings)
        UNION
        SELECT DISTINCT player2, 1500 FROM dart_matches
        WHERE player2 NOT IN (SELECT player FROM elo_rankings);
    """
    
    cursor.execute(insert_new_players_query)
    conn.commit()
    cursor.close()
    conn.close()

def calculate_elo():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

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

    cursor.execute("SELECT player, elo FROM elo_rankings")
    elo_dict = {row[0]: row[1] for row in cursor.fetchall()}

    def update_elo(winner, loser, k=32):
        Ra = elo_dict.get(winner, 1500)
        Rb = elo_dict.get(loser, 1500)

        Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))
        Eb = 1 / (1 + 10 ** ((Ra - Rb) / 400))

        elo_dict[winner] = Ra + k * (1 - Ea)
        elo_dict[loser] = Rb + k * (0 - Eb)

    for match in matches:
        match_id, p1, p2, p1_score, p2_score, winner = match
        loser = p1 if winner == p2 else p2

        update_elo(winner, loser)

        cursor.execute("UPDATE new_matches_log SET processed = TRUE WHERE match_id = %s", (match_id,))

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

# ✅ Define DAG before referencing it
default_args = {"owner": "airflow", "start_date": datetime(2025, 3, 13), "catchup": False}

with DAG("update_elo_ratings", default_args=default_args, schedule_interval="@daily") as dag:
    task_add_new_players = PythonOperator(
        task_id="add_new_players",
        python_callable=add_new_players
    )

    task_calculate_elo = PythonOperator(
        task_id="calculate_elo",
        python_callable=calculate_elo
    )

    task_add_new_players >> task_calculate_elo  # Ensure new players exist before Elo calculation
