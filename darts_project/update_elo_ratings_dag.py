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

    # ✅ Ensure `elo_match_log` table exists
    create_elo_log_table_query = """
    CREATE TABLE IF NOT EXISTS elo_match_log (
        match_id INT PRIMARY KEY REFERENCES dart_matches(match_id) ON DELETE CASCADE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        player1_elo_before INT,
        player2_elo_before INT,
        player1_elo_after INT,
        player2_elo_after INT,
        elo_change_p1 INT,
        elo_change_p2 INT,
        winner VARCHAR(100),
        match_date VARCHAR(100)
    );
    """
    cursor.execute(create_elo_log_table_query)
    conn.commit()

    cursor.execute("""
        SELECT match_id, player1, player2, player1score, player2score, winner, matchdate
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

        elo_gain = round(k * (1 - Ea))
        elo_loss = round(k * (0 - Eb))

        elo_dict[winner] = Ra + elo_gain
        elo_dict[loser] = Rb + elo_loss

    for match in matches:
        match_id, p1, p2, p1_score, p2_score, winner, match_date = match
        loser = p1 if winner == p2 else p2

        # ✅ Store Elo before update
        p1_elo_before = elo_dict.get(p1, 1500)
        p2_elo_before = elo_dict.get(p2, 1500)

        # ✅ Update Elo ratings
        def update_elo(winner, loser, k=32):
            Ra = elo_dict.get(winner, 1500)
            Rb = elo_dict.get(loser, 1500)

            Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))
            Eb = 1 / (1 + 10 ** ((Ra - Rb) / 400))

            raw_elo_gain = k * (1 - Ea)
            elo_gain = round(raw_elo_gain)
            elo_loss = -elo_gain  # Ensure loss is exactly the gain

            elo_dict[winner] = Ra + elo_gain
            elo_dict[loser] = Rb + elo_loss

            return Ra_after, Rb_after

        p1_elo_after, p2_elo_after = update_elo(winner, loser)

        # ✅ Calculate Elo change
        elo_change_p1 = p1_elo_after - p1_elo_before
        elo_change_p2 = p2_elo_after - p2_elo_before

        # ✅ Insert Elo match log entry
        cursor.execute("""
            INSERT INTO elo_match_log (match_id, player1, player2, player1_elo_before, player2_elo_before,
                                       player1_elo_after, player2_elo_after, elo_change_p1, elo_change_p2, winner, match_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id) DO NOTHING;
        """, (match_id, p1, p2, p1_elo_before, p2_elo_before, p1_elo_after, p2_elo_after, elo_change_p1, elo_change_p2, winner, match_date))

        # ✅ Mark match as processed
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
