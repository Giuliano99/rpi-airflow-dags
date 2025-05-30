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


    #DEBUG#
    # ✅ Ensure elo_rankings table exists
    create_elo_rankings_table_query = """
    CREATE TABLE IF NOT EXISTS elo_rankings (
        player VARCHAR(100) PRIMARY KEY,
        elo INT NOT NULL
    );
    """
    cursor.execute(create_elo_rankings_table_query)
    conn.commit()

    # ✅ Insert only the selected players from dart_matches
    insert_new_players_query = """
        INSERT INTO elo_rankings (player, elo)
        SELECT DISTINCT player1, 1500 FROM dart_matches
        WHERE player1 IN (
            'van Gerwen M.', 'Aspinall N.', 'Price G.', 'Bunting S.',
            'Dobey C.', 'Cross R.', 'Littler L.', 'Humphries L.'
        )
        AND player1 NOT IN (SELECT player FROM elo_rankings)
        UNION
        SELECT DISTINCT player2, 1500 FROM dart_matches
        WHERE player2 IN (
            'van Gerwen M.', 'Aspinall N.', 'Price G.', 'Bunting S.',
            'Dobey C.', 'Cross R.', 'Littler L.', 'Humphries L.'
        )
        AND player2 NOT IN (SELECT player FROM elo_rankings);
    """
    cursor.execute(insert_new_players_query)
    conn.commit()

    cursor.close()
    conn.close()


def calculate_elo():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    #DEBUG#
    cursor.execute("SELECT COUNT(*) FROM new_matches_log WHERE processed = FALSE")
    print(f"Unprocessed matches in DB seen by Airflow: {cursor.fetchone()[0]}")

    cursor.execute("SELECT current_database()")
    print(f"Connected to DB: {cursor.fetchone()[0]}")


    #DEBUG#

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
        match_date TIMESTAMP
    );
    """
    cursor.execute(create_elo_log_table_query)
    conn.commit()


    # ✅ Get new matches
    cursor.execute("""
    SELECT match_id, player1, player2, player1score, player2score, winner, matchdate
    FROM dart_matches
    WHERE match_id IN (
        SELECT match_id FROM new_matches_log
        WHERE processed = FALSE
    )
    AND matchdate >= '2025-02-06'
    AND EXTRACT(DOW FROM matchdate::timestamp) = 4
    AND player1 IN (
        'van Gerwen M.', 'Aspinall N.', 'Price G.', 'Bunting S.',
        'Dobey C.', 'Cross R.', 'Littler L.', 'Humphries L.'
    )
    AND player2 IN (
        'van Gerwen M.', 'Aspinall N.', 'Price G.', 'Bunting S.',
        'Dobey C.', 'Cross R.', 'Littler L.', 'Humphries L.'
    )
    ORDER BY matchdate ASC, match_id ASC
    """)
    matches = cursor.fetchall()

    if not matches:
        print("No new matches to process.")
        return

    # ✅ Load current Elo rankings
    cursor.execute("SELECT player, elo FROM elo_rankings")
    elo_dict = {row[0]: row[1] for row in cursor.fetchall()}

    def update_elo(winner, loser, k=32):
        Ra = elo_dict.get(winner, 1500)  # Winner's current Elo
        Rb = elo_dict.get(loser, 1500)   # Loser's current Elo

        Ea = 1 / (1 + 10 ** ((Rb - Ra) / 400))  # Expected score for winner
        Eb = 1 / (1 + 10 ** ((Ra - Rb) / 400))  # Expected score for loser

        raw_elo_gain = k * (1 - Ea)
        elo_gain = round(raw_elo_gain)
        elo_loss = -elo_gain  # Ensure gain and loss are equal

        new_Ra = Ra + elo_gain  # New Elo for winner
        new_Rb = Rb + elo_loss  # New Elo for loser

        # Update dictionary
        elo_dict[winner] = new_Ra
        elo_dict[loser] = new_Rb

        return new_Ra, new_Rb, elo_gain, elo_loss  # Return updated ratings and changes

    for match in matches:
        match_id, p1, p2, p1_score, p2_score, winner, match_date = match
        loser = p1 if winner == p2 else p2

        # ✅ Store Elo before update
        p1_elo_before = elo_dict.get(p1, 1500)
        p2_elo_before = elo_dict.get(p2, 1500)

        # ✅ Update Elo ratings
        p1_is_winner = (winner == p1)
        p1_elo_after, p2_elo_after, elo_change_winner, elo_change_loser = update_elo(winner, loser)

        # Assign changes to correct player columns
        if p1_is_winner:
            elo_change_p1 = elo_change_winner
            elo_change_p2 = elo_change_loser
        else:
            elo_change_p1 = elo_change_loser
            elo_change_p2 = elo_change_winner

            # ✅ Insert Elo match log entry
            cursor.execute("""
                INSERT INTO elo_match_log (match_id, player1, player2, player1_elo_before, player2_elo_before,
                                        player1_elo_after, player2_elo_after, elo_change_p1, elo_change_p2, winner, match_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO NOTHING;
            """, (
                match_id, p1, p2,
                p1_elo_before, p2_elo_before,
                p1_elo_after, p2_elo_after,
                elo_change_p1, elo_change_p2,
                winner, match_date
            ))

        # ✅ Mark match as processed
        cursor.execute("UPDATE new_matches_log SET processed = TRUE WHERE match_id = %s", (match_id,))

    # ✅ Update `elo_rankings` table
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
