import psycopg2

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def insert_upcoming():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_upcoming_matches (
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

    cursor.execute("""
    INSERT INTO raw_upcoming_matches (matchdate, player1, player2, odds)
    SELECT matchdate, player1, player2, odds
    FROM upcoming_matches_staging
    ON CONFLICT (matchdate, player1, player2) DO NOTHING;
    """)

    conn.commit()
    cursor.close()
    conn.close()