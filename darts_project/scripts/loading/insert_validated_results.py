import psycopg2

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def insert_results():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Ensure dart_matches_gold table exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dart_matches_gold (
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

    # Insert from validated clean table instead of staging
    cursor.execute("""
    INSERT INTO dart_matches_gold (matchdate, player1, player2, player1score, player2score, winner)
    SELECT matchdate, player1, player2, player1score, player2score, winner
    FROM dart_matches_silver
    ON CONFLICT (player1, player2, matchdate) DO NOTHING;
    """)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insert_results()
