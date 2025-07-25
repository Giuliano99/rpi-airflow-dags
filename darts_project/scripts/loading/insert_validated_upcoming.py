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

    # Create upcoming_matches_gold table with only scraped fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS upcoming_matches_gold (
        id SERIAL PRIMARY KEY,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        odds JSONB,
        UNIQUE (matchdate, player1, player2)
    );
    """)

    # Insert validated upcoming matches into raw table
    cursor.execute("""
    INSERT INTO upcoming_matches_gold (matchdate, player1, player2, odds)
    SELECT matchdate, player1, player2, odds
    FROM upcoming_matches_silver
    ON CONFLICT (matchdate, player1, player2) DO NOTHING;
    """)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insert_upcoming()