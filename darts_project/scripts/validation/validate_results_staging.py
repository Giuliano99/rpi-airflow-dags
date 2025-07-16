import pandas as pd
import psycopg2
import logging

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_results():
    logger.info("Starting validation and clean insertion from staging...")

    mandatory_columns = ["matchdate", "player1", "player2", "player1score", "player2score", "winner"]

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
            logger.info(f"Loaded {len(df)} rows from staging.")

            # Count rows with any null values
            rows_with_nulls = df[df[mandatory_columns].isnull().any(axis=1)]
            logger.info(f"Found {len(rows_with_nulls)} rows with NULL values that will be skipped.")

            # Drop rows with missing values in mandatory columns
            df_clean = df.dropna(subset=mandatory_columns)
            logger.info(f"Rows after dropping incomplete entries: {len(df_clean)}.")

            with conn.cursor() as cursor:
                # Ensure clean table exists
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS dart_matches_clean (
                    id SERIAL PRIMARY KEY,
                    matchdate DATE,
                    player1 VARCHAR(100),
                    player2 VARCHAR(100),
                    player1score INT,
                    player2score INT,
                    winner VARCHAR(100)
                );
                """)
                conn.commit()

                insert_count = 0

                for _, row in df_clean.iterrows():
                    cursor.execute("""
                        INSERT INTO dart_matches_clean (matchdate, player1, player2, player1score, player2score, winner)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (
                        row['matchdate'],
                        row['player1'],
                        row['player2'],
                        int(row['player1score']),
                        int(row['player2score']),
                        row['winner']
                    ))
                    insert_count += 1

                conn.commit()

                logger.info(f"✅ Inserted {insert_count} clean rows into dart_matches_clean table.")

    except Exception as e:
        logger.error(f"Error during validation and insertion: {e}")
        raise

if __name__ == "__main__":
    validate_results()
