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

def validate_upcoming():
    logger.info("🚀 Starting validation and clean insertion from upcoming_matches_bronze...")

    mandatory_columns = ["matchdate", "player1", "player2"]

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql("SELECT * FROM upcoming_matches_bronze", conn)
            logger.info(f"📥 Loaded {len(df)} rows from upcoming_matches_bronze.")

            # Identify and log rows with NULL values in mandatory columns
            rows_with_nulls = df[df[mandatory_columns].isnull().any(axis=1)]
            logger.info(f"⚠️ Found {len(rows_with_nulls)} rows with NULL values that will be skipped.")

            # Drop incomplete rows
            df_clean = df.dropna(subset=mandatory_columns)
            logger.info(f"✅ Rows after dropping incomplete entries: {len(df_clean)}.")

            with conn.cursor() as cursor:
                # Ensure clean upcoming table exists
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS upcoming_matches_silver (
                    id SERIAL PRIMARY KEY,
                    matchdate DATE,
                    player1 VARCHAR(100),
                    player2 VARCHAR(100)
                );
                """)
                conn.commit()
                logger.info("🗃️ Ensured upcoming_matches_silver table exists.")

                # Truncate clean table before inserting fresh data
                cursor.execute("TRUNCATE TABLE upcoming_matches_silver;")
                conn.commit()
                logger.info("🧹 Truncated upcoming_matches_silver table before inserting new validated rows.")

                # Insert validated clean data
                insert_count = 0
                for _, row in df_clean.iterrows():
                    cursor.execute("""
                        INSERT INTO upcoming_matches_silver (matchdate, player1, player2)
                        VALUES (%s, %s, %s);
                    """, (
                        row['matchdate'],
                        row['player1'],
                        row['player2']
                    ))
                    insert_count += 1

                conn.commit()
                logger.info(f"🏆 Inserted {insert_count} clean rows into upcoming_matches_silver table successfully.")

    except Exception as e:
        logger.error(f"❌ Error during validation and insertion: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    validate_upcoming()