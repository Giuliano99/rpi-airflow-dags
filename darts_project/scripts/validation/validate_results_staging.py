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
    # Load data
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
    conn.close()

    # Define validation checks
    checks = {
        "matchdate": df["matchdate"].notnull().all(),
        "player1": df["player1"].notnull().all(),
        "player2": df["player2"].notnull().all(),
        "winner": df["winner"].notnull().all()
    }

    failed_checks = [col for col, passed in checks.items() if not passed]

    if failed_checks:
        logger.error(f"Validation failed for columns: {failed_checks}")
        raise Exception(f"Validation failed for columns: {failed_checks}")
    else:
        logger.info("All validations passed successfully.")

if __name__ == "__main__":
    validate_results()
