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
    logger.info("Starting validation of dart_matches_staging...")

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)

        logger.info(f"Loaded {len(df)} rows from dart_matches_staging.")

        expected_columns = ["matchdate", "player1", "player2", "winner"]
        missing_columns = [col for col in expected_columns if col not in df.columns]

        if missing_columns:
            logger.error(f"Missing expected columns: {missing_columns}")
            raise Exception(f"Validation failed due to missing columns: {missing_columns}")

        failed = False

        for col in expected_columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                logger.error(f"Validation failed for column '{col}': {missing_count} missing values")
                failed = True
            else:
                logger.info(f"Column '{col}' passed validation. No missing values.")

        if failed:
            raise Exception("Validation failed. See logs for details.")
        else:
            logger.info("✅ All validations passed successfully.")

    except Exception as e:
        logger.error(f"Validation script encountered an error: {e}")
        raise

if __name__ == "__main__":
    validate_results()
