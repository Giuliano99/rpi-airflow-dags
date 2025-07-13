from great_expectations import get_context
import pandas as pd
import psycopg2
import logging

DB_CONFIG = {
    "host": "100.70.108.39",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

GE_ROOT_DIR = "/app/great_expectations/gx" 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_results():
    # Load data
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
    conn.close()

    # Init GE context with project root
    context = get_context(project_root_dir=GE_ROOT_DIR)
    suite_name = "darts_results_suite"

    try:
        suite = context.get_expectation_suite(suite_name)
        logger.info(f"Using existing suite '{suite_name}'")
    except Exception as e:
        logger.info(f"Suite '{suite_name}' not found. Creating it.")
        suite = context.add_expectation_suite(suite_name)

        # Create validator
        validator = context.sources.pandas_default.read_dataframe(df)
        validator.expect_column_values_to_not_be_null("matchdate")
        validator.expect_column_values_to_not_be_null("player1")
        validator.expect_column_values_to_not_be_null("player2")
        validator.expect_column_values_to_not_be_null("winner")

        # Save suite
        validator.save_expectation_suite(suite_name=suite_name)
        logger.info(f"Saved new expectation suite '{suite_name}'")
        return

    # Validate using existing suite
    validator = context.sources.pandas_default.read_dataframe(df)
    results = validator.validate(expectation_suite_name=suite_name)

    if not results["success"]:
        logger.error("Validation failed!")
        raise Exception("Validation failed")
    else:
        logger.info("Validation succeeded!")

if __name__ == "__main__":
    validate_results()