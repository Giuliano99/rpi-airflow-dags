from great_expectations import get_context
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.core.expectation_suite import ExpectationSuite

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

GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_results():
    # Load data
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
    conn.close()

    # Init GE context
    context = get_context(project_root_dir=GE_ROOT_DIR)
    suite_name = "darts_results_suite"

    # Check if suite exists using v3 API
    existing_suites = context.suites.list_suites()
    if suite_name not in existing_suites:
        logger.info(f"Suite '{suite_name}' not found. Creating it.")
        context.suites.add(ExpectationSuite(name=suite_name))

        validator = context.pandas_dataframe_validator(
            df=df,
            expectation_suite_name=suite_name
        )
        validator.expect_column_values_to_not_be_null("matchdate")
        validator.expect_column_values_to_not_be_null("player1")
        validator.expect_column_values_to_not_be_null("player2")
        validator.expect_column_values_to_not_be_null("winner")

        # Save expectations
        validator.save_expectation_suite(discard_failed_expectations=False)
        logger.info(f"Saved new expectation suite '{suite_name}'")
    else:
        logger.info(f"Using existing suite '{suite_name}'")
        validator = context.pandas_dataframe_validator(
            df=df,
            expectation_suite_name=suite_name
        )

    # Run validation
    results = validator.validate()
    if not results["success"]:
        logger.error("Validation failed!")
        raise Exception("Validation failed")
    else:
        logger.info("Validation succeeded!")

if __name__ == "__main__":
    validate_results()
