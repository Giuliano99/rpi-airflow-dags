import pandas as pd
import psycopg2
import logging
from great_expectations.data_context.data_context import DataContext
from great_expectations.exceptions import GreatExpectationsError

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

# Adjust this path to where your great_expectations/ folder lives
GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_results():
    # Connect to DB and read table into pandas dataframe
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
    conn.close()

    # Load GE context explicitly from your initialized project directory
    context = DataContext(GE_ROOT_DIR)
    suite_name = "darts_results_suite"

    try:
        # Try loading existing expectation suite
        expectation_suite = context.get_expectation_suite(suite_name)
        logger.info(f"Loaded existing expectation suite '{suite_name}'")
    except GreatExpectationsError:
        # Suite not found, create new one and add expectations
        logger.info(f"Expectation suite '{suite_name}' not found, creating a new one.")
        context.create_expectation_suite(suite_name, overwrite_existing=True)
        validator = context.get_validator(
            batch={"pandas": df},
            expectation_suite_name=suite_name
        )
        validator.expect_column_values_to_not_be_null("matchdate")
        validator.expect_column_values_to_not_be_null("player1")
        validator.expect_column_values_to_not_be_null("player2")
        validator.expect_column_values_to_not_be_null("winner")
        # Save the new expectation suite
        context.save_expectation_suite(expectation_suite=validator.get_expectation_suite())
        logger.info(f"Created and saved new expectation suite '{suite_name}'")
    else:
        # If suite exists, get a validator for validation
        validator = context.get_validator(
            batch={"pandas": df},
            expectation_suite_name=suite_name
        )

    # Validate data
    result = validator.validate()
    if not result.success:
        logger.error("Validation failed.")
        raise Exception("Validation failed")
    else:
        logger.info("Validation passed successfully.")

if __name__ == "__main__":
    validate_results()
