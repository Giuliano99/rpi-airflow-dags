from great_expectations import get_context
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
    # Load data using SQLAlchemy connection string to silence warning
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    df = pd.read_sql("SELECT * FROM dart_matches_staging", engine)

    # Initialize GE context (FileDataContext)
    context = get_context(project_root_dir=GE_ROOT_DIR)
    suite_name = "darts_results_suite"

    # Try to get existing suite, else create new
    try:
        suite = context.suites.get(suite_name)
        logger.info(f"Using existing suite '{suite_name}'")
    except Exception:
        logger.info(f"Suite '{suite_name}' not found. Creating it.")
        suite = ExpectationSuite(suite_name)
        context.suites.add(suite)

        # Create validator with pandas dataframe
        validator = context.get_validator(
            datasource_name="my_pandas_datasource",  # Adjust datasource name if different in your config
            data_asset_name="dart_matches_staging",
            batch_data=df
        )
        # Add expectations
        validator.expect_column_values_to_not_be_null("matchdate")
        validator.expect_column_values_to_not_be_null("player1")
        validator.expect_column_values_to_not_be_null("player2")
        validator.expect_column_values_to_not_be_null("winner")

        # Save the expectation suite
        validator.save_expectation_suite(suite_name=suite_name)
        logger.info(f"Saved new expectation suite '{suite_name}'")
        return

    # If suite exists, validate dataframe against it
    validator = context.get_validator(
        datasource_name="my_pandas_datasource",  # Adjust if needed
        data_asset_name="dart_matches_staging",
        batch_data=df,
        expectation_suite_name=suite_name
    )
    results = validator.validate()

    if not results["success"]:
        logger.error("Validation failed!")
        raise Exception("Validation failed")
    else:
        logger.info("Validation succeeded!")

if __name__ == "__main__":
    validate_results()
