import pandas as pd
import psycopg2
from great_expectations.data_context import get_context

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def validate_results():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM dart_matches_staging", conn)
    conn.close()

    context = get_context()
    suite_name = "darts_results_suite"

    try:
        context.get_expectation_suite(suite_name)
    except:
        context.create_expectation_suite(suite_name)
        validator = context.get_validator(pandas_df=df, expectation_suite_name=suite_name)
        validator.expect_column_values_to_not_be_null("matchdate")
        validator.expect_column_values_to_not_be_null("player1")
        validator.expect_column_values_to_not_be_null("player2")
        validator.expect_column_values_to_not_be_null("winner")
        context.save_expectation_suite(validator.get_expectation_suite())

    validator = context.get_validator(pandas_df=df, expectation_suite_name=suite_name)
    result = validator.validate()
    if not result.success:
        raise Exception("Validation failed")