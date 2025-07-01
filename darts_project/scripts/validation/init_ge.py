import great_expectations as ge
from great_expectations.data_context import DataContext

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = DataContext(GE_ROOT_DIR)

    # List existing suites via the store backend
    existing_suites = context.list_expectation_suites()
    existing_suite_names = [suite.expectation_suite_name for suite in existing_suites]

    if suite_name in existing_suite_names:
        print(f"Suite '{suite_name}' already exists.")
    else:
        context.create_expectation_suite(suite_name, overwrite_existing=False)
        print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()
