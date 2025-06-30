from great_expectations import get_context
from great_expectations.core.expectation_suite import ExpectationSuite  # Import ExpectationSuite
from great_expectations.data_context.types.base import DataContextConfig  # Potentially useful for type hinting, though not strictly necessary for this fix

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    # Check if the suite exists by listing existing suites
    existing_suite_names = [suite.expectation_suite_name for suite in context.list_expectation_suites()]

    if suite_name in existing_suite_names:
        print(f"Suite '{suite_name}' already exists.")
    else:
        # Suite doesn't exist, create it
        # You need to explicitly create an ExpectationSuite object
        new_suite = ExpectationSuite(expectation_suite_name=suite_name)
        context.save_expectation_suite(new_suite)
        print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()