from great_expectations import get_context
from great_expectations.exceptions import DataContextError

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    # ✅ Try to get suite instead of listing suites
    try:
        context.get_expectation_suite(suite_name)
        print(f"Suite '{suite_name}' already exists.")
    except DataContextError:
        # ✅ Create suite if it does not exist
        suite = context.create_expectation_suite(suite_name)
        context.save_expectation_suite(expectation_suite=suite)
        print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()
