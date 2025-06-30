from great_expectations import get_context
from great_expectations.exceptions import SuiteNotFoundError

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    try:
        # This is the correct way to get a suite in v1.5.1
        context.suites.get(suite_name)
        print(f"Suite '{suite_name}' already exists.")
    except SuiteNotFoundError:
        suite = context.suites.create(suite_name)
        context.suites.save(suite)
        print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()
