from great_expectations import get_context

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    # Check if suite exists
    existing_suites = [suite.name for suite in context.suites.list_suites()]
    if suite_name in existing_suites:
        print(f"Suite '{suite_name}' already exists.")
        return

    # Create new suite
    suite = context.suites.add(suite_name)
    context.suites.save(suite)
    print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()