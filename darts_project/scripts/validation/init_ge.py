from great_expectations import get_context

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    # ✅ Correct API usage — list existing suites
    existing_suites = [suite["expectation_suite_name"] for suite in context.list_expectation_suites()]

    if suite_name in existing_suites:
        print(f"Suite '{suite_name}' already exists.")
        return

    # ✅ Create and save new suite
    suite = context.suites.add(suite_name)
    context.suites.save(suite)
    print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()
