from great_expectations import get_context

def init_darts_results_suite():
    GE_ROOT_DIR = "/home/pi/airflow/dags/darts_project/great_expectations"
    suite_name = "darts_results_suite"

    context = get_context(project_root_dir=GE_ROOT_DIR)

    # ✅ Corrected for GE v1.5.1
    existing_suites = context.list_expectation_suite_names()

    if suite_name in existing_suites:
        print(f"Suite '{suite_name}' already exists.")
        return

    # ✅ Create and save new suite
    suite = context.create_expectation_suite(suite_name)
    context.save_expectation_suite(expectation_suite=suite)
    print(f"✅ Created new expectation suite '{suite_name}'")

if __name__ == "__main__":
    init_darts_results_suite()
