# scripts/validation/init_ge.py

import os
import yaml
from great_expectations.data_context import FileDataContext
from great_expectations.core import ExpectationSuite

def init_ge():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    ge_root = os.path.join(project_root, "great_expectations")

    os.makedirs(os.path.join(ge_root, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(ge_root, "checkpoints"), exist_ok=True)

    # Minimal config
    ge_config = {
        "config_version": 3,
        "datasources": {},
        "stores": {
            "expectations_store": {"class_name": "ExpectationsStore", "store_backend": {"class_name": "FilesystemStoreBackend", "base_directory": "expectations/"}},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validations_store": {"class_name": "ValidationsStore", "store_backend": {"class_name": "FilesystemStoreBackend", "base_directory": "validations/"}},
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "data_docs_sites": {},
        "anonymous_usage_statistics": {"enabled": False},
    }

    # Write config file
    with open(os.path.join(ge_root, "great_expectations.yml"), "w") as f:
        yaml.dump(ge_config, f)

    print("Created minimal Great Expectations project structure.")

    # Initialize context
    context = FileDataContext(ge_root)

    # Create empty suites for both tables
    for suite in ["darts_results_suite", "upcoming_matches_suite"]:
        if not context.list_expectation_suites():
            context.add_expectation_suite(suite)
            print(f"Created ExpectationSuite: {suite}")

    print("✅ Initialization complete.")

if __name__ == "__main__":
    init_ge()
