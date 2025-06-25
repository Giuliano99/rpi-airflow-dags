import os
import yaml

def init_ge():
    ge_root = "/home/pi/airflow/dags/darts_project/great_expectations"
    os.makedirs(ge_root, exist_ok=True)

    ge_config = {
        "config_version": 3,
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "FilesystemStoreBackend",
                    "base_directory": "expectations/"
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "FilesystemStoreBackend",
                    "base_directory": "validations/"
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            }
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "data_docs_sites": {},
        "anonymous_usage_statistics": {
            "enabled": False
        }
    }

    config_path = os.path.join(ge_root, "great_expectations.yml")
    with open(config_path, "w") as f:
        yaml.dump(ge_config, f, sort_keys=False)

    print(f"Created valid great_expectations.yml at {config_path}")

if __name__ == "__main__":
    init_ge()
