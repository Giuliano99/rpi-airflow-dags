import sys
import os

# Import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Direct imports instead of subprocess
from scripts.loading.load_results_staging import load_raw_results
from scripts.validation.validate_results_staging import validate_results
from scripts.loading.insert_validated_results import insert_results

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 20),
    "catchup": False,
}

with DAG("load_results_to_raw_dag", default_args=default_args, schedule_interval=None) as dag:
    t1 = PythonOperator(task_id="load_raw_results_csvs", python_callable=load_raw_results)
    t2 = PythonOperator(task_id="validate_results_staging", python_callable=validate_results)
    t3 = PythonOperator(task_id="insert_validated_results", python_callable=insert_results)

    t1 >> t2 >> t3
