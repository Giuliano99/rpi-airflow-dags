from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.load_results_staging import load_raw_results
from scripts.validate_results_staging import validate_results
from scripts.insert_validated_results import insert_results

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