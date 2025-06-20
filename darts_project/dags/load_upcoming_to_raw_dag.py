from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dag_utils import run_script

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 20),
    "catchup": False,
}

with DAG("load_upcoming_to_raw_dag", default_args=default_args, schedule_interval=None) as dag:
    t1 = PythonOperator(task_id="load_raw_upcoming_csvs", python_callable=run_script('load_upcoming_staging.py'))
    t2 = PythonOperator(task_id="validate_upcoming_staging", python_callable=run_script('validate_upcoming_staging.py'))
    t3 = PythonOperator(task_id="insert_validated_upcoming", python_callable=run_script('insert_validated_upcoming.py'))

    t1 >> t2 >> t3
