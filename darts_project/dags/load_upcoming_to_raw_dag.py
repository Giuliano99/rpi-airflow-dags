import sys
import os

# Import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Direct imports instead of subprocess
from scripts.loading.load_upcoming_staging import load_raw_upcoming
from scripts.validation.validate_upcoming_staging import validate_upcoming
from scripts.loading.insert_validated_upcoming import insert_validated_upcoming

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 20),
    "catchup": False,
}

with DAG("load_upcoming_to_raw_dag", default_args=default_args, schedule_interval=None) as dag:
    t1 = PythonOperator(task_id="load_raw_upcoming_csvs", python_callable=load_raw_upcoming)
    t2 = PythonOperator(task_id="validate_upcoming_staging", python_callable=validate_upcoming)
    t3 = PythonOperator(task_id="insert_validated_upcoming", python_callable=insert_validated_upcoming)

    t1 >> t2 >> t3
