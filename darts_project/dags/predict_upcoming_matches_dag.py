import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.prediction.predict_upcoming import predict_upcoming

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 19),
    "catchup": False,
}

with DAG("predict_upcoming_matches_dag", default_args=default_args, schedule_interval=None) as dag:
    t1 = PythonOperator(
        task_id="predict_upcoming_matches",
        python_callable=predict_upcoming
    )