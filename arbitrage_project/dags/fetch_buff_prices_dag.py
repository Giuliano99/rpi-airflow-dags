import sys, os
sys.path.append(os.path.expanduser("~/airflow/dags"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from arbitrage_project.scripts.api.buff_api import fetch_buff_prices

with DAG(
    dag_id="fetch_buff_prices",
    start_date=datetime(2025,10,20),
    schedule_interval="0 */8 * * *",
    catchup=False,
    tags=["arbitrage"]
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_buff",
        python_callable=fetch_buff_prices
    )
