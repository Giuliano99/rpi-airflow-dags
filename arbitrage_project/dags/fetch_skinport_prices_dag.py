from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from arbitrage_project.scripts.api.skinport_api import fetch_skinport_prices

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_skinport_prices",
    default_args=default_args,
    description="Fetch Skinport prices and store them in Postgres",
    schedule_interval="0 */8 * * *",
    start_date=datetime(2025, 10, 20),
    catchup=False,
    tags=["arbitrage"],
) as dag:

    fetch_skinport = PythonOperator(
        task_id="fetch_skinport",
        python_callable=fetch_skinport_prices
    )

    fetch_skinport
