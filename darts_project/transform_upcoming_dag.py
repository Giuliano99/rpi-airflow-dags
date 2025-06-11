from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from transform_upcoming import transform_upcoming

with DAG(
    dag_id="transform_upcoming",
    start_date=datetime(2025, 2, 2),
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id="calculate_best_odds_and_probs",
        python_callable=transform_upcoming
    )

    transform_task
