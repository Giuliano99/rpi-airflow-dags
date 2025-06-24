from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.elo.elo_update import add_new_players, calculate_elo


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 24),
    "catchup": False,
}

with DAG("update_elo_ratings_dag", default_args=default_args, schedule_interval="@daily") as dag:
    task_add_new_players = PythonOperator(
        task_id="add_new_players",
        python_callable=add_new_players
    )

    task_calculate_elo = PythonOperator(
        task_id="calculate_elo",
        python_callable=calculate_elo
    )

    task_add_new_players >> task_calculate_elo