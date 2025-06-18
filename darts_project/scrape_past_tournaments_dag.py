from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "catchup": False,
}

with DAG(
    dag_id="scrape_past_tournaments_dag",
    default_args=default_args,
    schedule_interval=None,   #Manual only
    description="Manually run the FlashScore past tournaments scraper",
) as dag:

    run_scraper = BashOperator(
        task_id="run_scraper_script",
        bash_command="python3 ~/airflow/dags/darts_project/scrape_past_tournaments.py"
    )

    run_scraper
