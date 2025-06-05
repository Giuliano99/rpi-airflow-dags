from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os
import subprocess

# Default arguments for both DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Set timezone to Germany
german_tz = timezone("Europe/Berlin")

# ─────────────────────────────────────────────
# DAG 1: Scrape Darts Results
# ─────────────────────────────────────────────
with DAG(
    'scrape_darts_results_dag',
    default_args=default_args,
    description='Daily scrape of completed darts match results',
    schedule_interval='00 21 * * *',  # Every day at 21:00
    start_date=datetime(2025, 1, 1, tzinfo=german_tz),
    catchup=False,
    tags=['darts', 'results'],
) as dag1:

    def run_scrape_darts_results_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scrape_darts_results.py')
        try:
            result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing results script: {e.stderr}")

    scrape_results_task = PythonOperator(
        task_id='scrape_darts_results_task',
        python_callable=run_scrape_darts_results_script,
    )

# ─────────────────────────────────────────────
# DAG 2: Scrape Upcoming Darts Matches
# ─────────────────────────────────────────────
with DAG(
    'scrape_darts_upcoming_dag',
    default_args=default_args,
    description='Daily scrape of upcoming darts matches and odds',
    schedule_interval='00 21 * * *',  # Same schedule, but can be changed independently
    start_date=datetime(2025, 1, 1, tzinfo=german_tz),
    catchup=False,
    tags=['darts', 'upcoming'],
) as dag2:

    def run_scrape_darts_upcoming_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scrape_darts_upcoming.py')
        try:
            result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing upcoming script: {e.stderr}")

    scrape_upcoming_task = PythonOperator(
        task_id='scrape_darts_upcoming_task',
        python_callable=run_scrape_darts_upcoming_script,
    )
