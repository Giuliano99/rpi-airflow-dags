from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os
import subprocess

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Timezone
german_tz = timezone("Europe/Berlin")

# ─────────────────────────────────────────────
# DAG 2: Scrape All Darts Tournaments
# ─────────────────────────────────────────────
with DAG(
    'scrape_all_darts_results_dag',
    default_args=default_args,
    description='Daily scrape of all darts tournaments and results',
    schedule_interval='30 23 * * *',  # Every day at 23:30
    start_date=datetime(2025, 1, 1, tzinfo=german_tz),
    catchup=False,
    tags=['darts', 'tournaments'],
) as dag2:

    def run_scrape_all_darts_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scrape_all_darts.py')
        try:
            result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing all-tournaments script: {e.stderr}")

    scrape_all_task = PythonOperator(
        task_id='scrape_past_tournaments',
        python_callable=run_scrape_all_darts_script,
    )
