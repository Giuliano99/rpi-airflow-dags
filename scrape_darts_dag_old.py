from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
from pendulum import timezone

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Set the timezone to German time
german_tz = timezone("Europe/Berlin")


# Define the DAG
with DAG(
    'darts_scraper',
    default_args=default_args,
    description='Daily execution of scrape_darts.py',
    schedule_interval='00 21 * * *',  # Executes daily at 21:00
    start_date=datetime(2025, 1, 1, tzinfo=german_tz),  # Start date with timezone
    catchup=False,  # Ensures it doesn't backfill past runs
    tags=['example'],
) as dag:
    
    # Define a function to run your script
    def run_scraper():
        script_path = os.path.join(os.path.dirname(__file__), 'scrape_darts.py')
        subprocess.run(['python3', script_path], check=True)

    # Define the Python task
    run_scrape_task = PythonOperator(
        task_id='run_scraper_task',
        python_callable=run_scraper,
    )

    # Define task dependencies (if you have more tasks, set them here)
    run_scrape_task

