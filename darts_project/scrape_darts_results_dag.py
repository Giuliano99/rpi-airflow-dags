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
    'scrape_darts_results_dag',
    default_args=default_args,
    description='Daily execution of scrape_darts.py',
    schedule_interval='00 21 * * *',  # Executes daily at 21:00
    start_date=datetime(2025, 1, 1, tzinfo=german_tz),
    catchup=False,  # Ensures it doesn't backfill past runs
    tags=['darts'],
) as dag:

    # Define a function to run your script
    def run_scrape_darts_results_script():
        script_path = os.path.join(os.path.dirname(__file__), 'scrape_darts_results.py')
        try:
            result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing script: {e.stderr}")

    # Define the Python task
    run_scrape_darts_results_task = PythonOperator(
        task_id='run_scrape_darts_results_task',
        python_callable=run_scrape_darts_results_script,
    )

    # Define task dependencies if needed
    run_scrape_darts_results_task
