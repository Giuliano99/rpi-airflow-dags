import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ✅ Import your backtest script
from scripts.backtesting.backtest_predictions import backtest_predictions

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 19),
    "catchup": False,
}

with DAG(
    "backtest_predictions_dag",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    description="Run backtesting on past darts predictions to log performance and ROI",
) as dag:
    backtest_task = PythonOperator(
        task_id="run_backtest_predictions",
        python_callable=backtest_predictions,
    )

    backtest_task
