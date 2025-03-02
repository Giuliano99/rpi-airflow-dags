from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os
from datetime import datetime


# PostgreSQL Connection Details
DB_CONFIG = {
    "host": "localhost",  # Or the IP of your Raspberry Pi if accessing remotely
    "port": "5432",
    "database": "darts_project",
    "user": "postgress",
    "password": "5ads15"  # Replace with your actual password
}

# Path to CSV files
CSV_FOLDER = "/home/pi/darts/dart_matches"

# Function to load CSVs into PostgreSQL
def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Loop through all CSV files
    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv"):
            file_path = os.path.join(CSV_FOLDER, filename)
            print(f"Loading {file_path} into database")

            # Read CSV file
            df = pd.read_csv(file_path)

            # Create table if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS dart_matches (
                match_id SERIAL PRIMARY KEY,
                player1 VARCHAR(100),
                player2 VARCHAR(100),
                score1 INT,
                score2 INT,
                match_date DATE
            );
            """
            cursor.execute(create_table_query)

            # Insert data into the database
            for _, row in df.iterrows():
                insert_query = """
                INSERT INTO dart_matches (player1, player2, score1, score2, match_date)
                VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (row['player1'], row['player2'], row['score1'], row['score2'], row['match_date']))

            conn.commit()
            print(f"✅ {filename} loaded successfully")

    cursor.close()
    conn.close()

# Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False
}

with DAG("load_darts_results",
         default_args=default_args,
         schedule_interval="@daily",  # Runs every day
         ) as dag:

    task_load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    task_load_csv