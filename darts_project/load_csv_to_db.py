from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os
from datetime import datetime

# PostgreSQL Connection Details
DB_CONFIG = {
    "host": "172.17.0.2",  # Or the IP of your Raspberry Pi if accessing remotely
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"  # Replace with your actual password
}

# Path to CSV files
CSV_FOLDER = "/home/pi/darts/dart_matches"

def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ✅ Ensure the `matchdate` column exists
    cursor.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name = 'dart_matches' AND column_name = 'matchdate') 
            THEN
                ALTER TABLE dart_matches ADD COLUMN matchdate DATE DEFAULT '1900-01-01';
            END IF;
        END $$;
    """)
    conn.commit()

    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv"):
            file_path = os.path.join(CSV_FOLDER, filename)
            print(f"Loading {file_path} into database")

            if os.path.getsize(file_path) == 0:
                print(f"Skipping empty file: {file_path}")
                continue  

            # ✅ Read CSV (including "Date" column)
            df = pd.read_csv(file_path, usecols=['Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'])

            # ✅ Replace missing or invalid dates with default '1900-01-01'
            df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y', errors='coerce')
            df['Date'].fillna(pd.Timestamp('1900-01-01'), inplace=True)  # Default date for missing values

            # Ensure no missing values remain
            df['Date'] = df['Date'].dt.date  

            if df.empty:
                print(f"⚠️ Skipping empty CSV: {filename}")
                continue

            expected_columns = {'Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'}
            if not expected_columns.issubset(df.columns):
                print(f"⚠️ Skipping file with missing columns: {filename}")
                continue

            MAX_INT = 2147483647  

            for _, row in df.iterrows():
                try:
                    p1_score = int(row['Player 1 Score'])
                    p2_score = int(row['Player 2 Score'])

                    if abs(p1_score) > MAX_INT or abs(p2_score) > MAX_INT:
                        print(f"⚠️ Skipping row with out-of-range values: {row}")
                        continue

                    # ✅ Insert with a guaranteed valid date
                    insert_query = """
                    INSERT INTO dart_matches (matchdate, player1, player2, player1score, player2score, winner)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(insert_query, (row['Date'], row['Player 1'], row['Player 2'], p1_score, p2_score, row['Winner']))

                except ValueError as e:
                    print(f"⚠️ Skipping row due to error {e}: {row}")
                    continue

            conn.commit()
            print(f"✅ {filename} loaded successfully")

    cursor.close()
    conn.close()

# Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 2),
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
