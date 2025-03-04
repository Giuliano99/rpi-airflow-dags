from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os
from datetime import datetime
import numpy as np

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

# Function to load CSVs into PostgreSQL
def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv"):
            file_path = os.path.join(CSV_FOLDER, filename)
            print(f"Loading {file_path} into database")

            # Check if the file is empty
            if os.path.getsize(file_path) == 0:
                print(f"Skipping empty file: {file_path}")
                return  # Exit the function

            # Read CSV file
            df = pd.read_csv(file_path)

            # Skip if DataFrame is empty
            if df.empty:
                print(f"⚠️ Skipping empty CSV: {filename}")
                continue

            # Ensure required columns exist
            expected_columns = {'Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'}
            if not expected_columns.issubset(df.columns):
                print(f"⚠️ Skipping file with missing columns: {filename}")
                continue

            # Set default date if missing
            df['Date'] = df['Date'].fillna("1970-01-01")  # Default date for missing values

            # Ensure table exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS dart_matches (
                match_id SERIAL PRIMARY KEY,
                matchdate DATE, 
                player1 VARCHAR(100),
                player2 VARCHAR(100),
                player1score INT,
                player2score INT,
                winner VARCHAR(100),
                UNIQUE (player1, player2, matchdate)  -- Prevent duplicates
            );
            """
            cursor.execute(create_table_query)
            conn.commit()

            # Replace NaN values in the DataFrame
            df['Player 1'] = df['Player 1'].fillna("Unknown").astype(str)
            df['Player 2'] = df['Player 2'].fillna("Unknown").astype(str)
            df['Date'] = df['Date'].fillna("1970-01-01").astype(str)
            
            for _, row in df.iterrows():
                try:
                    # Convert numeric columns and handle NaN cases
                    p1_score = int(row['Player 1 Score']) if not pd.isna(row['Player 1 Score']) else 0
                    p2_score = int(row['Player 2 Score']) if not pd.isna(row['Player 2 Score']) else 0
                    matchdate = str(row['Date'])
            
                    player1 = str(row['Player 1']).strip()  # Ensure it's a string
                    player2 = str(row['Player 2']).strip()
            
                    # Check if the record already exists
                    check_query = """
                    SELECT 1 FROM dart_matches 
                    WHERE player1 = %s AND player2 = %s AND matchdate = %s;
                    """
                    cursor.execute(check_query, (player1, player2, matchdate))
            
                    if cursor.fetchone() is None:  # No existing record, insert new one
                        insert_query = """
                        INSERT INTO dart_matches (matchdate, player1, player2, player1score, player2score, winner)
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """
                        cursor.execute(insert_query, (matchdate, player1, player2, p1_score, p2_score, row['Winner']))
                    else:
                        print(f"⚠️ Skipping duplicate match: {player1} vs {player2} on {matchdate}")
            
                except ValueError:
                    print(f"⚠️ Skipping row with invalid numeric value: {row}")
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