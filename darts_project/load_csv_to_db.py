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
CSV_FOLDER = "/home/pi/airflow/darts_results" #"/home/pi/darts/dart_matches"

# Function to load CSVs into PostgreSQL
def load_csv_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Ensure `new_matches_log` table exists
    create_log_table_query = """
    CREATE TABLE IF NOT EXISTS new_matches_log (
        match_id INT PRIMARY KEY REFERENCES dart_matches(match_id) ON DELETE CASCADE,
        processed BOOLEAN DEFAULT FALSE
    );
    """
    cursor.execute(create_log_table_query)
    conn.commit()

    for filename in os.listdir(CSV_FOLDER):
        if filename.endswith(".csv"):
            file_path = os.path.join(CSV_FOLDER, filename)
            print(f"Loading {file_path} into database")

            if os.path.getsize(file_path) == 0:
                print(f"Skipping empty file: {file_path}")
                return  # Exit function

            df = pd.read_csv(file_path)

            if df.empty:
                print(f"⚠️ Skipping empty CSV: {filename}")
                continue

            expected_columns = {'Date', 'Player 1', 'Player 2', 'Player 1 Score', 'Player 2 Score', 'Winner'}
            if not expected_columns.issubset(df.columns):
                print(f"⚠️ Skipping file with missing columns: {filename}")
                continue

            df['Date'] = df['Date'].fillna("1970-01-01")  

            create_table_query = """
            CREATE TABLE IF NOT EXISTS dart_matches (
                match_id SERIAL PRIMARY KEY,
                matchdate DATE, 
                player1 VARCHAR(100),
                player2 VARCHAR(100),
                player1score INT,
                player2score INT,
                winner VARCHAR(100),
                UNIQUE (player1, player2, matchdate)
            );
            """
            cursor.execute(create_table_query)
            conn.commit()

            df['Player 1'] = df['Player 1'].fillna("Unknown").astype(str)
            df['Player 2'] = df['Player 2'].fillna("Unknown").astype(str)
            df['Date'] = df['Date'].fillna("1970-01-01").astype(str)
            
            for _, row in df.iterrows():
                try:
                    p1_score = int(row['Player 1 Score']) if not pd.isna(row['Player 1 Score']) else 0
                    p2_score = int(row['Player 2 Score']) if not pd.isna(row['Player 2 Score']) else 0
                    matchdate = str(row['Date'])
            
                    player1 = str(row['Player 1']).strip()
                    player2 = str(row['Player 2']).strip()
            
                    check_query = """
                    SELECT match_id FROM dart_matches 
                    WHERE player1 = %s AND player2 = %s AND matchdate = %s;
                    """
                    cursor.execute(check_query, (player1, player2, matchdate))
                    existing_match = cursor.fetchone()
            
                    if existing_match is None:
                        insert_query = """
                        INSERT INTO dart_matches (matchdate, player1, player2, player1score, player2score, winner)
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING match_id;
                        """
                        cursor.execute(insert_query, (matchdate, player1, player2, p1_score, p2_score, row['Winner']))
                    
                except ValueError:
                    print(f"⚠️ Skipping row with invalid numeric value: {row}")
                    continue

    # ✅ **Add new matches to `new_matches_log` here (BEFORE closing cursor & connection)**
    log_new_matches_query = """
    INSERT INTO new_matches_log (match_id, processed)
    SELECT match_id, FALSE FROM dart_matches
    ON CONFLICT (match_id) DO NOTHING;
    """
    cursor.execute(log_new_matches_query)
    conn.commit()

    print("✅ New matches added to `new_matches_log` successfully.")

    # ✅ **Now close the cursor & connection**
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
         schedule_interval='10 21 * * *',  # Runs every day at 21:10
         ) as dag:

    task_load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    task_load_csv