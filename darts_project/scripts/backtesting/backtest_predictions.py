import psycopg2
import pandas as pd
import math
from datetime import datetime

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def backtest_predictions():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Fetch predictions and actual results
    query = """
    SELECT
        p.id AS prediction_id,
        p.matchdate,
        p.player1,
        p.player2,
        p.player1_elo,
        p.player2_elo,
        p.player1_match_count,
        p.player2_match_count,
        p.predicted_p1_prob,
        p.predicted_p2_prob,
        p.best_p1_odds,
        p.best_p2_odds,
        r.winner AS actual_winner
    FROM darts_match_predictions p
    JOIN dart_matches_gold r
      ON p.player1 = r.player1 AND p.player2 = r.player2 AND p.matchdate = r.matchdate
    WHERE p.matchdate < now();
    """

    df = pd.read_sql(query, conn)

    results = []

    for _, row in df.iterrows():
        # Implied probabilities
        best_p1_implied = 1 / row['best_p1_odds'] if row['best_p1_odds'] else None
        best_p2_implied = 1 / row['best_p2_odds'] if row['best_p2_odds'] else None

        # Log loss
        actual = row['actual_winner']
        log_loss = None
        if actual == row['player1'] and pd.notnull(row['predicted_p1_prob']):
            log_loss = -math.log(row['predicted_p1_prob']) if row['predicted_p1_prob'] > 0 else None
        elif actual == row['player2'] and pd.notnull(row['predicted_p2_prob']):
            log_loss = -math.log(row['predicted_p2_prob']) if row['predicted_p2_prob'] > 0 else None

        results.append((
            int(row['prediction_id']),
            row['matchdate'],
            row['player1'],
            row['player2'],
            actual,
            float(row['player1_elo']) if pd.notnull(row['player1_elo']) else None,
            float(row['player2_elo']) if pd.notnull(row['player2_elo']) else None,
            int(row['player1_match_count']),
            int(row['player2_match_count']),
            float(row['predicted_p1_prob']) if pd.notnull(row['predicted_p1_prob']) else None,
            float(row['predicted_p2_prob']) if pd.notnull(row['predicted_p2_prob']) else None,
            float(row['best_p1_odds']) if pd.notnull(row['best_p1_odds']) else None,
            float(row['best_p2_odds']) if pd.notnull(row['best_p2_odds']) else None,
            best_p1_implied,
            best_p2_implied,
            float(log_loss) if log_loss is not None else None
        ))

    # Create table with UNIQUE constraint on prediction_id
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS darts_backtest_general_log (
        id SERIAL PRIMARY KEY,
        prediction_id INT UNIQUE,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        actual_winner VARCHAR(100),
        player1_elo FLOAT,
        player2_elo FLOAT,
        player1_match_count INT,
        player2_match_count INT,
        predicted_p1_prob FLOAT,
        predicted_p2_prob FLOAT,
        best_p1_odds FLOAT,
        best_p2_odds FLOAT,
        best_p1_implied_prob FLOAT,
        best_p2_implied_prob FLOAT,
        log_loss FLOAT,
        run_timestamp TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()

    # Insert with ON CONFLICT DO NOTHING to prevent duplicates
    insert_query = """
    INSERT INTO darts_backtest_general_log (
        prediction_id, matchdate, player1, player2,
        actual_winner, player1_elo, player2_elo,
        player1_match_count, player2_match_count,
        predicted_p1_prob, predicted_p2_prob,
        best_p1_odds, best_p2_odds,
        best_p1_implied_prob, best_p2_implied_prob,
        log_loss
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (prediction_id) DO NOTHING;
    """

    cursor.executemany(insert_query, results)
    conn.commit()

    print(f"[✅] General backtest data stored: {len(results)} matches attempted (duplicates skipped).")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    backtest_predictions()