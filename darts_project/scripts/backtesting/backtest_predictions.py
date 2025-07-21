import psycopg2
import pandas as pd
from datetime import datetime

DB_CONFIG = {...}

def backtest_predictions(value_threshold=0.05, stake=10):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
    SELECT
        p.id AS prediction_id,
        p.matchdate,
        p.player1,
        p.player2,
        p.predicted_p1_prob,
        p.predicted_p2_prob,
        p.best_p1_odds,
        p.best_p2_odds,
        p.value_p1,
        p.value_p2,
        r.winner AS actual_winner
    FROM darts_match_predictions p
    JOIN dart_matches_gold r
      ON p.player1 = r.player1 AND p.player2 = r.player2 AND p.matchdate = r.matchdate
    WHERE p.matchdate < now();
    """

    df = pd.read_sql(query, conn)

    results = []
    total_profit = 0
    correct = 0
    total_bets = 0

    for _, row in df.iterrows():
        bet_p1 = row['value_p1'] and row['value_p1'] >= value_threshold
        bet_p2 = row['value_p2'] and row['value_p2'] >= value_threshold

        # Determine bets placed and outcomes
        profit = 0
        bets = 0

        if bet_p1:
            bets += 1
            if row['actual_winner'] == row['player1']:
                profit += stake * (row['best_p1_odds'] - 1)
                correct += 1
            else:
                profit -= stake

        if bet_p2:
            bets += 1
            if row['actual_winner'] == row['player2']:
                profit += stake * (row['best_p2_odds'] - 1)
                correct += 1
            else:
                profit -= stake

        total_bets += bets
        total_profit += profit

        results.append({
            "prediction_id": row['prediction_id'],
            "matchdate": row['matchdate'],
            "player1": row['player1'],
            "player2": row['player2'],
            "actual_winner": row['actual_winner'],
            "bet_p1": bet_p1,
            "bet_p2": bet_p2,
            "profit": profit
        })

    # Insert results to darts_backtest_log table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS darts_backtest_log (
        id SERIAL PRIMARY KEY,
        prediction_id INT,
        matchdate DATE,
        player1 VARCHAR(100),
        player2 VARCHAR(100),
        actual_winner VARCHAR(100),
        bet_p1 BOOLEAN,
        bet_p2 BOOLEAN,
        profit FLOAT,
        run_timestamp TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()

    insert_query = """
    INSERT INTO darts_backtest_log (
        prediction_id, matchdate, player1, player2,
        actual_winner, bet_p1, bet_p2, profit
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
    """

    cursor.executemany(insert_query, [
        (
            r["prediction_id"],
            r["matchdate"],
            r["player1"],
            r["player2"],
            r["actual_winner"],
            r["bet_p1"],
            r["bet_p2"],
            r["profit"]
        )
        for r in results
    ])
    conn.commit()

    print(f"[✅] Backtest complete. Total bets: {total_bets}, Correct: {correct}, Total profit: {total_profit:.2f}, Accuracy: {correct/total_bets if total_bets>0 else 0:.2%}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    backtest_predictions()
