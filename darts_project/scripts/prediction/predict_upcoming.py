import psycopg2
import pandas as pd
import json
from datetime import datetime

DB_CONFIG = {
    "host": "172.17.0.2",
    "port": "5432",
    "database": "darts_project",
    "user": "postgres",
    "password": "5ads15"
}

def calculate_win_probability(player1_elo, player2_elo):
    """Returns expected win probability for player1 based on Elo."""
    try:
        expected_p1 = 1 / (1 + 10 ** ((player2_elo - player1_elo) / 400))
        return expected_p1
    except:
        return None

def predict_upcoming():
    print(f"[ℹ️] Starting upcoming matches prediction... {datetime.now()}")

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch upcoming matches with latest elo before matchdate
        query = """
        SELECT
            u.id,
            u.matchdate,
            u.player1,
            u.player2,
            u.odds,
            (SELECT player1_elo_after
             FROM elo_match_log
             WHERE (player1 = u.player1 OR player2 = u.player1)
               AND match_date < u.matchdate
             ORDER BY match_date DESC
             LIMIT 1) AS player1_elo,
            (SELECT player1_elo_after
             FROM elo_match_log
             WHERE (player1 = u.player2 OR player2 = u.player2)
               AND match_date < u.matchdate
             ORDER BY match_date DESC
             LIMIT 1) AS player2_elo
        FROM upcoming_matches_gold u;
        """

        df = pd.read_sql(query, conn)

        if df.empty:
            print("[⚠️] No upcoming matches found.")
            return

        # Calculate probabilities and value bets
        df['predicted_p1_prob'] = df.apply(lambda x: calculate_win_probability(x['player1_elo'], x['player2_elo']), axis=1)
        df['predicted_p2_prob'] = 1 - df['predicted_p1_prob']

        records = []
        for idx, row in df.iterrows():
            odds = json.loads(row['odds']) if row['odds'] else {}
            # Example: calculate implied prob and value bet for bwin_P1 if available
            bwin_p1_odds = float(odds.get('bwin_P1')) if 'bwin_P1' in odds else None
            if bwin_p1_odds:
                implied_prob = 1 / bwin_p1_odds
                value = row['predicted_p1_prob'] - implied_prob
            else:
                implied_prob = None
                value = None

            records.append((
                row['id'],
                row['matchdate'],
                row['player1'],
                row['player2'],
                row['player1_elo'],
                row['player2_elo'],
                row['predicted_p1_prob'],
                row['predicted_p2_prob'],
                json.dumps(odds),
                implied_prob,
                value
            ))

        # Create prediction table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS darts_match_predictions (
            id SERIAL PRIMARY KEY,
            match_id INT,
            matchdate DATE,
            player1 VARCHAR(100),
            player2 VARCHAR(100),
            player1_elo INT,
            player2_elo INT,
            predicted_p1_prob FLOAT,
            predicted_p2_prob FLOAT,
            odds JSONB,
            implied_prob_bwin_p1 FLOAT,
            value_bwin_p1 FLOAT,
            created_at TIMESTAMP DEFAULT now()
        );
        """)
        conn.commit()

        # Insert predictions
        insert_query = """
        INSERT INTO darts_match_predictions (
            match_id, matchdate, player1, player2,
            player1_elo, player2_elo,
            predicted_p1_prob, predicted_p2_prob,
            odds, implied_prob_bwin_p1, value_bwin_p1
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """

        cursor.executemany(insert_query, records)
        conn.commit()

        print(f"[✅] Predictions inserted: {len(records)}")

    except Exception as e:
        print(f"[❌] Error in predict_upcoming: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("[ℹ️] Database connection closed.")

if __name__ == "__main__":
    predict_upcoming()
