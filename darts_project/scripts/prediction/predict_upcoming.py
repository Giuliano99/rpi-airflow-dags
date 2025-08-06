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

INITIAL_ELO = 1500

def calculate_win_probability(player1_elo, player2_elo):
    try:
        return 1 / (1 + 10 ** ((player2_elo - player1_elo) / 400))
    except Exception as e:
        print(f"[ERROR] calculate_win_probability: {e}")
        return None

def predict_upcoming():
    print(f"[ℹ️] Starting upcoming matches prediction... {datetime.now()}")

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch upcoming matches
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
            (SELECT COUNT(*)
             FROM elo_match_log
             WHERE (player1 = u.player1 OR player2 = u.player1)
               AND match_date < u.matchdate) AS player1_match_count,
            (SELECT player1_elo_after
             FROM elo_match_log
             WHERE (player1 = u.player2 OR player2 = u.player2)
               AND match_date < u.matchdate
             ORDER BY match_date DESC
             LIMIT 1) AS player2_elo,
            (SELECT COUNT(*)
             FROM elo_match_log
             WHERE (player1 = u.player2 OR player2 = u.player2)
               AND match_date < u.matchdate) AS player2_match_count
        FROM upcoming_matches_gold u;
        """

        df = pd.read_sql(query, conn)

        if df.empty:
            print("[⚠️] No upcoming matches found.")
            return

        df['player1_elo'] = df['player1_elo'].fillna(INITIAL_ELO)
        df['player2_elo'] = df['player2_elo'].fillna(INITIAL_ELO)

        df['predicted_p1_prob'] = df.apply(lambda x: calculate_win_probability(x['player1_elo'], x['player2_elo']), axis=1)
        df['predicted_p2_prob'] = 1 - df['predicted_p1_prob']

        records = []
        for _, row in df.iterrows():
            odds_json = row['odds'] if isinstance(row['odds'], dict) else json.loads(row['odds']) if row['odds'] else {}

            p1_odds = [float(v) for k, v in odds_json.items() if '_P1' in k]
            p2_odds = [float(v) for k, v in odds_json.items() if '_P2' in k]

            best_p1_odds = max(p1_odds) if p1_odds else None
            best_p2_odds = max(p2_odds) if p2_odds else None

            best_p1_implied_prob = 1 / best_p1_odds if best_p1_odds else None
            best_p2_implied_prob = 1 / best_p2_odds if best_p2_odds else None

            value_p1 = row['predicted_p1_prob'] - best_p1_implied_prob if best_p1_implied_prob else None
            value_p2 = row['predicted_p2_prob'] - best_p2_implied_prob if best_p2_implied_prob else None

            records.append((
                row['id'],
                row['matchdate'],
                row['player1'],
                row['player2'],
                row['player1_elo'],
                row['player2_elo'],
                row['player1_match_count'],
                row['player2_match_count'],
                row['predicted_p1_prob'],
                row['predicted_p2_prob'],
                json.dumps(odds_json),
                best_p1_odds,
                best_p1_implied_prob,
                value_p1,
                best_p2_odds,
                best_p2_implied_prob,
                value_p2
            ))

        # Create predictions table with UNIQUE constraint on match_id
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS darts_match_predictions (
            id SERIAL PRIMARY KEY,
            match_id INT UNIQUE,  -- <-- Prevent duplicates on match
            matchdate DATE,
            player1 VARCHAR(100),
            player2 VARCHAR(100),
            player1_elo FLOAT,
            player2_elo FLOAT,
            player1_match_count INT,
            player2_match_count INT,
            predicted_p1_prob FLOAT,
            predicted_p2_prob FLOAT,
            odds JSONB,
            best_p1_odds FLOAT,
            best_p1_implied_prob FLOAT,
            value_p1 FLOAT,
            best_p2_odds FLOAT,
            best_p2_implied_prob FLOAT,
            value_p2 FLOAT,
            created_at TIMESTAMP DEFAULT now()
        );
        """)
        conn.commit()

        # Insert with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO darts_match_predictions (
            match_id, matchdate, player1, player2,
            player1_elo, player2_elo,
            player1_match_count, player2_match_count,
            predicted_p1_prob, predicted_p2_prob,
            odds,
            best_p1_odds, best_p1_implied_prob, value_p1,
            best_p2_odds, best_p2_implied_prob, value_p2
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (match_id) DO NOTHING;
        """

        cursor.executemany(insert_query, records)
        conn.commit()

        print(f"[✅] Predictions inserted: {len(records)} attempted (duplicates skipped).")

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