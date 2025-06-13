import psycopg2
import pandas as pd
import json

def transform_upcoming_odds():
    conn = psycopg2.connect(
        host="172.17.0.2",
        port="5432",
        database="darts_project",
        user="postgres",
        password="5ads15"
    )
    cursor = conn.cursor()

    df = pd.read_sql("SELECT id, odds FROM upcoming_matches", conn)

    for _, row in df.iterrows():
        match_id = row["id"]
        odds_data = row["odds"]

        if not odds_data or not isinstance(odds_data, dict):
            continue

        try:
            p1_odds = []
            p2_odds = []

            for key, val in odds_data.items():
                try:
                    if "_P1" in key and val is not None:
                        p1_odds.append(float(val))
                    elif "_P2" in key and val is not None:
                        p2_odds.append(float(val))
                except ValueError:
                    continue  # Skip non-floatable odds

            if not p1_odds or not p2_odds:
                continue

            best_p1 = max(p1_odds)
            best_p2 = max(p2_odds)

            win_prob_p1 = 1 / best_p1 if best_p1 else None
            win_prob_p2 = 1 / best_p2 if best_p2 else None

            cursor.execute("""
                UPDATE upcoming_matches
                SET best_odd_player1 = %s,
                    win_prob_player1 = %s,
                    best_odd_player2 = %s,
                    win_prob_player2 = %s
                WHERE id = %s
            """, (best_p1, win_prob_p1, best_p2, win_prob_p2, match_id))

        except Exception as e:
            print(f"Failed on row ID {match_id}: {e}")
            continue


    conn.commit()
    cursor.close()
    conn.close()
