import psycopg2
import pandas as pd
import json
import math

def transform_upcoming_odds_and_update_elo():
    conn = psycopg2.connect(
        host="172.17.0.2",
        port="5432",
        database="darts_project",
        user="postgres",
        password="5ads15"
    )
    cursor = conn.cursor()

    # Fetch Elo ratings
    cursor.execute("SELECT player, elo FROM elo_rankings")
    elo_dict = {row[0]: row[1] for row in cursor.fetchall()}

    # Read match data
    df = pd.read_sql("SELECT id, player1, player2, odds FROM upcoming_matches", conn)

    for _, row in df.iterrows():
        match_id = row["id"]
        player1 = row["player1"]
        player2 = row["player2"]
        odds_data = row["odds"]

        if not odds_data or not isinstance(odds_data, dict):
            continue

        try:
            # --- Bookmaker odds ---
            p1_odds = []
            p2_odds = []

            for key, val in odds_data.items():
                try:
                    if "_P1" in key and val is not None:
                        p1_odds.append(float(val))
                    elif "_P2" in key and val is not None:
                        p2_odds.append(float(val))
                except ValueError:
                    continue

            best_p1 = max(p1_odds) if p1_odds else None
            best_p2 = max(p2_odds) if p2_odds else None

            win_prob_p1 = 1 / best_p1 if best_p1 else None
            win_prob_p2 = 1 / best_p2 if best_p2 else None

            implied_margin = None
            if win_prob_p1 is not None and win_prob_p2 is not None:
                implied_margin = (win_prob_p1 + win_prob_p2) - 1

            # --- Elo-based probabilities ---
            elo1 = elo_dict.get(player1, 1500)
            elo2 = elo_dict.get(player2, 1500)
            expected1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))
            expected2 = 1 - expected1

            # --- Update DB ---
            cursor.execute("""
                UPDATE upcoming_matches
                SET best_odd_player1 = %s,
                    win_prob_player1 = %s,
                    best_odd_player2 = %s,
                    win_prob_player2 = %s,
                    implied_margin = %s,
                    elo_prob_player1 = %s,
                    elo_prob_player2 = %s
                WHERE id = %s
            """, (
                best_p1, win_prob_p1, best_p2, win_prob_p2, implied_margin,
                expected1, expected2, match_id
            ))

        except Exception as e:
            print(f"Failed on row ID {match_id}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print("Updated upcoming_matches with odds and Elo-based probabilities.")
