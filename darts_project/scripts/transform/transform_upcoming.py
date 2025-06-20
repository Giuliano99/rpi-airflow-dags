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

    df = pd.read_sql("SELECT * FROM upcoming_matches", conn)

    for _, row in df.iterrows():
        match_id = row["id"]
        player1 = row["player1"]
        player2 = row["player2"]
        odds_data_raw = row["odds"]
        winner = row.get("winner")

        try:
            odds_data = json.loads(odds_data_raw) if isinstance(odds_data_raw, str) else odds_data_raw
        except Exception:
            continue

        if not odds_data or not isinstance(odds_data, dict):
            continue

        try:
            # Get best odds
            p1_odds = [float(val) for key, val in odds_data.items() if "_P1" in key and val]
            p2_odds = [float(val) for key, val in odds_data.items() if "_P2" in key and val]

            best_p1 = max(p1_odds) if p1_odds else None
            best_p2 = max(p2_odds) if p2_odds else None

            win_prob_p1 = 1 / best_p1 if best_p1 else None
            win_prob_p2 = 1 / best_p2 if best_p2 else None

            norm_prob_p1 = None
            norm_prob_p2 = None
            implied_margin = None

            if win_prob_p1 and win_prob_p2:
                total = win_prob_p1 + win_prob_p2
                norm_prob_p1 = win_prob_p1 / total
                norm_prob_p2 = win_prob_p2 / total
                implied_margin = total - 1

            # Compute Elo expected probabilities if not already set
            cursor.execute("SELECT elo_prob_player1 FROM upcoming_matches WHERE id = %s", (match_id,))
            elo_prob_set = cursor.fetchone()[0]

            if elo_prob_set is None:
                elo1 = elo_dict.get(player1, 1500)
                elo2 = elo_dict.get(player2, 1500)
                expected1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))
                expected2 = 1 - expected1

                cursor.execute("""
                    UPDATE upcoming_matches
                    SET best_odd_player1 = %s,
                        win_prob_player1 = %s,
                        best_odd_player2 = %s,
                        win_prob_player2 = %s,
                        implied_margin = %s,
                        norm_prob_player1 = %s,
                        norm_prob_player2 = %s,
                        elo_prob_player1 = %s,
                        elo_prob_player2 = %s
                    WHERE id = %s
                """, (
                    best_p1, win_prob_p1, best_p2, win_prob_p2, implied_margin,
                    norm_prob_p1, norm_prob_p2, expected1, expected2, match_id
                ))
            else:
                cursor.execute("""
                    UPDATE upcoming_matches
                    SET best_odd_player1 = %s,
                        win_prob_player1 = %s,
                        best_odd_player2 = %s,
                        win_prob_player2 = %s,
                        implied_margin = %s,
                        norm_prob_player1 = %s,
                        norm_prob_player2 = %s
                    WHERE id = %s
                """, (
                    best_p1, win_prob_p1, best_p2, win_prob_p2, implied_margin,
                    norm_prob_p1, norm_prob_p2, match_id
                ))

            # === Calculate Brier and Log Loss if match result is known ===
            if winner in [player1, player2] and norm_prob_p1 is not None:
                actual = 1 if winner == player1 else 0

                # Elo probability
                elo1 = elo_dict.get(player1, 1500)
                elo2 = elo_dict.get(player2, 1500)
                elo_prob_p1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))

                # Brier Score
                brier_elo = (elo_prob_p1 - actual) ** 2
                brier_book = (norm_prob_p1 - actual) ** 2

                # Log loss
                clip = lambda x: max(min(x, 1 - 1e-15), 1e-15)
                logloss_elo = - (actual * math.log(clip(elo_prob_p1)) + (1 - actual) * math.log(clip(1 - elo_prob_p1)))
                logloss_book = - (actual * math.log(clip(norm_prob_p1)) + (1 - actual) * math.log(clip(1 - norm_prob_p1)))

                cursor.execute("""
                    UPDATE upcoming_matches
                    SET brier_elo = %s,
                        brier_bookmaker = %s,
                        logloss_elo = %s,
                        logloss_bookmaker = %s
                    WHERE id = %s
                """, (brier_elo, brier_book, logloss_elo, logloss_book, match_id))

        except Exception as e:
            print(f"Failed on row ID {match_id}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print("Updated upcoming_matches with odds, normalized probs, Elo, and evaluation scores.")