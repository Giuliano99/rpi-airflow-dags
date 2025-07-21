import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import json
from darts_project.utils.elo import elo_prediction
from darts_project.utils.db import get_connection_string
from darts_project.utils.logging import logger

# Constants
INITIAL_ELO = 1500

def predict_upcoming_matches():
    logger.info("[ℹ️] Starting upcoming matches prediction...")

    try:
        engine = create_engine(get_connection_string())
        conn = engine.connect()

        # Get upcoming matches
        upcoming_matches_query = """
        SELECT id, matchdate, player1, player2, odds
        FROM upcoming_matches
        WHERE matchdate >= CURRENT_DATE
        """
        df = pd.read_sql(upcoming_matches_query, conn)

        if df.empty:
            logger.info("[ℹ️] No upcoming matches found.")
            conn.close()
            return

        # Get latest Elo data
        elo_query = """
        SELECT playername, latest_elo, match_count
        FROM elo_latest_view
        """
        elo_df = pd.read_sql(elo_query, conn)

        # Prepare Elo lookup with default INITIAL_ELO
        elo_lookup = elo_df.set_index('playername').to_dict(orient='index')

        df['player1_elo'] = df['player1'].apply(
            lambda x: elo_lookup.get(x, {}).get('latest_elo', INITIAL_ELO)
        )
        df['player2_elo'] = df['player2'].apply(
            lambda x: elo_lookup.get(x, {}).get('latest_elo', INITIAL_ELO)
        )

        df['player1_match_count'] = df['player1'].apply(
            lambda x: elo_lookup.get(x, {}).get('match_count', 0)
        )
        df['player2_match_count'] = df['player2'].apply(
            lambda x: elo_lookup.get(x, {}).get('match_count', 0)
        )

        # Calculate prediction probabilities
        df['predicted_p1_prob'] = df.apply(lambda row: elo_prediction(
            row['player1_elo'], row['player2_elo']), axis=1)
        df['predicted_p2_prob'] = 1 - df['predicted_p1_prob']

        # Prepare records for insertion
        records = []
        for idx, row in df.iterrows():
            odds_json = row['odds'] or {}

            # Extract all P1 and P2 odds
            p1_odds = [float(v) for k, v in odds_json.items() if '_P1' in k]
            p2_odds = [float(v) for k, v in odds_json.items() if '_P2' in k]

            best_p1_odds = max(p1_odds) if p1_odds else None
            best_p2_odds = max(p2_odds) if p2_odds else None

            best_p1_implied_prob = (1 / best_p1_odds) if best_p1_odds else None
            best_p2_implied_prob = (1 / best_p2_odds) if best_p2_odds else None

            value_p1 = (row['predicted_p1_prob'] - best_p1_implied_prob) if best_p1_implied_prob else None
            value_p2 = (row['predicted_p2_prob'] - best_p2_implied_prob) if best_p2_implied_prob else None

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
                json.dumps(odds_json),  # Store as JSON string
                best_p1_odds,
                best_p1_implied_prob,
                value_p1,
                best_p2_odds,
                best_p2_implied_prob,
                value_p2
            ))

        # Insert into prediction_upcoming table
        insert_query = """
        INSERT INTO prediction_upcoming (
            id, matchdate, player1, player2,
            player1_elo, player2_elo,
            player1_match_count, player2_match_count,
            predicted_p1_prob, predicted_p2_prob,
            odds, best_p1_odds, best_p1_implied_prob, value_p1,
            best_p2_odds, best_p2_implied_prob, value_p2
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            matchdate = EXCLUDED.matchdate,
            player1 = EXCLUDED.player1,
            player2 = EXCLUDED.player2,
            player1_elo = EXCLUDED.player1_elo,
            player2_elo = EXCLUDED.player2_elo,
            player1_match_count = EXCLUDED.player1_match_count,
            player2_match_count = EXCLUDED.player2_match_count,
            predicted_p1_prob = EXCLUDED.predicted_p1_prob,
            predicted_p2_prob = EXCLUDED.predicted_p2_prob,
            odds = EXCLUDED.odds,
            best_p1_odds = EXCLUDED.best_p1_odds,
            best_p1_implied_prob = EXCLUDED.best_p1_implied_prob,
            value_p1 = EXCLUDED.value_p1,
            best_p2_odds = EXCLUDED.best_p2_odds,
            best_p2_implied_prob = EXCLUDED.best_p2_implied_prob,
            value_p2 = EXCLUDED.value_p2
        ;
        """

        with engine.begin() as connection:
            connection.execute(insert_query, records)

        logger.info(f"[✅] Predictions inserted/updated for {len(df)} upcoming matches.")

    except Exception as e:
        logger.error(f"[❌] Error in predict_upcoming: {e}")

    finally:
        conn.close()
        logger.info("[ℹ️] Database connection closed.")