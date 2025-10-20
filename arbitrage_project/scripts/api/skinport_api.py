import requests
import json
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def fetch_skinport_prices(**kwargs):
    logger.info("Fetching Skinport market data...")

    url = "https://api.skinport.com/v1/items"
    headers = {
        "Accept": "application/json",  # ✅ required
        "User-Agent": "Mozilla/5.0 (compatible; ArbitrageBot/1.0; +https://yourdomain.com)"
    }
    params = {
        "app_id": 730,  # CS:GO / CS2
        "currency": "EUR"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f"❌ Error fetching data: {response.status_code} → {response.text}")
            return None

        data = response.json()
        logger.info(f"✅ Successfully fetched {len(data)} items from Skinport")

        # Optional: log first few items
        logger.info(f"Sample: {json.dumps(data[:3], indent=2, ensure_ascii=False)}")

        # Return for downstream tasks if needed
        return data

    except Exception as e:
        logger.error(f"⚠️ Exception fetching Skinport data: {str(e)}")
        return None
