import requests
import json
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def fetch_skinport_prices(**kwargs):
    logger.info("Fetching Skinport market data...")

    url = "https://api.skinport.com/v1/items"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "identity",  # disable gzip/br
        "User-Agent": "Mozilla/5.0 (compatible; ArbitrageBot/1.0)"
    }
    params = {
        "app_id": 730,
        "currency": "EUR"
    }

    try:
        logger.info(f"Requesting: {url} with params={params} and headers={headers}")
        response = requests.get(url, headers=headers, params=params, timeout=15)
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response headers: {response.headers}")

        if response.status_code != 200:
            logger.error(f"❌ Error fetching data: {response.status_code} → {response.text}")
            return None

        data = response.json()
        logger.info(f"✅ Successfully fetched {len(data)} items from Skinport")
        logger.info(f"Sample: {json.dumps(data[:3], indent=2, ensure_ascii=False)}")
        return data

    except Exception as e:
        logger.error(f"⚠️ Exception fetching Skinport data: {str(e)}")
        return None
