from great_expectations import get_context
from great_expectations.core import ExpectationSuite
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_suite():
    context = get_context()  # assumes GE config is in /app/great_expectations/gx
    suite_name = "darts_results_suite"

    try:
        suite = context.suites.get(suite_name)
        logger.info(f"Suite '{suite_name}' already exists.")
    except Exception:
        logger.info(f"Suite '{suite_name}' not found. Creating it.")
        suite = ExpectationSuite(suite_name)
        context.suites.add(suite)
        logger.info(f"Suite '{suite_name}' created.")

if __name__ == "__main__":
    create_suite()
