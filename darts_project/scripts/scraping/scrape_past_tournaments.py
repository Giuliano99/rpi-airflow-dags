import os
import time
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
logger.addHandler(handler)

# Headless browser
def make_browser():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.binary_location = '/usr/bin/chromium-browser'
    service = Service("/usr/bin/chromedriver")
    return webdriver.Chrome(service=service, options=options)

# Collect tournament URLs from the left menu
# def get_tournament_links(browser, base_url="https://www.flashscore.com"):
#     browser.get(base_url + "/darts/")
#     wait = WebDriverWait(browser, 10)

#     # Wait for left menu to load
#     wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".lmc__element")))

#     # Expand all regions
#     regions = browser.find_elements(By.CSS_SELECTOR, ".lmc__element")
#     for region in regions:
#         try:
#             browser.execute_script("arguments[0].scrollIntoView(true);", region)
#             region.click()
#             time.sleep(1)
#         except Exception as e:
#             logger.warning(f"Couldn't expand region: {e}")
#             continue

#     # Get only expanded blocks
#     expanded_blocks = browser.find_elements(By.CSS_SELECTOR, ".lmc__block.lmc__blockOpened")
#     logger.info(f"Found {len(expanded_blocks)} expanded countries.")

#     tournaments = []
#     for block in expanded_blocks:
#         try:
#             country_name = block.find_element(By.CSS_SELECTOR, ".lmc__elementName").text.strip()
#             league_links = block.find_elements(By.CSS_SELECTOR, ".lmc__templateHref")

#             for link in league_links:
#                 name = link.text.strip()
#                 href = link.get_attribute("href")
#                 if name and href:
#                     if not href.startswith("http"):
#                         href = base_url + href
#                     full_url = href.rstrip("/") + "/results/"
#                     full_name = f"{country_name} - {name}"
#                     tournaments.append((full_name, full_url))

#         except Exception as e:
#             logger.warning(f"Error processing block: {e}")
#             continue

#     logger.info(f"Found {len(tournaments)} tournaments under expanded countries.")
#     return tournaments
def get_tournament_links(browser, base_url="https://www.flashscore.com"):
    browser.get(base_url + "/darts/")
    wait = WebDriverWait(browser, 10)

    # Wait for regions to load
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".lmc__element")))

    # Locate and expand only "World"
    world_region = None
    regions = browser.find_elements(By.CSS_SELECTOR, ".lmc__element")
    for region in regions:
        try:
            name = region.find_element(By.CSS_SELECTOR, ".lmc__elementName").text.strip().lower()
            if name == "world":
                world_region = region
                browser.execute_script("arguments[0].scrollIntoView(true);", region)
                region.click()
                time.sleep(1)
                break
        except Exception as e:
            logger.warning(f"Error locating or clicking 'World': {e}")

    if not world_region:
        logger.error("Could not find 'World' region in the menu.")
        return []

    # Only collect from the expanded "World" block
    try:
        block = world_region.find_element(By.XPATH, "./ancestor::div[contains(@class, 'lmc__blockOpened')]")
        country_name = block.find_element(By.CSS_SELECTOR, ".lmc__elementName").text.strip()
        league_links = block.find_elements(By.CSS_SELECTOR, ".lmc__templateHref")

        tournaments = []
        for link in league_links:
            name = link.text.strip()
            href = link.get_attribute("href")
            if name and href:
                if not href.startswith("http"):
                    href = base_url + href
                full_url = href.rstrip("/") + "/results/"
                full_name = f"{country_name} - {name}"
                tournaments.append((full_name, full_url))

        logger.info(f"Found {len(tournaments)} tournaments under 'World'.")
        return tournaments

    except Exception as e:
        logger.error(f"Error extracting tournaments under 'World': {e}")
        return []


# Scrape a single tournament
def scrape_tournament(tournament_name, url, browser, output_folder):
    logger.info(f"Scraping {tournament_name} → {url}")
    browser.get(url)
    time.sleep(2)
    WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.event__match')))

    matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
    logger.info(f"  → Found {len(matches)} matches")

    data = []
    orig_window = browser.current_window_handle

    for i in range(len(matches)):
        try:
            matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
            m = matches[i]
            if 'event__match__header' in m.get_attribute('class'):
                continue

            mid = m.get_attribute("id") or ""
            mid_clean = mid[4:].lstrip("_")
            match_url = f"https://www.flashscore.com/match/darts/{mid_clean}/#/match-summary/match-summary"

            browser.execute_script("window.open('');")
            browser.switch_to.window(browser.window_handles[-1])
            browser.get(match_url)

            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.duelParticipant__startTime')))
            info = {}

            try:
                dt_text = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__startTime div').text
                dt_obj = datetime.strptime(dt_text, "%d.%m.%Y %H:%M")
                info['MatchDateTime'] = dt_obj
                info['Date'] = dt_obj.date()
                info['Time'] = dt_obj.time()
            except:
                info['MatchDateTime'] = info['Date'] = info['Time'] = None

            try:
                info['Player 1'] = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__home .participant__participantName').text
                info['Player 2'] = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__away .participant__participantName').text
            except:
                info['Player 1'] = info['Player 2'] = None

            try:
                info['Player 1 Score'] = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(1)').text
                info['Player 2 Score'] = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(3)').text
                info['Winner'] = browser.find_element(By.CSS_SELECTOR, '.duelParticipant--winner .participant__participantName').text
            except:
                info['Player 1 Score'] = info['Player 2 Score'] = info['Winner'] = None

            try:
                rows = browser.find_elements(By.CSS_SELECTOR, '.wcl-row_OFViZ')
                avg1 = avg2 = None
                for r in rows:
                    lbl = r.find_element(By.CSS_SELECTOR, '.wcl-category_7qsgP strong').text
                    if "Durchschnitt (3 Darts)" in lbl:
                        avg1 = r.find_element(By.CSS_SELECTOR, '.wcl-homeValue_-iJBW strong').text
                        avg2 = r.find_element(By.CSS_SELECTOR, '.wcl-awayValue_rQvxs strong').text
                        break
                info['Average Player 1'] = avg1
                info['Average Player 2'] = avg2
            except:
                info['Average Player 1'] = info['Average Player 2'] = None

            data.append(info)
            browser.close()
            browser.switch_to.window(orig_window)
            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"Failed match {i} in {tournament_name}: {e}")
            browser.switch_to.window(orig_window)
            continue

    # Save CSV
    df = pd.DataFrame(data)
    print(df)
    safe_name = tournament_name.replace(" ", "_").replace(":", "").replace("(", "").replace(")", "")
    csv_path = os.path.join(output_folder, f"{safe_name}.csv")
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved {len(df)} matches to {csv_path}")

# Main runner
def main():
    output_folder = os.path.expanduser("~/airflow/darts_results")
    os.makedirs(output_folder, exist_ok=True)

    browser = make_browser()
    try:
        tournaments = get_tournament_links(browser)
        for name, url in tournaments:
            try:
                scrape_tournament(name, url, browser, output_folder)
            except Exception as e:
                logger.error(f"Error scraping {name}: {e}")
                continue
    finally:
        browser.quit()

if __name__ == "__main__":
    main()
