import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
import os

def hide_cookie_banner(driver):
    try:
        driver.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
    except:
        pass

# Setup browser
options = Options()
#options.add_argument('--headless=new')  # Enable headless scraping
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.binary_location = '/usr/bin/chromium-browser'
service = Service("/usr/bin/chromedriver")
browser = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(browser, 15)

# Load main page
browser.get('https://www.flashscore.com/darts/')
time.sleep(3)

# Remove cookie banner if present
try:
    browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
except:
    pass

# Click 'Tomorrow' button
try:
    wait = WebDriverWait(browser, 10)
    tomorrow_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--tomorrow')))
    tomorrow_button.click()
    time.sleep(2)
except Exception as e:
    print(f"Error navigating to tomorrow: {e}")

# Collect matches
match_data = []
original_window = browser.current_window_handle
wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.event__match')))
matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')

for i in range(len(matches)):
    try:
        matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
        match = matches[i]

        if 'event__match__header' in match.get_attribute('class'):
            continue

        match_id = match.get_attribute('id')
        if not match_id:
            continue

        match_id_clean = match_id[4:].lstrip("_")
        match_url = f"https://www.flashscore.com/match/darts/{match_id_clean}/#/match-summary/match-summary"

        # Open match in new tab
        browser.execute_script("window.open('');")
        browser.switch_to.window(browser.window_handles[-1])
        browser.get(match_url)
        time.sleep(2)
        hide_cookie_banner(browser)

        # Wait for players
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.duelParticipant__startTime')))
        player1 = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__home .participant__participantName').text
        player2 = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__away .participant__participantName').text

        match_info = {
            'Date': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
            'Player 1': player1,
            'Player 2': player2,
        }

        # Remove cookie banner if present
        try:
            browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
        except:
            pass

        # Click "Odds" tab
        try:
            time.sleep(2)
            odds_tab_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Odds')]"))
            )
            odds_tab_button.click()
        except Exception as e:
            print(f"Odds tab not found for match {match_id_clean}: {e}")
            browser.close()
            browser.switch_to.window(original_window)
            continue

        # Remove cookie banner if present
        try:
            time.sleep(2)
            browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
        except:
            pass

        # Scrape odds
        odds_table_rows = browser.find_elements(By.CSS_SELECTOR, '.ui-table__row')
        for row in odds_table_rows:
            try:
                bookmaker_name = row.find_element(By.CSS_SELECTOR, '.oddsCell__bookmaker img').get_attribute('alt')
                odds = row.find_elements(By.CSS_SELECTOR, '.oddsCell__odd span')
                if len(odds) < 2:
                    continue  # Skip incomplete odds

                match_info[f'Odds {bookmaker_name} Player 1'] = odds[0].text
                match_info[f'Odds {bookmaker_name} Player 2'] = odds[1].text
            except Exception as e:
                print(f"Could not parse odds row: {e}")
                continue

        match_data.append(match_info)

        # Close tab
        browser.close()
        browser.switch_to.window(original_window)
        time.sleep(1)

    except Exception as e:
        print(f"Error processing match {i}: {e}")
        browser.switch_to.window(original_window)
        continue

browser.quit()

# Save to CSV
df = pd.DataFrame(match_data)
print(df)

if not df.empty:
    output_folder = os.path.expanduser("~/airflow/darts_upcoming")
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, f"upcoming_odds_{datetime.now().strftime('%Y-%m-%d')}.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved odds data to {output_path}")
else:
    print("⚠️ No match data collected.")