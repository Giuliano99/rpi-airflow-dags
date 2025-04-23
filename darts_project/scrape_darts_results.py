import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os

# Set up Chrome options
options = Options()
options.add_argument('--headless=new')  # Use new headless mode
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--remote-debugging-port=9222')
options.binary_location = '/usr/bin/chromium-browser'

service = Service("/usr/bin/chromedriver")
browser = webdriver.Chrome(service=service, options=options)
browser.get('https://www.flashscore.de/dart/')
browser.maximize_window()

time.sleep(3)

# Remove cookie banner
browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
browser.execute_script("document.getElementsByClassName('otPlaceholder')[0].style.display = 'none';")

# Go back one day if needed
days_to_go_back = 1
for day in range(days_to_go_back):
    try:
        wait = WebDriverWait(browser, 10)
        previous_day_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--yesterday'))
        )
        previous_day_button.click()
        print(f"Clicked on 'Vorheriger Tag' button for day {day + 1}.")
        time.sleep(2)
    except Exception as e:
        print(f"Error clicking 'Vorheriger Tag' button: {e}")
        break

wait = WebDriverWait(browser, 10)
match_data_list = []

try:
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.event__match')))
    matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
    print(f"Found {len(matches)} matches")

    for i in range(len(matches)):
        try:
            # Re-find match elements to avoid stale references
            matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
            match_element = matches[i]

            if 'event__match__header' in match_element.get_attribute('class'):
                print(f"Skipping non-match element at index {i}")
                continue

            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.event__match')))
            match_element.click()

            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.duelParticipant__startTime')))

            match_info = {}

            try:
                match_date_time = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__startTime div').text
                match_date = match_date_time.split(' ')[0]
                match_info['Date'] = match_date
            except Exception as e:
                print(f"Error extracting date/time: {e}")

            try:
                player_1 = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__home .participant__participantName').text
                player_2 = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__away .participant__participantName').text
                match_info['Player 1'] = player_1
                match_info['Player 2'] = player_2
            except Exception as e:
                print(f"Error extracting players: {e}")

            try:
                score_player_1 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(1)').text
                score_player_2 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(3)').text
                winner = browser.find_element(By.CSS_SELECTOR, '.duelParticipant--winner .participant__participantName').text
                match_info['Player 1 Score'] = score_player_1
                match_info['Player 2 Score'] = score_player_2
                match_info['Winner'] = winner
            except Exception as e:
                print(f"Error extracting result: {e}")

            try:
                statistic_rows = browser.find_elements(By.CSS_SELECTOR, '.wcl-row_OFViZ')
                average_player_1 = None
                average_player_2 = None
                for row in statistic_rows:
                    label = row.find_element(By.CSS_SELECTOR, '.wcl-category_7qsgP strong').text
                    if "Durchschnitt (3 Darts)" in label:
                        average_player_1 = row.find_element(By.CSS_SELECTOR, '.wcl-homeValue_-iJBW strong').text
                        average_player_2 = row.find_element(By.CSS_SELECTOR, '.wcl-awayValue_rQvxs strong').text
                        break
                match_info['Average Player 1'] = average_player_1
                match_info['Average Player 2'] = average_player_2
            except Exception as e:
                print(f"Error extracting averages: {e}")

            match_data_list.append(match_info)

            browser.back()
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.event__match')))
            time.sleep(1)

        except Exception as e:
            print(f"Error processing match at index {i}: {e}")
            continue

except Exception as e:
    print(f"Top-level error: {e}")
finally:
    browser.quit()

# Save results
df = pd.DataFrame(match_data_list)
current_date = (datetime.now() - timedelta(days=days_to_go_back)).strftime('%Y-%m-%d')
output_folder = os.path.expanduser("~/airflow/darts_results")
os.makedirs(output_folder, exist_ok=True)
csv_filename = os.path.join(output_folder, f"match_data_airflow_{current_date}.csv")
df.to_csv(csv_filename, index=False)

print(f"Data saved to {csv_filename}")