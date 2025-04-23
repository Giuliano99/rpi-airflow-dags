import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from tabulate import tabulate
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os

# Set up the Chrome options
options = Options()
options.headless = True
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Path to ChromeDriver
service = Service("/usr/bin/chromedriver")

# Initialize the browser
browser = webdriver.Chrome(service=service, options=options)
browser.get('https://www.flashscore.de/dart/')
wait = WebDriverWait(browser, 10)

time.sleep(3)

# Remove banner
browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
browser.execute_script("document.getElementsByClassName('otPlaceholder')[0].style.display = 'none';")

# Go back N days
days_to_go_back = 2
for day in range(days_to_go_back):
    try:
        previous_day_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--yesterday'))
        )
        previous_day_button.click()
        print(f"Clicked on 'Vorheriger Tag' button for day {day + 1}.")
        time.sleep(2)
    except Exception as e:
        print(f"Error clicking 'Vorheriger Tag' button on day {day + 1}: {e}")
        break

# Match scraping
match_data_list = []

try:
    match_count = len(browser.find_elements(By.CSS_SELECTOR, '.event__match'))

    for i in range(match_count):
        try:
            # Re-fetch match list
            matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
            match_element = matches[i]

            if 'event__match__header' in match_element.get_attribute('class'):
                print(f"Skipping non-match element at index {i}")
                continue

            # Scroll into view for safety
            browser.execute_script("arguments[0].scrollIntoView(true);", match_element)
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.event__match')))
            match_element.click()

            # Wait for match detail page to load
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.detailScore__wrapper')))
            time.sleep(1)

            match_info = {}

            # Match date/time
            try:
                match_date_time = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__startTime div').text
                match_info['Date'] = match_date_time.split(' ')[0]
            except Exception as e:
                print(f"Error extracting date and time: {e}")

            # Player names
            try:
                player_1 = browser.find_element(By.CSS_SELECTOR,
                                                '.duelParticipant__home .participant__participantName').text
                player_2 = browser.find_element(By.CSS_SELECTOR,
                                                '.duelParticipant__away .participant__participantName').text
                match_info['Player 1'] = player_1
                match_info['Player 2'] = player_2
            except Exception as e:
                print(f"Error extracting players: {e}")

            # Scores and winner
            try:
                score_player_1 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(1)').text
                score_player_2 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(3)').text
                winner = browser.find_element(By.CSS_SELECTOR,
                                              '.duelParticipant--winner .participant__participantName').text

                match_info['Player 1 Score'] = score_player_1
                match_info['Player 2 Score'] = score_player_2
                match_info['Winner'] = winner
            except Exception as e:
                print(f"Error extracting result: {e}")

            # Averages
            try:
                statistic_rows = browser.find_elements(By.CSS_SELECTOR, '.wcl-row_OFViZ')
                average_player_1 = None
                average_player_2 = None
                for row in statistic_rows:
                    category_label = row.find_element(By.CSS_SELECTOR, '.wcl-category_7qsgP strong').text
                    if "Durchschnitt (3 Darts)" in category_label:
                        average_player_1 = row.find_element(By.CSS_SELECTOR, '.wcl-homeValue_-iJBW strong').text
                        average_player_2 = row.find_element(By.CSS_SELECTOR, '.wcl-awayValue_rQvxs strong').text
                        break
                match_info['Average Player 1'] = average_player_1
                match_info['Average Player 2'] = average_player_2
            except Exception as e:
                print(f"Error extracting averages: {e}")

            match_data_list.append(match_info)

            # Return to match list page
            browser.back()
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.event__match')))
            time.sleep(2)

        except Exception as e:
            print(f"Error processing match at index {i}: {e}")
            continue

except Exception as e:
    print(f"General scraping error: {e}")
finally:
    browser.quit()

# Convert to DataFrame
df = pd.DataFrame(match_data_list)

# Save results
current_date = (datetime.now() - timedelta(days=days_to_go_back)).strftime('%Y-%m-%d')
output_folder = os.path.expanduser("~/airflow/darts_results")
os.makedirs(output_folder, exist_ok=True)
csv_filename = os.path.join(output_folder, f"match_data_airflow_{current_date}.csv")
df.to_csv(csv_filename, index=False)

print(f"Data saved to {csv_filename}")