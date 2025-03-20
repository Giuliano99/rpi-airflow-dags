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

#Set up the Chrome options (headless if you don't want the browser window to appear)
options = Options()
options.headless = True  # Uncomment if you don't want the browser window to open
options.add_argument('--headless')  # Run in headless mode (no GUI)
options.add_argument('--no-sandbox')  # Fix issues with running in Docker or environments without a GUI
options.add_argument('--disable-dev-shm-usage')  # Prevents errors due to limited shared memory


# Create a Service object for ChromeDriver
service = Service("/usr/bin/chromedriver")  # Path to ChromeDriver on Raspberry Pi

# Set up the WebDriver
browser = webdriver.Chrome(service=service, options=options)
browser.get('https://www.flashscore.de/dart/')

browser.maximize_window()

time.sleep(3)

# Remove banner
browser.execute_script("document.getElementById('onetrust-banner-sdk').style.display = 'none';")
browser.execute_script("document.getElementsByClassName('otPlaceholder')[0].style.display = 'none';")


days_to_go_back = 6  # Adjust this value for how many days before yesterday you want
for day in range(days_to_go_back):
    try:
        wait = WebDriverWait(browser, 10)
        previous_day_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--yesterday'))
        )
        previous_day_button.click()
        print(f"Clicked on 'Vorheriger Tag' button for day {day + 1}.")
        time.sleep(2)  # Adjust if needed to allow the page to load fully
    except Exception as e:
        print(f"Error clicking 'Vorheriger Tag' button on day {day + 1}: {e}")
        break  # Stop the loop if clicking fails

#Wait for the "Vorheriger Tag" button to be clickable and click it
#try:
 #   wait = WebDriverWait(browser, 10)
  #  previous_day_button = wait.until(
   #     EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--yesterday'))
   # )
   # previous_day_button.click()
   # print("Clicked on 'Vorheriger Tag' button.")
#except Exception as e:
#    print(f"Error clicking 'Vorheriger Tag' button: {e}")

wait = WebDriverWait(browser, 10)
window_before = browser.window_handles[0]
x = 2  # Start index

# List to store the match data
match_data_list = []
try:
    # Wait until matches are loaded
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.event__match')))
    matches = browser.find_elements(By.CSS_SELECTOR, '.event__match')
    print(f"Found {len(matches)} matches")

    for i, match_element in enumerate(matches):
        try:
            # Skip non-match elements by checking for the specific CSS classes
            if 'event__match__header' in match_element.get_attribute('class'):
                print(f"Skipping non-match element at index {i}")
                continue  # Skip headers or irrelevant rows

            # Wait until the match element is clickable and click it
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.event__match')))
            match_element.click()

            # Handle new window
            time.sleep(3)
            window_after = browser.window_handles[1]
            browser.switch_to.window(window_after)

            # Scrape match details
            match_info = {}
            try:
                # Extract match date and time
                match_date_time = browser.find_element(By.CSS_SELECTOR, '.duelParticipant__startTime div').text
                match_date = match_date_time.split(' ')[0]
                match_info['Date'] = match_date
            except Exception as e:
                print(f"Error extracting date and time: {e}")

            try:
                # Extract player names
                player_1 = browser.find_element(By.CSS_SELECTOR,
                                                '.duelParticipant__home .participant__participantName').text
                player_2 = browser.find_element(By.CSS_SELECTOR,
                                                '.duelParticipant__away .participant__participantName').text
                match_info['Player 1'] = player_1
                match_info['Player 2'] = player_2
            except Exception as e:
                print(f"Error extracting players: {e}")

            try:
                # Match result
                score_player_1 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(1)').text
                score_player_2 = browser.find_element(By.CSS_SELECTOR, '.detailScore__wrapper span:nth-child(3)').text
                winner = browser.find_element(By.CSS_SELECTOR,
                                              '.duelParticipant--winner .participant__participantName').text

                match_info['Player 1 Score'] = score_player_1
                match_info['Player 2 Score'] = score_player_2
                match_info['Winner'] = winner
            except Exception as e:
                print(f"Error extracting result: {e}")

            # Extract averages (3 Darts)
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


            # Add the match data to the list
            match_data_list.append(match_info)

            # Close the match details window
            browser.close()

            browser.switch_to.window(window_before)

        except Exception as e:
            print(f"Error processing match at index {i}: {e}")
            continue  # Continue to the next match


except Exception as e:
    print(f"Error: {e}")
finally:
    browser.quit()


# Convert the match data list to a DataFrame
df = pd.DataFrame(match_data_list)

# Get the current date in the format YYYY-MM-DD
current_date = (datetime.now() - timedelta(days=days_to_go_back)).strftime('%Y-%m-%d')

# Define the target directory for saving results
output_folder = os.path.expanduser("~/airflow/darts_results")

# Create the directory if it doesn't already exist
os.makedirs(output_folder, exist_ok=True)

# Construct the full file path for the CSV
csv_filename = os.path.join(output_folder, f"match_data_airflow_{current_date}.csv")

# Save the DataFrame to the CSV file in the airflow/darts_results directory
df.to_csv(csv_filename, index=False)

print(f"Data saved to {csv_filename}")

