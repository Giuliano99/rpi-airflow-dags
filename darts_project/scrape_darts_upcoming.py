from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
import pandas as pd
import os

def get_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")

    service = Service("/usr/bin/chromedriver")

    return webdriver.Chrome(service=service, options=options)

def hide_cookie_banner(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button#onetrust-accept-btn-handler'))
        ).click()
    except:
        pass

def force_hide_overlay(driver):
    try:
        driver.execute_script("""
            const overlay = document.querySelector('.skOT__ti');
            if (overlay) overlay.style.display = 'none';
        """)
    except:
        pass

def extract_odds(driver):
    odds_data = []

    try:
        print("⏳ Waiting for .section__prematchOdds to be present...")
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.section__prematchOdds'))
        )
        print("✅ Found .section__prematchOdds")

        odds_blocks = driver.find_elements(By.CSS_SELECTOR, ".section__prematchOdds .wclOddsContent")
        print(f"🔍 Found {len(odds_blocks)} odds blocks")

        for block in odds_blocks:
            try:
                bookmaker_el = block.find_element(By.CSS_SELECTOR, ".bookmaker a")
                bookmaker = bookmaker_el.get_attribute("title")
                print(f"📗 Bookmaker found: {bookmaker}")

                odds_buttons = block.find_elements(By.CSS_SELECTOR, "button[data-testid='wcl-oddsCell']")
                odds_values = []

                for btn in odds_buttons:
                    try:
                        val = btn.find_element(By.CSS_SELECTOR, "span[data-testid='wcl-oddsValue']").text.strip()
                        odds_values.append(val if val != "-" else None)
                    except Exception as e:
                        print(f"⚠️ Couldn't extract odds value: {e}")
                        odds_values.append(None)

                print(f"➡️ Odds: {odds_values}")
                odds_data.append({"bookmaker": bookmaker, "quoten": odds_values})

            except Exception as e:
                print(f"❌ Error in odds block: {e}")
                continue

    except Exception as e:
        print(f"❌ Failed to find odds section: {e}")

    return odds_data


def main():
    driver = get_driver()
    wait = WebDriverWait(driver, 10)
    match_data = []

    try:
        driver.get("https://www.flashscore.de/dart/")
        hide_cookie_banner(driver)

        # Navigate to tomorrow
        try:
            force_hide_overlay(driver)
            tomorrow_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.calendar__navigation--tomorrow'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", tomorrow_button)
            driver.execute_script("arguments[0].click();", tomorrow_button)
        except:
            pass

        matches = driver.find_elements(By.CSS_SELECTOR, '.event__match')

        for i in range(len(matches)):  # #len(matches)):
            try:
                matches = driver.find_elements(By.CSS_SELECTOR, '.event__match')
                match = matches[i]

                if 'event__match__header' in match.get_attribute('class'):
                    continue

                match_id = match.get_attribute('id')
                if not match_id:
                    continue

                match_id_clean = match_id[4:].lstrip("_")
                match_url = f"https://www.flashscore.de/spiel/dart/{match_id_clean}/#/spiel-zusammenfassung"

                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(match_url)

                force_hide_overlay(driver)

                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.duelParticipant__startTime')))
                player1 = driver.find_element(By.CSS_SELECTOR, '.duelParticipant__home .participant__participantName').text
                player2 = driver.find_element(By.CSS_SELECTOR, '.duelParticipant__away .participant__participantName').text

                match_info = {
                    'Date': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
                    'Player 1': player1,
                    'Player 2': player2,
                }

                odds = extract_odds(driver)
                for book in odds:
                    bm = book.get("bookmaker", "Unknown")
                    odds_values = book.get("quoten", [])
                    match_info[f"{bm}_P1"] = odds_values[0] if len(odds_values) > 0 else None
                    match_info[f"{bm}_P2"] = odds_values[1] if len(odds_values) > 1 else None

                match_data.append(match_info)

                driver.close()
                driver.switch_to.window(driver.window_handles[0])

            except:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                continue

    finally:
        driver.quit()

        if match_data:
            df = pd.DataFrame(match_data)
            output_folder = os.path.expanduser("~/airflow/darts_upcoming")
            os.makedirs(output_folder, exist_ok=True)
            output_path = os.path.join(output_folder, f"upcoming_odds_{datetime.now().strftime('%Y-%m-%d')}.csv")
            df.to_csv(output_path, index=False)
            print(f"\n✅ Saved data to {output_path}")
            print(df)
        else:
            print("⚠️ No match data collected.")

if __name__ == "__main__":
    main()