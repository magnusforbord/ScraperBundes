import time
from datetime import datetime, date
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
from selenium.common import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from selenium.webdriver.chrome.service import Service
from telegram import Bot
from webdriver_manager.chrome import ChromeDriverManager
import re
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID_1 = os.getenv('CHAT_ID_1')
CHAT_ID_2 = os.getenv('CHAT_ID_2')
MONGODB_URI = os.getenv('MONGODB_URI')
DB_NAME = os.getenv('DB_NAME')
SENT_TEAMS_COLLECTION = os.getenv('SENT_TEAMS_COLLECTION')
PROCESSED_LINKS_COLLECTION = os.getenv('PROCESSED_LINKS_COLLECTION')

bot = Bot(token=TELEGRAM_TOKEN)

client = MongoClient(MONGODB_URI, tls=True, tlsAllowInvalidCertificates=True)
db = client[DB_NAME]
sent_teams_collection = db[SENT_TEAMS_COLLECTION]
processed_links_collection = db[PROCESSED_LINKS_COLLECTION]


def is_today(date_string):
    date_string = date_string.split()[0].rsplit('.', 1)[0]
    current_year = datetime.today().year
    match_date = datetime.strptime(f"{date_string}.{current_year}", "%d.%m.%Y")
    today = datetime.today().date()
    return match_date.date() == today


def get_team_homepage_links(browser, home_team, away_team):
    home_team_link, away_team_link = None, None

    try:
        team_links = browser.find_elements(By.XPATH, '//a[contains(@href, "/de/import/teams/")]')
        for team_link in team_links:
            href = team_link.get_attribute("href")
            if home_team.lower() in href.lower():
                home_team_link = href
            if away_team.lower() in href.lower():
                away_team_link = href

            if home_team_link and away_team_link:
                break
    except NoSuchElementException:
        print("Team homepage links not found.")

    return home_team_link, away_team_link


def click_statistik_button(browser):
    try:
        button = browser.find_element(By.XPATH, '//li[@data-linkid="ab2de132"]/a')
        button.click()
    except NoSuchElementException:
        print("Statistik button not found.")


def extract_all_player_info(browser):
    player_name_list = []
    players_goals_list = []
    players_assists_list = []

    try:
        player_name_elements = browser.find_elements(By.XPATH, '//td[@class="aleft footable-visible"]/a')
        player_goals_elements = browser.find_elements(By.XPATH, '//tr/td[4]')
        player_assist_elements = browser.find_elements(By.XPATH, '//tr/td[9]')

        for player_name_element in player_name_elements:
            player_name_parts = player_name_element.text.strip().split('\n')
            player_full_name = f"{player_name_parts[0]} {player_name_parts[1].strip()}".upper()
            player_name_list.append(player_full_name)

        for player_goals_element in player_goals_elements:
            player_goals = player_goals_element.text.strip()
            players_goals_list.append(player_goals)

        for player_assist_element in player_assist_elements:
            player_assists = player_assist_element.text.strip()
            players_assists_list.append(player_assists)

    except NoSuchElementException:
        print("Player info not found.")

    return player_name_list, players_goals_list, players_assists_list


def normalize_player_name(player_name):
    player_name = player_name.replace(',', '').strip()
    return ' '.join(player_name.split()[::-1])


def is_preliminary_lineup(browser):
    try:
        browser.find_element(By.XPATH, '//div[contains(text(), "Vorläufig")]')
        return True
    except NoSuchElementException:
        return False


def insert_missing_players(team_name, date, collection):
    document = {
        "team_name": team_name,
        "date": date
    }
    collection.insert_one(document)

def is_link_processed(link):
    query = {"link": link}
    result = processed_links_collection.find_one(query)
    return result is not None

def store_processed_link(link):
    document = {
        "link": link,
        "date": datetime.today().date().isoformat()
    }
    processed_links_collection.insert_one(document)

def already_sent(team_name, date, collection):
    query = {"team_name": team_name, "date": date}
    result = collection.find_one(query)
    return result is not None


def send_telegram_message(players_info, team_name, chat_id):
    formatted_text = f"Players missing for {team_name}:\n"
    formatted_text += '\n'.join([f"{player} - {info[0]} goals - {info[1]} assists" for player, info in players_info.items()])
    bot.send_message(chat_id=chat_id, text=formatted_text)


def wait_for_players_or_preliminary(browser):
    def players_or_preliminary(_):
        try:
            browser.find_element(By.XPATH, '//div[contains(text(), "Vorläufig")]')
            return True
        except NoSuchElementException:
            pass

        try:
            browser.find_elements(By.XPATH, '//div[@class="sr-matchlineups-row sr-border sr-clearfix"]')
            return True
        except NoSuchElementException:
            pass

        return False

    return players_or_preliminary


base_url = 'https://www.liquimoly-hbl.de'

chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chromedriver_path = os.environ.get("CHROMEDRIVER_PATH") or ChromeDriverManager().install()
chrome_service = Service(executable_path=chromedriver_path)
browser = webdriver.Chrome(service=chrome_service, options=chrome_options)
browser.set_window_size(1920, 1080)

browser.get(base_url)

try:
    accept_cookies_button = WebDriverWait(browser, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="truendo_container"]/div[2]/div/div[2]/div[2]/div/button[2]'))
    )
    accept_cookies_button.click()
except TimeoutException:
    print("Accept cookies button not found or not clickable.")


wait = WebDriverWait(browser, 20)
wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/section[3]/div[2]/div/div/div/div/table/tbody')))


page_source = browser.page_source

soup = BeautifulSoup(page_source, 'html.parser')

match_rows = soup.select('#schedule1x2content1350470 div table tbody tr')

match_links = []

for row in match_rows:
    date_col = row.select_one('td.aleft')
    if date_col is not None:
        date_str = date_col.get_text(strip=True).split('\n')[0].strip()
        if is_today(date_str):
            match_link = base_url + row.select_one('td.acenter.large a')['href']
            match_links.append(match_link)

for match_link in match_links:
    if is_link_processed(match_link):
        continue
    browser.get(match_link)

    pattern = r'spieltag--([\w\s-]+?)---([\w\s-]+?)\/'
    team_names = re.search(pattern, match_link)
    home_team, away_team = team_names.group(1), team_names.group(2)

    time.sleep(5)

    try:
        home_team_player_names = []
        away_team_player_names = []


        # Identify the iframe
        target_iframe = browser.find_element(By.CSS_SELECTOR, "iframe.sportradar-widget")
        browser.switch_to.frame(target_iframe)

        # Locate and click on the Aufstellungen div to open lineups
        aufstellungen_div = browser.find_element(By.CSS_SELECTOR, 'body > div.srl-wrapper > div > div.srl-content > div.srl-main-content.srl-flex > div.srl-tabs-wrapper.srl-flex-child > div > div.srl-tabs-header.sr-clearfix > div:nth-child(2)')
        browser.execute_script("arguments[0].click()", aufstellungen_div)

        wait = WebDriverWait(browser, 5)
        try:
            wait.until(wait_for_players_or_preliminary(browser))
        except TimeoutException:
            print("Neither players nor 'Vorläufig' found.")
            continue

        # Wait for the desired element to appear
        wait = WebDriverWait(browser, 20)
        element_found = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[@class="sr-matchlineups-row sr-border sr-clearfix"]')))

        if is_preliminary_lineup(browser):
            continue

        # Find player names using the XPath
        home_team_players_first_name_xpath = '//div[contains(@class, "sr-matclineups-teams-home")]//div[contains(@class, "sr-player-name")]/span[@class="sr-matchlineups-player-first-name"]'
        home_team_players_last_name_xpath = '//div[contains(@class, "sr-matclineups-teams-home")]//div[contains(@class, "sr-player-name")]/span[@class="sr-matchlineups-player-lastname"]'
        away_team_players_first_name_xpath = '//div[contains(@class, "sr-matclineups-teams-away")]//div[contains(@class, "sr-player-name")]/span[@class="sr-matchlineups-player-first-name"]'
        away_team_players_last_name_xpath = '//div[contains(@class, "sr-matclineups-teams-away")]//div[contains(@class, "sr-player-name")]/span[@class="sr-matchlineups-player-lastname"]'

        home_team_player_first_name_elements = browser.find_elements(By.XPATH, home_team_players_first_name_xpath)
        home_team_player_last_name_elements = browser.find_elements(By.XPATH, home_team_players_last_name_xpath)
        away_team_player_first_name_elements = browser.find_elements(By.XPATH, away_team_players_first_name_xpath)
        away_team_player_last_name_elements = browser.find_elements(By.XPATH, away_team_players_last_name_xpath)

        for first_name_element, last_name_element in zip(home_team_player_first_name_elements, home_team_player_last_name_elements):
            player_name = f"{last_name_element.text.strip().upper()}, {first_name_element.text.strip().upper()}"
            home_team_player_names.append(player_name)

        for first_name_element, last_name_element in zip(away_team_player_first_name_elements, away_team_player_last_name_elements):
            player_name = f"{last_name_element.text.strip().upper()}, {first_name_element.text.strip().upper()}"
            away_team_player_names.append(player_name)

        # Switch back to the default content
        browser.switch_to.default_content()

        # Get team homepage links
        home_team_link, away_team_link = get_team_homepage_links(browser, home_team, away_team)

        # Create sets of player names and numbers from match lineup
        wait = WebDriverWait(browser, 10)
        home_team_player_elements_set = set((player_name) for player_name in home_team_player_names)
        away_team_player_elements_set = set((player_name) for player_name in away_team_player_names)


        # Navigate to home team's homepage
        browser.get(home_team_link)
        time.sleep(5)

        click_statistik_button(browser)
        time.sleep(3)
        home_team_player_info, home_team_goals_info, home_team_assists_info = extract_all_player_info(browser)
        home_team_player_info_set = set(home_team_player_info)

        # Navigate to away team's homepage
        browser.get(away_team_link)
        time.sleep(5)

        click_statistik_button(browser)
        time.sleep(3)
        away_team_player_info, away_team_goals_info, away_team_assists_info = extract_all_player_info(browser)
        away_team_player_info_set = set(away_team_player_info)



        # Compare and print the players that are only in extract_all_player_info
        missing_home_team_players = home_team_player_info_set - home_team_player_elements_set
        missing_away_team_players = away_team_player_info_set - away_team_player_elements_set

        missing_home_team_players_info = {player: (goals, assists) for player, goals, assists in zip(home_team_player_info, home_team_goals_info, home_team_assists_info) if player in missing_home_team_players}
        missing_away_team_players_info = {player: (goals, assists) for player, goals, assists in zip(away_team_player_info, away_team_goals_info, away_team_assists_info) if player in missing_away_team_players}

        today = datetime.combine(date.today(), datetime.min.time())

        store_processed_link(match_link)

        if not already_sent(home_team, today, sent_teams_collection):
            send_telegram_message(missing_home_team_players_info, home_team, CHAT_ID_1)
            send_telegram_message(missing_home_team_players_info, home_team, CHAT_ID_2)
            insert_missing_players(home_team, today, sent_teams_collection)

        if not already_sent(away_team, today, sent_teams_collection):
            send_telegram_message(missing_away_team_players_info, away_team, CHAT_ID_1)
            send_telegram_message(missing_away_team_players_info, away_team, CHAT_ID_2)
            insert_missing_players(away_team, today, sent_teams_collection)

    except NoSuchElementException:
        print("No such elements found")
        continue
    except TimeoutException:
        print("Script timed out")
        continue

browser.quit()
