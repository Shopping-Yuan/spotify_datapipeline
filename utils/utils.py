import time
import requests
import spotipy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from settings.user import username, password,client_id,client_secret
from spotipy.oauth2 import SpotifyClientCredentials
from opencc import OpenCC

def to_traditional(simplified_text):
    converter = OpenCC('s2t')  # 's2t' 表示从简体到繁体
    traditional_text = converter.convert(simplified_text)
    return traditional_text

def get_driver():
    chrome_options = Options()
    grid_url = "http://selenium-chrome:4444/wd/hub"
    chrome_options_argument = ["--headless","--no-sandbox","--disable-dev-shm-usage"]
    for argument in chrome_options_argument:
        chrome_options.add_argument(argument)
    driver = webdriver.Remote(
        command_executor=grid_url,
        options=chrome_options
    )
    return driver

def log_in(driver):
    driver.get("https://charts.spotify.com/home")
    time.sleep(5)
    login_field = driver.find_element(By.XPATH, "//*[@class='ButtonInner-sc-14ud5tc-0 bRLmlP encore-bright-accent-set']")
    login_field.click()
    time.sleep(5)
    wait = WebDriverWait(driver, 10)
    u_field = wait.until(EC.presence_of_element_located((By.ID, "login-username")))
    p_field = wait.until(EC.presence_of_element_located((By.ID, "login-password")))
    show_password = driver.find_element(By.XPATH, "//*[@class='Button-sc-1dqy6lx-0 bnvbjc']")
    time.sleep(1)
    show_password.click()
    time.sleep(3)
    u_field.send_keys(username)
    time.sleep(3)
    p_field.send_keys(password)
    login_button = driver.find_element(By.ID, "login-button")
    time.sleep(1)
    login_button.click()
    time.sleep(5)

def get_access_token():
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    return response.json().get('access_token')

from spotipy.oauth2 import SpotifyClientCredentials

def get_spotify_client():
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)