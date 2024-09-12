import os
from datetime import date,datetime,timedelta
import pandas as pd
from airflow.decorators import dag, task
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException
import random
import os
from utils.utils import get_driver, log_in
from google.cloud import storage
from google.oauth2 import service_account

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["shopping0789604@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Global functions
def replace_re_entry(data):
    return data.replace("Re-\nEnt\nry", "0")

def split_for_one(data):
    return data.split("\n")

def remove_index(daily_data_split, rmindex=[1, 4]):
    return [data for i, data in enumerate(daily_data_split) if i not in rmindex]

def get_pk(data):
    return data.split("\n")[4].split(" ")
def get_date(offset): #台灣時間上8點後latest是前一天的data，以及資料統計固定延遲一天
    today = date.today()
    target_date = today + timedelta(days=offset)
    return target_date.isoformat()
def get_file_path(day):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(current_directory)
    file_path = os.path.join(parent_directory, 'datas')
    os.makedirs(file_path, exist_ok=True)
    file_path = os.path.join(file_path, f"{day}_spotify_chart_tw.csv")
    return file_path
@dag(
    dag_id="get_ranks_dag",
    default_args=default_args,
    description="Get daily rank data of Taiwanese tracks on Spotify.",
    schedule_interval="00 8 * * *",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    tags=["spotify", "rank"]
)
def get_ranks_dag():

    @task
    def fetch_data(rank_day):
        driver = get_driver()
        try:
            log_in(driver)
        except InvalidSessionIdException:
            print("Session has already been terminated or invalid.")
        
        columns = ["date", "rank", "track_name", "artist_names", "peak_rank", "previous_rank", "days_on_chart", "streams", "track_id", "artist_id"]

        print(f"Processing date: {rank_day}")
        
        chart_url = f"https://charts.spotify.com/charts/view/regional-tw-daily/{rank_day}"
        driver.get(chart_url)
        
        random_wait_time = random.uniform(5, 12)
        wait = WebDriverWait(driver, random_wait_time)
        tbody = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
        
        links = tbody.find_elements(By.CSS_SELECTOR, 'a[href^="https://open.spotify.com/"]')
        rows = tbody.find_elements(By.TAG_NAME, 'tr')
        
        daily_data = [row.text for row in rows]
        daily_data_cleans = []

        for data in daily_data:
            data = replace_re_entry(data)
            daily_data_split = split_for_one(data)
            daily_data_clean = remove_index(daily_data_split)
            Peak_Prev_Streak_Streams = get_pk(data)
            daily_data_clean.extend(Peak_Prev_Streak_Streams)
            daily_data_cleans.append(daily_data_clean)
        
        check_track_id = ""
        track_number = -1
        element_number = -1
        
        for link in links:
            href = link.get_attribute('href')
            track_id = href.split("/")[-1]
            element_number += 1
            
            if href.split("/")[-2] == "track" and track_id != check_track_id:
                check_track_id = track_id
                track_number += 1
                element_number = 0
                if track_number < len(daily_data_cleans):
                    daily_data_cleans[track_number].append(track_id)
            elif element_number == 2 and track_number < len(daily_data_cleans):
                daily_data_cleans[track_number].append(track_id)
        
        df = pd.DataFrame(data=daily_data_cleans, columns=columns[1:])
        df['date'] = rank_day
        df = df[columns]
        file_path = get_file_path(rank_day)

        

        df.to_csv(file_path, index=False, encoding='utf-8')
        driver.quit()
        print("Data fetch complete")
        return file_path
    @task
    def upload_rank_to_gcs(bucket_name, destination_blob_name):
        source_file_path = get_file_path()
        # 上傳GCS需要的key
        current_directory = os.path.dirname(os.path.abspath(__file__))
        parent_directory = os.path.dirname(current_directory)
        file_path = os.path.join(parent_directory, 'keys','key_to_gcs.json')
        credentials = service_account.Credentials.from_service_account_file(
            f'{file_path}')
        
        """Uploads a file to the bucket."""
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_path)
        print(
            f"File {source_file_path} uploaded to {destination_blob_name}."
        )
    rank_day = get_date(offset=-2)
    fetch_data(rank_day)
    destination_blob_name = f"uploads/{rank_day[:4]}/{rank_day}_spotify_chart_tw.csv"
    upload_rank_to_gcs("spotify_data_for_analysis",destination_blob_name)

# Instantiate the DAG
get_ranks_dag()
