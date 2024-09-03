# from datetime import datetime, timedelta

# import pandas as pd
# from airflow.decorators import dag, task


# # 起始和结束日期
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import InvalidSessionIdException
# import pandas as pd
# import random
# from utils.utils import get_driver,log_in

# # Default arguments for the DAG
# default_args = {
#     "owner": "Admin",
#     "depends_on_past": False,
#     "email": ["your_email@example.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# @dag(
#     dag_id="get_ranks_dag",
#     default_args=default_args,
#     description="demo for tir102 spotify pipeline",
#     schedule_interval="5 * * * *",
#     start_date=datetime(2024, 8, 1),
#     catchup=False,
#     tags=["spotify", "decorator"]  # Optional: Add tags for better filtering in the UI
# )

# def get_ranks_dag(): #官方建議ETL個別邏輯分開來定義函數，取名可以用e_, t_, l_
#     # @task
#     def replace_re_entry(data):
#         return data.replace("Re-\nEnt\nry","0")

#     @task
#     def split_for_one(data):
#     #split for one
#         daily_data_split = data.split("\n")
#         return daily_data_split

#     @task
#     def remove_index(daily_data_split,rmindex = [1,4]):
#         daily_data_clean = []
#         for i, data in enumerate(daily_data_split):
#             if i not in rmindex:
#                 daily_data_clean.append(data)
#         return daily_data_clean
    
#     # @task
#     # def get_pk(data):
#     #     #(統計資料)peak, previous rank, streak and streams
#     #     Peak_Prev_Streak_Streams = data.split("\n")[4].split(" ")
#     #     return Peak_Prev_Streak_Streams

#     @task
#     def main():
#         driver = get_driver()
#         try:
#             log_in(driver)
#         except InvalidSessionIdException:
#             print("Session has already been terminated or invalid.")


#         start_date = datetime(2023, 9, 10)
#         end_date = datetime(2023, 9, 13)

#         # 当前日期初始化为起始日期
#         current_date = start_date

#         # 使用while循环遍历日期范围

#         # 定義列名
#         columns = ["date", "rank", "track_name", "artist_names", "peak_rank", "previous_rank", "days_on_chart", "streams","track_id","artist_id"]   
        
#         while current_date <= end_date:
#             # 獲取當前日期
#             # today = date.today().isoformat()
#             today = current_date.strftime('%Y-%m-%d')
#             if current_date.strftime('%d') == end_date.strftime('%d'):
#                 print(today)  
#             #進入指定日期charts和抓取page sou
#             chart_url = f"https://charts.spotify.com/charts/view/regional-tw-daily/{today}"
#             driver.get(chart_url)
            
#             # 生成 3 到 10 秒之間的隨機等待時間
#             random_wait_time = random.uniform(5, 12)
#             # 使用隨機等待時間創建 WebDriverWait 對象
#             wait = WebDriverWait(driver, random_wait_time)
#             tbody = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
#             # 找到所有包含 Spotify 链接的 a 元素
#             links = tbody.find_elements(By.CSS_SELECTOR, 'a[href^="https://open.spotify.com/"]')
#             # 找到 tbody 下的所有 tr 元素
#             rows = tbody.find_elements(By.TAG_NAME, 'tr')               
#             daily_data = []
#             for row in rows:
#                 # 處理每一行的數據
#                 daily_data.append(row.text)

#             daily_data_cleans = []
#             for data in daily_data:
#                 data = replace_re_entry(data)
#                 daily_data_split = split_for_one(data)
#                 daily_data_clean = remove_index(daily_data_split)
#                 Peak_Prev_Streak_Streams = get_pk(data)
#                 daily_data_clean.extend(Peak_Prev_Streak_Streams)#加入切完的統計資料
#                 daily_data_cleans.append(daily_data_clean)
#             # print(links)
#             check_track_id = ""
#             track_number = -1
#             element_number = -1
#             for link in links:
#                 href = link.get_attribute('href')
#                 text = link.text
#                 row = link.find_element(By.XPATH, './ancestor::tr')
#                 row_text = replace_re_entry(row.text)
#                 if href.split("/")[-2] == "track":
#                     is_track = True
#                 else :
#                     is_track = False
#                 track_id = href.split("/")[-1]
#                 element_number +=1
#                 if (is_track and track_id!= check_track_id):
#                     check_track_id = track_id
#                     track_number += 1
#                     element_number = 0
#                     daily_data_cleans[track_number].append(href.split("/")[-1])
#                 elif element_number == 2 :
#                     daily_data_cleans[track_number].append(href.split("/")[-1])

#             # 創建 DataFrame
#             df = pd.DataFrame(data=daily_data_cleans, columns=columns[1:])  # 先創建不包含 date 列的 DataFrame

#             # 添加 date 列並將其移到第一列
#             df['date'] = today
#             df = df[columns]  # 重新排序列
#             print(df.head())

#             # file保存路徑
#             file_path = f"/spotify_data/{current_date.strftime('%Y')}/{today}_spotify_chart_tw.csv"
#             df.to_csv(file_path, index=False, encoding='utf-8')
#             current_date += timedelta(days=1)  # 增加一天


#     # Task dependencies defined by calling the tasks in sequence
#     main()

# # Instantiate the DAG
# get_ranks_dag()
import os
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException
import random
import os
from utils.utils import get_driver, log_in

# Default arguments for the DAG
default_args = {
    "owner": "Admin",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
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

@dag(
    dag_id="get_ranks_dag",
    default_args=default_args,
    description="Demo for tir102 Spotify pipeline",
    schedule_interval="5 * * * *",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    tags=["spotify", "decorator"]
)
def get_ranks_dag():

    @task
    def fetch_data(start_date, end_date):
        driver = get_driver()
        try:
            log_in(driver)
        except InvalidSessionIdException:
            print("Session has already been terminated or invalid.")
        
        current_date = start_date
        columns = ["date", "rank", "track_name", "artist_names", "peak_rank", "previous_rank", "days_on_chart", "streams", "track_id", "artist_id"]

        while current_date <= end_date:
            today = current_date.strftime('%Y-%m-%d')
            print(f"Processing date: {today}")
            
            chart_url = f"https://charts.spotify.com/charts/view/regional-tw-daily/{today}"
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
            df['date'] = today
            df = df[columns]
            current_directory = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_directory, 'spotify_Data',current_date.strftime('%Y'))
            output_dir = f"{file_path}"
            os.makedirs(output_dir, exist_ok=True)
            file_path = f"{output_dir}{today}_spotify_chart_tw.csv"
            df.to_csv(file_path, index=False, encoding='utf-8')
            print(df.head())
            current_date += timedelta(days=1)
        
        driver.quit()
        return "Data fetch complete"

    fetch_data(datetime(2023, 9, 10), datetime(2023, 9, 13))

# Instantiate the DAG
get_ranks_dag()
