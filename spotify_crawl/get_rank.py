from datetime import datetime, timedelta

# 起始和结束日期
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException
import pandas as pd
import random
from Utils.utils import get_driver,log_in
def replace_re_entry(data):
    return data.replace("Re-\nEnt\nry","0")
def split_for_one(data):
#split for one
  daily_data_split = data.split("\n")
  return daily_data_split
            
#remove index 1 and 4
def remove_index(daily_data_split,rmindex = [1,4]):
  daily_data_clean = []
  for i, data in enumerate(daily_data_split):
      if i not in rmindex:
          daily_data_clean.append(data)
  return daily_data_clean

def get_pk(data):
    #(統計資料)peak, previous rank, streak and streams
    Peak_Prev_Streak_Streams = data.split("\n")[4].split(" ")
    return Peak_Prev_Streak_Streams  

def main():
    driver = get_driver()
    try:
        log_in(driver)
    except InvalidSessionIdException:
        print("Session has already been terminated or invalid.")


    start_date = datetime(2023, 9, 10)
    end_date = datetime(2023, 9, 13)

    # 当前日期初始化为起始日期
    current_date = start_date

    # 使用while循环遍历日期范围

    # 定義列名
    columns = ["date", "rank", "track_name", "artist_names", "peak_rank", "previous_rank", "days_on_chart", "streams","track_id","artist_id"]   
    
    while current_date <= end_date:
                # 獲取當前日期
                # today = date.today().isoformat()
                today = current_date.strftime('%Y-%m-%d')
                if current_date.strftime('%d') == end_date.strftime('%d'):
                    print(today)  # 打印当前日期
                #進入指定日期charts和抓取page sou
                chart_url = f"https://charts.spotify.com/charts/view/regional-tw-daily/{today}"
                driver.get(chart_url)
                
                # 生成 3 到 10 秒之間的隨機等待時間
                random_wait_time = random.uniform(5, 12)
                # 使用隨機等待時間創建 WebDriverWait 對象
                wait = WebDriverWait(driver, random_wait_time)
                tbody = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
                # 找到所有包含 Spotify 链接的 a 元素
                links = tbody.find_elements(By.CSS_SELECTOR, 'a[href^="https://open.spotify.com/"]')
                # 找到 tbody 下的所有 tr 元素
                rows = tbody.find_elements(By.TAG_NAME, 'tr')               
                daily_data = []
                for row in rows:
                    # 處理每一行的數據
                    daily_data.append(row.text)
 
                daily_data_cleans = []
                for data in daily_data:
                    data = replace_re_entry(data)
                    daily_data_split = split_for_one(data)
                    daily_data_clean = remove_index(daily_data_split)
                    Peak_Prev_Streak_Streams = get_pk(data)
                    daily_data_clean.extend(Peak_Prev_Streak_Streams)#加入切完的統計資料
                    daily_data_cleans.append(daily_data_clean)
                # print(links)
                check_track_id = ""
                track_number = -1
                element_number = -1
                for link in links:
                    href = link.get_attribute('href')
                    text = link.text
                    row = link.find_element(By.XPATH, './ancestor::tr')
                    row_text = replace_re_entry(row.text)
                    if href.split("/")[-2] == "track":
                        is_track = True
                    else :
                        is_track = False
                    track_id = href.split("/")[-1]
                    element_number +=1
                    if (is_track and track_id!= check_track_id):
                        check_track_id = track_id
                        track_number += 1
                        element_number = 0
                        daily_data_cleans[track_number].append(href.split("/")[-1])
                    elif element_number == 2 :
                        daily_data_cleans[track_number].append(href.split("/")[-1])

                    


                    # print(f"Href: {href}")
                    # print(f"Link text: {text}")
                    # print(f"Row text: {row_text}")
                    # print(f"Row text: {row_text.split("\n")[2]}")
                    # print("---") 

                # 創建 DataFrame
                df = pd.DataFrame(data=daily_data_cleans, columns=columns[1:])  # 先創建不包含 date 列的 DataFrame

                # 添加 date 列並將其移到第一列
                df['date'] = today
                df = df[columns]  # 重新排序列

                # 設置顯示選項
                # pd.set_option('display.max_columns', None)  # 顯示所有列
                # pd.set_option('display.width', None)  # 自動調整寬度
                # pd.set_option('display.max_colwidth', None)  # 顯示完整的列內容

                # 顯示前幾行，使用更好的格式
                # print(df.head().to_string(index=False))
                

                # file保存路徑
                file_path = f'/workspace/Spotify/Rank/{current_date.strftime('%Y')}/{today}_spotify_chart_tw.csv'
                df.to_csv(file_path, index=False, encoding='utf-8')
                current_date += timedelta(days=1)  # 增加一天

                # 將table存成csv
    driver.close()

if __name__ == "__main__":
    main()

# /workspace/Spotify/Rank/2023/2023-08-19_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-08-27_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-01_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-05_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-08_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-10_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-11_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-09-13_spotify_chart_tw.csv
# /workspace/Spotify/Rank/2023/2023-11-01_spotify_chart_tw.csv