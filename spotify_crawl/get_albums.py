import requests
import pandas as pd
import time
from dags.utils import get_access_token


# 查詢歌手專輯
def get_artist_album_id(artist_id, access_token):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?market=TW&limit=50"
    # url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?market=TW&limit=50&include_groups=album,single,compilation"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()

        # 取得專輯 info
        albums = data.get('items', [])
        artist_album_info = []
        for album in albums:
            album_id = album.get("id")  # 取得專輯 ID
            album_name = album.get("name")  # 取得專輯名稱
            album_tracks = album.get("total_tracks")  # 取得專輯歌曲數
            album_date = album.get("release_date") # 取得專輯發行日
            artist_album_info.append((album_id, album_name, album_tracks, album_date))  # 將 專輯 info 加到 list

        return artist_album_info
    
    elif response.status_code == 401:  # Unauthorized, possibly due to an expired token
        print("Access token expired. Refreshing token...")
        access_token = get_access_token()  # Get a new token
        return get_artist_album_id(artist_id, access_token)  # Retry with new token

    elif response.status_code == 429:  # Too Many Requests
        retry_after = int(response.headers.get("Retry-After", 1))
        print(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
        time.sleep(retry_after)
        return get_artist_album_id(artist_id, access_token)  # Retry after waiting

    else:
        print(f"Failed to get artist_album_info {artist_id}. Status Code: {response.status_code}, Response: {response.text}")
        return []


# 主程式
def main():
    access_token = get_access_token()

    # 讀取 CSV
    artist_info_path = r"/workspace/Spotify/Artist/artist_info.csv"
    df = pd.read_csv(artist_info_path)

    # 將查詢結果存入 DataFrame
    result_data = []
    
    for index, row in df.iterrows():
        artist_id = row["Artist_id"]
        artist_album_ids = get_artist_album_id(artist_id, access_token)

        if artist_album_ids:
            for album_id, album_name, album_tracks, album_date in artist_album_ids:  # 專輯 info
                result_data.append({"Artist_id": artist_id,
                                    "Artist_album_id": album_id,
                                    "Artist_album_name": album_name,
                                    "Album_tracks": album_tracks,
                                    "Album_release_date": album_date})
        else:
            print(f"Artist ID {artist_id} has no album.")
            
    result_df = pd.DataFrame(result_data)

    # 保存結果到 CSV
    result_csv_path = r"/workspace/Spotify/Album/album_info.csv"
    result_df.to_csv(result_csv_path, index=False)
    

if __name__ == "__main__":
    main()