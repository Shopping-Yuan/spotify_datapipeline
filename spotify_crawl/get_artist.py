import requests
import pandas as pd
import time
from dags.utils import get_access_token

# 查詢歌手 Info
def get_artist_info(artist_name, access_token):
    url = f"https://api.spotify.com/v1/search?q={artist_name}&type=artist&limit=1"
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # 取得歌手 Info
        artists = data.get('artists', {}).get('items', [])
        artist_info = []
        for artist in artists:
            artist_id = artist.get("id")
            artist_genres = artist.get("genres")
            artist_followers = artist.get("followers", {}).get("total")
            artist_popularity = artist.get("popularity")
            artist_info.append((artist_id, artist_genres, artist_followers, artist_popularity))
        return artist_info
    
    elif response.status_code == 401:  # Unauthorized, possibly due to an expired token
        print("Access token expired. Refreshing token...")
        access_token = get_access_token()  # Get a new token
        return get_artist_info(artist_name, access_token)  # Retry with new token

    elif response.status_code == 429:  # Too Many Requests
        retry_after = int(response.headers.get("Retry-After", 1))
        print(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
        time.sleep(retry_after)
        return get_artist_info(artist_name, access_token)  # Retry after waiting


    else:
        print(f"Failed to get artist info {artist_name}. Status Code: {response.status_code}, Response: {response.text}")
        return []


# 主程式
def main():
    access_token = get_access_token()
    print(access_token)
    # 讀取 CSV
    spotify_chart_sample_data_path = r"/workspace/Spotify/artists_in_rank.csv"
    df = pd.read_csv(spotify_chart_sample_data_path)
      
    # 將查詢結果存入 DataFrame
    result_data = []

    for index, row in df.iterrows():
        artist = row['artist']

        try:
            artist_infos = get_artist_info(artist, access_token)

            if artist_infos:
                for artist_id, artist_genre, artist_followers, artist_popularity in artist_infos:  # 歌手 info
                    result_data.append({"Artist_name": artist, 
                                        "Artist_id": artist_id, 
                                        "Artist_genres": artist_genre, 
                                        "Artist_followers": artist_followers, 
                                        "Artist_popularity": artist_popularity})
            else:
                print(f"No info available for artist {artist}.")

        except Exception as e:
            print(f"Error processing artist {artist}: {e}")

    # 將查詢結果存入 DataFrame
    result_df = pd.DataFrame(result_data)

    # 保存結果到 CSV
    result_csv_path = r"/workspace/Spotify/artist_info.csv"
    result_df.to_csv(result_csv_path, index=False)
    

if __name__ == "__main__":
    main()