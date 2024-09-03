import os
import csv
import time
from datetime import datetime, timedelta
from dags.utils import get_spotify_client

def process_tracks(input_file, output_file,start_index=0,end_index=None):
    sp = get_spotify_client()
    last_token_refresh = datetime.now()
    if os.path.exists(output_file) == False:
        open(output_file, 'w').close()

    with open(input_file, 'r', encoding="utf-8") as infile, open(output_file, 'a', newline='', encoding="utf-8") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # 寫入標題row
        header = next(reader)
        header.extend(['Acousticness','Danceability', 'Energy', 'Instrumentalness', 'Key', 'Liveness', 'Loudness', 'Mode', 'Speechiness', 'Tempo', 'Time_signature', 'Valence'])
        writer.writerow(header)

        for index,row in enumerate(reader):
            if index < start_index:
                continue
            if end_index is not None and index == end_index:
                break
        # 檢查是否需要刷新令牌
            if datetime.now() - last_token_refresh > timedelta(hours=1):
                sp = get_spotify_client()
                last_token_refresh = datetime.now()
                print("Token refreshed at:", last_token_refresh)

            track_id = row[1]  # 假設track ID在第二column
            try:
                audio_features = sp.audio_features(track_id)[0]
                if audio_features:
                    new_row = row + [
                        audio_features['acousticness'],
                        audio_features['danceability'],
                        audio_features['energy'],
                        audio_features['instrumentalness'],
                        audio_features['key'],
                        audio_features['liveness'],
                        audio_features['loudness'],
                        audio_features['mode'],
                        audio_features['speechiness'],
                        audio_features['tempo'],
                        audio_features['time_signature'],
                        audio_features['valence'],
                    ]
                    writer.writerow(new_row)
                else:
                    print(f"No audio features found for track ID: {track_id}")
            except Exception as e:
                print(f"Error processing track ID {track_id}: {str(e)}")

            # 添加一個小延遲以避免超過API速率限制
            time.sleep(0.5)

if __name__ == "__main__":
    input_file = r'/workspace/Spotify/tracks_in_rank_2022-07-01_to_2024-07-31.csv'
    output_file = r'/workspace/Spotify/Feature/track_in_rank_audio_features.csv'
    process_tracks(input_file, output_file,0,None)