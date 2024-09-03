import os
import csv
import time
from dags.utils import get_spotify_client


def refresh_token(spotify_client):
    spotify_client.client_credentials_manager.get_access_token(force_refresh=True)

def get_audio_analysis(spotify_client, track_id):
    try:
        return spotify_client.audio_analysis(track_id)
    except:
        return None

def process_tracks(input_file, output_file,start_index=0,end_index = None):
    spotify_client = get_spotify_client()
    last_refresh_time = time.time()
    if os.path.exists(output_file) == False:
        open(output_file, 'w').close()
    with open(input_file, 'r', encoding="utf-8") as infile, open(output_file, 'a', newline='', encoding="utf-8") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
    
        # 寫入標題row
        header = next(reader)
        header.extend(['End_of_fade_in', 'Start_of_fade_out', 'Loudness', 'Tempo', 'Tempo Confidence'])
        writer.writerow(header)

        for index,row in enumerate(reader):
            if index < start_index:
                continue
            if end_index is not None and index == end_index:
                break
            # 檢查是否需要更新 token（每小時更新一次）
            current_time = time.time()
            if current_time - last_refresh_time > 3600:
                refresh_token(spotify_client)
                last_refresh_time = current_time

            track_id = row[1]  # 假設 track ID 在第二個column
            # print(track_id)
            analysis = get_audio_analysis(spotify_client, track_id)

            if analysis:
                end_of_fade_in = analysis['track']['end_of_fade_in']
                start_of_fade_out = analysis['track']['start_of_fade_out']
                loudness = analysis['track']['loudness']
                tempo = analysis['track']['tempo']
                tempo_confidence = analysis['track']['tempo_confidence']
                row.extend([end_of_fade_in, start_of_fade_out, loudness, tempo, tempo_confidence])
            else:
                row.extend([None, None, None, None, None])

            writer.writerow(row)

            # 為了避免超過 API 速率限制，在每次請求後暫停一下
            time.sleep(0.5)

if __name__ == "__main__":
    input_file = r'/workspace/Spotify/tracks_in_rank_2022-07-01_to_2024-07-31.csv'
    output_file = r'/workspace/Spotify/Analysis/track_in_rank_audio_analysis.csv'
    process_tracks(input_file, output_file,2000,None)