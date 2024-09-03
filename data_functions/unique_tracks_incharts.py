import pandas as pd
import glob
import os
import itertools
from datetime import datetime
from utils import to_traditional

strat_date = datetime.strptime("2022-07-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-07-31", "%Y-%m-%d")
path = r'/workspace/Spotify/Rank/' # use your path
def list_subfolders(path):
    all_entries = os.listdir(path)
    subfolders = [os.path.join(path, entry) for entry in all_entries if os.path.isdir(os.path.join(path, entry))]
    return subfolders
subfolders = list_subfolders(path)
all_files = []
for folder_path in subfolders:
    file_paths = glob.glob(os.path.join(folder_path, "*.csv"))
    for file_path in file_paths:
        date_str = file_path.split("/")[-1].split("_")[0]
        date = datetime.strptime(date_str, "%Y-%m-%d")
        if date>= strat_date and date <= end_date:
            all_files.append(file_path)
track_id_list = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    if len(df["track_id"])!=200:
        print(filename,len(df["track_id"]))
    if len(df[df["track_id"].isna()]):
        print(filename)
    track_id_list.append(df)

df_track = pd.concat(track_id_list)
df_track = df_track.drop_duplicates(subset=['track_id'])
df_track = df_track[["track_name","track_id"]]
df_track = df_track.dropna()
df_track["track_name"] = df_track["track_name"].apply(to_traditional)
df_track  = df_track.rename(columns={'track': 'track_name'})
print(df_track.head(),len(df_track))
df_track.to_csv(r"/workspace/Spotify/tracks_in_rank_"+f"{strat_date.date()}_to_{end_date.date()}.csv",index = False)