import pandas as pd
import glob
import os
import itertools
from datetime import datetime
from utils import to_traditional
strat_date = datetime.strptime("2022-07-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-07-31", "%Y-%m-%d")
path = r'/workspace/Spotify/Rank' # use your path
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
artist_name_list = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    artist_name_list.extend(df["artist_names"].tolist())
artist_name_list = list(itertools.chain.from_iterable([x.split(",") for x in artist_name_list]))
artist_name_list = [x.strip() for x in artist_name_list if len(x.strip())>0]

df1 = pd.DataFrame(list(set(artist_name_list)),columns=["Artist_name"])
df1["Artist_name"] = df1["Artist_name"].apply(to_traditional)
df2 = pd.read_csv("/workspace/Spotify/Artist/artist_info.csv", index_col=None, header=0)[["Artist_name","Artist_id"]]
df_artist = df1.merge(df2, on="Artist_name")
df_artist  = df_artist .rename(columns={'Artist_name': 'artist', 'Artist_id': 'artist_id'})
print(df_artist.head())
df_artist.to_csv(r"/workspace/Spotify/artists_in_rank_"+f"{strat_date.date()}_to_{end_date.date()}.csv",index = False)