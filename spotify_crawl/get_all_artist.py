import pandas as pd
import glob
import os
import itertools

path = r'/workspace/Spotify/Rank' # use your path
def list_subfolders(path):
    all_entries = os.listdir(path)
    subfolders = [os.path.join(path, entry) for entry in all_entries if os.path.isdir(os.path.join(path, entry))]
    return subfolders
subfolders = list_subfolders(path)
all_files = []
for folder_path in subfolders:
    all_files += glob.glob(os.path.join(folder_path, "*.csv"))
artist_list = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    artist_list.extend(df["artist_names"].tolist())
artist_list = list(itertools.chain.from_iterable([x.split(",") for x in artist_list]))
print(len(set(artist_list)))
df = pd.Series(list(set(artist_list)),name="artist")
df.to_csv(r"/workspace/Spotify/artists_in_rank.csv")
# frame = pd.concat(artist_list, axis=0, ignore_index=True)
