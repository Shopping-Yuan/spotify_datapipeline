import pandas as pd
import time
import random
import glob
import os
import itertools
from datetime import datetime
from utils.utils import get_access_token,to_traditional
from urllib3.exceptions import MaxRetryError
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


strat_date = datetime.strptime("2022-07-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-07-31", "%Y-%m-%d")
path = r'spotify_Data' # use your path

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
    df_artist.to_csv(r"/workspace/Spotify/artists_in_rank_"+f"{strat_date.date()}_to_{end_date.date()}.csv",index = False)
            
# Default arguments for the DAG
default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'email': ['shopping0789604@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Define the DAG
dag = DAG(
    dag_id='get_tracks_dag',
    default_args=default_args,
    description='Get tracks from spotify.',
    schedule="0 12 * * *",
    start_date=datetime(2024, 9, 1),
    catchup=False
)


# Define the tasks
track_task_obj = PythonOperator(
    task_id='track',
    python_callable=track_main,
    dag=dag,
)


track_task_obj