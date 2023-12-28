import pypyodbc as odbc
import sqlalchemy
import pandas as pd
from dotenv import load_dotenv
import os
# imort functions to extract dat from Spotify API
from extract_transform_data import extract_artists_followers_table, extract_artists_popularity_table, extract_albums_popularity_table, extract_tracks_popularity_table


# connect to database
# specify server and DB name
server = "spotifyrockdb.database.windows.net"
database = "SpotifyRockDB"
# load credentials
load_dotenv()
username = os.getenv("username")
password = os.getenv("password")
# set connection string
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:spotifyrockdb.database.windows.net,1433;Database=SpotifyRockDB;Uid=sqladmin;Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=1500;'


# Using SQLAlcehmy engine to load data with pandas
engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
# get artist_ids, album_ids, track_ids lists
query = f'SELECT artist_id FROM artists_table'
artist_ids = pd.read_sql(query, engine)['artist_id'].to_list() 

query = f'SELECT album_id FROM albums_table'
album_ids = pd.read_sql(query, engine)['album_id'].to_list() 

query = f'SELECT track_id FROM tracks_table'
track_ids = pd.read_sql(query, engine)['track_id'].to_list() 

# Load into DB
df_artists_followers_table = extract_artists_followers_table(artist_ids=artist_ids)
df_artists_followers_table.to_sql('artists_followers_table', con=engine, if_exists='append', index=False)

df_artists_popularity_table = extract_artists_popularity_table(artist_ids=artist_ids)
df_artists_popularity_table.to_sql('artists_popularity_table', con=engine, if_exists='append', index=False)

df_albums_popularity_table = extract_albums_popularity_table(album_ids=album_ids)
df_albums_popularity_table.to_sql('albums_popularity_table', con=engine, if_exists='append', index=False)

df_tracks_popularity_table = extract_tracks_popularity_table(track_ids=track_ids)
df_tracks_popularity_table.to_sql('tracks_popularity_table', con=engine, if_exists='append', index=False)

# close SQLAlchemy engine
engine.dispose()



