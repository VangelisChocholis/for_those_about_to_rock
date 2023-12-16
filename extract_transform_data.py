import pandas as pd
from datetime import date
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import re
import time
from datetime import datetime
from dotenv import load_dotenv
import os



# access Spotify
def get_spotify_client():
    """Creates a Spotify client object using the provided client_id and client_secret.

    Returns:
        spotipy.Spotify: Spotify client object.
    """
    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp


# artists_table
def extract_artists_table(artists_list):
    """Takes an artist list as an input and extracts data from Spotify API
    Returns a pandas DataFrame, containing the data in tabular form.

    Args:
        artists_list (list): A list of artist names.

    Returns:
        pandas.DataFrame: dataframe containing artist id, followers, and name. 
    """
    # acces spotipy
    sp = get_spotify_client()
    
    artist_id_list = []
    artist_name_list = []
    artist_followers_list = []
    for artist in artists_list:
        try:
            # Search for artist
            results = sp.search(q=artist, type='artist')
            # Get artist ID, name, followers 
            artist_id_list.append(results['artists']['items'][0]['id'])
            artist_name_list.append(results['artists']['items'][0]['name'])
            artist_followers_list.append(results['artists']['items'][0]['followers']['total'])
        except Exception as e:
            print(f'Error in data extraction for artist \'{artist}\': {e}')
    # make DataFrame
    artists_table = pd.DataFrame(data={'artist_id': artist_id_list,
                                      'artist_name': artist_name_list,
                                      'artist_followers': artist_followers_list})
    return artists_table


# artists_popularity_table
def extract_artists_popularity_table(artists_list):
    """Takes an artist list as an input and extracts artist popularity from Spotify API
    Returns a pandas DataFrame, containing artist popularity and the current date.

    Args:
        artists_list (list): A list of artist names.

    Returns:
        pandas.DataFrame: dataframe containing artist id, popularity, and the current date.
    """
    # acces spotipy
    sp = get_spotify_client()
    
    artist_id_list = []
    artist_name_list = []
    artist_popularity_list = []
    for artist in artists_list:
        try:
            # Search for artist
            results = sp.search(q=artist, type='artist')
            # Get artist ID, name, followers 
            artist_id_list.append(results['artists']['items'][0]['id'])
            artist_name_list.append(results['artists']['items'][0]['name'])
            artist_popularity_list.append(results['artists']['items'][0]['popularity'])
        except Exception as e:
            print(f'Error in data extraction for artist \'{artist}\': {e}')
    # make DataFrame
    artists_popularity_table = pd.DataFrame(data={'date': date.today(), 
                                       'artist_id': artist_id_list, 
                                       'artist_name': artist_name_list, 
                                       'artist_popularity': artist_popularity_list}
                                )
    return artists_popularity_table


# albums_table initial form, without proper album selection
def extract_albums_table(artist_id_list):
    """Extracting all albums of the artists contained in artist_id_list

    Args:
        artist_id_list (list): List of artist ids

    Returns:
        pandas.DataFrame: every album of corresponging to the given artist ids
    """
    # acces spotipy
    sp = get_spotify_client()
    
    albums_table = pd.DataFrame()
    for artist_id in artist_id_list:
        try:
            # extract album data with Spotify API
            offset = 0
            limit = 50
            albums = sp.artist_albums(artist_id, album_type='album', limit=limit, offset=offset) 
            # If there are more albums, use offset to get the next set
            while len(albums['items']) == limit:
                offset += limit
                additional_albums = sp.artist_albums(artist_id, album_type='album', limit=limit, offset=offset)
                albums['items'].extend(additional_albums['items'])
            #artist_id = artist_id
            album_id_list = []
            album_name_list = []
            album_release_date_list = []
            album_total_tracks_list = []
            album_image_large_list = []
            album_image_medium_list = []
            album_image_small_list = []
            
            for album in albums['items']:
                album_id_list.append(album['id'])
                album_name_list.append(album['name'])
                album_release_date_list.append(album['release_date'])
                album_total_tracks_list.append(album['total_tracks'])
                album_image_large_list.append(album['images'][0]['url'])
                album_image_medium_list.append(album['images'][1]['url'])
                album_image_small_list.append(album['images'][2]['url'])
                           
            albums_df = pd.DataFrame(data={'album_id': album_id_list,
                                         'artist_id': artist_id,
                                         'album_name': album_name_list,
                                         'album_release_date': album_release_date_list,
                                         'album_total_tracks': album_total_tracks_list,
                                         'album_image_large': album_image_large_list,
                                         'album_image_medium': album_image_medium_list,
                                         'album_image_small': album_image_small_list}
                                   )
            albums_table = pd.concat([albums_table, albums_df], ignore_index=True)
            
        except Exception as e:
            print(f'Error in data extraction for artist_id \'{artist_id}\': {e}')
    '''# Check artists with 50 albums. We must use offset for those, because the maximum limit is 50
    group = albums_table.groupby(by='artist_id')['album_id'].count().reset_index()
    group = group[group['album_id']==50]
    art_id_list = group['artist_id'].to_list()'''
    # realease date keep year format
    albums_table['album_release_date'] = pd.to_datetime(albums_table['album_release_date'], format='ISO8601').dt.year
    return albums_table


# removing live, remix, deluxe and demo albums, without dropping any original album in the process
def album_selection_vol1(df):
    """This function takes albums_table as input and excludes live, demo, and remix albums.
    Additionally, the function ensures that it doesn't drop any albums
    containing 'live,' 'demo,' or 'remix' in their actual names. 
    For example, it won't exclude albums with names like
    'The Demon Album,' 'Delivered,' or 'Remixed Feelings.

    Args:
        df (pandas.DataFrame): This is the albums_table 
        given by extract_albums_table function

    Returns:
        pandas.DataFrame: The albums_table without live, demo and remix albums
    """

    # Function to check the conditions
    def check_album_conditions(string, pattern):
        """This function detects albums that contain "live" in their original names

        Args:
            string (string): album name to check

        Returns:
            bool: True if the condition is met
        """
        # Regular expression pattern
        #pattern1 = r'(.*live[a-z].*|.*[a-z]live.*)|(.*demo[a-z].*|.*[a-z]demo.*)|(.*remix[a-z].*|.*[a-z]remix.*)'
        return bool(re.search(pattern, string, re.IGNORECASE) )  #and not bool(re.match(pattern2, string, re.IGNORECASE))



    # album names containing string "live", "demo" or "remix"
    df_cont = df[(df['album_name'].str.contains('live', case=False)) |
                      (df['album_name'].str.contains('demo', case=False))|
                      (df['album_name'].str.contains('deluxe', case=False))|
                      (df['album_name'].str.contains('remix', case=False))].copy()
    # album names without the strings
    df_to_keep = df[~(
        (df['album_name'].str.contains('live', case=False)) |
                      (df['album_name'].str.contains('demo', case=False))|
                      (df['album_name'].str.contains('deluxe', case=False))|
                      (df['album_name'].str.contains('remix', case=False))
                      )].copy()
    
    # Apply the function check_album_conditions to the 'album_name' column
    pattern = r'(.*live[a-z].*|.*[a-z]live.*)|(.*demo[a-z].*|.*[a-z]demo.*)|(.*remix[a-z].*|.*[a-z]remix.*)|(.*deluxe[a-z].*|.*[a-z]deluxe.*)'
    df_cont['matches_condition'] = df_cont['album_name'].apply(check_album_conditions, pattern=pattern)
    
    # keep only albums that contain 'live', 'demo' , 'deluxe' or 'remix' strings in their original name
    df_cont = df_cont[df_cont['matches_condition']==True].drop(columns=['matches_condition'], axis=1)
    
    # concat to get albums
    df_alb = pd.concat([df_to_keep, df_cont], axis=0)
    
    # also we check for 'demos', bcs some albums include it, and we did not remove those yet
    # again, we want to keep albums that contain 'demos' string as a part of their name
    # album_names containing string "demos"
    df_cont = df_alb[df_alb['album_name'].str.contains('demos', case=False)].copy()
    
    # albums without the string
    df_to_keep = df_alb[~df_alb['album_name'].str.contains('demos', case=False)].copy()
    
    # apply the funtion check_album_conditions
    pattern =  r'\bdemos\b(?![\w\'()])' # we want NOT to match with this condition
    df_cont['matches_condition'] = df_cont['album_name'].apply(check_album_conditions, pattern=pattern)
    
    # keep only albums that contain 'demos' in their original name, remember we want NOT to match, thus we set False
    df_cont = df_cont[df_cont['matches_condition']==False].drop(columns=['matches_condition'], axis=1)
    # concat to get albums
    df_alb = pd.concat([df_to_keep, df_cont], axis=0)
    
    return df_alb


# select only the most popular version for each album
def album_selection_vol2(albums_table, artists_table):
    """This function removes the non original albums. Calls album_selection_vol1 to 
    remove live, demo, deluxe and remix albums. Then for the remaining albums, keeps album versions
    with the highest album popularity.

    Args:
        albums_table (pd.DataFrame): albums_table as given by extract_albums_table function
        artists_table (pd.DataFrame): We also include this table to have a better inspection of the data transformatin process.
        
    Returns:
        pandas.DataFrame: A DataFrame containing only the original albums from each artist
    """
    
    albums_artists = albums_table.merge(right=artists_table, on='artist_id')

    # we create new_album_name column with the the original album name
    filtered_albums = album_selection_vol1(albums_artists)
    
    # we filter albums with tracks less than 50. Albums above 50 are not original albums. 
    filtered_albums = filtered_albums[filtered_albums['album_total_tracks']<50]
    # we want to get only the original albums.
    # there are for example "remaster" and "deluxe" versions of the same album
    #  We will keep the version with the highest album popularity 
    # Function to get album popularity information
    def get_album_info(artist_name, album_id):
        # acces spotipy
        sp = get_spotify_client()
        # get data
        try:
            album = sp.album(album_id)
            album_name = album['name']
            album_popularity = album['popularity']
            album_release_date = album['release_date']
            return pd.DataFrame({'artist_name': [artist_name], 'album_id': [album_id], 
                'album_name': [album_name], 'album_release_date': [album_release_date], 'album_popularity': [album_popularity]})
        except Exception as e:
            print("Exception raised for artist_id: {artist_name}, album_id: {album_id}: {e}")
        
    
    # get album popularity to select the most popular version of each ablum
    df_artist_album_pop = pd.DataFrame(columns=['artist_name', 'album_id', 'album_name', 'album_release_date', 'album_popularity'])
    for artist_name in filtered_albums['artist_name'].unique():
        for album_id in filtered_albums[filtered_albums['artist_name']==artist_name]['album_id']: 
            df_new = get_album_info(artist_name, album_id)
            df_artist_album_pop = pd.concat([df_artist_album_pop, df_new], axis=0, ignore_index=True)
        
    # we create new_album_name with the the actual album name
    df_albums_names_pop = df_artist_album_pop['album_name'].apply(lambda x: 
                                            str(
                                                x.split('(')[0].strip()
                                            )
                                        ).reset_index().drop('index', axis=1).rename({'album_name':
                                                                                        'new_album_name'},
                                                                                        axis=1)
    df_albums_names_pop = pd.concat((df_albums_names_pop,df_artist_album_pop), axis=1)   
    
    # concat release date next to album name. There are albums with the same name ,
    # e.g there are two "Fleetwood Mac" albums, released in different years
    df_albums_names_pop['album_release_date'] = pd.to_datetime(df_albums_names_pop['album_release_date'], format='ISO8601').dt.year
    df_albums_names_pop['new_album_name'] = df_albums_names_pop['new_album_name'] \
        + ' ' + '(' + df_albums_names_pop['album_release_date'].astype('string') + ')'
        
    # moving on with album selection
    df_albums_names_pop['album_popularity'] = pd.to_numeric(df_albums_names_pop['album_popularity'],
                                                            errors='coerce')
    
    # Use groupby and idxmax to get the row indices with the maximum popularity for each album name
    max_pop_indices = df_albums_names_pop.groupby('new_album_name')['album_popularity'].idxmax()

    # Use the obtained indices to extract the corresponding album_id
    album_ids = df_albums_names_pop.loc[max_pop_indices, 'album_id'].tolist()  
    
    # select the most popular album version
    df_albums = (filtered_albums[filtered_albums['album_id'].isin(album_ids)]
                 .drop(columns=['artist_name', 'artist_followers'])
    )
    # column with original album name
    df_albums['original_album_name'] = df_albums['album_name'].apply(lambda x: 
                                            str(
                                                x.split('(')[0].strip()
                                            )
                                        )
    
    return df_albums


# extract tracks VOL1
def extract_tracks_data(album_ids):
    """This function extracts data for every track from every album
    
    Args:
        album_ids (list or pandas.Series): A list of album IDs

    Returns:
        pandas.DataFrame: dataframe containing the tracks in each album
    """
    # acces spotipy
    sp = get_spotify_client()
    
    # basic information
    track_id_list = []
    album_id_list = []
    track_name_list = []
    track_duration_list = []
    track_spotify_url_list = []
    track_preview_url_list = []
    
    
    # Loop through each album ID
    for album_id in album_ids:
        try:
            # Get album tracks
            album_tracks = sp.album_tracks(album_id)
        
            # Loop through each track in the album
            for track in album_tracks['items']:
                track_id = track['id']
                track_name = track['name']
                track_duration_ms = track['duration_ms']
                track_spotify_url = track['external_urls']['spotify']
                track_preview_url = track['preview_url']
                
                # Append data to lists
                track_id_list.append(track_id)
                album_id_list.append(album_id)
                track_name_list.append(track_name)
                track_duration_list.append(track_duration_ms)
                track_spotify_url_list.append(track_spotify_url)
                track_preview_url_list.append(track_preview_url)
        except Exception as e:
            print(f'Exception raised: {e}')
        
            # stop code for 30 seconds to avoid time limit
            #time.sleep(10)
            
    # Create a DataFrame
        data = {
        'track_id': track_id_list,
        'album_id': album_id_list,
        'track_name': track_name_list,
        'track_duration_ms': track_duration_list,
        'track_spotify_url': track_spotify_url_list,
        'track_preview_url': track_preview_url_list
        }

    tracks_df = pd.DataFrame(data)
    return tracks_df


# get track popularity
def extract_track_popularity(track_ids):
    """ This function extracts track popularity given a list of track IDs
    Args:
        track_ids (list): A list of tracks IDs

    Returns:
        pandas.DataFrame: A dataframe containing track popularity and the current date
    """
    # acces spotipy
    sp = get_spotify_client()
    
    df_track_pop = pd.DataFrame()
    request_cnt = 0
    # we can use a maximum of 50 track ids each time
    for i in range(0,len(track_ids),50):
        request_cnt += 1
        # get 50 tracks in each iteration
        print(f"i = {i}, request_cnt = {request_cnt}")
        try:
            track_info = sp.tracks(track_ids[i:i+50])
            # make sure that there is not None
            #track_info = [item for item in track_info if item is not None]
            # create temporary datafame
            df_temp = pd.DataFrame(track_info['tracks'])[['id', 'popularity']]
            df_track_pop = pd.concat((df_track_pop, df_temp), axis=0)
        except Exception as e:
            print(e)
        # sleep for 5 seconds for every 10 requests
        if request_cnt % 10 == 0:
            time.sleep(5)
    df_track_pop['date'] = datetime.now().date()
    df_track_pop = df_track_pop.rename({'id': 'track_id',
                                        'popularity': 'track_popularity'}, axis=1)
    return df_track_pop


# extract acoustic features
def extract_tracks_acoustic_features(track_ids):
    """This function extracts acoustic features data for every track
    
    Args:
        track_ids (list or pandas.Series): A list of track IDs

    Returns:
        pandas.DataFrame: dataframe containing acoustic features for each track
    """
    # acces spotipy
    sp = get_spotify_client()
    
    df = pd.DataFrame()
    request_cnt = 0
    # we can use a maximum of 100 track ids each time
    for i in range(0,len(track_ids),100):
        request_cnt += 1
        # get 100 tracks in each iteration
        print(f"i = {i}, request_cnt = {request_cnt}")
        try:
            track_features = sp.audio_features(track_ids[i:i+100])
            # be sure that there is not None response
            track_features = [item for item in track_features if item is not None]
            # store in temporary DataFrame
            df_temp = pd.DataFrame(track_features)
            # concat
            df = pd.concat((df, df_temp), axis=0)
        except Exception as e:
            print(e)
        # sleep for 5 seconds for every 10 requests
        if request_cnt % 10 == 0:
            time.sleep(5)
    df = df.rename({'id': 'track_id'}, axis=1)
    return df
    





# Testing code
artists_list = ['Pink Floyd', 'The Doors', 'Led Zeppelin', 'Queen', 'Deep Purple',
               'The Rolling Stones', 'AC/DC', 'Fleetwood Mac', 'The Beatles', 'Dire Straits', 'Nirvana',
               'Guns N\'Roses', 'Pixies', 'The Police', 'ZZ Top', 'Aerosmith', 'The Who', 
               'Bon Jovi', 'Lynyrd Skynyrd', 'Scorpions', 'U2', 'David Bowie',
                'Jimi Hendrix', 'Eric Clapton', 'Red Hot Chili Peppers']




# extract acoustic features
'''tracks_table = pd.read_csv('tracks_data_vol1.csv')
track_ids = tracks_table['track_id'].to_list()
df = extract_tracks_acoustic_features(track_ids)
print(df.shape[0], len(track_ids))
df.to_csv("acoustic_features_table.csv", index=False)
print(df)'''



# Testing track data extraction
'''albums_table = pd.read_csv('albums_table_not_cleaned.csv')
album_ids = albums_table['album_id'].to_list()
df = extract_tracks_data(album_ids)
print(df)
df.to_csv("tracks_data_vol1.csv", index=False)'''


'''albums_table = pd.read_csv('albums_table_not_cleaned.csv')
album_ids = albums_table['album_id'].to_list()
df = extract_tracks_data(album_ids)
print(df.head())
df.to_csv('tracks_table.csv', index=False)'''



# creating albums_table_not_cleaned.csv
'''artists_table = extract_artists_table(artists_list)
artist_id_list = extract_artists_table(artists_list)['artist_id'].to_list()
albums_table1 = album_selection_vol1(extract_albums_table(artist_id_list))
albums_table2 = album_selection_vol2(albums_table1, artists_table)
print(albums_table2.shape)
print(albums_table2['original_album_name'].nunique())
albums_table2.to_csv('albums_table_not_cleaned.csv', index=False)'''



# testing album image
'''artists_table = extract_artists_table(artists_list)
artist_id_list = extract_artists_table(artists_list)['artist_id'].to_list()[0:5]
df = extract_albums_table(artist_id_list=artist_id_list)
print(df.columns)
print(df['album_image_large'][0])'''



# Testimg ablum_selection_vol1 and ablum_selection_vol2
'''artists_table = extract_artists_table(artists_list)
artist_id_list = extract_artists_table(artists_list)['artist_id'].to_list()
albums_table = album_selection_vol1(extract_albums_table(artist_id_list))

df_albums = album_selection_vol1(albums_table)
df_albums_2 = album_selection_vol2(df_albums, artists_table)

df_albums['new_album_name'] = df_albums['album_name'].apply(lambda x: 
                                            str(
                                                x.split('(')[0].strip()
                                            )
                                        )

# albums after album_selecton_vol1
print("abums after selction_vol1")
print(df_albums.shape)
# unique values
print("unique album names after vol1")
print(df_albums['new_album_name'].nunique())


df_albums_2['new_album_name'] = df_albums_2['album_name'].apply(lambda x: 
                                            str(
                                                x.split('(')[0].strip()
                                            )
                                        )
# albums after album_selecton_vol2
print("abums after selction_vol2")
print(df_albums_2.shape)
# unique values
print("unique album names after vol2")
print(df_albums_2['new_album_name'].nunique())
df_albums_2.to_csv('albums_table_uncleaned.csv', index=False)'''





# testing reg exp
'''pattern =  r'\bdemos\b(?![\w\'()])'

text = "The 'Mercury' demos (with John 'Hutch' Hutchinson)"
if re.search(pattern, text):
    print("Match found!")
else:
    print("No match found.")'''
    




# Sample data
data = {'album_name': [
    "This is a live performance!",
    "The band is playing live.",
    "{live} concert",
    "Demolized",
    "The wall Demo",
    "the Demon",
    "The wall remix",
    "the dark side of the moon demo",
    "Live music",
    "Hunky Dory",
    "The Dark Side of the moon",
    "The Doors",
    "LA Woman",
    "This is not delivered",
    "nine lives",
    "lived long",
    "the dark side of the moon (Live at Wembley)",
    "Live at Amstedam", 
    "The Wall",
    "Liverthel",
    "Remixed feelings",
    "Perfect strangters (Remix)",
    "Demos",
    "The unreleaseh demos",
    "Democracy Chinese",
    "Demosercdenio",
    "DemosA",
    'the Wall deluxe',
    'Fleetwood Mac',
    'Fleet Wood Max (Deluxe)',
    'animals (Deluxe edition)',
    'deluxed'
]}

'''
df = pd.DataFrame(data)
df_ = album_selection_vol1(df)
print(df_)'''








