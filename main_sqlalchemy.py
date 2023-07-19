import pandas as pd
import spotipy
from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
from sqlalchemy.orm import Session
from sql_tables import TopTracks, TopArtists, RecentTracks, RelatedArtists
import os
import uuid
import time
import psycopg2
import fastparquet

scope = "user-top-read user-library-read user-read-recently-played"
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id="...",
        client_secret="...",
        redirect_uri="http://127.0.0.1:9090",
        scope=scope
    )
)

# PostgreSQL connection (hardcoded for this example)
engine = create_engine('postgresql://postgres:password@localhost:5432/spotipy')

# Check if the directories to save Parquet files exist, if not create them
directories = [
    "C:/SparkCourse/spotify/checkpoints/topartists",
    "C:/SparkCourse/spotify/checkpoints/related_artists",
    "C:/SparkCourse/spotify/checkpoints/toptracks",
    "C:/SparkCourse/spotify/checkpoints/recent_tracks"
]

for directory in directories:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Define functions

def write_df_to_postgres(df, table_name):
    df.to_sql(table_name, engine, if_exists='append', index=False)

def write_df_to_parquet(df, base_path):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    path = f"{base_path}_{timestamp}.parquet"
    df.to_parquet(path)

def write_data(df, table_name, base_path):
    write_df_to_postgres(df, table_name)
    write_df_to_parquet(df, base_path)

def process_related_artists(sp, artist_id, artist_name, existing_pairs):
    """
    Retrieves related artists for a given artist, checks if the artist-related_artist pair
    already exists in the set of existing pairs, and adds the related artist data to a list.

    Args:
        sp (Spotipy.Spotify): An instance of the Spotipy Spotify client.
        artist_id (str): The ID of the artist.
        artist_name (str): The name of the artist.
        existing_pairs (set): A set of existing artist-related artist pairs.

    Returns:
        tuple: A tuple containing a list of dictionaries representing the related artists' data
               and an updated set of existing artist-related artist pairs.
    """
    related_artists_data = []
    related_artists = sp.artist_related_artists(artist_id)
    for related in related_artists['artists']:
        pair = (artist_name, related['name'])
        if pair not in existing_pairs:
            related_artists_data.append({
                'artist_name': artist_name,
                'related_artist': related['name'],
                'id_version': f"{uuid.uuid4()}"
            })
            existing_pairs.add(pair)
    return related_artists_data, existing_pairs

def get_top_artists(term):
    """
    Fetches the 20 top artists for a user for each term (short_term, medium_term, or long_term),
    adds them to the top_artists table, and updates the related_artists table.

    Parameters:
    term (str): The term for which to fetch top artists. One of 'short_term', 'medium_term', or 'long_term'.

    Returns:
    List[dict]: A list of dictionaries, each representing a top artist.
    """

    artists = []
    related_artists_data = []

    # Create a new session
    session = Session(bind=engine)

    # Get the existing unique tuples of artist_name and related_artist
    existing_pairs = {(tup.artist_name, tup.related_artist)
                      for tup in session.query(RelatedArtists.artist_name, RelatedArtists.related_artist)}

    results = sp.current_user_top_artists(20, time_range=term)
    for idx, item in enumerate(results['items']):
        artist = {}
        artist['rank'] = f"{idx}"
        artist['id'] = item["id"]
        artist['name'] = item['name']
        artist['genre'] = item["genres"]
        artist['popularity'] = item["popularity"]
        artist_top_tracks = sp.artist_top_tracks(artist['id'])
        artist['top_tracks'] = [tracks['name'] for tracks in artist_top_tracks['tracks']]
        artist['term'] = term
        artist["time"] = datetime.now()
        # Generate 'id-version' column
        artist['id_version'] = f"{uuid.uuid4()}"

        related_artists_data, existing_pairs = process_related_artists(sp, artist['id'], artist['name'], existing_pairs)

        artists.append(artist)

    df = pd.DataFrame(artists)
    related_artists_df = pd.DataFrame(related_artists_data)

    write_data(df, 'top_artists', "C:/SparkCourse/spotify/checkpoints/topartists")
    write_data(related_artists_df, 'related_artists', "C:/SparkCourse/spotify/checkpoints/related_artists")

def get_top_tracks(term):
    """
    Fetches the 20 top tracks for a user for each term (short_term, medium_term, or long_term),
    adds them to the top_tracks table, and updates the related_artists table.

    Parameters:
    term (str): The term for which to fetch top tracks. One of 'short_term', 'medium_term', or 'long_term'.

    Returns:
    List[dict]: A list of dictionaries, each representing a top track.
    """

    tracks = []
    related_artists_data = []

    # Create a new session
    session = Session(bind=engine)

    # Get the existing unique tuples of rank, id and term
    existing_tuples = {(tup.rank, tup.id, tup.term)
                      for tup in session.query(TopTracks.rank, TopTracks.id, TopTracks.term)}

    # Get the existing unique tuples of artist_name and related_artist
    existing_pairs = {(tup.artist_name, tup.related_artist)
                      for tup in session.query(RelatedArtists.artist_name, RelatedArtists.related_artist)}

    results = sp.current_user_top_tracks(20, time_range=term)
    for idx, item in enumerate(results['items']):
        track = {}
        track['rank'] = f"{idx}"
        track['id'] = item["id"]
        track['name'] = item['name']
        track['artist'] = item["artists"][0]["name"]
        track['preview_url'] = item["preview_url"]
        analysis = sp.audio_analysis(track['id'])
        track['duration'] = analysis["track"]["duration"]
        track['bpm'] = analysis["track"]["tempo"]
        track['popularity'] = item["popularity"]
        track['key'] = analysis["track"]["key"]
        track['loudness'] = analysis["track"]["loudness"]
        track['explicit'] = item["explicit"]
        track['term'] = term
        track["time"] = datetime.now()

        # Generate 'id-version' column
        track['id_version'] = f"{uuid.uuid4()}"

        tup = (track['rank'], track['id'], track['term'])

        if tup not in existing_tuples:
            tracks.append(track)

        # Get related artists for the artist of the current track
        related_artists_data, existing_pairs = process_related_artists(sp, item["artists"][0]["id"], track['artist'],
                                                                       existing_pairs)

    df = pd.DataFrame(tracks)
    related_artists_df = pd.DataFrame(related_artists_data)

    write_data(df, 'top_tracks', "C:/SparkCourse/spotify/checkpoints/toptracks")
    write_data(related_artists_df, 'related_artists', "C:/SparkCourse/spotify/checkpoints/related_artists")


    # Close the session
    session.close()

    return tracks


def get_recent_tracks():
    """
    Fetches the 20 recently played tracks for a user, adds them to the recent_tracks table,
    and updates the related_artists table.

    Returns:
    List[dict]: A list of dictionaries, each representing a recently played track.
    """

    tracks = []
    related_artists_data = []

    # Create a new session
    session = Session(bind=engine)

    # Fetch the recent tracks from Spotify
    results = sp.current_user_recently_played()

    # Get the existing unique pairs of id and played_at
    existing_pairs = {(pair.id, pair.played_at)
                      for pair in session.query(RecentTracks.id, RecentTracks.played_at)}

    # Get the existing unique tuples of artist_name and related_artist
    existing_related_pairs = {(tup.artist_name, tup.related_artist)
                      for tup in session.query(RelatedArtists.artist_name, RelatedArtists.related_artist)}

    for idx, item in enumerate(results['items']):
        track = {}
        track['name'] = item["track"]["name"]
        track['artist'] = item["track"]["artists"][0]["name"]
        track['id'] = item["track"]['id']
        track['popularity'] = item["track"]["popularity"]
        track['preview_url'] = item["track"]["preview_url"]
        analysis = sp.audio_analysis(track['id'])
        track['duration'] = analysis["track"]["duration"]
        track['bpm'] = analysis["track"]["tempo"]
        track['key'] = analysis["track"]["key"]
        track['loudness'] = analysis["track"]["loudness"]
        track['explicit'] = item["track"]["explicit"]
        track['played_at'] = item['played_at']
        track["time"] = datetime.now()

        # Generate 'id-version' column
        track['id_version'] = f"{uuid.uuid4()}"

        pair = (track['id'], track['played_at'])

        if pair not in existing_pairs:
            tracks.append(track)

        # Get related artists for the artist of the current track
        related_artists_data, existing_related_pairs = process_related_artists(sp, item["track"]["artists"][0]["id"],
                                                                               track['artist'], existing_related_pairs)

    df = pd.DataFrame(tracks)
    related_artists_df = pd.DataFrame(related_artists_data)

    write_data(df, 'recent_tracks', "C:/SparkCourse/spotify/checkpoints/recent_tracks")
    write_data(related_artists_df, 'related_artists', "C:/SparkCourse/spotify/checkpoints/related_artists")

    # Close the session
    session.close()

    return tracks

# The get_top_artists() and get_top_tracks functions need a time range, which will be either short, medium or long term
terms = ["short_term", "medium_term", "long_term"]

# Start timer to check script execution time
start_time = time.time()




try:
    for term in terms:
        get_top_tracks(term)
        get_top_artists(term)

    get_recent_tracks()


except spotipy.SpotifyException as e:
    print(f"An error has occurred with Spotipy: {e}")
except spotipy.SpotifyOauthError as e:
    print(f"Authentication Error: {e}")
except Exception as e:
    print(f"An unexpected error has occurred: {e}")
else:
    print("SUCCESS ! ;)")
finally:
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The script took {execution_time:.2f} seconds to run.")
