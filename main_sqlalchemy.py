
import pandas as pd
import spotipy
from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy.orm import Session
from sql_tables import TopTracks, TopArtists, RecentTracks, RelatedArtists, ArtistsImageUrls
import os
import uuid
import time
import psycopg2
import fastparquet
from datetime import datetime, timezone
import pytz

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

def convert_to_paris_time(utc_timestamp):
    utc_time = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))  # replace 'Z' with '+00:00' to form a valid isoformat string
    utc_time = utc_time.replace(tzinfo=timezone.utc)  # specify current timezone as UTC
    paris_time = utc_time.astimezone(pytz.timezone('Europe/Paris'))  # convert to Paris time
    return paris_time.strftime('%Y-%m-%d %H:%M:%S')  # convert to string in the desired format

def get_top_artists(term):
    artists = []
    artist_ids = []

    results = sp.current_user_top_artists(20, time_range=term)
    for idx, item in enumerate(results['items']):
        artist = {}
        artist['rank'] = f"{idx}"
        artist['id'] = item["id"]
        artist['name'] = item['name']
        artist['genre'] = ', '.join(item["genres"])  # Convert list to string
        artist['popularity'] = item["popularity"]
        artist_top_tracks = sp.artist_top_tracks(artist['id'])
        artist['top_tracks'] = ', '.join([tracks['name'] for tracks in artist_top_tracks['tracks']])  # Convert list to string
        artist['term'] = term
        artist["time"] = datetime.now()
        artist['id_version'] = f"{uuid.uuid4()}"

        artists.append(artist)
        artist_ids.append(artist['id'])

    df = pd.DataFrame(artists)
    write_data(df, 'top_artists', "C:/SparkCourse/spotify/checkpoints/topartists")

    return artist_ids

def get_top_tracks(term):
    tracks = []
    artist_ids = []

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
        track['id_version'] = f"{uuid.uuid4()}"

        tracks.append(track)
        artist_ids.append(item["artists"][0]["id"])

    df = pd.DataFrame(tracks)
    write_data(df, 'top_tracks', "C:/SparkCourse/spotify/checkpoints/toptracks")

    return artist_ids

def get_recent_tracks():
    tracks = []
    artist_ids = []

    results = sp.current_user_recently_played()
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
        track['played_at'] = convert_to_paris_time(item['played_at'])
        track["time"] = datetime.now()
        track['id_version'] = f"{uuid.uuid4()}"

        tracks.append(track)
        artist_ids.append(item["track"]["artists"][0]["id"])

    df = pd.DataFrame(tracks)
    write_data(df, 'recent_tracks', "C:/SparkCourse/spotify/checkpoints/recent_tracks")

    return artist_ids

def process_related_artists(sp, artist_id, artist_name, existing_pairs):
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

def populate_related_artists(artist_ids):
    session = Session(bind=engine)

    existing_pairs = {(tup.artist_name, tup.related_artist)
                      for tup in session.query(RelatedArtists.artist_name, RelatedArtists.related_artist)}

    processed_artists = set()  # New set to track processed artists
    related_artists_data = []

    for artist_id in artist_ids:
        if artist_id not in processed_artists:  # Only process the artist if it hasn't been processed yet
            artist = sp.artist(artist_id)
            related_artists_data_temp, existing_pairs = process_related_artists(sp, artist['id'], artist['name'], existing_pairs)
            related_artists_data.extend(related_artists_data_temp)
            processed_artists.add(artist_id)  # Mark the artist as processed

    related_artists_df = pd.DataFrame(related_artists_data)
    write_data(related_artists_df, 'related_artists', "C:/SparkCourse/spotify/checkpoints/related_artists")

    # Close the session
    session.close()

def update_artist_urls():
    # Start a new session
    session = Session(bind=engine)

    # Query all artist names from the artist_image_urls table
    existing_artists = {row.artist_name for row in session.query(ArtistsImageUrls.artist_name)}

    # Query all distinct artists from the related_artists table
    artists = session.query(RelatedArtists.artist_name).distinct()

    # Initialize a list to hold the new data
    artist_image_urls = []

    for artist_name_row in artists:
        artist_name = artist_name_row[0]  # Get the actual artist name from the Row object

        # Only process the artist if it doesn't already exist in the artist_image_urls table
        if artist_name not in existing_artists:
            # Retrieve the artist's data from Spotify
            artist_data = sp.search(q='artist:' + artist_name, type='artist')

            # Get the artist's Spotify image URL and add it to artist_image_urls
            try:
                artist_image_url = artist_data['artists']['items'][0]['images'][0]['url']
            except IndexError:  # Handle cases where the artist has no images
                artist_image_url = ''

            artist_image_urls.append({'artist_name': artist_name, 'image_url': artist_image_url})
            existing_artists.add(artist_name)  # Add the artist to existing_artists

    # Convert artist_image_urls to a DataFrame
    df = pd.DataFrame(artist_image_urls)

    # Update the existing table
    df.to_sql('artist_image_urls', engine, if_exists='append')

    # Close the session
    session.close()


# The get_top_artists() and get_top_tracks functions need a time range, which will be either short, medium or long term
terms = ["short_term", "medium_term", "long_term"]

# Start timer to check script execution time
start_time = time.time()

# try:
artist_ids = []
try:
    for term in terms:
        artist_ids.extend(get_top_artists(term))

    artist_ids.extend(get_recent_tracks())
    populate_related_artists(artist_ids)
    update_artist_urls()

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
