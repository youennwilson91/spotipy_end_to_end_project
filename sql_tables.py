from sqlalchemy.sql import func
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TIMESTAMP
import uuid

Base = declarative_base()

# Define table structures
class TopTracks(Base):
    __tablename__ = 'top_tracks'
    rank = Column(String)
    id_version = Column(String, primary_key=True, default=str(uuid.uuid4()))
    id = Column(String)
    name = Column(String)
    artist = Column(String)
    preview_url = Column(String)
    duration = Column(Float)
    bpm = Column(Float)
    popularity = Column(Integer)
    key = Column(Integer)
    loudness = Column(Float)
    explicit = Column(String)
    term = Column(String)
    time = Column(TIMESTAMP, default=func.now())


class TopArtists(Base):
    __tablename__ = 'top_artists'
    rank = Column(String)
    id_version = Column(String, primary_key=True, default=str(uuid.uuid4()))
    id = Column(String, primary_key=True)
    name = Column(String)
    genre = Column(String)
    popularity = Column(Integer)
    top_tracks = Column(String)
    term = Column(String)
    time = Column(TIMESTAMP, default=func.now())

class RecentTracks(Base):
    __tablename__ = 'recent_tracks'
    name = Column(String)
    artist = Column(String)
    id_version = Column(String, primary_key=True, default=str(uuid.uuid4()))
    id = Column(String)
    popularity = Column(Integer)
    preview_url = Column(String)
    duration = Column(Float)
    bpm = Column(Float)
    key = Column(Integer)
    loudness = Column(Float)
    explicit = Column(String)
    played_at = Column(String)
    time = Column(TIMESTAMP, default=func.now())


class RelatedArtists(Base):
    __tablename__ = 'related_artists'
    artist_name = Column(String)
    id_version = Column(String, primary_key=True, default=str(uuid.uuid4()))
    related_artist = Column(String)

class ArtistsImageUrls(Base):
    __tablename__ = 'artist_image_urls'
    index = Column(Integer)
    artist_name = Column(String, primary_key=True)
    image_url = Column(String)



# Connect to the PostgreSQL database
engine = create_engine('postgresql://postgres:2585@localhost:5432/spotipy')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
