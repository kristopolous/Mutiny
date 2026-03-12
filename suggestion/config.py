import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging based on LOGLEVEL env var
LOGLEVEL = os.getenv("LOGLEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

DISCOGS_TOKEN = os.getenv("DISCOGS_USER_TOKEN")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "MusicRecommender/1.0")
DISCOGS_API_URL = os.getenv("DISCOGS_API_URL", "https://api.discogs.com")

DEFAULT_TOP_N = int(os.getenv("DEFAULT_TOP_N", "20"))