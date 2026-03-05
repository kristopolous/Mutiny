import os

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

DISCOGS_API_URL = os.getenv("DISCOGS_API_URL", "https://api.discogs.com")
DISCOGS_TOKEN = os.getenv("DISCOGS_USER_TOKEN")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "BaconDistanceEngine/1.0")

DEFAULT_TOP_N = int(os.getenv("DEFAULT_TOP_N", "20"))
DEFAULT_MAX_DEPTH = int(os.getenv("DEFAULT_MAX_DEPTH", "3"))