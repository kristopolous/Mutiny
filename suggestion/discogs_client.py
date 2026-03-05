from dotenv import load_dotenv
import os
import requests

load_dotenv()

DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "BaconDistanceEngine/1.0")

def make_discogs_request(endpoint):
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": DISCOGS_USER_AGENT
    }
    response = requests.get(f"https://api.discogs.com/{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_release(release_id):
    return make_discogs_request(f"releases/{release_id}")

def fetch_artist(artist_id):
    return make_discogs_request(f"artists/{artist_id}")

def fetch_label(label_id):
    return make_discogs_request(f"labels/{label_id}")

def fetch_artist_releases(artist_id):
    return make_discogs_request(f"artists/{artist_id}/releases")