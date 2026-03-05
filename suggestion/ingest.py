import re
import math
import requests
from dotenv import load_dotenv
import os
from cache import cache_get, cache_set
from graph import run_query, get_total_artists, get_artists_sharing_property

load_dotenv()

DISCOGS_TOKEN = os.getenv("DISCOGS_USER_TOKEN")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "BaconDistanceEngine/1.0")
DISCOGS_API_URL = "https://api.discogs.com"

DISCOGS_RELEASE_ID_PATTERN = r"/release/(\d+)"
DISCOGS_ARTIST_ID_PATTERN = r"/artist/(\d+)"

def extract_release_id(url):
    match = re.search(DISCOGS_RELEASE_ID_PATTERN, url)
    if match:
        return match.group(1)
    return None

def extract_artist_id(url):
    match = re.search(DISCOGS_ARTIST_ID_PATTERN, url)
    if match:
        return match.group(1)
    return None

def extract_discogs_id(url):
    """Extract either release or artist ID from URL"""
    release_id = extract_release_id(url)
    if release_id:
        return ("release", release_id)
    artist_id = extract_artist_id(url)
    if artist_id:
        return ("artist", artist_id)
    return (None, None)

def safe_get(data, key, default=None):
    return data.get(key, default) if data else default

def make_discogs_request(endpoint, max_retries=3):
    import time
    cache_key = f"discogs:{endpoint}"
    cached = cache_get(cache_key)
    if cached:
        return cached
    
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": DISCOGS_USER_AGENT
    }
    
    for attempt in range(max_retries):
        response = requests.get(f"{DISCOGS_API_URL}/{endpoint}", headers=headers)
        if response.status_code == 404:
            return None  # Return None for deleted/invalid releases
        if response.status_code == 429:
            # Rate limited - wait and retry
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2  # Longer wait each attempt
                time.sleep(wait_time)
                continue
            else:
                raise requests.exceptions.HTTPError(f"Rate limited after {max_retries} attempts")
        if response.status_code >= 500:
            # Server error - wait and retry
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt * 2
                time.sleep(wait_time)
                continue
            else:
                raise requests.exceptions.HTTPError(f"Server error after {max_retries} attempts")
        response.raise_for_status()
        data = response.json()
        
        cache_set(cache_key, data, ttl=3600)
        time.sleep(1)  # Delay to avoid rate limiting
        return data
    
    return None

def fetch_release(release_id):
    return make_discogs_request(f"releases/{release_id}")

def fetch_artist(artist_id):
    return make_discogs_request(f"artists/{artist_id}")

def fetch_label(label_id):
    return make_discogs_request(f"labels/{label_id}")

def fetch_artist_releases(artist_id):
    return make_discogs_request(f"artists/{artist_id}/releases")

def calculate_weight(total_artists, artists_sharing_property):
    # Lower weight = stronger connection (more common = less distinctive)
    # Higher weight = rarer connection (more distinctive)
    if artists_sharing_property <= 1:
        return 1.0
    if total_artists <= 1:
        return 1.0
    # Inverse: more shared = lower weight
    return 1.0 / math.log(artists_sharing_property)

def get_or_create_artist(artist_id):
    existing = run_query(
        "MATCH (a:Artist {discogs_id: $id}) RETURN a",
        {"id": artist_id}
    )
    if existing:
        return existing[0]["a"]
    
    artist_data = fetch_artist(artist_id)
    if not artist_data:
        return None
    
    name = safe_get(artist_data, "name", f"Artist {artist_id}")
    if not name or name.strip() == "":
        return None
    
    url = safe_get(artist_data, "uri", f"{DISCOGS_API_URL}/artists/{artist_id}")
    
    run_query(
        """
        CREATE (a:Artist {
            discogs_id: $id,
            name: $name,
            url: $url
        })
        RETURN a
        """,
        {
            "id": artist_id,
            "name": name,
            "url": url
        }
    )
    return {"discogs_id": artist_id, "name": name}

def get_or_create_label(label_id):
    existing = run_query(
        "MATCH (l:Label {discogs_id: $id}) RETURN l",
        {"id": label_id}
    )
    if existing:
        return existing[0]["l"]
    
    label_data = fetch_label(label_id)
    if not label_data:
        return None
    
    run_query(
        """
        CREATE (l:Label {
            discogs_id: $id,
            name: $name,
            release_count: $release_count
        })
        RETURN l
        """,
        {
            "id": label_id,
            "name": label_data.get("name", f"Label {label_id}"),
            "release_count": label_data.get("num_releases", 0)
        }
    )
    return {"discogs_id": label_id, "name": label_data.get("name", f"Label {label_id}")}

def get_or_create_producer(producer_id):
    existing = run_query(
        "MATCH (p:Producer {discogs_id: $id}) RETURN p",
        {"id": producer_id}
    )
    if existing:
        return existing[0]["p"]
    
    producer_data = fetch_artist(producer_id)
    if not producer_data:
        return None
    
    run_query(
        """
        CREATE (p:Producer {
            discogs_id: $id,
            name: $name
        })
        RETURN p
        """,
        {
            "id": producer_id,
            "name": producer_data.get("name", f"Producer {producer_id}")
        }
    )
    return {"discogs_id": producer_id, "name": producer_data.get("name", f"Producer {producer_id}")}

def ingest_release_with_connections(release_id):
    release_data = fetch_release(release_id)
    
    artist_id = str(release_data.get("artists", [{}])[0].get("id"))
    if not artist_id:
        return None
    
    artist = get_or_create_artist(artist_id)
    releases = fetch_artist_releases(artist_id)
    
    for release in releases.get("releases", []):
        if str(release.get("id")) == str(release_id):
            continue
        
        release_data = fetch_release(str(release.get("id")))
        
        for label in release_data.get("labels", []):
            label_id = str(label.get("id"))
            label_name = label.get("name", "")
            # Skip placeholder labels with no real identity
            if label_id and label_name != "Not On Label":
                # Only create label if it has reasonable cardinality (not hundreds of thousands of artists)
                artists_sharing = get_artists_sharing_property("label", label_id)
                if artists_sharing < 1000:  # Threshold: only include labels with < 1000 artists
                    get_or_create_label(label_id)
                    weight = calculate_weight(get_total_artists(), artists_sharing)
                    run_query(
                        """
                        MATCH (a:Artist {discogs_id: $artist_id}), (l:Label {discogs_id: $label_id})
                        MERGE (a)-[r:RELEASED_ON]->(l)
                        ON CREATE SET r.weight = $weight
                        ON MATCH SET r.weight = $weight
                        """,
                        {"artist_id": artist_id, "label_id": label_id, "weight": weight}
                    )
        
        for producer in release_data.get("extraartists", []):
            producer_id = str(producer.get("id"))
            producer_name = producer.get("name", "")
            if producer_id and producer_name:
                producer_roles = producer.get("role", "")
                if "Producer" in producer_roles:
                    # Only create producer if it has reasonable cardinality
                    artists_sharing = get_artists_sharing_property("producer", producer_id)
                    if artists_sharing < 1000:
                        get_or_create_producer(producer_id)
                        weight = calculate_weight(get_total_artists(), artists_sharing)
                        run_query(
                            """
                            MATCH (a:Artist {discogs_id: $artist_id}), (p:Producer {discogs_id: $producer_id})
                            MERGE (a)-[r:PRODUCED_BY]->(p)
                            ON CREATE SET r.weight = $weight
                            ON MATCH SET r.weight = $weight
                            """,
                            {"artist_id": artist_id, "producer_id": producer_id, "weight": weight}
                        )
    
    return artist

def ingest_artist_with_connections(artist_id):
    artist = get_or_create_artist(artist_id)
    if not artist:
        return None
    
    releases = fetch_artist_releases(artist_id)
    if not releases:
        return artist
    
    related_artists = set()
    # Track which artists worked together on each release
    # key: release_id, value: set of artist_ids on that release
    release_artists = {}
    
    for release in releases.get("releases", []):
        release_id = str(release.get("id"))
        if not release_id:
            continue
        
        release_data = fetch_release(release_id)
        if not release_data:
            continue
        
        # Get all artists on this release
        artists_on_release = set()
        for artist_entry in safe_get(release_data, "artists", []):
            aid = str(safe_get(artist_entry, "id", ""))
            if aid:
                artists_on_release.add(aid)
        
        # Also add all extraartists (producers, sound engineers, managers, etc.)
        # These people select projects based on their interests, so they're tastemakers
        for extraartist in safe_get(release_data, "extraartists", []):
            aid = str(safe_get(extraartist, "id", ""))
            if aid:
                artists_on_release.add(aid)
        
        if artists_on_release:
            release_artists[release_id] = artists_on_release
        
        # Add to related artists
        for aid in artists_on_release:
            if aid and aid != artist_id:
                related_artists.add(aid)
    
    # Ingest related artists
    for related_id in related_artists:
        if related_id != artist_id:
            get_or_create_artist(related_id)
    
    total_artists = get_total_artists()
    
    # Create SIMILAR relationships between artists who worked together on same release
    for release_id, artist_list in release_artists.items():
        artist_list = list(artist_list)
        for i, aid1 in enumerate(artist_list):
            for aid2 in artist_list[i+1:]:
                if aid1 != aid2:
                    # Calculate weight based on how many other releases these two artists share
                    # For now, use a fixed weight for direct collaboration
                    weight = 1.0  # Direct collaboration = strongest connection
                    
                    run_query(
                        """
                        MATCH (a1:Artist {discogs_id: $aid1}), (a2:Artist {discogs_id: $aid2})
                        MERGE (a1)-[r1:SIMILAR]->(a2)
                        ON CREATE SET r1.weight = $weight
                        ON MATCH SET r1.weight = $weight
                        MERGE (a2)-[r2:SIMILAR]->(a1)
                        ON CREATE SET r2.weight = $weight
                        ON MATCH SET r2.weight = $weight
                        """,
                        {"aid1": aid1, "aid2": aid2, "weight": weight}
                    )
    
    # Also create relationships for labels and producers (weaker connections)
    label_artists = {}  # label_id -> set of artist_ids
    producer_artists = {}  # producer_id -> set of artist_ids
    
    for release_id, artist_list in release_artists.items():
        release_data = fetch_release(release_id)
        if not release_data:
            continue
        
        for label in safe_get(release_data, "labels", []):
            label_id = str(safe_get(label, "id", ""))
            label_name = safe_get(label, "name", "")
            # Skip placeholder labels with no real identity
            if label_id and label_name != "Not On Label":
                # Only include labels with reasonable cardinality
                artists_sharing = get_artists_sharing_property("label", label_id)
                if artists_sharing < 1000:
                    if label_id not in label_artists:
                        label_artists[label_id] = set()
                    for aid in artist_list:
                        label_artists[label_id].add(aid)
        
        for producer in safe_get(release_data, "extraartists", []):
            producer_id = str(safe_get(producer, "id", ""))
            producer_name = safe_get(producer, "name", "")
            if producer_id and producer_name:
                producer_roles = safe_get(producer, "role", "")
                if "Producer" in producer_roles:
                    # Only include producers with reasonable cardinality
                    artists_sharing = get_artists_sharing_property("producer", producer_id)
                    if artists_sharing < 1000:
                        if producer_id not in producer_artists:
                            producer_artists[producer_id] = set()
                        for aid in artist_list:
                            producer_artists[producer_id].add(aid)
    
    # Create label relationships
    for label_id, label_artist_ids in label_artists.items():
        label_data = get_or_create_label(label_id)
        if not label_data:
            continue
        artists_sharing = len(label_artist_ids)
        weight = calculate_weight(total_artists, artists_sharing)
        
        for aid in label_artist_ids:
            run_query(
                """
                MATCH (a:Artist {discogs_id: $artist_id}), (l:Label {discogs_id: $label_id})
                MERGE (a)-[r:RELEASED_ON]->(l)
                ON CREATE SET r.weight = $weight
                ON MATCH SET r.weight = $weight
                """,
                {"artist_id": aid, "label_id": label_id, "weight": weight}
            )
    
    # Create producer relationships
    for producer_id, producer_artist_ids in producer_artists.items():
        producer_data = get_or_create_producer(producer_id)
        if not producer_data:
            continue
        artists_sharing = len(producer_artist_ids)
        weight = calculate_weight(total_artists, artists_sharing)
        
        for aid in producer_artist_ids:
            run_query(
                """
                MATCH (a:Artist {discogs_id: $artist_id}), (p:Producer {discogs_id: $producer_id})
                MERGE (a)-[r:PRODUCED_BY]->(p)
                ON CREATE SET r.weight = $weight
                ON MATCH SET r.weight = $weight
                """,
                {"artist_id": aid, "producer_id": producer_id, "weight": weight}
            )
    
    return artist

def find_similar_artists(source_artist_id, top_n=20):
    # Find similar artists through multiple connection types and aggregate results
    # Return artists sorted by minimum weight (best connection)
    
    results = []
    
    # 1. Direct collaborations (weight 1.0)
    direct_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[r:SIMILAR]-(direct:Artist)
        WHERE direct <> source
        RETURN DISTINCT direct.discogs_id AS discogs_id, direct.name AS name, direct.url AS url, 1.0 AS weight, 1 AS hops
        """
    results.extend(run_query(direct_query, {"source_id": source_artist_id}))
    
    # 2. Label sharing (weight 1.5)
    label_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:RELEASED_ON]->(label:Label)<-[:RELEASED_ON]-(label_artist:Artist)
        WHERE label_artist <> source
        RETURN DISTINCT label_artist.discogs_id AS discogs_id, label_artist.name AS name, label_artist.url AS url, 1.5 AS weight, 2 AS hops
        """
    results.extend(run_query(label_query, {"source_id": source_artist_id}))
    
    # 3. Producer sharing (weight 1.5)
    producer_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:PRODUCED_BY]->(prod:Producer)<-[:PRODUCED_BY]-(prod_artist:Artist)
        WHERE prod_artist <> source
        RETURN DISTINCT prod_artist.discogs_id AS discogs_id, prod_artist.name AS name, prod_artist.url AS url, 1.5 AS weight, 2 AS hops
        """
    results.extend(run_query(producer_query, {"source_id": source_artist_id}))
    
    # 4. 2nd degree collaborations (weight 2.0)
    two_hop_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:SIMILAR]-(mid:Artist)-[:SIMILAR]-(two_hop:Artist)
        WHERE two_hop <> source AND two_hop <> mid
        RETURN DISTINCT two_hop.discogs_id AS discogs_id, two_hop.name AS name, two_hop.url AS url, 2.0 AS weight, 2 AS hops
        """
    results.extend(run_query(two_hop_query, {"source_id": source_artist_id}))
    
    # Aggregate by artist: take minimum weight and hops
    aggregated = {}
    for row in results:
        discogs_id = row["discogs_id"]
        if discogs_id not in aggregated:
            aggregated[discogs_id] = {
                "discogs_id": discogs_id,
                "name": row["name"],
                "url": row["url"],
                "bacon_distance": row["weight"],
                "hops": row["hops"]
            }
        else:
            if row["weight"] < aggregated[discogs_id]["bacon_distance"]:
                aggregated[discogs_id]["bacon_distance"] = row["weight"]
            if row["hops"] < aggregated[discogs_id]["hops"]:
                aggregated[discogs_id]["hops"] = row["hops"]
    
    # Sort by bacon_distance (ascending) and return top_n
    sorted_results = sorted(aggregated.values(), key=lambda x: (x["bacon_distance"], x["hops"]))
    
    return sorted_results[:top_n]