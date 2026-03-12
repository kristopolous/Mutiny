import re
import math
import requests
from dotenv import load_dotenv
import os
import time
import logging
from cache import cache_get, cache_set, get_redis_client

load_dotenv()

logger = logging.getLogger(__name__)

# Rate limiting: be polite to Discogs - 30 requests/minute
# 60s / 30 = 2 seconds between requests
RATE_LIMIT_LOCK_EXPIRE = 2  # seconds


def wait_for_rate_limit():
    """
    Simple Redis lock-based rate limiter.
    Try to acquire a lock with short expiry. If fails, wait and retry.
    """
    client = get_redis_client()
    lock_key = "discogs:rate_limit:lock"
    
    while True:
        # Try to acquire lock (NX = only if not exists, EX = expiry seconds)
        acquired = client.set(lock_key, "1", nx=True, ex=RATE_LIMIT_LOCK_EXPIRE)
        
        if acquired:
            return  # Got the lock, proceed with request
        
        # Lock exists, wait and retry
        logger.info(f"Rate limiting: waiting {RATE_LIMIT_LOCK_EXPIRE}s for lock")
        time.sleep(RATE_LIMIT_LOCK_EXPIRE)

DISCOGS_TOKEN = os.getenv("DISCOGS_USER_TOKEN")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "MusicRecommender/1.0")
DISCOGS_API_URL = os.getenv("DISCOGS_API_URL", "https://api.discogs.com")

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
    cache_key = f"discogs:{endpoint}"
    cached = cache_get(cache_key)
    if cached:
        return cached
    
    # Wait for rate limit before making request
    wait_for_rate_limit()
    
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": DISCOGS_USER_AGENT
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{DISCOGS_API_URL}/{endpoint}", headers=headers)
            
            if response.status_code == 404:
                return None
            if response.status_code == 429:
                logger.warning(f"RATE LIMIT (429) on {endpoint} - attempt {attempt + 1}/{max_retries}")
                wait_for_rate_limit()  # Wait and retry
                continue
            if response.status_code >= 500:
                logger.warning(f"SERVER ERROR {response.status_code} on {endpoint} - attempt {attempt + 1}/{max_retries}")
                time.sleep(2 ** attempt)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            cache_set(cache_key, data, ttl=604800)
            return data
            
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on {endpoint}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    
    return None

def fetch_release(release_id):
    return make_discogs_request(f"releases/{release_id}")

def fetch_artist(artist_id):
    return make_discogs_request(f"artists/{artist_id}")

def fetch_label(label_id):
    return make_discogs_request(f"labels/{label_id}")

def fetch_artist_releases(artist_id):
    return make_discogs_request(f"artists/{artist_id}/releases")

def calculate_weight(degree):
    if degree <= 1:
        return 1.0
    return 1.0 / math.log(degree)

def get_or_create_artist(artist_id):
    from graph import run_query
    
    existing = run_query(
        "MATCH (a:Artist {discogs_id: $id}) RETURN a",
        {"id": artist_id}
    )
    if existing:
        return existing[0]["a"]
    
    artist_data = fetch_artist(artist_id)
    if not artist_data:
        return None
    
    name = safe_get(artist_data, "name")
    if not name or name.strip() == "":
        return None
    
    run_query(
        """
        CREATE (a:Artist {
            discogs_id: $id,
            name: $name
        })
        RETURN a
        """,
        {
            "id": artist_id,
            "name": name
        }
    )
    return {"discogs_id": artist_id, "name": name}

def get_or_create_release(release_id):
    from graph import run_query
    
    existing = run_query(
        "MATCH (r:Release {discogs_id: $id}) RETURN r",
        {"id": release_id}
    )
    if existing:
        return existing[0]["r"]
    
    release_data = fetch_release(release_id)
    if not release_data:
        return None
    
    title = safe_get(release_data, "title")
    if not title:
        return None
    
    run_query(
        """
        CREATE (r:Release {
            discogs_id: $id,
            title: $title
        })
        RETURN r
        """,
        {
            "id": release_id,
            "title": title
        }
    )
    return {"discogs_id": release_id, "title": title}

def get_or_create_label(label_id):
    from graph import run_query
    
    existing = run_query(
        "MATCH (l:Label {discogs_id: $id}) RETURN l",
        {"id": label_id}
    )
    if existing:
        return existing[0]["l"]
    
    label_data = fetch_label(label_id)
    if not label_data:
        return None
    
    name = safe_get(label_data, "name")
    if not name:
        return None
    
    run_query(
        """
        CREATE (l:Label {
            discogs_id: $id,
            name: $name
        })
        RETURN l
        """,
        {
            "id": label_id,
            "name": name
        }
    )
    return {"discogs_id": label_id, "name": name}

def get_or_create_producer(producer_id):
    from graph import run_query
    
    existing = run_query(
        "MATCH (p:Producer {discogs_id: $id}) RETURN p",
        {"id": producer_id}
    )
    if existing:
        return existing[0]["p"]
    
    producer_data = fetch_artist(producer_id)
    if not producer_data:
        return None
    
    name = safe_get(producer_data, "name")
    if not name:
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
            "name": name
        }
    )
    return {"discogs_id": producer_id, "name": name}

def _ingest_artist_releases(artist_id, max_releases=50):
    """
    Fetch and ingest all releases for an artist.
    Called when expanding contributors to populate the graph with their full discography.
    """
    releases_data = fetch_artist_releases(artist_id)
    if not releases_data:
        return
    
    for release in safe_get(releases_data, "releases", [])[:max_releases]:
        release_id = str(safe_get(release, "id", ""))
        if release_id:
            # Only ingest the release node and its connections, not recursively expand
            _ingest_single_release(release_id)

def _ingest_single_release(release_id):
    """Ingest just a release's metadata without expanding contributors."""
    from graph import run_query
    
    release_data = fetch_release(release_id)
    if not release_data:
        return
    
    # Create release node
    release = get_or_create_release(release_id)
    if not release:
        return
    
    # Get all contributors
    all_contributors = set()
    for artist_entry in safe_get(release_data, "artists", []):
        aid = str(safe_get(artist_entry, "id", ""))
        if aid:
            all_contributors.add(aid)
    for extraartist in safe_get(release_data, "extraartists", []):
        aid = str(safe_get(extraartist, "id", ""))
        if aid:
            all_contributors.add(aid)
    
    # Create CONTRIBUTED_TO relationships
    for aid in all_contributors:
        run_query(
            "MATCH (a:Artist {discogs_id: $aid}), (r:Release {discogs_id: $release_id}) MERGE (a)-[:CONTRIBUTED_TO]->(r)",
            {"aid": aid, "release_id": release_id}
        )
    
    # Process labels
    for label in safe_get(release_data, "labels", []):
        label_id = str(safe_get(label, "id", ""))
        label_name = safe_get(label, "name", "")
        if not label_id or label_name == "Not On Label":
            continue
        get_or_create_label(label_id)
        run_query(
            "MATCH (r:Release {discogs_id: $release_id}), (l:Label {discogs_id: $label_id}) MERGE (r)-[:RELEASED_ON]->(l)",
            {"release_id": release_id, "label_id": label_id}
        )

def get_label_degree(label_id):
    from graph import run_query
    result = run_query(
        "MATCH (l:Label {discogs_id: $id})<-[:RELEASED_ON]-() RETURN count(*) as degree",
        {"id": label_id}
    )
    return result[0]["degree"] if result else 0

def get_producer_degree(producer_id):
    from graph import run_query
    result = run_query(
        "MATCH (p:Producer {discogs_id: $id})<-[:PRODUCED_BY]-() RETURN count(*) as degree",
        {"id": producer_id}
    )
    return result[0]["degree"] if result else 0

# Role categories that should create contributor connections
# These are "tastemakers" - people who make artistic decisions
CONTRIBUTOR_ROLES = {
    "Producer", "Mastered By", "Engineered By", "Mixed By",
    "Artwork", "Design", "Photography", "Illustrator",
    "Written By", "Composed By", "Lyrics By",
    "Director", "Animator", "Video By"
}

def ingest_release_with_connections(release_id, expand_contributors=False):
    """
    Ingest a release and its connections into the graph.
    
    If expand_contributors=True, also ingest all releases for each contributor
    so we can discover connections through them.
    """
    from graph import run_query
    
    release_data = fetch_release(release_id)
    if not release_data:
        return None
    
    # Get primary artist
    artist_id = str(safe_get(release_data, "artists", [{}])[0].get("id"))
    if not artist_id:
        return None
    
    artist = get_or_create_artist(artist_id)
    if not artist:
        return None
    
    release = get_or_create_release(release_id)
    if not release:
        return None
    
    # Get all contributors (artists + extraartists)
    all_contributors = set()
    
    for artist_entry in safe_get(release_data, "artists", []):
        aid = str(safe_get(artist_entry, "id", ""))
        if aid:
            all_contributors.add(aid)
    
    for extraartist in safe_get(release_data, "extraartists", []):
        aid = str(safe_get(extraartist, "id", ""))
        if aid:
            all_contributors.add(aid)
    
    # Create CONTRIBUTED_TO relationships for ALL contributors (primary + extraartists)
    for aid in all_contributors:
        run_query(
            "MATCH (a:Artist {discogs_id: $aid}), (r:Release {discogs_id: $release_id}) MERGE (a)-[:CONTRIBUTED_TO]->(r)",
            {"aid": aid, "release_id": release_id}
        )
    
    # Get or create all contributors
    for aid in all_contributors:
        if aid != artist_id:
            get_or_create_artist(aid)
    
    # If expand_contributors=True, fetch all releases for each contributor
    # This populates the graph with their full discography for discovery
    if expand_contributors:
        for aid in all_contributors:
            if aid != artist_id:  # Don't re-ingest the primary artist (already done via releases)
                _ingest_artist_releases(aid)
    
    # Create SIMILAR relationships between all contributors (tastemakers)
    contributors_list = list(all_contributors)
    for i, aid1 in enumerate(contributors_list):
        for aid2 in contributors_list[i+1:]:
            if aid1 != aid2:
                run_query(
                    """
                    MATCH (a1:Artist {discogs_id: $aid1}), (a2:Artist {discogs_id: $aid2})
                    MERGE (a1)-[:SIMILAR]->(a2)
                    MERGE (a2)-[:SIMILAR]->(a1)
                    """,
                    {"aid1": aid1, "aid2": aid2}
                )
    
    # Process labels - use higher threshold (5000) to capture more connections
    # The degree filter is applied during query time in aggregate.py for scoring
    for label in safe_get(release_data, "labels", []):
        label_id = str(safe_get(label, "id", ""))
        label_name = safe_get(label, "name", "")
        
        # Skip placeholder labels only
        if not label_id or label_name == "Not On Label":
            continue
        
        label_degree = get_label_degree(label_id)
        # Don't skip high-degree labels during ingest - they're needed for the graph
        # The degree filter for scoring is applied at query time
        
        label_obj = get_or_create_label(label_id)
        if label_obj:
            weight = calculate_weight(label_degree)
            run_query(
                "MATCH (r:Release {discogs_id: $release_id}), (l:Label {discogs_id: $label_id}) MERGE (r)-[:RELEASED_ON]->(l) ON CREATE SET r.weight = $weight",
                {"release_id": release_id, "label_id": label_id, "weight": weight}
            )
    
    # Process all contributor roles (tastemakers)
    for extraartist in safe_get(release_data, "extraartists", []):
        contributor_id = str(safe_get(extraartist, "id", ""))
        contributor_name = safe_get(extraartist, "name", "")
        contributor_role = safe_get(extraartist, "role", "")
        
        if not contributor_id:
            continue
        
        # Create the contributor as an Artist node
        contributor = get_or_create_artist(contributor_id)
        if not contributor:
            continue
        
        # Each extraartist already added to all_contributors above and gets SIMILAR links
        # No role parsing needed - every person with an ID is a tastemaker
    
    return release

def ingest_artist_with_connections(artist_id, depth=1):
    from graph import run_query
    
    artist = get_or_create_artist(artist_id)
    if not artist:
        return None
    
    releases = fetch_artist_releases(artist_id)
    if not releases:
        return artist
    
    for release in safe_get(releases, "releases", []):
        release_id = str(safe_get(release, "id", ""))
        if release_id:
            ingest_release_with_connections(release_id)
    
    # Ingest SIMILAR artists (1-degree connections)
    if depth > 0:
        similar_artists = run_query(
            "MATCH (a:Artist {discogs_id: $id})-[:SIMILAR]-(s:Artist) RETURN DISTINCT s.discogs_id AS id",
            {"id": artist_id}
        )
        for record in similar_artists:
            similar_id = record["id"]
            if similar_id != artist_id:
                ingest_artist_with_connections(similar_id, depth=depth-1)
    
    return artist

def find_similar_releases(source_artist_id, top_n=20):
    from graph import run_query
    
    results = []
    
    # 1. Direct SIMILAR connections (weight 1.0) - same contributors
    direct_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(r1:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
        WHERE other <> source
        MATCH (other)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> r1
        RETURN DISTINCT r2.discogs_id AS release_id, r2.title AS title, 1.0 AS weight, 1 AS hops
        """
    results.extend(run_query(direct_query, {"source_id": source_artist_id}))
    
    # 1b. Same artist, different releases (weight 1.5) - fallback when no shared contributors
    same_artist_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(r1:Release)
        MATCH (source)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> r1
        RETURN r2.discogs_id AS release_id, r2.title AS title, 1.5 AS weight, 1 AS hops
        """
    results.extend(run_query(same_artist_query, {"source_id": source_artist_id}))
    
    # 2. Label sharing (weight = 1.0 / log(degree))
    label_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)-[:RELEASED_ON]->(label:Label)<-[:RELEASED_ON]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
        WHERE other <> source
        WITH DISTINCT r2, label, COUNT { (label)<-[:RELEASED_ON]-() } AS degree
        WHERE degree < 1000
        RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
        """
    results.extend(run_query(label_query, {"source_id": source_artist_id}))
    
    # 3. Producer sharing (weight = 1.0 / log(degree))
    producer_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)-[:PRODUCED_BY]->(prod:Producer)<-[:PRODUCED_BY]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
        WHERE other <> source
        WITH DISTINCT r2, prod, COUNT { (prod)<-[:PRODUCED_BY]-() } AS degree
        WHERE degree < 1000
        RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
        """
    results.extend(run_query(producer_query, {"source_id": source_artist_id}))
    
    # 4. 2nd-degree SIMILAR (FoF) (weight 2.0) - artists who share releases with source
    two_hop_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)<-[:CONTRIBUTED_TO]-(mid:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> source AND mid <> source
        WITH DISTINCT r2, 2.0 AS weight, 2 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        """
    results.extend(run_query(two_hop_query, {"source_id": source_artist_id}))
    
    # 5. SIMILAR artists' releases (weight 2.5) - artists connected via SIMILAR relationship
    similar_artist_query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:SIMILAR]-(similar:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> source
        WITH DISTINCT r2, 2.5 AS weight, 3 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        """
    results.extend(run_query(similar_artist_query, {"source_id": source_artist_id}))
    
    # Aggregate by release: take minimum weight
    aggregated = {}
    for row in results:
        release_id = row["release_id"]
        if release_id not in aggregated:
            aggregated[release_id] = {
                "release_id": release_id,
                "title": row["title"],
                "bacon_distance": row["weight"],
                "hops": row["hops"]
            }
        else:
            if row["weight"] < aggregated[release_id]["bacon_distance"]:
                aggregated[release_id]["bacon_distance"] = row["weight"]
            if row["hops"] < aggregated[release_id]["hops"]:
                aggregated[release_id]["hops"] = row["hops"]
    
    # Sort by bacon_distance (ascending) and return top_n
    sorted_results = sorted(aggregated.values(), key=lambda x: (x["bacon_distance"], x["hops"]))
    
    return sorted_results[:top_n]

def find_similar_releases_with_label_prop(source_artist_id, labeled_releases, top_n=20):
    """
    Find similar releases using label propagation with user preference scores.
    
    labeled_releases: dict of {release_id: score} where score is user's preference (0-5)
    Returns: list of releases with predicted scores
    """
    from graph import run_query
    
    # Get all similar releases with weights
    similar = find_similar_releases(source_artist_id, top_n=100)
    
    # Calculate weighted average of labeled neighbor scores
    results = []
    for release in similar:
        release_id = release["release_id"]
        
        # Find labeled neighbors connected to this release
        query = """
            MATCH (r:Release {discogs_id: $release_id})<-[:CONTRIBUTED_TO]-(a:Artist)-[:CONTRIBUTED_TO]->(labeled:Release)
            WHERE labeled.discogs_id IN $labeled_ids
            WITH labeled, COUNT { (labeled)<-[:CONTRIBUTED_TO]-() } AS degree
            RETURN labeled.discogs_id AS neighbor_id, 1.0 / log(degree) AS weight
            """
        neighbors = run_query(query, {"release_id": release_id, "labeled_ids": list(labeled_releases.keys())})
        
        # Weighted average of neighbor scores
        total_weight = 0.0
        weighted_sum = 0.0
        for n in neighbors:
            neighbor_id = n["neighbor_id"]
            weight = n["weight"]
            if neighbor_id in labeled_releases:
                score = labeled_releases[neighbor_id]
                weighted_sum += weight * score
                total_weight += weight
        
        if total_weight > 0:
            predicted_score = weighted_sum / total_weight
            results.append({
                "release_id": release_id,
                "title": release["title"],
                "bacon_distance": release["bacon_distance"],
                "predicted_score": predicted_score,
                "hops": release["hops"]
            })
    
    # Sort by predicted score descending (higher = more recommended)
    results.sort(key=lambda x: (-x["predicted_score"], x["bacon_distance"]))
    
    return results[:top_n]