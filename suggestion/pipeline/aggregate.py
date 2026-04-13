#!/usr/bin/env python3
import sys
import os
import json
import argparse
import logging
import math

# Add parent directory for imports (config, ingest, lib)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # Sets up logging from LOGLEVEL env var
from ingest import extract_discogs_id, ingest_release_with_connections
from similar import find_similar_releases_with_meta, fetch_release

logger = logging.getLogger(__name__)

# Depth configuration: max degree thresholds for each hop level
# Currently disabled thresholds for debugging - all connections pass through
DEPTH_CONFIG = {
    1: {
        # Direct connections: same artist, direct contributor sharing
        'max_degree': 999999,  # No threshold - show all
        'weights': {
            'direct_contributor': 1.0,
            'same_artist': 1.5,
        }
    },
    2: {
        # Label/producer sharing
        'max_degree': 999999,  # No threshold - show all
        'weights': {
            'label_sharing': '1.0/log(degree)',
            'producer_sharing': '1.0/log(degree)',
        }
    },
    3: {
        # SIMILAR artist connections
        'max_degree': 999999,  # No threshold - show all
        'weights': {
            'similar_artist': 2.5,
            'fof': 2.0,
        }
    }
}

def load_release_urls(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_contributors_from_url(url):
    """Extract ALL contributor IDs from a release URL (artists + extraartists)."""
    discogs_type, discogs_id = extract_discogs_id(url)
    logger.debug(f"get_contributors_from_url: {url} -> type={discogs_type}, id={discogs_id}")
    if discogs_type == "release":
        release_data = fetch_release(discogs_id)
        if release_data:
            contributors = set()
            # Primary artists
            for a in release_data.get("artists", []):
                aid = a.get("id")
                if aid:
                    contributors.add(str(aid))
                    logger.debug(f"  Primary artist: {aid} - {a.get('name')}")
            # Extraartists (Mastered By, Artwork, Producer, etc.)
            for ea in release_data.get("extraartists", []):
                eaid = ea.get("id")
                if eaid:
                    contributors.add(str(eaid))
                    logger.debug(f"  Extraartist: {eaid} - {ea.get('name')} ({ea.get('role')})")
            logger.debug(f"  Total contributors: {contributors}")
            return list(contributors)
    return []

def aggregate_recommendations(urls, top_n=20, depth=2):
    """
    Aggregate recommendations from multiple release URLs.
    
    depth: How many hops to follow from source releases
        1 = direct connections only (same artist, contributors)
        2 = + label/producer sharing (max_degree=500)
        3 = + SIMILAR/FoF connections (max_degree=100)
    
    For long-tail discovery, deeper levels use stricter degree thresholds
    to avoid converging on popular/top40 releases.
    """
    from graph import run_query
    
    # Get degree thresholds based on depth
    max_degree = DEPTH_CONFIG.get(depth, {}).get('max_degree', 100)
    
    # Ingest all source releases first
    source_contributors = []
    for url in urls:
        discogs_type, discogs_id = extract_discogs_id(url)
        logger.debug(f"Processing URL: {url}")
        if discogs_type == "release":
            logger.debug(f"Ingesting release {discogs_id}...")
            # Also ingest all releases for each contributor so we can discover
            # connections through them (e.g., the mastering engineer who worked on 18 releases)
            ingest_release_with_connections(discogs_id, expand_contributors=True)
            artists = get_contributors_from_url(url)
            logger.debug(f"Found contributors: {artists}")
            source_contributors.extend(artists)
    
    logger.debug(f"Total source contributors: {source_contributors}")
    
    # Collect all similar releases based on depth
    all_releases = {}
    
    # === DEPTH 1: Direct connections (same artist, direct contributors) ===
    for artist_id in source_contributors:
        if not artist_id:
            continue
        
        # Same artist, different releases
        same_results = run_query('''
            MATCH (source:Artist {discogs_id: $artist_id})-[:CONTRIBUTED_TO]->(r1:Release)
            MATCH (source)-[:CONTRIBUTED_TO]->(r2:Release)
            WHERE r2 <> r1
            WITH DISTINCT r2, 1.5 AS weight, 1 AS hops
            RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        ''', {"artist_id": artist_id})
        
        for row in same_results:
            release_id = row["release_id"]
            if release_id not in all_releases:
                all_releases[release_id] = {
                    "release_id": release_id,
                    "title": row["title"],
                    "bacon_distance": row["weight"],
                    "hops": row["hops"],
                    "sources": []
                }
            if row["weight"] < all_releases[release_id]["bacon_distance"]:
                all_releases[release_id]["bacon_distance"] = row["weight"]
            all_releases[release_id]["sources"].append(artist_id)
        
        # Direct contributor sharing (same tastemaker on different releases)
        contributor_results = run_query('''
            MATCH (source:Artist {discogs_id: $artist_id})-[:CONTRIBUTED_TO]->(r1:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
            WHERE other <> source
            MATCH (other)-[:CONTRIBUTED_TO]->(r2:Release)
            WHERE r2 <> r1
            WITH DISTINCT r2, 1.0 AS weight, 1 AS hops
            RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        ''', {"artist_id": artist_id})
        
        for row in contributor_results:
            release_id = row["release_id"]
            if release_id not in all_releases:
                all_releases[release_id] = {
                    "release_id": release_id,
                    "title": row["title"],
                    "bacon_distance": row["weight"],
                    "hops": row["hops"],
                    "sources": [artist_id]
                }
            else:
                if row["weight"] < all_releases[release_id]["bacon_distance"]:
                    all_releases[release_id]["bacon_distance"] = row["weight"]
                all_releases[release_id]["sources"].append(artist_id)
    
    # === DEPTH 2: Label/Producer sharing (if depth >= 2) ===
    logger.debug(f"Running DEPTH 2 queries with max_degree={max_degree}")
    if depth >= 2:
        for artist_id in source_contributors:
            if not artist_id:
                continue
            
            # Label sharing with degree threshold
            label_results = run_query('''
                MATCH (source:Artist {discogs_id: $artist_id})-[:CONTRIBUTED_TO]->(:Release)-[:RELEASED_ON]->(label:Label)<-[:RELEASED_ON]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
                WHERE other <> source
                WITH DISTINCT r2, label
                MATCH (label)<-[:RELEASED_ON]-()
                WITH r2, label, count(*) AS degree
                WHERE degree < $max_degree
                RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
            ''', {"artist_id": artist_id, "max_degree": max_degree})
            
            for row in label_results:
                release_id = row["release_id"]
                weight = row["weight"]
                if release_id not in all_releases:
                    all_releases[release_id] = {
                        "release_id": release_id,
                        "title": row["title"],
                        "bacon_distance": weight,
                        "hops": row["hops"],
                        "sources": []
                    }
                if weight < all_releases[release_id]["bacon_distance"]:
                    all_releases[release_id]["bacon_distance"] = weight
                all_releases[release_id]["sources"].append(artist_id)
            
            # Producer sharing with degree threshold
            producer_results = run_query('''
                MATCH (source:Artist {discogs_id: $artist_id})-[:CONTRIBUTED_TO]->(:Release)-[:PRODUCED_BY]->(prod:Producer)<-[:PRODUCED_BY]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
                WHERE other <> source
                WITH DISTINCT r2, prod
                MATCH (prod)<-[:PRODUCED_BY]-()
                WITH r2, prod, count(*) AS degree
                WHERE degree < $max_degree
                RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
            ''', {"artist_id": artist_id, "max_degree": max_degree})
            
            for row in producer_results:
                release_id = row["release_id"]
                weight = row["weight"]
                if release_id not in all_releases:
                    all_releases[release_id] = {
                        "release_id": release_id,
                        "title": row["title"],
                        "bacon_distance": weight,
                        "hops": row["hops"],
                        "sources": []
                    }
                if weight < all_releases[release_id]["bacon_distance"]:
                    all_releases[release_id]["bacon_distance"] = weight
                all_releases[release_id]["sources"].append(artist_id)
    
    # === DEPTH 3: SIMILAR artist connections (if depth >= 3) ===
    # Use stricter degree threshold (100) to favor rare connections
    if depth >= 3:
        depth3_max_degree = DEPTH_CONFIG[3]['max_degree']  # 100
        logger.debug(f"Running DEPTH 3 queries with depth3_max_degree={depth3_max_degree}")
        
        for artist_id in source_contributors:
            if not artist_id:
                continue
            
            # 2nd-degree FoF (friends of friends) - filtered by label degree
            fof_results = run_query('''
                MATCH (source:Artist {discogs_id: $artist_id})-[:CONTRIBUTED_TO]->(:Release)<-[:CONTRIBUTED_TO]-(mid:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
                WHERE r2 <> source AND mid <> source
                WITH DISTINCT r2
                MATCH (r2)-[:RELEASED_ON]->(l:Label)
                WITH r2, l, count(*) AS label_degree
                WHERE label_degree < $max_degree
                RETURN r2.discogs_id AS release_id, r2.title AS title, 2.0 AS weight, 2 AS hops
            ''', {"artist_id": artist_id, "max_degree": depth3_max_degree})
            
            for row in fof_results:
                release_id = row["release_id"]
                if release_id not in all_releases:
                    all_releases[release_id] = {
                        "release_id": release_id,
                        "title": row["title"],
                        "bacon_distance": row["weight"],
                        "hops": row["hops"],
                        "sources": [artist_id]
                    }
                else:
                    if row["weight"] < all_releases[release_id]["bacon_distance"]:
                        all_releases[release_id]["bacon_distance"] = row["weight"]
                    all_releases[release_id]["sources"].append(artist_id)
            
            # SIMILAR artist connections - filtered by label degree
            similar_results = run_query('''
                MATCH (source:Artist {discogs_id: $artist_id})-[:SIMILAR]-(similar:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
                WHERE r2 <> source
                WITH DISTINCT r2
                MATCH (r2)-[:RELEASED_ON]->(l:Label)
                WITH r2, l, count(*) AS label_degree
                WHERE label_degree < $max_degree
                RETURN r2.discogs_id AS release_id, r2.title AS title, 2.5 AS weight, 3 AS hops
            ''', {"artist_id": artist_id, "max_degree": depth3_max_degree})
            
            for row in similar_results:
                release_id = row["release_id"]
                if release_id not in all_releases:
                    all_releases[release_id] = {
                        "release_id": release_id,
                        "title": row["title"],
                        "bacon_distance": row["weight"],
                        "hops": row["hops"],
                        "sources": [artist_id]
                    }
                else:
                    if row["weight"] < all_releases[release_id]["bacon_distance"]:
                        all_releases[release_id]["bacon_distance"] = row["weight"]
                    all_releases[release_id]["sources"].append(artist_id)

    # === SCORING: Compute Adamic-Adar and Jaccard similarity ===
    # Get source release IDs for similarity computation
    source_release_ids = set()
    for url in urls:
        discogs_type, discogs_id = extract_discogs_id(url)
        if discogs_type == "release":
            source_release_ids.add(discogs_id)
    
    # Pre-fetch contributor degrees for Adamic-Adar weighting
    contributor_degrees = {}
    for contrib_id in source_contributors:
        degree_result = run_query('''
            MATCH (a:Artist {discogs_id: $contrib_id})-[:CONTRIBUTED_TO]->(r:Release)
            RETURN count(r) AS degree
        ''', {"contrib_id": contrib_id})
        if degree_result:
            contributor_degrees[contrib_id] = degree_result[0].get("degree", 1)
        else:
            contributor_degrees[contrib_id] = 1
    
    logger.debug(f"Pre-fetched degrees for {len(contributor_degrees)} contributors")
    
    # Compute proper similarity scores
    results = []
    for release_id, data in all_releases.items():
        # Skip if this IS a source release (don't recommend what we already have)
        if release_id in source_release_ids:
            continue
        
        # Get all contributors for this candidate release
        candidate_contributors_result = run_query('''
            MATCH (a:Artist)-[:CONTRIBUTED_TO]->(r:Release {discogs_id: $release_id})
            RETURN collect(a.discogs_id) AS contributors
        ''', {"release_id": release_id})
        
        candidate_contributors = set()
        if candidate_contributors_result:
            candidate_contributors = set(candidate_contributors_result[0].get("contributors", []))
        
        # === ADAMIC-ADAR: Sum of 1/log(degree) for shared contributors ===
        adamic_adar = 0.0
        shared_contributors = set(source_contributors) & candidate_contributors
        for shared in shared_contributors:
            deg = contributor_degrees.get(shared, 1)
            # Higher score for rarer contributors (lower degree = higher weight)
            aa_weight = 1.0 / (1.0 + math.log(deg + 1))  # +1 to avoid log(0)
            adamic_adar += aa_weight
            logger.debug(f"  Shared contributor {shared} (degree={deg}): aa_weight={aa_weight:.4f}")
        
        # === JACCARD: |A ∩ B| / |A ∪ B| ===
        union_size = len(set(source_contributors) | candidate_contributors)
        jaccard = len(shared_contributors) / union_size if union_size > 0 else 0.0
        
        # === LABEL IDF: Weight by inverse log degree of labels ===
        label_weights = []
        label_result = run_query('''
            MATCH (r:Release {discogs_id: $release_id})-[:RELEASED_ON]->(l:Label)
            MATCH (l)<-[:RELEASED_ON]-(other:Release)
            WITH l, count(other) AS label_degree
            RETURN collect(l.name) AS labels, collect(label_degree) AS degrees
        ''', {"release_id": release_id})
        
        if label_result and label_result[0].get("labels"):
            for label_name, label_deg in zip(label_result[0]["labels"], label_result[0]["degrees"]):
                # Rare labels (low degree) get higher weight
                label_weight = 1.0 / (1.0 + math.log(label_deg + 1))
                label_weights.append(label_weight)
                logger.debug(f"  Label {label_name} (degree={label_deg}): weight={label_weight:.4f}")
        
        label_idf = sum(label_weights) / len(label_weights) if label_weights else 0.0
        
        # === COMBINE SCORES ===
        # Adamic-Adar is the strongest signal for long-tail discovery
        # Jaccard provides overlap normalization
        # Label IDF penalizes releases on popular labels
        
        # Base score from Adamic-Adar (scaled to be meaningful)
        aa_score = adamic_adar * 10.0  # Scale up
        
        # Bonus for high Jaccard overlap
        jaccard_bonus = jaccard * 5.0
        
        # Penalty for popular labels
        label_penalty = 1.0 - label_idf  # 0 = rare label (good), 1 = popular label (bad)
        
        # Combine: AA is primary, boosted by Jaccard, reduced by popular labels
        predicted_score = (aa_score + jaccard_bonus) * (1.0 - 0.3 * label_penalty)
        
        logger.debug(f"Release {release_id}: AA={adamic_adar:.4f}, Jaccard={jaccard:.4f}, Label IDF={label_idf:.4f} -> score={predicted_score:.4f}")
        
        results.append({
            "release_id": release_id,
            "title": data["title"],
            "discogs_url": f"https://www.discogs.com/release/{release_id}",
            "adamic_adar": round(adamic_adar, 4),
            "jaccard": round(jaccard, 4),
            "label_idf": round(label_idf, 4),
            "hops": data["hops"],
            "bacon_distance": data.get("bacon_distance", 99),
            "predicted_score": round(predicted_score, 4),
            "source_count": len(data["sources"]),
            "shared_contributors": len(shared_contributors),
            "artists": [],
            "labels": []
        })
    
    # Sort by predicted score only - it already encapsulates:
    # - Adamic-Adar (rarer contributors = higher score)
    # - Jaccard (more overlap = higher score)  
    # - Label IDF (rarer labels = higher score)
    results.sort(key=lambda x: -x["predicted_score"])
    
    # Log depth configuration used
    logger.debug(f"Final results: {len(all_releases)} releases found")
    print(f"# Using depth={depth}, max_degree={max_degree} for long-tail discovery", file=sys.stderr)
    
    # Fetch metadata for top results
    for result in results[:top_n]:
        release_data = fetch_release(result["release_id"])
        if release_data:
            for artist in release_data.get("artists", []):
                aid = str(artist.get("id", ""))
                if aid:
                    result["artists"].append({
                        "discogs_id": aid,
                        "name": artist.get("name", ""),
                        "role": artist.get("role", "")
                    })
            for label in release_data.get("labels", []):
                lid = str(label.get("id", ""))
                if lid:
                    result["labels"].append({
                        "discogs_id": lid,
                        "name": label.get("name", "")
                    })
    
    return results[:top_n]

def main():
    parser = argparse.ArgumentParser(description='Aggregate music recommendations from multiple Discogs releases')
    parser.add_argument('urls', nargs='*', help='Discogs URLs (can be file with -f, or individual URLs)')
    parser.add_argument('-f', '--file', help='File containing release URLs (one per line)')
    parser.add_argument('-n', '--top-n', type=int, default=20, help='Number of results to return (default: 20)')
    parser.add_argument('-d', '--depth', type=int, default=2, choices=[1, 2, 3], 
                        help='Traversal depth: 1=direct, 2=+label/producer, 3=+similar. Default 2. Higher depth uses stricter degree thresholds for long-tail.')
    
    args = parser.parse_args()
    
    urls = []
    
    if args.file:
        urls = load_release_urls(args.file)
    
    urls.extend([u for u in args.urls if u.startswith('https://')])
    
    if not urls:
        parser.print_help()
        sys.exit(1)
    
    results = aggregate_recommendations(urls, top_n=args.top_n, depth=args.depth)
    
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()