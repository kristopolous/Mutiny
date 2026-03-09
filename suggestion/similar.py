#!/usr/bin/env python3
import sys
import json
from ingest import extract_discogs_id, find_similar_releases

def main():
    if len(sys.argv) < 2:
        print("Usage: ./similar.py <discogs_url> [top_n]", file=sys.stderr)
        sys.exit(1)
    
    discogs_url = sys.argv[1]
    top_n = 20
    
    if len(sys.argv) > 2:
        try:
            top_n = int(sys.argv[2])
        except ValueError:
            print(f"Invalid top_n value: {sys.argv[2]}", file=sys.stderr)
            sys.exit(1)
    
    discogs_type, discogs_id = extract_discogs_id(discogs_url)
    if not discogs_id:
        print(f"Could not extract Discogs ID from URL: {discogs_url}", file=sys.stderr)
        sys.exit(1)
    
    if discogs_type == "release":
        release_data = fetch_release(discogs_id)
        if not release_data:
            print(f"Could not fetch release {discogs_id}", file=sys.stderr)
            sys.exit(1)
        artist_id = str(release_data.get("artists", [{}])[0].get("id", ""))
        if not artist_id:
            print(f"Could not extract artist ID from release {discogs_id}", file=sys.stderr)
            sys.exit(1)
        # Ingest this release
        from ingest import ingest_release_with_connections
        ingest_release_with_connections(discogs_id)
        # Don't ingest SIMILAR artists - too slow, just use label/producer sharing
    elif discogs_type == "artist":
        artist_id = discogs_id
        from ingest import ingest_artist_with_connections
        ingest_artist_with_connections(artist_id, depth=0)
    else:
        print(f"Unknown Discogs ID type: {discogs_type}", file=sys.stderr)
        sys.exit(1)
    
    results = find_similar_releases_with_meta(artist_id, top_n=top_n)
    
    print(json.dumps(results, indent=2))

def find_similar_releases_with_meta(source_artist_id, top_n=20):
    from graph import run_query
    from ingest import fetch_release, safe_get
    
    # Get releases with metadata in one query
    query = """
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)<-[:CONTRIBUTED_TO]-(other:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> source AND other <> source
        WITH DISTINCT r2, 1.0 AS weight, 1 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        UNION
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(r1:Release)
        MATCH (source)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> r1
        WITH DISTINCT r2, 1.5 AS weight, 1 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        UNION
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)-[:RELEASED_ON]->(label:Label)<-[:RELEASED_ON]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
        WHERE other <> source
        WITH DISTINCT r2, label
        MATCH (label)<-[:RELEASED_ON]-()
        WITH r2, label, count(*) AS degree
        WHERE degree < 1000
        RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
        UNION
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)-[:PRODUCED_BY]->(prod:Producer)<-[:PRODUCED_BY]-(r2:Release)<-[:CONTRIBUTED_TO]-(other:Artist)
        WHERE other <> source
        WITH DISTINCT r2, prod
        MATCH (prod)<-[:PRODUCED_BY]-()
        WITH r2, prod, count(*) AS degree
        WHERE degree < 1000
        RETURN r2.discogs_id AS release_id, r2.title AS title, 1.0 / log(degree) AS weight, 2 AS hops
        UNION
        MATCH (source:Artist {discogs_id: $source_id})-[:CONTRIBUTED_TO]->(:Release)<-[:CONTRIBUTED_TO]-(mid:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> source AND mid <> source
        WITH DISTINCT r2, 2.0 AS weight, 2 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
        UNION
        MATCH (source:Artist {discogs_id: $source_id})-[:SIMILAR]-(similar:Artist)-[:CONTRIBUTED_TO]->(r2:Release)
        WHERE r2 <> source
        WITH DISTINCT r2, 2.5 AS weight, 3 AS hops
        RETURN r2.discogs_id AS release_id, r2.title AS title, weight, hops
    """
    
    raw_results = run_query(query, {"source_id": source_artist_id})
    
    # Aggregate by release: take minimum weight
    aggregated = {}
    for row in raw_results:
        release_id = row["release_id"]
        if release_id not in aggregated:
            aggregated[release_id] = {
                "release_id": release_id,
                "title": row["title"],
                "bacon_distance": row["weight"],
                "hops": row["hops"],
                "artists": [],
                "labels": []
            }
        else:
            if row["weight"] < aggregated[release_id]["bacon_distance"]:
                aggregated[release_id]["bacon_distance"] = row["weight"]
            if row["hops"] < aggregated[release_id]["hops"]:
                aggregated[release_id]["hops"] = row["hops"]
    
    # Fetch artist and label info for each release
    for release_id in aggregated.keys():
        release_data = fetch_release(release_id)
        if release_data:
            for artist in safe_get(release_data, "artists", []):
                aid = str(safe_get(artist, "id", ""))
                if aid:
                    aggregated[release_id]["artists"].append({
                        "discogs_id": aid,
                        "name": safe_get(artist, "name", ""),
                        "role": safe_get(artist, "role", "")
                    })
            for label in safe_get(release_data, "labels", []):
                lid = str(safe_get(label, "id", ""))
                if lid:
                    aggregated[release_id]["labels"].append({
                        "discogs_id": lid,
                        "name": safe_get(label, "name", "")
                    })
    
    # Sort by bacon_distance and return top_n
    sorted_results = sorted(aggregated.values(), key=lambda x: (x["bacon_distance"], x["hops"]))
    
    return sorted_results[:top_n]

def fetch_release(release_id):
    from ingest import fetch_release as fr
    return fr(release_id)

if __name__ == "__main__":
    main()