import sys
import json
from ingest import extract_discogs_id, ingest_artist_with_connections, find_similar_artists, fetch_release
from config import DEFAULT_TOP_N, DEFAULT_MAX_DEPTH

def main():
    if len(sys.argv) < 2:
        print("Usage: ./similar.py <discogs_url> [top_n] [max_depth]", file=sys.stderr)
        sys.exit(1)
    
    discogs_url = sys.argv[1]
    top_n = DEFAULT_TOP_N
    max_depth = DEFAULT_MAX_DEPTH
    
    if len(sys.argv) > 2:
        try:
            top_n = int(sys.argv[2])
        except ValueError:
            print(f"Invalid top_n value: {sys.argv[2]}", file=sys.stderr)
            sys.exit(1)
    
    if len(sys.argv) > 3:
        try:
            max_depth = int(sys.argv[3])
        except ValueError:
            print(f"Invalid max_depth value: {sys.argv[3]}", file=sys.stderr)
            sys.exit(1)
    
    discogs_type, discogs_id = extract_discogs_id(discogs_url)
    if not discogs_id:
        print(f"Could not extract Discogs ID from URL: {discogs_url}", file=sys.stderr)
        sys.exit(1)
    
    if discogs_type == "release":
        release_data = fetch_release(discogs_id)
        artist_id = str(release_data.get("artists", [{}])[0].get("id"))
        if not artist_id:
            print(f"Could not extract artist ID from release {discogs_id}", file=sys.stderr)
            sys.exit(1)
    elif discogs_type == "artist":
        artist_id = discogs_id
    else:
        print(f"Unknown Discogs ID type: {discogs_type}", file=sys.stderr)
        sys.exit(1)
    
    ingest_artist_with_connections(artist_id)
    
    results = find_similar_artists(artist_id, top_n=top_n)
    
    output = []
    for row in results:
        output.append({
            "artist": row.get("name", ""),
            "discogs_url": row.get("url", ""),
            "bacon_distance": row.get("bacon_distance", 0)
        })
    
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()