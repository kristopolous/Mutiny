#!/usr/bin/env python3
"""
Generic BFS release discovery - traverses all connection types equally.

Depth represents graph distance, not special handling for any connection type.
"""
import sys
import json
import argparse
from dotenv import load_dotenv
load_dotenv()

from ingest import fetch_release, fetch_artist_releases, safe_get, make_discogs_request


def build_release_index(release_ids):
    """
    Build release index with all connection types.
    
    Returns:
        releases: dict of release_id -> {title, artists, labels, extraartists}
    """
    releases = {}
    
    for rid in release_ids:
        data = fetch_release(rid)
        if not data:
            continue
            
        releases[rid] = extract_release_data(data)
    
    return releases


def extract_release_data(data):
    """Extract all relevant data from release API response."""
    release = {
        "title": safe_get(data, "title", "Unknown"),
        "artists": [],
        "labels": [],
        "extraartists": []
    }
    
    # Primary artists
    for a in safe_get(data, "artists", []):
        aid = str(a.get("id", ""))
        if aid:
            release["artists"].append({"id": aid, "name": a.get("name", "")})
    
    # Extraartists (Mastered By, Artwork, etc.)
    for ea in safe_get(data, "extraartists", []):
        eaid = str(ea.get("id", ""))
        if eaid:
            release["extraartists"].append({"id": eaid, "name": ea.get("name", ""), "role": ea.get("role", "")})
    
    # Labels
    for label in safe_get(data, "labels", []):
        lid = str(label.get("id", ""))
        lname = label.get("name", "")
        if lid and lname != "Not On Label":
            release["labels"].append({"id": lid, "name": lname})
    
    return release


def get_contributors(release_data):
    """Get all valid contributor IDs from a release (filter out null/empty)."""
    contributors = set()
    for a in release_data.get("artists", []):
        aid = a.get("id", "")
        if aid and aid != "0":  # Filter null/empty IDs
            contributors.add(aid)
    for ea in release_data.get("extraartists", []):
        eaid = ea.get("id", "")
        if eaid and eaid != "0":  # Filter null/empty IDs
            contributors.add(eaid)
    return contributors


def get_labels(release_data):
    """Get all valid label IDs from a release (filter out null/empty)."""
    labels = set()
    for label in release_data.get("labels", []):
        lid = label.get("id", "")
        lname = label.get("name", "")
        if lid and lid != "0" and lname and lname != "Not On Label":
            labels.add(lid)
    return labels


def traverse(source_release_ids, depth=2, max_pages_per_label=5, max_releases_per_artist=20):
    """
    Generic BFS traversal.
    
    At each depth level, we alternate between:
    - Finding releases via contributors (artists, extraartists)
    - Finding releases via labels
    
    Args:
        source_release_ids: list of release IDs to start from
        depth: how many hops to traverse
        max_pages_per_label: max pages to fetch from label API
        max_releases_per_artist: max releases to fetch per artist
    """
    # Step 1: Build initial index
    print(f"Building index from {len(source_release_ids)} source releases...", file=sys.stderr)
    releases = build_release_index(source_release_ids)
    print(f"  Indexed {len(releases)} releases", file=sys.stderr)
    
    # Track what's been found at each depth level
    depth_results = {0: set(source_release_ids)}  # 0 = source releases
    
    # Track all contributors we've discovered (for finding their releases)
    all_contributors = set()
    for rid in source_release_ids:
        all_contributors.update(get_contributors(releases.get(rid, {})))
    
    print(f"  Found {len(all_contributors)} contributors", file=sys.stderr)
    
    # BFS traversal - alternating between contributor and label connections
    for current_depth in range(1, depth + 1):
        print(f"\n=== DEPTH {current_depth} ===", file=sys.stderr)
        
        # Alternate: odd depths = contributor expansion, even depths = label expansion
        if current_depth % 2 == 1:
            # ODD: Expand via contributors (artists, extraartists)
            new_releases = expand_via_contributors(
                releases, all_contributors, max_releases_per_artist
            )
        else:
            # EVEN: Expand via labels
            new_releases = expand_via_labels(
                releases, max_pages_per_label
            )
        
        # Track depth results
        new_rids = set(new_releases.keys()) - releases.keys()
        depth_results[current_depth] = new_rids
        
        # Fetch full release data for new releases (label API doesn't include labels/extraartists)
        if current_depth % 2 == 0:  # Even depths = label expansion
            print(f"  Fetching full data for {len(new_rids)} new releases...", file=sys.stderr)
            for rid in list(new_rids):
                data = fetch_release(rid)
                if data:
                    new_releases[rid] = extract_release_data(data)
        
        releases.update(new_releases)
        
        # Update contributors list from new releases
        for rid in depth_results[current_depth]:
            all_contributors.update(get_contributors(releases.get(rid, {})))
        
        print(f"  Found {len(depth_results[current_depth])} new releases", file=sys.stderr)
        
        # Show first few
        for rid in sorted(list(depth_results[current_depth])[:10]):
            title = releases.get(rid, {}).get("title", "Unknown")
            print(f"    {rid}: {title}", file=sys.stderr)
        if len(depth_results[current_depth]) > 10:
            print(f"    ... and {len(depth_results[current_depth]) - 10} more", file=sys.stderr)
    
    print(f"\n=== TOTAL: {len(releases)} releases reachable ===", file=sys.stderr)
    
    # Return all reachable releases
    results = []
    for rid in releases:
        results.append({
            "release_id": rid,
            "title": releases[rid].get("title", "Unknown"),
            "discogs_url": f"https://www.discogs.com/release/{rid}"
        })
    
    return results


def expand_via_contributors(releases, known_contributors, max_releases_per_artist=20):
    """
    Expand releases by finding other releases from known contributors.
    
    For each contributor we've discovered, fetch their releases from Discogs.
    """
    new_releases = {}
    
    # Fetch releases for each known contributor
    for contrib_id in known_contributors:
        print(f"  Fetching releases for contributor {contrib_id}...", file=sys.stderr)
        artist_releases = fetch_artist_releases(contrib_id)
        if not artist_releases:
            continue
        
        for r in safe_get(artist_releases, "releases", [])[:max_releases_per_artist]:
            rid = str(r.get("id", ""))
            if rid and rid not in releases and rid not in new_releases:
                new_releases[rid] = {
                    "title": r.get("title", "Unknown"),
                    "artists": [{"id": contrib_id, "name": r.get("artist", "")}],
                    "labels": [],  # Will be filled when we fetch full data
                    "extraartists": []
                }
    
    return new_releases


def expand_via_labels(releases, max_pages=5):
    """
    Expand releases by finding all releases on known labels.
    
    Collect all labels from known releases, then fetch all releases on those labels.
    """
    new_releases = {}
    
    # Collect all labels from all known releases
    all_label_ids = set()
    label_names = {}
    for rid, data in releases.items():
        for label in data.get("labels", []):
            all_label_ids.add(label["id"])
            label_names[label["id"]] = label["name"]
    
    print(f"  Found {len(all_label_ids)} labels to explore", file=sys.stderr)
    
    # For each label, fetch all its releases
    for label_id in all_label_ids:
        label_name = label_names.get(label_id, "Unknown")
        print(f"  Fetching releases for label {label_id} ({label_name})...", file=sys.stderr)
        
        page = 1
        while page <= max_pages:
            page_data = make_discogs_request(f"labels/{label_id}/releases?page={page}")
            if not page_data:
                break
            
            pagination = page_data.get("pagination", {})
            total_pages = pagination.get("pages", 1)
            
            print(f"    Page {page}/{total_pages}...", file=sys.stderr)
            
            for r in safe_get(page_data, "releases", []):
                rid = str(r.get("id", ""))
                if rid and rid not in releases and rid not in new_releases:
                    new_releases[rid] = {
                        "title": r.get("title", "Unknown"),
                        "artists": [{"id": r.get("artist_id", ""), "name": r.get("artist", "")}],
                        "labels": [],
                        "extraartists": []
                    }
            
            if page >= total_pages:
                break
            page += 1
    
    return new_releases


def main():
    parser = argparse.ArgumentParser(description='Traverse releases - generic BFS discovery')
    parser.add_argument('urls', nargs='*', help='Discogs release URLs')
    parser.add_argument('-f', '--file', help='File with release URLs')
    parser.add_argument('-d', '--depth', type=int, default=2, help='Traversal depth')
    
    args = parser.parse_args()
    
    # Load URLs
    urls = []
    if args.file:
        with open(args.file) as f:
            urls = [line.strip() for line in f if line.strip()]
    urls.extend([u for u in args.urls if u.startswith('https://')])
    
    if not urls:
        parser.print_help()
        sys.exit(1)
    
    # Extract release IDs
    import re
    release_ids = []
    for url in urls:
        match = re.search(r'/release/(\d+)', url)
        if match:
            release_ids.append(match.group(1))
    
    print(f"Starting from {len(release_ids)} releases: {release_ids}", file=sys.stderr)
    
    results = traverse(release_ids, depth=args.depth)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()