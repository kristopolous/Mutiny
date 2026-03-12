#!/usr/bin/env python3
"""
Common library functions for Discogs release traversal and ranking.

This module provides reusable functions for:
- Building release indexes from contributors/labels
- Fetching paginated label/artist releases
- Ranking features by frequency
"""
import sys
import re
from collections import Counter

# These are imported from ingest.py in the consuming scripts
# kept here as documentation and for potential future refactoring


def extract_release_ids(urls):
    """
    Extract release IDs from a list of Discogs URLs.
    
    Args:
        urls: List of Discogs URLs
        
    Returns:
        List of release ID strings
    """
    release_ids = []
    for url in urls:
        match = re.search(r'/release/(\d+)', url)
        if match:
            release_ids.append(match.group(1))
    return release_ids


def get_releases_by_label(contributors, releases, source_release_ids, max_pages=5):
    """
    Fetch all releases for labels of source releases.
    
    Args:
        contributors: dict of artist_id -> set of release_ids
        releases: dict of release_id -> {title, artists, labels, extraartists}
        source_release_ids: list of source release IDs
        max_pages: maximum pages to fetch per label
        
    Returns:
        Tuple of (new_releases dict, depth_results set)
    """
    from ingest import make_discogs_request, safe_get
    
    new_releases = {}
    depth_results = set()
    
    # Collect all labels from source releases
    source_label_ids = set()
    source_label_names = {}
    for rid in source_release_ids:
        for label in releases.get(rid, {}).get("labels", []):
            source_label_ids.add(label["id"])
            source_label_names[label["id"]] = label["name"]
    
    # For each label, fetch releases from Discogs
    for label_id in source_label_ids:
        label_name = source_label_names.get(label_id, "Unknown")
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
                if rid:
                    new_releases[rid] = {
                        "title": r.get("title", "Unknown"),
                        "artists": [{"id": r.get("artist_id", ""), "name": r.get("artist", "")}],
                        "labels": [],
                        "extraartists": []
                    }
                    depth_results.add(rid)
            
            if page >= total_pages:
                break
            page += 1
    
    return new_releases, depth_results


def rank_feature_from_releases(releases, feature_name):
    """
    Rank a feature by raw frequency across all releases.
    
    Args:
        releases: dict of release_id -> release data (or list of release dicts)
        feature_name: one of 'label', 'artist', 'extraartist', 'label_id'
        
    Returns:
        dict of {feature_name: count} sorted by count descending
    """
    from ingest import fetch_release, safe_get
    
    counter = Counter()
    
    # Handle both dict and list input
    release_ids = []
    if isinstance(releases, dict):
        release_ids = list(releases.keys())
    elif isinstance(releases, list):
        for r in releases:
            if isinstance(r, dict) and "release_id" in r:
                release_ids.append(r["release_id"])
    
    for rid in release_ids:
        # Fetch full release data to get the feature
        data = fetch_release(rid)
        if not data:
            continue
        
        if feature_name == "label":
            for label in safe_get(data, "labels", []):
                lid = label.get("id", "")
                lname = label.get("name", "")
                if lid and lname and lname != "Not On Label":
                    counter[lname] += 1
                    
        elif feature_name == "artist":
            # Primary artists
            for a in safe_get(data, "artists", []):
                aname = a.get("name", "").strip()
                if aname:
                    counter[aname] += 1
            # Extraartists as well
            for ea in safe_get(data, "extraartists", []):
                eaname = ea.get("name", "").strip()
                if eaname:
                    counter[eaname] += 1
                    
        elif feature_name == "extraartist":
            for ea in safe_get(data, "extraartists", []):
                eaname = ea.get("name", "").strip()
                role = ea.get("role", "").strip()
                if eaname:
                    key = f"{eaname} ({role})" if role else eaname
                    counter[key] += 1
                    
        elif feature_name == "label_id":
            for label in safe_get(data, "labels", []):
                lid = label.get("id", "")
                lname = label.get("name", "")
                if lid and lname and lname != "Not On Label":
                    counter[f"{lid}: {lname}"] += 1
    
    return dict(counter.most_common())