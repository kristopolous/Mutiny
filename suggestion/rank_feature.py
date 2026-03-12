#!/usr/bin/env python3
"""
Rank features by raw frequency.

Usage: cat traverse_output.json | ./rank_feature.py label
       cat traverse_output.json | ./rank_feature.py artist

Outputs JSON with {feature_name: count} sorted by count descending.
"""
import sys
import json
import argparse
from collections import Counter

from ingest import fetch_release, safe_get


def rank_feature(releases, feature_name):
    """
    Rank a feature by raw frequency across all releases.
    
    Features: label, artist, extraartist
    """
    counter = Counter()
    
    for release in releases:
        rid = release.get("release_id")
        if not rid:
            continue
        
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
                    # Include role for specificity
                    key = f"{eaname} ({role})" if role else eaname
                    counter[key] += 1
                    
        elif feature_name == "label_id":
            for label in safe_get(data, "labels", []):
                lid = label.get("id", "")
                lname = label.get("name", "")
                if lid and lname and lname != "Not On Label":
                    counter[f"{lid}: {lname}"] += 1
    
    return dict(counter.most_common())


def main():
    parser = argparse.ArgumentParser(description='Rank features by frequency')
    parser.add_argument('feature', choices=['label', 'artist', 'extraartist', 'label_id'], 
                        help='Feature to rank')
    parser.add_argument('-n', '--top', type=int, default=None, help='Show top N results')
    
    args = parser.parse_args()
    
    # Read JSON from stdin
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not isinstance(input_data, list):
        print("Error: Expected JSON array of releases", file=sys.stderr)
        sys.exit(1)
    
    # Rank the feature
    ranked = rank_feature(input_data, args.feature)
    
    # Output
    if args.top:
        ranked = dict(list(ranked.items())[:args.top])
    
    print(json.dumps(ranked, indent=2))


if __name__ == "__main__":
    main()