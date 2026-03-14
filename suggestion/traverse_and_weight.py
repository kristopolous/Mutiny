#!/usr/bin/env python3
"""
Combined traverse and weight script.

Takes multiple source releases, traverses the graph to find connected releases,
and computes IDF-weighted similarity against ALL source releases combined.

Uses functions from lib.py and traverse.py for reusability and testability.
"""
import sys
import json
import argparse
from dotenv import load_dotenv
load_dotenv()

# Import from our library
from lib import extract_release_ids
from traverse import traverse
from weight import compute_weights as compute_idf_weights


def main():
    parser = argparse.ArgumentParser(description='Traverse and weight releases')
    parser.add_argument('urls', nargs='*', help='Discogs release URLs')
    parser.add_argument('-f', '--file', help='File with release URLs (one per line)')
    parser.add_argument('-d', '--depth', type=int, default=2, help='Traversal depth')
    parser.add_argument('--max-pages-per-label', type=int, default=5, help='Max pages per label')
    parser.add_argument('--max-releases-per-artist', type=int, default=20, help='Max releases per artist')
    
    args = parser.parse_args()
    
    # Load URLs from file and/or args
    urls = []
    if args.file:
        with open(args.file) as f:
            urls = [line.strip() for line in f if line.strip()]
    urls.extend([u for u in args.urls if u.startswith('https://')])
    
    if not urls:
        parser.print_help()
        sys.exit(1)
    
    source_ids = extract_release_ids(urls)
    print(f"Starting from {len(source_ids)} source releases: {source_ids}", file=sys.stderr)
    
    # Traverse - use function from traverse.py
    releases = traverse(
        source_ids, 
        depth=args.depth,
        max_pages_per_label=args.max_pages_per_label,
        max_releases_per_artist=args.max_releases_per_artist
    )
    
    # Weight - use function from weight.py
    weighted = compute_idf_weights(releases, source_ids)
    
    # Output
    output = {
        "source_ids": source_ids,
        "releases": weighted
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()