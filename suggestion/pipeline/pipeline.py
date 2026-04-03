#!/usr/bin/env python3
"""
Pipeline: Bandcamp preferences → Discogs recommendations.

Usage:
    ./pipeline.py -f preferences.txt -d 2 -n 20

preferences.txt format:
    label/release-love 5
    label/release-ok 2
    label/release-dont-like 0

Scores: 0-5 (higher = more preferred)
"""

import argparse
import json
import sys
import os

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from correlate_local_pg import process_path as correlate_to_discogs
from traverse import traverse
from weight import compute_weights
from lib import extract_release_ids


def parse_preferences(filepath):
    """
    Parse preferences file.
    
    Returns:
        positive: list of (path, score) tuples for positive preferences
        negative: list of (path, score) tuples for negative preferences
    """
    positive = []
    negative = []
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.rsplit(None, 1)
            if len(parts) != 2:
                print(f"Skipping invalid line: {line}", file=sys.stderr)
                continue
            
            path, score = parts
            try:
                score = int(score)
            except ValueError:
                print(f"Invalid score on line: {line}", file=sys.stderr)
                continue
            
            if score > 0:
                positive.append((path, score))
            else:
                negative.append((path, score))
    
    return positive, negative


def correlate_preferences(preferences, redis_client, db_url, verbose=False):
    """
    Correlate Bandcamp paths to Discogs IDs.
    
    Returns:
        dict: {discogs_id: score} mapping
    """
    import psycopg2
    import redis
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_url)
    
    # Connect to Redis
    if redis_client is None:
        redis_client = redis.Redis(decode_responses=False)
    
    discogs_map = {}
    
    for path, score in preferences:
        # Resolve path to page.html
        if os.path.isfile(path):
            html_path = path
        elif os.path.isdir(path):
            html_path = os.path.join(path, 'page.html')
        else:
            print(f"Path not found: {path}", file=sys.stderr)
            continue
        
        # Extract stub for Redis lookup
        stub = '/'.join(os.path.abspath(html_path).split('/')[-3:-1])
        
        # Check Redis cache
        cached_url = redis_client.hget('bc2dg', stub)
        if cached_url:
            url = cached_url.decode('utf-8') if isinstance(cached_url, bytes) else str(cached_url)
            # Extract Discogs ID from URL
            discogs_id = url.rstrip('/').split('/')[-1]
            if discogs_id.isdigit():
                discogs_map[int(discogs_id)] = score
                if verbose:
                    print(f"[CACHE] {stub} -> {discogs_id} (score: {score})", file=sys.stderr)
                continue
        
        # Check failure cache
        if redis_client.sismember('bc2fail', stub):
            if verbose:
                print(f"[CACHED FAIL] {stub}", file=sys.stderr)
            continue
        
        # Not in cache - would need to correlate
        # For now, skip uncached entries
        print(f"[NOT CACHED] {stub} - run correlate-local-pg.py first", file=sys.stderr)
    
    conn.close()
    return discogs_map


def run_pipeline(preferences_file, db_url, depth=2, top_n=20, redis_client=None, verbose=False):
    """
    Run the full recommendation pipeline.
    
    1. Parse preferences
    2. Correlate to Discogs IDs
    3. Traverse graph
    4. Weight by IDF
    5. Rank and return
    """
    # Step 1: Parse preferences
    positive, negative = parse_preferences(preferences_file)
    
    if not positive:
        print("No positive preferences found. Need at least one release with score > 0.", file=sys.stderr)
        return []
    
    print(f"Loaded {len(positive)} positive, {len(negative)} negative preferences", file=sys.stderr)
    
    # Step 2: Correlate to Discogs IDs
    discogs_positive = correlate_preferences(positive, redis_client, db_url, verbose)
    discogs_negative = correlate_preferences(negative, redis_client, db_url, verbose)
    
    if not discogs_positive:
        print("No positive preferences could be correlated to Discogs IDs.", file=sys.stderr)
        print("Run: find <dir> -name page.html | ./correlate-local-pg.py --db $DATABASE_URL", file=sys.stderr)
        return []
    
    print(f"Correlated to {len(discogs_positive)} Discogs releases (positive)", file=sys.stderr)
    
    # Step 3: Traverse graph from positive seeds
    source_ids = list(discogs_positive.keys())
    print(f"Traversing from {len(source_ids)} seed releases at depth {depth}...", file=sys.stderr)
    
    releases = traverse(
        source_ids,
        depth=depth,
        max_pages_per_label=5,
        max_releases_per_artist=20
    )
    
    print(f"Found {len(releases)} connected releases", file=sys.stderr)
    
    # Step 4: Weight by IDF
    weighted = compute_weights(releases, source_ids)
    
    # Step 5: Apply preference scores and rank
    scored_releases = []
    for release in weighted:
        release_id = release.get('id')
        
        # Skip if in negative set
        if release_id in discogs_negative:
            continue
        
        # Boost by preference score
        base_score = release.get('score', 0)
        
        # If this is a seed release, use its preference score
        if release_id in discogs_positive:
            pref_score = discogs_positive[release_id]
            base_score *= (1 + pref_score / 5.0)  # Boost up to 2x for max preference
        
        release['score'] = base_score
        scored_releases.append(release)
    
    # Sort by score descending
    scored_releases.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    # Return top N
    return scored_releases[:top_n]


def main():
    parser = argparse.ArgumentParser(
        description='Pipeline: Bandcamp preferences → Discogs recommendations'
    )
    parser.add_argument(
        '-f', '--file',
        required=True,
        help='Preferences file (path score per line)'
    )
    parser.add_argument(
        '-d', '--depth',
        type=int,
        default=2,
        help='Traversal depth (default: 2)'
    )
    parser.add_argument(
        '-n', '--top-n',
        type=int,
        default=20,
        help='Number of recommendations (default: 20)'
    )
    parser.add_argument(
        '--db',
        default=os.environ.get('DATABASE_URL'),
        help='PostgreSQL URL (default: $DATABASE_URL)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output to stderr'
    )
    
    args = parser.parse_args()
    
    if not args.db:
        print("Error: Set --db or DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    
    recommendations = run_pipeline(
        args.file,
        args.db,
        depth=args.depth,
        top_n=args.top_n,
        verbose=args.verbose
    )
    
    # Output as JSON
    print(json.dumps(recommendations, indent=2))


if __name__ == '__main__':
    main()
