#!/usr/bin/env python3
"""
Combined traverse and weight script.

Takes multiple source releases (Bandcamp or Discogs URLs), traverses the
graph to find connected releases, and computes IDF-weighted similarity
against ALL source releases combined.

Bandcamp URLs are first correlated to Discogs (Redis cache, local page.html,
or Discogs API search). Recommendations are emitted as Discogs URLs with a
best-effort Bandcamp URL when the correlation cache knows it.
"""
import sys
import os
import re
import json
import argparse
from dotenv import load_dotenv
load_dotenv()

# Add parent directory for imports (lib)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from our library
from lib import extract_release_ids
from traverse import traverse
from weight import compute_weights as compute_idf_weights
from bc import bc_url_to_discogs, discogs_to_bc_url

DISCOGS_RELEASE_RE = re.compile(r'discogs\.com/release/(\d+)')


def collect_source_ids(urls, verbose=False):
    """
    Resolve a mixed list of Bandcamp/Discogs release URLs to Discogs release IDs.

    Returns (source_ids, bandcamp_map) where bandcamp_map maps
    discogs_id -> bandcamp_url for sources that came from Bandcamp.
    """
    source_ids = []
    bandcamp_map = {}

    for url in urls:
        m = DISCOGS_RELEASE_RE.search(url)
        if m:
            if m.group(1) not in source_ids:
                source_ids.append(m.group(1))
            continue

        # Bandcamp URL (or anything else): correlate to Discogs
        discogs_url, _info = bc_url_to_discogs(url, verbose=verbose)
        if not discogs_url:
            if verbose:
                print(f"[TAW] no Discogs match for {url}", file=sys.stderr)
            continue
        dm = DISCOGS_RELEASE_RE.search(discogs_url)
        if dm and dm.group(1) not in source_ids:
            source_ids.append(dm.group(1))
            if '.bandcamp.com' in url:
                bandcamp_map[dm.group(1)] = url

    return source_ids, bandcamp_map


def annotate_with_bandcamp(releases, verbose=False):
    """Add bandcamp_url (best effort, from Redis correlation cache) to results."""
    for r in releases:
        rid = r.get('release_id')
        if rid is None:
            continue
        bc_url = discogs_to_bc_url(rid)
        if bc_url:
            r['bandcamp_url'] = bc_url
    return releases


def main():
    parser = argparse.ArgumentParser(description='Traverse and weight releases')
    parser.add_argument('urls', nargs='*', help='Bandcamp or Discogs release URLs')
    parser.add_argument('-f', '--file', help='File with release URLs (one per line)')
    parser.add_argument('-d', '--depth', type=int, default=2, help='Traversal depth')
    parser.add_argument('-n', '--top-n', type=int, default=0, help='Limit results (0 = all)')
    parser.add_argument('--max-pages-per-label', type=int, default=5, help='Max pages per label')
    parser.add_argument('--max-releases-per-artist', type=int, default=20, help='Max releases per artist')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose progress on stderr')
    
    args = parser.parse_args()
    
    # Load URLs from file and/or args
    urls = []
    if args.file:
        with open(args.file) as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    urls.extend([u for u in args.urls if u.startswith('https://')])
    
    if not urls:
        parser.print_help()
        sys.exit(1)
    
    # Correlate Bandcamp URLs to Discogs IDs
    source_ids, bandcamp_map = collect_source_ids(urls, verbose=args.verbose)
    
    if not source_ids:
        print("No source releases could be resolved to Discogs IDs.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting from {len(source_ids)} source releases: {source_ids}", file=sys.stderr)
    if bandcamp_map:
        print(f"  (from Bandcamp: {len(bandcamp_map)})", file=sys.stderr)
    
    # Traverse - use function from traverse.py
    releases = traverse(
        source_ids, 
        depth=args.depth,
        max_pages_per_label=args.max_pages_per_label,
        max_releases_per_artist=args.max_releases_per_artist
    )
    
    # Weight - use function from weight.py
    weighted = compute_idf_weights(releases, source_ids)
    
    # Deduplicate: Discogs lists one album as many releases (formats/editions),
    # and label catalogs repeat reissues. Group by (artist, title) keeping the
    # highest-weighted release per group.
    groups = {}
    for r in weighted:
        # r['contributors'] are the primary artist IDs (from traverse.py)
        artists = tuple(sorted(set(r.get('contributors', []))))
        key = (artists, r.get('title', '').strip().lower())
        if key not in groups:
            groups[key] = r
    
    weighted = list(groups.values())
    if args.top_n > 0:
        weighted = weighted[:args.top_n]
    
    annotate_with_bandcamp(weighted, verbose=args.verbose)
    
    # Output
    output = {
        "source_ids": source_ids,
        "bandcamp_sources": bandcamp_map,
        "releases": weighted
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()