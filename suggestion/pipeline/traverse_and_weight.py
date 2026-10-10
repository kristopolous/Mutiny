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


def _norm_title(s):
    """Normalize a title for grouping: strip zero-width chars, collapse spaces."""
    s = s or ''
    s = ''.join(c for c in s if c not in '\u200b\u200c\u200d\ufeff')
    return ' '.join(s.split()).lower()


def dedupe_releases(weighted, source_ids):
    """Collapse Discogs format/edition duplicates into one release per album.

    Groups by (primary artist IDs, normalized title), keeping the first member
    (input must be sorted by weight desc). A group is dropped entirely if it
    is a seed album: either a member is a source release, or the group matches
    the (contributors, title) of a source release - the latter catches the
    case where the source's own ID was already removed from the list but its
    other editions (different Discogs release IDs) remain.
    """
    import local_db

    source_ids_set = set(str(sid) for sid in source_ids)

    # Identify the seed albums themselves, so their sibling editions are dropped.
    fetch = local_db.fetch_release if local_db.available() else None
    if fetch is None:
        import traverse as _traverse_mod  # module-level fetch_release (API)
        fetch = _traverse_mod.fetch_release
    seed_album_keys = set()
    for sid in source_ids:
        try:
            sr = fetch(int(sid))
        except (ValueError, TypeError):
            continue
        if not sr:
            continue
        contribs = tuple(sorted(a['id'] for a in sr.get('artists', []) if a.get('id')))
        seed_album_keys.add((contribs, _norm_title(sr.get('title'))))

    groups = {}
    group_is_seed = {}
    for r in weighted:
        # r['contributors'] are the primary artist IDs (from traverse.py)
        artists = tuple(sorted(set(r.get('contributors', []))))
        key = (artists, _norm_title(r.get('title')))
        is_seed = (str(r.get('release_id')) in source_ids_set) or (key in seed_album_keys)
        if key not in groups:
            groups[key] = r
            group_is_seed[key] = is_seed
        else:
            group_is_seed[key] = group_is_seed[key] or is_seed
    return [groups[k] for k in groups if not group_is_seed[k]]


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
    
    # Deduplicate: one album = many Discogs releases (formats/editions/reissues)
    weighted = dedupe_releases(weighted, source_ids)
    
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