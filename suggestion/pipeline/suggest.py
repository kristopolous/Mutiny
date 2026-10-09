#!/usr/bin/env python3
"""
suggest: Bandcamp URLs in -> recommended releases out (with Bandcamp links).

    ./suggest.py -n 10 https://detund.bandcamp.com/album/multiverse [more...]

Each source URL is correlated to Discogs, the graph is traversed + weighted,
results are deduplicated, then every suggestion is enriched with artist,
label, year, and a Bandcamp URL (cache hit, or on-the-fly Bandcamp search).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # suggestion/ for ingest/config/cache

from bc import discogs_to_bc_url, bandcamp_url_for_release
from ingest import fetch_release as _api_fetch_release


def fetch_release(release_id):
    """Prefer the local SQLite Discogs dump (offline, no rate limits)."""
    try:
        import local_db
        if local_db.available():
            data = local_db.fetch_release(release_id)
            if data:
                return data
    except Exception:
        pass
    return _api_fetch_release(release_id)


def _fetch_meta(release_id):
    """Get title, artists, labels, year for a Discogs release (cached in Redis)."""
    r = fetch_release(release_id)
    if not r:
        return {'title': 'Unknown', 'artists': [], 'labels': [], 'year': None}
    return {
        'title': r.get('title', 'Unknown'),
        'artists': [a.get('name', '') for a in r.get('artists', []) if a.get('name')],
        'labels': [l.get('name', '') for l in r.get('labels', [])
                   if l.get('name') and l.get('name') != 'Not On Label'],
        'year': r.get('year'),
    }


def _bc_link(meta, release_id):
    """Bandcamp URL for a suggestion: cache first, then a live search (cached).

    Uses the primary artist; falls back to the label name when the artist is
    a placeholder like 'Various' (compilations are usually on the label page).
    """
    url = discogs_to_bc_url(release_id)
    if url:
        return url
    artists = meta.get('artists') or []
    labels = meta.get('labels') or []
    search_artist = artists[0] if artists else None
    if (not search_artist or search_artist.strip().lower() in ('various', 'various artists', '')) and labels:
        search_artist = labels[0]
    if search_artist:
        return bandcamp_url_for_release(search_artist, meta['title'])
    return None


def main():
    import argparse
    p = argparse.ArgumentParser(
        description='Suggest music from Bandcamp release URLs (via Discogs graph)'
    )
    p.add_argument('urls', nargs='+', help='Bandcamp (or Discogs) release URLs')
    p.add_argument('-f', '--file', help='File with release URLs (one per line)')
    p.add_argument('-n', '--top-n', type=int, default=10, help='Number of results (default 10)')
    p.add_argument('-d', '--depth', type=int, default=2, help='Traversal depth (default 2)')
    p.add_argument('--max-pages-per-label', type=int, default=5)
    p.add_argument('--max-releases-per-artist', type=int, default=20)
    p.add_argument('-v', '--verbose', action='store_true')
    p.add_argument('--json', action='store_true', help='Emit raw JSON instead of a table')
    p.add_argument('--no-search', action='store_true',
                   help='Skip on-the-fly Bandcamp search (only use the correlation cache)')
    args = p.parse_args()

    from traverse_and_weight import collect_source_ids, traverse, compute_idf_weights

    urls = []
    if args.file:
        with open(args.file) as fh:
            urls = [line.strip() for line in fh if line.strip() and not line.startswith('#')]
    urls.extend(args.urls)
    if not urls:
        p.print_help()
        sys.exit(1)

    source_ids, bandcamp_map = collect_source_ids(urls, verbose=args.verbose)
    if not source_ids:
        print("No source releases could be resolved to Discogs IDs.", file=sys.stderr)
        sys.exit(1)
    if args.verbose:
        print(f"Seeds: {source_ids}", file=sys.stderr)

    releases = traverse(
        source_ids,
        depth=args.depth,
        max_pages_per_label=args.max_pages_per_label,
        max_releases_per_artist=args.max_releases_per_artist,
    )
    weighted = compute_idf_weights(releases, source_ids)

    # Deduplicate by (artist, title)
    groups = {}
    for r in weighted:
        artists = tuple(sorted(set(r.get('contributors', []))))
        key = (artists, r.get('title', '').strip().lower())
        if key not in groups:
            groups[key] = r
    # Keep only the highest-weighted entries before any per-release fetching
    # (each fetch hits the Discogs API and is rate-limited).
    results = list(groups.values())
    results.sort(key=lambda r: r.get('final_weight', 0), reverse=True)
    if args.top_n > 0:
        results = results[:args.top_n]

    if args.json:
        import json
        for r in results:
            meta = _fetch_meta(r['release_id'])
            r['title'] = meta['title']
            r['artists'] = meta['artists']
            r['labels'] = meta['labels']
            r['year'] = meta['year']
            r['bandcamp_url'] = _bc_link(meta, r['release_id']) if not args.no_search else discogs_to_bc_url(r['release_id'])
        print(json.dumps({
            'source_ids': source_ids,
            'bandcamp_sources': bandcamp_map,
            'releases': results,
        }, indent=2))
        return

    print(f"# {len(results)} suggestions from {len(source_ids)} source(s)\n")
    for i, r in enumerate(results, 1):
        rid = r['release_id']
        meta = _fetch_meta(rid)
        bc = None if args.no_search else _bc_link(meta, rid)
        discogs = f"https://www.discogs.com/release/{rid}"

        print(f"{i:>2}. {meta['title']}  ({meta['year'] or '?'})")
        if meta['artists']:
            print(f"     by {', '.join(meta['artists'])}")
        if meta['labels']:
            print(f"     label: {', '.join(dict.fromkeys(meta['labels']))}")
        if bc:
            print(f"     bandcamp: {bc}")
        print(f"     discogs : {discogs}")
        print(f"     score={r.get('final_weight', 0):.3f}  depth={r.get('depth', '?')}")
        print()


if __name__ == '__main__':
    main()
