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
import time

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


def _clean(s):
    """Strip zero-width chars (Discogs data has them) and collapse whitespace."""
    if not s:
        return ''
    s = ''.join(c for c in s if c not in '\u200b\u200c\u200d\ufeff')
    return ' '.join(s.split())


def _fetch_meta(release_id):
    """Get title, artists, labels, year for a Discogs release (cached in Redis)."""
    r = fetch_release(release_id)
    if not r:
        return {'title': 'Unknown', 'artists': [], 'labels': [], 'year': None}
    return {
        'title': _clean(r.get('title', 'Unknown')) or 'Unknown',
        'artists': [_clean(a.get('name')) for a in r.get('artists', []) if _clean(a.get('name'))],
        'labels': [_clean(l.get('name')) for l in r.get('labels', [])
                   if _clean(l.get('name')) and l.get('name') != 'Not On Label'],
        'year': r.get('year'),
    }


def _bc_link(meta, release_id, wait=0, verbose=False):
    """Bandcamp URL for a suggestion: correlation cache first, then the
    resolver queue (Bandcamp search at a polite fixed rate).

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
        return bandcamp_url_for_release(
            search_artist, meta['title'],
            year=meta.get('year'), discogs_id=release_id,
            wait=wait, verbose=verbose)
    return None


def _resolver_running():
    """True if the resolver worker has a live pidfile."""
    pidfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'resolver.pid')
    try:
        with open(pidfile) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def _warn_no_local_db():
    import local_db
    if not local_db.available():
        print("[warn] local Discogs DB not found "
              f"({local_db.db_path()}) - falling back to the rate-limited "
              "Discogs API: expect slow runs and 429s.",
              file=sys.stderr)


def _load_ratings():
    """User weights (.data/ratings.json): normalized BC URL -> '3'|'4'|'5'.
    These are taste parameters, not popularity - they only ever scale a
    seed's own vote in rank fusion."""
    import json
    path = os.path.join(os.path.dirname(HERE), '.data', 'ratings.json')
    try:
        with open(path) as fh:
            return {u: v for u, v in json.load(fh).items() if v in ('3', '4', '5')}
    except (OSError, ValueError):
        return {}


def _load_purged():
    """Releases the user listened to and rejected (.data/purged.json).

    Purged is influential: a purged release is excluded from every future
    suggestion run - by Bandcamp URL, and by its Discogs release ID via the
    correlation cache (so it cannot re-enter through a different URL).
    It is a release-level veto, not a neighborhood poison: hating one
    Oráculo record says nothing about the rest of the label.
    """
    import json
    path = os.path.join(os.path.dirname(HERE), '.data', 'purged.json')
    try:
        with open(path) as fh:
            return {u for u, v in json.load(fh).items() if v}
    except (OSError, ValueError):
        return set()


def _norm_bc(url):
    from urllib.parse import urlsplit, urlunsplit
    p = urlsplit(url or '')
    return urlunsplit((p.scheme, p.netloc, p.path, '', '')).rstrip('/')


def _purge_exclusions(purged_urls, verbose=False):
    """Discogs release IDs behind purged Bandcamp URLs (via bc2dg)."""
    ids = set()
    if not purged_urls:
        return ids
    import re as _re
    import redis as _redis
    from bc import _slug_from_bc_url
    r = _redis.Redis(decode_responses=True)
    for u in purged_urls:
        stub = _slug_from_bc_url(u)
        dg = r.hget('bc2dg', stub) if stub else None
        m = _re.search(r'/release/(\d+)', dg or '')
        if m:
            ids.add(m.group(1))
    if verbose and ids:
        print(f"[purge] discogs ids blocked: {sorted(ids)}", file=sys.stderr)
    return ids


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
    p.add_argument('--all', action='store_true', dest='show_all',
                   help="Include suggestions without a Bandcamp URL (Discogs-only). "
                        "Default: only show releases available on Bandcamp.")
    p.add_argument('--wait-seconds', type=float, default=180,
                   help='Max seconds to wait for the resolver to find Bandcamp URLs (default 180; 0 = cache only)')
    p.add_argument('--no-search', action='store_true',
                   help='Only use the correlation cache; never queue Bandcamp searches')
    args = p.parse_args()

    purged_urls = _load_purged()
    purged_norm = {_norm_bc(u) for u in purged_urls}
    purged_rids = _purge_exclusions(purged_urls, verbose=args.verbose)

    from traverse_and_weight import (collect_source_ids, traverse, compute_idf_weights,
                                     dedupe_releases, fuse_reciprocal_rank)

    _warn_no_local_db()

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
        bc_urls = [u for u in urls if '.bandcamp.com' in u]
        if not bc_urls:
            print("No source releases could be resolved to Discogs IDs.", file=sys.stderr)
            sys.exit(1)
        # Seed(s) not in the Discogs catalog - fall back to Bandcamp's own
        # recommendation footer (co-ownership + neighborhood), all links
        # directly on Bandcamp.
        print("[note] seed not found in Discogs catalog - using Bandcamp's own "
              "recommendations (no graph traversal)", file=sys.stderr)
        from bc_rec import recommend
        items, seen = [], set()
        n_purged = 0
        for u in bc_urls:
            for it in recommend(u, verbose=args.verbose):
                if it['url'] not in seen:
                    seen.add(it['url'])
                    it['source'] = u
                    if _norm_bc(it['url']) in purged_norm:
                        n_purged += 1  # user vetoed this one: never again
                        continue
                    items.append(it)
        if n_purged and args.verbose:
            print(f"[purge] excluded {n_purged} purged suggestion(s)", file=sys.stderr)
        if args.top_n > 0:
            items = items[:args.top_n]
        if not items:
            print("No suggestions (Bandcamp has no recommendation footer for this album).",
                  file=sys.stderr)
            sys.exit(1)

        if args.json:
            import json
            print(json.dumps({
                'mode': 'bandcamp-native',
                'sources': bc_urls,
                'releases': [{
                    'title': it['title'], 'artists': [it['artist']],
                    'bandcamp_url': it['url'],
                    'source': it['source'],
                } for it in items],
            }, indent=2))
            return

        print(f"# {len(items)} suggestions (Bandcamp's own picks, not in Discogs)\n")
        for i, it in enumerate(items, 1):
            print(f"{i:>2}. {it['title']}")
            if it['artist']:
                print(f"     by {it['artist']}")
            print(f"     bandcamp: {it['url']}")
            print()
        return
    if args.verbose:
        print(f"Seeds: {source_ids}", file=sys.stderr)

    if len(source_ids) == 1:
        releases = traverse(
            source_ids,
            depth=args.depth,
            max_pages_per_label=args.max_pages_per_label,
            max_releases_per_artist=args.max_releases_per_artist,
        )
        weighted = compute_idf_weights(releases, source_ids)
        # Deduplicate by (artist, title); drop groups that are the seed albums
        weighted = dedupe_releases(weighted, source_ids)
    else:
        # Multi-seed: the collection is the query. Each seed gets its own
        # traversal + IDF (its own ranking); then lists merge with weighted
        # rank fusion. Seed weights come from the user's ratings (3/4/5,
        # unrated = 1); purged collection members are NEGATIVE parameters:
        # their vote subtracts. A dense neighborhood still cannot out-shout
        # a sparse one - only cross-seed consensus compounds.
        ratings_map = {}
        for u, v in _load_ratings().items():
            ratings_map[_norm_bc(u)] = float(v)
        weights, negative = {}, set()
        for sid in source_ids:
            n = _norm_bc(bandcamp_map.get(sid) or '')
            if not n:
                continue
            if n in purged_norm:
                negative.add(sid)
            elif n in ratings_map:
                weights[sid] = ratings_map[n]
        if args.verbose and (weights or negative):
            print(f"[query] seed weights: {weights} negative: {sorted(negative)}",
                  file=sys.stderr)
        per_seed = {}
        for sid in source_ids:
            if args.verbose:
                print(f"[multi] traversing seed {sid} ...", file=sys.stderr)
            rel = traverse(
                [sid],
                depth=args.depth,
                max_pages_per_label=args.max_pages_per_label,
                max_releases_per_artist=args.max_releases_per_artist,
            )
            w = compute_idf_weights(rel, [sid])
            # dedupe against ALL seeds: another seed's album is never a suggestion
            per_seed[sid] = dedupe_releases(w, source_ids)
        weighted = fuse_reciprocal_rank(per_seed, weights=weights,
                                        negative=negative)

    # Weighted list is already sorted by final_weight desc.
    results = weighted
    if purged_rids:
        kept = [r for r in results if str(r['release_id']) not in purged_rids]
        if args.verbose and len(kept) != len(results):
            print(f"[purge] excluded {len(results) - len(kept)} purged release(s)",
                  file=sys.stderr)
        results = kept

    # Resolve Bandcamp links: cache hits are instant; misses are queued to the
    # resolver worker (Bandcamp search at a fixed polite rate) and we wait up
    # to the budget for them to land. Default mode keeps only releases that
    # resolved to a Bandcamp URL; --all keeps the rest as Discogs-only.
    scan_cap = max(args.top_n * 3, 15) if args.top_n > 0 else 50
    candidates = results[:scan_cap]

    from resolver import enqueue, wait_for

    meta_by_rid = {}
    bc_by_rid = {}
    enqueued = []
    can_queue = (not args.no_search) and _resolver_running()
    if not args.no_search and not can_queue:
        print("[warn] resolver worker not running - Bandcamp links come from the "
              "cache only; start it with: python3 pipeline/resolver.py",
              file=sys.stderr)

    for r in candidates:
        rid = r['release_id']
        meta = _fetch_meta(rid)
        meta_by_rid[rid] = meta
        url = discogs_to_bc_url(rid)
        artists = meta.get('artists') or []
        labels = meta.get('labels') or []
        sa = artists[0] if artists else None
        if (not sa or sa.strip().lower() in ('various', 'various artists', '')) and labels:
            sa = labels[0]
        if not url and sa:
            # correlation-cache miss; check the reverse cache (instant)
            url = bandcamp_url_for_release(sa, meta['title'], wait=0,
                                           verbose=args.verbose)
            if url:
                if args.verbose:
                    print(f"[bc-reverse] {sa} - {meta['title']} -> {url}",
                          file=sys.stderr)
        if url:
            bc_by_rid[rid] = url
        elif can_queue and sa:
            from resolver import enqueue
            enqueue(sa, meta['title'], year=meta.get('year'), discogs_id=rid)
            if args.verbose:
                print(f"[queue] {sa} - {meta['title']}", file=sys.stderr)
            enqueued.append((rid, sa, meta['title']))

    if enqueued and args.wait_seconds > 0 and can_queue:
        deadline = time.time() + args.wait_seconds
        pending = list(enqueued)
        while pending and time.time() < deadline:
            still = []
            for rid, sa, title in pending:
                url = wait_for(sa, title, timeout=0)  # single cache check
                if url:
                    bc_by_rid[rid] = url
                else:
                    still.append((rid, sa, title))
            pending = still
            if len(bc_by_rid) >= args.top_n or not pending:
                break
            time.sleep(3)

    # Final list, in weight order. Default: only Bandcamp-resolved releases.
    final = []
    for r in candidates:
        rid = r['release_id']
        if rid in bc_by_rid:
            if _norm_bc(bc_by_rid[rid]) in purged_norm:
                continue  # resolved to a URL the user vetoed
            final.append(r)
            if args.top_n > 0 and len(final) >= args.top_n:
                break
    if args.show_all:
        have = {r['release_id'] for r in final}
        for r in candidates:
            if r['release_id'] not in have:
                final.append(r)
                if args.top_n > 0 and len(final) >= args.top_n:
                    break
    if not final:
        print("No suggestions (nothing on Bandcamp resolved yet - re-run later, "
              "the resolver keeps caching in the background).", file=sys.stderr)
        sys.exit(1)

    if args.json:
        import json
        for r in final:
            rid = r['release_id']
            meta = meta_by_rid.get(rid) or _fetch_meta(rid)
            r['title'] = meta['title']
            r['artists'] = meta['artists']
            r['labels'] = meta['labels']
            r['year'] = meta['year']
            r['bandcamp_url'] = bc_by_rid.get(rid)
        print(json.dumps({
            'source_ids': source_ids,
            'bandcamp_sources': bandcamp_map,
            'mode': 'discogs-graph-rrf' if len(source_ids) > 1 else 'discogs-graph',
            'releases': final,
        }, indent=2))
        return

    print(f"# {len(final)} suggestions from {len(source_ids)} source(s)\n")
    for i, r in enumerate(final, 1):
        rid = r['release_id']
        meta = meta_by_rid.get(rid) or _fetch_meta(rid)
        bc = bc_by_rid.get(rid)
        discogs = f"https://www.discogs.com/release/{rid}"

        print(f"{i:>2}. {meta['title']}  ({meta['year'] or '?'})")
        if meta['artists']:
            print(f"     by {', '.join(meta['artists'])}")
        if meta['labels']:
            print(f"     label: {', '.join(dict.fromkeys(meta['labels']))}")
        if bc:
            print(f"     bandcamp: {bc}")
        if r.get('seeds'):
            print(f"     from seeds: {', '.join(map(str, r['seeds']))}")
        print(f"     discogs : {discogs}")
        print(f"     score={r.get('final_weight', 0):.3f}  depth={r.get('depth', '?')}")
        print()


if __name__ == '__main__':
    main()
