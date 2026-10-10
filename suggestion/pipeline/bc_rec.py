#!/usr/bin/env python3
"""
Bandcamp-native recommendations (no Discogs).

Bandcamp's album pages carry a server-rendered recommendation footer:
cross-collection picks (footer-cc, with "N fans who also own this" co-ownership
counts) and neighborhood picks (footer-nn). For releases that are not in the
Discogs catalog at all, these are the only graph edges we have - so they
become the recommender.

Usage:
    ./bc_rec.py https://wernerkarloff.bandcamp.com/album/atemporal-ep-2
"""
import re
import sys
from html import unescape
from urllib.parse import urlsplit, urlunsplit


def _clean(s):
    if not s:
        return ''
    s = ''.join(c for c in s if c not in '\u200b\u200c\u200d\ufeff')
    return ' '.join(unescape(s).split())


def fetch_page(url, verbose=False):
    from bc import _fetch_page
    return _fetch_page(url)


def parse_recommendations(html, verbose=False):
    """Parse the footer recommendation blocks of a Bandcamp album page.

    Returns a list of dicts: {url, title, artist, kind} in page order.
    kind: 'cc' = cross-collection, 'nn' = neighborhood.

    NOTE: Bandcamp shows "N fans also own" counts on some of these. We
    deliberately ignore them: this project does not rank by popularity,
    so the count is neither a score nor a sort key. Page order is kept.
    """
    items = []
    for m in re.finditer(r'<li class="recommended-album footer-(\w+)"', html):
        kind = m.group(1)
        start = m.start()
        end = html.find('</li>', start)
        block = html[start:end if end != -1 else start + 6000]

        def _attr(name):
            mm = re.search(rf'data-{name}="([^"]*)"', block)
            return _clean(mm.group(1)) if mm else ''

        title = _attr('albumtitle')
        artist = _attr('artist')
        if not title:
            continue

        um = re.search(r'href="(https://[^"]+?\.bandcamp\.com/[^"]*?)"', block)
        if not um:
            continue
        url = unescape(um.group(1))
        # strip bandcamp's ?from=... tracking params
        parts = urlsplit(url)
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))

        items.append({
            'url': url,
            'title': title,
            'artist': artist,
            'kind': kind,
        })
    return items


def recommend(album_url, limit=0, verbose=False):
    """Bandcamp's own footer recommendations for an album, in page order.

    No re-ranking: popularity (fan co-ownership counts) is not a signal in
    this project, so Bandcamp's own presentation order is preserved.
    limit=0 means no cap.
    """
    html = fetch_page(album_url, verbose=verbose)
    if not html:
        if verbose:
            print(f"[bc-rec] could not fetch {album_url}", file=sys.stderr)
        return []
    items = parse_recommendations(html)
    if limit > 0:
        items = items[:limit]
    return items


def parse_label_catalog(html, base_url):
    """Parse a Bandcamp label page's music grid (data-client-items JSON).

    Returns list of dicts: {url, title, artist, type} in page order
    (Bandcamp's own presentation, which is newest-first by default).
    """
    import json as _json
    m = re.search(r'data-client-items="([^"]*)"', html)
    if not m:
        return []
    try:
        items = _json.loads(unescape(m.group(1)))
    except ValueError:
        return []
    sub = urlsplit(base_url).netloc
    out = []
    for it in items:
        if it.get('type') not in ('album', 'track', 'playlist'):
            continue
        url = urlunsplit(('https', sub, it.get('page_url', ''), '', ''))
        out.append({
            'url': url,
            'title': _clean(it.get('title', '')),
            'artist': _clean(it.get('artist', '')),
            'type': it.get('type'),
        })
    return out


def label_catalog(label_url, verbose=False):
    """All items on a Bandcamp label page, in page order."""
    html = fetch_page(label_url, verbose=verbose)
    if not html:
        if verbose:
            print(f"[bc-rec] could not fetch {label_url}", file=sys.stderr)
        return []
    return parse_label_catalog(html, label_url)


def main():
    import argparse
    p = argparse.ArgumentParser(description='Bandcamp-native album recommendations')
    p.add_argument('urls', nargs='+')
    p.add_argument('-n', '--limit', type=int, default=0)
    p.add_argument('-v', '--verbose', action='store_true')
    args = p.parse_args()

    seen = set()
    all_items = []
    for url in args.urls:
        for it in recommend(url, limit=0, verbose=args.verbose):
            if it['url'] not in seen:
                seen.add(it['url'])
                it['source'] = url
                all_items.append(it)
    if args.limit > 0:
        all_items = all_items[:args.limit]

    for i, it in enumerate(all_items, 1):
        print(f"{i:>2}. {it['title']}  by {it['artist']}")
        print(f"     {it['url']}")


if __name__ == '__main__':
    main()
