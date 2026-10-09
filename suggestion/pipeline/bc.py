#!/usr/bin/env python3
"""
Bandcamp <-> Discogs correlation, no local database needed.

- bc_url_to_discogs(url): Bandcamp release URL -> Discogs release URL
  Uses Redis cache first, then local page.html, then fetches the page and
  searches the Discogs API.

- discogs_to_bc_url(release_id): reverse lookup from the Redis correlation
  cache (populated by previous bc_url_to_discogs runs).
"""
import os
import re
import sys
from difflib import SequenceMatcher
from html import unescape
from urllib.parse import urlparse

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingest import make_discogs_request
from cache import get_redis_client

load_dotenv()

MIN_CONFIDENCE = 0.6


_redis = get_redis_client


def _to_str(v):
    if v is None:
        return None
    return v.decode('utf-8') if isinstance(v, bytes) else str(v)


def _slug_from_bc_url(url):
    """
    Bandcamp release URL -> cache stub.

    The local music tree is keyed by the BANDCAMP subdomain (label or artist
    name, e.g. 'detund'), NOT the path segments, so the stub is
    <subdomain>/<last-path-segment>:

        detund.bandcamp.com/album/multiverse -> detund/multiverse
        abroxis.bandcamp.com/multiverse      -> abroxis/multiverse
    """
    p = urlparse(url)
    host = p.netloc.lower().split(':')[0]
    if host.endswith('.bandcamp.com') and host != 'bandcamp.com':
        subdomain = host.split('.')[0]
    else:
        return None
    parts = [seg for seg in p.path.split('/') if seg]
    if not parts:
        return None
    return f"{subdomain}/{parts[-1]}"


def _sim(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def parse_description(content):
    """Parse Bandcamp meta description: 'Title by Artist, released DATE\\n1. ...'"""
    content = unescape(content.strip())
    lines = content.split('\n')
    if not lines:
        return None

    match = re.match(r'(.+?)\s+by\s+(.+?),\s+release[sd]\s+(.+)', lines[0].strip())
    if not match:
        return None

    release_name, artist_name, released_date = (g.strip() for g in match.groups())

    track_list = []
    for line in lines[1:]:
        m = re.match(r'^\d+\.\s+(.+)$', line.strip())
        if m:
            track_list.append(m.group(1).strip())

    year = None
    ym = re.search(r'\b(19|20)\d{2}\b', released_date)
    if ym:
        year = ym.group(0)

    return {
        'artist_name': artist_name,
        'release_name': release_name,
        'track_list': track_list,
        'year': year,
        'raw_description': content,
    }


def parse_bc_page(html):
    """Extract meta description from a Bandcamp page and parse it."""
    m = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
    if not m:
        return None
    return parse_description(m.group(1))


def _search_discogs(parsed):
    """Search Discogs and return candidate releases (API dicts).

    Uses the local SQLite dump when available (offline, no rate limits),
    falling back to the Discogs API otherwise.

    API path uses a plain-text query: the field-qualified form (artist: ...
    release_title: ...) is unreliable (quotes and multi-word terms get
    mangled), and plain text still matches artist + title.
    """
    # Local DB search first (no rate limit, works offline)
    try:
        import local_db
        if local_db.available():
            rows = local_db.search_releases(
                artist=parsed['artist_name'],
                title=parsed['release_name'],
                year=parsed.get('year'),
                limit=30)
            return [
                {"id": r["id"], "title": r["title"],
                 "artist_name": r["artist"],
                 "year": (r["released"] or "")[:4] or None}
                for r in rows
            ]
    except Exception:
        pass

    query = f"{parsed['artist_name']} {parsed['release_name']}"
    if parsed.get('year'):
        query += f" {parsed['year']}"

    from urllib.parse import quote
    data = make_discogs_request(
        f"database/search?type=release&q={quote(query)}&per_page=30"
    )
    if not data:
        return []
    return data.get('results', [])


def _fetch_full(release_id):
    # Local DB first (no rate limit, works offline)
    try:
        import local_db
        if local_db.available():
            data = local_db.fetch_release(release_id)
            if data:
                return data
    except Exception:
        pass
    return make_discogs_request(f"releases/{release_id}") or {}


def _label_matches_subdomain(candidate, subdomain):
    """Does any of the candidate's labels look like the Bandcamp subdomain?

    Subdomains are slugs (dashes, no spaces) so compare slugified names:
    detroitunderground matches 'Detroit Underground'.
    """
    full = _fetch_full(candidate.get('id'))
    labels = full.get('labels') or candidate.get('labels') or []
    if isinstance(labels, dict):
        labels = [labels]
    def slug(s):
        return re.sub(r'[^a-z0-9]+', '', s.lower())
    target = slug(subdomain)
    for l in labels:
        name = l.get('name') if isinstance(l, dict) else getattr(l, 'name', '')
        if name and name.lower() != 'not on label' and slug(name) == target:
            return True
    return False


def score_match(parsed, candidate):
    """Score a parsed Bandcamp release against a Discogs candidate."""
    full = _fetch_full(candidate.get('id'))
    title = full.get('title') or candidate.get('title') or ''
    artists = [a.get('name', '') for a in full.get('artists', [])]
    if not artists:
        artists = [candidate.get('artist_name', '')]
    tracks = [t.get('title', '') for t in full.get('tracklist', [])]
    year = full.get('year')

    artist_scores = [_sim(parsed['artist_name'], a) for a in artists]
    scores = {
        'artist': max(artist_scores) if artist_scores else 0.0,
        'title': _sim(parsed['release_name'], title),
        'tracks': 0.0,
        'year': 1.0 if (parsed.get('year') and year and str(year) == str(parsed['year'])) else 0.0,
    }

    if parsed['track_list'] and tracks:
        matched = sum(
            1 for pt in parsed['track_list']
            if any(_sim(pt, dt) >= 0.8 for dt in tracks)
        )
        scores['tracks'] = matched / max(len(parsed['track_list']), len(tracks))

    confidence = (
        scores['artist'] * 1.0 +
        scores['title'] * 1.0 +
        scores['tracks'] * 0.5 +
        scores['year'] * 0.5
    ) / 3.0

    return min(1.0, confidence), title, artists, scores


def bc_url_to_discogs(url, verbose=False):
    """
    Map a Bandcamp release URL to a Discogs release URL.

    Returns (discogs_url, info) where info has 'source' ('cache'/'local'/'api')
    and 'confidence'. Returns (None, None) when no match is found.
    """
    stub = _slug_from_bc_url(url)
    if not stub:
        if verbose:
            print(f"[BC] not a bandcamp release url: {url}", file=sys.stderr)
        return None, None

    r = _redis()

    cached = _to_str(r.hget('bc2dg', stub))
    if cached:
        if verbose:
            print(f"[BC][cache] {stub} -> {cached}", file=sys.stderr)
        return cached, {'source': 'cache', 'confidence': 1.0}

    if r.sismember('bc2fail', stub):
        if verbose:
            print(f"[BC][fail-cache] {stub}", file=sys.stderr)
        return None, None

    # Try a local page.html: <base>/<label>/<release>/page.html
    local_html = None
    base = os.environ.get('MUTINY_BASE')
    if base and os.path.isfile(os.path.join(base, stub, 'page.html')):
        local_html = os.path.join(base, stub, 'page.html')
    elif os.path.isfile(os.path.join(stub, 'page.html')):
        local_html = os.path.join(stub, 'page.html')

    parsed = None
    if local_html:
        with open(local_html, encoding='utf-8') as f:
            parsed = parse_bc_page(f.read())
        if verbose:
            print(f"[BC][local] {stub}", file=sys.stderr)
    else:
        html = _fetch_page(url)
        if html is None:
            r.sadd('bc2fail', stub)
            if verbose:
                print(f"[BC][fetch-fail] {stub}", file=sys.stderr)
            return None, None
        parsed = parse_bc_page(html)
        if verbose:
            print(f"[BC][fetched] {stub} artist={parsed['artist_name'] if parsed else '?'}", file=sys.stderr)

    if not parsed:
        r.sadd('bc2fail', stub)
        if verbose:
            print(f"[BC][parse-fail] {stub}", file=sys.stderr)
        return None, None

    candidates = _search_discogs(parsed)
    if not candidates:
        r.sadd('bc2fail', stub)
        if verbose:
            print(f"[BC][no-results] {stub} {parsed['artist_name']} - {parsed['release_name']}", file=sys.stderr)
        return None, None

    scored = []
    for c in candidates:
        try:
            conf, title, artists, _ = score_match(parsed, c)
        except Exception as e:
            if verbose:
                print(f"[BC][score-error] {c.get('id')}: {e}", file=sys.stderr)
            continue
        if verbose:
            print(f"[BC]   candidate {c.get('id')} {title[:50]:50} conf={conf:.2f}", file=sys.stderr)
        if conf >= MIN_CONFIDENCE:
            scored.append((conf, c))

    # Ties happen (reissues / label + artist pages list the same album).
    # Prefer the candidate whose label matches the Bandcamp subdomain.
    subdomain = stub.split('/')[0].lower()
    scored.sort(key=lambda x: (
        -(1 if _label_matches_subdomain(x[1], subdomain) else 0),
        -x[0],
    ))

    if not scored:
        best = None
    else:
        conf, c = scored[0]
        best = (conf, c)

    if not best:
        r.sadd('bc2fail', stub)
        if verbose:
            print(f"[BC][no-match] {stub} {parsed['artist_name']} - {parsed['release_name']}", file=sys.stderr)
        return None, None

    conf, c = best
    release_id = c.get('id')
    discogs_url = f"https://www.discogs.com/release/{release_id}"
    r.srem('bc2fail', stub)
    r.hset('bc2dg', stub, discogs_url)
    info = {'source': 'local' if local_html else 'api', 'confidence': round(conf, 3)}
    if verbose:
        print(f"[BC][match] {stub} -> {discogs_url} ({conf:.0%})", file=sys.stderr)
    return discogs_url, info


def _fetch_page(url):
    """Fetch a Bandcamp page straight from the web (not the Discogs API)."""
    import requests
    headers = {
        'User-Agent': os.getenv('DISCOGS_USER_AGENT', 'MusicRecommender/1.0')
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code == 200:
            return resp.text
    except requests.RequestException:
        pass
    return None


def discogs_to_bc_url(release_id):
    """Reverse lookup: Discogs release ID -> Bandcamp URL (Redis cache only)."""
    r = _redis()
    for key, val in r.hscan_iter('bc2dg'):
        if _to_str(val).rstrip('/').split('/')[-1] == str(release_id):
            return f"https://{_bc_domain(_to_str(key))}.bandcamp.com/album/{_to_str(key).split('/')[-1]}"
    return None


def _bc_domain(stub):
    """The Bandcamp subdomain for a <subdomain>/<release-slug> stub."""
    return stub.split('/')[0]


def _title_in_description(parsed, title):
    """Lightweight check: does the page's release name match the expected title?"""
    page_title = (parsed.get('release_name') or '').strip().lower()
    want = title.strip().lower()
    if not page_title or not want:
        return False
    # Exact or near match
    if page_title == want:
        return True
    # Handle 'compilation vol. 1' vs 'compilation vol 1' style diffs
    norm = lambda s: re.sub(r'[^a-z0-9]', '', s)
    return norm(page_title) == norm(want)


def bandcamp_url_for_release(artist, title, verbose=False):
    """
    Find a Bandcamp URL for an (artist, title) pair that isn't in the
    correlation cache. Searches DuckDuckGo HTML for a site:bandcamp.com page,
    then verifies it via a Discogs search (reuses the same scoring).

    Results (and misses) are cached in Redis under 'bc:reverse' so repeated
    runs are instant. Returns a Bandcamp URL or None.
    """
    import requests
    from urllib.parse import urlparse, unquote

    r = _redis()
    key = f"bc:reverse:{artist.lower()}|{title.lower()}"
    cached = _to_str(r.get(key))
    if cached:
        if cached == '__none__':
            return None
        return cached

    # Plain query works better than the site: operator on DDG HTML.
    # DDG serves a reduced page (and often no results) to bot-like user agents,
    # so use a real browser UA for the search fetch.
    query = f"{artist} {title} bandcamp"
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'}
    bc_urls = []
    seen = set()
    resp = None
    # DDG rate-limits with HTTP 202 (a reduced/queued page with no results).
    # Retry a couple of times with backoff before giving up.
    import time
    for attempt in range(3):
        try:
            resp = requests.get(
                'https://html.duckduckgo.com/html/',
                params={'q': query},
                headers=headers,
                timeout=20,
            )
        except requests.RequestException as e:
            if verbose:
                print(f"[BC-REV] search failed for {artist} - {title}: {e}", file=sys.stderr)
            resp = None
            break
        if resp.status_code == 202:
            if verbose:
                print(f"[BC-REV] rate-limited (202), retry {attempt + 1}/3 for {artist} - {title}", file=sys.stderr)
            time.sleep(15 * (attempt + 1))
            continue
        break

    if resp is not None and resp.status_code == 200:
        # DDG wraps result URLs: href="...uddg=<urlencoded>"
        for m in re.finditer(r'uddg=([^"&]+)', resp.text):
            url = unquote(m.group(1))
            p = urlparse(url)
            host = p.netloc.lower().split(':')[0]
            if not (host.endswith('.bandcamp.com') and host != 'bandcamp.com'):
                continue
            if '/album/' not in url and not re.search(r'bandcamp\.com/[^/]+/?$', url):
                continue
            if url not in seen:
                seen.add(url)
                bc_urls.append(url)

    # No candidate URLs: this is usually DDG rate-limiting (a reduced page
    # with no results), NOT a real "not found". Don't cache the miss so the
    # next run retries instead of being poisoned for a week.
    if not bc_urls:
        if verbose:
            print(f"[BC-REV] no bandcamp urls for {artist} - {title} (not caching miss)", file=sys.stderr)
        return None

    # Parse candidate pages and verify. Prefer Discogs scoring; fall back to
    # a simple title match on the page's own meta description if the Discogs
    # API is unavailable (rate-limited).
    title_matched_any = False
    for url in bc_urls[:5]:
        html = _fetch_page(url)
        if not html:
            continue
        parsed = parse_bc_page(html)
        if not parsed:
            continue
        # Primary: full Discogs verification
        candidates = _search_discogs(parsed)
        if candidates:
            best_conf, best_c = 0.0, None
            for c in candidates[:10]:
                try:
                    conf, _t, _a, _s = score_match(parsed, c)
                except Exception:
                    continue
                if conf >= MIN_CONFIDENCE and conf > best_conf:
                    best_conf, best_c = conf, c
            if best_c is not None:
                subdomain = urlparse(url).netloc.lower().split(':')[0].split('.')[0]
                stub = f"{subdomain}/{urlparse(url).path.strip('/').split('/')[-1]}"
                discogs_url = f"https://www.discogs.com/release/{best_c.get('id')}"
                r.hset('bc2dg', stub, discogs_url)
                if verbose:
                    print(f"[BC-REV] {artist} - {title} -> {url} (via {discogs_url}, {best_conf:.0%})", file=sys.stderr)
                r.setex(key, 604800, url)
                return url
        # Fallback: accept if the page's description matches the title
        elif _title_in_description(parsed, title):
            subdomain = urlparse(url).netloc.lower().split(':')[0].split('.')[0]
            stub = f"{subdomain}/{urlparse(url).path.strip('/').split('/')[-1]}"
            if verbose:
                print(f"[BC-REV] {artist} - {title} -> {url} (title match, no Discogs verify)", file=sys.stderr)
            r.setex(key, 604800, url)
            return url
        title_matched_any = title_matched_any or _title_in_description(parsed, title)

    # Reached here only if URLs existed but none verified. Cache a short-lived
    # miss only when the title genuinely mismatched (a real "not found"); if
    # the title matched but Discogs verification was unavailable, don't cache
    # so a later run can retry once the API recovers.
    if not title_matched_any:
        r.setex(key, 3600, '__none__')
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Bandcamp URL -> Discogs release URL')
    parser.add_argument('urls', nargs='+', help='Bandcamp release URLs')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    for url in args.urls:
        discogs_url, _ = bc_url_to_discogs(url, verbose=args.verbose)
        if discogs_url:
            print(discogs_url)
        else:
            print(f"# no match: {url}", file=sys.stderr)


if __name__ == '__main__':
    main()
