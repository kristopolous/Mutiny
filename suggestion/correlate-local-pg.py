#!/usr/bin/env python3
"""
Match Bandcamp releases to Discogs using a local PostgreSQL database.

Reads paths from stdin, caches results in Redis (same as correlate.py).

Usage:
    find <bandcamp-dir> -name page.html | python correlate-local-pg.py --db postgresql://... [-v]
"""

import argparse
import json
import os
import re
import sys
import psycopg2
import redis
from html import unescape
from difflib import SequenceMatcher


def get_redis_client():
    """Get Redis client."""
    return redis.Redis(decode_responses=False)


def calculate_similarity(a, b):
    """Calculate similarity ratio between two strings."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def parse_description(content):
    """Parse the meta description tag content to extract release metadata."""
    content = unescape(content.strip())
    lines = content.split('\n')
    if not lines:
        return None

    first_line = lines[0].strip()

    match = re.match(r'(.+?)\s+by\s+(.+?),\s+release[sd]\s+(.+)', first_line)
    if match:
        release_name = match.group(1).strip()
        artist_name = match.group(2).strip()
        released_date = match.group(3).strip()

        track_list = []
        for line in lines[1:]:
            line = line.strip()
            if line and re.match(r'^\d+\.', line):
                track_match = re.match(r'^\d+\.\s+(.+)$', line)
                if track_match:
                    track_list.append(track_match.group(1).strip())

        return {
            'artist_name': artist_name,
            'release_name': release_name,
            'track_list': track_list,
            'year': released_date[-4:] if len(released_date) >= 4 else None,
            'raw_description': content
        }

    return None


def resolve_html_path(path):
    """Resolve input path to an actual page.html file."""
    if os.path.isfile(path):
        return path
    if os.path.isdir(path):
        page_html = os.path.join(path, 'page.html')
        if os.path.isfile(page_html):
            return page_html
    return None


def search_discogs_local(conn, parsed_data, limit=100):
    """Search the local Discogs database for matching releases."""
    cursor = conn.cursor()
    
    artist_name = parsed_data['artist_name']
    release_name = parsed_data['release_name']
    
    # Extract key words from title
    skip_words = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'for', 'to', 'at', 'by', 'with'}
    title_words = [w for w in release_name.lower().split() if w not in skip_words and len(w) > 2]
    
    # Search using release_artists table
    query = '''
        SELECT DISTINCT r.id, r.title, r.released, r.year,
               (SELECT array_agg(a.name) FROM release_artists ra
                JOIN artists a ON a.id = ra.artist_id WHERE ra.release_id = r.id) as artist_names
        FROM releases r
        JOIN release_artists ra ON r.id = ra.release_id
        JOIN artists a ON a.id = ra.artist_id
        WHERE a.name ILIKE %s
    '''
    params = [f'%{artist_name}%']
    
    # Add title conditions
    if title_words:
        title_conditions = ' OR '.join([f'r.title ILIKE %s' for _ in title_words[:3]])
        query += f' AND ({title_conditions})'
        params.extend([f'%{w}%' for w in title_words[:3]])
    
    query += ' LIMIT %s'
    params.append(limit)
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    releases = []
    for row in results:
        releases.append({
            'id': row[0],
            'title': row[1],
            'released': row[2],
            'year': row[3],
            'artists': row[4] or []
        })
    
    return releases


def get_release_tracks(conn, release_id):
    """Get tracklist for a release."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT title FROM tracks WHERE release_id = %s ORDER BY id
    ''', (release_id,))
    
    return [row[0] for row in cursor.fetchall()]


def score_match(parsed_data, discogs_release, tracklist):
    """Score a match between parsed data and Discogs release."""
    reasons = []
    scores = {}

    discogs_title = discogs_release.get('title', '')
    discogs_artists = discogs_release.get('artists', [])
    discogs_year = discogs_release.get('year')

    # Artist scoring
    artist_scores = []
    for discogs_artist in discogs_artists:
        artist_sim = calculate_similarity(parsed_data['artist_name'], discogs_artist)
        artist_scores.append(artist_sim)
        if artist_sim >= 0.8:
            reasons.append(f"Artist match: {discogs_artist}")
    scores['artist'] = max(artist_scores) if artist_scores else 0

    # Title scoring
    title_sim = calculate_similarity(parsed_data['release_name'], discogs_title)
    scores['title'] = title_sim
    if title_sim >= 0.8:
        reasons.append(f"Title match: {discogs_title}")

    # Track scoring
    track_matches = 0
    for parsed_track in parsed_data['track_list']:
        for discogs_track in tracklist:
            if calculate_similarity(parsed_track, discogs_track) >= 0.8:
                track_matches += 1
                break

    if parsed_data['track_list'] and tracklist:
        track_overlap = track_matches / max(len(parsed_data['track_list']), len(tracklist))
        scores['tracks'] = track_overlap
        if track_overlap >= 0.5:
            reasons.append(f"{track_matches}/{len(parsed_data['track_list'])} tracks match")
    else:
        scores['tracks'] = 0

    # Year scoring
    year_sim = 0.0
    if parsed_data.get('year') and discogs_year:
        if str(parsed_data['year']) == str(discogs_year):
            year_sim = 1.0
            reasons.append(f"Year match: {discogs_year}")
    scores['year'] = year_sim

    # Weighted total
    total_weight = sum([1.0, 1.0, 0.5, 0.5])
    weighted_score = (
        scores['artist'] * 1.0 +
        scores['title'] * 1.0 +
        scores['tracks'] * 0.5 +
        scores['year'] * 0.5
    ) / total_weight

    confidence = min(1.0, weighted_score)
    return confidence, reasons


def correlate_local(conn, parsed_data, html_path):
    """Main correlation function using local database."""
    results = search_discogs_local(conn, parsed_data)

    matches = []
    for release in results:
        try:
            tracklist = get_release_tracks(conn, release['id'])
            confidence, reasons = score_match(parsed_data, release, tracklist)
        except Exception as ex:
            confidence, reasons = 0, []

        if confidence >= 0.5:
            matches.append({
                'source_file': html_path,
                'parsed_data': parsed_data,
                'discogs_release': release,
                'confidence': confidence,
                'match_reasons': reasons
            })

    matches.sort(key=lambda x: x['confidence'], reverse=True)
    return matches


def process_path(conn, redis_client, path, verbose=False):
    """Process a single path, check cache, update Redis."""
    html_path = resolve_html_path(path)
    
    if not html_path or not os.path.exists(html_path):
        return None
    
    stub = '/'.join(os.path.abspath(html_path).split('/')[-3:-1])
    
    # Check Redis cache first
    cached_url = redis_client.hget('bc2dg', stub)
    if cached_url is not None:
        url = cached_url.decode('utf-8') if isinstance(cached_url, bytes) else str(cached_url)
        if verbose:
            print(f"[CACHE HIT] {stub} -> {url}", file=sys.stderr)
        return url
    
    # Check if previously failed
    if redis_client.sismember('bc2fail', stub):
        if verbose:
            print(f"[CACHED FAIL] {stub}", file=sys.stderr)
        return None
    
    # Parse page.html
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        if verbose:
            print(f"[ERROR] {stub}: {e}", file=sys.stderr)
        return None
    
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', content, re.IGNORECASE)
    if not match:
        if "Sorry, that something isn't here" not in content:
            if verbose:
                print(f"[NO META] {stub}", file=sys.stderr)
        return None
    
    parsed_data = parse_description(match.group(1))
    if not parsed_data:
        if verbose:
            print(f"[PARSE FAIL] {stub}", file=sys.stderr)
        return None
    
    # Search and score
    matches = correlate_local(conn, parsed_data, html_path)
    
    if not matches:
        # Cache the failure
        redis_client.sadd('bc2fail', stub)
        if verbose:
            artist = parsed_data.get('artist_name', 'unknown')
            release = parsed_data.get('release_name', 'unknown')
            print(f"[NO MATCH] {stub} ({artist} - {release})", file=sys.stderr)
        return None
    
    # Store success in Redis
    best = matches[0]
    release_id = best['discogs_release']['id']
    url = f"https://www.discogs.com/release/{release_id}"
    
    redis_client.srem('bc2fail', stub)
    redis_client.hset('bc2dg', stub, url)
    
    if verbose:
        print(f"[FOUND] {stub} -> {url} ({best['confidence']:.0%})", file=sys.stderr)
    
    return url


def main():
    parser = argparse.ArgumentParser(
        description='Match Bandcamp releases to Discogs using a local PostgreSQL database. '
                    'Results are cached in Redis (same as correlate.py).'
    )
    parser.add_argument(
        '--db', '-d',
        default=os.environ.get('DATABASE_URL'),
        help='PostgreSQL connection URL (default: $DATABASE_URL)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show progress and statistics on stderr'
    )
    
    args = parser.parse_args()
    
    if not args.db:
        print("Error: No database specified. Use --db or set DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(args.db)
    
    # Connect to Redis
    redis_client = get_redis_client()
    
    count = 0
    matched = 0
    failed = 0
    cached = 0
    
    try:
        for line in sys.stdin:
            path = line.strip()
            if not path:
                continue
            
            count += 1
            
            result = process_path(conn, redis_client, path, args.verbose)
            
            if result:
                matched += 1
                print(result)
            else:
                failed += 1
    
    except KeyboardInterrupt:
        print(f"\nInterrupted after {count} paths.", file=sys.stderr)
    
    finally:
        conn.close()
    
    if args.verbose:
        print(f"\nSummary:", file=sys.stderr)
        print(f"  Total: {count}", file=sys.stderr)
        print(f"  Matched: {matched}", file=sys.stderr)
        print(f"  Failed: {failed}", file=sys.stderr)
        if count > 0:
            print(f"  Rate: {matched/count:.1%}", file=sys.stderr)


if __name__ == '__main__':
    main()
