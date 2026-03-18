#!/usr/bin/env python3
"""
Match Bandcamp releases to Discogs using a local PostgreSQL database.

Reads paths from stdin, outputs Discogs URLs to stdout.

Usage:
    find <bandcamp-dir> -name page.html | python correlate-local-pg.py --db postgresql://... [-j]
"""

import argparse
import json
import os
import re
import sys
import psycopg2
from html import unescape
from difflib import SequenceMatcher


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


def process_path(conn, path, json_output=False):
    """Process a single path and output results."""
    html_path = resolve_html_path(path)
    
    if not html_path or not os.path.exists(html_path):
        return None
    
    stub = '/'.join(os.path.abspath(html_path).split('/')[-3:-1])
    
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {html_path}: {e}", file=sys.stderr)
        return None
    
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', content, re.IGNORECASE)
    if not match:
        return None
    
    parsed_data = parse_description(match.group(1))
    if not parsed_data:
        return None
    
    matches = correlate_local(conn, parsed_data, html_path)
    
    if not matches:
        return None
    
    best = matches[0]
    release_id = best['discogs_release']['id']
    url = f"https://www.discogs.com/release/{release_id}"
    
    if json_output:
        return {
            'stub': stub,
            'url': url,
            'match': best
        }
    else:
        return url


def main():
    parser = argparse.ArgumentParser(
        description='Match Bandcamp releases to Discogs using a local PostgreSQL database.'
    )
    parser.add_argument(
        '--db', '-d',
        default=os.environ.get('DATABASE_URL'),
        help='PostgreSQL connection URL (default: $DATABASE_URL)'
    )
    parser.add_argument(
        '-j', '--json',
        action='store_true',
        help='Output matches in JSON format'
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
    
    conn = psycopg2.connect(args.db)
    
    count = 0
    matched = 0
    failed = 0
    
    try:
        for line in sys.stdin:
            path = line.strip()
            if not path:
                continue
            
            count += 1
            if args.verbose:
                print(f"[{count}] Processing: {path}", file=sys.stderr)
            
            result = process_path(conn, path, args.json)
            
            if result:
                matched += 1
                if not args.json:
                    print(result)
                else:
                    print(json.dumps(result))
            else:
                failed += 1
                if args.verbose:
                    print(f"  -> No match", file=sys.stderr)
    
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
