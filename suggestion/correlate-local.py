#!/usr/bin/env python3
"""
Match Bandcamp releases to Discogs using a local SQLite database.

Reads paths from stdin (one per line) and outputs matches to stdout.
Each input path should be a directory containing page.html or a page.html file directly.

Usage:
    find <bandcamp-dir> -name page.html | python correlate-local.py [--db discogs.db] [-j]

Or:
    find <bandcamp-dir> -type d | python correlate-local.py [--db discogs.db] [-j]
"""

import argparse
import json
import os
import re
import sys
import sqlite3
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
    """
    Resolve input path to an actual page.html file.

    If path is a directory, looks for page.html inside it.
    If path is a file, returns it directly.
    """
    if os.path.isfile(path):
        return path

    if os.path.isdir(path):
        page_html = os.path.join(path, 'page.html')
        if os.path.isfile(page_html):
            return page_html

    return None


def search_discogs_local(conn, parsed_data, limit=50):
    """
    Search the local Discogs database for matching releases.
    
    Uses a combination of artist name and title similarity to find candidates.
    """
    cursor = conn.cursor()
    
    artist_name = parsed_data['artist_name']
    release_name = parsed_data['release_name']
    
    # Extract key words from title (skip common words)
    skip_words = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'for', 'to', 'at', 'by', 'with'}
    title_words = [w for w in release_name.lower().split() if w not in skip_words and len(w) > 2]
    
    # Build search patterns
    # Search by artist name containing the parsed artist
    artist_pattern = f'%{artist_name}%'
    
    # Search by title containing key words
    title_patterns = [f'%{w}%' for w in title_words[:3]]  # Use first 3 meaningful words
    
    # Query: find releases where artist name matches and title has some overlap
    # Using a subquery to get artist names for each release
    query = '''
        SELECT DISTINCT r.id, r.title, r.released,
               (SELECT GROUP_CONCAT(a.name, ' | ') 
                FROM artists a 
                JOIN release_artists ra ON a.id = ra.artist_id 
                WHERE ra.release_id = r.id) as artists
        FROM releases r
        JOIN release_artists ra ON r.id = ra.release_id
        JOIN artists a ON ra.artist_id = a.id
        WHERE a.name LIKE ?
    '''
    params = [artist_pattern]
    
    # Add title conditions (OR of first 2 words for broader match)
    if title_patterns:
        title_conditions = ' OR '.join([f'r.title LIKE ?' for _ in title_patterns[:2]])
        query += f' AND ({title_conditions})'
        params.extend(title_patterns[:2])
    
    query += ' LIMIT ?'
    params.append(limit)
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    releases = []
    for row in results:
        releases.append({
            'id': row[0],
            'title': row[1],
            'released': row[2],
            'artists': row[3].split(' | ') if row[3] else []
        })
    
    return releases


def get_release_tracks(conn, release_id):
    """Get tracklist for a release."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT position, title FROM tracks WHERE release_id = ? ORDER BY id
    ''', (release_id,))
    
    return [(row[0], row[1]) for row in cursor.fetchall()]


def score_match(parsed_data, discogs_release, tracklist):
    """Score a match between parsed data and Discogs release."""
    reasons = []
    scores = {}

    discogs_title = discogs_release.get('title', '')
    discogs_artists = discogs_release.get('artists', [])
    discogs_tracks = [t[1] for t in tracklist]
    discogs_year = discogs_release.get('released', '')[:4] if discogs_release.get('released') else None

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
        for discogs_track in discogs_tracks:
            if calculate_similarity(parsed_track, discogs_track) >= 0.8:
                track_matches += 1
                break

    if parsed_data['track_list'] and discogs_tracks:
        track_overlap = track_matches / max(len(parsed_data['track_list']), len(discogs_tracks))
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


def process_path(conn, path, json_output=False, output_file=None):
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
    
    # Parse meta description
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', content, re.IGNORECASE)
    if not match:
        if "Sorry, that something isn't here" not in content and "Sorry, that something isn't here" not in content:
            pass  # Silently skip invalid pages
        return None
    
    parsed_data = parse_description(match.group(1))
    if not parsed_data:
        return None
    
    # Search and score
    matches = correlate_local(conn, parsed_data, html_path)
    
    if not matches:
        return None
    
    # Output
    best = matches[0]
    release_id = best['discogs_release']['id']
    url = f"https://www.discogs.com/release/{release_id}"
    
    if json_output:
        result = {
            'stub': stub,
            'url': url,
            'match': best
        }
    else:
        result = url
    
    if output_file:
        if json_output:
            output_file.write(json.dumps(result) + '\n')
        else:
            output_file.write(url + '\n')
    else:
        if json_output:
            print(json.dumps(result))
        else:
            print(url)
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description='Match Bandcamp releases to Discogs using a local SQLite database. '
                    'Reads paths from stdin, outputs Discogs URLs to stdout.'
    )
    parser.add_argument(
        '--db', '-d',
        default='discogs.db',
        help='SQLite database path (default: discogs.db)'
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
    
    # Check database exists
    if not os.path.exists(args.db):
        print(f"Error: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)
    
    # Connect to database
    conn = sqlite3.connect(args.db)
    
    # Process stdin line by line
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
