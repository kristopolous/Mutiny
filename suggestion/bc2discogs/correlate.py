#!/usr/bin/env python3
"""Correlate Bandcamp releases with Discogs."""

import argparse
import json
import os
import re
import sys
from difflib import SequenceMatcher
from html import unescape

import discogs_client
from dotenv import load_dotenv

# Add parent directory to path to import cache module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cache import get_redis_client

load_dotenv()


def parse_description(content):
    """Parse the meta description tag content to extract release metadata."""
    content = unescape(content.strip())
    lines = content.split('\n')
    
    if not lines:
        return None
    
    first_line = lines[0].strip()
    
    match = re.match(r'(.+?)\s+by\s+(.+?),\s+released\s+(.+)', first_line)
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


def parse_html(html_path):
    """Parse a page.html file and extract metadata from meta description."""
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', content, re.IGNORECASE)
    if match:
        return parse_description(match.group(1))
    
    return None


def search_discogs(client, parsed_data):
    """Search Discogs API for matching release."""
    query_parts = {
        'artist': parsed_data['artist_name'],
        'type': 'release',
        'release_title': parsed_data['release_name']
    }

    if parsed_data.get('year'):
        query_parts['year'] = parsed_data.get('year')
    
    try:
        results = client.search(**query_parts)
        return results
    except Exception as e:
        print(f"Error searching Discogs: {e}", file=sys.stderr)
        return []


def calculate_similarity(a, b):
    """Calculate similarity ratio between two strings."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def score_match(parsed_data, discogs_release):
    """Score a match between parsed data and Discogs release."""
    reasons = []
    scores = {}
    
    discogs_title = getattr(discogs_release, 'title', '')
    discogs_artists = [getattr(a, 'name', '') for a in getattr(discogs_release, 'artists', [])]
    discogs_tracks = [getattr(t, 'title', '') for t in getattr(discogs_release, 'tracklist', [])]
    discogs_year = getattr(discogs_release, 'year', None)
    
    artist_scores = []
    for discogs_artist in discogs_artists:
        artist_sim = calculate_similarity(parsed_data['artist_name'], discogs_artist)
        artist_scores.append(artist_sim)
        if artist_sim >= 0.8:
            reasons.append(f"Artist match: {discogs_artist}")
    scores['artist'] = max(artist_scores) if artist_scores else 0
    
    title_sim = calculate_similarity(parsed_data['release_name'], discogs_title)
    scores['title'] = title_sim
    if title_sim >= 0.8:
        reasons.append(f"Title match: {discogs_title}")
    
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
    
    year_sim = 0.0
    if parsed_data.get('year') and discogs_year:
        if str(parsed_data['year']) == str(discogs_year):
            year_sim = 1.0
            reasons.append(f"Year match: {discogs_year}")
    scores['year'] = year_sim
    
    total_weight = sum([1.0, 1.0, 0.5, 0.5])
    weighted_score = (
        scores['artist'] * 1.0 +
        scores['title'] * 1.0 +
        scores['tracks'] * 0.5 +
        scores['year'] * 0.5
    ) / total_weight
    
    confidence = min(1.0, weighted_score)
    
    return confidence, reasons


def resolve_html_path(path):
    """
    Resolve input path to an actual page.html file.
    
    If path is a directory, looks for page.html inside it.
    If path is a file, returns it directly.
    
    Args:
        path: User-provided path (directory or file)
        
    Returns:
        str: Absolute path to page.html file
        
    Raises:
        ValueError: If page.html not found in directory or path invalid
    """
    if os.path.isfile(path):
        return path
    
    if os.path.isdir(path):
        page_html = os.path.join(path, 'page.html')
        if os.path.isfile(page_html):
            return page_html
        else:
            raise ValueError(f"No page.html found in directory: {path}")
    
    raise ValueError(f"Path does not exist: {path}")


def get_cache_key(html_path):
    """Generate a cache key based on the resolved file path."""
    # Use absolute normalized path as cache key - it's unique and debuggable
    abs_path = '/'.join(os.path.abspath(html_path).split('/')[-3:-1])
    return f"bc2:{abs_path}"


def get_discogs_data(release):
    """Extract relevant data from a Discogs release object."""
    data = {
        'id': getattr(release, 'id', None),
        'uri': getattr(release, 'uri', ''),
        'resource_url': getattr(release, 'resource_url', ''),
        'title': getattr(release, 'title', ''),
        'year': getattr(release, 'year', None),
        'artists': [getattr(a, 'name', '') for a in getattr(release, 'artists', [])],
        'tracklist': [getattr(t, 'title', '') for t in getattr(release, 'tracklist', [])],
        'labels': [getattr(l, 'name', '') for l in getattr(release, 'labels', [])],
        'formats': [getattr(f, 'name', '') for f in getattr(release, 'formats', [])],
    }
    # Ensure we have a usable Discogs web URL; construct from ID if needed
    if not data['uri'] and data['id']:
        data['uri'] = f"https://www.discogs.com/release/{data['id']}"
    return data


def correlate(html_path, client):
    """Main correlation function."""
    parsed_data = parse_html(html_path)
    if not parsed_data:
        return None
    
    results = search_discogs(client, parsed_data)
    
    matches = []
    for release in results:
        if not hasattr(release, 'id'):
            continue
        
        confidence, reasons = score_match(parsed_data, release)
        
        if confidence >= 0.5:
            matches.append({
                'source_file': html_path,
                'parsed_data': parsed_data,
                'discogs_release': get_discogs_data(release),
                'confidence': confidence,
                'match_reasons': reasons
            })
    
    matches.sort(key=lambda x: x['confidence'], reverse=True)
    return matches


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Match Bandcamp releases to Discogs using locally saved page.html files. '
                    'Results are cached in Redis to minimize API calls.'
    )
    parser.add_argument(
        'input',
        help='Path to a Bandcamp release directory (containing page.html) or directly to a page.html file'
    )
    parser.add_argument(
        '-o', '--output',
        type=argparse.FileType('w'),
        default=sys.stdout,
        help='Write results to FILE; defaults to stdout'
    )
    parser.add_argument(
        '-j', '--json',
        action='store_true',
        help='Output matches in JSON format for programmatic consumption'
    )
    parser.add_argument(
        '--token',
        help='Discogs user token (overrides DISCOGS_USER_TOKEN environment variable)'
    )
    
    args = parser.parse_args()
    
    try:
        # Reject URL inputs
        if args.input.startswith('http'):
            print("URL input not supported. Provide a path to a page.html file or its containing directory.", file=sys.stderr)
            sys.exit(1)
        
        # Resolve input to actual page.html path
        try:
            html_path = resolve_html_path(args.input)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        
        cache_key = get_cache_key(html_path)
        r = get_redis_client()
        cached_url = r.get(cache_key)
        if cached_url is not None:
            # Cache hit: decode bytes and output URL only
            url = cached_url.decode('utf-8') if isinstance(cached_url, bytes) else str(cached_url)
            if url:  # Non-empty means we have a match
                if args.json:
                    json.dump({"url": url}, args.output)
                else:
                    args.output.write(url + '\n')
                sys.exit(0)
            else:  # Empty cached value means previous attempt found no match
                print(f"No matches found (cached) for {cache_key}", file=sys.stderr)
                sys.exit(1)
        
        # Cache miss: need to call Discogs API
        token = args.token or os.getenv('DISCOGS_USER_TOKEN')
        if not token:
            print("Error: DISCOGS_USER_TOKEN not set", file=sys.stderr)
            sys.exit(1)
        client = discogs_client.Client('Correlate/1.0', user_token=token)
        matches = correlate(html_path, client)
        if matches is None:
            print("Failed to parse HTML file. Check that it contains a valid meta description.", file=sys.stderr)
            sys.exit(1)
        
        # Extract URL from best match (if any)
        if matches:
            best = matches[0]
            url = best['discogs_release'].get('uri', '')
            if not url and best['discogs_release'].get('id'):
                url = f"https://www.discogs.com/release/{best['discogs_release']['id']}"
        else:
            url = ''
        
        # Cache the URL (avoid repeated lookups)
        try:
            r.setex(cache_key, 3600, url)
        except Exception:
            pass
        
        if not matches:
            # No matches: show helpful error with artist/release info
            parsed = parse_html(html_path)
            if parsed:
                artist = parsed.get('artist_name', 'unknown')
                release = parsed.get('release_name', 'unknown')
                print(f"No matches found for {artist} - {release} ({cache_key})", file=sys.stderr)
            else:
                print(f"No matches found for {cache_key}", file=sys.stderr)
            sys.exit(1)
        
        # Output full match information
        if args.json:
            json.dump(matches, args.output, indent=2)
        else:
            for match in matches:
                args.output.write(f"\n{'='*60}\n")
                args.output.write(f"Source: {match['source_file']}\n")
                args.output.write(f"Artist: {match['parsed_data']['artist_name']}\n")
                args.output.write(f"Release: {match['parsed_data']['release_name']}\n")
                args.output.write(f"Discogs: {match['discogs_release']['title']} (ID: {match['discogs_release']['id']})\n")
                # Show this match's URL
                match_url = match['discogs_release'].get('uri')
                if not match_url and match['discogs_release'].get('id'):
                    match_url = f"https://www.discogs.com/release/{match['discogs_release']['id']}"
                if match_url:
                    args.output.write(f"URL: {match_url}\n")
                args.output.write(f"Confidence: {match['confidence']:.2%}\n")
                if match['match_reasons']:
                    args.output.write("Reasons:\n")
                    for reason in match['match_reasons']:
                        args.output.write(f"  - {reason}\n")
    
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)  # 128 + SIGINT = 130


if __name__ == '__main__':
    main()
