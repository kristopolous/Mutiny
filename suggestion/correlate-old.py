#!/usr/bin/env python3
"""Correlate Bandcamp releases to Discogs using fuzzy matching."""

import os
import re
import sys
from typing import Optional, Tuple, List
from difflib import SequenceMatcher

import discogs_client
from dotenv import load_dotenv

load_dotenv()


def bandcamp_url_to_data(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract artist and release name from a Bandcamp URL.
    
    Returns (artist_name, release_name) or (None, None) if parsing fails.
    """
    # Pattern: bandcamp.com/artist/release-name
    pattern = r'https?://[^/]+\.bandcamp\.com/album/([^/?]+)(?:\?.*)?$'
    match = re.search(pattern, url)
    if match:
        slug = match.group(1)
        # Replace hyphens/underscores with spaces for comparison
        return ('bandcamp-artist', slug.replace('-', ' ').replace('_', ' '))
    
    # Pattern: bandcamp.com/artist/release-name (with possible artist name in path)
    pattern = r'https?://([^/]+\.bandcamp\.com)/[^/]+/([^/?]+)(?:\?.*)?$'
    match = re.search(pattern, url)
    if match:
        artist_name = match.group(1).replace('.bandcamp.com', '')
        release_name = match.group(2).replace('-', ' ').replace('_', ' ')
        return (artist_name, release_name)
    
    return (None, None)


def normalize_text(text: str) -> str:
    """Normalize text for comparison by removing common discogs artifacts."""
    if not text:
        return ''
    # Remove discogs (#123) artist disambiguation
    text = re.sub(r'\s*#\d+\s*$', '', text)
    text = re.sub(r'\s*#\d+', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text


def similarity_score(s1: str, s2: str) -> float:
    """Calculate similarity score between two strings using SequenceMatcher."""
    s1_norm = normalize_text(s1.lower())
    s2_norm = normalize_text(s2.lower())
    return SequenceMatcher(None, s1_norm, s2_norm).ratio()


def combined_similarity(artist1: str, release1: str, artist2: str, release2: str) -> float:
    """Calculate a weighted combined similarity score for artist and release."""
    artist_sim = similarity_score(artist1, artist2)
    release_sim = similarity_score(release1, release2)
    # Weight both equally
    return (artist_sim + release_sim) / 2


def find_discogs_release(artist: str, release: str, client: discogs_client.Client) -> Optional[dict]:
    """Search Discogs for a matching release using fuzzy matching.
    
    Returns the best matching release info or None if no good match found.
    """
    # Search by release name
    release_query = f"{artist} {release}"
    search = client.search(release_query, type='release')
    
    best_match = None
    best_score = 0.0
    
    # Need to expand results to get Release objects
    release_id = search.page
    target_release = client.release(release_id)
    
    # Check this single result
    score = combined_similarity(artist, release, target_release.artists[0].name, target_release.title)
    if score > best_score:
        best_score = score
        best_match = {
            'release': target_release,
            'artist': target_release.artists[0].name if target_release.artists else None,
            'title': target_release.title,
            'discogs_id': target_release.id,
            'url': target_release.link,
            'score': score
        }
    
    return best_match


def correlate_bandcamp_to_discogs(url: str) -> Optional[dict]:
    """Main function to correlate Bandcamp URL to Discogs release.
    
    Args:
        url: Bandcamp URL to correlate
        
    Returns:
        Dictionary with Discogs release info and similarity score, or None
    """
    # Extract artist and release from Bandcamp URL
    artist, release = bandcamp_url_to_data(url)
    if not artist or not release:
        print(f"Error: Could not parse artist and release from URL: {url}")
        return None
    
    print(f"Found: Artist={artist}, Release={release}")
    
    # Initialize Discogs client
    user_token = os.getenv('DISCOGS_USER_TOKEN')
    app_token = os.getenv('DISCOGS_APP_TOKEN')
    
    if user_token:
        client = discogs_client.Client('BandcampCorrelator/1.0', 
                                       user_token=user_token,
                                       user_agent='BandcampCorrelator/1.0 +https://github.com/music-explorer')
    else:
        client = discogs_client.Client('BandcampCorrelator/1.0', 
                                       user_agent='BandcampCorrelator/1.0 +https://github.com/music-explorer')
    
    # Search for matches
    result = find_discogs_release(artist, release, client)
    
    if result and result['score'] > 0.7:
        return result
    
    return None


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python correlate.py <bandcamp-url>")
        print("Example: python correlate.py 'https://artist.bandcamp.com/album/release-title'")
        sys.exit(1)
    
    url = sys.argv[1]
    result = correlate_bandcamp_to_discogs(url)
    
    if result:
        print(f"\nBest match found:")
        print(f"  Artist: {result['artist']}")
        print(f"  Title: {result['title']}")
        print(f"  Discogs URL: {result['url']}")
        print(f"  Discogs ID: {result['discogs_id']}")
        print(f"  Similarity Score: {result['score']:.2%}")
    else:
        print("No matching release found on Discogs.")
        sys.exit(1)


if __name__ == '__main__':
    main()
