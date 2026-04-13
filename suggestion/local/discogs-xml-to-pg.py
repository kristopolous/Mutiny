#!/usr/bin/env python3
"""
Parse Discogs XML release database and import into PostgreSQL.

Optimized for building collaboration networks:
- Artists and their IDs (primary + extraartists on tracks)
- Track listings
- Release metadata for matching

Usage:
    python discogs-xml-to-pg.py /path/to/discogs.xml --db postgresql://user:pass@localhost/discogs
"""

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from psycopg2.extras import execute_batch, Json
import psycopg2


def create_schema(conn):
    """Create schema optimized for network queries."""
    cursor = conn.cursor()
    
    # Check if already imported (re-entrancy check)
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'releases'
        )
    """)
    if cursor.fetchone()[0]:
        cursor.execute("SELECT COUNT(*) FROM releases")
        count = cursor.fetchone()[0]
        if count > 1000000:
            print(f"Database already populated ({count} releases). Skipping import.")
            return False
        else:
            print(f"Found existing partial import ({count} releases). Recreating schema...")
    
    cursor.execute('DROP TABLE IF EXISTS releases')
    cursor.execute('DROP TABLE IF EXISTS artists')
    cursor.execute('DROP TABLE IF EXISTS track_artists')
    cursor.execute('DROP TABLE IF EXISTS tracks')
    cursor.execute('DROP TABLE IF EXISTS release_artists')
    cursor.execute('DROP TABLE IF EXISTS release_extraartists')
    
    # Artists - Discogs ID is primary key
    cursor.execute('''
        CREATE UNLOGGED TABLE artists (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    # Releases - core metadata for matching
    cursor.execute('''
        CREATE UNLOGGED TABLE releases (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            master_id INTEGER,
            country TEXT,
            released TEXT,
            year INTEGER,
            data JSONB
        )
    ''')
    
    # Release <-> Artist (primary artists)
    cursor.execute('''
        CREATE UNLOGGED TABLE release_artists (
            release_id INTEGER,
            artist_id INTEGER,
            join_text TEXT,
            anv TEXT,
            PRIMARY KEY (release_id, artist_id)
        )
    ''')
    
    # Release <-> Extra Artist (producers, engineers, etc.)
    cursor.execute('''
        CREATE UNLOGGED TABLE release_extraartists (
            release_id INTEGER,
            artist_id INTEGER,
            role TEXT,
            anv TEXT,
            PRIMARY KEY (release_id, artist_id)
        )
    ''')
    
    # Tracks
    cursor.execute('''
        CREATE UNLOGGED TABLE tracks (
            id BIGSERIAL PRIMARY KEY,
            release_id INTEGER,
            position TEXT,
            title TEXT
        )
    ''')
    
    # Track <-> Artist (who worked on each track)
    cursor.execute('''
        CREATE UNLOGGED TABLE track_artists (
            track_id BIGINT,
            artist_id INTEGER,
            role TEXT,
            anv TEXT,
            PRIMARY KEY (track_id, artist_id)
        )
    ''')
    
    conn.commit()
    print("Schema created. Starting import...")


def create_indexes(conn):
    """Create indexes after data loading."""
    # Must close any existing transaction first
    conn.commit()
    conn.set_session(autocommit=True)
    
    cursor = conn.cursor()
    print("Creating indexes...")
    
    # Enable trigram extension for fast substring searches
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    
    indexes = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_releases_title ON releases(title)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_releases_year ON releases(year)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_releases_released ON releases(released)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_artists_name ON artists(name)",
        # Trigram index for fast artist name substring search
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_artists_name_trgm ON artists USING GIN (name gin_trgm_ops)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artists_artist ON release_artists(artist_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artists_release ON release_artists(release_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_extraartists_artist ON release_extraartists(artist_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_extraartists_release ON release_extraartists(release_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tracks_release ON tracks(release_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_track_artists_artist ON track_artists(artist_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_track_artists_track ON track_artists(track_id)",
    ]
    
    for idx_sql in indexes:
        cursor.execute(idx_sql)
    
    conn.set_session(autocommit=False)
    print("Indexes created.")


def parse_release(elem):
    """Parse a <release> element into structured data."""
    release_id = int(elem.get('id', 0))
    
    # Basic fields
    title = elem.findtext('title', '')
    country = elem.findtext('country', '')
    released = elem.findtext('released', '')
    notes = elem.findtext('notes', '')
    data_quality = elem.findtext('data_quality', '')
    
    # Year from released date
    year = None
    if released and len(released) >= 4:
        try:
            year = int(released[:4])
        except ValueError:
            pass
    
    # Master ID
    master_elem = elem.find('master_id')
    master_id = int(master_elem.get('id', 0)) if master_elem is not None else None
    
    # Primary artists
    release_artists = []
    artists_elem = elem.find('artists')
    if artists_elem is not None:
        for artist_elem in artists_elem.findall('artist'):
            aid_elem = artist_elem.find('id')
            name_elem = artist_elem.find('name')
            join_elem = artist_elem.find('join')
            anv_elem = artist_elem.find('anv')
            
            if aid_elem is not None and name_elem is not None:
                release_artists.append({
                    'id': int(aid_elem.text),
                    'name': name_elem.text or '',
                    'join': join_elem.text if join_elem is not None else '',
                    'anv': anv_elem.text if anv_elem is not None else ''
                })
    
    # Extra artists (release level)
    release_extraartists = []
    ea_elem = elem.find('extraartists')
    if ea_elem is not None:
        for ea in ea_elem.findall('artist'):
            ea_id = ea.findtext('id')
            ea_name = ea.findtext('name')
            ea_role = ea.findtext('role')
            if ea_id and ea_name:
                release_extraartists.append({
                    'id': int(ea_id),
                    'name': ea_name,
                    'role': ea_role or '',
                    'anv': ea.findtext('anv', '')
                })
    
    # Tracks with their artists
    tracks = []
    tracklist_elem = elem.find('tracklist')
    if tracklist_elem is not None:
        for track_elem in tracklist_elem.findall('track'):
            track = {
                'position': track_elem.findtext('position', ''),
                'title': track_elem.findtext('title', '')
            }
            
            # Track-level artists
            track_artists = []
            tea_elem = track_elem.find('extraartists')
            if tea_elem is not None:
                for ta in tea_elem.findall('artist'):
                    ta_id = ta.findtext('id')
                    ta_name = ta.findtext('name')
                    ta_role = ta.findtext('role')
                    if ta_id and ta_name:
                        track_artists.append({
                            'id': int(ta_id),
                            'name': ta_name,
                            'role': ta_role or '',
                            'anv': ta.findtext('anv', '')
                        })
            track['artists'] = track_artists
            tracks.append(track)
    
    # Build JSONB data blob for matching
    data = {
        'country': country,
        'released': released,
        'notes': notes,
        'data_quality': data_quality
    }
    
    return {
        'id': release_id,
        'title': title,
        'master_id': master_id,
        'country': country,
        'released': released,
        'year': year,
        'release_artists': release_artists,
        'release_extraartists': release_extraartists,
        'tracks': tracks,
        'data': data
    }


def parse_xml_file(xml_path, db_url, batch_size=50000):
    """Parse the XML file and import into PostgreSQL."""
    print(f"Parsing {xml_path}...")
    
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    if not create_schema(conn):
        # Already populated, just ensure indexes exist
        create_indexes(conn)
        cursor.execute("SELECT COUNT(*) FROM releases")
        count = cursor.fetchone()[0]
        print(f"\nDatabase ready!")
        print(f"  Total releases: {count}")
        conn.close()
        return
    
    # Buffers
    artists_buf = []
    releases_buf = []
    release_artists_buf = []
    release_extraartists_buf = []
    tracks_buf = []
    track_artists_buf = []
    
    artist_ids_seen = set()
    
    releases_count = 0
    
    context = ET.iterparse(xml_path, events=('end',))
    
    for event, elem in context:
        if elem.tag == 'release':
            release = parse_release(elem)
            
            # Queue artists
            for artist in release['release_artists']:
                if artist['id'] not in artist_ids_seen:
                    artists_buf.append((artist['id'], artist['name']))
                    artist_ids_seen.add(artist['id'])
            
            for artist in release['release_extraartists']:
                if artist['id'] not in artist_ids_seen:
                    artists_buf.append((artist['id'], artist['name']))
                    artist_ids_seen.add(artist['id'])
            
            for track in release['tracks']:
                for artist in track.get('artists', []):
                    if artist['id'] not in artist_ids_seen:
                        artists_buf.append((artist['id'], artist['name']))
                        artist_ids_seen.add(artist['id'])
            
            # Queue release
            releases_buf.append((
                release['id'],
                release['title'],
                release['master_id'],
                release['country'],
                release['released'],
                release['year'],
                Json(release['data'])
            ))
            
            # Queue release artists
            for artist in release['release_artists']:
                release_artists_buf.append((
                    release['id'],
                    artist['id'],
                    artist['join'],
                    artist['anv']
                ))
            
            # Queue release extra artists
            for artist in release['release_extraartists']:
                release_extraartists_buf.append((
                    release['id'],
                    artist['id'],
                    artist['role'],
                    artist['anv']
                ))
            
            # Queue tracks and track artists
            for track in release['tracks']:
                tracks_buf.append((
                    release['id'],
                    track['position'],
                    track['title']
                ))
                track_id = len(tracks_buf)  # Temp ID for this batch
                
                for artist in track.get('artists', []):
                    track_artists_buf.append((
                        track_id,
                        artist['id'],
                        artist['role'],
                        artist['anv']
                    ))
            
            releases_count += 1
            elem.clear()
            
            # Batch insert
            if releases_count % batch_size == 0:
                if artists_buf:
                    execute_batch(cursor,
                        "INSERT INTO artists (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                        artists_buf)
                    artists_buf = []
                
                if releases_buf:
                    execute_batch(cursor,
                        """INSERT INTO releases 
                           (id, title, master_id, country, released, year, data)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        releases_buf)
                    releases_buf = []
                
                if release_artists_buf:
                    execute_batch(cursor,
                        "INSERT INTO release_artists (release_id, artist_id, join_text, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (release_id, artist_id) DO NOTHING",
                        release_artists_buf)
                    release_artists_buf = []
                
                if release_extraartists_buf:
                    execute_batch(cursor,
                        "INSERT INTO release_extraartists (release_id, artist_id, role, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (release_id, artist_id) DO NOTHING",
                        release_extraartists_buf)
                    release_extraartists_buf = []
                
                if tracks_buf:
                    execute_batch(cursor,
                        "INSERT INTO tracks (release_id, position, title) VALUES (%s, %s, %s)",
                        tracks_buf)
                    tracks_buf = []
                
                if track_artists_buf:
                    execute_batch(cursor,
                        "INSERT INTO track_artists (track_id, artist_id, role, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (track_id, artist_id) DO NOTHING",
                        track_artists_buf)
                    track_artists_buf = []
                
                conn.commit()
                print(f"  Processed {releases_count} releases...")
    
    # Final flush
    if artists_buf:
        execute_batch(cursor, "INSERT INTO artists (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", artists_buf)
    if releases_buf:
        execute_batch(cursor,
            """INSERT INTO releases 
               (id, title, master_id, country, released, year, data)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            releases_buf)
    if release_artists_buf:
        execute_batch(cursor,
            "INSERT INTO release_artists (release_id, artist_id, join_text, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (release_id, artist_id) DO NOTHING",
            release_artists_buf)
    if release_extraartists_buf:
        execute_batch(cursor,
            "INSERT INTO release_extraartists (release_id, artist_id, role, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (release_id, artist_id) DO NOTHING",
            release_extraartists_buf)
    if tracks_buf:
        execute_batch(cursor,
            "INSERT INTO tracks (release_id, position, title) VALUES (%s, %s, %s)",
            tracks_buf)
    if track_artists_buf:
        execute_batch(cursor,
            "INSERT INTO track_artists (track_id, artist_id, role, anv) VALUES (%s, %s, %s, %s) ON CONFLICT (track_id, artist_id) DO NOTHING",
            track_artists_buf)
    
    conn.commit()
    
    # Convert to logged
    print("Converting tables to logged...")
    for table in ['artists', 'releases', 'release_artists', 'release_extraartists', 'tracks', 'track_artists']:
        cursor.execute(f"ALTER TABLE {table} SET LOGGED")
    conn.commit()
    
    create_indexes(conn)
    
    cursor.execute("SELECT COUNT(*) FROM releases")
    count = cursor.fetchone()[0]
    
    print(f"\nImport complete!")
    print(f"  Total releases: {count}")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Parse Discogs XML release database and import into PostgreSQL.'
    )
    parser.add_argument(
        'xml_file',
        help='Path to Discogs XML file'
    )
    parser.add_argument(
        '--db', '-d',
        default=os.environ.get('DATABASE_URL'),
        help='PostgreSQL connection URL (default: $DATABASE_URL)'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=50000,
        help='Batch size for commits (default: 50000)'
    )
    
    args = parser.parse_args()
    
    xml_path = Path(args.xml_file)
    if not xml_path.exists():
        print(f"Error: XML file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)
    
    if not args.db:
        print("Error: No database specified. Use --db or set DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    
    parse_xml_file(xml_path, args.db, args.batch_size)


if __name__ == '__main__':
    main()
