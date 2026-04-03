#!/usr/bin/env python3
"""
Parse Discogs XML release database and import into SQLite.

Usage:
    python discogs-xml-to-db.py /path/to/discogs.xml [--db discogs.db]

The XML file should contain <release> elements like:
    <release id="54">
        <artists>...</artists>
        <title>...</title>
        ...
    </release>
"""

import argparse
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def create_schema(conn):
    """Create the database schema."""
    cursor = conn.cursor()
    
    # Main releases table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY,
            title TEXT,
            country TEXT,
            released TEXT,
            notes TEXT,
            data_quality TEXT,
            master_id INTEGER,
            master_is_main INTEGER
        )
    ''')
    
    # Artists table (many-to-many with releases)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')
    
    # Release artists junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_artists (
            release_id INTEGER,
            artist_id INTEGER,
            join_text TEXT,
            PRIMARY KEY (release_id, artist_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (artist_id) REFERENCES artists(id)
        )
    ''')
    
    # Labels table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS labels (
            id INTEGER PRIMARY KEY,
            name TEXT,
            catno TEXT
        )
    ''')
    
    # Release labels junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_labels (
            release_id INTEGER,
            label_id INTEGER,
            catno TEXT,
            PRIMARY KEY (release_id, label_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (label_id) REFERENCES labels(id)
        )
    ''')
    
    # Formats table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS formats (
            id INTEGER PRIMARY KEY,
            name TEXT,
            qty TEXT,
            text TEXT
        )
    ''')
    
    # Release formats junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_formats (
            release_id INTEGER,
            format_id INTEGER,
            PRIMARY KEY (release_id, format_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (format_id) REFERENCES formats(id)
        )
    ''')
    
    # Format descriptions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS format_descriptions (
            id INTEGER PRIMARY KEY,
            format_id INTEGER,
            description TEXT,
            FOREIGN KEY (format_id) REFERENCES formats(id)
        )
    ''')
    
    # Genres table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')
    
    # Release genres junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_genres (
            release_id INTEGER,
            genre_id INTEGER,
            PRIMARY KEY (release_id, genre_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id)
        )
    ''')
    
    # Styles table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS styles (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')
    
    # Release styles junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_styles (
            release_id INTEGER,
            style_id INTEGER,
            PRIMARY KEY (release_id, style_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (style_id) REFERENCES styles(id)
        )
    ''')
    
    # Extra artists table (producers, mixers, etc.)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extraartists (
            id INTEGER PRIMARY KEY,
            name TEXT,
            anv TEXT,
            role TEXT
        )
    ''')
    
    # Release extra artists junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_extraartists (
            release_id INTEGER,
            extraartist_id INTEGER,
            PRIMARY KEY (release_id, extraartist_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (extraartist_id) REFERENCES extraartists(id)
        )
    ''')
    
    # Tracks table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id INTEGER,
            position TEXT,
            title TEXT,
            FOREIGN KEY (release_id) REFERENCES releases(id)
        )
    ''')
    
    # Track extra artists junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS track_extraartists (
            track_id INTEGER,
            extraartist_id INTEGER,
            PRIMARY KEY (track_id, extraartist_id),
            FOREIGN KEY (track_id) REFERENCES tracks(id),
            FOREIGN KEY (extraartist_id) REFERENCES extraartists(id)
        )
    ''')
    
    # Videos table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY,
            release_id INTEGER,
            src TEXT,
            duration INTEGER,
            embed TEXT,
            title TEXT,
            description TEXT,
            FOREIGN KEY (release_id) REFERENCES releases(id)
        )
    ''')
    
    # Companies table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            name TEXT,
            entity_type TEXT,
            entity_type_name TEXT
        )
    ''')
    
    # Release companies junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_companies (
            release_id INTEGER,
            company_id INTEGER,
            PRIMARY KEY (release_id, company_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        )
    ''')
    
    # Series table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY,
            name TEXT,
            catno TEXT
        )
    ''')
    
    # Release series junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS release_series (
            release_id INTEGER,
            series_id INTEGER,
            catno TEXT,
            PRIMARY KEY (release_id, series_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (series_id) REFERENCES series(id)
        )
    ''')
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_releases_title ON releases(title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_release_artists_artist ON release_artists(artist_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracks_title ON tracks(title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracks_release ON tracks(release_id)')
    
    conn.commit()


def get_or_insert(cursor, table, columns, values, where_clause=None):
    """Get existing record ID or insert new one and return ID."""
    if where_clause is None:
        where_clause = ' AND '.join(f"{col} = ?" for col in columns.keys())
    
    cursor.execute(f"SELECT id FROM {table} WHERE {where_clause}", list(values.values()))
    result = cursor.fetchone()
    if result:
        return result[0]
    
    cols = ', '.join(columns.keys())
    placeholders = ', '.join('?' for _ in values)
    cursor.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", list(values.values()))
    return cursor.lastrowid


def parse_release(element, conn):
    """Parse a <release> element and insert into database."""
    cursor = conn.cursor()
    
    release_id = int(element.get('id', 0))
    
    # Extract basic release info
    title = element.findtext('title', '')
    country = element.findtext('country', '')
    released = element.findtext('released', '')
    notes = element.findtext('notes', '')
    data_quality = element.findtext('data_quality', '')
    
    master_elem = element.find('master_id')
    master_id = int(master_elem.get('id', 0)) if master_elem is not None else 0
    master_is_main = 1 if master_elem is not None and master_elem.get('is_main_release') == 'true' else 0
    
    # Insert release
    cursor.execute('''
        INSERT OR REPLACE INTO releases 
        (id, title, country, released, notes, data_quality, master_id, master_is_main)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (release_id, title, country, released, notes, data_quality, master_id, master_is_main))
    
    # Parse artists
    artists_elem = element.find('artists')
    if artists_elem is not None:
        for artist_elem in artists_elem.findall('artist'):
            artist_id_elem = artist_elem.find('id')
            name_elem = artist_elem.find('name')
            join_elem = artist_elem.find('join')
            
            if artist_id_elem is not None and name_elem is not None:
                artist_id = int(artist_id_elem.text)
                artist_name = name_elem.text or ''
                join_text = join_elem.text if join_elem is not None else ''
                
                # Insert artist
                cursor.execute('INSERT OR IGNORE INTO artists (id, name) VALUES (?, ?)', 
                             (artist_id, artist_name))
                
                # Link to release
                cursor.execute('''
                    INSERT OR IGNORE INTO release_artists (release_id, artist_id, join_text)
                    VALUES (?, ?, ?)
                ''', (release_id, artist_id, join_text))
    
    # Parse labels
    labels_elem = element.find('labels')
    if labels_elem is not None:
        for label_elem in labels_elem.findall('label'):
            label_id = int(label_elem.get('id', 0))
            label_name = label_elem.get('name', '')
            catno = label_elem.get('catno', '')
            
            if label_id and label_name:
                cursor.execute('INSERT OR IGNORE INTO labels (id, name, catno) VALUES (?, ?, ?)',
                             (label_id, label_name, catno))
                
                cursor.execute('''
                    INSERT OR IGNORE INTO release_labels (release_id, label_id, catno)
                    VALUES (?, ?, ?)
                ''', (release_id, label_id, catno))
    
    # Parse formats
    formats_elem = element.find('formats')
    if formats_elem is not None:
        for format_elem in formats_elem.findall('format'):
            format_name = format_elem.get('name', '')
            qty = format_elem.get('qty', '')
            text = format_elem.get('text', '')
            
            format_id = get_or_insert(cursor, 'formats', 
                                      {'name': format_name, 'qty': qty, 'text': text},
                                      {'name': format_name, 'qty': qty, 'text': text})
            
            cursor.execute('''
                INSERT OR IGNORE INTO release_formats (release_id, format_id)
                VALUES (?, ?)
            ''', (release_id, format_id))
            
            # Parse format descriptions
            desc_elem = format_elem.find('descriptions')
            if desc_elem is not None:
                for desc in desc_elem.findall('description'):
                    if desc.text:
                        cursor.execute('''
                            INSERT INTO format_descriptions (format_id, description)
                            VALUES (?, ?)
                        ''', (format_id, desc.text))
    
    # Parse genres
    genres_elem = element.find('genres')
    if genres_elem is not None:
        for genre_elem in genres_elem.findall('genre'):
            if genre_elem.text:
                genre_id = get_or_insert(cursor, 'genres',
                                        {'name': genre_elem.text},
                                        {'name': genre_elem.text})
                cursor.execute('''
                    INSERT OR IGNORE INTO release_genres (release_id, genre_id)
                    VALUES (?, ?)
                ''', (release_id, genre_id))
    
    # Parse styles
    styles_elem = element.find('styles')
    if styles_elem is not None:
        for style_elem in styles_elem.findall('style'):
            if style_elem.text:
                style_id = get_or_insert(cursor, 'styles',
                                        {'name': style_elem.text},
                                        {'name': style_elem.text})
                cursor.execute('''
                    INSERT OR IGNORE INTO release_styles (release_id, style_id)
                    VALUES (?, ?)
                ''', (release_id, style_id))
    
    # Parse extra artists
    extraartists_elem = element.find('extraartists')
    if extraartists_elem is not None:
        for ea_elem in extraartists_elem.findall('artist'):
            ea_id_elem = ea_elem.find('id')
            name_elem = ea_elem.find('name')
            anv_elem = ea_elem.find('anv')
            role_elem = ea_elem.find('role')
            
            if ea_id_elem is not None and name_elem is not None:
                ea_id = int(ea_id_elem.text)
                ea_name = name_elem.text or ''
                anv = anv_elem.text if anv_elem is not None else ''
                role = role_elem.text if role_elem is not None else ''
                
                cursor.execute('''
                    INSERT OR IGNORE INTO extraartists (id, name, anv, role)
                    VALUES (?, ?, ?, ?)
                ''', (ea_id, ea_name, anv, role))
                
                cursor.execute('''
                    INSERT OR IGNORE INTO release_extraartists (release_id, extraartist_id)
                    VALUES (?, ?)
                ''', (release_id, ea_id))
    
    # Parse tracks
    tracklist_elem = element.find('tracklist')
    if tracklist_elem is not None:
        for track_elem in tracklist_elem.findall('track'):
            position = track_elem.findtext('position', '')
            track_title = track_elem.findtext('title', '')
            
            cursor.execute('''
                INSERT INTO tracks (release_id, position, title)
                VALUES (?, ?, ?)
            ''', (release_id, position, track_title))
            
            track_id = cursor.lastrowid
            
            # Parse track extra artists
            track_ea_elem = track_elem.find('extraartists')
            if track_ea_elem is not None:
                for tea_elem in track_ea_elem.findall('artist'):
                    tea_id_elem = tea_elem.find('id')
                    tea_name_elem = tea_elem.find('name')
                    tea_anv_elem = tea_elem.find('anv')
                    tea_role_elem = tea_elem.find('role')
                    
                    if tea_id_elem is not None and tea_name_elem is not None:
                        tea_id = int(tea_id_elem.text)
                        tea_name = tea_name_elem.text or ''
                        tea_anv = tea_anv_elem.text if tea_anv_elem is not None else ''
                        tea_role = tea_role_elem.text if tea_role_elem is not None else ''
                        
                        cursor.execute('''
                            INSERT OR IGNORE INTO extraartists (id, name, anv, role)
                            VALUES (?, ?, ?, ?)
                        ''', (tea_id, tea_name, tea_anv, tea_role))
                        
                        cursor.execute('''
                            INSERT OR IGNORE INTO track_extraartists (track_id, extraartist_id)
                            VALUES (?, ?)
                        ''', (track_id, tea_id))
    
    # Parse videos
    videos_elem = element.find('videos')
    if videos_elem is not None:
        for video_elem in videos_elem.findall('video'):
            src = video_elem.get('src', '')
            duration = video_elem.get('duration', '')
            embed = video_elem.get('embed', '')
            video_title = video_elem.findtext('title', '')
            video_desc = video_elem.findtext('description', '')
            
            cursor.execute('''
                INSERT INTO videos (release_id, src, duration, embed, title, description)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (release_id, src, duration or '0', embed, video_title, video_desc))
    
    # Parse companies
    companies_elem = element.find('companies')
    if companies_elem is not None:
        for company_elem in companies_elem.findall('company'):
            company_id = int(company_elem.get('id', 0))
            company_name = company_elem.findtext('name', '')
            entity_type = company_elem.findtext('entity_type', '')
            entity_type_name = company_elem.findtext('entity_type_name', '')
            
            if company_id and company_name:
                cursor.execute('''
                    INSERT OR IGNORE INTO companies (id, name, entity_type, entity_type_name)
                    VALUES (?, ?, ?, ?)
                ''', (company_id, company_name, entity_type, entity_type_name))
                
                cursor.execute('''
                    INSERT OR IGNORE INTO release_companies (release_id, company_id)
                    VALUES (?, ?)
                ''', (release_id, company_id))
    
    # Parse series
    series_elem = element.find('series')
    if series_elem is not None:
        for series_item in series_elem.findall('series'):
            series_name = series_item.get('name', '')
            series_catno = series_item.get('catno', '')
            series_id = int(series_item.get('id', 0))
            
            if series_id and series_name:
                cursor.execute('''
                    INSERT OR IGNORE INTO series (id, name, catno)
                    VALUES (?, ?, ?)
                ''', (series_id, series_name, series_catno))
                
                cursor.execute('''
                    INSERT OR IGNORE INTO release_series (release_id, series_id, catno)
                    VALUES (?, ?, ?)
                ''', (release_id, series_id, series_catno))


def parse_xml_file(xml_path, db_path, batch_size=1000):
    """Parse the XML file and import into SQLite using iterative parsing."""
    print(f"Parsing {xml_path}...")
    print(f"Database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    
    releases_count = 0
    batch_count = 0
    
    # Use iterative parsing for large files
    context = ET.iterparse(xml_path, events=('end',))
    
    for event, elem in context:
        if elem.tag == 'release':
            parse_release(elem, conn)
            releases_count += 1
            batch_count += 1
            
            # Clear element to free memory
            elem.clear()
            
            # Commit in batches
            if batch_count >= batch_size:
                conn.commit()
                print(f"  Processed {releases_count} releases...")
                batch_count = 0
    
    # Final commit
    conn.commit()
    
    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM releases")
    count = cursor.fetchone()[0]
    
    print(f"\nImport complete!")
    print(f"  Total releases: {count}")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Parse Discogs XML release database and import into SQLite.'
    )
    parser.add_argument(
        'xml_file',
        help='Path to Discogs XML file (e.g., discogs_20240101_releases.xml)'
    )
    parser.add_argument(
        '--db', '-d',
        default='discogs.db',
        help='Output SQLite database path (default: discogs.db)'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=1000,
        help='Number of releases to process before each commit (default: 1000)'
    )
    
    args = parser.parse_args()
    
    xml_path = Path(args.xml_file)
    if not xml_path.exists():
        print(f"Error: XML file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)
    
    db_path = Path(args.db)
    
    parse_xml_file(xml_path, db_path, args.batch_size)


if __name__ == '__main__':
    main()
