#!/usr/bin/env python3
"""
Fast Discogs XML release database importer -> SQLite.

Optimised for a single sequential pass over the ~59G dump:
  - lxml iterparse (much faster than ElementTree)
  - Python-side dedupe sets/dicts (no per-row SELECTs)
  - batched executemany inserts, commit every N releases
  - WAL + synchronous=OFF + big cache

Usage:
    python discogs-xml-to-db-fast.py /path/to/discogs_releases.xml \
        [--db discogs.db] [--batch-size 50000]

NOTE: PRAGMA synchronous=OFF means a power loss mid-import loses the
in-flight commit only (and the DB is rebuildable from the XML), so this
is an acceptable trade for a bulk import.
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

from lxml import etree


def create_schema(conn):
    cursor = conn.cursor()
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY,
            title TEXT,
            country TEXT,
            released TEXT,
            notes TEXT,
            data_quality TEXT,
            master_id INTEGER,
            master_is_main INTEGER
        );

        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY,
            name TEXT
        );

        CREATE TABLE IF NOT EXISTS release_artists (
            release_id INTEGER,
            artist_id INTEGER,
            join_text TEXT,
            PRIMARY KEY (release_id, artist_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (artist_id) REFERENCES artists(id)
        );

        CREATE TABLE IF NOT EXISTS labels (
            id INTEGER PRIMARY KEY,
            name TEXT,
            catno TEXT
        );

        CREATE TABLE IF NOT EXISTS release_labels (
            release_id INTEGER,
            label_id INTEGER,
            catno TEXT,
            PRIMARY KEY (release_id, label_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (label_id) REFERENCES labels(id)
        );

        CREATE TABLE IF NOT EXISTS formats (
            id INTEGER PRIMARY KEY,
            name TEXT,
            qty TEXT,
            text TEXT
        );

        CREATE TABLE IF NOT EXISTS release_formats (
            release_id INTEGER,
            format_id INTEGER,
            PRIMARY KEY (release_id, format_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (format_id) REFERENCES formats(id)
        );

        CREATE TABLE IF NOT EXISTS format_descriptions (
            format_id INTEGER,
            description TEXT,
            PRIMARY KEY (format_id, description),
            FOREIGN KEY (format_id) REFERENCES formats(id)
        );

        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS release_genres (
            release_id INTEGER,
            genre_id INTEGER,
            PRIMARY KEY (release_id, genre_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id)
        );

        CREATE TABLE IF NOT EXISTS styles (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS release_styles (
            release_id INTEGER,
            style_id INTEGER,
            PRIMARY KEY (release_id, style_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (style_id) REFERENCES styles(id)
        );

        CREATE TABLE IF NOT EXISTS extraartists (
            id INTEGER PRIMARY KEY,
            name TEXT,
            anv TEXT,
            role TEXT
        );

        CREATE TABLE IF NOT EXISTS release_extraartists (
            release_id INTEGER,
            extraartist_id INTEGER,
            PRIMARY KEY (release_id, extraartist_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (extraartist_id) REFERENCES extraartists(id)
        );

        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY,
            release_id INTEGER,
            position TEXT,
            title TEXT,
            FOREIGN KEY (release_id) REFERENCES releases(id)
        );

        CREATE TABLE IF NOT EXISTS track_extraartists (
            track_id INTEGER,
            extraartist_id INTEGER,
            PRIMARY KEY (track_id, extraartist_id),
            FOREIGN KEY (track_id) REFERENCES tracks(id),
            FOREIGN KEY (extraartist_id) REFERENCES extraartists(id)
        );

        CREATE TABLE IF NOT EXISTS videos (
            release_id INTEGER,
            src TEXT,
            duration INTEGER,
            embed TEXT,
            title TEXT,
            description TEXT,
            FOREIGN KEY (release_id) REFERENCES releases(id)
        );

        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            name TEXT,
            entity_type TEXT,
            entity_type_name TEXT
        );

        CREATE TABLE IF NOT EXISTS release_companies (
            release_id INTEGER,
            company_id INTEGER,
            PRIMARY KEY (release_id, company_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        );

        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY,
            name TEXT,
            catno TEXT
        );

        CREATE TABLE IF NOT EXISTS release_series (
            release_id INTEGER,
            series_id INTEGER,
            catno TEXT,
            PRIMARY KEY (release_id, series_id),
            FOREIGN KEY (release_id) REFERENCES releases(id),
            FOREIGN KEY (series_id) REFERENCES series(id)
        );

        CREATE INDEX IF NOT EXISTS idx_releases_title ON releases(title);
        CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name);
        CREATE INDEX IF NOT EXISTS idx_release_artists_artist ON release_artists(artist_id);
        CREATE INDEX IF NOT EXISTS idx_release_labels_label ON release_labels(label_id);
        CREATE INDEX IF NOT EXISTS idx_tracks_title ON tracks(title);
        CREATE INDEX IF NOT EXISTS idx_tracks_release ON tracks(release_id);
        CREATE INDEX IF NOT EXISTS idx_release_extraartists_ea ON release_extraartists(extraartist_id);
    ''')
    conn.commit()


class Importer:
    def __init__(self, conn, batch_size):
        self.conn = conn
        self.batch_size = batch_size

        # dedupe state (XML provides stable IDs for most entities)
        self.seen_artists = set()
        self.seen_labels = set()
        self.seen_extra = set()
        self.seen_companies = set()
        self.seen_series = set()
        # no XML IDs -> assign our own
        self.fmt_id = {}
        self.genre_id = {}
        self.style_id = {}
        self._next_fmt = 1
        self._next_genre = 1
        self._next_style = 1

        # batch buffers
        self.b = {
            'releases': [], 'artists': [], 'release_artists': [],
            'labels': [], 'release_labels': [],
            'formats': [], 'release_formats': [], 'format_descriptions': [],
            'genres': [], 'release_genres': [],
            'styles': [], 'release_styles': [],
            'extraartists': [], 'release_extraartists': [],
            'tracks': [], 'track_extraartists': [],
            'videos': [],
            'companies': [], 'release_companies': [],
            'series': [], 'release_series': [],
        }
        self.track_base = 0
        self.track_seq = 0
        self.count = 0
        self.t0 = time.time()

    # -- id helpers ---------------------------------------------------
    def fmt(self, name, qty, text):
        key = (name, qty, text)
        v = self.fmt_id.get(key)
        if v is None:
            v = self._next_fmt
            self._next_fmt += 1
            self.fmt_id[key] = v
            self.b['formats'].append((v, name, qty, text))
        return v

    def genre(self, name):
        v = self.genre_id.get(name)
        if v is None:
            v = self._next_genre
            self._next_genre += 1
            self.genre_id[name] = v
            self.b['genres'].append((v, name))
        return v

    def style(self, name):
        v = self.style_id.get(name)
        if v is None:
            v = self._next_style
            self._next_style += 1
            self.style_id[name] = v
            self.b['styles'].append((v, name))
        return v

    def track_id(self):
        self.track_seq += 1
        return self.track_base + self.track_seq

    # -- per-release parsing ------------------------------------------
    def parse_release(self, e):
        b = self.b
        rid = int(e.get('id', 0))
        if not rid:
            return

        def txt(tag):
            el = e.find(tag)
            return el.text if el is not None and el.text else ''

        master = e.find('master_id')
        b['releases'].append((
            rid, txt('title'), txt('country'), txt('released'),
            txt('notes'), txt('data_quality'),
            int(master.get('id', 0)) if master is not None else 0,
            1 if master is not None and master.get('is_main_release') == 'true' else 0,
        ))

        # artists
        arts = e.find('artists')
        if arts is not None:
            for a in arts.findall('artist'):
                id_el = a.find('id')
                name_el = a.find('name')
                if id_el is None or name_el is None:
                    continue
                aid = int(id_el.text)
                if aid not in self.seen_artists:
                    self.seen_artists.add(aid)
                    b['artists'].append((aid, name_el.text or ''))
                join_el = a.find('join')
                b['release_artists'].append(
                    (rid, aid, join_el.text if join_el is not None else ''))

        # labels
        labs = e.find('labels')
        if labs is not None:
            for l in labs.findall('label'):
                lid = int(l.get('id', 0))
                lname = l.get('name', '')
                if not lid or not lname:
                    continue
                catno = l.get('catno', '')
                if lid not in self.seen_labels:
                    self.seen_labels.add(lid)
                    b['labels'].append((lid, lname, catno))
                b['release_labels'].append((rid, lid, catno))

        # formats
        fmts = e.find('formats')
        if fmts is not None:
            for f in fmts.findall('format'):
                fid = self.fmt(f.get('name', ''), f.get('qty', ''), f.get('text', ''))
                b['release_formats'].append((rid, fid))
                descs = f.find('descriptions')
                if descs is not None:
                    for d in descs.findall('description'):
                        if d.text:
                            b['format_descriptions'].append((None, fid, d.text))

        # genres / styles
        genres = e.find('genres')
        if genres is not None:
            for g in genres.findall('genre'):
                if g.text:
                    b['release_genres'].append((rid, self.genre(g.text)))
        styles = e.find('styles')
        if styles is not None:
            for s in styles.findall('style'):
                if s.text:
                    b['release_styles'].append((rid, self.style(s.text)))

        # extra artists
        eas = e.find('extraartists')
        if eas is not None:
            for ea in eas.findall('artist'):
                id_el = ea.find('id')
                name_el = ea.find('name')
                if id_el is None or name_el is None:
                    continue
                eid = int(id_el.text)
                if eid not in self.seen_extra:
                    self.seen_extra.add(eid)
                    anv = ea.find('anv')
                    role = ea.find('role')
                    b['extraartists'].append((
                        eid, name_el.text or '',
                        anv.text if anv is not None else '',
                        role.text if role is not None else ''))
                b['release_extraartists'].append((rid, eid))

        # tracks (explicit sequential ids: base + seq)
        tl = e.find('tracklist')
        if tl is not None:
            for t in tl.findall('track'):
                tid = self.track_id()
                b['tracks'].append((tid, rid, t.findtext('position', '') or '',
                                    t.findtext('title', '') or ''))
                tea = t.find('extraartists')
                if tea is not None:
                    for ea in tea.findall('artist'):
                        id_el = ea.find('id')
                        name_el = ea.find('name')
                        if id_el is None or name_el is None:
                            continue
                        eid = int(id_el.text)
                        if eid not in self.seen_extra:
                            self.seen_extra.add(eid)
                            anv = ea.find('anv')
                            role = ea.find('role')
                            b['extraartists'].append((
                                eid, name_el.text or '',
                                anv.text if anv is not None else '',
                                role.text if role is not None else ''))
                        b['track_extraartists'].append((tid, eid))

        # videos
        vids = e.find('videos')
        if vids is not None:
            for v in vids.findall('video'):
                dur = v.get('duration', '')
                b['videos'].append((
                    rid, v.get('src', ''), dur or '0', v.get('embed', ''),
                    v.findtext('title', '') or '', v.findtext('description', '') or ''))

        # companies
        comps = e.find('companies')
        if comps is not None:
            for c in comps.findall('company'):
                cid = int(c.get('id', 0))
                cname = c.findtext('name', '')
                if not cid or not cname:
                    continue
                if cid not in self.seen_companies:
                    self.seen_companies.add(cid)
                    b['companies'].append((
                        cid, cname,
                        c.findtext('entity_type', '') or '',
                        c.findtext('entity_type_name', '') or ''))
                b['release_companies'].append((rid, cid))

        # series
        ser = e.find('series')
        if ser is not None:
            for s in ser.findall('series'):
                sid = int(s.get('id', 0))
                sname = s.get('name', '')
                if not sid or not sname:
                    continue
                scatno = s.get('catno', '')
                if sid not in self.seen_series:
                    self.seen_series.add(sid)
                    b['series'].append((sid, sname, scatno))
                b['release_series'].append((rid, sid, scatno))

    # -- flush ---------------------------------------------------------
    def flush(self):
        cur = self.conn.cursor()
        cur.executemany(
            'INSERT OR REPLACE INTO releases (id, title, country, released, '
            'notes, data_quality, master_id, master_is_main) VALUES (?,?,?,?,?,?,?,?)',
            self.b['releases'])
        cur.executemany('INSERT INTO artists (id, name) VALUES (?,?)',
                        self.b['artists'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_artists (release_id, artist_id, join_text) '
            'VALUES (?,?,?)', self.b['release_artists'])
        cur.executemany('INSERT INTO labels (id, name, catno) VALUES (?,?,?)',
                        self.b['labels'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_labels (release_id, label_id, catno) '
            'VALUES (?,?,?)', self.b['release_labels'])
        cur.executemany('INSERT INTO formats (id, name, qty, text) VALUES (?,?,?,?)',
                        self.b['formats'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_formats (release_id, format_id) '
            'VALUES (?,?)', self.b['release_formats'])
        cur.executemany(
            'INSERT OR IGNORE INTO format_descriptions (format_id, description) '
            'VALUES (?,?)',
            [(f, d) for (_, f, d) in self.b['format_descriptions']])
        cur.executemany('INSERT INTO genres (id, name) VALUES (?,?)',
                        self.b['genres'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_genres (release_id, genre_id) '
            'VALUES (?,?)', self.b['release_genres'])
        cur.executemany('INSERT INTO styles (id, name) VALUES (?,?)',
                        self.b['styles'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_styles (release_id, style_id) '
            'VALUES (?,?)', self.b['release_styles'])
        cur.executemany(
            'INSERT INTO extraartists (id, name, anv, role) VALUES (?,?,?,?)',
            self.b['extraartists'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_extraartists (release_id, '
            'extraartist_id) VALUES (?,?)', self.b['release_extraartists'])
        cur.executemany(
            'INSERT INTO tracks (id, release_id, position, title) '
            'VALUES (?,?,?,?)', self.b['tracks'])
        cur.executemany(
            'INSERT OR IGNORE INTO track_extraartists (track_id, '
            'extraartist_id) VALUES (?,?)', self.b['track_extraartists'])
        cur.executemany(
            'INSERT INTO videos (release_id, src, duration, embed, title, '
            'description) VALUES (?,?,?,?,?,?)', self.b['videos'])
        cur.executemany(
            'INSERT INTO companies (id, name, entity_type, entity_type_name) '
            'VALUES (?,?,?,?)', self.b['companies'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_companies (release_id, company_id) '
            'VALUES (?,?)', self.b['release_companies'])
        cur.executemany('INSERT INTO series (id, name, catno) VALUES (?,?,?)',
                        self.b['series'])
        cur.executemany(
            'INSERT OR IGNORE INTO release_series (release_id, series_id, catno) '
            'VALUES (?,?,?)', self.b['release_series'])
        self.conn.commit()

        # advance track id base for the next batch
        self.track_base += self.track_seq
        self.track_seq = 0
        for k in self.b:
            self.b[k] = []

    def progress(self):
        dt = time.time() - self.t0
        rate = self.count / dt if dt > 0 else 0
        print(f"  {self.count:>12,} releases  {rate:,.0f}/s  ({dt / 60:.1f} min)",
              flush=True)


def parse_xml_file(xml_path, db_path, batch_size):
    print(f"Parsing {xml_path} ...", flush=True)
    print(f"Database: {db_path}", flush=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=OFF')
    cur.execute('PRAGMA cache_size=-262144')
    cur.execute('PRAGMA temp_store=MEMORY')
    cur.execute('PRAGMA mmap_size=1073741824')
    conn.commit()

    create_schema(conn)

    imp = Importer(conn, batch_size)
    t0 = time.time()

    context = etree.iterparse(xml_path, events=('end',), tag=b'release',
                              huge_tree=False)
    for _event, elem in context:
        imp.parse_release(elem)
        imp.count += 1
        elem.clear()
        # keep the root from accumulating cleared siblings
        parent = elem.getparent()
        if parent is not None:
            parent.remove(elem)

        if imp.count % batch_size == 0:
            imp.flush()
            if imp.count % (batch_size * 10) == 0:
                imp.progress()

    imp.flush()
    conn.execute('ANALYZE')
    conn.commit()

    # verify
    counts = {}
    for table in ('releases', 'artists', 'labels', 'tracks',
                  'release_artists', 'release_labels', 'extraartists',
                  'genres', 'styles'):
        counts[table] = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]

    dt = time.time() - t0
    print(f"\nImport complete! {dt / 60:.1f} min total", flush=True)
    for t, c in counts.items():
        print(f"  {t:>16}: {c:,}", flush=True)
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Fast Discogs XML -> SQLite importer (lxml).')
    parser.add_argument('xml_file',
                        help='Path to Discogs XML file')
    parser.add_argument('--db', '-d', default='discogs.db',
                        help='Output SQLite database path (default: discogs.db)')
    parser.add_argument('--batch-size', '-b', type=int, default=50000,
                        help='Releases per commit (default: 50000)')
    args = parser.parse_args()

    xml_path = Path(args.xml_file)
    if not xml_path.exists():
        print(f"Error: XML file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)

    parse_xml_file(xml_path, Path(args.db), args.batch_size)


if __name__ == '__main__':
    main()
