#!/usr/bin/env python3
"""
Local Discogs SQLite backend.

Provides the same data shapes that `ingest.py` returns from the rate-limited
Discogs API, but served from the locally-imported SQLite database (built by
`local/discogs-xml-to-db-fast.py` from the Discogs XML dump).

The pipeline (traverse.py / suggest.py / bc.py) can therefore run fully
offline and with no per-request rate limits.

Environment:
    DISCOGS_LOCAL_DB  path to the SQLite file (default: .data/discogs.db
                      next to this file). If unset or the file is missing,
                      `available()` returns False and callers fall back to
                      the API.
"""
import os
import sqlite3
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB = os.path.join(_HERE, ".data", "discogs.db")

_lock = threading.Lock()
_conn = None


def db_path():
    return os.getenv("DISCOGS_LOCAL_DB") or _DEFAULT_DB


def available():
    """True if the local DB exists and is readable."""
    p = db_path()
    if not os.path.exists(p):
        return False
    try:
        c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        c.execute("SELECT 1 FROM releases LIMIT 1").fetchone()
        c.close()
        return True
    except sqlite3.Error:
        return False


def _connect():
    """Open (and cache) a read-optimized connection."""
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(f"file:{db_path()}?mode=ro", uri=True,
                                check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA query_only=ON")
        _conn.execute("PRAGMA cache_size=-65536")  # 64 MB
        _conn.execute("PRAGMA mmap_size=1073741824")
    return _conn


# ---------------------------------------------------------------------------
# API-shaped accessors (mirror ingest.py return structures)
# ---------------------------------------------------------------------------

def fetch_release(release_id):
    """
    Mirror of `releases/{id}` response, trimmed to the fields the pipeline
    uses: title, artists, extraartists, labels.
    """
    c = _connect()
    rid = int(release_id)
    row = c.execute(
        "SELECT title, released FROM releases WHERE id=?", (rid,)).fetchone()
    if row is None:
        return None

    import re as _re
    year = None
    m = _re.match(r'\s*(\d{4})', row["released"] or "")
    if m:
        year = int(m.group(1))

    tracklist = [
        {"title": r["title"]} for r in c.execute(
            "SELECT title FROM tracks WHERE release_id=? ORDER BY id", (rid,))
    ]

    artists = [
        {"id": str(r["artist_id"]), "name": r["name"]}
        for r in c.execute(
            "SELECT ra.artist_id AS artist_id, a.name AS name "
            "FROM release_artists ra JOIN artists a ON a.id=ra.artist_id "
            "WHERE ra.release_id=? ORDER BY ra.rowid", (rid,))
    ]
    extraartists = [
        {"id": str(r["ea_id"]), "name": r["name"], "role": r["role"]}
        for r in c.execute(
            "SELECT rea.extraartist_id AS ea_id, e.name AS name, e.role AS role "
            "FROM release_extraartists rea JOIN extraartists e "
            "ON e.id=rea.extraartist_id WHERE rea.release_id=?", (rid,))
    ]
    labels = [
        {"id": str(r["label_id"]), "name": r["name"]}
        for r in c.execute(
            "SELECT rl.label_id AS label_id, l.name AS name "
            "FROM release_labels rl JOIN labels l ON l.id=rl.label_id "
            "WHERE rl.release_id=? AND l.name != 'Not On Label'", (rid,))
    ]
    return {
        "id": rid,
        "title": row["title"],
        "year": year,
        "artists": artists,
        "extraartists": extraartists,
        "labels": labels,
        "tracklist": tracklist,
    }


def fetch_artist_releases(artist_id, limit=200):
    """
    Mirror of `artists/{id}/releases`, trimmed to {releases:[{id,title,artist}]}.
    Sorted by release year (desc) then id, matching how the API tends to rank
    recent/most-relevant first.
    """
    c = _connect()
    aid = int(artist_id)
    rows = c.execute(
        """
        SELECT ra.release_id AS id, r.title AS title, a.name AS artist
        FROM release_artists ra
        JOIN releases r ON r.id=ra.release_id
        JOIN artists a ON a.id=ra.artist_id
        WHERE ra.artist_id=?
        ORDER BY r.released DESC, r.id DESC
        LIMIT ?
        """, (aid, limit)).fetchall()
    return {
        "releases": [
            {"id": r["id"], "title": r["title"], "artist": r["artist"]}
            for r in rows
        ]
    }


def fetch_label_releases(label_id, page=1, per_page=25):
    """
    Mirror of `labels/{id}/releases?page={page}`:
    {pagination:{pages}, releases:[{id, artist_id, artist, title}]}
    """
    c = _connect()
    lid = int(label_id)
    total = c.execute(
        "SELECT COUNT(*) AS n FROM release_labels WHERE label_id=?",
        (lid,)).fetchone()["n"]
    pages = max(1, (total + per_page - 1) // per_page)
    offset = (max(1, page) - 1) * per_page
    rows = c.execute(
        """
        SELECT rl.release_id AS id,
               r.title AS title,
               ra.artist_id AS artist_id,
               a.name AS artist
        FROM release_labels rl
        JOIN releases r ON r.id=rl.release_id
        LEFT JOIN release_artists ra ON ra.release_id=rl.release_id
        LEFT JOIN artists a ON a.id=ra.artist_id
        WHERE rl.label_id=?
        ORDER BY r.released DESC, r.id DESC
        LIMIT ? OFFSET ?
        """, (lid, per_page, offset)).fetchall()
    return {
        "pagination": {"page": page, "pages": pages, "items": total},
        "releases": [
            {"id": r["id"], "title": r["title"],
             "artist_id": r["artist_id"], "artist": r["artist"]}
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Search helpers (no Discogs API needed)
# ---------------------------------------------------------------------------

def _norm(s):
    import re
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def search_releases(artist=None, title=None, year=None, limit=20):
    """
    Find releases matching artist name / release title (fuzzy, normalized)
    and optional release year. Used as an offline replacement for the
    Discogs search endpoint when correlating a Bandcamp page to a Discogs id.

    If a year is given it's applied first (indexed), which cuts the scan
    from ~18M rows to ~150k before the LIKE filters run.
    """
    c = _connect()
    clauses, params = [], []
    if year:
        clauses.append("r.released LIKE ?")
        params.append(f"{int(year)}%")
    if title:
        clauses.append("lower(r.title) LIKE ?")
        params.append(f"%{title.lower()}%")
    if artist:
        clauses.append(
            "EXISTS (SELECT 1 FROM release_artists ra2 "
            "JOIN artists a2 ON a2.id=ra2.artist_id "
            "WHERE ra2.release_id=r.id AND lower(a2.name) LIKE ?)")
        params.append(f"%{artist.lower()}%")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = c.execute(
        f"""
        SELECT r.id, r.title, r.released,
               (SELECT a2.name FROM release_artists ra2
                JOIN artists a2 ON a2.id=ra2.artist_id
                WHERE ra2.release_id=r.id
                ORDER BY ra2.rowid LIMIT 1) AS artist
        FROM releases r {where}
        ORDER BY r.id
        LIMIT ?
        """, params + [limit]).fetchall()
    return [
        {"id": r["id"], "title": r["title"], "released": r["released"],
         "artist": r["artist"]}
        for r in rows
    ]


if __name__ == "__main__":
    print(f"DB: {db_path()}")
    print(f"available: {available()}")
    if available():
        n = _connect().execute("SELECT COUNT(*) FROM releases").fetchone()[0]
        print(f"releases: {n}")
