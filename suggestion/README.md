# Music Explorer - Discogs Release Recommender

Graph-based music recommendation system using Discogs data and Neo4j.

## Overview

Finds similar releases by traversing connections between:
- Artists and contributors (producers, engineers, mastering, artwork)
- Labels
- Similar artists

Uses IDF-style weighting to surface long-tail discoveries over popular releases.

## Requirements

- Python 3.12+
- Neo4j (bolt://localhost:7687)
- Redis (localhost:6379)
- PostgreSQL 16+ (for local Discogs XML import and Bandcamp correlation)
- Discogs API token

## Setup

### 1. Start infrastructure servers

```bash
./install_deps.sh
cp dot_env.example .env
# Edit .env with your Discogs token
./start_servers.sh
source .venv/bin/activate
```

### 2. PostgreSQL (optional, needed for local correlation and Discogs XML import)

Run via Docker (or use a local install):

```bash
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  --name postgres \
  postgres:16

export DATABASE_URL="postgresql://postgres:password@localhost/discogs"
createdb -h localhost -U postgres discogs
```

Add `DATABASE_URL` to `.env` so scripts pick it up automatically.

### 3. Import Discogs XML dump (one-time, ~5 hours)

Download the **releases** data dump using the helper script (only `discogs_YYYYMMDD_releases.xml.gz` is needed — artists and labels are extracted from the release records):

```bash
./local/download-discogs-dump.sh
```

This saves the file as `discogs_YYYYMMDD_releases.xml.gz` and symlinks it to
`discogs_releases.xml.gz`. Running it again when the DB is already populated
will warn you and exit.

Then import:

```bash
# Extract (the gz file contains a single XML)
gunzip discogs_releases.xml.gz

# Import into PostgreSQL
./local/discogs-xml-to-pg.py discogs_releases.xml
```

This imports ~18M releases into PostgreSQL, creating tables for releases, artists,
tracks, and their relationships. Only needed if you want to correlate Bandcamp
releases offline instead of using the Discogs API.

Without PostgreSQL, Bandcamp correlation uses the Discogs API (`correlate/correlate.py`),
which is slower (~9s per release) but doesn't need a local database.

## Usage

**Full pipeline (recommended):**
```bash
# Create preferences file
cat > prefs.txt << EOF
label/release-love 5
label/release-ok 2
label/release-dont-like 0
EOF

# Run pipeline: correlate → traverse → weight → rank
# --db is the PostgreSQL connection string from DATABASE_URL
./pipeline/pipeline.py -f prefs.txt -d 2 -n 20 --db "$DATABASE_URL"
```

**Step-by-step:**

1. Import Discogs XML dump (one-time, ~5 hours):
```bash
./local/discogs-xml-to-pg.py discogs_$(date +%Y%m%d)_releases.xml
```

2. Correlate Bandcamp pages to Discogs IDs:
```bash
find <bandcamp-dir> -name page.html | ./local/correlate-local-pg.py -v
```

3. Traverse and weight from seed releases:
```bash
./pipeline/traverse_and_weight.py -f releases.txt -d 2
```

4. Find similar releases for a single Discogs URL:
```bash
./pipeline/similar.py https://www.discogs.com/release/12345
```

## Architecture

**local/** - Local database import and correlation (no API rate limits):
- `discogs-xml-to-pg.py` - Import 45GB Discogs XML dump into PostgreSQL (~5 hours for 18M releases)
- `correlate-local-pg.py` - Match Bandcamp pages to Discogs using local PostgreSQL (sub-second per release)
- `discogs-xml-to-db.py` - Import Discogs XML into SQLite (legacy, too slow for large datasets)
- `correlate-local.py` - Match Bandcamp pages using local SQLite (legacy)

**correlate/** - API-based correlation:
- `correlate.py` - Match Bandcamp pages to Discogs API with Redis caching (~9s per request)

**pipeline/** - Recommendation pipeline:
- `pipeline.py` - Full pipeline: preferences → correlate → traverse → weight → rank
- `aggregate.py` - Multi-release recommender with Adamic-Adar + Jaccard + IDF scoring
- `traverse_and_weight.py` - Combined BFS traversal and IDF weighting
- `traverse.py` - BFS graph traversal from seed releases
- `weight.py` - IDF weighting for graph features
- `similar.py` - Find similar releases for a single Discogs URL
- `rank_feature.py` - Feature ranking utilities

**Root** - Core infrastructure:
- `ingest.py` - Discogs API client + Neo4j ingestion
- `graph.py` - Neo4j query interface
- `lib.py` - Shared utility functions
- `cache.py` - Redis caching for API responses and correlations
- `config.py` - Environment configuration and logging
- `install_deps.sh` - Install Python dependencies
- `start_servers.sh` - Start Neo4j and Redis servers

## Depth Levels

- `1` - Direct connections only (same artist, shared contributors)
- `2` - + Label/producer sharing (default)
- `3` - + Similar artist connections

Higher depths use stricter degree thresholds for long-tail discovery.