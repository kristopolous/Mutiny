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
- Discogs API token

## Setup

```bash
./install_deps.sh
cp dot_env.example .env
# Edit .env with your Discogs token
./start_servers.sh
source .venv/bin/activate
```

## Usage

**Aggregate recommendations from multiple releases:**
```bash
./aggregate.py -f releases.txt -n 20 -d 2
```

**Traverse and weight releases:**
```bash
./traverse_and_weight.py -f releases.txt -d 2
```

**Find similar releases for a single artist/release:**
```bash
./similar.py https://www.discogs.com/release/12345
```

**Correlate Bandcamp URL to Discogs:**
```bash
python correlate.py https://artist.bandcamp.com/album/release-name
```

## Architecture

- `aggregate.py` - Main recommender with Adamic-Adar + Jaccard + Label IDF scoring
- `traverse.py` - BFS graph traversal
- `weight.py` - IDF weighting for features
- `ingest.py` - Discogs API client + Neo4j ingestion
- `graph.py` - Neo4j query interface
- `cache.py` - Redis caching for API responses
- `config.py` - Environment configuration

## Depth Levels

- `1` - Direct connections only (same artist, shared contributors)
- `2` - + Label/producer sharing (default)
- `3` - + Similar artist connections

Higher depths use stricter degree thresholds for long-tail discovery.