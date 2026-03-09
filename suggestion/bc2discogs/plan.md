# Match Releases from HTML to Discogs

## Goal
Parse record label pages (page.html) to extract release metadata, search Discogs API for matching releases, and emit matched pairs with confidence scores.

## Input Files
- Page HTML: `example-scrapes/<domain>/<artist-slug>/page.html`

## Page Structure
The `<meta name="description">` tag contains structured data:
```
<release_name> by <artist_name>, released <date>

<track_number>. <track_name>
...
```

Example:
```
Best of Bal Pare "1982 - 2016" by Bal Pare, released 10 September 2021

1. Palais D'Amour
2. Eine Nacht im 9.Stocklwerk
...
```

Only parse pages that follow this pattern. Skip t-shirts, 404s, and non-album pages.

## Steps

### 1. Build Page Parser
Parse HTML to extract from meta description:
```python
{
    "artist_name": str,          # e.g., "Anum Preto"
    "release_name": str,         # e.g., "Best of Bal Pare"
    "track_list": [str, ...],    # e.g., ["Track 1", "Track 2"]
    "year": str (optional),      # extracted from date
    "raw_description": str
}
```

### 2. Search Discogs API
- Endpoint: `GET /database/search`
- Build weighted search query:
  - `artist={artist_name} release_title={release_name}`
  - Optional: `year={year}`
- Reference: `dicogs-api/search.md`

### 3. Score & Match Releases
- Compare parsed metadata against each Discogs result
- Score matches based on:
  - Artist name match (fuzzy, using SequenceMatcher)
  - Release title match (fuzzy)
  - Track list overlap (Jaccard-like)
  - Year match

### 4. Emit Results
Output matched pairs:
```python
{
    "source_file": str,          # path to page.html
    "parsed_data": {...},        # from step 1
    "discogs_release": {
        "id": int,
        "uri": str,
        "resource_url": str,
        "title": str,
        "artists": [str],
        "tracklist": [str],
        "labels": [str],
        "formats": [str],
        ...
    },
    "confidence": float,         # 0.0 - 1.0
    "match_reasons": [str]       # e.g. ["artist exact match", "3/4 tracks match"]
}
```

## Usage

```bash
# Set your Discogs user token
export DISCOGS_USER_TOKEN="your_token_here"

# Run correlate on a page.html
python correlate.py example-scrapes/youngandcoldrecords/gegenwelt/page.html

# Output as JSON
python correlate.py -j example-scrapes/youngandcoldrecords/gegenwelt/page.html > results.json
```

## Implementation
See `correlate.py` for the complete implementation.
