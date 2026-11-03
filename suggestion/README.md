# Music Explorer — Bandcamp-first release recommender

Bandcamp release URLs in → ranked music suggestions out. Discogs is the
graph backbone (who worked on what, on which label); Bandcamp is the
ground truth for what you can actually get. Every suggestion carries
artist / label / year and a clickable Bandcamp player.

## Core invariant

**Popularity is worth exactly 0.** Not a small weight, not a tiebreaker,
not a "signal" — zero. No recommendation may be influenced by how many
people own, bought, streamed, or fan-out a release. The only signals are
structural: shared people and labels, scored by the *rarity* of the
shared feature (IDF within the traversal — the opposite direction of
popularity).

Why it's enforced, not just preferred: popularity weights compound
multiplicatively across a depth-N walk ((1+ε)^N), so every neighborhood
converges on the same mega-label attractors. Structural affinity alone
keeps depth meaningful.

Consequences, enforced in code:
- Bandcamp "N fans also own" counts are parsed nowhere and never used
  (`bc_rec.py` keeps Bandcamp's own page order).
- No listener/owner/sales/stream/"trending" metric may enter ranking.
  Tiebreakers must be structural (year, depth), never popularity.
- Flood control is inverse weighting *within the traversal* (a feature
  seen 500× self-weights ~0.16 vs ~1.4 for a rare one) plus hard caps
  (`max_pages_per_label`, `max_releases_per_artist`) — never a global
  "is this a big label" lookup, which would be popularity in disguise.

## The model: your collection IS the crawl query

Not a saved list — the search parameters. State lives in `.data/`:

| file | role |
|---|---|
| `collection.json` | the parameters (Bandcamp URLs). Seeds the crawl. |
| `ratings.json` | `3`/`4`/`5` = that seed's vote multiplier (unrated = 1) |
| `purged.json` | `P` = veto: release blocked everywhere; if it's a collection member it also becomes a **negative** parameter (its neighborhood vote is subtracted) |

Rating scale is exactly **P 3 4 5** — one state per release. P is a
release-level veto, not neighborhood poison (hating one record says
nothing about its label).

Every click on the live page edits the query and re-runs the crawl:
rate a collection member, add/remove a parameter → re-crawl. Rate a
plain suggestion 3/4/5 → banked only (it's not a parameter yet); P
always re-crawls (global veto).

### Anti-feedback-loop

Serve 90% of type A, watch what gets rated, conclude "they love type A"
— the classic recommender spiral. Cut at three places:

1. **Ratings never auto-promote.** A 5 on a suggestion changes nothing
   until *you* add it to the collection.
2. **Exploration cap** (`--max-per-label`, default 3): at most N picks
   from any label/storefront, so the menu is always broad and a rating
   is a choice among alternatives. Structural (label identity), not
   popularity.
3. **Correlated parameters share a vote.** N parameters from the same
   label = one sample of that label: each gets weight/N. Three Oráculo
   records don't triple-vote for Oráculo-land.

## How a suggestion is made

```
collection (params, weighted P/3/4/5)
  └─ correlate to Discogs release IDs (bc.py + redis bc2dg)
       └─ BFS depth 1-2: shared people → their releases; shared labels → rosters
            └─ per-seed IDF: score = rarity of the shared connection
                 └─ fuse per-seed rankings: weighted reciprocal rank fusion
                      (consensus across params compounds; P params subtract)
                           └─ keep only releases with a Bandcamp home,
                             minus vetoes, minus params, cap 3/label
```

Multi-seed fusion is RRF (rank-based, scale-free) — pooled IDF score
summing is not comparable across neighborhoods and one seed used to win
everything. Seeds that aren't in Discogs fall back to Bandcamp-native
recommendations (`/api/mobile/4/recommendations`), but that path is a
fallback only; the graph is the product.

## Quickstart

Requirements: Python 3.12+, Redis (localhost:6379), lightpanda
(`~/.local/bin/lightpanda`, used to render Bandcamp search), and the
local Discogs DB (`.data/discogs.db`, ~19.5M releases, rebuildable from
the XML dump — see Data below).

```bash
python3 -m venv .venv && .venv/bin/pip install redis requests python-dotenv
.venv/bin/python pipeline/browse_server.py 8080   # everything: page + crawl + resolver
```

One process, one experience. `browse_server` serves the page, re-runs the
crawl when the query changes, and runs Bandcamp URL discovery (the
resolver queue, one polite search per ~20s via lightpanda) as an
in-process worker thread — discovering URLs is part of the pipeline, not
a daemon to babysit. When queued lookups land, the crawl re-runs in the
background and the page offers a "new results — update" button (never a
forced reload: playback is sacred). `pipeline/resolver.py` still runs
standalone (`--once`, `--interval`) if you want it separate.

Open `http://<host>:8080/` — that's `live.html`, generated from the
current collection. Paging is client-side (9/page, 3 per row); players
are Bandcamp's own embeds (the embed *is* the artwork window; tracklist
on, height fitted per album).

CLI:

```bash
# suggestions from URLs (graph path; queues Bandcamp lookups as needed)
.venv/bin/python pipeline/suggest.py -n 24 https://detund.bandcamp.com/album/multiverse ...

# options: --all (include Discogs-only finds), --wait-seconds N (default 180),
# --no-search (cache only), --max-per-label N (default 3, 0 = off),
# -d DEPTH, -v (shows seed weights / negative params / purge blocks)

# regenerate the live page from the collection
.venv/bin/python pipeline/browse.py --live
.venv/bin/python pipeline/browse.py URL...      # one-off page
.venv/bin/python pipeline/browse.py --import FILE   # merge exported ratings
```

## Bandcamp resolution (be polite)

A suggestion only appears by default if it resolves to a Bandcamp URL.
Resolution order: redis correlation cache → reverse cache
(`bc:reverse:<artist>|<title>`) → **queued to the resolver**, which runs
one Bandcamp search every ~20s (lightpanda renders the Client
Challenge) and caches hits (7d) / misses (1h). Never hammer search; the
queue is the design.

## Tests

```bash
.venv/bin/python pipeline/test_suite.py --smoke   # seeds in pipeline/test_seeds.txt
```

Asserts each seed produces suggestions with working Bandcamp URLs and
correct modes. `pipeline/multiseed_eval.py` is the harness that
compared pooled-sum vs RRF fusion.

## Data & state (relocatable)

Everything lives under `.data/` — move the directory, the project moves.

- `discogs.db` — SQLite backbone (`local_db.py` queries it; import with
  `local/discogs-xml-to-db-fast.py <releases.xml> --db .data/discogs.db`,
  dump via `local/download-discogs-dump.sh`, ~5h import)
- `collection.json` / `ratings.json` / `purged.json` — the query
- `browse/live.html` — served page; `bc-art-ids.json` — player ids/artwork
- redis: `bc2dg` (bandcamp→discogs), `bc:reverse:*`, `bc:resolve:queue|pending`
- `pipeline/data/` — resolver pid + Bandcamp cookie jar (gitignored)

## Files

**pipeline/**
- `suggest.py` — the query engine: correlate → traverse → IDF → RRF →
  Bandcamp filter → caps. Reads ratings/purged/collection.
- `browse.py` — HTML page builder (collection section = params with
  weight badges; suggestions paged; P/3/4/5 widgets)
- `browse_server.py` — serves the page; `POST /rate`, `POST /collection`
  edit the query and re-run the crawl when the query changed
- `resolver.py` — queued Bandcamp search worker (lightpanda)
- `traverse_and_weight.py` — BFS + IDF + RRF core
- `bc.py` / `bc_rec.py` — correlation + Bandcamp-native recommendations
- `test_suite.py`, `multiseed_eval.py` — tests/evals
- `local_db.py` (project root) — SQLite query layer

**Legacy** (Neo4j/Postgres era, kept for reference, not used by the
pipeline): `ingest.py`, `graph.py`, `aggregate.py`, `similar.py`,
`pipeline.py`, `local/*-pg*`, `correlate/`.

## Depth levels

- `1` — direct connections only (shared people)
- `2` — + label rosters and people-of-people (default)
- `3` — wider reach; stricter degree thresholds keep it long-tail
