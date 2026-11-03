#!/usr/bin/env python3
"""
browse: generate a self-contained HTML listening page for a suggestion run.

Runs the normal suggestion pipeline for a seed, then builds an HTML page with
Bandcamp's embedded players so you can actually listen to seed + suggestions
side by side and judge whether the similarity holds.

    ./browse.py <seed-url> [more-seed-urls...]
    ./browse.py -n 6 --all <seed-url>
    ./browse.py -o /tmp/page.html <seed-url>

Output: .data/browse/<slug>.html  (or --output)
"""
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime
from html import escape
from urllib.parse import urlsplit, urlunsplit

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SUGGEST = os.path.join(HERE, 'suggest.py')
ART_CACHE = os.path.join(ROOT, '.data', 'bc-art-ids.json')

# ---------------------------------------------------------------- fetch / ids

def _norm_url(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', '')).rstrip('/')


def get_info(url, cache):
    """{'id': player item_id, 'art': cover url} for a Bandcamp album URL.

    Bandcamp's own embeds use the *item_id* (per edition), in path-style:
    https://bandcamp.com/EmbeddedPlayer/v=2/album=<item_id>/size=large/...
    (query-string ?album=... does NOT resolve the album, and neither does the
    art_id.) Cover art comes from og:image, suffix swapped for a big square.
    Cached to .data/bc-art-ids.json.
    """
    key = _norm_url(url)
    if key in cache and isinstance(cache[key], dict):
        return cache[key]
    from bc import _fetch_page
    html = _fetch_page(url)
    info = {'id': None, 'art': None}
    if html:
        m = re.search(r'EmbeddedPlayer/[^"\'>\s]*?album=(\d+)', html)   # their own embed
        if not m:
            m = re.search(r'&quot;item_id&quot;:(\d+)', html)             # label grid json
        if not m:
            m = re.search(r'"item_id",\s*"value":\s*(\d+)', html)      # JSON-LD
        if not m:
            m = re.search(r'"item_id":(\d+)', html)
        if m:
            info['id'] = m.group(1)
        im = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]*)"', html)
        if im:
            info['art'] = re.sub(r'_\d+\.(jpg|png)$', r'_10.\1', im.group(1))
        time.sleep(1.0)  # be polite; plain subdomain album pages (no challenge)
    cache[key] = info
    return info


def player_src(pid):
    # size=large renders the artwork with play controls underneath (mutiny-
    # player style); no separate cover image needed.
    return ('https://bandcamp.com/EmbeddedPlayer/v=2'
            f'/album={pid}/size=large/bgcol=141414/linkcol=4fc3f7'
            '/tracklist=true/transparent=true/')


def player_iframe(pid):
    if not pid:
        return '<div class="noplayer">player unavailable</div>'
    return (f'<iframe class="player" src="{player_src(pid)}" frameborder="0" '
            f'allowfullscreen '
            f'allow="autoplay; encrypted-media; fullscreen; picture-in-picture">'
            f'</iframe>')


def save_cache(cache):
    os.makedirs(os.path.dirname(ART_CACHE), exist_ok=True)
    with open(ART_CACHE, 'w') as fh:
        json.dump(cache, fh)


def load_cache():
    try:
        with open(ART_CACHE) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


# ---------------------------------------------------------------- pipeline

def run_suggest(urls, n, depth, wait_seconds, show_all):
    cmd = [sys.executable, SUGGEST, '--json', '-n', str(n), '-d', str(depth),
           '--wait-seconds', str(wait_seconds)]
    if show_all:
        cmd.append('--all')
    cmd += urls
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        print('suggest.py failed:\n' + proc.stderr, file=sys.stderr)
        sys.exit(1)
    return json.loads(proc.stdout)


def seed_block(title, artists, year, labels, cover, url):
    rows = []
    if artists:
        rows.append(f'<span class="a">{escape(", ".join(artists))}</span>')
    if year:
        rows.append(f'<span class="y">{escape(str(year))}</span>')
    if labels:
        rows.append(f'<span class="l">{escape(", ".join(dict.fromkeys(labels)))}</span>')
    player = player_iframe(cover.get('id'))
    return f'''
  <section class="seed">
    <h1>SEED</h1>
    <div class="card" style="max-width:440px">
      <div class="meta">{''.join(rows)}</div>
      <h2>{escape(title)}</h2>
      {player}
      <a class="open" href="{escape(url)}" target="_blank">open on bandcamp &nearr;</a>
    </div>
  </section>'''


def result_card(i, rel, cover):
    title = rel.get('title') or '?'
    artists = rel.get('artists') or []
    year = rel.get('year')
    labels = rel.get('labels') or []
    url = rel.get('bandcamp_url')
    meta = [f'<span class="a">{escape(", ".join(artists))}</span>' if artists else '',
            f'<span class="y">{escape(str(year))}</span>' if year else '',
            f'<span class="l">{escape(", ".join(dict.fromkeys(labels)))}</span>' if labels else '']
    if url:
        link = f'<a class="open" href="{escape(url)}" target="_blank">open on bandcamp &nearr;</a>'
    else:
        link = '<span class="discogs-only">discogs only (no bandcamp)</span>'
    player = player_iframe(cover.get('id'))
    return f'''
    <div class="card">
      <div class="meta"><span class="num">{i}</span>{''.join(meta)}</div>
      <h2>{escape(title)}</h2>
      {player}
      {link}
    </div>'''


def label_stats(data, releases, seed):
    """Label frequency stats across the suggestions.

    Graph mode: labels come from Discogs metadata.
    Bandcamp-native: no Discogs metadata, so use the storefront subdomain
    (the Bandcamp store the album lives in) as the label proxy.
    Returns (title, [(name, count)], n_distinct, n_total).
    """
    from collections import Counter
    counts = Counter()
    mode = data.get('mode', 'discogs-graph')
    for rel in releases:
        if mode == 'bandcamp-native':
            url = rel.get('bandcamp_url') or ''
            host = urlsplit(url).netloc
            sub = host.split('.')[0] if host else 'unknown'
            if sub != 'bandcamp':
                counts[sub] += 1
        else:
            for lab in dict.fromkeys(rel.get('labels') or []):
                counts[lab] += 1
    if not counts:
        return None
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    title = ('STOREFRONTS (bandcamp-native mode has no Discogs labels -'
             ' counting the store each album lives in)'
             if mode == 'bandcamp-native' else 'LABELS ACROSS SUGGESTIONS')
    return title, ordered, len(counts), len(releases)


def labels_section(title, ordered, n_distinct, n_total):
    max_count = ordered[0][1]
    rows = []
    for name, count in ordered:
        pct = max(4, round(count / max_count * 100))
        rows.append(f'''      <div class="lrow">
        <span class="lname">{escape(name)}</span>
        <span class="lbar"><span style="width:{pct}%"></span></span>
        <span class="lcount">{count}</span>
      </div>''')
    return f'''  <section class="stats">
    <h1>{escape(title)} &mdash; {n_distinct} distinct / {n_total} suggestions</h1>
    <div class="lchart">
{chr(10).join(rows)}
    </div>
  </section>'''


def build_html(seed, data, cards, elapsed, n_total, n_with, releases):
    seed_title = seed.get('title') or '?'
    seed_artists = seed.get('artists') or []
    seed_year = seed.get('year')
    seed_labels = seed.get('labels') or []
    mode = data.get('mode', 'discogs-graph')
    mode_label = 'bandcamp-native (seed not in Discogs)' if mode == 'bandcamp-native' \
        else 'discogs graph traversal'
    seed_cover = seed.get('_cover') or {}
    stats = label_stats(data, releases, seed)
    stats_html = labels_section(*stats) if stats else ''
    return f'''<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(seed_title)} — suggestions</title>
<style>
  :root {{ color-scheme: dark; }}
  body {{ margin:0; background:#141414; color:#e8e8e8;
         font:15px/1.5 system-ui,-apple-system,sans-serif; padding:24px 16px 60px; }}
  .wrap {{ max-width:1080px; margin:0 auto; }}
  h1 {{ font-size:12px; letter-spacing:.25em; color:#4fc3f7; margin:0 0 12px; }}
  h2 {{ font-size:17px; margin:0 0 2px; }}
  .seed {{ margin-bottom:36px; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
           gap:18px; }}
  .card {{ background:#1c1c1c; border:1px solid #2a2a2a; border-radius:10px;
           padding:14px; }}
  .meta {{ display:flex; flex-wrap:wrap; gap:6px 12px; align-items:baseline;
           margin-bottom:8px; font-size:13px; color:#aaa; }}
  .meta .a {{ color:#e8e8e8; font-weight:600; }}
  .meta .y {{ color:#888; }}
  .meta .l {{ color:#777; font-style:italic; }}
  .num {{ color:#4fc3f7; font-weight:700; }}
  .noplayer {{ height:400px; display:flex; align-items:center; justify-content:center;
               color:#555; font-size:13px; }}
  .open {{ display:inline-block; margin-top:10px; color:#4fc3f7; text-decoration:none;
           font-size:13px; }}
  .open:hover {{ text-decoration:underline; }}
  .discogs-only {{ margin-top:10px; color:#666; font-size:13px; font-style:italic; }}
  .player {{ border-width:0; width:100%; height:420px; display:block;
             margin:10px 0 4px; border-radius:6px; }}
  .stats {{ margin-bottom:36px; max-width:640px; }}
  .statrow {{ display:grid; grid-template-columns:minmax(140px,240px) 1fr 70px;
             gap:10px; align-items:center; margin:6px 0; font-size:13px; }}
  .statlabel {{ color:#ccc; text-align:right; overflow:hidden; text-overflow:ellipsis;
               white-space:nowrap; }}
  .seedtag {{ color:#4fc3f7; font-size:11px; font-style:italic; margin-left:6px; }}
  .bar {{ background:#262626; height:16px; border-radius:4px; overflow:hidden; }}
  .fill {{ height:100%; background:#555; border-radius:4px; }}
  .fill.seed {{ background:#4fc3f7; }}
  .statcount {{ color:#e8e8e8; font-variant-numeric:tabular-nums; }}
  .statcount .pct {{ color:#777; font-size:11px; }}
  .footer {{ margin-top:40px; color:#555; font-size:12px; }}
  .stats {{ margin:36px 0; }}
  .lchart {{ background:#1c1c1c; border:1px solid #2a2a2a; border-radius:10px;
             padding:16px; max-width:720px; }}
  .lrow {{ display:flex; align-items:center; gap:10px; margin:7px 0; }}
  .lname {{ width:240px; flex-shrink:0; font-size:13px; color:#ccc;
            white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .lbar {{ flex:1; height:14px; background:#262626; border-radius:3px; overflow:hidden; }}
  .lbar > span {{ display:block; height:100%; background:#4fc3f7; border-radius:3px; }}
  .lcount {{ width:24px; text-align:right; font-size:13px; color:#4fc3f7;
             font-weight:600; }}
</style></head>
<body><div class="wrap">
{seed_block(seed_title, seed_artists, seed_year, seed_labels, seed_cover,
            seed.get('_url') or '')}
{stats_html}
  <section>
    <h1>SUGGESTIONS &mdash; {escape(mode_label)} &mdash; {n_with}/{n_total} with player</h1>
    <div class="grid">
{cards}
    </div>
  </section>
  <div class="footer">generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
   &middot; music-explorer suggestion pipeline &middot; structural similarity, zero popularity signal</div>
</div></body></html>'''


# ---------------------------------------------------------------- main

def main():
    import argparse
    p = argparse.ArgumentParser(description='HTML listening page for a suggestion run')
    p.add_argument('urls', nargs='+', help='seed Bandcamp URL(s)')
    p.add_argument('-n', '--top-n', type=int, default=6)
    p.add_argument('-d', '--depth', type=int, default=2)
    p.add_argument('--wait-seconds', type=float, default=180)
    p.add_argument('--all', action='store_true', dest='show_all')
    p.add_argument('-o', '--output')
    args = p.parse_args()

    t0 = time.time()
    print(f'[browse] running suggestions for {args.urls} ...', file=sys.stderr)
    data = run_suggest(args.urls, args.top_n, args.depth, args.wait_seconds,
                       args.show_all)
    releases = data.get('releases', [])
    print(f'[browse] {len(releases)} suggestions in {time.time() - t0:.0f}s',
          file=sys.stderr)
    if not releases:
        print('no suggestions', file=sys.stderr)
        sys.exit(1)

    cache = load_cache()

    def seed_meta(url):
        from bc import bandcamp_meta
        try:
            m = bandcamp_meta(url)
            return {'title': m.get('title'), 'artists': m.get('artists', []),
                    'year': m.get('year'), 'labels': [], 'art_id': None, '_url': url}
        except Exception:
            return {'title': url, 'artists': [], 'year': None, 'labels': [],
                    'art_id': None, '_url': url}

    seed = seed_meta(args.urls[0])
    seed['_cover'] = get_info(args.urls[0], cache)

    # Enrich the seed with its Discogs label so the label stats can flag
    # "same label as seed".
    source_ids = data.get('source_ids', [])
    if source_ids and not seed['labels']:
        try:
            import local_db
            rel = local_db.fetch_release(int(source_ids[0]))
            if rel:
                seed['labels'] = [l['name'] for l in rel.get('labels', [])
                                  if l.get('name') and l.get('name') != 'Not On Label']
        except Exception:
            pass

    card_html, n_with = [], 0
    for i, rel in enumerate(releases, 1):
        url = rel.get('bandcamp_url')
        cover = get_info(url, cache) if url else {'id': None, 'art': None}
        if cover['id']:
            n_with += 1
        print(f'[browse]   {i:>2}. {rel.get("title")}  player_id={cover["id"]}',
              file=sys.stderr)
        card_html.append(result_card(i, rel, cover))
    save_cache(cache)

    html = build_html(seed, data, '\n'.join(card_html), time.time() - t0,
                      len(releases), n_with, releases)
    slug = re.sub(r'[^a-z0-9]+', '-', (seed.get('title') or 'page').lower()).strip('-')
    out = args.output or os.path.join(ROOT, '.data', 'browse', f'{slug}.html')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as fh:
        fh.write(html)
    print(out)


if __name__ == '__main__':
    main()
