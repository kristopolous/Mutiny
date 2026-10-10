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

# ---------------------------------------------------------------- fetch / art_id

def _norm_url(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', '')).rstrip('/')


def get_art_id(url, cache):
    """Numeric Bandcamp art_id for an album URL (embedded player requires it)."""
    key = _norm_url(url)
    if key in cache:
        return cache[key]
    from bc import _fetch_page
    html = _fetch_page(url)
    art_id = None
    if html:
        m = re.search(r'"art_id",\s*"value":\s*(\d+)', html)      # JSON-LD
        if not m:
            m = re.search(r'&quot;art_id&quot;:\s*(\d+)', html)   # escaped grid json
        if not m:
            m = re.search(r'"art_id":\s*(\d+)', html)
        if m:
            art_id = m.group(1)
        time.sleep(1.0)  # be polite; these are plain album pages (no challenge)
    cache[key] = art_id
    return art_id


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


# ---------------------------------------------------------------- html

def player_iframe(art_id):
    src = ('https://bandcamp.com/EmbeddedPlayer'
           f'?album={art_id}&size=large&track=0'
           '&bgcol=141414&linkcol=4fc3f7&artwork=small'
           '&tracklist=false&bandname=false&trackname=false&transparent=true')
    return (f'<iframe style="border-width:0" src="{src}" '
            f'width="100%" height="400" seamless frameborder="0" '
            f'allowfullscreen allow="autoplay; encrypted-media; fullscreen; gamepad; '
            f'picture-in-picture; clipboard-write"></iframe>')


def seed_block(title, artists, year, labels, art_id, url):
    rows = []
    if artists:
        rows.append(f'<span class="a">{escape(", ".join(artists))}</span>')
    if year:
        rows.append(f'<span class="y">{escape(str(year))}</span>')
    if labels:
        rows.append(f'<span class="l">{escape(", ".join(dict.fromkeys(labels)))}</span>')
    player = player_iframe(art_id) if art_id else \
        f'<div class="noplayer">player unavailable</div>'
    return f'''
  <section class="seed">
    <h1>SEED</h1>
    <div class="card" style="max-width:440px">
      <div class="meta">{''.join(rows)}</div>
      {player}
      <a class="open" href="{escape(url)}" target="_blank">open on bandcamp &nearr;</a>
    </div>
  </section>'''


def result_card(i, rel, art_id):
    title = rel.get('title') or '?'
    artists = rel.get('artists') or []
    year = rel.get('year')
    labels = rel.get('labels') or []
    url = rel.get('bandcamp_url')
    meta = [f'<span class="a">{escape(", ".join(artists))}</span>' if artists else '',
            f'<span class="y">{escape(str(year))}</span>' if year else '',
            f'<span class="l">{escape(", ".join(dict.fromkeys(labels)))}</span>' if labels else '']
    if rel.get('bandcamp_url') is None:
        link = '<span class="discogs-only">discogs only (no bandcamp)</span>'
    else:
        link = f'<a class="open" href="{escape(url)}" target="_blank">open on bandcamp &nearr;</a>'
    player = player_iframe(art_id) if art_id else \
        f'<div class="noplayer">player unavailable</div>'
    return f'''
    <div class="card">
      <div class="meta"><span class="num">{i}</span>{''.join(meta)}</div>
      <h2>{escape(title)}</h2>
      {player}
      {link}
    </div>'''


def build_html(seed, data, cards, elapsed, n_total, n_with):
    seed_title = seed.get('title') or '?'
    seed_artists = seed.get('artists') or []
    seed_year = seed.get('year')
    seed_labels = seed.get('labels') or []
    mode = data.get('mode', 'discogs-graph')
    mode_label = 'bandcamp-native (seed not in Discogs)' if mode == 'bandcamp-native' \
        else 'discogs graph traversal'
    seed_player_id = seed.get('_art_id')
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
  .footer {{ margin-top:40px; color:#555; font-size:12px; }}
</style></head>
<body><div class="wrap">
{seed_block(seed_title, seed_artists, seed_year, seed_labels, seed_player_id,
            seed.get('_url') or '')}
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
    seed['_art_id'] = get_art_id(args.urls[0], cache)

    card_html, n_with = [], 0
    for i, rel in enumerate(releases, 1):
        url = rel.get('bandcamp_url')
        art_id = None
        if url:
            art_id = get_art_id(url, cache)
            if art_id:
                n_with += 1
        print(f'[browse]   {i:>2}. {rel.get("title")}  art_id={art_id}', file=sys.stderr)
        card_html.append(result_card(i, rel, art_id))
    save_cache(cache)

    html = build_html(seed, data, '\n'.join(card_html), time.time() - t0,
                      len(releases), n_with)
    slug = re.sub(r'[^a-z0-9]+', '-', (seed.get('title') or 'page').lower()).strip('-')
    out = args.output or os.path.join(ROOT, '.data', 'browse', f'{slug}.html')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as fh:
        fh.write(html)
    print(out)


if __name__ == '__main__':
    main()
