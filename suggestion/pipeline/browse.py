#!/usr/bin/env python3
"""
browse: interactive HTML listening page for a suggestion run.

Layout (mutiny-style): COLLECTION on top (every seed ever run + anything you
add from the suggestions), SUGGESTIONS below. Every release carries the
mutiny weight scale: P = purge (veto - excluded from all future suggestions),
3 / 4 / 5 = weight. One state per release. State lives in browser
localStorage, is seeded from .data/{ratings,purged}.json at generation time,
and exports with one click; feed the export back with --import.

    ./browse.py <seed-url> [more-seed-urls...]
    ./browse.py -n 6 --all <seed-url>
    ./browse.py --import bc-ratings.json <seed-url>
    ./browse.py -o /tmp/page.html <seed-url>

Output: .data/browse/<slug>.html  (or --output)
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from html import escape
from urllib.parse import urlsplit, urlunsplit

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SUGGEST = os.path.join(HERE, 'suggest.py')
ART_CACHE = os.path.join(ROOT, '.data', 'bc-art-ids.json')
COLLECTION = os.path.join(ROOT, '.data', 'collection.json')
RATINGS = os.path.join(ROOT, '.data', 'ratings.json')
PURGED = os.path.join(ROOT, '.data', 'purged.json')

# P = purge (veto), 3/4/5 = weight
RATE_VALUES = ('P', '3', '4', '5')


def load_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as fh:
        json.dump(obj, fh)


# ---------------------------------------------------------------- fetch / ids

def _norm_url(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', '')).rstrip('/')


def get_info(url, cache):
    """{'id': player item_id, 'art': cover url, 'tracks': n} for a BC album URL.

    Bandcamp's own embeds use the *item_id* (per edition), in path-style:
    https://bandcamp.com/EmbeddedPlayer/v=2/album=<item_id>/size=large/...
    (query-string ?album=... does NOT resolve the album, and neither does the
    art_id.) Cover art comes from og:image, suffix swapped for a big square.
    Cached to .data/bc-art-ids.json.
    """
    key = _norm_url(url)
    if key in cache and isinstance(cache[key], dict) and 'tracks' in cache[key]:
        return cache[key]
    from bc import _fetch_page
    html = _fetch_page(url)
    info = {'id': None, 'art': None, 'tracks': None}
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
        tm = re.search(r'data-tralbum="([^"]*)"', html)
        if tm:
            try:
                import html as _html
                tdata = json.loads(_html.unescape(tm.group(1)))
                info['tracks'] = len(tdata.get('trackinfo') or [])
            except ValueError:
                pass
        time.sleep(1.0)  # be polite; plain subdomain album pages (no challenge)
    cache[key] = info
    return info


def player_src(pid):
    # size=large renders the artwork with play controls underneath.
    return ('https://bandcamp.com/EmbeddedPlayer/v=2'
            f'/album={pid}/size=large/bgcol=141414/linkcol=4fc3f7'
            '/tracklist=true/transparent=true/')


def player_iframe(pid, tracks=None):
    if not pid:
        return '<div class="noplayer">player unavailable</div>'
    return (f'<iframe class="player" data-tracks="{tracks if tracks else 6}" '
            f'src="{player_src(pid)}" frameborder="0" allowfullscreen '
            f'allow="autoplay; encrypted-media; fullscreen; picture-in-picture">'
            f'</iframe>')


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


def seed_meta(url):
    from bc import bandcamp_meta
    try:
        m = bandcamp_meta(url)
        return {'title': m.get('title') or url, 'artists': m.get('artists', []),
                'year': m.get('year')}
    except Exception:
        return {'title': url, 'artists': [], 'year': None}


# ---------------------------------------------------------------- widgets

def rate_widget(url):
    btns = ''.join(f'<button data-v="{v}">{v}</button>' for v in RATE_VALUES)
    return f'<span class="rate" data-rate="{escape(url)}">{btns}</span>'


def collection_section(collection, ratings_n, purged_n):
    cards = []
    for url, e in collection.items():
        meta = [e.get('artists', [])[:2], str(e.get('year') or '')]
        meta = ' · '.join(x for x in (', '.join(map(str, meta[0])), meta[1]) if x)
        art = e.get('art')
        cover = (f'<img class="cover" src="{escape(art)}">' if art
                 else '<div class="nocover">?</div>')
        st = 'P' if purged_n.get(url) else ratings_n.get(url)
        badge = f'<span class="wtag {st}">{st}</span>' if st else ''
        drop = (f'<button class="dropbtn" data-dropurl="{escape(url)}" '
                f'title="remove from collection">&#8722;</button>')
        cards.append(f'''      <div class="ccard" data-cardurl="{escape(url)}">
        <div class="cactions">{rate_widget(url)}{drop}</div>
        <a href="{escape(url)}" target="_blank">{cover}</a>
        <div class="ctitle">{escape(e.get("title") or url)}{badge}</div>
        <div class="cmeta">{escape(meta)}</div>
      </div>''')
    return f'''  <section class="collection">
    <h1>COLLECTION &mdash; {len(collection)} search parameters</h1>
    <div class="cgrid">
{chr(10).join(cards)}
    </div>
  </section>'''


def result_card(i, rel, cover, collection_urls):
    title = rel.get('title') or '?'
    artists = rel.get('artists') or []
    year = rel.get('year')
    labels = rel.get('labels') or []
    url = rel.get('bandcamp_url')
    meta = [f'<span class="a">{escape(", ".join(artists))}</span>' if artists else '',
            f'<span class="y">{escape(str(year))}</span>' if year else '',
            f'<span class="l">{escape(", ".join(dict.fromkeys(labels)))}</span>' if labels else '']
    if url:
        rate = rate_widget(url)
        tag = ('<span class="intag">IN COLLECTION</span>'
               if _norm_url(url) in collection_urls else
               f'<button class="addcol" data-addurl="{escape(url)}">+ collection</button>')
        note = ''
    else:
        rate, tag = '', ''
        note = '<span class="discogs-only">discogs only (no bandcamp)</span>'
    return f'''
    <div class="card" data-cardurl="{escape(url or '')}">
      <div class="meta"><span class="num">{i}</span>{''.join(meta)}{tag}{rate}{note}</div>
      <h2>{escape(title)}</h2>
      {player_iframe((cover or {}).get('id'), (cover or {}).get('tracks'))}
    </div>'''


# ---------------------------------------------------------------- html

CSS = '''
  :root { color-scheme: dark; }
  body { margin:0; background:#141414; color:#e8e8e8;
         font:15px/1.5 system-ui,-apple-system,sans-serif; padding:24px 16px 60px; }
  .wrap { max-width:1080px; margin:0 auto; }
  h1 { font-size:12px; letter-spacing:.25em; color:#4fc3f7; margin:0 0 12px; }
  h2 { font-size:17px; margin:0 0 2px; }
  .collection { margin-bottom:36px; }
  .cgrid { display:grid; grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
           gap:14px; }
  .ccard { background:#1c1c1c; border:1px solid #2a2a2a; border-radius:10px;
           padding:10px; font-size:13px; }
  .ccard .cover, .nocover { width:100%; aspect-ratio:1/1; object-fit:cover;
           border-radius:6px; display:block; }
  .nocover { display:flex; align-items:center; justify-content:center; color:#555; }
  .ctitle { font-weight:600; margin-top:6px; }
  .wtag { font-size:10px; font-weight:700; border-radius:4px; padding:1px 6px;
          margin-left:6px; background:#2c5b78; color:#4fc3f7; }
  .wtag.P { background:#c0392b; color:#fff; }
  .dropbtn { margin-left:auto; background:none; border:1px solid #3a2a2a;
             color:#a77; border-radius:5px; width:24px; height:24px;
             font-size:14px; cursor:pointer; font-family:inherit; }
  .dropbtn:hover { background:#3a2222; color:#fff; }
  .cmeta { color:#999; font-size:12px; margin-bottom:6px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
          gap:18px; }
  .card { background:#1c1c1c; border:1px solid #2a2a2a; border-radius:10px;
          padding:14px; }
  .meta { display:flex; flex-wrap:wrap; gap:6px 12px; align-items:center;
          margin-bottom:8px; font-size:13px; color:#aaa; }
  .meta .addcol { margin-left:auto; }
  .meta .intag { margin-left:auto; }
  .meta .rate { margin-left:10px; }
  .meta .a { color:#e8e8e8; font-weight:600; }
  .meta .y { color:#888; }
  .meta .l { color:#777; font-style:italic; }
  .num { color:#4fc3f7; font-weight:700; }
  .noplayer { height:120px; display:flex; align-items:center; justify-content:center;
              color:#555; font-size:13px; }
  .player { border-width:0; width:100%; height:420px; display:block;
            margin:10px 0 4px; border-radius:6px; }
  .discogs-only { color:#666; font-size:13px; font-style:italic; margin-left:auto; }
  .rate { display:inline-flex; gap:3px; }
  .rate button { background:#262626; color:#bbb; border:1px solid #333;
                 border-radius:5px; width:26px; height:24px; font-size:12px;
                 cursor:pointer; font-family:inherit; }
  .rate button:hover { background:#333; color:#fff; }
  .rate button.on { background:#4fc3f7; color:#10222e; border-color:#4fc3f7;
                    font-weight:700; }
  .cactions { display:flex; align-items:center; gap:8px; margin-bottom:8px; }
  .rate button[data-v="P"].on { background:#c0392b; border-color:#c0392b;
                                color:#fff; }
  .card.ispurged, .ccard.ispurged { opacity:.35; }
  .addcol { background:none; border:1px solid #333; color:#8ab; border-radius:5px;
            font-size:12px; padding:2px 8px; cursor:pointer; font-family:inherit; }
  .addcol.incol { border-color:#4fc3f7; color:#4fc3f7; }
  .intag { font-size:10px; letter-spacing:.15em; color:#4fc3f7;
           border:1px solid #2c5b78; border-radius:5px; padding:3px 7px; }
  .toolbar { display:flex; gap:14px; align-items:center; margin-bottom:22px;
             font-size:13px; color:#999; }
  .toolbar button { background:#262626; color:#ddd; border:1px solid #3a3a3a;
                    border-radius:6px; padding:5px 12px; cursor:pointer;
                    font-family:inherit; font-size:13px; }
  body.hiderated .card.israted, body.hiderated .card.ispurged { display:none; }
  body.busy .grid, body.busy .cgrid { opacity:.35; pointer-events:none; }
  #status { color:#4fc3f7; }
  .stats { margin:36px 0; }
  .lchart { background:#1c1c1c; border:1px solid #2a2a2a; border-radius:10px;
            padding:16px; max-width:720px; }
  .lrow { display:flex; align-items:center; gap:10px; margin:7px 0; }
  .lname { width:240px; flex-shrink:0; font-size:13px; color:#ccc;
           white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .lbar { flex:1; height:14px; background:#262626; border-radius:3px;
          overflow:hidden; }
  .lbar > span { display:block; height:100%; background:#4fc3f7; border-radius:3px; }
  .lcount { width:24px; text-align:right; font-size:13px; color:#4fc3f7;
            font-weight:600; }
  .pager { display:flex; gap:16px; justify-content:center; align-items:center;
           margin-top:22px; color:#999; font-size:13px; }
  .pager button { background:#262626; color:#ddd; border:1px solid #3a3a3a;
                  border-radius:6px; padding:6px 14px; cursor:pointer;
                  font-family:inherit; font-size:13px; }
  .pager button:hover { background:#333; }
  .footer { margin-top:40px; color:#555; font-size:12px; }
'''

JS = '''
const EMBEDDED = %RATINGS%;
const EMBEDDED_PURGED = %PURGED%;
const LSR = 'bc-ratings-v2', LSC = 'bc-col-adds-v2', LSP = 'bc-purged-v2';
let ratings = Object.assign({}, EMBEDDED);
let purged = Object.assign({}, EMBEDDED_PURGED);
try { Object.assign(ratings, JSON.parse(localStorage.getItem(LSR) || '{}')); } catch (e) {}
try { Object.assign(purged, JSON.parse(localStorage.getItem(LSP) || '{}')); } catch (e) {}
let adds = [];
try { adds = JSON.parse(localStorage.getItem(LSC) || '[]'); } catch (e) {}

function lsSet(k, v) {
  try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {}
}
// One state per release: P (purge, veto) | 3 | 4 | 5 (weight)
function state(u) {
  if (purged[u]) return 'P';
  return ratings[u];
}
function setState(u, v) {
  const cur = state(u);
  const next = cur === v ? '' : v;
  applyState(u, next);
  paint();
  if (LIVE) rerun('/rate', {url: u, value: next});
  else { lsSet(LSR, ratings); lsSet(LSP, purged); }
}
function applyState(u, v) {
  delete ratings[u]; delete purged[u];
  if (v === 'P') purged[u] = '1';
  else if (v) ratings[u] = v;
}
function paint() {
  document.querySelectorAll('.rate').forEach(g => {
    const s = state(g.dataset.rate);
    g.querySelectorAll('button').forEach(b =>
      b.classList.toggle('on', b.dataset.v === s));
  });
  document.querySelectorAll('[data-cardurl]').forEach(c => {
    const s = state(c.dataset.cardurl);
    c.classList.toggle('ispurged', s === 'P');
    c.classList.toggle('israted', s === '3' || s === '4' || s === '5');
  });
  document.querySelectorAll('.addcol').forEach(b =>
    b.classList.toggle('incol', adds.includes(b.dataset.addurl)));
}
function setRating(url, v) {
  setState(url, v);
}
// On the live server the collection IS the crawl query: adding or removing
// a member re-runs the search. Over file:// it degrades to local state +
// export/import.
const LIVE = /^https?:$/.test(location.protocol);
let busy = false;
async function api(path, body) {
  try {
    const r = await fetch(path, {method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body || {})});
    return r.ok;
  } catch (e) { return false; }
}
async function rerun(path, body) {
  if (busy) return;
  busy = true;
  document.body.classList.add('busy');
  document.getElementById('status').textContent = 're-running crawl...';
  await api(path, body);
  location.reload();
}
function toggleCol(url, add) {
  if (LIVE) { rerun('/collection', {url: url, add: add}); return; }
  const i = adds.indexOf(url);
  if (!add && i >= 0) adds.splice(i, 1);
  else if (add && i < 0) adds.push(url);
  lsSet(LSC, adds);
  paint();
}
document.addEventListener('click', e => {
  const b = e.target.closest('.rate button');
  if (b) { setRating(b.closest('.rate').dataset.rate, b.dataset.v); return; }
  const a = e.target.closest('.addcol');
  if (a) { toggleCol(a.dataset.addurl, true); return; }
  const d = e.target.closest('.dropbtn');
  if (d) toggleCol(d.dataset.dropurl, false);
});
function exportAll() {
  const out = {ratings: ratings, purged: purged, collection: adds};
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([JSON.stringify(out, null, 1)],
                                        {type: 'application/json'}));
  a.download = 'bc-ratings.json';
  a.click();
}
document.getElementById('exportbtn').onclick = exportAll;
document.getElementById('hiderated').onchange = e =>
  document.body.classList.toggle('hiderated', e.target.checked);
paint();

// Suggestions are paged client-side: the crawl already ranked them all.
const PAGE = %PAGE%;
let page = 0;
const scards = Array.from(document.querySelectorAll('.grid > .card'));
function showPage() {
  const pages = Math.max(1, Math.ceil(scards.length / PAGE));
  if (page >= pages) page = pages - 1;
  if (page < 0) page = 0;
  scards.forEach((c, i) => {
    c.hidden = (i < page * PAGE || i >= (page + 1) * PAGE);
  });
  document.querySelectorAll('.pgnum').forEach(e =>
    e.textContent = (page + 1) + ' / ' + pages);
  document.querySelectorAll('iframe.player').forEach(fitPlayer);
}
document.getElementById('pprev').onclick = () => {
  if (page) { page--; showPage(); pageTop.scrollIntoView({block:'start'}); }
};
document.getElementById('pnext').onclick = () => {
  if (page < Math.ceil(scards.length / PAGE) - 1) {
    page++; showPage(); pageTop.scrollIntoView({block:'start'});
  }
};
const pageTop = document.querySelector('.grid');
showPage();

// Bandcamp large embed = square artwork (width x width) + ~170px chrome
// + 33px per track row. Size each iframe to fit its own tracklist.
function fitPlayer(f) {
  if (!f.clientWidth) return;  // hidden card: measure when shown
  var n = parseInt(f.dataset.tracks, 10) || 6;
  f.style.height = Math.round(f.clientWidth + 170 + 33 * n) + 'px';
}
var players = document.querySelectorAll('iframe.player');
players.forEach(fitPlayer);
window.addEventListener('resize', function () { players.forEach(fitPlayer); });
'''


def label_stats(data, releases):
    """Label frequency across suggestions. Bandcamp-native mode has no
    Discogs labels, so use the storefront subdomain as the proxy."""
    from collections import Counter
    counts = Counter()
    mode = data.get('mode', 'discogs-graph')
    for rel in releases:
        if mode == 'bandcamp-native':
            host = urlsplit(rel.get('bandcamp_url') or '').netloc
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


def build_html(data, cards_html, collection_html, ratings, purged,
               elapsed, n_total, n_with, releases, page_size=8):
    mode = data.get('mode', 'discogs-graph')
    mode_label = {'bandcamp-native': 'bandcamp-native (seed not in Discogs)',
                  'discogs-graph-rrf': 'discogs graph, rank-fused multi-seed'
                  }.get(mode, 'discogs graph traversal')
    stats = label_stats(data, releases)
    stats_html = labels_section(*stats) if stats else ''
    js = (JS.replace('%RATINGS%', json.dumps(ratings))
            .replace('%PURGED%', json.dumps(purged))
            .replace('%PAGE%', str(max(1, page_size))))
    return f'''<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>mutiny &mdash; suggestions</title>
<style>{CSS}</style></head>
<body><div class="wrap">
  <div class="toolbar">
    <span>{escape(mode_label)} &middot; {n_with}/{n_total} with player
      &middot; {elapsed:.0f}s</span>
    <span id="status"></span>
    <label><input type="checkbox" id="hiderated"> hide marked</label>
    <button id="exportbtn">export ratings</button>
  </div>
{collection_html}
{stats_html}
  <section>
    <h1>SUGGESTIONS</h1>
    <div class="grid">
{cards_html}
    </div>
    <div class="pager">
      <button id="pprev">&larr; prev</button>
      <span class="pgnum"></span>
      <button id="pnext">next &rarr;</button>
    </div>
  </section>
  <div class="footer">generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
   &middot; music-explorer suggestion pipeline &middot; structural similarity,
   zero popularity signal</div>
</div>
<script>{js}</script>
</body></html>'''


# ---------------------------------------------------------------- main

def main():
    import argparse
    p = argparse.ArgumentParser(description='HTML listening page for a suggestion run')
    p.add_argument('urls', nargs='*', help='seed Bandcamp URL(s)')
    p.add_argument('-n', '--top-n', type=int, default=24)
    p.add_argument('--page-size', type=int, default=9,
                   help='suggestions per page (client-side paging; 3 per row)')
    p.add_argument('-d', '--depth', type=int, default=2)
    p.add_argument('--wait-seconds', type=float, default=180)
    p.add_argument('--all', action='store_true', dest='show_all')
    p.add_argument('-o', '--output')
    p.add_argument('--import', dest='import_file',
                   help='merge an exported bc-ratings.json into .data and exit '
                        'or combine with seed urls to regenerate')
    p.add_argument('--add', action='append', default=[],
                   help='add a release URL to the collection (search parameter)')
    p.add_argument('--drop', action='append', default=[],
                   help='remove a release URL from the collection')
    p.add_argument('--live', action='store_true',
                   help='query = the whole collection (default when no urls '
                        'are given); writes .data/browse/live.html')
    args = p.parse_args()

    cache = load_json(ART_CACHE, {})
    collection = load_json(COLLECTION, {})
    ratings = load_json(RATINGS, {})
    purged = load_json(PURGED, {})

    def add_to_collection(url):
        key = _norm_url(url)
        if key in collection:
            return
        meta = seed_meta(url)
        info = get_info(url, cache)
        collection[key] = {'title': meta['title'], 'artists': meta['artists'],
                           'year': meta['year'], 'art': info.get('art'),
                           'added': datetime.now().strftime('%Y-%m-%d')}

    if args.import_file:
        exported = load_json(args.import_file, {})
        rats = exported.get('ratings', exported) if isinstance(exported, dict) else {}
        merged = {k: v for k, v in rats.items() if v}
        ratings.update(merged)
        pur = exported.get('purged', {}) if isinstance(exported, dict) else {}
        pur_merged = {k: v for k, v in pur.items() if v}
        purged.update(pur_merged)
        for u in exported.get('collection', []) if isinstance(exported, dict) else []:
            add_to_collection(u)
        save_json(RATINGS, ratings)
        save_json(PURGED, purged)
        save_json(COLLECTION, collection)
        print(f'imported {len(merged)} ratings, {len(pur_merged)} purged '
              f'-> {RATINGS}', file=sys.stderr)
        if not args.urls:
            return

    for u in args.add:
        add_to_collection(u)
    for u in args.drop:
        collection.pop(_norm_url(u), None)
    if args.add or args.drop:
        save_json(COLLECTION, collection)

    urls = list(args.urls)
    if args.live or not urls:
        urls = list(collection)  # the collection IS the crawl query
    if not urls:
        p.error('no seed urls and empty collection (give urls or --add)')

    t0 = time.time()
    print(f'[browse] running suggestions for {len(urls)} search parameter(s) ...',
          file=sys.stderr)
    data = run_suggest(urls, args.top_n, args.depth, args.wait_seconds,
                       args.show_all)
    releases = data.get('releases', [])
    print(f'[browse] {len(releases)} suggestions in {time.time() - t0:.0f}s',
          file=sys.stderr)
    if not releases:
        print('no suggestions', file=sys.stderr)
        sys.exit(1)

    for u in urls:
        add_to_collection(u)
    save_json(COLLECTION, collection)
    save_json(RATINGS, ratings)
    save_json(PURGED, purged)
    collection_urls = set(collection)
    ratings_n = {_norm_url(k): v for k, v in ratings.items() if v}
    purged_n = {_norm_url(k): v for k, v in purged.items() if v}

    card_html, n_with = [], 0
    for i, rel in enumerate(releases, 1):
        url = rel.get('bandcamp_url')
        cover = get_info(url, cache) if url else {'id': None, 'art': None}
        if cover.get('id'):
            n_with += 1
        print(f'[browse]   {i:>2}. {rel.get("title")}  player_id={cover.get("id")}',
              file=sys.stderr)
        card_html.append(result_card(i, rel, cover, collection_urls))
    save_json(ART_CACHE, cache)

    html = build_html(data, '\n'.join(card_html),
                      collection_section(collection, ratings_n, purged_n),
                      ratings, purged,
                      time.time() - t0, len(releases), n_with, releases,
                      args.page_size)
    if args.live:
        slug = 'live'
    else:
        slug = re.sub(r'[^a-z0-9]+', '-',
                      (collection.get(_norm_url(urls[0]), {}).get('title')
                       or 'page').lower()).strip('-')
    out = args.output or os.path.join(ROOT, '.data', 'browse', f'{slug}.html')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as fh:
        fh.write(html)
    print(out)


if __name__ == '__main__':
    main()
