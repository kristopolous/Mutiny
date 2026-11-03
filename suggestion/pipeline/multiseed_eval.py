#!/usr/bin/env python3
"""
multiseed_eval: is multi-seed weighting reasonable?

Runs the graph pipeline (traverse + IDF weight + dedupe, no Bandcamp
resolution - ranking only) for single seeds and seed pairs, then compares:

  - how the pair's top-N splits between items from seed A's list, seed B's
    list, BOTH (consensus), or neither
  - where consensus items rank vs single-seed items
  - how much the IDF re- pooling reshuffles the singles' top lists

    .venv/bin/python pipeline/multiseed_eval.py [-d 2] [-n 20]

Writes .data/multiseed/eval.json + prints a report.
"""
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

from traverse_and_weight import (traverse, compute_idf_weights, dedupe_releases,  # noqa: E402
                                 fuse_reciprocal_rank)
import local_db  # noqa: E402

OUT_DIR = os.path.join(ROOT, '.data', 'multiseed')

# Discogs release IDs (correlated via bc2dg cache)
SEEDS = {
    'multiverse': '24262334',          # detund (IDM, self-released)
    'litmus': '28852630',              # detund (same artist)
    'lost-in-black': '37621272',       # COLD UNION / Oraculo Records
    'hagen-redux': '36074581',         # Oraculo Records (different artist)
}

PAIRS = [
    ('multiverse', 'litmus'),          # same artist
    ('lost-in-black', 'hagen-redux'),  # same label, different artist
    ('multiverse', 'lost-in-black'),   # distant: IDM vs EBM
]


def run_set(names, depth):
    ids = [SEEDS[n] for n in names]
    t0 = time.time()
    releases = traverse(ids, depth=depth)
    weighted = compute_idf_weights(releases, ids)
    weighted = dedupe_releases(weighted, ids)
    print(f'  -> {len(weighted)} ranked releases in {time.time() - t0:.0f}s',
          file=sys.stderr)
    return weighted


def brief(rid):
    r = local_db.fetch_release(int(rid))
    if not r:
        return (rid, '?', '')
    artists = ', '.join(a['name'] for a in r.get('artists', [])[:2])
    labels = ', '.join(dict.fromkeys(l['name'] for l in r.get('labels', [])
                                     if l.get('name') != 'Not On Label'))
    return (r.get('title', '?'), artists, f"{r.get('year') or '?'} | {labels}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('-d', '--depth', type=int, default=2)
    p.add_argument('-n', '--top', type=int, default=20)
    args = p.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    lists = {}
    for name in SEEDS:
        print(f'[eval] single: {name}', file=sys.stderr)
        lists[name] = run_set([name], args.depth)
    for a, b in PAIRS:
        key = f'{a}+{b}'
        print(f'[eval] pair: {key}', file=sys.stderr)
        lists[key] = run_set([a, b], args.depth)

    slim = {k: [{'rid': str(r['release_id']), 'w': r['final_weight'],
                 'depth': r.get('depth')} for r in v[:100]]
            for k, v in lists.items()}
    with open(os.path.join(OUT_DIR, 'eval.json'), 'w') as fh:
        json.dump(slim, fh, indent=1)

    # ---- report ----
    N = args.top
    print(f'\n=== MULTI-SEED EVAL (top {N}, depth {args.depth}) ===')
    for a, b in PAIRS:
        key = f'{a}+{b}'
        ra = {r['rid']: i + 1 for i, r in enumerate(slim[a])}
        rb = {r['rid']: i + 1 for i, r in enumerate(slim[b])}
        blend = slim[key][:N]
        both = a_only = b_only = neither = 0
        print(f'\n--- {key} ---')
        print(f'{"#":>3}  {"A":>4} {"B":>4}  title / artist / year+label')
        for i, r in enumerate(blend, 1):
            in_a, in_b = r['rid'] in ra, r['rid'] in rb
            if in_a and in_b:
                both += 1
                tag = 'BOTH'
            elif in_a:
                a_only += 1
                tag = 'A'
            elif in_b:
                b_only += 1
                tag = 'B'
            else:
                neither += 1
                tag = '--'
            t, ar, yl = brief(r['rid'])
            print(f'{i:>3}  {ra.get(r["rid"], "-"):>4} {rb.get(r["rid"], "-"):>4}'
                  f'  [{tag:>4}] {t[:40]:<40} {ar[:24]:<24} {yl[:34]}')
        print(f'consensus(in both singles top100): {both}/{N}'
              f'  A-only: {a_only}  B-only: {b_only}  neither: {neither}')
        # overlap of the two singles with each other (baseline)
        ov = len(set(r['rid'] for r in slim[a][:N]) & set(r['rid'] for r in slim[b][:N]))
        print(f'baseline: singles top{N} overlap with each other: {ov}')

        # --- rank-fused (RRF) merge of the two single lists ---
        fused = fuse_reciprocal_rank({a: lists[a], b: lists[b]})
        fl = [{'rid': str(r['release_id']), 'seeds': r.get('seeds', [])}
              for r in fused[:N]]
        fboth = sum(1 for r in fl if r['rid'] in ra and r['rid'] in rb)
        fa = sum(1 for r in fl if r['rid'] in ra and r['rid'] not in rb)
        fb = sum(1 for r in fl if r['rid'] in rb and r['rid'] not in ra)
        fn = N - fboth - fa - fb
        n_a = sum(1 for r in fl if len(r['seeds']) == 1 and r['seeds'][0] == a)
        n_b = sum(1 for r in fl if len(r['seeds']) == 1 and r['seeds'][0] == b)
        print(f'RRF fused: consensus {fboth}/{N}  A-only {fa}  B-only {fb}  '
              f'neither {fn}  |  pole balance A:{n_a} vs B:{n_b}')
        print(f'     top 10 fused: ' + ' / '.join(
            ('AB' if len(r['seeds']) > 1 else
             ('A' if r['seeds'] and r['seeds'][0] == a else 'B'))
            + ':' + brief(r['rid'])[0][:22] for r in fl[:10]))


if __name__ == '__main__':
    main()
