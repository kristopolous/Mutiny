#!/usr/bin/env python3
"""
Automated end-to-end test suite for the suggestion pipeline.

Runs each seed URL through suggest.py (JSON mode) and verifies the whole
process works: Bandcamp->Discogs correlation, graph traversal, Bandcamp URL
resolution via the resolver queue, and the Bandcamp-native fallback for
seeds not in Discogs.

    ./test_suite.py                    # full battery from test_seeds.txt
    ./test_suite.py --smoke            # first 2 seeds, cache-only, fast
    ./test_suite.py -f my_seeds.txt -n 5 --wait-seconds 120

A seed PASSES when:
  - suggest.py exits 0
  - it emits valid JSON with a non-empty release list
  - EVERY release has a Bandcamp URL (the core output contract)

Writes a report to .data/test-suite/run-<timestamp>/ (report.md + results.json).
Exit code: 0 if all seeds pass, 1 otherwise.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SUGGEST = os.path.join(HERE, 'suggest.py')
DEFAULT_SEEDS = os.path.join(HERE, 'test_seeds.txt')
RESULTS_DIR = os.path.join(ROOT, '.data', 'test-suite')


def load_seeds(path, limit=0):
    seeds = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith('#'):
                seeds.append(line)
    return seeds[:limit] if limit else seeds


def resolver_pid():
    pidfile = os.path.join(HERE, 'data', 'resolver.pid')
    try:
        with open(pidfile) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return pid
    except (OSError, ValueError):
        return None


def ensure_resolver(start=True):
    """Check the resolver worker is alive; start it if not and start=True."""
    pid = resolver_pid()
    if pid:
        return pid
    if not start:
        return None
    log = open('/tmp/resolver.log', 'a')
    subprocess.Popen(
        [sys.executable, os.path.join(HERE, 'resolver.py')],
        stdout=log, stderr=log, cwd=ROOT,
        start_new_session=True,
    )
    for _ in range(30):
        time.sleep(1)
        pid = resolver_pid()
        if pid:
            return pid
    print("[suite] WARNING: resolver worker did not come up", file=sys.stderr)
    return None


def run_seed(url, n, depth, wait_seconds, timeout):
    """Run suggest.py for one seed; return a result dict."""
    t0 = time.time()
    cmd = [sys.executable, SUGGEST, '--json', '-n', str(n), '-d', str(depth),
           '--wait-seconds', str(wait_seconds), url]
    rec = {'seed': url, 'elapsed': 0, 'pass': False, 'mode': None,
           'results': 0, 'with_bc': 0, 'errors': [], 'releases': []}
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True,
                              text=True, timeout=timeout)
        rec['elapsed'] = round(time.time() - t0, 1)
        if proc.returncode != 0:
            tail = (proc.stderr.strip().splitlines() or ['<no stderr>'])[-3:]
            rec['errors'].append(f"exit {proc.returncode}: {tail}")
            return rec
        data = json.loads(proc.stdout)
        rec['mode'] = data.get('mode', 'discogs-graph')
        rec['source_ids'] = data.get('source_ids', data.get('sources'))
        releases = data.get('releases', [])
        rec['results'] = len(releases)
        rec['with_bc'] = sum(1 for r in releases if r.get('bandcamp_url'))
        rec['releases'] = [{
            'title': r.get('title'),
            'artists': r.get('artists'),
            'year': r.get('year'),
            'bandcamp_url': r.get('bandcamp_url'),
        } for r in releases]
        if not releases:
            rec['errors'].append('no releases returned')
        missing = [r.get('title') for r in releases if not r.get('bandcamp_url')]
        if missing:
            rec['errors'].append(f"missing bandcamp_url: {missing}")
        rec['pass'] = bool(releases) and not missing and not rec['errors']
        return rec
    except subprocess.TimeoutExpired:
        rec['elapsed'] = round(time.time() - t0, 1)
        rec['errors'].append(f'timeout after {timeout}s')
        return rec
    except json.JSONDecodeError as e:
        rec['elapsed'] = round(time.time() - t0, 1)
        rec['errors'].append(f'invalid JSON: {e}')
        return rec
    except Exception as e:  # noqa: BLE001 - record and continue
        rec['elapsed'] = round(time.time() - t0, 1)
        rec['errors'].append(f'{type(e).__name__}: {e}')
        return rec


def write_report(run_dir, recs, meta):
    ok = sum(1 for r in recs if r['pass'])
    lines = [
        '# Suggestion pipeline test suite',
        '',
        f"- run: {meta['timestamp']}",
        f"- seeds: {meta['n_seeds']}  ({ok} passed, {len(recs) - ok} failed)",
        f"- n={meta['n']}  depth={meta['depth']}  wait_seconds={meta['wait_seconds']}",
        f"- total wall time: {meta['wall']:.0f}s",
        '',
        '| # | seed | mode | results | with BC URL | time | result |',
        '|---|------|------|---------|-------------|------|--------|',
    ]
    for i, r in enumerate(recs, 1):
        seed = r['seed'].replace('https://', '')
        lines.append(
            f"| {i} | {seed} | {r['mode']} | {r['results']} | {r['with_bc']} "
            f"| {r['elapsed']}s | {'PASS' if r['pass'] else 'FAIL'} |")
        for err in r['errors']:
            lines.append(f"| | ↳ {err} | | | | | |")
    lines += ['', '## Failures', '']
    failed = [r for r in recs if not r['pass']]
    if not failed:
        lines.append('none')
    for r in failed:
        lines.append(f"- **{r['seed']}**")
        for err in r['errors']:
            lines.append(f"  - {err}")
    lines += ['', '## Per-seed detail', '']
    for i, r in enumerate(recs, 1):
        lines.append(f"### {i}. {r['seed']}  —  {'PASS' if r['pass'] else 'FAIL'} "
                     f"({r['mode']}, {r['elapsed']}s)")
        for rel in r['releases']:
            art = ', '.join(rel['artists']) if rel['artists'] else '?'
            lines.append(f"- {rel['year'] or '?'} {rel['title']} — {art}  {rel['bandcamp_url'] or '(no BC url)'}")
        lines.append('')
    with open(os.path.join(run_dir, 'report.md'), 'w') as fh:
        fh.write('\n'.join(lines))
    with open(os.path.join(run_dir, 'results.json'), 'w') as fh:
        json.dump({'meta': meta, 'results': recs}, fh, indent=2)
    return '\n'.join(lines)


def main():
    import argparse
    p = argparse.ArgumentParser(description='End-to-end test suite for suggest.py')
    p.add_argument('-f', '--seeds-file', default=DEFAULT_SEEDS)
    p.add_argument('-n', '--top-n', type=int, default=5)
    p.add_argument('-d', '--depth', type=int, default=2)
    p.add_argument('--wait-seconds', type=float, default=180)
    p.add_argument('--timeout', type=float, default=420,
                   help='Hard per-seed wall-clock timeout (default 420)')
    p.add_argument('--smoke', action='store_true',
                   help='Quick: first 2 seeds, n=3, same wait budget')
    p.add_argument('--no-start-resolver', action='store_true')
    args = p.parse_args()

    if args.smoke:
        args.top_n = min(args.top_n, 3)
        # keep the wait budget: the point is verifying the queue path works

    seeds = load_seeds(args.seeds_file, limit=2 if args.smoke else 0)
    if not seeds:
        print('no seeds found', file=sys.stderr)
        sys.exit(1)

    rpid = ensure_resolver(start=not args.no_start_resolver)
    print(f"[suite] {len(seeds)} seeds | resolver pid={rpid} | "
          f"n={args.top_n} depth={args.depth} wait={args.wait_seconds}s",
          file=sys.stderr)

    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    run_dir = os.path.join(RESULTS_DIR, f'run-{ts}')
    os.makedirs(run_dir, exist_ok=True)

    recs = []
    t_start = time.time()
    for i, url in enumerate(seeds, 1):
        print(f"[suite] ({i}/{len(seeds)}) {url}", file=sys.stderr, flush=True)
        rec = run_seed(url, args.top_n, args.depth, args.wait_seconds,
                       args.timeout)
        recs.append(rec)
        status = 'PASS' if rec['pass'] else 'FAIL'
        detail = '; '.join(rec['errors']) if rec['errors'] else \
            f"{rec['with_bc']}/{rec['results']} with bandcamp url"
        print(f"[suite]   {status}  {rec['mode']}  {rec['elapsed']}s  {detail}",
              file=sys.stderr, flush=True)

    meta = {
        'timestamp': ts,
        'n_seeds': len(seeds),
        'n': args.top_n,
        'depth': args.depth,
        'wait_seconds': args.wait_seconds,
        'wall': time.time() - t_start,
    }
    report = write_report(run_dir, recs, meta)
    print(f"\n[suite] report: {os.path.join(run_dir, 'report.md')}\n")
    print(report)

    ok = sum(1 for r in recs if r['pass'])
    sys.exit(0 if ok == len(recs) else 1)


if __name__ == '__main__':
    main()
