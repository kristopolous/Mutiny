#!/usr/bin/env python3
"""
browse_server: the whole live experience in one process.

Serves .data/browse/ (GET / -> live.html) and accepts JSON POSTs:

    POST /rate        {"url": ..., "value": "P"|"3"|"4"|"5"|""}
    POST /collection  {"url": ..., "add": true|false}
    GET  /status      {"gen": N, "queue": N}   (page polling)

Every mutation updates .data/{ratings,purged,collection}.json and re-runs
the suggestion crawl (browse.py --live), so the page you see always reflects
the current query. One crawl at a time (lock); the client shows a spinner.

Bandcamp URL discovery (the resolver queue, one polite search per ~20s)
runs IN-PROCESS as a worker thread: discovering URLs is part of the
pipeline, not a daemon to babysit. When the queue drains and new links
arrived, the crawl re-runs in the background and the page offers an
"update" button - never a forced reload (playback is sacred).

    ./browse_server.py [port]        (default 8080)
"""
import json
import os
import subprocess
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, HERE := os.path.dirname(os.path.abspath(__file__)))
import resolver  # noqa: E402  (Bandcamp discovery = part of the pipeline)

ROOT = os.path.dirname(HERE)
BROWSE = os.path.join(HERE, 'browse.py')
DIR = os.path.join(ROOT, '.data', 'browse')
RATINGS = os.path.join(ROOT, '.data', 'ratings.json')
PURGED = os.path.join(ROOT, '.data', 'purged.json')

crawl_lock = threading.Lock()
GEN = 0  # page generation: bumps on every regen; the page polls /status


def _load(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def _save(path, obj):
    with open(path, 'w') as fh:
        json.dump(obj, fh)


def update_rate(url, value):
    """One state per release: P -> purged, 3/4/5 -> weight, '' -> clear.
    Returns True if the crawl query actually changed (worth re-running):
    - P (or clearing a P) changes the global veto list for any release
    - 3/4/5 only matter for collection members (seed weights)"""
    if not url:
        return False
    ratings, purged = _load(RATINGS), _load(PURGED)
    was_purged = bool(purged.pop(url, None))
    was_rated = ratings.pop(url, None)
    if value == 'P':
        purged[url] = '1'
    elif value in ('3', '4', '5'):
        ratings[url] = value
    _save(RATINGS, ratings)
    _save(PURGED, purged)
    in_collection = url in _collection_urls()
    query_changed = (value == 'P' or was_purged
                     or (in_collection and (value or was_rated)))
    return query_changed


def _collection_urls():
    from browse import _norm_url
    col = _load(os.path.join(ROOT, '.data', 'collection.json'))
    return {_norm_url(k) for k in col}


def regen():
    """Re-run the crawl for the whole collection -> live.html."""
    global GEN
    subprocess.run([sys.executable, BROWSE, '--live'], cwd=ROOT,
                   capture_output=True, text=True, timeout=3600)
    GEN += 1


def regen_async():
    def _run():
        with crawl_lock:
            regen()
    threading.Thread(target=_run, daemon=True).start()


def resolver_loop():
    """In-process Bandcamp URL discovery: drain bc:resolve:queue at one
    polite search per interval. When the queue empties and we resolved
    something, re-crawl in the background so the page fills itself in."""
    r = resolver.get_redis_client()
    interval = float(os.getenv('RESOLVER_INTERVAL', '20'))
    pidfile = os.path.join(HERE, 'data', 'resolver.pid')
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    with open(pidfile, 'w') as f:
        f.write(str(os.getpid()))  # suggest.py's worker-alive check
    print(f'[resolver] in-process worker (interval={interval}s)')
    resolved_since_crawl = 0
    while True:
        item = r.lpop(resolver.QUEUE_KEY)
        if item is None:
            if resolved_since_crawl and not r.llen(resolver.QUEUE_KEY):
                resolved_since_crawl = 0
                regen_async()  # new links landed: page fills itself in
            time.sleep(2)
            continue
        try:
            job = json.loads(item)
        except Exception:
            continue
        try:
            if resolver.resolve_job(job):
                resolved_since_crawl += 1
        except Exception as e:
            print(f'[resolver] job failed: {e}', file=sys.stderr)
        finally:
            r.srem(resolver.PENDING_KEY,
                   f"{resolver._norm(job.get('artist', ''))}|"
                   f"{resolver._norm(job.get('title', ''))}")
        time.sleep(interval)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=DIR, **k)

    def handle_one_request(self):
        # Browsers cancel in-flight requests constantly (reload, iframe
        # teardown); the resulting BrokenPipe tracebacks are noise.
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def end_headers(self):
        # live.html is regenerated on every query change; never let a
        # browser serve a stale page (that's how "9 per page" looked like 8).
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def do_GET(self):
        if self.path == '/status':
            q = 0
            try:
                q = resolver.get_redis_client().llen(resolver.QUEUE_KEY)
            except Exception:
                pass
            self._json({'gen': GEN, 'queue': q})
        elif self.path in ('/', '/index.html', '/live.html'):
            if not os.path.exists(os.path.join(DIR, 'live.html')):
                with crawl_lock:
                    regen()
            self.path = '/live.html'
        elif self.path.endswith('.html'):
            # pre-paging one-off pages (multiverse.html etc.): the live
            # page is the product now - send old bookmarks there.
            self.send_response(302)
            self.send_header('Location', '/')
            self.end_headers()
            return
        super().do_GET()

    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        try:
            data = json.loads(self.rfile.read(n) or b'{}')
        except ValueError:
            data = {}
        if self.path == '/rate':
            changed = update_rate(data.get('url'), data.get('value'))
            if changed:
                with crawl_lock:
                    regen()
            self._json({'ok': True, 'rerun': changed})
        elif self.path == '/collection':
            url = data.get('url')
            flag = '--add' if data.get('add') else '--drop'
            subprocess.run([sys.executable, BROWSE, flag, url, '--live'],
                           cwd=ROOT, capture_output=True, text=True,
                           timeout=3600)
            self._json({'ok': True})
        else:
            self.send_error(404)

    def _json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *a):
        sys.stderr.write('%s %s\n' % (self.address_string(), fmt % a))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    os.makedirs(DIR, exist_ok=True)
    threading.Thread(target=resolver_loop, daemon=True).start()
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'browse_server on :{port} -> {DIR} (GET / = live.html, '
          f'resolver in-process)')
    srv.serve_forever()


if __name__ == '__main__':
    main()
