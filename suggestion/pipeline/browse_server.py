#!/usr/bin/env python3
"""
browse_server: live listening page where the collection IS the crawl query.

Serves .data/browse/ (GET / -> live.html) and accepts JSON POSTs:

    POST /rate        {"url": ..., "value": "P"|"3"|"4"|"5"|""}
    POST /collection  {"url": ..., "add": true|false}

Every mutation updates .data/{ratings,purged,collection}.json and re-runs
the suggestion crawl (browse.py --live), so the page you see always reflects
the current query. One crawl at a time (lock); the client shows a spinner.

    ./browse_server.py [port]        (default 8080)
"""
import json
import os
import subprocess
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BROWSE = os.path.join(HERE, 'browse.py')
DIR = os.path.join(ROOT, '.data', 'browse')
RATINGS = os.path.join(ROOT, '.data', 'ratings.json')
PURGED = os.path.join(ROOT, '.data', 'purged.json')

crawl_lock = threading.Lock()


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
    subprocess.run([sys.executable, BROWSE, '--live'], cwd=ROOT,
                   capture_output=True, text=True, timeout=3600)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=DIR, **k)

    def do_GET(self):
        if self.path in ('/', '/index.html'):
            if not os.path.exists(os.path.join(DIR, 'live.html')):
                with crawl_lock:
                    regen()
            self.path = '/live.html'
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
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    print(f'browse_server on :{port} -> {DIR} (GET / = live.html)')
    srv.serve_forever()


if __name__ == '__main__':
    main()
