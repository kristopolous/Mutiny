#!/usr/bin/env python3
"""
Bandcamp URL resolver worker.

Consumes the `bc:resolve:queue` Redis list at a fixed, polite rate
(default: one search every 20 seconds), resolves (artist, title) pairs to
Bandcamp album URLs using Bandcamp's own search page (rendered with
lightpanda, which solves Bandcamp's client challenge), and caches results
in the `bc:reverse` hash used by the pipeline.

Usage:
    resolver.py               # run forever (daemon)
    resolver.py --once        # process one item and exit
    resolver.py --interval 20 # seconds between searches (default 20)

Everything is cached: re-running the pipeline for the same releases never
re-queries Bandcamp.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from difflib import SequenceMatcher
from html import unescape
from urllib.parse import quote_plus, urlsplit, urlunsplit

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cache import get_redis_client

LIGHTPANDA = os.getenv("LIGHTPANDA", os.path.expanduser("~/.local/bin/lightpanda"))
COOKIE_JAR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bc-cookies.json")
QUEUE_KEY = "bc:resolve:queue"
PENDING_KEY = "bc:resolve:pending"
REVERSE_TTL = 7 * 86400          # positive results: week
NEG_TTL = 3600                   # "not found on Bandcamp": 1 hour
LIGHTPANDA_TIMEOUT = 60          # seconds per page load


def _norm(s):
    s = s or ""
    s = "".join(c for c in s if c not in "\u200b\u200c\u200d\ufeff")
    return " ".join(unescape(s).split()).lower()


def enqueue(artist, title, year=None, discogs_id=None):
    """Add a resolution job to the queue (deduplicated). Returns True if queued."""
    r = get_redis_client()
    key = f"{_norm(artist)}|{_norm(title)}"
    if r.exists(f"bc:reverse:{key}"):
        return False
    if not r.sadd(PENDING_KEY, key):
        return False  # already queued
    job = {"artist": artist, "title": title, "year": year,
           "discogs_id": discogs_id, "ts": int(time.time())}
    r.rpush(QUEUE_KEY, json.dumps(job))
    return True


def wait_for(artist, title, timeout=0, poll=2.0):
    """Poll the reverse cache for a queued job. timeout=0 = single check.
    Returns URL, or None (not resolved yet, or cached as not-found)."""
    key = f"bc:reverse:{_norm(artist)}|{_norm(title)}"
    r = get_redis_client()

    def _check():
        val = r.get(key)
        if val is None:
            return None, False
        s = val if isinstance(val, str) else val.decode()
        return (None if s == "__none__" else s), True

    val, hit = _check()
    if hit:
        return val
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(poll)
        val, hit = _check()
        if hit:
            return val
    return None


def _lightpanda_fetch(url):
    """Render a URL with lightpanda (solves the Bandcamp client challenge)."""
    os.makedirs(os.path.dirname(COOKIE_JAR), exist_ok=True)
    cmd = [LIGHTPANDA, "fetch", url,
           "--dump", "html",
           "--wait-until", "networkalmostidle",
           "--wait-ms", "20000", "--terminate-ms", "30000",
           "--cookie", COOKIE_JAR, "--cookie-jar", COOKIE_JAR]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=LIGHTPANDA_TIMEOUT)
    except subprocess.TimeoutExpired:
        return None
    if p.returncode != 0:
        return None
    return p.stdout


def _parse_search_results(html_text):
    """Yield {type, url, title, artist, year} for each search result card."""
    results = []
    for li in re.split(r'<li class="searchresult', html_text)[1:]:
        li = li[:li.find("</li>")] if "</li>" in li else li
        m_url = re.search(r'class="artcont"\s+href="([^"]+)"', li)
        if not m_url:
            continue
        url = unescape(m_url.group(1))
        # strip bandcamp's search-tracking query params
        parts = urlsplit(url)
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

        def _div(cls):
            m = re.search(rf'<div class="{cls}">.*?</div>', li, re.S)
            if not m:
                return ""
            text = re.sub(r"<[^>]+>", "", m.group(0))
            return unescape(text).strip()

        itemtype = _div("itemtype").upper()
        title = _div("heading") or re.sub(r"<[^>]+>", "", li).strip()
        subhead = _div("subhead")
        released = _div("released")
        artist = subhead.lstrip("by ").strip() if subhead.startswith("by ") else subhead

        year = None
        ym = re.search(r"\b(19|20)\d{2}\b", released)
        if ym:
            year = int(ym.group(0))

        results.append({
            "type": itemtype,
            "url": url,
            "title": title,
            "artist": artist,
            "year": year,
        })
    return results


def _best_match(results, artist, title, year):
    """Pick the best album card for (artist, title). None if nothing decent."""
    title_n = _norm(title)
    best, best_score = None, 0.0
    for res in results:
        if res["type"] != "ALBUM":
            continue
        sim = SequenceMatcher(None, _norm(res["title"]), title_n).ratio()
        score = sim
        # artist match in the card's subhead
        if artist and _norm(artist) in _norm(res["artist"]):
            score += 0.2
        elif res["artist"]:
            score -= 0.3
        # year corroboration
        if year and res["year"] and abs(res["year"] - int(year)) <= 1:
            score += 0.1
        if score > best_score:
            best, best_score = res, score
    if best and best_score >= 0.75:
        return best
    return None


def resolve_job(job, verbose=True):
    """Resolve one job: Bandcamp search -> best album match -> cache."""
    artist, title = job.get("artist", ""), job.get("title", "")
    year, discogs_id = job.get("year"), job.get("discogs_id")
    key = f"bc:reverse:{_norm(artist)}|{_norm(title)}"
    r = get_redis_client()

    q = f"{artist} {title}".strip()
    url = f"https://bandcamp.com/search?q={quote_plus(q)}"
    if verbose:
        print(f"[resolver] searching bandcamp: {q!r}")

    html_text = _lightpanda_fetch(url)
    if html_text is None:
        print("[resolver] lightpanda fetch failed", file=sys.stderr)
        return None
    if "Client Challenge" in html_text[:2000]:
        print("[resolver] still on challenge page", file=sys.stderr)
        return None

    match = _best_match(_parse_search_results(html_text), artist, title, year)
    if match:
        r.set(key, match["url"], ex=REVERSE_TTL)
        if verbose:
            print(f"[resolver] found: {match['url']} "
                  f"(card: {match['title']!r} by {match['artist']!r} {match['year']})")
        return match["url"]

    r.set(key, "__none__", ex=NEG_TTL)
    if verbose:
        print(f"[resolver] no album match for {q!r}")
    return None


def worker(interval, once=False, verbose=True):
    r = get_redis_client()
    # pidfile so clients (suggest.py) can tell the worker is alive
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    pidfile = os.path.join(data_dir, "resolver.pid")
    if not once:
        with open(pidfile, "w") as f:
            f.write(str(os.getpid()))
    if verbose:
        print(f"[resolver] worker started (interval={interval}s, "
              f"queue={QUEUE_KEY}, lightpanda={LIGHTPANDA})")
    try:
        _worker_loop(r, interval, once, verbose)
    finally:
        if not once:
            try:
                os.remove(pidfile)
            except OSError:
                pass


def _worker_loop(r, interval, once, verbose):
    while True:
        item = r.lpop(QUEUE_KEY)
        if item is None:
            if once:
                break
            time.sleep(2)
            continue
        try:
            job = json.loads(item)
        except Exception:
            continue
        try:
            resolve_job(job, verbose=verbose)
        except Exception as e:
            print(f"[resolver] job failed: {e}", file=sys.stderr)
        finally:
            r.srem(PENDING_KEY, f"{_norm(job.get('artist', ''))}|{_norm(job.get('title', ''))}")
        if once:
            break
        time.sleep(interval)


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    p = argparse.ArgumentParser(description="Bandcamp URL resolver worker")
    p.add_argument("--once", action="store_true", help="process one job and exit")
    p.add_argument("--interval", type=float,
                   default=float(os.getenv("RESOLVER_INTERVAL", "20")),
                   help="seconds between searches (default 20)")
    p.add_argument("-q", "--quiet", action="store_true")
    args = p.parse_args()
    worker(args.interval, once=args.once, verbose=not args.quiet)
