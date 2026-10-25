#!/usr/bin/env python3
"""MovieHouse Library — the cinefreak.net index (2026-10-09).

cinefreak.net is a WordPress site: category pages list cards, a title page carries the details and, per quality
(a film) or per episode (a series), links to its file host (new5.cinecloud.site). A file page `/f/<fid>` of a
recent upload names the direct file on the host's Cloudflare R2 bucket (`pub-*.r2.dev/...mkv`: public, ranged,
no referer or token). Older uploads sit on Google Drive and carry no direct link.

This script keeps a local SQLite mirror of what the site lists, resolves every file id once, and pushes the
delta to the Library worker (`POST /ingest`). Nothing is fetched twice: a title page is read again only when its
card says it changed (time-ago) or it is a series still airing.

    python3 cinefreak.py crawl   [--full] [--cats a,b] [--max-pages N]    cards -> titles -> files, into the DB
    python3 cinefreak.py resolve [--limit N]                              /f/<fid> -> direct URL for unresolved files
    python3 cinefreak.py push    --base https://... --key ...             delta to the worker (rows whose hash changed)
    python3 cinefreak.py stats

Environment: LIB_DB (default ./lib-index.sqlite), LIB_BASE, LIB_KEY, LIB_CONCURRENCY (default 6).
"""
from __future__ import annotations

import argparse
import base64
import itertools
import hashlib
import json
import os
import re
import socket
import unicodedata
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape

from bs4 import BeautifulSoup

SITE = "https://cinefreak.net"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
DB_PATH = os.environ.get("LIB_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib-index.sqlite"))
CONC = int(os.environ.get("LIB_CONCURRENCY", "6"))
RCONC = int(os.environ.get("LIB_RESOLVE_CONCURRENCY", "8"))
QUIET = os.environ.get("LIB_QUIET") == "1"          # public logs: counts only, never names or links
DAILY_ROWS = int(os.environ.get("LIB_DAILY_ROWS", "90000"))   # D1 free plan: 100k rows written/day
# Rows D1 bills per row the worker reports written (title + files + the search table and its indexes). Measured
# 2026-10-11: the old 1.7 estimated 43.7k for a morning D1 billed ~99k for (00:00-03:14 UTC), so 4.0 keeps a day's
# pushes under the free 100k with room for the rest of the day.
ROW_FACTOR = float(os.environ.get("LIB_ROW_FACTOR", "4.0"))


def deadline_of(name: str) -> float:
    """Epoch seconds from env (LIB_<name>_BY), or never."""
    v = os.environ.get(f"LIB_{name}_BY")
    return float(v) if v else float("inf")


def say(msg: str, private: str = "") -> None:
    print(msg if QUIET or not private else f"{msg} {private}", flush=True)
LIB_BASE_NUM = 2_000_000_000
PARSER = 2   # bump when parse_title changes what it reads: every page is re-read once (see cmd_crawl)

# every category the site's menu lists (2026-10-08); the crawl also follows any /category/ link it meets
CATEGORIES = [
    "bangla-movies", "bangla-dubbed", "web-series", "tv-show", "k-drama", "english-movies", "hindi-movies",
    "hindi-dubbed-movies", "anime", "animation", "korean", "chinese", "japanese", "indonesian", "russian", "spanish",
    "tamil", "telugu", "kannada", "malayalam", "dual-audio", "studio-dub", "horror", "mcu", "reality-competition",
    "others", "adult",
]

# ---------------------------------------------------------------- http

# Egress lanes: the direct line plus any proxies named in LIB_PROXIES (comma-separated URLs). The site
# limits requests per IP, so each lane paces itself and the threads take lanes in turn.
_LANES = [None] + [u for u in os.environ.get("LIB_PROXIES", "").split(",") if u.strip()]
_OPENERS = [urllib.request.build_opener(urllib.request.ProxyHandler({"http": u, "https": u}) if u else urllib.request.ProxyHandler({})) for u in _LANES]
_PACE = [{"sleep": 0.0, "until": 0.0} for _ in _LANES]
_TURN = itertools.count()
_DEAD: set[int] = set()   # proxy lanes that refused us (e.g. the proxy denylists this runner's IP); never lane 0


def _pick() -> int:
    alive = [i for i in range(len(_LANES)) if i not in _DEAD] or [0]
    return alive[next(_TURN) % len(alive)]


def _proxy_refused(e: Exception) -> bool:
    """True when the proxy itself said no (tunnel/auth failure), not the site: the lane is useless for this run."""
    if getattr(e, "code", None) == 407:
        return True
    if isinstance(e, urllib.error.URLError) and not isinstance(e, urllib.error.HTTPError) \
            and isinstance(e.reason, (ConnectionRefusedError, socket.gaierror)):
        return True   # the proxy host itself is unreachable
    m = str(getattr(e, "reason", "") or e)
    return "Tunnel connection failed" in m or "ProxyError" in m or "Cannot connect to proxy" in m


def probe_lanes() -> None:
    """Try every proxy lane once before the run; a lane the proxy refuses is dropped so no request waits on it."""
    for i in range(1, len(_LANES)):
        try:
            req = urllib.request.Request(f"{SITE}/", headers={"User-Agent": UA})
            with _OPENERS[i].open(req, timeout=25) as r:
                r.read(512)
        except urllib.error.HTTPError as e:   # the site answered through the proxy (429 etc.): the lane works
            if e.code == 407:
                _DEAD.add(i)
        except Exception:  # noqa: BLE001
            _DEAD.add(i)
    print(f"[lanes] {len(_LANES) - len(_DEAD)} of {len(_LANES)} usable", flush=True)


def _slow_down(lane: int):
    """A 429 from the site: every request on that lane sleeps a while before going out (shared across threads)."""
    p = _PACE[lane]
    p["sleep"] = min(5.0, (p["sleep"] or 0.5) * 2)
    p["until"] = time.time() + 120


def fetch(url: str, timeout: int = 40, tries: int = 4) -> str:
    last = None
    lane = _pick()
    for i in range(tries):
        p = _PACE[lane]
        if p["sleep"] and time.time() < p["until"]:
            time.sleep(p["sleep"])
        elif p["sleep"]:
            p["sleep"] = 0.0
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,*/*", "Accept-Language": "en,bn;q=0.8"})
            with _OPENERS[lane].open(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as e:  # noqa: BLE001
            last = e
            code = getattr(e, "code", None)
            if code == 404:
                raise
            if lane and _proxy_refused(e):
                if lane not in _DEAD:
                    _DEAD.add(lane)
                    print(f"[lanes] a proxy lane refused us; {len(_LANES) - len(_DEAD)} of {len(_LANES)} left", flush=True)
                lane = _pick()
                continue
            if code == 429:
                _slow_down(lane)
                time.sleep(8 * (i + 1))
                lane = _pick()
                continue
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"fetch failed {url}: {last}")


# ---------------------------------------------------------------- db

SCHEMA = """
CREATE TABLE IF NOT EXISTS titles (
  slug TEXT PRIMARY KEY, post_id INTEGER, type TEXT, title TEXT, year INTEGER, poster TEXT, overview TEXT,
  genres TEXT, language TEXT, country TEXT, rating REAL, release_date TEXT, runtime INTEGER, tmdb_id INTEGER, imdb_id TEXT,
  status TEXT, cats TEXT, ott TEXT, badges TEXT, card_title TEXT, time_ago TEXT, published TEXT, modified TEXT,
  seen_at INTEGER, fetched_at INTEGER, hash TEXT, pushed_hash TEXT, err TEXT
);
CREATE TABLE IF NOT EXISTS episodes (
  slug TEXT, season INTEGER, ep INTEGER, name TEXT, aired TEXT, runtime INTEGER, lang TEXT,
  PRIMARY KEY (slug, season, ep)
);
CREATE TABLE IF NOT EXISTS files (
  fid TEXT PRIMARY KEY, slug TEXT, season INTEGER, ep_from INTEGER, ep_to INTEGER, quality INTEGER, hevc INTEGER,
  size_mb REAL, label TEXT, lang TEXT, watch INTEGER,
  host TEXT, url TEXT, sub_url TEXT, bytes INTEGER, resolved_at INTEGER, res_err TEXT
);
CREATE INDEX IF NOT EXISTS files_slug ON files(slug);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=60)
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    if "start_ms" not in [r[1] for r in c.execute("PRAGMA table_info(files)")]:
        # NULL = not checked yet; 0 = no spoken advert; >0 = where the film begins (see voice_start)
        c.execute("ALTER TABLE files ADD COLUMN start_ms INTEGER")
        c.commit()
    return c


# ---------------------------------------------------------------- the uploader's spoken advert
# Some files open with a voice saying "This video downloaded from cinefreak.net" (measured 2026-10-10: 3 of 15
# files, always inside the first 4.2 s). It is one recording, so it is found by matching the opening's
# spectrogram against that recording (ad_ref.raw: 8 kHz mono s16le, 3.4 s, cut from a file that has it).
# Measured on 57 files, every one checked by speech-to-text: the 12 with the voice score 0.56-1.00 (the lowest has
# music under it), the 45 without score 0.40 or less; 0.5 decides. The film starts 0.2 s after the recording ends.
AD_REF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ad_ref.raw")
AD_SR, AD_HOP, AD_N, AD_MATCH = 8000, 80, 256, 0.5
_AD = None


def _spec(a):
    import numpy as np
    frames = np.lib.stride_tricks.sliding_window_view(a, AD_N)[::AD_HOP] * np.hanning(AD_N)
    s = np.log(np.abs(np.fft.rfft(frames, axis=1))[:, 4:100] + 1e-6)   # ~125 Hz to 3.1 kHz: the voice
    return s - s.mean(1, keepdims=True)


def voice_start(url: str) -> int | None:
    """Where the film begins past the spoken advert, in ms; 0 when there is none; None when it could not be read."""
    global _AD
    try:
        import numpy as np
        if _AD is None:
            ref = np.frombuffer(open(AD_REF, "rb").read(), dtype="<i2").astype(np.float32) / 32768
            r = _spec(ref)
            _AD = (r - r.mean()) / r.std()
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": "bytes=0-3000000"})
        with urllib.request.urlopen(req, timeout=40) as resp:
            head = resp.read(3_000_001)
        p = subprocess.run(["ffmpeg", "-v", "error", "-i", "pipe:0", "-t", "9", "-map", "0:a:0", "-vn", "-ac", "1", "-ar", str(AD_SR), "-f", "s16le", "pipe:1"],
                           input=head, capture_output=True, timeout=60)
        pcm = p.stdout[: len(p.stdout) // 2 * 2]
        if len(pcm) < AD_SR * 2 * 4:
            return None
        a = _spec(np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768)
        L = len(_AD)
        if len(a) <= L:
            return None
        corr = np.empty(len(a) - L)
        for k in range(len(corr)):                     # normalised correlation at every 10 ms offset
            x = a[k:k + L]
            corr[k] = float(((x - x.mean()) * _AD).mean() / (x.std() + 1e-9))
        i = int(corr.argmax())
        if corr[i] < AD_MATCH:
            return 0
        # the recording's own length past where it starts, plus 0.2 s of quiet
        return int(round((i * AD_HOP / AD_SR + L * AD_HOP / AD_SR + 0.2) * 1000))
    except Exception:  # noqa: BLE001
        return None


def write_retry(conn: sqlite3.Connection, sql: str, params: tuple, tries: int = 30) -> None:
    """One write, retried while another process holds the file (SQLite returns BUSY at once on a lock cycle)."""
    for i in range(tries):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e) or i == tries - 1:
                raise
            conn.rollback()
            time.sleep(1 + i * 0.5)


# ---------------------------------------------------------------- parsing

def parse_cards(html: str) -> list[dict]:
    """The cards of a category page: slug, card title, poster, time-ago, badges."""
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("a.movie-card[href]"):
        href = a["href"]
        m = re.match(r"https?://cinefreak\.net/([^/?#]+)/?$", href)
        if not m:
            continue
        img = a.select_one("img")
        h3 = a.select_one(".movie-card-title")
        ago = a.select_one(".time-ago")
        out.append({
            "slug": m.group(1),
            "card_title": (h3.get_text(" ", strip=True) if h3 else ""),
            "poster": (img.get("src") or img.get("data-src") or "") if img else "",
            "time_ago": ago.get_text(" ", strip=True) if ago else "",
            "badges": [s.get_text(" ", strip=True) for s in a.select(".movie-card-overlay .movie-card-format")],
        })
    return out


def max_page(html: str) -> int:
    pages = [int(x) for x in re.findall(r"/page/(\d+)/", html)]
    return max(pages) if pages else 1


# 2026-10-14: the host's file codes grew from 8 lowercase characters to ~62 mixed-case ones ('utrZO3Qk...-T_g0...').
# The old [a-z0-9]+ cut them at the first capital ('utr', '2', 'x'): files got wrong codes that collide across titles
# (2,309 such rows in the Library, every one unplayable), and their /f/ pages could not open.
_FID = re.compile(r"cinecloud\.site/([fxwd])/([A-Za-z0-9_-]+)")


def _hidden(el) -> bool:
    """A decoy: the element or a parent is aria-hidden, or the link is out of the tab order (the site's scraper traps)."""
    if el is None:
        return False
    if el.get("tabindex") == "-1" or el.get("aria-hidden") == "true":
        return True
    return el.find_parent(attrs={"aria-hidden": "true"}) is not None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):  # noqa: ANN002, ANN003
        return None


# one no-redirect opener per egress lane: the direct line first, then the proxy lane(s), so one blocked IP does not stop it
_GEN_OPENERS = [urllib.request.build_opener(urllib.request.ProxyHandler({"http": u, "https": u}) if u else urllib.request.ProxyHandler({}), _NoRedirect)
                for u in _LANES]


def _unwrap(href: str | None) -> str | None:
    """18+ posts live on a sister site whose buttons go through generate.php?id=<base64>. 2026-10-15: the base64 holds a
    DECOY code (".../f/<16 chars>newgo32", 620 files, every one "not found" in the browser); the real file is where
    generate.php redirects (a full 62-char code, the same from every IP, measured from two networks). So the redirect
    is followed, one request per 18+ file. When it does not redirect, the page fails and is read again in 6 hours:
    a decoy code is never stored."""
    if not href or not re.search(r"generate\.php\?id=", href):
        return href
    url = urllib.parse.urljoin(SITE + "/", href)
    last = None
    for i in range(max(3, len(_GEN_OPENERS))):
        try:
            _GEN_OPENERS[i % len(_GEN_OPENERS)].open(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30)
            last = "no redirect"
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location") if e.code in (301, 302, 303, 307, 308) else None
            if loc and _FID.search(loc) and not _FID.search(loc).group(2).endswith("newgo32"):
                return loc
            last = f"http {e.code}"
        except Exception as e:  # noqa: BLE001
            last = str(e)[:80]
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"18+ link did not lead to a file ({last})")


def _fid(href: str | None) -> str | None:
    m = _FID.search(href or "")
    return m.group(2) if m else None


def _size_mb(text: str) -> float | None:
    # "[2..2 GB]" / "[1.8. GB]" are typos on the page: a malformed number is no size (it used to crash the title)
    m = re.search(r"\[?\s*(\d+(?:\.\d+)?)\.?\s*(GB|MB)\s*\]?", re.sub(r"(\d)\.\.(\d)", r"\1.\2", text or ""), re.I)
    if not m:
        return None
    n = float(m.group(1))
    return round(n * 1024, 1) if m.group(2).upper() == "GB" else n


def _quality(text: str) -> int | None:
    m = re.search(r"(\d{3,4})p", text or "", re.I)
    if m:
        return int(m.group(1))
    if re.search(r"\b4k\b|2160", text or "", re.I):
        return 2160
    return None


def _lang_tag(text: str) -> str | None:
    m = re.search(r"\[([^\]]+)\]", text or "")
    return m.group(1).strip() if m and not re.search(r"\d\s*(GB|MB)", m.group(1), re.I) else None


def _details(soup: BeautifulSoup) -> dict:
    """The `<b>Label:</b> value` block under 'Details'."""
    d = {}
    for b in soup.select("b"):
        label = b.get_text(" ", strip=True).rstrip(":").strip()
        if not label or len(label) > 40:
            continue
        # the value is the text that follows the <b> until the next <br> / <b>
        parts = []
        for sib in b.next_siblings:
            name = getattr(sib, "name", None)
            if name in ("br", "b", "h3", "p", "div"):
                break
            parts.append(sib.get_text(" ", strip=True) if name else str(sib))
        val = unescape(" ".join(p for p in parts if p)).strip()
        key = re.sub(r"[^a-z]", "", label.lower())
        if key and key not in d:
            d[key] = val
    return d


def _date(s: str) -> str | None:
    """'October 8, 2026' / 'Jul 2, 2026' -> '2026-10-08'."""
    if not s:
        return None
    s = s.strip().replace("Aired", "").strip()
    s = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", s)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y", "%B %Y", "%Y"):
        try:
            return time.strftime("%Y-%m-%d", time.strptime(s, fmt))
        except ValueError:
            continue
    m = re.search(r"(\d{4})", s)
    return f"{m.group(1)}-01-01" if m else None


def parse_title(html: str, slug: str) -> dict:
    """A title page -> {title fields, episodes[], files[]}."""
    soup = BeautifulSoup(html, "lxml")
    body_cls = " ".join(soup.body.get("class", [])) if soup.body else ""
    pm = re.search(r"postid-(\d+)", body_cls)
    post_id = int(pm.group(1)) if pm else None
    series = soup.select_one("#cfSeries") is not None
    det = _details(soup)
    name = det.get("fullmoviename") or det.get("seriesname") or det.get("moviename") or det.get("name") or ""
    h1 = soup.select_one("h1")
    h1t = h1.get_text(" ", strip=True) if h1 else ""
    ym = re.search(r"^(.*?)\s*\((\d{4})\)", name or h1t)
    title = (ym.group(1) if ym else (name or h1t)).strip(" -–|")
    year = int(ym.group(2)) if ym else None
    if year is None:
        hy = re.search(r"\((\d{4})\)", h1t) or re.search(r"\b(19\d{2}|20\d{2})\b", h1t) or re.search(r"-(19\d{2}|20\d{2})-", slug)
        year = int(hy.group(1)) if hy else None
    if not title:
        title = re.sub(r"\s*\(\d{4}\).*$", "", h1t).strip()
    title = re.sub(r"\s+", " ", title)
    rating = None
    rm = re.search(r"([\d.]+)\s*/\s*10", det.get("tmdbrating") or det.get("rating") or det.get("imdbrating") or "")
    if rm:
        rating = float(rm.group(1))
    tm = re.search(r"themoviedb\.org/(movie|tv)/(\d+)", html)
    tmdb_id = int(tm.group(2)) if tm else None
    im = re.search(r"imdb\.com/title/(tt\d{6,9})", html)
    imdb_id = im.group(1) if im else None
    # The title's OWN poster sits in div.poster-image (2026-10-11). The old rule — the first wp-post-image with a
    # TMDB/cineimg link — skipped a poster the site hosts itself and took a "related" card's instead: 102 of 1,955
    # titles wore another film's poster (56 the "Happily Married" one). Self-hosted posters are served to the app
    # through the Library worker's /img/ (the host refuses the app's image loader).
    poster = None
    own = soup.select_one("div.poster-image img")
    if own:
        src = (own.get("src") or own.get("data-src") or own.get("data-lazy-src") or "").strip()
        if src.startswith("http") and "placeholder" not in src:
            poster = src
    if not poster:
        for img in soup.select("img.wp-post-image"):
            src = img.get("src") or ""
            if "tmdb.org" in src or "cineimg" in src:
                poster = src
                break
    # plot: the paragraph after the "Plot Summary" heading
    overview = None
    for h in soup.select("h3.movie-title"):
        if "plot" in h.get_text(" ", strip=True).lower():
            p = h.find_next("p")
            if p:
                overview = p.get_text(" ", strip=True)
            break
    runtime = None
    rts = det.get("runtime") or ""
    hm = re.search(r"(\d+)\s*h(?:ours?|rs?)?\b", rts, re.I)
    mm = re.search(r"(\d+)\s*m(?:in(?:ute)?s?)?\b", rts, re.I)
    if hm or mm:
        runtime = (int(hm.group(1)) * 60 if hm else 0) + (int(mm.group(1)) if mm else 0)
    cats = sorted({m for m in re.findall(r'href="https?://cinefreak\.net/category/([^"/]+)/?"', html)})
    ott = sorted({unescape(m) for m in re.findall(r'<span class="meta-badge badge-ott">([^<]+)</span>', html)})
    genres_tax = sorted({unescape(m) for m in re.findall(r'href="https?://cinefreak\.net/genre/([^"/]+)/?"', html)})
    pub = re.search(r'property="article:published_time" content="([^"]+)"', html)
    mod = re.search(r'property="article:modified_time" content="([^"]+)"', html)
    status = det.get("status")
    out = {
        "slug": slug, "post_id": post_id, "type": "tv" if series else "movie", "title": title, "year": year,
        "poster": poster, "overview": overview, "genres": det.get("genres") or det.get("genre"),
        "language": det.get("language"), "country": det.get("country"), "rating": rating,
        "release_date": _date((det.get("releasedate") or det.get("releasedates") or "").split(" to ")[0]),
        "runtime": runtime, "tmdb_id": tmdb_id, "imdb_id": imdb_id, "status": status, "cats": ",".join(cats), "ott": ",".join(ott), "genres_tax": ",".join(genres_tax),
        "published": pub.group(1) if pub else None, "modified": mod.group(1) if mod else None,
        "episodes": [], "files": [],
    }
    if series:
        for grp in soup.select("#cfSeries .cft-grp"):
            season = int(grp.get("data-s") or 0) or 1
            for it in grp.select(".cft-it"):
                info = it.select_one(".cft-info")
                if not info:
                    continue
                b = info.select_one("b")
                em = re.search(r"(\d+)\s*(?:[-–]\s*(\d+))?", b.get_text(" ", strip=True) if b else "")
                if not em:
                    continue
                ep_from = int(em.group(1))
                ep_to = int(em.group(2)) if em.group(2) else ep_from
                epn = info.select_one(".cft-epn")
                metas = [s.get_text(" ", strip=True) for s in info.select(".cft-meta > span")]
                lang = next((m for m in metas if re.search(r"[A-Za-z]", m) and not re.search(r"min|Aired|watched|WEB|HD|BluRay|Rip", m, re.I)), None)
                rtm = next((int(re.search(r"(\d+)\s*min", m).group(1)) for m in metas if re.search(r"\d+\s*min", m)), None)
                aired = next((_date(m) for m in metas if m.startswith("Aired")), None)
                for ep in range(ep_from, ep_to + 1):
                    out["episodes"].append({"season": season, "ep": ep, "name": epn.get_text(" ", strip=True) if epn and ep == ep_from else None, "aired": aired, "runtime": rtm, "lang": lang})
                watch = it.select_one("a.cfs-watch")
                watch_fids = set((watch.get("data-fids") or "").split(",")) if watch else set()
                if watch and _fid(watch.get("href")):
                    watch_fids.add(_fid(watch.get("href")))
                for a in it.select("a.cfs-dl"):
                    fid = _fid(a.get("href"))
                    if not fid:
                        continue
                    hq = a.get("data-hq") or a.get_text(" ", strip=True)
                    small = a.select_one("small")
                    out["files"].append({
                        "fid": fid, "season": season, "ep_from": ep_from, "ep_to": ep_to, "quality": _quality(hq),
                        "hevc": 1 if (a.get("data-hevc") == "1" or re.search(r"hevc|x265", hq, re.I)) else 0,
                        "size_mb": _size_mb(small.get_text() if small else hq), "label": hq.strip(), "lang": lang,
                        "watch": 1 if fid in watch_fids else 0,
                    })
    elif soup.select_one(".episode-grid .ep-card"):
        # THE OLDER SERIES LAYOUT (2026-10-15): no #cfSeries, one .ep-card per episode, the files in two pop-up boxes
        # ("Download Resolution" / "Watch Resolution"). It was read as a film with no files, so ~180 series never
        # reached the app. The download box's links are the files; a watch link with the same quality marks it watch.
        for card in soup.select(".episode-grid .ep-card"):
            if _hidden(card):
                continue
            tt = card.select_one(".ep-title")
            sm = re.search(r"Season\s*(\d+)", tt.get_text(" ", strip=True) if tt else "", re.I)
            season = int(sm.group(1)) if sm else 1
            badge = card.select_one(".episode-badge")
            # only "Episode 3" / "Episode 1-8" / "Episode: 1-8" is an episode card; an empty badge, "Batch/MKV",
            # "Season 1 Complete" or a pasted title ("Goodbye Earth (2024) Season 1") is a whole-season pack, skipped
            # like the #cfSeries layout skips its packs (measured on the 1,017 pages: 311 empty, 40 batch, ~90 titles)
            em = re.search(r"\bEp(?:isode)?s?\s*[:.#]?\s*(\d{1,4})(?:\s*(?:[-–~]|to)\s*(?:Ep(?:isode)?\s*)?(\d{1,4}))?(?!\d)",
                           badge.get_text(" ", strip=True) if badge else "", re.I)
            if not em or re.search(r"isn.t available", badge.get_text(" ", strip=True), re.I):
                continue
            ep_from = int(em.group(1))
            ep_to = int(em.group(2)) if em.group(2) and int(em.group(2)) >= ep_from else ep_from
            if ep_from <= 0 or ep_to - ep_from > 60:
                continue
            for ep in range(ep_from, ep_to + 1):
                out["episodes"].append({"season": season, "ep": ep, "name": None, "aired": None, "runtime": None, "lang": None})
            boxes = {b.get("id", ""): b for b in card.select(".quality-box")}
            dl = next((b for k, b in boxes.items() if k.startswith("dl")), None)
            wb = next((b for k, b in boxes.items() if k.startswith("watch")), None)
            watch_q = {_quality(a.get_text(" ", strip=True)) for a in (wb.select("a") if wb else []) if not _hidden(a)}
            for a in (dl or wb).select("a") if (dl or wb) else []:
                if _hidden(a):
                    continue
                fid = _fid(a.get("href"))
                if not fid:
                    continue
                hq = a.get_text(" ", strip=True)
                out["files"].append({
                    "fid": fid, "season": season, "ep_from": ep_from, "ep_to": ep_to, "quality": _quality(hq),
                    "hevc": 1 if re.search(r"hevc|x265|10\s*bit", hq, re.I) else 0, "size_mb": _size_mb(hq), "label": hq,
                    "lang": None, "watch": 1 if (dl is None or _quality(hq) in watch_q) else 0,
                })
        # a page of whole-season packs only (no episode card) stays as it was read before: no files, not a series to re-check
        if out["episodes"]:
            out["type"] = "tv"
    else:
        seen_ep: set = set()
        for h4 in soup.select(".download-links-div h4"):
            # a decoy block (aria-hidden, its links tabindex=-1) sits in front of the real ones: never a file of ours
            if _hidden(h4):
                continue
            label = h4.get_text(" ", strip=True)
            box = h4.find_next_sibling("div")
            if not box:
                continue
            # THE EPISODE-BUTTON SERIES LAYOUT (2026-10-15): "<h4>Monvojon (2024) Season 1 [Bengali] HD 720p</h4>" and a
            # row of "Episode 1" ... "Episode 15" buttons, one file each. Read as a film it became Episode 1 alone.
            eps = []
            for a in box.select("a") if "downloads-btns-div" in (box.get("class") or []) else []:
                m = re.fullmatch(r"Ep(?:isode)?s?\s*[:.#]?\s*(\d{1,4})(?:\s*[-–~]\s*(\d{1,4}))?", a.get_text(" ", strip=True), re.I)
                if m and not _hidden(a):
                    eps.append((a, int(m.group(1)), int(m.group(2) or m.group(1))))
            if eps:
                out["type"] = "tv"
                sm = re.search(r"Season\s*(\d+)", label, re.I)
                season = int(sm.group(1)) if sm else 1
                for a, ep_from, ep_to in eps:
                    fid = _fid(_unwrap(a.get("href")))
                    # the same file behind two buttons (Cute Programmer "Episode 10" and "Episode 11") is one file: the
                    # first button keeps it; for a file the host serves directly, refine() reads the true episode from its name
                    if not fid or fid in seen_ep or ep_from <= 0 or ep_to < ep_from or ep_to - ep_from > 60:
                        continue
                    seen_ep.add(fid)
                    for ep in range(ep_from, ep_to + 1):
                        if not any(e["season"] == season and e["ep"] == ep for e in out["episodes"]):
                            out["episodes"].append({"season": season, "ep": ep, "name": None, "aired": None, "runtime": None, "lang": _lang_tag(label)})
                    out["files"].append({
                        "fid": fid, "season": season, "ep_from": ep_from, "ep_to": ep_to, "quality": _quality(label),
                        "hevc": 1 if re.search(r"hevc|x265|10\s*bit", label, re.I) else 0, "size_mb": None,
                        "label": f"{label} Episode {ep_from}" + (f"-{ep_to}" if ep_to != ep_from else ""), "lang": _lang_tag(label), "watch": 0,
                    })
                continue
            dl = box.select_one("a.dlbtn-download") or box.select_one(".downloads-btns-div a, a.btn") or (box.select_one("a") if "downloads-btns-div" in (box.get("class") or []) else None)
            if dl is not None and _hidden(dl):
                continue
            w = box.select_one("a.dlbtn-watch")
            fid = _fid(_unwrap(dl.get("href")) if dl else None)
            if not fid:
                continue
            out["files"].append({
                "fid": fid, "season": None, "ep_from": None, "ep_to": None, "quality": _quality(label),
                "hevc": 1 if re.search(r"hevc|x265|10\s*bit", label, re.I) else 0, "size_mb": _size_mb(label),
                "label": label, "lang": _lang_tag(label), "watch": 1 if (w and _fid(w.get("href")) == fid) else 0,
            })
    _settle_title(out, name, h1t)
    return out


def _norm(s: str) -> str:
    # an apostrophe is dropped, never turned into a space: "Charlotte’s Web" (the field, curly) and "Charlotte's Web"
    # (the file labels, straight) must read as the same name (2026-10-15: they did not, so the labels "named another
    # film" and every file of such a title was dropped)
    s = re.sub(r"['’‘`´]", "", s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _named(s: str) -> str | None:
    """'Ferar Opekkhay (2026) [Bengali] HD 720p' -> 'Ferar Opekkhay'; None when there is no '(year)' to cut at."""
    # a leading tag ("[18+] Ungol (2024) ...", the 18+ sister site's labels) is not part of the name
    s = re.sub(r"^\s*(?:\[[^\]]*\]\s*)+", "", s or "")
    m = re.search(r"^(.*?)\s*\((\d{4})\)", s)
    return m.group(1).strip(" -–|") if m else None


def _settle_title(out: dict, name: str, h1t: str) -> None:
    """
    WHICH NAME IS THE FILM (owner, 2026-10-10: "ভিডিও একটা আর টাইটেলে দেখাচ্ছে আরেকটা").
    The page's "Full Movie Name" field is a template the uploader fills by hand and sometimes leaves from the post
    copied (post 54259: field "Look A Like", but H1, <title>, every file label and the files themselves are
    "Ferar Opekkhay"). The file labels name what is really in the files, so they vote: the H1's name replaces the
    field's only when more labels carry it. And when the labels name a third film altogether (post 53769: page
    "Ab Tak Chhappan 2", files "Adarsh Baal Vidyalaya"), those files are not this title's and are dropped rather
    than shown under the wrong name and poster. Measured on all 1,955 indexed titles: 5 names change, 1 title
    loses its files; the 22 titles where only an apostrophe or an "[18+]" differs keep the field's name.
    """
    labels = [_norm(_named(f["label"]) or "") for f in out["files"] if f.get("season") is None]
    labels = [x for x in labels if x]
    if not labels:
        return
    def votes(x: str) -> int:
        x = _norm(x)
        return sum(1 for l in labels if x and (l.startswith(x) or x.startswith(l))) if x else 0
    h1_name = _named(h1t)
    if h1_name and _norm(h1_name) != _norm(out["title"]) and votes(h1_name) > votes(out["title"]):
        out["title"] = re.sub(r"\s+", " ", h1_name)
    # the H1 naming the labels' film anywhere keeps the files: a dub's post is "Shatamanam Bhavati – Poribar 2024"
    # with every label "Poribar (2024)" (2026-10-15: 184 such titles had lost all their files)
    h1n = f" {_norm(h1t)} "
    in_h1 = any(f" {l} " in h1n for l in labels)
    if len(labels) >= 2 and votes(out["title"]) == 0 and not (h1_name and votes(h1_name)) and not votes(name or "") and not in_h1:
        out["files"] = [f for f in out["files"] if f.get("season") is not None]
        out["files_dropped"] = "labels name another film"


R2_RE = re.compile(r'https://pub-[a-z0-9]+\.r2\.dev/[^"\'\s<>]+')
SUB_RE = re.compile(r'sub(?:%5B0%5D|\[0\])=([^&"\']+)')


def parse_file_page(html: str) -> dict:
    """`/f/<fid>` -> the direct file (R2) or the host kind that holds it."""
    m = R2_RE.search(html)
    if m:
        url = unescape(m.group(0))
        # a season packed as an archive is a download, not a stream: kept, never offered for play
        if re.search(r"\.(zip|rar|7z)(\?|$)", url, re.I):
            return {"host": "r2zip", "url": url}
        return {"host": "r2", "url": url}
    if "Instant Download" in html or "/w/" in html or "/d/" in html or "Login To DL" in html:
        return {"host": "gd", "url": None}
    return {"host": "unknown", "url": None}


def parse_watch_page(html: str) -> dict:
    """`/x/<fid>` -> the embed's file and subtitle, when the page carries them."""
    out = {"url": None, "sub_url": None}
    m = re.search(r'player\.yagaverse\.net/embed2/\?id=([^&"\']+)', html)
    if m:
        out["url"] = urllib.parse.unquote(m.group(1))
    s = SUB_RE.search(html)
    if s:
        out["sub_url"] = urllib.parse.unquote(unescape(s.group(1)))
    return out


# ---------------------------------------------------------------- crawl

def title_hash(t: dict, episodes: list, files: list) -> str:
    keep = {k: t.get(k) for k in ("post_id", "type", "title", "year", "poster", "overview", "genres", "language", "country", "rating", "release_date", "runtime", "tmdb_id", "imdb_id", "status", "cats", "ott")}
    keep["episodes"] = sorted((e["season"], e["ep"], e.get("name"), e.get("aired"), e.get("runtime")) for e in episodes)
    keep["files"] = sorted((f["fid"], f.get("season"), f.get("ep_from"), f.get("ep_to"), f.get("quality"), f.get("hevc"), f.get("size_mb"), f.get("watch")) for f in files)
    return hashlib.sha1(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()[:16]


def recent(time_ago: str, hours: float) -> bool:
    m = re.search(r"(\d+)\s*(second|min|hour|day|week|month|year)", time_ago or "", re.I)
    if not m:
        return False
    n, unit = int(m.group(1)), m.group(2).lower()
    h = {"second": n / 3600, "min": n / 60, "hour": n, "day": n * 24, "week": n * 168, "month": n * 720, "year": n * 8760}[unit]
    return h <= hours


def crawl_cards(conn, cats: list[str], full: bool, max_pages: int | None, recent_hours: float) -> list[str]:
    """Walk category pages; return slugs whose title page must be (re)fetched."""
    todo: dict[str, None] = {}
    now = int(time.time())
    for cat in cats:
        page = 1
        last = None
        while True:
            url = f"{SITE}/{cat}/" if page == 1 else f"{SITE}/{cat}/page/{page}/"
            try:
                html = fetch(url)
            except Exception as e:  # noqa: BLE001
                say(f"[cards] {cat} p{page}: fetch failed")
                break
            if last is None:
                last = max_page(html)
                if max_pages:
                    last = min(last, max_pages)
            cards = parse_cards(html)
            fresh = 0
            for c in cards:
                row = conn.execute("SELECT fetched_at, hash, time_ago, type, status, err FROM titles WHERE slug=?", (c["slug"],)).fetchone()
                conn.execute(
                    "INSERT INTO titles(slug, card_title, poster, time_ago, seen_at, badges) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(slug) DO UPDATE SET card_title=excluded.card_title, poster=COALESCE(titles.poster, excluded.poster), time_ago=excluded.time_ago, seen_at=excluded.seen_at, badges=excluded.badges",
                    (c["slug"], c["card_title"], c["poster"], c["time_ago"], now, ",".join(c["badges"])),
                )
                need = row is None or not row["fetched_at"] or row["err"] is not None or recent(c["time_ago"], recent_hours)
                if need:
                    todo[c["slug"]] = None
                    fresh += 1
            conn.commit()
            print(f"[cards] {cat} p{page}/{last}: {len(cards)} cards, {fresh} to fetch")
            if not full and fresh == 0 and page >= 2:
                break  # a delta walk stops once a page holds nothing new
            page += 1
            if page > last:
                break
    return list(todo)


def fetch_title(slug: str) -> tuple[str, dict | None, str | None]:
    try:
        html = fetch(f"{SITE}/{slug}/")
        return slug, parse_title(html, slug), None
    except Exception as e:  # noqa: BLE001
        return slug, None, str(e)[:200]


def store_title(conn, t: dict) -> bool:
    """Write a parsed title; True when its hash changed."""
    h = title_hash(t, t["episodes"], t["files"])
    old = conn.execute("SELECT hash FROM titles WHERE slug=?", (t["slug"],)).fetchone()
    now = int(time.time())
    conn.execute(
        "UPDATE titles SET post_id=?, type=?, title=?, year=?, poster=COALESCE(?, poster), overview=?, genres=?, language=?, country=?, rating=?, "
        "release_date=?, runtime=?, tmdb_id=?, imdb_id=?, status=?, cats=?, ott=?, published=?, modified=?, fetched_at=?, hash=?, err=NULL WHERE slug=?",
        (t["post_id"], t["type"], t["title"], t["year"], t["poster"], t["overview"], t["genres"], t["language"], t["country"], t["rating"],
         t["release_date"], t["runtime"], t["tmdb_id"], t["imdb_id"], t["status"], t["cats"], t["ott"], t["published"], t["modified"], now, h, t["slug"]),
    )
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        conn.execute("INSERT INTO titles(slug, fetched_at, hash) VALUES(?,?,?)", (t["slug"], now, h))
        conn.execute(
            "UPDATE titles SET post_id=?, type=?, title=?, year=?, poster=?, overview=?, genres=?, language=?, country=?, rating=?, release_date=?, runtime=?, tmdb_id=?, imdb_id=?, status=?, cats=?, ott=?, published=?, modified=? WHERE slug=?",
            (t["post_id"], t["type"], t["title"], t["year"], t["poster"], t["overview"], t["genres"], t["language"], t["country"], t["rating"], t["release_date"], t["runtime"], t["tmdb_id"], t["imdb_id"], t["status"], t["cats"], t["ott"], t["published"], t["modified"], t["slug"]),
        )
    conn.execute("DELETE FROM episodes WHERE slug=?", (t["slug"],))
    conn.executemany("INSERT OR REPLACE INTO episodes(slug, season, ep, name, aired, runtime, lang) VALUES(?,?,?,?,?,?,?)",
                     [(t["slug"], e["season"], e["ep"], e["name"], e["aired"], e["runtime"], e["lang"]) for e in t["episodes"]])
    seen = {f["fid"] for f in t["files"]}
    # 2026-10-15: a page read again gives its files new codes. A file already resolved to a lasting link (the public R2 copy)
    # keeps it: the new row of the same file (same season, episodes, quality, size, label) takes the old row's link, when
    # exactly one old and one new row match. 'gu' is not carried: the gateway asks the host with the file's own code.
    fkey = lambda f: (f["season"], f["ep_from"], f["ep_to"], f["quality"], f["hevc"], f["size_mb"], f["label"])  # noqa: E731
    keep: dict = {}
    for o in conn.execute("SELECT * FROM files WHERE slug=? AND host IN ('r2','r2zip') AND url IS NOT NULL", (t["slug"],)).fetchall():
        if o["fid"] not in seen:
            k = fkey(o)
            keep[k] = None if k in keep else o
    newk: dict = {}
    for f in t["files"]:
        newk[fkey(f)] = newk.get(fkey(f), 0) + 1
    conn.execute("DELETE FROM files WHERE slug=? AND fid NOT IN (%s)" % ",".join("?" * len(seen)) if seen else "DELETE FROM files WHERE slug=?",
                 (t["slug"], *seen) if seen else (t["slug"],))
    for f in t["files"]:
        conn.execute(
            "INSERT INTO files(fid, slug, season, ep_from, ep_to, quality, hevc, size_mb, label, lang, watch) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(fid) DO UPDATE SET slug=excluded.slug, season=excluded.season, ep_from=excluded.ep_from, ep_to=excluded.ep_to, quality=excluded.quality, "
            "hevc=excluded.hevc, size_mb=excluded.size_mb, label=excluded.label, lang=excluded.lang, watch=excluded.watch",
            (f["fid"], t["slug"], f["season"], f["ep_from"], f["ep_to"], f["quality"], f["hevc"], f["size_mb"], f["label"], f["lang"], f["watch"]),
        )
        o = keep.get(fkey(f))
        if o is not None and newk[fkey(f)] == 1:
            conn.execute("UPDATE files SET host=?, url=?, sub_url=?, bytes=?, start_ms=?, resolved_at=?, res_err=NULL WHERE fid=? AND (host IS NULL OR host NOT IN ('r2','r2zip'))",
                         (o["host"], o["url"], o["sub_url"], o["bytes"], o["start_ms"], o["resolved_at"], f["fid"]))
    return old is None or old["hash"] != h


def cmd_crawl(args):
    conn = db()
    cats = args.cats.split(",") if args.cats else CATEGORIES
    if args.titles_only:
        slugs = [r["slug"] for r in conn.execute("SELECT slug FROM titles WHERE fetched_at IS NULL OR fetched_at=0 OR err IS NOT NULL")]
    else:
        slugs = crawl_cards(conn, cats, args.full, args.max_pages, args.recent_hours)
    if args.slugs:
        slugs = list(dict.fromkeys(slugs + args.slugs.split(",")))
    # pages seen on a card walk but never read (a run that ran out of time) are always picked up
    known = set(slugs)
    # ONCE (genfix, 2026-10-15): 620 18+ files on 219 pages were read from the decoy inside generate.php (codes ending
    # "newgo32", "not found" in the browser). They are dropped and those pages are read again (fetched_at NULL is
    # picked up just below, and again by every run until the page has been read), now through the redirect.
    if not conn.execute("SELECT 1 FROM meta WHERE k='genfix'").fetchone():
        bad = [r["slug"] for r in conn.execute("SELECT DISTINCT slug FROM files WHERE fid LIKE '%newgo32'")]
        conn.execute("DELETE FROM files WHERE fid LIKE '%newgo32'")
        conn.executemany("UPDATE titles SET fetched_at=NULL WHERE slug=?", [(x,) for x in bad])
        conn.execute("INSERT INTO meta(k,v) VALUES('genfix',?)", (str(int(time.time())),))
        conn.commit()
        print(f"[genfix] {len(bad)} 18+ pages are read again for their real file codes", flush=True)
    for r in conn.execute("SELECT slug FROM titles WHERE fetched_at IS NULL OR (err IS NOT NULL AND fetched_at < ?) ORDER BY rowid DESC", (int(time.time()) - 6 * 3600,)):
        if r["slug"] not in known:
            slugs.append(r["slug"]); known.add(r["slug"])
    # A NEW PARSER RE-READS EVERY PAGE ONCE (2026-10-11: title vote + the title's own poster). The moment this
    # parser first ran is kept in meta; every page read before it is queued behind the run's normal work, newest
    # first, and the crawl time budget spreads the re-read over as many runs as it needs. The push then writes
    # only the titles whose fields really changed.
    pv = conn.execute("SELECT v FROM meta WHERE k='parser'").fetchone()
    if not pv or int(pv["v"].split(":")[0]) < PARSER:
        conn.execute("INSERT INTO meta(k,v) VALUES('parser',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (f"{PARSER}:{int(time.time())}",))
        conn.commit()
        pv = conn.execute("SELECT v FROM meta WHERE k='parser'").fetchone()
    since = int(pv["v"].split(":")[1])
    for r in conn.execute("SELECT slug FROM titles WHERE fetched_at > 0 AND fetched_at < ? ORDER BY COALESCE(published, '') DESC", (since,)):
        if r["slug"] not in known:
            slugs.append(r["slug"]); known.add(r["slug"])
    # ONCE (fidfix): every title holding a file code cut by the old pattern, or a file never classified, is read again
    # so its files get their full codes; the push then replaces the cut rows in the Library (ingest drops a title's
    # files that are no longer sent).
    if not conn.execute("SELECT 1 FROM meta WHERE k='fidfix'").fetchone():
        for r in conn.execute("SELECT DISTINCT f.slug FROM files f LEFT JOIN titles t ON t.slug=f.slug "
                              "WHERE (length(f.fid) < 20 AND length(f.fid) != 8) OR f.host IN ('unknown','wall') OR f.res_err IS NOT NULL "
                              "ORDER BY COALESCE(t.published, '') DESC"):
            if r["slug"] and r["slug"] not in known:
                slugs.append(r["slug"]); known.add(r["slug"])
        conn.execute("INSERT INTO meta(k,v) VALUES('fidfix',?)", (str(int(time.time())),))
        conn.commit()
    # ONCE PER TITLE (layouts, 2026-10-15): the parser now reads the older series layouts (episode cards, episode
    # buttons), the 18+ sister site's wrapped buttons and the dub posts whose labels carry the dub's name. 1,017 read
    # pages had no file at all (227 of them 18+) and a page with no file is never read again, so each is queued once,
    # newest first, until it has been read by this parser: the moment it first ran is kept in meta, and a title read
    # after it drops out of the query by itself, so a run that runs out of time loses nothing. Titles that have files
    # are NOT re-read (that would re-resolve and re-write ~10,000 of them for nothing).
    lv = conn.execute("SELECT v FROM meta WHERE k='layouts'").fetchone()
    if not lv:
        conn.execute("INSERT INTO meta(k,v) VALUES('layouts',?)", (str(int(time.time())),))
        conn.commit()
        lv = conn.execute("SELECT v FROM meta WHERE k='layouts'").fetchone()
    nre = 0
    for r in conn.execute("SELECT slug FROM titles t WHERE fetched_at > 0 AND fetched_at < ? AND NOT EXISTS (SELECT 1 FROM files f WHERE f.slug=t.slug) "
                          "ORDER BY COALESCE(published, '') DESC", (int(lv["v"]),)):
        if r["slug"] not in known:
            slugs.append(r["slug"]); known.add(r["slug"]); nre += 1
    if nre:
        print(f"[layouts] {nre} pages with no file are read again by the new parser", flush=True)
    # series still airing are re-read on every delta run (new episodes do not move the card)
    if not args.full:
        for r in conn.execute("SELECT slug FROM titles WHERE type='tv' AND (status IS NULL OR status NOT LIKE '%All Episodes%') AND fetched_at < ?", (int(time.time()) - args.series_recheck_hours * 3600,)):
            if r["slug"] not in slugs:
                slugs.append(r["slug"])
    print(f"[titles] {len(slugs)} pages to fetch")
    changed = 0
    done = 0
    by = deadline_of("CRAWL")
    ex = ThreadPoolExecutor(max_workers=CONC)
    try:
        futs = [ex.submit(fetch_title, s) for s in slugs]
        for fut in as_completed(futs):
            if time.time() > by:
                say(f"[titles] time budget reached at {done}/{len(slugs)}; the rest waits for the next run")
                for f in futs: f.cancel()
                break
            slug, t, err = fut.result()
            done += 1
            if err:
                conn.execute("UPDATE titles SET err=?, fetched_at=COALESCE(fetched_at, 0) WHERE slug=?", (err, slug))
                say("[titles] one page failed", f"{slug}: {err}")
            else:
                if store_title(conn, t):
                    changed += 1
            if done % 25 == 0:
                conn.commit()
                time.sleep(0.4)  # a window for the resolve and the push, which share the file
                print(f"[titles] {done}/{len(slugs)} ({changed} changed)", flush=True)
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    conn.commit()
    print(f"[titles] done: {done} fetched, {changed} changed")
    if args.resolve:
        cmd_resolve(args)


def resolve_one(fid: str, watch: int) -> tuple[str, dict]:
    out = {"host": None, "url": None, "sub_url": None, "bytes": None, "err": None, "start_ms": None}
    try:
        f = parse_file_page(fetch(f"https://new5.cinecloud.site/f/{fid}", tries=2))
        out.update(f)
        if f["host"] == "gd":
            # A Drive copy: '/w/<fid>' names a fresh googleusercontent link (works from any network, no
            # ranges) for the newer uploads; the oldest point at a Drive id that is gone. Only the class is
            # kept: the gateway asks /w/ again at play time, because the link is signed and short-lived.
            w = fetch(f"https://new5.cinecloud.site/w/{fid}")
            out["host"] = "gu" if "video-downloads.googleusercontent.com" in w else ("gdid" if "download.aspx?id=" in w else "gd")
            return fid, out
        if watch:
            try:
                w = parse_watch_page(fetch(f"https://new5.cinecloud.site/x/{fid}"))
                out["sub_url"] = w.get("sub_url")
                if not out["url"] and w.get("url"):
                    out["url"], out["host"] = w["url"], "r2"
            except Exception:  # noqa: BLE001
                pass
        if out["url"]:
            req = urllib.request.Request(out["url"], headers={"User-Agent": UA, "Range": "bytes=0-1023"})
            with urllib.request.urlopen(req, timeout=30) as r:
                cr = r.headers.get("Content-Range") or ""
                m = re.search(r"/(\d+)$", cr)
                out["bytes"] = int(m.group(1)) if m else int(r.headers.get("Content-Length") or 0)
                if r.status not in (200, 206):
                    out["err"] = f"http {r.status}"
            if out["host"] == "r2" and not out["err"]:
                out["start_ms"] = voice_start(out["url"])
    except Exception as e:  # noqa: BLE001
        if getattr(e, "code", None) == 403 or "HTTP Error 403" in str(e):
            # 2026-10-14: the file host shows a Turnstile page to every plain request since 2026-10-11; such a file
            # waits for the browser pass (cmd_bresolve) instead of failing again every run
            out["host"], out["err"] = "wall", None
        else:
            out["err"] = str(e)[:200]
    return fid, out


# ---------------------------------------------------------------- the browser pass (2026-10-14)
# Since 2026-10-11 '/f/<fid>' answers a plain request with Turnstile. A real browser (patchright, under xvfb) passes
# it in ~10 s and the page then shows its buttons. Measured 2026-10-14: on most 720p/1080p film files the
# "FAST CLOUD [FSL]" button is the same public R2 link as before (pub-*.r2.dev, ranged 206, no token): it is kept,
# exactly as the old resolve kept it. 480p files and the newest series episodes carry only "FAST CLOUD [V2]" (a
# vidzer ticket bound to the browser, expires in 6 h) or a Drive copy: nothing permanent, so they are kept as 'v2'
# or 'gd' and never offered for play. Pages are opened one after another (one tab): the host is never hammered.
BR_HOST = "https://new5.cinecloud.site"
BR_ADS = re.compile(r"profitableratecpm|llvpn\.com|ay267\.com|googletagmanager|google-analytics|cloudflareinsights")


def _buttons(c: str) -> list[dict]:
    c = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div>', "", c, flags=re.S)   # hidden decoys
    out = []
    for m in re.finditer(r"<a\b([^>]*)>(.*?)</a>", c, re.S):
        a = dict(re.findall(r'([\w-]+)="([^"]*)"', m.group(1)))
        if a.get("tabindex") == "-1":
            continue
        out.append({"href": unescape(a.get("href", "")), "text": re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()[:60]})
    return out


async def _br_page(page, fid: str, wait: int = 75) -> tuple[str, list]:
    """Open one file page, pass Turnstile, return (title, buttons). The first page shows Cloudflare's full-page check,
    then the page's own Turnstile box guards the buttons; each is clicked once it has drawn (the click pattern proven on
    GitHub's runners, 2026-10-14: 24 s for the first page, 10 s for every page after it)."""
    import asyncio
    t0 = time.time()
    await page.goto(f"{BR_HOST}/f/{fid}", timeout=90000)

    async def ev(js):
        try:
            return await page.evaluate(js)
        except Exception:  # noqa: BLE001  (the page navigates while Turnstile finishes)
            return None

    async def click_ts():
        for fr in page.frames:
            if "challenges.cloudflare.com" in fr.url:
                try:
                    box = await (await fr.frame_element()).bounding_box()
                    if box and box["width"] > 0:
                        await page.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                except Exception:  # noqa: BLE001
                    pass
    c, clicks, marks = "", 0, (6, 20, 40, 60)
    while time.time() - t0 < wait:
        c = await ev("() => { const e=document.getElementById('download-buttons-container'); return e ? e.innerHTML : ''; }") or ""
        if c.strip() and "cf-ts-box" not in c:
            break
        # 2026-10-15: a file code read a day ago opens 'Link expired' (5/5 of the pages that showed no buttons, probe run
        # 38720704966); that page and 'File not Found' never draw buttons, so they are not waited on for 75 s
        tt = ((await ev("() => document.title")) or "").lower()
        if "link expired" in tt or "file not found" in tt:
            return tt, []
        # an episode page with only the V2 player shows no buttons at all, just the vidzer iframe
        if await ev("() => !!document.querySelector('iframe#cfPlayer[src]')"):
            return (await ev("() => document.title")) or "", [{"href": "", "text": "PLAYER V2"}]
        if clicks < len(marks) and time.time() - t0 > marks[clicks] and any("challenges.cloudflare.com" in f.url for f in page.frames):
            clicks += 1
            await click_ts()
        await asyncio.sleep(2)
    if "cf-ts-box" in c:
        c = ""
    return (await ev("() => document.title")) or "", _buttons(c)


# 2026-10-15 ~05:00Z the file page changed: every button is now a relay '/f/<token>' instead of the direct link. The
# "FAST CLOUD [FSL]" relay answers the browser (its Turnstile clearance) with 307 to a signed, short-lived link
# '<account>.r2.cloudflarestorage.com/<bucket>/<key>'. The same object is public at 'pub-<id>.r2.dev/<key>' (the link the
# page used to show; probe run 38722349367: The Dark Knight 720p -> account ae105825..., key = the stored pub-10da0f... key).
# The public copy is kept (as before); the account -> pub host pair is learned once and kept in meta.
R2_PUBS = ["eb0dd3d4c4c84462bc3e3f1f8a3f1b61", "8b46bbc58f8c4eb084601251ab7c7f58", "10da0f41c8ea4953ac9ba4a3ec208325",
           "517c71b46fb044a6ad291c50b48f19a7", "882ef609e0d044d2a950ceb655d0a294", "d7567522e12c40939611b17e558f21d5",
           "cffc8b5518bd4251a162a6ad7810bd6b", "4bf6c51d258b4008b0f88bff0bb9d208", "40da3f36e12a4ce7bf7be00b6ed7fa6f",
           "ba5ab25a043c49e5aef446ce41e787c4", "fb1739d1f25f4d65b0a39164a25e1bd0", "ce8a116fe4e349ae967ede26d78b3fda",
           "e1b76cc556b244f6910482717bcc0f00", "1e48ca6945304766a8827c90f3b3a1b2", "ce92b2c18bfc406e927bcd1a98a6cf92"]
SIGNED_RE = re.compile(r"^https://([0-9a-f]{32})\.r2\.cloudflarestorage\.com/([^/?]+)/([^?]+)")


def _range_size(url: str) -> int | None:
    """The object's full size from a 1 KB ranged GET; None when it does not answer 206."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": "bytes=0-1023"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 206:
                return None
            m = re.search(r"/(\d+)$", resp.headers.get("Content-Range") or "")
            return int(m.group(1)) if m else None
    except Exception:  # noqa: BLE001
        return None


def _pub_of_signed(known: str | None, signed: str) -> tuple[str | None, str | None, str]:
    """Signed R2 link -> (public pub-*.r2.dev link of the same object, its pub id, why not). The public copy must answer
    206 with the same full size as the signed one. Runs in a worker thread: no database here."""
    m = SIGNED_RE.match(signed)
    if not m:
        return None, None, "relay: not an R2 link"
    key = m.group(3)
    want = _range_size(signed)
    if not want:
        return None, None, "relay: signed link did not answer"
    for pub in ([known] if known else []) + [p for p in R2_PUBS if p != known]:
        url = f"https://pub-{pub}.r2.dev/{key}"
        if _range_size(url) == want:
            return url, pub, ""
    return None, None, f"relay: no public copy ({m.group(1)[:8]})"


async def _br_fsl(page, conn, btns: list) -> dict | None:
    """Follow the page's FSL relay in the page's own browser context; the public R2 copy, or None."""
    import asyncio
    for b in btns:
        if "FSL" in b["text"] and b["href"].startswith("/f/"):
            r = await page.request.get(BR_HOST + b["href"], max_redirects=0, timeout=30000)
            loc = r.headers.get("location", "")
            if r.status not in (301, 302, 303, 307, 308) or not loc:
                return {"host": "wall", "url": None, "err": f"relay: {r.status}"}
            m = SIGNED_RE.match(loc)
            acct = m.group(1) if m else ""
            k = conn.execute("SELECT v FROM meta WHERE k=?", ("r2pub:" + acct,)).fetchone() if acct else None
            url, pub, why = await asyncio.to_thread(_pub_of_signed, k[0] if k else None, loc)
            if not url:
                return {"host": "wall", "url": None, "err": why}
            if pub and not (k and k[0] == pub):
                write_retry(conn, "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", ("r2pub:" + acct, pub))
            if re.search(r"\.(zip|rar|7z)(\?|$)", url, re.I):
                return {"host": "r2zip", "url": url}
            return {"host": "r2", "url": url}
    return None


def _br_class(btns: list) -> dict:
    out = {"host": "unknown", "url": None}
    for b in btns:
        m = R2_RE.match(b["href"])
        if m:
            return parse_file_page(m.group(0))
    texts = " ".join(b["text"] for b in btns)
    if "V2" in texts and "Instant Download" not in texts:
        out["host"] = "v2"
    elif "Instant Download" in texts or "Cloud [Resumable]" in texts:
        out["host"] = "gd" if "V2" not in texts else "v2"
    return out


def cmd_bresolve(args):
    """Files behind the wall (and, since 2026-10-15, files a plain request read as 'unknown': the page's Turnstile box hid
    the buttons), newest first, through LIB_BROWSER_TABS browser tabs until the deadline or the limit."""
    import asyncio
    conn = db()
    # ONCE (fslfix, 2026-10-15): from ~05:00Z the page's buttons became relays and 720p/1080p files were marked 'v2' (no
    # direct link seen); they are opened again, now through the FSL relay (_br_fsl)
    if not conn.execute("SELECT 1 FROM meta WHERE k='fslfix'").fetchone():
        n = conn.execute("UPDATE files SET host='wall', res_err=NULL WHERE host IN ('v2','gd') AND quality != 480 AND resolved_at >= 1792040400").rowcount
        conn.execute("INSERT INTO meta(k,v) VALUES('fslfix',?)", (str(int(time.time())),))
        conn.commit()
        print(f"[fslfix] {n} files marked 'v2' since the page change are opened again", flush=True)
    # a file that failed in the browser (no buttons, relay error) waits 6 h before it is tried again: before 2026-10-15 the
    # same ~200 dead pages headed every round at 75 s each and the round never reached older files (the 18+ ones)
    rows = conn.execute(
        "SELECT f.fid, f.quality, f.slug FROM files f LEFT JOIN titles t ON t.slug=f.slug "
        "WHERE (f.host='wall' OR (f.res_err IS NOT NULL AND f.res_err LIKE '%403%') "
        "OR (f.host='unknown' AND f.res_err IS NULL AND (length(f.fid) = 8 OR length(f.fid) >= 20))) "
        "AND NOT (f.res_err IS NOT NULL AND f.res_err NOT LIKE '%403%' AND COALESCE(f.resolved_at, 0) > ?) "
        "ORDER BY (COALESCE(t.badges, '') LIKE '%Adult%') DESC, (f.res_err IS NULL) DESC, COALESCE(t.published, '') DESC, f.quality DESC LIMIT ?",
        (int(time.time()) - 6 * 3600, args.limit)).fetchall()
    if not rows:
        print("[browser] nothing behind the wall", flush=True)
        return 0
    by = deadline_of("RESOLVE")
    print(f"[browser] {len(rows)} files behind the wall", flush=True)
    try:
        from patchright.async_api import async_playwright
    except ImportError:
        say("[browser] patchright is not installed; skipped")
        return 0
    stat: dict = {}

    async def run():
        n = 0
        async with async_playwright() as p:
            ctx = await p.chromium.launch_persistent_context(os.environ.get("LIB_BROWSER_PROFILE", "/tmp/lib-prof"), channel="chromium",
                                                             headless=False, no_viewport=True)

            async def block(route):
                await route.abort()
            await ctx.route(BR_ADS, block)
            # LIB_BROWSER_TABS tabs share one profile (one Turnstile clearance), each taking the next file
            tabs = max(1, int(os.environ.get("LIB_BROWSER_TABS", "3")))
            pages = [ctx.pages[0] if ctx.pages else await ctx.new_page()]
            while len(pages) < tabs:
                pages.append(await ctx.new_page())
            todo = list(rows)

            def probe(url):
                req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": "bytes=0-1023"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    m = re.search(r"/(\d+)$", resp.headers.get("Content-Range") or "")
                    size = int(m.group(1)) if m else int(resp.headers.get("Content-Length") or 0)
                return size, voice_start(url)

            async def worker(page, k):
                nonlocal n
                await asyncio.sleep(k * 4)   # stagger the first loads
                while todo:
                    if time.time() > by - 120:
                        return
                    r = todo.pop(0)
                    fid = r["fid"]
                    out = {"host": "unknown", "url": None, "bytes": None, "start_ms": None, "err": None}
                    if r["quality"] == 480:
                        # no 480p file carries the R2 copy (0 of the 13,152 R2 files in the Library, 2026-10-14): not opened
                        write_retry(conn, "UPDATE files SET host='v2', url=NULL, resolved_at=?, res_err=NULL WHERE fid=?", (int(time.time()), fid))
                        stat["v2"] = stat.get("v2", 0) + 1
                        n += 1
                        continue
                    why = ""
                    try:
                        title, btns = await _br_page(page, fid)
                        out.update(_br_class(btns))
                        if btns and out["host"] not in ("r2", "r2zip"):
                            fsl = await _br_fsl(page, conn, btns)
                            if fsl:
                                out.update(fsl)
                                if out["host"] == "wall":
                                    why = "relay"
                        if not btns and "link expired" in title.lower():
                            # the code is a day old: the title page is read again for fresh codes (store_title keeps
                            # what its other files already resolved)
                            why = "expired"
                            out["host"], out["err"] = "expired", "link expired"
                            write_retry(conn, "UPDATE titles SET fetched_at=NULL WHERE slug=?", (r["slug"],))
                        elif not btns:
                            why = "moment" if "moment" in title.lower() else ("notfound" if "not found" in title.lower() or not title else "nobuttons")
                            out["host"], out["err"] = "wall", f"no buttons ({why})"
                        if out["host"] == "r2":
                            out["bytes"], out["start_ms"] = await asyncio.to_thread(probe, out["url"])
                    except Exception as e:  # noqa: BLE001
                        why = "exc-" + type(e).__name__
                        out["host"], out["err"] = "wall", str(e)[:200]
                    if out["host"] == "unknown" and not out["err"]:
                        # the browser saw the buttons but none it knows: marked, so this pass does not take it again every run
                        out["err"] = "browser: no known button"
                    key = out["host"] + (f":{why}" if out["host"] == "wall" else "")
                    stat[key] = stat.get(key, 0) + 1
                    write_retry(conn, "UPDATE files SET host=?, url=?, bytes=?, start_ms=?, resolved_at=?, res_err=? WHERE fid=?",
                                (out["host"], out["url"], out["bytes"], out["start_ms"], int(time.time()), out["err"], fid))
                    n += 1
                    if n % 10 == 0:
                        conn.commit()
                        print(f"[browser] {n}/{len(rows)} {json.dumps(stat, sort_keys=True)}", flush=True)

            await asyncio.gather(*(worker(pg, k) for k, pg in enumerate(pages)))
            if todo:
                say(f"[browser] time budget reached at {n}/{len(rows)}")
            await ctx.close()
        conn.commit()
        return n
    n = asyncio.run(run())
    print(f"[browser] done {n} {json.dumps(stat, sort_keys=True)}", flush=True)
    return n


def cmd_resolve(args):
    conn = db()
    rows = conn.execute(
        "SELECT f.fid, f.watch, f.url FROM files f LEFT JOIN titles t ON t.slug=f.slug "
        "WHERE (length(f.fid) = 8 OR length(f.fid) >= 20) AND (f.resolved_at IS NULL OR (f.host IN ('unknown') AND f.resolved_at < ?) OR (f.res_err IS NOT NULL AND f.resolved_at < ?)) "
        "ORDER BY (f.res_err='dead' AND f.resolved_at=0) DESC, COALESCE(t.published, '') DESC, f.watch DESC, f.rowid DESC LIMIT ?",
        (int(time.time()) - 86400, int(time.time()) - 6 * 3600, args.limit)).fetchall()
    by = deadline_of("RESOLVE")
    print(f"[resolve] {len(rows)} files", flush=True)
    n, pending = 0, []
    # 2026-10-14: the file host shows Turnstile to every plain request. Five files are tried first; when all five
    # hit the wall, the rest go straight to the browser pass instead of costing one request each.
    if len(rows) > 5:
        probe = [resolve_one(r["fid"], r["watch"]) for r in rows[:5]]
        # 2026-10-15: through a proxy lane the page loads, but its own Turnstile box hides the buttons, so the file reads as
        # 'unknown' (2,930 such files, 0 resolved by a plain request since 2026-10-14 15:00): that is the wall too
        if all(o["host"] in ("wall", "unknown") and not o.get("url") for _, o in probe):
            now = int(time.time())
            for i in range(30):
                try:
                    was_dead = {(r["fid"]) for r in rows if r.get("url") and any(r["url"].startswith(h) for h in DEAD_R2_PREFIXES)}
                    conn.executemany("UPDATE files SET host='wall', url=NULL, resolved_at=?, res_err=NULL WHERE fid=?", [(now, r["fid"]) for r in rows])
                    conn.executemany("UPDATE files SET res_err='dead' WHERE fid=?", [(f,) for f in was_dead])
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e) or i == 29:
                        raise
                    conn.rollback()
                    time.sleep(1 + i * 0.5)
            print(f"[resolve] the wall is up: {len(rows)} files wait for the browser pass", flush=True)
            return len(rows)

    def flush():
        # one short transaction per batch: the crawl may hold the file most of the time
        for i in range(60):
            try:
                conn.executemany("UPDATE files SET host=?, url=?, sub_url=?, bytes=?, resolved_at=?, res_err=?, start_ms=? WHERE fid=?", pending)
                conn.commit()
                pending.clear()
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e) or i == 59:
                    raise
                conn.rollback()
                time.sleep(1)

    ex = ThreadPoolExecutor(max_workers=RCONC)
    try:
        futs = [ex.submit(resolve_one, r["fid"], r["watch"]) for r in rows]
        for fut in as_completed(futs):
            if time.time() > by:
                say(f"[resolve] time budget reached at {n}/{len(rows)}")
                for f in futs: f.cancel()
                break
            fid, out = fut.result()
            pending.append((out["host"], out["url"], out["sub_url"], out["bytes"], int(time.time()), out["err"], out["start_ms"], fid))
            n += 1
            if len(pending) >= 25:
                flush()
                print(f"[resolve] {n}/{len(rows)}", flush=True)
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    flush()
    print(f"[resolve] done {n}", flush=True)
    return n



# ---------------------------------------------------------------- what goes out (2026-10-12 audit)
# Fixes applied when a title is pushed, from what the crawler already holds, so no page is read again for them.
# REFINE_V bumps re-send, once, only the titles whose payload these rules change (see cmd_push).
REFINE_V = 2   # 2 (2026-10-15): Hoichoi/Chorki card badges added to the platforms (~146 titles go out again)

# Files whose picture was checked by eye and is another work (frames at 15:00 compared, 2026-10-12): the page links a
# file of a different title. Dropped from the payload; the title's other files stay.
WRONG_FILES = {
    "56a2e468": "Pil's Adventures page, file is Mars Needs Moms (2011)",
    "7bb9e29e": "Koi Sath Hai page, file is Krantiveer (1994)",
    "5309b1f8": "Neymar page, file is Fyakra (2025, Bengali series)",
}
# Pages whose every file is another work: the post is broken at the source (a film page carrying a series' files).
WRONG_PAGES = {
    "ab-tak-chhappan-2-2015-full-movie-download": "film page, files are the series Adarsh Baal Vidyalaya (2026)",
}


def other_name(title, card_title):
    """(display title, aka) from the post's headline "Nilar Baba - Deiva Thirumagal (2026) Bengali Dubbed ..." when the
    page's own name is one half of it; (title, None) otherwise."""
    name = _named(card_title or "")
    if not name or not title:
        return title, None
    nt = _norm(title)
    if not nt or _norm(name) == nt:
        return title, None
    m = re.match(r"^(.+?)\s+[-–]\s+(.+)$", name)
    if m:
        a, b = m.group(1).strip(), m.group(2).strip()
        if _norm(b) == nt and _norm(a) and not re.fullmatch(r"\d{4}", _norm(a)):
            return a, title                   # "Dub name - Original": the dub name is what people search for
        if _norm(a) == nt and _norm(b) and not re.fullmatch(r"\d{4}", _norm(b)):
            return title, b
    m = re.match(r"^(.+?):\s+(.+)$", name)
    if m:
        a, b = m.group(1).strip(), m.group(2).strip()
        if _norm(b) == nt or _norm(a) == nt:
            other = a if _norm(b) == nt else b
            if len(_norm(other)) >= 2:
                return title, other
    return title, None


FILE_EP_RE = re.compile(r"(?<![A-Za-z0-9])[Ss](\d{1,2})[ ._-]?[Ee][Pp]?(\d{1,4})(?:\s*[-–_ ]\s*[Ee]?[Pp]?(\d{1,4}))?(?![0-9])")


def file_episodes(url: str | None) -> tuple[int, int] | None:
    """The episode range a file's own name gives ("Reacher S04E05", "Vikings Valhalla S03E05-08"), or None."""
    if not url:
        return None
    name = urllib.parse.unquote(url.rsplit("/", 1)[-1])
    m = FILE_EP_RE.search(name)
    if not m:
        return None
    a = int(m.group(2))
    b = int(m.group(3)) if m.group(3) else a
    if a <= 0 or b < a or b - a > 60:
        return None
    return a, b


PLATFORM_BADGES = ("Hoichoi", "Chorki")


def refine(t: dict, files: list) -> tuple[str, str | None, list, dict]:
    """(title, aka, files, what changed) for one title about to be pushed."""
    changed: dict = {}
    title, aka = other_name(t["title"], t.get("card_title"))
    if aka:
        changed["name"] = [title, aka]
    # THE PLATFORM THE SOURCE FILES A POST UNDER (2026-10-15, owner: platform pages "ফুলফিলভাবে ও সঠিকভাবে"). The site
    # lists its Hoichoi and Chorki posts in categories of their own (the card's badge) and names the platform on the page
    # only sometimes: 178 titles carried the badge but not the page's platform (140 Hoichoi, so Hoichoi showed 126 of
    # its ~266). The badge is the source's own filing, so it is added to the page's platforms, never replacing them.
    have = {x.strip().lower() for x in (t.get("ott") or "").split(",") if x.strip()}
    extra = [b for b in PLATFORM_BADGES if b in {x.strip() for x in (t.get("badges") or "").split(",")} and b.lower() not in have]
    if extra:
        t["ott"] = ",".join([x.strip() for x in (t.get("ott") or "").split(",") if x.strip()] + extra)
        changed["ott"] = extra
    out = []
    if t["slug"] in WRONG_PAGES:
        changed["wrong_page"] = 1
        files = []
    for f in files:
        if f["fid"] in WRONG_FILES:
            changed.setdefault("dropped", []).append(f["fid"])
            continue
        if f.get("season") is not None and f.get("host") == "r2":
            # The page's episode number is typed by hand; the file's own name is what the file is
            # (Reacher S4 "E6" played E5; Vikings: Valhalla S3 E1-4 and E5-8 swapped; Vikram on Duty 4 off).
            fe = file_episodes(f.get("url"))
            # A name with one number on a page range ("Part-Timers [S01E21]" for 21-24) names only where a pack
            # starts: measured, those files run 95-118 min, i.e. the whole range. The page stands then.
            if fe and fe[0] == fe[1] and (f.get("ep_to") or 0) > (f.get("ep_from") or 0):
                fe = None
            if fe and fe != (f.get("ep_from"), f.get("ep_to")):
                f = {**f, "ep_from": fe[0], "ep_to": fe[1]}
                changed.setdefault("eps", []).append([f["fid"], fe[0], fe[1]])
        out.append(f)
    return title, aka, out, changed

# ---------------------------------------------------------------- push

def title_payload(conn, slug: str) -> dict:
    t = dict(conn.execute("SELECT * FROM titles WHERE slug=?", (slug,)).fetchone())
    eps = [dict(r) for r in conn.execute("SELECT season, ep, name, aired, runtime, lang FROM episodes WHERE slug=? ORDER BY season, ep", (slug,))]
    files = [dict(r) for r in conn.execute("SELECT fid, season, ep_from, ep_to, quality, hevc, size_mb, label, lang, watch, host, url, sub_url, bytes, COALESCE(start_ms, 0) start_ms FROM files WHERE slug=? ORDER BY season, ep_from, quality", (slug,))]
    title, aka, files, changed = refine(t, files)
    # the worker writes a title only when this hash moves: a refinement adds its own short digest, so a refined title
    # is written once and an untouched one keeps the exact hash it had (no rewrite of the other ~10,000)
    h = t["hash"] + ("~" + hashlib.sha1(json.dumps(changed, sort_keys=True).encode()).hexdigest()[:8] if changed else "")
    return {
        "num": LIB_BASE_NUM + (t["post_id"] or 0), "slug": slug, "type": t["type"], "title": title, "aka": aka, "year": t["year"], "_base_hash": t["hash"],
        "poster": t["poster"], "overview": t["overview"], "genres": t["genres"], "language": t["language"], "country": t["country"],
        "rating": t["rating"], "releaseDate": t["release_date"], "runtime": t["runtime"], "tmdbId": t["tmdb_id"], "imdbId": t["imdb_id"], "status": t["status"],
        "cats": t["cats"], "ott": t["ott"], "badges": t["badges"], "published": t["published"], "modified": t["modified"], "hash": h, "episodes": eps, "files": files,
    }


PLAYABLE_HOSTS = ("r2", "gu")


def resend_refined(conn) -> None:
    """Once per REFINE_V: titles already pushed whose payload the refine rules change go out again (counts only)."""
    have = int((conn.execute("SELECT v FROM meta WHERE k='refine_v'").fetchone() or [0])[0])
    if have >= REFINE_V:
        return
    n = 0
    kinds: dict = {}
    for r in conn.execute("SELECT slug FROM titles WHERE pushed_hash IS NOT NULL").fetchall():
        p = title_payload(conn, r["slug"])
        if p["hash"] != p["_base_hash"]:
            changed = refine(dict(conn.execute("SELECT * FROM titles WHERE slug=?", (r["slug"],)).fetchone()),
                             [dict(x) for x in conn.execute("SELECT fid, season, ep_from, ep_to, host, url FROM files WHERE slug=?", (r["slug"],))])[3]
            for k in changed:
                kinds[k] = kinds.get(k, 0) + 1
            conn.execute("UPDATE titles SET pushed_hash=NULL WHERE slug=?", (r["slug"],))
            n += 1
    conn.execute("INSERT INTO meta(k,v) VALUES('refine_v',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(REFINE_V),))
    conn.commit()
    print(f"[refine] v{REFINE_V}: {n} titles go out again {json.dumps(kinds, sort_keys=True)}", flush=True)


def cmd_push(args):
    conn = db()
    base = (args.base or os.environ.get("LIB_BASE") or "").rstrip("/")
    key = args.key or os.environ.get("LIB_KEY") or ""
    if not base or not key:
        sys.exit("need --base and --key (or LIB_BASE / LIB_KEY)")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    resend_refined(conn)
    spent = int((conn.execute("SELECT v FROM meta WHERE k=?", (f"rows:{day}",)).fetchone() or [0])[0])
    budget = DAILY_ROWS - spent
    if budget <= 0:
        say(f"[push] today's D1 write budget is used ({spent} est. rows); next push after 00:00 UTC")
        return 0
    # A title goes out once every file of it has been resolved (one write per title instead of two; a file the browser
    # has not opened yet counts as unresolved, 2026-10-14), and only
    # when it can be played, or when the worker already holds it (so a title that lost its files is updated).
    # Playable and newest first, so the titles users can watch land first when the day's budget runs out.
    rows = conn.execute(
        "SELECT t.slug, (SELECT COUNT(*) FROM files f WHERE f.slug=t.slug) nf, (SELECT COUNT(*) FROM episodes e WHERE e.slug=t.slug) ne, "
        "EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.host IN ('r2','gu')) pl FROM titles t "
        "WHERE t.post_id IS NOT NULL AND t.title IS NOT NULL "
        "AND NOT EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND (f.resolved_at IS NULL OR (f.host='wall' AND f.res_err IS NULL))) "
        "AND (t.pushed_hash IS NULL OR t.pushed_hash != t.hash || ':' || COALESCE((SELECT MAX(resolved_at) FROM files f WHERE f.slug=t.slug), 0)) "
        "AND (t.pushed_hash IS NOT NULL OR EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.host IN ('r2','gu'))) "
        "ORDER BY pl DESC, COALESCE(t.published,'') DESC LIMIT ?", (args.limit,)).fetchall()
    print(f"[push] {len(rows)} titles waiting; budget {budget} est. rows today")
    batch, sent, used = [], 0, 0

    def flush():
        nonlocal batch, sent, used
        if not batch:
            return
        body = json.dumps({"titles": [{k: v for k, v in b[0].items() if not k.startswith("_")} for b in batch]}).encode()
        req = urllib.request.Request(base + "/ingest", data=body, method="POST", headers={"content-type": "application/json", "x-lib-key": key, "user-agent": "MovieHouse-LibIndex/2"})
        for i in range(4):
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    res = json.loads(r.read().decode())
                break
            except Exception as e:  # noqa: BLE001
                if i == 3:
                    raise
                time.sleep(5 * (i + 1))
        if not res.get("ok"):
            raise RuntimeError(f"ingest: {str(res)[:300]}")
        for p, cost in batch:
            mx = conn.execute("SELECT COALESCE(MAX(resolved_at),0) FROM files WHERE slug=?", (p["slug"],)).fetchone()[0]
            write_retry(conn, "UPDATE titles SET pushed_hash=? WHERE slug=?", (f"{p['_base_hash']}:{mx}", p["slug"]))
        # the worker's own count of rows it wrote, times what D1 bills per such row (ROW_FACTOR)
        used += max(int(int(res.get("written") or 0) * ROW_FACTOR), len(batch))
        conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (f"rows:{day}", str(spent + used)))
        conn.commit()
        sent += len(batch)
        print(f"[push] {sent}/{len(rows)} written={res.get('written')} est.rows today={spent + used}", flush=True)
        batch = []

    for r in rows:
        cost = int(ROW_FACTOR * (2 + r["nf"] + r["ne"]))
        if used + sum(c for _, c in batch) + cost > budget:
            break
        batch.append((title_payload(conn, r["slug"]), cost))
        if len(batch) >= args.batch:
            flush()
    flush()
    if sent < len(rows):
        say(f"[push] stopped at today's budget: {sent}/{len(rows)}; the rest goes tomorrow (00:00 UTC)")
    print(f"[push] done {sent}")
    return sent


def cmd_skipcheck(args):
    """Files resolved before voice_start existed: check them, newest first, until the deadline or the limit.
    A file found with the advert gets a fresh resolved_at so its title goes out again; 0 needs no push (the default)."""
    conn = db()
    rows = conn.execute(
        "SELECT f.fid, f.url FROM files f LEFT JOIN titles t ON t.slug=f.slug WHERE f.host='r2' AND f.url IS NOT NULL AND f.start_ms IS NULL "
        "ORDER BY COALESCE(t.published,'') DESC LIMIT ?", (args.limit,)).fetchall()
    by = min(deadline_of("RESOLVE"), time.time() + float(os.environ.get("LIB_SKIPCHECK_SECONDS", "900")))
    print(f"[skip] {len(rows)} files to check", flush=True)
    found = done = 0
    ex = ThreadPoolExecutor(max_workers=RCONC)
    try:
        futs = {ex.submit(voice_start, r["url"]): r["fid"] for r in rows}
        for fut in as_completed(futs):
            if time.time() > by:
                for f in futs: f.cancel()
                break
            ms = fut.result()
            if ms is None:
                continue
            fid = futs[fut]
            if ms > 0:
                found += 1
                write_retry(conn, "UPDATE files SET start_ms=?, resolved_at=? WHERE fid=?", (ms, int(time.time()), fid))
            else:
                write_retry(conn, "UPDATE files SET start_ms=0 WHERE fid=?", (fid,))
            done += 1
            if done % 50 == 0:
                conn.commit()
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    conn.commit()
    print(f"[skip] checked {done}, spoken advert in {found}", flush=True)


DEAD_R2_PREFIXES = [h.strip() for h in os.environ.get("LIB_DEAD_R2", "").split(",") if h.strip()]
# measured 2026-10-25: pub-eb0dd3d4... (2,765 files) and pub-8b46bbc5... (1,425 files) answer 403
# Forbidden (their r2.dev subdomain is gone); every other stored bucket answers 206.
if not DEAD_R2_PREFIXES:
    DEAD_R2_PREFIXES = ["https://pub-eb0dd3d4c4c84462bc3e3f1f8a3f", "https://pub-8b46bbc58f8c4eb084601251ab7c"]


def cmd_recheck(args):
    """The stored direct links rot (owner, 2026-10-25).
    =============================================================================
    *"cinefreak ও movielinkbd এরগুলোর অনেকগুলোই চলে না... যেগুলো আগে থেকেই এসে ছিলো"*. Live
    2026-10-25: a 45-title random play sample through the gateway answered 6 ok / 35 no_server /
    4 no_file, and the newest 20 titles answered 5 ok / 15 no_server — while the file host itself
    was healthy (fsldownload.com 200; a known-good stored pair still ranged 206 on a 1.88 GB and a
    3.68 GB MKV). So the index stores links that die: the host deletes old uploads and issues fresh
    file codes on every page re-read, and nothing ever looked at a link again after resolve proved
    it once. The gateway can only prove what the index stores, so a dead stored link plays as
    "no server" for weeks.

    The cure: one cheap ranged GET per stored link, oldest resolved first; a healthy link gets its
    resolved_at renewed (so coverage keeps rotating through the whole index), a dead one is marked
    res_err='dead' with resolved_at=0 so this run's resolve re-resolves it — the browser pass
    covers the Turnstile wall, and a genuinely deleted file falls out of play honestly instead of
    serving a dead link. Bumping resolved_at also changes a title's push hash, so freshly
    re-proven titles are re-pushed and the library API stops promising what cannot play.
    """
    conn = db()
    for h in DEAD_R2_PREFIXES:
        ids = [r[0] for r in conn.execute(
            "SELECT fid FROM files WHERE host IN ('r2','r2zip') AND url LIKE ? AND NOT (res_err='dead' AND resolved_at=0)",
            (h + "%",)).fetchall()]
        if ids:
            conn.executemany("UPDATE files SET resolved_at=0, res_err='dead' WHERE fid=?", [(i,) for i in ids])
            conn.commit()
            print(f"[recheck] {len(ids)} links on the dead host {h[:14]}... go to the head of the resolve queue", flush=True)
    rows = conn.execute(
        "SELECT f.fid, f.url FROM files f WHERE f.host IN ('r2','r2zip') AND f.url IS NOT NULL "
        "ORDER BY COALESCE(f.resolved_at, 0) ASC LIMIT ?", (args.limit,)).fetchall()
    by = min(deadline_of("RESOLVE"), time.time() + float(os.environ.get("LIB_RECHECK_SECONDS", "240")))
    print(f"[recheck] {len(rows)} stored links, oldest resolved first", flush=True)
    alive = dead = 0
    ex = ThreadPoolExecutor(max_workers=RCONC)
    pending: list = []

    def flush():
        nonlocal pending
        for i in range(30):
            try:
                conn.executemany("UPDATE files SET resolved_at=?, res_err=? WHERE fid=?", pending)
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e) or i == 29:
                    raise
                conn.rollback()
                time.sleep(1 + i * 0.5)
        pending = []

    try:
        futs = {ex.submit(_range_size, r["url"]): r["fid"] for r in rows}
        for fut in as_completed(futs):
            if time.time() > by:
                for f in futs: f.cancel()
                break
            fid = futs[fut]
            if fut.result() is not None:
                alive += 1
                pending.append((int(time.time()), None, fid))   # res_err cleared: proven again now
            else:
                dead += 1
                pending.append((0, "dead", fid))                # resolved_at=0 + res_err: resolve re-picks it this run
            if len(pending) >= 100:
                flush()
        flush()
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    conn.commit()
    print(f"[recheck] alive {alive}, dead {dead}", flush=True)
    return dead


def cmd_cycle(args):
    """One scheduled run: crawl what changed, then resolve and push in rounds until the time budget is spent."""
    cmd_crawl(args)
    args.limit = 100000
    cmd_skipcheck(args)
    cmd_recheck(args)
    by = deadline_of("RESOLVE")
    # owner 2026-10-25: don't stop at the first short round — keep resolving until the
    # run's time budget is spent (the recheck keeps surfacing dead links as it rotates).
    while time.time() < by - 60:
        args.limit = args.round
        n = cmd_resolve(args)
        args.limit = 100000
        cmd_push(args)
        if n == 0:
            break
    # 2026-10-14: files behind the Turnstile wall go through the browser (LIB_BROWSER_TABS tabs), in rounds of LIB_BROWSER_ROUND files,
    # each round pushed at once (a run that is stopped keeps what it found)
    if os.environ.get("LIB_BROWSER") == "1":
        rnd = int(os.environ.get("LIB_BROWSER_ROUND", "300"))
        while time.time() < by - 300:
            n = cmd_bresolve(argparse.Namespace(limit=rnd))
            args.limit = 100000
            cmd_push(args)
            if n < rnd:
                break


def cmd_stats(args):
    conn = db()
    q = lambda s: conn.execute(s).fetchone()[0]  # noqa: E731
    print("titles", q("SELECT COUNT(*) FROM titles"), "fetched", q("SELECT COUNT(*) FROM titles WHERE fetched_at>0"), "errors", q("SELECT COUNT(*) FROM titles WHERE err IS NOT NULL"))
    print("movies", q("SELECT COUNT(*) FROM titles WHERE type='movie'"), "series", q("SELECT COUNT(*) FROM titles WHERE type='tv'"), "episodes", q("SELECT COUNT(*) FROM episodes"))
    print("files", q("SELECT COUNT(*) FROM files"), "resolved", q("SELECT COUNT(*) FROM files WHERE resolved_at IS NOT NULL"))
    print("titles playable (r2 or gu)", q("SELECT COUNT(DISTINCT slug) FROM files WHERE host IN ('r2','gu')"), "with r2", q("SELECT COUNT(DISTINCT slug) FROM files WHERE host='r2'"),
          "pushed", q("SELECT COUNT(*) FROM titles WHERE pushed_hash IS NOT NULL"))
    for r in conn.execute("SELECT host, COUNT(*) n FROM files GROUP BY host"):
        print("  host", r["host"], r["n"])
    # why a post is not in the app (counts only): no file link at all, only Drive files the host gives no direct
    # copy of, files not resolved yet, or the page failed to read
    print("not in the app: no files", q("SELECT COUNT(*) FROM titles t WHERE fetched_at>0 AND NOT EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug)"),
          "| only unplayable hosts", q("SELECT COUNT(*) FROM titles t WHERE EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug) AND NOT EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND (f.host IN ('r2','gu') OR f.resolved_at IS NULL))"),
          "| waiting to resolve", q("SELECT COUNT(DISTINCT slug) FROM files WHERE resolved_at IS NULL"),
          "| page errors", q("SELECT COUNT(*) FROM titles WHERE err IS NOT NULL"))
    for r in conn.execute("SELECT year, COUNT(*) n, SUM(CASE WHEN EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.host='r2') THEN 1 ELSE 0 END) r2 FROM titles t WHERE fetched_at>0 GROUP BY year ORDER BY year DESC LIMIT 12"):
        print("  year", r["year"], r["n"], "with r2:", r["r2"])


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("crawl")
    c.add_argument("--full", action="store_true")
    c.add_argument("--cats")
    c.add_argument("--slugs")
    c.add_argument("--max-pages", type=int)
    c.add_argument("--recent-hours", type=float, default=3.0)
    c.add_argument("--series-recheck-hours", type=float, default=12.0)
    c.add_argument("--resolve", action="store_true")
    c.add_argument("--titles-only", action="store_true", help="skip the category walk; fetch the title pages not yet read")
    c.add_argument("--limit", type=int, default=100000)
    c.set_defaults(fn=cmd_crawl)
    r = sub.add_parser("resolve")
    r.add_argument("--limit", type=int, default=100000)
    r.set_defaults(fn=cmd_resolve)
    k = sub.add_parser("skipcheck")
    k.add_argument("--limit", type=int, default=100000)
    k.set_defaults(fn=cmd_skipcheck)
    r2 = sub.add_parser("recheck")
    r2.add_argument("--limit", type=int, default=100000)
    r2.set_defaults(fn=cmd_recheck)
    p = sub.add_parser("push")
    p.add_argument("--base")
    p.add_argument("--key")
    p.add_argument("--limit", type=int, default=100000)
    p.add_argument("--batch", type=int, default=25)
    p.set_defaults(fn=cmd_push)
    y = sub.add_parser("cycle")
    for a_, kw in (("--full", {"action": "store_true"}), ("--cats", {}), ("--slugs", {}), ("--max-pages", {"type": int}),
                   ("--recent-hours", {"type": float, "default": 3.0}), ("--series-recheck-hours", {"type": float, "default": 12.0}),
                   ("--titles-only", {"action": "store_true"}), ("--base", {}), ("--key", {}), ("--batch", {"type": int, "default": 25}),
                   ("--round", {"type": int, "default": 1500})):
        y.add_argument(a_, **kw)
    y.set_defaults(fn=cmd_cycle, resolve=False, limit=100000)
    b = sub.add_parser("bresolve")
    b.add_argument("--limit", type=int, default=100000)
    b.set_defaults(fn=cmd_bresolve)
    s = sub.add_parser("stats")
    s.set_defaults(fn=cmd_stats)
    args = ap.parse_args()
    if args.cmd in ("crawl", "resolve", "cycle") and len(_LANES) > 1:
        probe_lanes()
    args.fn(args)


if __name__ == "__main__":
    main()
