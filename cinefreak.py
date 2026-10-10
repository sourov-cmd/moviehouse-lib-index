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
import itertools
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
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
DAILY_ROWS = int(os.environ.get("LIB_DAILY_ROWS", "90000"))   # D1 free plan: 100k rows written/day; estimate is ~1.15x real, so ~78k real


def deadline_of(name: str) -> float:
    """Epoch seconds from env (LIB_<name>_BY), or never."""
    v = os.environ.get(f"LIB_{name}_BY")
    return float(v) if v else float("inf")


def say(msg: str, private: str = "") -> None:
    print(msg if QUIET or not private else f"{msg} {private}", flush=True)
LIB_BASE_NUM = 2_000_000_000

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


def _slow_down(lane: int):
    """A 429 from the site: every request on that lane sleeps a while before going out (shared across threads)."""
    p = _PACE[lane]
    p["sleep"] = min(5.0, (p["sleep"] or 0.5) * 2)
    p["until"] = time.time() + 120


def fetch(url: str, timeout: int = 40, tries: int = 4) -> str:
    last = None
    lane = next(_TURN) % len(_LANES)
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
            if code == 429:
                _slow_down(lane)
                time.sleep(8 * (i + 1))
                lane = next(_TURN) % len(_LANES)
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
    return c


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


_FID = re.compile(r"cinecloud\.site/([fxwd])/([a-z0-9]+)")


def _fid(href: str | None) -> str | None:
    m = _FID.search(href or "")
    return m.group(2) if m else None


def _size_mb(text: str) -> float | None:
    m = re.search(r"\[?\s*([\d.]+)\s*(GB|MB)\s*\]?", text or "", re.I)
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
    poster = None
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
    else:
        for h4 in soup.select(".download-links-div h4.movie-title"):
            label = h4.get_text(" ", strip=True)
            box = h4.find_next_sibling("div")
            if not box:
                continue
            dl = box.select_one("a.dlbtn-download")
            w = box.select_one("a.dlbtn-watch")
            fid = _fid(dl.get("href") if dl else None)
            if not fid:
                continue
            out["files"].append({
                "fid": fid, "season": None, "ep_from": None, "ep_to": None, "quality": _quality(label),
                "hevc": 1 if re.search(r"hevc|x265|10\s*bit", label, re.I) else 0, "size_mb": _size_mb(label),
                "label": label, "lang": _lang_tag(label), "watch": 1 if (w and _fid(w.get("href")) == fid) else 0,
            })
    return out


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
    conn.execute("DELETE FROM files WHERE slug=? AND fid NOT IN (%s)" % ",".join("?" * len(seen)) if seen else "DELETE FROM files WHERE slug=?",
                 (t["slug"], *seen) if seen else (t["slug"],))
    for f in t["files"]:
        conn.execute(
            "INSERT INTO files(fid, slug, season, ep_from, ep_to, quality, hevc, size_mb, label, lang, watch) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(fid) DO UPDATE SET slug=excluded.slug, season=excluded.season, ep_from=excluded.ep_from, ep_to=excluded.ep_to, quality=excluded.quality, "
            "hevc=excluded.hevc, size_mb=excluded.size_mb, label=excluded.label, lang=excluded.lang, watch=excluded.watch",
            (f["fid"], t["slug"], f["season"], f["ep_from"], f["ep_to"], f["quality"], f["hevc"], f["size_mb"], f["label"], f["lang"], f["watch"]),
        )
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
    for r in conn.execute("SELECT slug FROM titles WHERE fetched_at IS NULL OR (err IS NOT NULL AND fetched_at < ?) ORDER BY rowid DESC", (int(time.time()) - 6 * 3600,)):
        if r["slug"] not in known:
            slugs.append(r["slug"]); known.add(r["slug"])
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
    out = {"host": None, "url": None, "sub_url": None, "bytes": None, "err": None}
    try:
        f = parse_file_page(fetch(f"https://new5.cinecloud.site/f/{fid}"))
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
    except Exception as e:  # noqa: BLE001
        out["err"] = str(e)[:200]
    return fid, out


def cmd_resolve(args):
    conn = db()
    rows = conn.execute(
        "SELECT f.fid, f.watch FROM files f LEFT JOIN titles t ON t.slug=f.slug "
        "WHERE f.resolved_at IS NULL OR (f.host IN ('unknown') AND f.resolved_at < ?) OR (f.res_err IS NOT NULL AND f.resolved_at < ?) "
        "ORDER BY COALESCE(t.published, '') DESC, f.watch DESC, f.rowid DESC LIMIT ?",
        (int(time.time()) - 86400, int(time.time()) - 6 * 3600, args.limit)).fetchall()
    by = deadline_of("RESOLVE")
    print(f"[resolve] {len(rows)} files", flush=True)
    n, pending = 0, []

    def flush():
        # one short transaction per batch: the crawl may hold the file most of the time
        for i in range(60):
            try:
                conn.executemany("UPDATE files SET host=?, url=?, sub_url=?, bytes=?, resolved_at=?, res_err=? WHERE fid=?", pending)
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
            pending.append((out["host"], out["url"], out["sub_url"], out["bytes"], int(time.time()), out["err"], fid))
            n += 1
            if len(pending) >= 25:
                flush()
                print(f"[resolve] {n}/{len(rows)}", flush=True)
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    flush()
    print(f"[resolve] done {n}", flush=True)
    return n


# ---------------------------------------------------------------- push

def title_payload(conn, slug: str) -> dict:
    t = dict(conn.execute("SELECT * FROM titles WHERE slug=?", (slug,)).fetchone())
    eps = [dict(r) for r in conn.execute("SELECT season, ep, name, aired, runtime, lang FROM episodes WHERE slug=? ORDER BY season, ep", (slug,))]
    files = [dict(r) for r in conn.execute("SELECT fid, season, ep_from, ep_to, quality, hevc, size_mb, label, lang, watch, host, url, sub_url, bytes FROM files WHERE slug=? ORDER BY season, ep_from, quality", (slug,))]
    return {
        "num": LIB_BASE_NUM + (t["post_id"] or 0), "slug": slug, "type": t["type"], "title": t["title"], "year": t["year"],
        "poster": t["poster"], "overview": t["overview"], "genres": t["genres"], "language": t["language"], "country": t["country"],
        "rating": t["rating"], "releaseDate": t["release_date"], "runtime": t["runtime"], "tmdbId": t["tmdb_id"], "imdbId": t["imdb_id"], "status": t["status"],
        "cats": t["cats"], "ott": t["ott"], "badges": t["badges"], "published": t["published"], "modified": t["modified"], "hash": t["hash"], "episodes": eps, "files": files,
    }


PLAYABLE_HOSTS = ("r2", "gu")


def cmd_push(args):
    conn = db()
    base = (args.base or os.environ.get("LIB_BASE") or "").rstrip("/")
    key = args.key or os.environ.get("LIB_KEY") or ""
    if not base or not key:
        sys.exit("need --base and --key (or LIB_BASE / LIB_KEY)")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    spent = int((conn.execute("SELECT v FROM meta WHERE k=?", (f"rows:{day}",)).fetchone() or [0])[0])
    budget = DAILY_ROWS - spent
    if budget <= 0:
        say(f"[push] today's D1 write budget is used ({spent} est. rows); next push after 00:00 UTC")
        return 0
    # A title goes out once every file of it has been resolved (one write per title instead of two), and only
    # when it can be played, or when the worker already holds it (so a title that lost its files is updated).
    # Playable and newest first, so the titles users can watch land first when the day's budget runs out.
    rows = conn.execute(
        "SELECT t.slug, (SELECT COUNT(*) FROM files f WHERE f.slug=t.slug) nf, (SELECT COUNT(*) FROM episodes e WHERE e.slug=t.slug) ne, "
        "EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.host IN ('r2','gu')) pl FROM titles t "
        "WHERE t.post_id IS NOT NULL AND t.title IS NOT NULL "
        "AND NOT EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.resolved_at IS NULL) "
        "AND (t.pushed_hash IS NULL OR t.pushed_hash != t.hash || ':' || COALESCE((SELECT MAX(resolved_at) FROM files f WHERE f.slug=t.slug), 0)) "
        "AND (t.pushed_hash IS NOT NULL OR EXISTS(SELECT 1 FROM files f WHERE f.slug=t.slug AND f.host IN ('r2','gu'))) "
        "ORDER BY pl DESC, COALESCE(t.published,'') DESC LIMIT ?", (args.limit,)).fetchall()
    print(f"[push] {len(rows)} titles waiting; budget {budget} est. rows today")
    batch, sent, used = [], 0, 0

    def flush():
        nonlocal batch, sent, used
        if not batch:
            return
        body = json.dumps({"titles": [b[0] for b in batch]}).encode()
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
            write_retry(conn, "UPDATE titles SET pushed_hash=? WHERE slug=?", (f"{p['hash']}:{mx}", p["slug"]))
        # the worker's own count of rows it wrote; D1 billed 1.5x that (indexes, search table), measured 2026-10-10
        used += max(int(int(res.get("written") or 0) * 1.7), len(batch))
        conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (f"rows:{day}", str(spent + used)))
        conn.commit()
        sent += len(batch)
        print(f"[push] {sent}/{len(rows)} written={res.get('written')} est.rows today={spent + used}", flush=True)
        batch = []

    for r in rows:
        cost = int(1.7 * (2 + r["nf"] + r["ne"]))
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


def cmd_cycle(args):
    """One scheduled run: crawl what changed, then resolve and push in rounds until the time budget is spent."""
    cmd_crawl(args)
    by = deadline_of("RESOLVE")
    while time.time() < by - 60:
        args.limit = args.round
        n = cmd_resolve(args)
        args.limit = 100000
        cmd_push(args)
        if n < args.round:
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
    s = sub.add_parser("stats")
    s.set_defaults(fn=cmd_stats)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
