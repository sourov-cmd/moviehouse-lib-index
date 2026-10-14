#!/usr/bin/env python3
"""MovieHouse Library — the movielinkbd index (Viktor, 2026-10-14).

movielinkbd (today movielinkbd.tv; movielinkbd.com names the host in use) carries more than the first source — Bangla
dubs like "Nilar Baba" that neither MovieBox nor cinefreak has. It sits behind a Cloudflare managed challenge, so
every request goes through one real browser (mlbd_browser.Session: fetch() inside the page after the challenge).

A file is reached in three hops: the title page's button (obfuscated, mlbd_dec.dec) -> /file/A -> the "fsl" host
button -> /file/B -> https://fsldownload.com/d/... . That last link is the same in two separate browser sessions,
answers a ranged GET with 206 from anywhere (a home line in Dhaka, a Cloudflare Worker, GitHub), with no cookie,
referer or token: it is kept exactly like the first source's public R2 links (host 'r2' = "a direct, ranged link
the gateway proves before play"), so the Library worker and the app need no change.

What it writes is what the first source writes: one POST /ingest per batch, a title once all its files are
resolved and at least one plays, under the same daily D1 row budget. The Library's own rules then apply as they
are: a work MovieBox already plays (same title, kind, year, language) leaves the shelves but stays searchable
(mbmatch.py, mb=1), and a second copy of a work the Library already holds is hidden (dup_of).

    python3 mlbd.py index  [--shards] [--listing N]   what the site lists (sitemaps; + search shards; + /new pages)
    python3 mlbd.py crawl  [--limit N]                title pages -> titles, episodes, files
    python3 mlbd.py resolve [--limit N]               file buttons -> direct links (206 checked)
    python3 mlbd.py push                              delta to the Library worker
    python3 mlbd.py cycle                             one scheduled run (index, crawl, resolve, push in rounds)
    python3 mlbd.py stats

Num = 2,100,000,000 + the site's content id (the first source uses 2,000,000,000 + its post id: no overlap).
Adult posts (/download18plus/) are read too since 2026-10-14 (owner: "18+ গুলোও দিবেন, midnight এ রাখবেন"): they go
out with the badge "Adult", so the Library keeps them off every shelf and family search (the app shows them under Midnight). Logs print counts only when LIB_QUIET=1 (public repository).
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from html import unescape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mlbd_browser as mb  # noqa: E402
from mlbd_dec import dec  # noqa: E402

DB_PATH = os.environ.get("MLBD_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "mlbd.sqlite"))
QUIET = os.environ.get("LIB_QUIET") == "1"
DAILY_ROWS = int(os.environ.get("LIB_DAILY_ROWS", "40000"))   # this source's share of the D1 free plan (100k rows/day)
ROW_FACTOR = float(os.environ.get("LIB_ROW_FACTOR", "4.0"))
BASE_NUM = 2_100_000_000
KINDS = ("movie", "series", "drama", "anime", "download18plus")  # 18+ posts carry badges "Adult"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
PACE = float(os.environ.get("MLBD_PACE", "0.3"))               # seconds between page requests (one tab)
BATCH = int(os.environ.get("MLBD_BATCH", "4"))                 # pages fetched together


def say(msg: str, private: str = "") -> None:
    print(msg if QUIET or not private else f"{msg} {private}", flush=True)


def deadline(name: str) -> float:
    v = os.environ.get(f"MLBD_{name}_BY")
    return float(v) if v else time.time() + 10 ** 9


SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  path TEXT PRIMARY KEY, kind TEXT, title TEXT, year INTEGER, poster TEXT, lastmod TEXT, card_poster TEXT,
  seen_at INTEGER, fetched_at INTEGER, fetched_mod TEXT, err TEXT
);
CREATE INDEX IF NOT EXISTS items_card ON items(card_poster);
CREATE TABLE IF NOT EXISTS titles (
  path TEXT PRIMARY KEY, cid INTEGER, kind TEXT, title TEXT, aka TEXT, year INTEGER, badge TEXT, language TEXT, quality TEXT,
  rating REAL, genres TEXT, released TEXT, overview TEXT, poster TEXT, status TEXT, hash TEXT, pushed_hash TEXT, fetched_at INTEGER
);
CREATE TABLE IF NOT EXISTS episodes (path TEXT, season INTEGER, ep INTEGER, PRIMARY KEY (path, season, ep));
CREATE TABLE IF NOT EXISTS files (
  fid TEXT PRIMARY KEY, path TEXT, season INTEGER, ep_from INTEGER, ep_to INTEGER, quality INTEGER, hevc INTEGER, size_mb REAL,
  label TEXT, lang TEXT, aurl TEXT, host TEXT, url TEXT, bytes INTEGER, fname TEXT, resolved_at INTEGER, res_err TEXT
);
CREATE INDEX IF NOT EXISTS files_path ON files(path);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=60)
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    return c


def meta_get(conn, k, d=None):
    r = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r[0] if r else d


def meta_set(conn, k, v):
    conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


# ---------------------------------------------------------------- parsing

PATH_RE = re.compile(r"^/(movie|series|drama|anime|download18plus)/([a-z0-9][a-z0-9-]*)$")


def _attrs(tag: str) -> dict:
    return {k: unescape(v) for k, v in re.findall(r'([\w-]+)="([^"]*)"', tag)}


def _text(s: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def _size_mb(label: str) -> float | None:
    m = re.search(r"([\d.]+)\s*(GB|MB)", label, re.I)
    if not m:
        return None
    v = float(m.group(1))
    return round(v * 1024 if m.group(2).upper() == "GB" else v, 1)


def _quality(label: str) -> int | None:
    m = re.search(r"(2160|1080|720|576|480|360)p", label, re.I)
    return int(m.group(1)) if m else (2160 if re.search(r"\b4k\b", label, re.I) else None)


def _buttons(seg: str) -> list[dict]:
    out = []
    for m in re.finditer(r"(<a\b[^>]*\bdata-dl=\"[^\"]+\"[^>]*>)(.*?)</a>", seg, re.S):
        a = _attrs(m.group(1))
        label = _text(m.group(2))
        try:
            url = dec(a["data-dl"], a.get("data-ds", ""), int(a.get("data-dr") or 0))
        except Exception:  # noqa: BLE001
            continue
        if not re.match(r"/file(?:18plus)?/", url):   # 18+ posts link /file18plus/ (measured 2026-10-14)
            continue
        out.append({"label": re.sub(r"^Download\s*", "", label).strip("[] ").strip(), "aurl": url, "pos": m.start()})
    return out


def parse_title(h: str) -> dict:
    canon = re.search(r'<link rel="canonical" href="https?://[^/"]+(/[^"]+)"', h)
    path = canon.group(1).rstrip("/") if canon else None
    cid = re.search(r'data-content-id="(\d+)"', h)
    h1 = re.search(r"<h1\b[^>]*>(.*?)</h1>", h, re.S)
    full = _text(h1.group(1)) if h1 else ""
    year = None
    m = re.search(r"\((\d{4})\)\s*$", full)
    if m:
        year, full = int(m.group(1)), full[: m.start()].strip()
    title, aka = (full.split(" / ", 1) + [None])[:2] if " / " in full else (full, None)
    badge = re.search(r'<p class="text-sm text-yellow font-semibold text-center">(.*?)</p>', h, re.S)
    info = {k.strip().lower(): _text(v) for k, v in re.findall(r'<p class="info-line">[^<]*<b>([^<:]+):</b>(.*?)</p>', h, re.S)}
    flex = {k.strip().lower(): _text(v) for k, v in re.findall(r'<li class="flex-item[^"]*">\s*([A-Za-z]+)\s*<br><b[^>]*>(.*?)</b>', h, re.S)}
    story = re.search(r'<p class="story-text">(.*?)</p>', h, re.S)
    og = re.search(r'<meta property="og:image" content="([^"]+)"', h)
    status = re.search(r'<div class="mlbd-series-status[^"]*">(.*?)</div>', h, re.S)
    rating = None
    if info.get("imdb"):
        r = re.match(r"([\d.]+)", info["imdb"])
        rating = float(r.group(1)) if r else None
    kind = (flex.get("type") or "").lower()
    t = {
        "path": path, "cid": int(cid.group(1)) if cid else None, "title": title.strip(), "aka": (aka or "").strip() or None, "year": year,
        "badge": _text(badge.group(1)).strip("[] ") if badge else None, "language": info.get("language") or flex.get("language"),
        "quality": info.get("quality") or flex.get("quality"), "rating": rating, "genres": info.get("genre"),
        "released": info.get("released"), "overview": _text(story.group(1)) if story else None, "poster": og.group(1) if og else None,
        "status": _text(status.group(1)) if status else None, "kind": kind,
    }
    # files: a series lists them per episode card; a film has its buttons in the download box
    files, eps = [], set()
    sec = h.find('id="mlbdEpisodeDownloads"')
    cards = [m for m in re.finditer(r'<div class="[^"]*\bep-card\b[^"]*"[^>]*>', h)] if sec >= 0 else []
    if cards:
        for i, c in enumerate(cards):
            a = _attrs(c.group(0))
            seg = h[c.end(): cards[i + 1].start() if i + 1 < len(cards) else c.end() + 20000]
            try:
                se = int(a.get("data-season-number") or 1)
                e1 = int(a.get("data-range-start") or a.get("data-episode-number"))
                e2 = int(a.get("data-range-end") or e1)
            except (TypeError, ValueError):
                continue
            for e in range(e1, e2 + 1):
                eps.add((se, e))
            for b in _buttons(seg):
                files.append({**b, "season": se, "ep_from": e1, "ep_to": e2})
    else:
        for b in _buttons(h):
            files.append({**b, "season": None, "ep_from": None, "ep_to": None})
    for f in files:
        f["quality"] = _quality(f["label"])
        f["hevc"] = 1 if re.search(r"hevc|x265|h\.?265", f["label"], re.I) else 0
        f["size_mb"] = _size_mb(f["label"])
        f["fid"] = "ml" + hashlib.sha1(f"{path}|{f['season']}|{f['ep_from']}|{f['ep_to']}|{f['label']}".encode()).hexdigest()[:10]
    # one file per episode, picture size and codec: some pages list a second button for the same file (a mirror:
    # same file name, a few MB apart, measured on a drama 2026-10-14), and a 480p copy is left out where a 720p one
    # exists (the gateway offers 720p first; a third less to resolve and to write)
    seen, uniq = set(), []
    has720 = {(f["season"], f["ep_from"], f["ep_to"]) for f in files if f["quality"] == 720}
    for f in files:
        k = (f["season"], f["ep_from"], f["ep_to"], f["quality"], f["hevc"])
        if k in seen or (f["quality"] == 480 and (f["season"], f["ep_from"], f["ep_to"]) in has720):
            continue
        seen.add(k); uniq.append(f)
    # an episode keeps ONE file (2026-10-14: the first GitHub run read ~10 files per post, series being most of them):
    # 720p plain first (plays on every phone), then 720p HEVC, 1080p, 480p. Films keep every picture size.
    rank = lambda f: ({720: 0, 1080: 2, 480: 3}.get(f["quality"], 4) + (1 if f["hevc"] and f["quality"] == 720 else 0))  # noqa: E731
    best = {}
    for f in uniq:
        if f["season"] is None:
            continue
        k = (f["season"], f["ep_from"], f["ep_to"])
        if k not in best or rank(f) < rank(best[k]):
            best[k] = f
    uniq = [f for f in uniq if f["season"] is None or best[(f["season"], f["ep_from"], f["ep_to"])] is f]
    t["files"], t["episodes"] = uniq, sorted(eps)
    return t


def title_hash(t: dict) -> str:
    keep = {k: t.get(k) for k in ("cid", "title", "aka", "year", "badge", "language", "quality", "rating", "genres", "released", "overview", "status")}
    keep["files"] = sorted((f["fid"], f["season"], f["ep_from"], f["ep_to"]) for f in t["files"])
    keep["eps"] = t["episodes"]
    return hashlib.sha1(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()[:16]


# ---------------------------------------------------------------- index

def upsert_item(conn, path, **kw) -> None:
    now = int(time.time())
    conn.execute("INSERT INTO items(path, seen_at) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET seen_at=excluded.seen_at", (path, now))
    for k, v in kw.items():
        if v is not None:
            conn.execute(f"UPDATE items SET {k}=? WHERE path=?", (v, path))


async def do_index(s, conn, shards: bool, listing: int) -> None:
    n0 = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    # 1. sitemaps: every post with its last change (the delta signal)
    st, h, _ = await s.get("/sitemap.xml")
    maps = [u for u in re.findall(r"<loc>([^<]+)</loc>", h or "") if "sitemap_post" in u]
    got = await s.get_many(maps)
    for st, x, _ in got:
        for loc, mod in re.findall(r"<loc>https?://[^/<]+(/[^<]+)</loc>\s*<lastmod>([^<]+)</lastmod>", x or ""):
            m = PATH_RE.match(loc.rstrip("/"))
            if m:
                upsert_item(conn, loc.rstrip("/"), kind=m.group(1), lastmod=mod)
    conn.commit()
    # 2. search shards: title, year, poster of most posts (each shard holds at most 240 rows)
    if shards:
        st, mtxt, _ = await s.get("/search-suggest/manifest")
        man = json.loads(mtxt)
        al = "abcdefghijklmnopqrstuvwxyz0123456789"
        keys = [a + b for a in al for b in al]
        # 12 at a time with a pause: 48 at once drew 1,284 "slow down" (429) answers on the first GitHub run (2026-10-14);
        # a shard still answered 429 (432 of 1,296 on the second run) is asked once more at the end, 6 at a time
        later: list = []

        async def take(todo, step, pause, retry):
            for i in range(0, len(todo), step):
                part = todo[i:i + step]
                res = await s.get_many([f"/assets/search-suggest/v{man['v']}/{k}.json" for k in part], pause)
                for k, (st, txt, _) in zip(part, res):
                    if st == 429 and retry:
                        later.append(k)
                    if st != 200:
                        continue
                    try:
                        rows = json.loads(txt)
                    except Exception:  # noqa: BLE001
                        continue
                    for r in rows:
                        if len(r) < 5 or r[3] not in KINDS:
                            continue
                        yr = int(r[4]) if str(r[4]).isdigit() else None
                        upsert_item(conn, f"/{r[3]}/{r[1]}", kind=r[3], title=r[0], year=yr, poster=r[2])
                conn.commit()
        await take(keys, 12, 1.0, True)
        if later:
            await asyncio.sleep(30)
            n429 = len(later)
            await take(list(later), 6, 2.0, False)
            print(f"[index] {n429} shards asked again after a pause", flush=True)
    # 3. the newest-first listing: its cards link through a per-request token, so a card is known by its poster;
    #    an unknown poster is a post this index has not seen (read its page to learn where it lives)
    unknown = []
    for pg in range(1, listing + 1):
        st, x, _ = await s.get("/new" if pg == 1 else f"/new/page/{pg}", PACE)
        if st != 200:
            break
        cards = re.findall(r'<a data-mlbd-click-ad="movie-card" href="(/(?:movie|series|drama|anime|download18plus)/[^"]+)">\s*<img[^>]*data-src="([^"]+)"', x)
        new_here = [(href, poster) for href, poster in cards if not conn.execute("SELECT 1 FROM items WHERE card_poster=?", (poster,)).fetchone()]
        unknown += new_here
        if pg > 2 and not new_here and listing < 50:   # a delta walk stops at the first page it fully knows
            break
    for i in range(0, len(unknown), BATCH):
        part = unknown[i:i + BATCH]
        res = await s.get_many([u for u, _ in part], PACE)
        for (href, poster), (st, x, fin) in zip(part, res):
            if st != 200:
                continue
            canon = re.search(r'<link rel="canonical" href="https?://[^/"]+(/[^"]+)"', x)
            path = canon.group(1).rstrip("/") if canon else None
            m = PATH_RE.match(path or "")
            if m:
                upsert_item(conn, path, kind=m.group(1), card_poster=poster)
                store_title(conn, parse_title(x), path)   # the page is here already: keep it (no second read)
        conn.commit()
    n1 = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    print(f"[index] {n1} posts known (+{n1 - n0}); {len(unknown)} new on the listing", flush=True)


# ---------------------------------------------------------------- crawl

def store_title(conn, t: dict, path: str) -> bool:
    now = int(time.time())
    if not t.get("cid") or not t.get("title"):
        conn.execute("UPDATE items SET fetched_at=?, err=? WHERE path=?", (now, "no content id", path))
        return False
    t["path"] = path
    h = title_hash(t)
    old = conn.execute("SELECT hash FROM titles WHERE path=?", (path,)).fetchone()
    conn.execute(
        "INSERT INTO titles(path,cid,kind,title,aka,year,badge,language,quality,rating,genres,released,overview,poster,status,hash,fetched_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET cid=excluded.cid, kind=excluded.kind, title=excluded.title, "
        "aka=excluded.aka, year=excluded.year, badge=excluded.badge, language=excluded.language, quality=excluded.quality, rating=excluded.rating, "
        "genres=excluded.genres, released=excluded.released, overview=excluded.overview, poster=excluded.poster, status=excluded.status, "
        "hash=excluded.hash, fetched_at=excluded.fetched_at",
        (path, t["cid"], t["kind"], t["title"], t["aka"], t["year"], t["badge"], t["language"], t["quality"], t["rating"], t["genres"],
         t["released"], t["overview"], t["poster"], t["status"], h, now))
    conn.execute("DELETE FROM episodes WHERE path=?", (path,))
    conn.executemany("INSERT OR IGNORE INTO episodes(path, season, ep) VALUES(?,?,?)", [(path, s, e) for s, e in t["episodes"]])
    fids = [f["fid"] for f in t["files"]]
    if fids:
        conn.execute("DELETE FROM files WHERE path=? AND fid NOT IN (%s)" % ",".join("?" * len(fids)), (path, *fids))
    else:
        conn.execute("DELETE FROM files WHERE path=?", (path,))
    for f in t["files"]:
        # the button's /file/A link changes with every page view: keep the newest, it is what resolve opens
        conn.execute(
            "INSERT INTO files(fid,path,season,ep_from,ep_to,quality,hevc,size_mb,label,lang,aurl) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(fid) DO UPDATE SET aurl=excluded.aurl",
            (f["fid"], path, f["season"], f["ep_from"], f["ep_to"], f["quality"], f["hevc"], f["size_mb"], f["label"], t["language"], f["aurl"]))
    item = conn.execute("SELECT lastmod FROM items WHERE path=?", (path,)).fetchone()
    conn.execute("UPDATE items SET fetched_at=?, fetched_mod=?, err=NULL WHERE path=?", (now, item["lastmod"] if item else None, path))
    return old is None or old["hash"] != h


async def do_crawl(s, conn, limit: int) -> int:
    rows = conn.execute(
        "SELECT path FROM items WHERE fetched_at IS NULL OR (lastmod IS NOT NULL AND (fetched_mod IS NULL OR lastmod > fetched_mod)) "
        "OR (err IS NOT NULL AND fetched_at < ?) ORDER BY COALESCE(lastmod, '') DESC LIMIT ?", (int(time.time()) - 86400, limit)).fetchall()
    only = os.environ.get("MLBD_ONLY")   # a test may read just one kind of post (e.g. /download18plus/)
    if only:
        rows = conn.execute("SELECT path FROM items WHERE fetched_at IS NULL AND path LIKE ? LIMIT ?", (only + "%", limit)).fetchall()
    paths = [r["path"] for r in rows]
    by = deadline("CRAWL")
    print(f"[crawl] {len(paths)} pages to read", flush=True)
    done = changed = 0
    for i in range(0, len(paths), BATCH):
        if time.time() > by:
            say(f"[crawl] time budget reached at {done}/{len(paths)}")
            break
        part = paths[i:i + BATCH]
        res = await s.get_many(part, PACE)
        for p, (st, x, fin) in zip(part, res):
            done += 1
            if st != 200:
                conn.execute("UPDATE items SET fetched_at=?, err=? WHERE path=?", (int(time.time()), f"http {st}", p))
                continue
            if store_title(conn, parse_title(x), p):
                changed += 1
        if done % 40 < BATCH:
            conn.commit()
            print(f"[crawl] {done}/{len(paths)} ({changed} changed) req={s.nreq}", flush=True)
    conn.commit()
    print(f"[crawl] done {done} ({changed} changed)", flush=True)
    return done


# ---------------------------------------------------------------- resolve

def check_direct(url: str) -> tuple[int | None, str | None, str | None]:
    """(bytes, filename, error): one ranged GET, as the gateway proves a file before play."""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Range": "bytes=0-1023"})
    with urllib.request.urlopen(req, timeout=40) as r:
        if r.status != 206:
            return None, None, f"http {r.status}"
        ct = (r.headers.get("Content-Type") or "").lower()
        cd = r.headers.get("Content-Disposition") or ""
        m = re.search(r"/(\d+)$", r.headers.get("Content-Range") or "")
        fn = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", cd)
        name = urllib.request.unquote(fn.group(1)) if fn else None
        if name and re.search(r"\.(zip|rar|7z)$", name, re.I):
            return None, name, "archive"
        if ct and not (ct.startswith("video/") or "octet-stream" in ct or "matroska" in ct):
            return None, name, f"type {ct[:40]}"
        return (int(m.group(1)) if m else None), name, None


async def resolve_file(s, aurl: str) -> tuple[str | None, str | None]:
    st, a, _ = await s.get(aurl, PACE)
    if st != 200:
        return None, f"A http {st}"
    fsl = [_attrs(t).get("href") for t in re.findall(r'<a\b[^>]*data-host="fsl"[^>]*>', a)]
    fsl = [x for x in fsl if x]
    if not fsl:
        hosts = sorted({_attrs(t).get("data-host") for t in re.findall(r'<a\b[^>]*data-host="[^"]+"[^>]*>', a)})
        return None, "no fsl (" + ",".join(h for h in hosts if h)[:60] + ")"
    st, b, _ = await s.get(fsl[0], PACE)
    if st != 200:
        return None, f"B http {st}"
    d = re.findall(r'href="(https://fsldownload\.com/d/[^"]+)"', b)
    return (unescape(d[0]), None) if d else (None, "no direct link")


async def do_resolve(s, conn, limit: int) -> int:
    rows = conn.execute(
        "SELECT f.fid, f.aurl FROM files f JOIN items i ON i.path=f.path WHERE f.aurl IS NOT NULL AND "
        "(f.resolved_at IS NULL OR (f.res_err IS NOT NULL AND f.res_err NOT LIKE 'archive%' AND f.resolved_at < ?)) "
        "ORDER BY COALESCE(i.lastmod,'') DESC, f.quality=720 DESC, f.quality DESC LIMIT ?", (int(time.time()) - 86400, limit)).fetchall()
    only = os.environ.get("MLBD_ONLY")   # a test resolves just the kind it read
    if only:
        rows = conn.execute("SELECT fid, aurl FROM files WHERE aurl IS NOT NULL AND resolved_at IS NULL AND path LIKE ? LIMIT ?", (only + "%", limit)).fetchall()
    by = deadline("RESOLVE")
    print(f"[resolve] {len(rows)} files", flush=True)
    n, stat = 0, {}
    for r in rows:
        if time.time() > by:
            say(f"[resolve] time budget reached at {n}/{len(rows)}")
            break
        url, err, size, name = None, None, None, None
        try:
            url, err = await resolve_file(s, r["aurl"])
            if url:
                size, name, err = await asyncio.to_thread(check_direct, url)
        except Exception as e:  # noqa: BLE001
            err = str(e)[:150]
        host = "r2" if url and not err else None
        key = "ok" if host else (err or "?").split(" ")[0]
        stat[key] = stat.get(key, 0) + 1
        conn.execute("UPDATE files SET host=?, url=?, bytes=?, fname=?, resolved_at=?, res_err=? WHERE fid=?",
                     (host, url if host else None, size, name, int(time.time()), err, r["fid"]))
        n += 1
        if n % 20 == 0:
            conn.commit()
            print(f"[resolve] {n}/{len(rows)} {json.dumps(stat, sort_keys=True)} req={s.nreq}", flush=True)
    conn.commit()
    print(f"[resolve] done {n} {json.dumps(stat, sort_keys=True)}", flush=True)
    return n


# ---------------------------------------------------------------- push

def payload(conn, path: str) -> dict:
    t = dict(conn.execute("SELECT * FROM titles WHERE path=?", (path,)).fetchone())
    it = conn.execute("SELECT poster FROM items WHERE path=?", (path,)).fetchone()
    files = [dict(r) for r in conn.execute(
        "SELECT fid, season, ep_from, ep_to, quality, hevc, size_mb, label, lang, host, url, bytes FROM files WHERE path=? ORDER BY season, ep_from, quality", (path,))]
    for f in files:
        f["label"] = f"{t['language'] or ''} {f['label'] or ''}".strip()
        if f["host"] != "r2":
            f["host"], f["url"] = "none", None
    eps = [{"season": r["season"], "ep": r["ep"]} for r in conn.execute("SELECT season, ep FROM episodes WHERE path=? ORDER BY season, ep", (path,))]
    poster = (it["poster"] if it and it["poster"] else None) or t["poster"]
    if poster and "image.tmdb.org" in poster:
        poster = poster.replace("/original/", "/w500/")
    kind = "tv" if t["kind"] in ("series", "drama") or (t["kind"] == "anime" and eps) else "movie"
    mx = max([f["bytes"] or 0 for f in files] + [0])
    return {
        "num": BASE_NUM + int(t["cid"]), "slug": "mlbd-" + path.strip("/").replace("/", "-"), "type": kind, "title": t["title"], "aka": t["aka"],
        "year": t["year"], "poster": poster, "overview": t["overview"], "genres": t["genres"], "language": t["language"],
        "country": None, "rating": t["rating"], "releaseDate": None, "runtime": None, "tmdbId": None, "imdbId": None, "status": t["status"],
        "cats": ",".join(x for x in (t["kind"].title() if t["kind"] else None, t["language"]) if x), "ott": None, "badges": "Adult" if path.startswith("/download18plus/") else None,
        "published": None, "modified": None, "hash": f"{t['hash']}:{mx}", "episodes": eps, "files": files,
    }


def do_push(conn, base: str, key: str, limit: int = 100000, batch: int = 25) -> int:
    day = time.strftime("%Y-%m-%d", time.gmtime())
    spent = int(meta_get(conn, f"rows:{day}", 0))
    budget = DAILY_ROWS - spent
    if budget <= 0:
        say(f"[push] today's D1 write budget is used ({spent} est. rows)")
        return 0
    rows = conn.execute(
        "SELECT t.path, (SELECT COUNT(*) FROM files f WHERE f.path=t.path) nf, (SELECT COUNT(*) FROM episodes e WHERE e.path=t.path) ne, "
        "(SELECT COALESCE(MAX(resolved_at),0) FROM files f WHERE f.path=t.path) mx FROM titles t "
        "WHERE t.cid IS NOT NULL AND NOT EXISTS(SELECT 1 FROM files f WHERE f.path=t.path AND f.resolved_at IS NULL) "
        "AND (t.pushed_hash IS NOT NULL OR EXISTS(SELECT 1 FROM files f WHERE f.path=t.path AND f.host='r2')) "
        "ORDER BY t.fetched_at DESC LIMIT ?", (limit,)).fetchall()
    rows = [r for r in rows if conn.execute("SELECT pushed_hash FROM titles WHERE path=?", (r["path"],)).fetchone()[0] != f"{conn.execute('SELECT hash FROM titles WHERE path=?', (r['path'],)).fetchone()[0]}:{r['mx']}"]
    print(f"[push] {len(rows)} titles waiting; budget {budget} est. rows today", flush=True)
    sent = used = 0
    cur: list = []

    def flush():
        nonlocal cur, sent, used
        if not cur:
            return
        body = json.dumps({"titles": [p for p, _, _ in cur]}).encode()
        req = urllib.request.Request(base.rstrip("/") + "/ingest", data=body, method="POST",
                                     headers={"content-type": "application/json", "x-lib-key": key, "user-agent": "MovieHouse-LibIndex/2"})
        res = None
        for i in range(4):
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    res = json.loads(r.read().decode())
                break
            except Exception:  # noqa: BLE001
                if i == 3:
                    raise
                time.sleep(5 * (i + 1))
        if not res or not res.get("ok"):
            raise RuntimeError("ingest refused")
        for p, path, mx in cur:
            h = conn.execute("SELECT hash FROM titles WHERE path=?", (path,)).fetchone()[0]
            conn.execute("UPDATE titles SET pushed_hash=? WHERE path=?", (f"{h}:{mx}", path))
        used += max(int(int(res.get("written") or 0) * ROW_FACTOR), len(cur))
        meta_set(conn, f"rows:{day}", spent + used)
        conn.commit()
        sent += len(cur)
        print(f"[push] {sent}/{len(rows)} written={res.get('written')} est.rows today={spent + used}", flush=True)
        cur = []

    for r in rows:
        cost = int(ROW_FACTOR * (2 + r["nf"] + r["ne"]))
        if used + cost * (len(cur) + 1) > budget:
            break
        cur.append((payload(conn, r["path"]), r["path"], r["mx"]))
        if len(cur) >= batch:
            flush()
    flush()
    print(f"[push] done {sent}", flush=True)
    return sent


# ---------------------------------------------------------------- commands

def stats(conn) -> None:
    q = lambda s: conn.execute(s).fetchone()[0]  # noqa: E731
    print("posts", q("SELECT COUNT(*) FROM items"), "read", q("SELECT COUNT(*) FROM items WHERE fetched_at>0"),
          "titles", q("SELECT COUNT(*) FROM titles"), "episodes", q("SELECT COUNT(*) FROM episodes"))
    print("files", q("SELECT COUNT(*) FROM files"), "resolved", q("SELECT COUNT(*) FROM files WHERE resolved_at IS NOT NULL"),
          "direct", q("SELECT COUNT(*) FROM files WHERE host='r2'"),
          "titles playable", q("SELECT COUNT(DISTINCT path) FROM files WHERE host='r2'"), "pushed", q("SELECT COUNT(*) FROM titles WHERE pushed_hash IS NOT NULL"))
    print("posts by kind", {r[0]: r[1] for r in conn.execute("SELECT kind, COUNT(*) FROM items GROUP BY kind")},
          "titles by kind", {r[0]: r[1] for r in conn.execute("SELECT substr(path, 2, instr(substr(path, 2), '/') - 1), COUNT(*) FROM titles GROUP BY 1")},
          "playable by kind", {r[0]: r[1] for r in conn.execute("SELECT substr(path, 2, instr(substr(path, 2), '/') - 1), COUNT(DISTINCT path) FROM files WHERE host='r2' GROUP BY 1")})
    for r in conn.execute("SELECT COALESCE(substr(res_err,1,12),'ok') e, COUNT(*) n FROM files WHERE resolved_at IS NOT NULL GROUP BY e ORDER BY n DESC LIMIT 8"):
        print("  ", r["e"], r["n"])


async def run(args) -> None:
    conn = db()
    if args.cmd == "stats":
        stats(conn); return
    if args.cmd == "push":
        do_push(conn, args.base or os.environ.get("LIB_BASE", ""), args.key or os.environ.get("LIB_KEY", "")); return
    from patchright.async_api import async_playwright
    async with async_playwright() as p:
        s, ok = await mb.start(p, os.environ.get("MLBD_PROFILE", "/tmp/mlbd-prof"))
        if not ok:
            say("[mlbd] the challenge was not passed; nothing done")
            await s.ctx.close()
            sys.exit(2)
        print("[mlbd] challenge passed", flush=True)
        try:
            if args.cmd in ("index", "cycle"):
                await do_index(s, conn, getattr(args, "shards", False) or not conn.execute("SELECT 1 FROM items LIMIT 1").fetchone(), args.listing)
            if args.cmd == "crawl":
                await do_crawl(s, conn, args.limit)
            if args.cmd == "resolve":
                await do_resolve(s, conn, args.limit)
            if args.cmd == "cycle":
                base, key = os.environ.get("LIB_BASE", ""), os.environ.get("LIB_KEY", "")
                await do_crawl(s, conn, args.limit)
                by = deadline("RESOLVE")
                while time.time() < by - 60:
                    n = await do_resolve(s, conn, args.round)
                    if base and key and os.environ.get("MLBD_PUSH") == "1":
                        do_push(conn, base, key)
                    if n < args.round:
                        break
        finally:
            conn.commit()
            print(f"[mlbd] requests {s.nreq} status {dict(s.status)} errors {dict(s.errors)}", flush=True)
            await s.ctx.close()


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("index"); i.add_argument("--shards", action="store_true"); i.add_argument("--listing", type=int, default=3)
    c = sub.add_parser("crawl"); c.add_argument("--limit", type=int, default=100000)
    r = sub.add_parser("resolve"); r.add_argument("--limit", type=int, default=100000)
    p = sub.add_parser("push"); p.add_argument("--base"); p.add_argument("--key")
    y = sub.add_parser("cycle"); y.add_argument("--listing", type=int, default=3); y.add_argument("--limit", type=int, default=100000)
    y.add_argument("--round", type=int, default=300)
    sub.add_parser("stats")
    asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    main()
