#!/usr/bin/env python3
"""MovieHouse Library shelf flags (2026-10-12).

The owner: "শুধু যেগুলো Moviebox এ নাই শুধু সেগুলোই ক্যাটাগরিগুলোতে ঢুকবে … আর সার্চে অবশ্যই পাওয়া যাবে". The Library's Home shelf
and platform grids must list only what MovieBox does not already play, and a work the Library holds twice must be
listed once. This script decides both and writes the answer to the Library worker (`POST /flags`, table lib_flags):

  mb = 1      MovieBox plays the same work in this copy's language. Decided with the app's own rule (LibMerge.keep,
              core/.../search/LibMerge.kt): same plain title (or the post's other name), same kind, years within 1,
              a MovieBox copy that has a file, and MovieBox covering the copy's language. A Bengali dub of a film
              MovieBox has only in Tamil stays on the shelves; so does anything MovieBox lists as "Coming Soon".
  dup_of = N  another Library title is the same work in the same language and the better copy (direct files first,
              then more files, then the newer post); for a series only when the other covers every season this one has.

The worker hides mb = 1 from the shelves only (search still finds it; the phone merges the two sources) and dup_of
from everything. MovieBox is asked exactly as the phone asks it (the mobile API, signed with the app's recipe, read
from the MB_RECIPE secret). Each title is asked once, then again every RECHECK_DAYS while MovieBox does not have it
(MovieBox adds titles later) and every RECHECK_MB_DAYS while it does; logs print counts only.

    python3 mbmatch.py run [--budget N] [--minutes M]

Environment: LIB_BASE (the Library worker), FLAGS_KEY, MB_RECIPE (JSON: the "mobile" block of mb_recipe.json).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

RECHECK_DAYS = float(os.environ.get("MB_RECHECK_DAYS", "10"))
RECHECK_MB_DAYS = float(os.environ.get("MB_RECHECK_MB_DAYS", "45"))
CONC = int(os.environ.get("MB_CONCURRENCY", "3"))
PACE = float(os.environ.get("MB_PACE", "0.35"))      # seconds between requests per worker thread

# ---------------------------------------------------------------- the app's rules, ported line by line

def plain_title(t: str) -> str:
    """MovieBoxCards.plainTitle."""
    t = re.sub(r"\[[^\]]*\]", " ", t or "")
    t = re.sub(r"\((?i:cam|hdcam|hindi|tamil|telugu|bengali|bangla|dub|dubbed)\)", " ", t)
    t = re.sub(r"\s+(?i:s)\d+(\s*-\s*(?i:s)?\d+)?\s*$", " ", t)
    t = re.sub(r"\s+(?i:season)\s+\d+(\s*-\s*\d+)?\s*$", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def key(t: str) -> str:
    """MovieBoxCards.norm(plainTitle(t)): lowercase letters and digits only (Kotlin isLetterOrDigit keeps any script)."""
    return "".join(ch for ch in plain_title(t).lower() if ch.isalnum())


CODES = {"bn", "hi", "en", "ta", "te", "ml", "kn", "ko", "ja", "zh", "id", "es", "ru", "tr", "ur", "pa", "mr", "gu", "th", "fr", "de", "ar"}
PREFIX = [("bang", "bn"), ("beng", "bn"), ("hind", "hi"), ("eng", "en"), ("tam", "ta"), ("tel", "te"), ("mal", "ml"), ("kan", "kn"),
          ("kor", "ko"), ("jap", "ja"), ("chi", "zh"), ("man", "zh"), ("can", "zh"), ("ind", "id"), ("spa", "es"), ("rus", "ru"),
          ("tur", "tr"), ("urd", "ur"), ("pun", "pa"), ("mar", "mr"), ("guj", "gu"), ("tha", "th"), ("fre", "fr"), ("fra", "fr"),
          ("ger", "de"), ("deu", "de"), ("ara", "ar")]


def _code(w: str) -> str | None:
    for p, c in PREFIX:
        if w.startswith(p):
            return c
    return None


def langs(raw: str | None) -> set[str]:
    """LibMerge.langs."""
    if not raw or not raw.strip():
        return set()
    raw = re.sub(r"\b(org|original|dubbed|dub|audio|dual|multi|clean|line|hq|esub|subs?)\b", " ", raw.lower())
    out = set()
    for w in re.split(r"[^a-z]+", raw):
        if not w:
            continue
        c = (w if w in CODES else None) if len(w) == 2 else _code(w)
        if c:
            out.add(c)
    return out


LANG_NAMES = {"english": "en", "bengali": "bn", "bangla": "bn", "hindi": "hi", "urdu": "ur", "tamil": "ta", "telugu": "te", "malayalam": "ml",
              "kannada": "kn", "marathi": "mr", "punjabi": "pa", "arabic": "ar", "korean": "ko", "japanese": "ja", "chinese": "zh", "french": "fr",
              "spanish": "es", "indonesian": "id", "thai": "th", "nepali": "ne", "sinhala": "si", "filipino": "fil", "russian": "ru", "turkish": "tr"}


def mb_region_language(raw: str | None) -> str:
    """MbRegion.language: one of their `language` values as a code, '' when it is not one name."""
    t = (raw or "").strip().lower()
    if not t:
        return ""
    if t in LANG_NAMES:
        return LANG_NAMES[t]
    return t if 2 <= len(t) <= 3 and t.isalpha() and t.isascii() else ""


def moviebox_langs(hit: dict) -> set[str]:
    """LibMerge.movieBoxLangs: the [Hindi] edition tag, else the catalogue language. Empty = an original whose language
    is unknown (covers every language but Bengali, as in the app)."""
    m = re.search(r"\[([^\]]+)\]", hit.get("title") or "")
    tag = langs(m.group(1)) if m else set()
    if tag:
        return tag
    # SHELVES ONLY, stricter than the phone (2026-10-12): an untagged copy is the ORIGINAL, and MovieBox's `language`
    # for it is often a list of the film's spoken languages ("English, French, Latin, Spanish"). The phone's
    # MbRegion.language turns a list into "" and, beside a [Hindi] copy, the original then counted for nothing:
    # V for Vendetta (Hindi & English) stayed on the shelf although MovieBox plays both. The first language listed
    # is the original's audio, so that is the one it covers. Search is unaffected (the phone keeps its own rule).
    raw = (hit.get("language") or "").split(",")[0]
    return langs(mb_region_language(raw))


def years_close(a, b) -> bool:
    try:
        x = int(str(a)[:4])
        y = int(str(b)[:4])
    except (TypeError, ValueError):
        return True
    return abs(x - y) <= 1


def keys_of(title: str) -> set[str]:
    """The title's key, and the key of its main name when it carries a subtitle ("Khalifa: The Ruler" is MovieBox's
    "Khalifa"; checked live 2026-10-12). Only a main name of 4+ letters counts, and the year still has to agree."""
    out = {key(title)} if key(title) else set()
    head = re.split(r"\s*:\s+|\s+[-–]\s+", plain_title(title), maxsplit=1)[0]
    hk = key(head)
    if len(hk) >= 4:
        out.add(hk)
    return out


def covered_by(t: dict, hits: list[dict]) -> str | None:
    """The MovieBox title that covers this Library copy (LibMerge.keep says 'drop'), or None (keep it on the shelves)."""
    keys = keys_of(t["title"]) | (keys_of(t["aka"]) if t.get("aka") else set())
    same = [m for m in hits if m.get("hasResource") and int(m.get("subjectType") or 0) == int(t["type"])
            and keys & keys_of(m.get("title") or "") and years_close((m.get("releaseDate") or "")[:4], t.get("year"))]
    if not same:
        return None
    covered: set[str] = set()
    unknown_original = False
    for m in same:
        got = moviebox_langs(m)
        covered |= got
        unknown_original = unknown_original or not got
    mine = langs(t.get("language"))
    if not mine:
        keep = False
    elif unknown_original:
        # an original of unknown language covers everything but a Bengali copy MovieBox has no tag for
        keep = "bn" in mine and "bn" not in covered
    else:
        # 2026-10-13 (owner: "যেগুলো Moviebox এ নাই শুধু সেগুলোই ক্যাটাগরিগুলোতে"): a dual-audio copy is the same film
        # to a viewer as soon as MovieBox plays it in ONE of its languages — Mura (Hindi & Malayalam) beside MovieBox's
        # [Hindi] showed twice. It stays on the shelves only when MovieBox has none of its languages, or when it
        # brings Bengali that MovieBox lacks. Search is unchanged: every copy is still found there.
        keep = not (mine & covered) or ("bn" in mine and "bn" not in covered)
    return None if keep else same[0].get("title")


# ---------------------------------------------------------------- MovieBox mobile API (as MovieBoxMobile.kt)

class MovieBox:
    def __init__(self, recipe: dict):
        self.r = recipe
        self.dev = hashlib.md5(b"moviehouse-lib-flags").hexdigest()
        self.tok = None
        self.lock = threading.Lock()

    @staticmethod
    def _md5(b: bytes) -> str:
        return hashlib.md5(b).hexdigest()

    @staticmethod
    def _sorted_target(p: str) -> str:
        if "?" not in p:
            return p
        path, q = p.split("?", 1)
        pairs = sorted([(x.split("=", 1) + [""])[:2] for x in q.split("&") if x], key=lambda t: t[0])
        return path + "?" + "&".join(f"{a}={b}" for a, b in pairs)

    def _sign(self, method: str, ts: int, body: str | None, p: str) -> str:
        ct = "application/json"
        bl = str(len(body.encode())) if body is not None else ""
        bm = self._md5(body.encode()[:102400]) if body is not None else ""
        can = "\n".join([method, ct, ct, bl, str(ts), bm, self._sorted_target(p)])
        mac = hmac.new(base64.b64decode(self.r["signKey"]), can.encode(), hashlib.md5).digest()
        return f"{ts}|{self.r['sigVersion']}|" + base64.b64encode(mac).decode()

    def call(self, p: str, body: str | None, token: str | None = None) -> dict:
        ts = int(time.time() * 1000)
        ci = dict(self.r["clientInfo"])
        ci["device_id"] = self.dev
        h = {"User-Agent": self.r["userAgent"], "Accept": "application/json", "Content-Type": "application/json",
             "x-client-token": f"{ts},{self._md5(str(ts)[::-1].encode())}", "x-tr-signature": self._sign("POST", ts, body, p),
             "x-client-info": json.dumps(ci, separators=(",", ":")), "x-client-status": "0"}
        if token:
            h["Authorization"] = "Bearer " + token
        last = None
        for host in self.r["hosts"]:
            try:
                req = urllib.request.Request(host + p, data=body.encode() if body is not None else None, headers=h, method="POST")
                with urllib.request.urlopen(req, timeout=25) as res:
                    return json.loads(res.read())
            except urllib.error.HTTPError as e:
                last = e.code
                if e.code not in self.r.get("rotateOn", []):
                    break
            except Exception as e:  # noqa: BLE001
                last = type(e).__name__
        raise RuntimeError(f"moviebox {last}")

    def token(self) -> str:
        with self.lock:
            if not self.tok:
                self.tok = self.call(self.r["paths"]["login"], "{}")["data"]["token"]
            return self.tok

    def search(self, q: str, per: int = 20) -> list[dict]:
        b = json.dumps({"keyword": q, "page": 1, "perPage": per, "subjectType": "All", "tabId": "All"}, separators=(",", ":"))
        j = self.call(self.r["paths"]["search"], b, self.token())
        out: list[dict] = []
        for g in (j.get("data") or {}).get("results") or []:
            out += g.get("subjects") or []
        return out


# ---------------------------------------------------------------- the Library's own second copies

def dup_groups(titles: list[dict]) -> dict[int, int]:
    """num -> the num of the card that stands for the same work in the same language.

    A film: the weaker second copy points at the better one and is simply left out.
    A series (owner, 2026-10-12: "একই টাইটেল প্রয়োজনের তুলনায় বেশিবার দেখানো হচ্ছে কি না"): the source posts a show
    season by season ("Game of Thrones" was four cards, "Sweet Home" three), and a later season's post carries that
    season's year. Every post of one show points at its main card, and the Library worker shows the main card with
    every season any of them has (per season, the main card's own files first). A post counts as the same show when
    the title, the language set and the TMDB id agree (the year when no TMDB id is known), so two shows that only
    share a name stay apart."""
    # A second source (movielinkbd, 2026-10-14) posts no TMDB id: its copy of a show joins the one TMDB-known show of
    # that title and language set when its year falls within that show's years (±1); otherwise it is matched by year.
    pre: dict[tuple, list[dict]] = {}
    for t in titles:
        if not t.get("playable") or t.get("adult"):
            continue
        k = key(t["title"])
        if not k:
            continue
        pre.setdefault((int(t["type"]), k, frozenset(langs(t.get("language")))), []).append(t)
    groups: dict[tuple, list[dict]] = {}
    for (typ, k, ls), ts in pre.items():
        span: dict[int, list[int]] = {}   # TMDB id -> the years its posts carry
        if typ == 2:
            for t in ts:
                if t.get("tmdb_id") and t.get("year"):
                    span.setdefault(int(t["tmdb_id"]), []).append(int(t["year"]))
        only = next(iter(span.items())) if len(span) == 1 else None
        for t in ts:
            if typ == 2 and t.get("tmdb_id"):
                same = ("tmdb", int(t["tmdb_id"]))
            elif typ == 2 and only and t.get("year") and min(only[1]) - 1 <= int(t["year"]) <= max(only[1]) + 1:
                same = ("tmdb", only[0])
            else:
                same = ("year", t.get("year"))
            groups.setdefault((typ, k, same, ls), []).append(t)
    out: dict[int, int] = {}
    for (typ, *_), g in groups.items():
        if len(g) < 2:
            continue
        if typ == 2:
            # the main card: plays directly, carries the most seasons, then the most files, then the newest
            g.sort(key=lambda t: (0 if int(t["playable"]) == 1 else 1, -len(set(t.get("seasons") or [])), -int(t.get("files_n") or 0),
                                  -int(t.get("first_seen") or 0), -int(t["num"])))
        else:
            g.sort(key=lambda t: (0 if int(t["playable"]) == 1 else 1, -int(t.get("files_n") or 0), -int(t.get("first_seen") or 0), -int(t["num"])))
        best = g[0]
        for t in g[1:]:
            out[int(t["num"])] = int(best["num"])
    return out


# ---------------------------------------------------------------- the worker

def worker(method: str, path: str, body: dict | None = None) -> dict:
    base = os.environ["LIB_BASE"].rstrip("/")
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, method=method,
                                 headers={"x-flags-key": os.environ["FLAGS_KEY"], "content-type": "application/json", "user-agent": "MovieHouse-LibFlags/1"})
    for i in range(4):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read())
        except Exception:  # noqa: BLE001
            if i == 3:
                raise
            time.sleep(5 * (i + 1))
    return {}


# The dual-audio rule changed on 2026-10-13 (covered_by): every dual-language copy kept on the shelves under the old
# rule is asked again once, so the new rule applies everywhere within a day instead of after RECHECK_DAYS.
RULE_DUAL_TS = 1791835551   # 2026-10-12T20:05Z, when the rule changed


def due(t: dict, now: int) -> bool:
    if not t.get("playable") or t.get("adult"):
        return False
    c = int(t.get("checked") or 0)
    if not c:
        return True
    if not t.get("mb") and c < RULE_DUAL_TS and len(langs(t.get("language"))) >= 2:
        return True
    age = (now - c) / 86400
    return age >= (RECHECK_MB_DAYS if t.get("mb") else RECHECK_DAYS)


def cmd_run(args) -> None:
    started = time.time()
    by = started + args.minutes * 60
    titles = worker("GET", "/flags/titles")["items"]
    now = int(time.time())
    dups = dup_groups(titles)
    flags: dict[int, dict] = {}
    for t in titles:
        n = int(t["num"])
        want_dup = dups.get(n)
        if (t.get("dup_of") or None) != want_dup:
            flags[n] = {"num": n, "mb": int(t.get("mb") or 0), "dup_of": want_dup, "mb_hit": None, "checked": int(t.get("checked") or 0)}
    todo = [t for t in titles if due(t, now) and int(t["num"]) not in dups]
    # never-checked first, then the dual-audio copies the 2026-10-13 rule must re-decide, then newest first
    todo.sort(key=lambda t: (int(t.get("checked") or 0) != 0,
                             not (not t.get("mb") and int(t.get("checked") or 0) < RULE_DUAL_TS and len(langs(t.get("language"))) >= 2),
                             -int(t.get("first_seen") or 0)))
    todo = todo[: args.budget]
    print(f"[flags] {len(titles)} titles, {len(dups)} second copies, {len(todo)} to ask MovieBox (budget {args.budget})", flush=True)

    mb = MovieBox(json.loads(os.environ["MB_RECIPE"]))
    asked = errors = hidden = 0
    pending: list[dict] = []

    def judge(t: dict):
        hits: list[dict] = []
        for q in [plain_title(t["title"])] + ([plain_title(t["aka"])] if t.get("aka") else []):
            if not q:
                continue
            got = None
            for a in range(3):
                try:
                    got = mb.search(q, 20)
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(3 * (a + 1))
            if got is None:
                return t, None
            hits += got
            hit = covered_by(t, hits)
            if hit:
                return t, hit
            time.sleep(PACE)
        return t, ""

    def flush():
        nonlocal pending
        for i in range(0, len(pending), 500):
            worker("POST", "/flags", {"flags": pending[i:i + 500]})
        pending = []

    ex = ThreadPoolExecutor(max_workers=CONC)
    try:
        futs = []
        for t in todo:
            futs.append(ex.submit(judge, t))
        for fut in as_completed(futs):
            if time.time() > by:
                for f in futs:
                    f.cancel()
                print("[flags] time budget reached", flush=True)
                break
            t, hit = fut.result()
            asked += 1
            if hit is None:
                errors += 1
                continue
            n = int(t["num"])
            f = {"num": n, "mb": 1 if hit else 0, "dup_of": dups.get(n), "mb_hit": hit or None, "checked": int(time.time())}
            flags.pop(n, None)
            pending.append(f)
            hidden += 1 if hit else 0
            if len(pending) >= 250:
                flush()
                print(f"[flags] asked {asked}/{len(todo)}, on MovieBox {hidden}, errors {errors}", flush=True)
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    pending += list(flags.values())
    flush()
    print(f"[flags] done: asked {asked}, on MovieBox {hidden}, errors {errors}, dup changes {sum(1 for t in titles if (t.get('dup_of') or None) != dups.get(int(t['num'])))}", flush=True)
    if asked and errors > asked * 0.5:
        sys.exit("[flags] MovieBox answered fewer than half the questions")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--budget", type=int, default=int(os.environ.get("MB_BUDGET", "1500")))
    r.add_argument("--minutes", type=float, default=float(os.environ.get("MB_MINUTES", "20")))
    r.set_defaults(fn=cmd_run)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
