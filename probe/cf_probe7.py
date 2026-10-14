"""Probe 7 (2026-10-14): full V2 cdn.vidzer.me URL (not truncated), reuse now / +5 min / other IP; is /vz/ stable across unlocks?
Probe 6: for files WITHOUT an R2 button (480p): is the V2 /vz/ link reusable without the browser
(cookie-less fetch, twice, later from elsewhere)? What does Stream Online's play API return? Old: Probe 5 (2026-10-14): unlock /f/<token> (ads blocked), then follow every REAL button (skipping the aria-hidden
honeypot block) and record where it ends: redirect chain, final URL, links on the final page, Range check.
Output to /tmp/out (1-day artifact); the public log gets counts only."""
import asyncio, re, json, time, os, urllib.request, html as H
from patchright.async_api import async_playwright
HOST = "https://new5.cinecloud.site"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
ADS = re.compile(r"profitableratecpm|llvpn\.com|ay267\.com|googletagmanager|google-analytics|cloudflareinsights")
def get(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}), timeout=30).read().decode("utf8", "ignore")
def rng(u):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA, "Range": "bytes=0-1023"}), timeout=30)
        return [r.status, r.headers.get("Content-Range"), r.headers.get("Content-Type"), r.geturl()[:400]]
    except Exception as e:
        return ["ERR", str(e)[:100]]
def real_buttons(c):
    c = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div>', "", c, flags=re.S)
    out = []
    for m in re.finditer(r'<a\b([^>]*)>(.*?)</a>', c, re.S):
        a = dict(re.findall(r'([\w-]+)="([^"]*)"', m.group(1)))
        if a.get("tabindex") == "-1": continue
        out.append({"href": H.unescape(a.get("href", "")), "cls": a.get("class", ""), "text": re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()[:60]})
    return out

import http.client, urllib.parse
def raw(u, hdr=None):
    """one request, no redirect follow"""
    p = urllib.parse.urlsplit(u); c = http.client.HTTPSConnection(p.netloc, timeout=30)
    c.request("GET", p.path + ("?" + p.query if p.query else ""), headers={"User-Agent": UA, **(hdr or {})})
    r = c.getresponse(); b = r.read(2000) if r.status != 206 else b""
    out = [r.status, r.getheader("location", "")[:700], r.getheader("content-type", ""), r.getheader("content-range", ""), b[:200].decode("utf8", "ignore")]
    c.close(); return out
async def main():
    targets = []
    for s in ["toxic-a-fairy-tale-for-grown-ups-2026-full-movie-download", "reindeer-games-2000-full-movie-download"]:
        h = get(f"https://cinefreak.net/{s}/")
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        f = list(dict.fromkeys(re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)))
        targets += [(s, x) for x in f[:2]]
    print("targets", len(targets))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        async def block(route): await route.abort()
        await ctx.route(ADS, block)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        posts = []
        async def onresp(r):
            if r.request.method == "POST" and "cinecloud" in r.url:
                try: body = (await r.text())[:3000]
                except Exception as e: body = "ERR " + str(e)[:80]
                posts.append({"u": r.url[:200], "req": (r.request.post_data or "")[:300], "st": r.status, "body": body})
        ctx.on("response", lambda r: asyncio.ensure_future(onresp(r)))
        media = []
        ctx.on("request", lambda r: media.append(r.url[:700]) if re.search(r"\.(m3u8|mp4|mkv|ts)(\?|$)|vidzer|r2\.dev|/hls/|/stream", r.url) and not ADS.search(r.url) else None)
        async def click_ts(pg):
            for fr in pg.frames:
                if "challenges.cloudflare.com" in fr.url:
                    try:
                        box = await (await fr.frame_element()).bounding_box()
                        if box: await pg.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                    except Exception: pass
        async def solve(pg):
            for i in range(60):
                if "moment" not in (await pg.title()).lower(): return True
                await asyncio.sleep(2)
                if i in (4, 10, 18, 28): await click_ts(pg)
            return False
        async def unlock(tok):
            await page.goto(HOST + "/f/" + tok, timeout=90000); await solve(page)
            for i in range(60):
                c = await page.evaluate("() => { const e=document.getElementById('download-buttons-container'); return e ? e.innerHTML : ''; }")
                if 'cf-ts-box' not in c and c.strip(): return c
                await asyncio.sleep(1)
                if i in (20, 40): await click_ts(page)
            return ""
        results = []
        for n, (slug, tok) in enumerate(targets):
            rec = {"slug": slug, "tok": tok, "rounds": []}
            for rnd in range(2):
                c = await unlock(tok); btns = real_buttons(c)
                v2 = [b["href"] for b in btns if "/vz/" in b["href"]]
                if not v2: rec["rounds"].append({"err": "no v2"}); continue
                vz = HOST + v2[0]
                r = await ctx.request.get(vz, max_redirects=0)
                loc = r.headers.get("location", "")
                rec["rounds"].append({"vz": vz, "st": r.status, "cdn": loc, "len": len(loc),
                                      "t": time.time(), "a": raw(loc, {"Range": "bytes=0-1023"})[:4] if loc else None,
                                      "b": raw(loc, {"Range": "bytes=1048576-1049599"})[:4] if loc else None})
            rec["title"] = await page.title()
            results.append(rec)
            print("file", n, rec["title"][-30:], [(x.get("st"), x.get("len"), (x.get("a") or [None])[0], (x.get("b") or [None])[0]) for x in rec["rounds"]],
                  "vz same across unlocks:", len({x.get("vz") for x in rec["rounds"]}) == 1, "cdn same:", len({x.get("cdn") for x in rec["rounds"]}) == 1)
        await asyncio.sleep(300)
        for n, rec in enumerate(results):
            for x in rec["rounds"]:
                if x.get("cdn"): x["after5"] = raw(x["cdn"], {"Range": "bytes=0-1023"})[:4]
            print("file", n, "after 5 min:", [(x.get("after5") or [None])[0] for x in rec["rounds"]])
        json.dump(results, open(f"{OUT}/v2full.json", "w"), indent=1)
        await ctx.close()
asyncio.run(main())
