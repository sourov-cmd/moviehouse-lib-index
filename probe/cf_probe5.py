"""Probe 5 (2026-10-14): unlock /f/<token> (ads blocked), then follow every REAL button (skipping the aria-hidden
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
async def main():
    targets = []
    for s in ["toxic-a-fairy-tale-for-grown-ups-2026-full-movie-download", "reindeer-games-2000-full-movie-download"]:
        h = get(f"https://cinefreak.net/{s}/")
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        f = list(dict.fromkeys(re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)))
        targets += [(s, x) for x in f[:2]]
    print("targets", len(targets))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True, accept_downloads=True)
        async def block(route): await route.abort()
        await ctx.route(ADS, block)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        trail = []
        ctx.on("response", lambda r: trail.append({"st": r.status, "u": r.url[:600], "ct": r.headers.get("content-type", ""), "loc": r.headers.get("location", "")[:600]})
               if not ADS.search(r.url) and "challenges.cloudflare" not in r.url and "/assets/" not in r.url and "cdn-cgi" not in r.url else None)
        downloads = []
        ctx.on("download", lambda d: downloads.append(d.url[:600]))
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
                if i in (4, 10, 18, 28):
                    await click_ts(pg)
            return False
        async def unlock(tok):
            await page.goto(HOST + "/f/" + tok, timeout=90000); await solve(page)
            for i in range(60):
                c = await page.evaluate("() => { const e=document.getElementById('download-buttons-container'); return e ? e.innerHTML : ''; }")
                if 'cf-ts-box' not in c and c.strip(): return c
                await asyncio.sleep(1)
                if i in (20, 40): await click_ts(page)
            return ""
        meta_js = "() => ({title: document.title, size: (document.body.innerText.match(/\\d+(\\.\\d+)?\\s*(GB|MB)/)||[''])[0]})"
        results = []
        for n, (slug, tok) in enumerate(targets):
            t0 = time.time(); c = await unlock(tok)
            meta = await page.evaluate(meta_js)
            btns = real_buttons(c)
            rec = {"slug": slug, "tok": tok, "unlock_s": round(time.time() - t0, 1), "meta": meta, "buttons": []}
            for b in btns:
                if not b["href"]: continue
                # every button gets a fresh unlocked page: tokens may be one-time
                if b is not btns[0]:
                    c2 = await unlock(tok); bb = [x for x in real_buttons(c2) if x["text"] == b["text"] and x["cls"] == b["cls"]]
                    href = bb[0]["href"] if bb else b["href"]
                else: href = b["href"]
                url = href if href.startswith("http") else HOST + href
                trail.clear(); downloads.clear(); pages_before = len(ctx.pages)
                try:
                    await page.goto(url, timeout=60000); await solve(page); await asyncio.sleep(6)
                except Exception as e:
                    rec_err = str(e)[:200]
                else: rec_err = ""
                newp = ctx.pages[pages_before:]
                fin = page.url
                try:
                    pl = await page.evaluate("() => [...document.querySelectorAll('a[href],source[src],video[src],iframe[src]')].filter(e=>!e.closest('[aria-hidden=true]')).map(e => [e.tagName, e.getAttribute('href')||e.getAttribute('src'), (e.innerText||'').trim().slice(0,40)])")
                    body = (await page.content())[:30000]; ptitle = await page.title()
                except Exception: pl, body, ptitle = [], "", ""
                cand = [x[1] for x in pl if x[1] and re.search(r"\.(mkv|mp4|m3u8)|r2\.dev|fsl|pixeldrain|/d/|download", x[1], re.I)]
                item = {"text": b["text"], "cls": b["cls"], "href": href, "err": rec_err, "final": fin[:600], "title": ptitle[:120], "downloads": list(downloads),
                        "trail": list(trail)[:40], "page_links": pl[:60], "cand": cand[:20], "body": body}
                checks = {}
                for u in ([fin] if fin.startswith("http") and "cinecloud" not in fin else []) + list(downloads) + [x if x.startswith("http") else HOST + x for x in cand[:6]]:
                    checks[u[:400]] = rng(u)
                item["range"] = checks
                rec["buttons"].append(item)
                print("file", n, "button", b["text"][:24], "| final host", fin.split("/")[2] if "//" in fin else fin[:20],
                      "| downloads", len(downloads), "| cand", len(cand), "| 206:", sum(1 for v in checks.values() if v[0] == 206), "of", len(checks))
                for pg in newp:
                    try: await pg.close()
                    except Exception: pass
            print("file", n, "unlock_s", rec["unlock_s"], "title", meta.get("title", "")[:30], "buttons", len(btns))
            results.append(rec)
        json.dump(results, open(f"{OUT}/follow.json", "w"), indent=1)
        await ctx.close()
asyncio.run(main())
