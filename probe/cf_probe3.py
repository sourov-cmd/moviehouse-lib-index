"""Probe 3 (2026-10-14): pass the file page's own Turnstile (/f/<token> -> /n -> /unlock) and see the download
servers it hands out. Pages + links go to /tmp/out (1-day artifact); the public log gets counts only."""
import asyncio, re, json, time, os, urllib.request, html as H
from patchright.async_api import async_playwright
HOST = "https://new5.cinecloud.site"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
def get(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}), timeout=30).read().decode("utf8", "ignore")
def rng(u, ua=UA):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": ua, "Range": "bytes=0-1023"}), timeout=30)
        return [r.status, r.headers.get("Content-Range"), r.headers.get("Content-Type"), r.geturl()[:300]]
    except Exception as e:
        return ["ERR", str(e)[:100]]
async def main():
    home = get("https://cinefreak.net/")
    slugs = list(dict.fromkeys(re.findall(r'href="https://cinefreak\.net/([a-z0-9-]+-(?:full-movie|full-series)[a-z0-9-]*)/"', home)))
    targets = []
    for s in slugs[:3] + ["reindeer-games-2000-full-movie-download"]:
        h = get(f"https://cinefreak.net/{s}/")
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        f = list(dict.fromkeys(re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)))
        targets += [(s, x) for x in f[:2]]
    print("targets", len(targets))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        trail = []
        ctx.on("response", lambda r: trail.append({"st": r.status, "u": r.url[:500], "ct": r.headers.get("content-type", "")}) if "cinecloud" in r.url else None)
        async def solve(pg):
            for i in range(60):
                if "moment" not in (await pg.title()).lower(): return True
                await asyncio.sleep(2)
                if i in (4, 10, 18, 28):
                    for fr in pg.frames:
                        if "challenges.cloudflare.com" in fr.url:
                            try:
                                box = await (await fr.frame_element()).bounding_box()
                                if box: await pg.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                            except Exception: pass
            return False
        results = []
        for n, (slug, tok) in enumerate(targets):
            trail.clear(); t0 = time.time()
            await page.goto(HOST + "/f/" + tok, timeout=90000); await solve(page)
            unlocked = False; clicks = 0
            for i in range(45):
                c = await page.evaluate("() => { const e=document.getElementById('download-buttons-container'); return e ? e.innerHTML : ''; }")
                if 'cf-ts-box' not in c and c.strip():
                    unlocked = True; break
                if i in (6, 14, 24):
                    for fr in page.frames:
                        if "challenges.cloudflare.com" in fr.url:
                            try:
                                box = await (await fr.frame_element()).bounding_box()
                                if box and box["width"] > 0: await page.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2); clicks += 1
                            except Exception: pass
                await asyncio.sleep(1)
            note = await page.evaluate("() => (document.getElementById('cf-ts-note')||{}).textContent || ''")
            links = []
            if unlocked:
                for m in re.finditer(r'<a\b([^>]*)>(.*?)</a>', c, re.S):
                    a = dict(re.findall(r'([\w-]+)="([^"]*)"', m.group(1)))
                    links.append({"href": H.unescape(a.get("href", "")), "cls": a.get("class", ""), "text": re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()[:60]})
                for L in links:
                    if L["href"].startswith("http"): L["range"] = rng(L["href"])
            results.append({"slug": slug, "tok": tok, "unlocked": unlocked, "secs": round(time.time() - t0, 1), "clicks": clicks, "note": note, "links": links, "container": c[:20000], "trail": list(trail)})
            print("file", n, "unlocked", unlocked, "in", round(time.time() - t0, 1), "s clicks", clicks, "links", len(links),
                  "hosts", sorted({L["href"].split("/")[2] for L in links if L["href"].startswith("http")}),
                  "range206", sum(1 for L in links if L.get("range", [0])[0] == 206))
            await asyncio.sleep(2)
        json.dump(results, open(f"{OUT}/unlock.json", "w"), indent=1)
        # same links a few minutes later from a plain client: do they live?
        await asyncio.sleep(240)
        later = [{"href": L["href"], "range": rng(L["href"])} for r in results for L in r["links"] if L["href"].startswith("http")]
        json.dump(later, open(f"{OUT}/later.json", "w"), indent=1)
        print("4 min later 206:", sum(1 for x in later if x["range"][0] == 206), "of", len(later))
        await ctx.close()
asyncio.run(main())
