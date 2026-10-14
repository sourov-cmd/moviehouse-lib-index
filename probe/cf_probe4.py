"""Probe 4 (2026-10-14): why does the /f/<token> page's Turnstile never call back? Blocks ad scripts, logs every
Turnstile request, screenshots the widget, clicks inside the widget frame. Output to /tmp/out (1-day artifact)."""
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
        return [r.status, r.headers.get("Content-Range"), r.headers.get("Content-Type"), r.geturl()[:300]]
    except Exception as e:
        return ["ERR", str(e)[:100]]
async def main():
    home = get("https://cinefreak.net/")
    slugs = list(dict.fromkeys(re.findall(r'href="https://cinefreak\.net/([a-z0-9-]+-(?:full-movie|full-series)[a-z0-9-]*)/"', home)))
    targets = []
    for s in slugs[:2] + ["reindeer-games-2000-full-movie-download"]:
        h = get(f"https://cinefreak.net/{s}/")
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        f = list(dict.fromkeys(re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)))
        targets += [(s, x) for x in f[:2]]
    print("targets", len(targets))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        async def block(route):
            await route.abort()
        await ctx.route(ADS, block)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        ctx.on("page", lambda pg: asyncio.ensure_future(pg.close()) if pg != page else None)
        trail, cons = [], []
        ctx.on("response", lambda r: trail.append({"st": r.status, "u": r.url[:300]}) if ("challenges.cloudflare" in r.url or "/unlock" in r.url or r.url.endswith("/n")) else None)
        page.on("console", lambda m: cons.append(m.text[:300]))
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
            trail.clear(); cons.clear(); t0 = time.time(); log = []
            await page.goto(HOST + "/f/" + tok, timeout=90000); ok = await solve(page)
            log.append(["passed page challenge", ok, round(time.time() - t0, 1)])
            unlocked = False; c = ""
            for i in range(70):
                c = await page.evaluate("() => { const e=document.getElementById('download-buttons-container'); return e ? e.innerHTML : ''; }")
                if 'cf-ts-box' not in c and c.strip():
                    unlocked = True; break
                frs = [fr for fr in page.frames if "challenges.cloudflare.com" in fr.url]
                if i in (3, 12, 25, 40):
                    await page.screenshot(path=f"{OUT}/f{n}-{i}.png")
                    log.append(["t", i, "frames", [fr.url[:120] for fr in frs]])
                if i in (8, 20, 35, 50):
                    for fr in frs:
                        try:
                            fe = await fr.frame_element(); box = await fe.bounding_box()
                            log.append(["box", box])
                            try:
                                cb = fr.locator("input[type=checkbox]")
                                if await cb.count():
                                    await cb.first.click(timeout=5000); log.append(["clicked checkbox", i])
                                    continue
                            except Exception as e: log.append(["cb err", str(e)[:120]])
                            if box and box["width"] > 0:
                                await page.mouse.click(box["x"] + 28, box["y"] + box["height"] / 2); log.append(["clicked box", i])
                        except Exception as e: log.append(["click err", str(e)[:120]])
                await asyncio.sleep(1)
            note = await page.evaluate("() => (document.getElementById('cf-ts-note')||{}).textContent || ''")
            await page.screenshot(path=f"{OUT}/f{n}-end.png")
            links = []
            if unlocked:
                for m in re.finditer(r'<a\b([^>]*)>(.*?)</a>', c, re.S):
                    a = dict(re.findall(r'([\w-]+)="([^"]*)"', m.group(1)))
                    links.append({"href": H.unescape(a.get("href", "")), "cls": a.get("class", ""), "text": re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()[:60]})
                for L in links:
                    if L["href"].startswith("http"): L["range"] = rng(L["href"])
            results.append({"slug": slug, "tok": tok, "unlocked": unlocked, "secs": round(time.time() - t0, 1), "note": note, "log": log,
                            "links": links, "container": c[:20000], "trail": list(trail), "console": list(cons)})
            print("file", n, "unlocked", unlocked, "in", round(time.time() - t0, 1), "s ts-requests", len(trail), "links", len(links),
                  "hosts", sorted({L["href"].split("/")[2] for L in links if L["href"].startswith("http")}),
                  "range206", sum(1 for L in links if L.get("range", [0])[0] == 206), "| note:", note[:60])
            await asyncio.sleep(2)
        json.dump(results, open(f"{OUT}/unlock.json", "w"), indent=1)
        await asyncio.sleep(240)
        later = [{"href": L["href"], "range": rng(L["href"])} for r in results for L in r["links"] if L["href"].startswith("http")]
        json.dump(later, open(f"{OUT}/later.json", "w"), indent=1)
        print("4 min later 206:", sum(1 for x in later if x["range"][0] == 206), "of", len(later))
        await ctx.close()
asyncio.run(main())
