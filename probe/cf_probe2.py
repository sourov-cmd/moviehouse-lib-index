"""Probe 2 (2026-10-14): what does a new /f/<token> page do? Saves pages and the network trail of each button to
/tmp/out (uploaded as a 1-day artifact); the public log gets counts only."""
import asyncio, re, json, time, os, urllib.request
from patchright.async_api import async_playwright
HOST = "https://new5.cinecloud.site"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
def get(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}), timeout=30).read().decode("utf8", "ignore")
async def main():
    home = get("https://cinefreak.net/")
    slugs = list(dict.fromkeys(re.findall(r'href="https://cinefreak\.net/([a-z0-9-]+-(?:full-movie|full-series)[a-z0-9-]*)/"', home)))
    targets = []
    for s in slugs[:6]:
        h = get(f"https://cinefreak.net/{s}/")
        open(f"{OUT}/title-{s[:40]}.html", "w").write(h)
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        f = re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)
        if f: targets.append((s, f[0], f[-1]))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True, accept_downloads=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        trail = []
        def onreq(r): trail.append({"t": round(time.time(), 2), "m": r.method, "u": r.url[:600], "post": (r.post_data or "")[:300]})
        def onresp(r): trail.append({"t": round(time.time(), 2), "st": r.status, "u": r.url[:600], "ct": r.headers.get("content-type", ""), "loc": r.headers.get("location", "")[:600]})
        ctx.on("request", onreq); ctx.on("response", onresp)
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
        await page.goto(HOST + "/f/" + targets[0][1], timeout=90000); print("challenge passed:", await solve(page))
        old = await page.evaluate("async u => (await fetch(u)).text()", HOST + "/f/b774dd4e")
        open(f"{OUT}/old-fid.html", "w").write(old)
        for n, (slug, fid, fid2) in enumerate(targets[:3]):
            trail.clear()
            await page.goto(HOST + "/f/" + fid, timeout=90000); await solve(page); await asyncio.sleep(3)
            open(f"{OUT}/f-{n}.html", "w").write(await page.content())
            els = await page.evaluate("""() => [...document.querySelectorAll('a,button,form,[onclick],input[type=submit]')].map(e => ({tag: e.tagName, id: e.id, cls: e.className && e.className.toString(), text: (e.innerText||e.value||'').trim().slice(0,60), href: e.getAttribute('href'), action: e.getAttribute('action'), onclick: e.getAttribute('onclick'), vis: !!(e.offsetWidth||e.offsetHeight)}))""")
            json.dump(els, open(f"{OUT}/f-{n}-elements.json", "w"), indent=1)
            print("file page", n, "elements", len(els), "visible", sum(e["vis"] for e in els))
            # click each visible candidate in turn, on a fresh load, and keep the trail
            cands = [i for i, e in enumerate(els) if e["vis"] and re.search(r"download|watch|play|stream|server|generate|instant|cloud|direct|fast|link", (e["text"] or "") + " " + (e["cls"] or "") + " " + (e["id"] or ""), re.I)]
            print("candidates", len(cands))
            for ci in cands[:8]:
                trail.clear()
                await page.goto(HOST + "/f/" + fid, timeout=90000); await solve(page); await asyncio.sleep(2)
                pages_before = len(ctx.pages)
                try:
                    h = await page.evaluate("i => { const e=[...document.querySelectorAll('a,button,form,[onclick],input[type=submit]')][i]; e.scrollIntoView(); const r=e.getBoundingClientRect(); return [r.x+r.width/2, r.y+r.height/2]; }", ci)
                    await page.mouse.click(h[0], h[1])
                except Exception as e:
                    print("click err", str(e)[:80])
                await asyncio.sleep(12)
                for pg in ctx.pages[pages_before:]:
                    try: await solve(pg); await asyncio.sleep(3)
                    except Exception: pass
                snap = {"cand": els[ci], "url_after": page.url, "new_pages": [pg.url for pg in ctx.pages[pages_before:]], "trail": list(trail)}
                try: snap["html_after"] = (await page.content())[:60000]
                except Exception: pass
                json.dump(snap, open(f"{OUT}/f-{n}-click-{ci}.json", "w"), indent=1)
                print("clicked", ci, "requests", len(trail), "new pages", len(snap["new_pages"]))
                for pg in ctx.pages[pages_before:]:
                    try: await pg.close()
                    except Exception: pass
        await ctx.close()
asyncio.run(main())
