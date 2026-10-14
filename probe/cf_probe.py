"""GitHub-runner probe (2026-10-14): since 2026-10-11 every page of cinefreak's file host sits behind a Cloudflare
managed challenge. Can a free runner with a real browser pass it, and what do /f/ /w/ /x/ pages hold now for
old (8-char) and new (long) file codes? Prints counts, status codes and host kinds only (public log)."""
import asyncio, re, time, urllib.request
from patchright.async_api import async_playwright
HOST = "https://new5.cinecloud.site"
UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"
OLD = {"gu": ["b774dd4e", "860e4535", "5b3f2715", "adc032c2"], "r2": ["20eb87f9", "a80654ea"]}
R2 = re.compile(r'https://pub-[a-z0-9]+\.r2\.dev/[^"\'\s<>]+')
GU = re.compile(r'https://video-downloads\.googleusercontent\.com/[^"\'\s<>]+')
EMB = re.compile(r'player\.yagaverse\.net/embed2/\?id=')

def get(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}), timeout=30).read().decode("utf8", "ignore")

def kinds(h):
    doms = sorted({d for d in re.findall(r'https?://([a-z0-9.-]+)/', h) if "cloudflare" not in d})
    return {"len": len(h), "r2": bool(R2.search(h)), "gu": bool(GU.search(h)), "embed": bool(EMB.search(h)),
            "moment": "just a moment" in h.lower(), "csrf": "X-CSRF-TOKEN" in h, "instant": "Instant Download" in h,
            "login": "Login To DL" in h, "domains": doms[:12]}

async def main():
    home = get("https://cinefreak.net/")
    slugs = list(dict.fromkeys(re.findall(r'href="https://cinefreak\.net/([a-z0-9-]+-(?:full-movie|full-series)[a-z0-9-]*)/"', home)))[:4]
    new = []
    for s in slugs:
        h = get(f"https://cinefreak.net/{s}/")
        real = re.sub(r'<div class="[a-z0-9]+" aria-hidden="true">.*?</div></div>', "", h, flags=re.S)
        fids = re.findall(r'cinecloud\.site/f/([A-Za-z0-9_-]+)', real)
        decoy = len(re.findall(r'cinecloud\.site/f/', h)) - len(fids)
        print("title page: fids", len(fids), "decoy links", decoy, "fid lengths", sorted({len(f) for f in fids}))
        new += fids[:2]
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        async def solve():
            for i in range(60):
                if "moment" not in (await page.title()).lower(): return True
                await asyncio.sleep(2)
                if i in (4, 10, 18, 28):
                    for fr in page.frames:
                        if "challenges.cloudflare.com" in fr.url:
                            try:
                                box = await (await fr.frame_element()).bounding_box()
                                if box: await page.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                            except Exception: pass
            return False
        t = time.time()
        await page.goto(HOST + "/f/" + (new[0] if new else OLD["r2"][0]), timeout=90000)
        print("challenge passed:", await solve(), "in", round(time.time() - t), "s")
        cookies = [c["name"] for c in await ctx.cookies()]
        print("cookies:", cookies)
        async def fetch(path):
            return await page.evaluate("async u => { const r = await fetch(u, {credentials:'include', redirect:'follow'}); return [r.status, r.url.split('/')[2], await r.text()] }", HOST + path)
        for kind, lst in [("old-gu", OLD["gu"]), ("old-r2", OLD["r2"]), ("new", new)]:
            for fid in lst:
                for pre in ("/f/", "/w/", "/x/"):
                    try:
                        st, dom, h = await fetch(pre + fid)
                        print(kind, pre, st, dom, kinds(h))
                    except Exception as e:
                        print(kind, pre, "ERR", str(e)[:80])
                    await asyncio.sleep(1.5)
        # does a direct link found in the browser play from a plain client (no cookies)?
        for fid in new[:3] + OLD["gu"][:2]:
            st, dom, h = await fetch("/w/" + fid)
            m = GU.search(h) or R2.search(h)
            if m:
                u = m.group(0).replace("&amp;", "&")
                try:
                    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA, "Range": "bytes=0-1023"}), timeout=30)
                    print("direct from plain client:", u.split('/')[2], r.status, r.headers.get("Content-Range"))
                except Exception as e:
                    print("direct from plain client:", u.split('/')[2], "ERR", str(e)[:60])
        await page.screenshot(path="/tmp/shot.png")
        await ctx.close()
asyncio.run(main())
