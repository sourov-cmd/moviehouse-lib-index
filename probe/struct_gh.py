"""Structure probe (2026-10-14): index row format per type, URL path per type, series/drama/anime page structure
(episode labels on buttons), cookie growth (why 400 'Request Header Or Cookie Too Large' after ~1.4k requests).
Pages go to the 1-day artifact; the public log gets counts and labels only."""
import asyncio, json, re, html, time, urllib.request, random, string, collections, os
from mlbd_dec import dec
from sample_gh import current_host, attrs, buttons
from patchright.async_api import async_playwright
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
async def main():
    base = "https://" + current_host()
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
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
        await page.goto(base + "/", timeout=90000); print("challenge passed:", await solve(page))
        async def cookies():
            cs = await ctx.cookies(base)
            return sorted([(c["name"], len(c["value"])) for c in cs], key=lambda x: -x[1])
        print("cookies at start", len(await cookies()), sum(v for _, v in await cookies()))
        async def get(u):
            await asyncio.sleep(0.7)
            return await page.evaluate("async u => { const r = await fetch(u, {credentials:'include'}); return [r.status, await r.text(), r.url] }", u)
        st, m, _ = await get(base + "/search-suggest/manifest"); man = json.loads(m)
        rows = {}
        for k in ["ni", "th", "an", "dr", "se", "lo", "ma", "ka"]:
            st, x, _ = await get(f"{base}/assets/search-suggest/v{man['v']}/{k}.json")
            if st == 200:
                for r in json.loads(x): rows[r[1]] = r
        bytype = collections.defaultdict(list)
        for r in rows.values(): bytype[r[3]].append(r)
        for t, rs in bytype.items(): print("ROW", t, json.dumps(rs[0])[:300])
        json.dump({t: rs[:20] for t, rs in bytype.items()}, open(f"{OUT}/rows.json", "w"), indent=1)
        random.seed(5); out = {}
        for t, rs in bytype.items():
            for r in random.sample(rs, min(3, len(rs))):
                for path in ([t] if t in ("movie", "series") else [t, "series", "movie"]):
                    st, h, fin = await get(f"{base}/{path}/{r[1]}")
                    print("PAGE", t, "path", path, "st", st, "len", len(h), "final", fin.replace(base, "")[:60])
                    if st == 200:
                        bt = buttons(h)
                        print("   buttons", len(bt), "labels", [l[:40] for _, l in bt[:30]])
                        out[f"{t}-{r[1]}"] = h
                        # what surrounds the buttons: headings before each block
                        heads = re.findall(r'<h[2-5][^>]*>(.*?)</h[2-5]>', h, re.S)
                        print("   headings", [re.sub(r"<[^>]+>|\s+", " ", x).strip()[:40] for x in heads[:25]])
                        break
        json.dump(out, open(f"{OUT}/pages.json", "w"))
        # cookie growth: 300 cheap requests (shard files), print cookie size every 50
        ks = [a + b for a in string.ascii_lowercase for b in string.ascii_lowercase][:300]
        for i, k in enumerate(ks):
            await get(f"{base}/assets/search-suggest/v{man['v']}/{k}.json")
            if i % 50 == 49:
                c = await cookies(); print("after", i + 1, "shard req: cookies", len(c), "bytes", sum(v for _, v in c), "top", c[:4])
        pages = [r for r in bytype["movie"]][:60]
        for i, r in enumerate(pages):
            st, h, _ = await get(f"{base}/movie/{r[1]}")
            if i % 20 == 19:
                c = await cookies(); print("after", i + 1, "title pages: st", st, "cookies", len(c), "bytes", sum(v for _, v in c), "top", c[:4])
        await ctx.close()
asyncio.run(main())
