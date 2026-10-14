"""GitHub-runner probe: can a free GitHub Actions machine pass movielinkbd's Cloudflare challenge and reach FSL /d/ files?
Prints counts and status codes only (public logs)."""
import asyncio, json, re, html, time, urllib.request, sys
from mlbd_dec import dec
from patchright.async_api import async_playwright

def current_host():
    h = urllib.request.urlopen(urllib.request.Request("https://movielinkbd.com/", headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf8", "ignore")
    hosts = re.findall(r"https://([a-z0-9]+\.movielinkbd\.[a-z]+)", h)
    return max(set(hosts), key=hosts.count)

def links(h, base):
    out = [html.unescape(u) for u in re.findall(r'href="(https://[^"]*/file/[^"]+|/file/[^"]+)"', h)]
    out += [dec(html.unescape(b), ds, int(dr)) for b, ds, dr in re.findall(r'data-dl="([^"]+)" data-ds="([^"]*)" data-dr="(\d+)"', h)]
    return list(dict.fromkeys(base + u if u.startswith("/") else u for u in out))

async def main():
    host = current_host(); base = "https://" + host
    print("host found:", bool(host))
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/prof", channel="chromium", headless=False, no_viewport=True)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        t = time.time()
        await page.goto(base + "/", timeout=90000)
        for i in range(40):
            title = await page.title()
            if "moment" not in title.lower(): break
            await asyncio.sleep(1.5)
        print("challenge passed:", "moment" not in (await page.title()).lower(), "after", round(time.time() - t, 1), "s")
        async def get(u):
            return await page.evaluate("async u => { const r = await fetch(u, {credentials:'include'}); return [r.status, await r.text()] }", u)
        st, m = await get(base + "/search-suggest/manifest"); print("manifest", st)
        man = json.loads(m)
        st, x = await get(f"{base}/assets/search-suggest/v{man['v']}/ni.json"); rows = json.loads(x); print("index shard 'ni'", st, len(rows), "rows")
        nb = [r for r in rows if "nilar" in json.dumps(r).lower()]; print("Nilar Baba in index:", len(nb) > 0)
        ok_fsl = 0; tried = 0
        for r in rows[:6]:
            kind = "series" if r[3] == "series" else "movie"
            st, h = await get(f"{base}/{kind}/{r[1]}"); tried += 1
            files = links(h, base)
            got = None
            for f in files[:2]:
                s2, fh = await get(f)
                for sv in [u for u in links(fh, base) if u != f][:5]:
                    s3, sh = await get(sv)
                    d = re.findall(r'href="(https://fsldownload\.com/d/[^"]+)"', sh)
                    if d: got = html.unescape(d[0]); break
                if got: break
            if got:
                req = urllib.request.Request(got, headers={"Range": "bytes=0-1023", "User-Agent": "okhttp/4.12.0"})
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp: code = resp.status; n = len(resp.read())
                except Exception as e: code = getattr(e, "code", str(e)[:40]); n = 0
                print(f"title {tried}: page {st}, files {len(files)}, FSL /d/ fetched from runner: {code} ({n} bytes)")
                ok_fsl += code == 206
            else:
                print(f"title {tried}: page {st}, files {len(files)}, no FSL found")
        print("SUMMARY fsl_206", ok_fsl, "of", tried)
        await ctx.close()
asyncio.run(main())
