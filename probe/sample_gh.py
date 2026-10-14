"""Coverage sampler on a GitHub runner (counts only in logs): random titles from the whole index ->
quality/episode buttons -> FSL page -> fsldownload /d/ link -> Range check from the runner."""
import asyncio, json, re, html, time, urllib.request, sys, random, string, collections
from mlbd_dec import dec
from patchright.async_api import async_playwright
N = int(sys.argv[1]) if len(sys.argv) > 1 else 200
FILE_CAP = int(sys.argv[2]) if len(sys.argv) > 2 else 10
SHARDS = int(sys.argv[3]) if len(sys.argv) > 3 else 1296
PACE = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0

def current_host():
    h = urllib.request.urlopen(urllib.request.Request("https://movielinkbd.com/", headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf8", "ignore")
    hosts = re.findall(r"https://([a-z0-9]+\.movielinkbd\.[a-z]+)", h)
    return max(set(hosts), key=hosts.count)

def attrs(tag):
    return {k: html.unescape(v) for k, v in re.findall(r'([\w-]+)="([^"]*)"', tag)}

def buttons(h):
    out = []
    for m in re.finditer(r'(<a\b[^>]*>)(.*?)</a>', h, re.S):
        a = attrs(m[1]); label = re.sub(r'<[^>]+>|\s+', ' ', m[2]).strip()
        if "data-dl" in a:
            try: out.append((dec(a["data-dl"], a.get("data-ds", ""), int(a.get("data-dr", "0"))), label))
            except Exception: pass
        elif a.get("href", "").startswith("/file/") and "data-host" not in a:
            out.append((a["href"], label))
    seen = set(); res = []
    for u, l in out:
        if u not in seen: seen.add(u); res.append((u, l))
    return res

EP = re.compile(r'(?i)\b(?:s\d{1,2}\s*e\d{1,3}|e(?:p(?:isode)?)?\.?\s*\d{1,3}|episode|ep\s*\d)')
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
        nreq = 0; stats = collections.Counter(); errs = collections.Counter()
        async def get(u):
            nonlocal nreq; nreq += 1
            for attempt in range(3):
                try:
                    r = await page.evaluate("async u => { const r = await fetch(u, {credentials:'include'}); return [r.status, await r.text()] }", u)
                    stats[r[0]] += 1
                    if r[0] in (403, 503) and ("moment" in r[1][:3000].lower() or "challenge" in r[1][:3000].lower()):
                        await page.goto(base + "/", timeout=90000); await solve(page); continue
                    if r[0] == 429:
                        await asyncio.sleep(20 * (attempt + 1)); continue
                    await asyncio.sleep(PACE)
                    return r
                except Exception as e:
                    errs[type(e).__name__ + ":" + str(e)[:60]] += 1
                    await asyncio.sleep(3)
            return [0, ""]
        st, m = await get(base + "/search-suggest/manifest"); man = json.loads(m)
        print("manifest keys:", sorted(man.keys())[:20])
        rows = {}
        print("manifest:", {k: man[k] for k in man if k not in ("discovery", "catalog_probe")}, "discovery:", str(man.get("discovery"))[:200])
        shards = [a + b for a in string.ascii_lowercase + string.digits for b in string.ascii_lowercase + string.digits]
        t0 = time.time()
        for k in shards[:SHARDS]:
            st, x = await get(f"{base}/assets/search-suggest/v{man['v']}/{k}.json")
            if st == 200:
                try:
                    for r in json.loads(x): rows[r[1]] = r
                except Exception: pass
        types = collections.Counter(r[3] for r in rows.values())
        print("index rows:", len(rows), dict(types), "shards read in", round(time.time() - t0), "s")
        titles = [r for r in rows.values() if r[3] in ("movie", "series")]
        random.seed(int(time.time()) // 86400); sample = random.sample(titles, min(N, len(titles)))
        agg = collections.Counter(); per_type = collections.defaultdict(collections.Counter); no_fsl_hosts = collections.Counter()
        t0 = time.time()
        for i, r in enumerate(sample):
            kind = "series" if r[3] == "series" else "movie"
            st, h = await get(f"{base}/{kind}/{r[1]}")
            pt = per_type[kind]; pt["titles"] += 1
            if st != 200:
                pt["page_err"] += 1; pt[f"page_st_{st}"] += 1
                if pt["page_err"] <= 3: print("page err sample", st, len(h), re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", h))[:160])
                continue
            bt = buttons(h); pt["files_total"] += len(bt)
            if not bt: pt["no_buttons"] += 1; continue
            pt["ep_labelled"] += sum(1 for _, l in bt if EP.search(l))
            got_any = False; checked_d = False
            for u, l in bt[:FILE_CAP]:
                fu = base + u if u.startswith("/") else u
                s2, fh = await get(fu); pt["files_checked"] += 1
                fsl = [attrs(t).get("href", "") for t in re.findall(r'<a\b[^>]*data-host="fsl"[^>]*>', fh)]
                if not fsl:
                    for hh in set(re.findall(r'data-host="([^"]+)"', fh)): no_fsl_hosts[hh] += 1
                    continue
                f2 = fsl[0]; f2 = base + f2 if f2.startswith("/") else f2
                s3, dh = await get(f2)
                d = re.findall(r'href="(https://fsldownload\.com/d/[^"]+)"', dh)
                if not d: pt["fsl_page_no_d"] += 1; continue
                pt["files_fsl_d"] += 1; got_any = True
                if not checked_d:
                    checked_d = True
                    try:
                        with urllib.request.urlopen(urllib.request.Request(html.unescape(d[0]), headers={"Range": "bytes=0-1023", "User-Agent": "okhttp/4.12.0"}), timeout=30) as resp:
                            pt["d_206" if resp.status == 206 else "d_other"] += 1
                    except Exception: pt["d_fail"] += 1
            pt["titles_with_fsl"] += got_any
            if (i + 1) % 25 == 0: print(f"  {i+1}/{len(sample)} titles, {nreq} requests, {round(time.time()-t0)} s")
        for k, v in per_type.items(): print("RESULT", k, dict(v))
        print("hosts on file pages without FSL:", dict(no_fsl_hosts.most_common(12)))
        print("status counts", dict(stats), "fetch errors", dict(errs.most_common(5)))
        print("total requests", nreq, "time", round(time.time() - t0), "s")
        await ctx.close()
if __name__ == "__main__": asyncio.run(main())
