"""Listing probe: the search shards are capped at 240 rows (60 shards full) so ~2,100 of 15,369 titles never appear.
Enumerate the listing pages (/type/movies, /type/series, /drama, /anime, /new + 18+ candidates), count unique title
tokens, check that a slug URL redirects to the same /type/mKs_<token> in two separate requests (token stable?),
and time HEAD requests (slug -> token)."""
import asyncio, json, re, time, os, collections, random
from patchright.async_api import async_playwright
import mlbd_browser as mb
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
TOK = re.compile(r'href="(?:https://[a-z0-9]+\.movielinkbd\.[a-z]+)?/(movie|series|drama|anime|download18plus)/(mKs_[a-z0-9]+)"')

async def listing(s, base_path, maxpages=800):
    st, h, fin = await s.get(base_path)
    if st != 200:
        return st, {}, 0
    toks = {}
    for t, k in TOK.findall(h): toks.setdefault(k, t)
    last = max([int(x) for x in re.findall(re.escape(base_path) + r'/page/(\d+)', h)] or [1])
    pages = list(range(2, min(last, maxpages) + 1))
    for i in range(0, len(pages), 8):
        batch = pages[i:i + 8]
        for (st2, h2, _) in await s.get_many([f"{base_path}/page/{n}" for n in batch]):
            for t, k in TOK.findall(h2): toks.setdefault(k, t)
    return st, toks, last

async def main():
    async with async_playwright() as p:
        s, ok = await mb.start(p); print("challenge passed:", ok, s.base)
        st, h, _ = await s.get("/")
        home = re.findall(r'href="(?:https://[a-z0-9]+\.movielinkbd\.[a-z]+)?(/[^"#]*)"', h)
        print("HOME LINKS", sorted({x for x in home if not re.search(r"mKs_|\.(css|js|png|webp|ico)|/style/|/assets/", x)})[:120])
        allt = {}
        for path in ["/type/movies", "/type/series", "/drama", "/anime", "/new", "/type/18plus", "/category/18plus", "/18plus",
                     "/adult", "/category/adult", "/genre/adult", "/type/adult", "/bollywood", "/category/wwe"]:
            t0 = time.time(); st, toks, last = await listing(s, path)
            new = {k for k in toks if k not in allt}
            allt.update(toks)
            print("LIST", path, "st", st, "pages", last, "tokens", len(toks), "new", len(new), collections.Counter(toks.values()), round(time.time() - t0), "s")
        print("ALL TOKENS", len(allt), collections.Counter(allt.values()), "requests", s.nreq, dict(s.status))
        json.dump(allt, open(f"{OUT}/tokens.json", "w"))
        # token stability + HEAD timing
        rows = json.load(open("rows_sample.json")) if os.path.exists("rows_sample.json") else []
        st, m, _ = await s.get("/search-suggest/manifest"); man = json.loads(m)
        res = await s.get_many([f"/assets/search-suggest/v{man['v']}/{k}.json" for k in ["ni", "lo", "ma", "th", "an"]])
        for r in res:
            if r[0] == 200: rows += json.loads(r[1])
        random.seed(1); smp = random.sample(rows, min(200, len(rows)))
        urls = [f"/{r[3]}/{r[1]}" for r in smp]
        t0 = time.time()
        fins1 = []
        for i in range(0, len(urls), 8):
            out = await s.page.evaluate("""async us => Promise.all(us.map(async u => { try { const r = await fetch(u, {method:'HEAD', credentials:'include'}); return [r.status, r.url]; } catch (e) { return [0, String(e)]; } }))""", urls[i:i + 8])
            fins1 += out
        dt = time.time() - t0
        fins2 = await s.page.evaluate("""async us => Promise.all(us.map(async u => { try { const r = await fetch(u, {method:'HEAD', credentials:'include'}); return [r.status, r.url]; } catch (e) { return [0, String(e)]; } }))""", urls[:20])
        same = sum(1 for a, b in zip(fins1[:20], fins2) if a[1] == b[1])
        sts = collections.Counter(x[0] for x in fins1)
        got = [re.search(r"/(mKs_[a-z0-9]+)", x[1]) for x in fins1]
        inlist = sum(1 for g in got if g and g[1] in allt)
        print("HEAD", len(urls), "in", round(dt), "s", dict(sts), "stable", same, "/ 20", "token found", sum(1 for g in got if g), "in listings", inlist)
        print("HEAD sample", [x[1].replace(s.base, "") for x in fins1[:3]])
        print("cookies", len(await s.ctx.cookies()), "errors", dict(s.errors))
        await s.ctx.close()
asyncio.run(main())
