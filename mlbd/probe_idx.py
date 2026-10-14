"""Index completeness probe: manifest says count 15,368 but the 2-char shards gave 13,269 unique slugs.
Prints: full manifest (minus big blobs), shard size histogram (limit=240 -> truncated shards?), rows keyed by
type/slug vs slug, robots.txt, sitemap counts, and what 3-char shard / discovery URLs answer."""
import asyncio, json, re, string, collections, os
from patchright.async_api import async_playwright
import mlbd_browser as mb
OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)

async def main():
    async with async_playwright() as p:
        s, ok = await mb.start(p); print("challenge passed:", ok, s.base)
        st, m, _ = await s.get("/search-suggest/manifest"); man = json.loads(m)
        print("MANIFEST", json.dumps({k: (v if len(json.dumps(v)) < 400 else str(v)[:400]) for k, v in man.items()}))
        json.dump(man, open(f"{OUT}/manifest.json", "w"))
        al = string.ascii_lowercase + string.digits
        shards = [a + b for a in al for b in al]
        sizes = {}; rows = []
        import time; t0 = time.time()
        for i in range(0, len(shards), 24):
            batch = shards[i:i + 24]
            res = await s.get_many([f"/assets/search-suggest/v{man['v']}/{k}.json" for k in batch])
            for k, (st, x, _) in zip(batch, res):
                if st == 200:
                    try: r = json.loads(x)
                    except Exception: sizes[k] = -2; continue
                    sizes[k] = len(r); rows += [[k] + list(z) for z in r]
                else: sizes[k] = -st
        print("shards read in", round(time.time() - t0), "s; requests", s.nreq, dict(s.status))
        hist = collections.Counter(min(v, 240) // 40 * 40 if v >= 0 else v for v in sizes.values())
        print("SIZE HIST", sorted(hist.items()))
        full = [k for k, v in sizes.items() if v >= 240]; print("FULL SHARDS", len(full), full[:40])
        by_slug = {r[2] for r in rows}; by_ts = {(r[4], r[2]) for r in rows}
        print("ROWS", len(rows), "unique slug", len(by_slug), "unique type/slug", len(by_ts),
              collections.Counter(t for t, _ in by_ts))
        json.dump(rows, open(f"{OUT}/rows_all.json", "w"))
        # what does a shard key mean? show 3 rows of 2 shards
        for k in ["ni", "a1"]:
            print("SHARD", k, [r[1][:40] for r in rows if r[0] == k][:6])
        # 3-char shard for a full one
        if full:
            k = full[0]
            for kk in [k + "a", k + "e", k + "_2", k + "-2", k + "/2"]:
                st, x, fin = await s.get(f"/assets/search-suggest/v{man['v']}/{kk}.json"); print("TRY", kk, st, len(x), x[:80].replace("\n", " "))
        for u in ["/robots.txt", "/sitemap.xml", "/sitemap_index.xml", "/search-suggest/discovery", "/search-suggest?q=nilar",
                  "/search?q=nilar+baba", "/movie", "/movie?page=2", "/series?page=2", "/drama", "/anime", "/download18plus", "/feed"]:
            st, x, fin = await s.get(u)
            print("URL", u, st, len(x), fin.replace(s.base, "")[:60], re.sub(r"\s+", " ", x[:200]))
            if st == 200: open(f"{OUT}/u_{re.sub(r'[^a-z0-9]+', '_', u)}.txt", "w").write(x)
        await s.ctx.close()
asyncio.run(main())
