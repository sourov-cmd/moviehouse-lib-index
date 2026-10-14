"""Browser session for movielinkbd (MovieHouse Library crawler).

movielinkbd sits behind a Cloudflare *managed challenge* on rotating hosts (*.movielinkbd.li). A real Chromium
(patchright, headful under Xvfb) on a GitHub runner passes it, sometimes after one click on the Turnstile box.
After that every request is made with fetch() INSIDE the page, so the cf_clearance cookie and the browser's TLS
fingerprint are used. Proven on GitHub runners 2026-10-13/14 (runs 38497631606, 38503775431, 38508351876).

Known traps (all handled here):
  * The site sets one tiny cookie per title page viewed; after ~1,400 pages the Cookie header is too big and every
    request answers 400 "Request Header Or Cookie Too Large". prune() drops every cookie that was not present
    right after the challenge (cf_clearance is kept), every PRUNE_EVERY requests and on any 400.
  * A challenge can come back mid-run (403/503 "Just a moment"): re-open the home page, solve, retry.
  * 429: back off.
"""
import asyncio, re, time, urllib.request, collections, os

UA_PLAIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
PRUNE_EVERY = int(os.environ.get("MLBD_PRUNE_EVERY", "150"))


def current_host():
    """movielinkbd.com (no challenge) links to the host in use today."""
    for url in ("https://movielinkbd.com/", "https://movielinkbd.one/"):
        try:
            h = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA_PLAIN}), timeout=30).read().decode("utf8", "ignore")
        except Exception:
            continue
        hosts = re.findall(r"https://([a-z0-9]+\.movielinkbd\.[a-z]+)", h)
        if hosts:
            return max(set(hosts), key=hosts.count)
    raise RuntimeError("no current movielinkbd host found on movielinkbd.com / .one")


class Session:
    def __init__(self, ctx, page, base):
        self.ctx, self.page, self.base = ctx, page, base
        self.nreq = 0
        self.status = collections.Counter()
        self.errors = collections.Counter()
        self.keep = set()
        self.since_prune = 0
        self.lock = asyncio.Lock()

    async def solve(self):
        pg = self.page
        for i in range(60):
            try:
                t = (await pg.title()).lower()
            except Exception:
                t = "moment"
            if "moment" not in t and "attention" not in t:
                return True
            await asyncio.sleep(2)
            if i in (4, 10, 18, 28, 40):
                for fr in pg.frames:
                    if "challenges.cloudflare.com" in fr.url:
                        try:
                            box = await (await fr.frame_element()).bounding_box()
                            if box:
                                await pg.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                        except Exception:
                            pass
        return False

    async def open(self):
        for attempt in range(4):
            try:
                await self.page.goto(self.base + "/", timeout=90000)
            except Exception as e:
                self.errors["goto:" + type(e).__name__] += 1
            if await self.solve():
                names = {c["name"] for c in await self.ctx.cookies()}
                self.keep |= names
                return True
            await asyncio.sleep(10 * (attempt + 1))
        return False

    async def prune(self):
        cs = await self.ctx.cookies()
        if len(cs) <= len(self.keep) + 5:
            return
        kept = [c for c in cs if c["name"] in self.keep]
        await self.ctx.clear_cookies()
        if kept:
            await self.ctx.add_cookies(kept)
        self.since_prune = 0

    async def _fetch(self, urls):
        # several requests in one round trip to the page (static shards); titles go one by one
        return await self.page.evaluate(
            """async us => Promise.all(us.map(async u => {
                 try { const r = await fetch(u, {credentials: 'include'}); return [r.status, await r.text(), r.url]; }
                 catch (e) { return [0, String(e), u]; } }))""", urls)

    async def get_many(self, urls, pace=0.0):
        """Fetch absolute or site-relative URLs; returns [(status, text, final_url)] in order."""
        full = [u if u.startswith("http") else self.base + u for u in urls]
        out = [None] * len(full)
        todo = list(range(len(full)))
        for attempt in range(5):
            if not todo:
                break
            async with self.lock:
                if self.since_prune >= PRUNE_EVERY:
                    await self.prune()
                try:
                    res = await self._fetch([full[i] for i in todo])
                except Exception as e:
                    self.errors["eval:" + str(e)[:50]] += 1
                    await asyncio.sleep(5)
                    if "closed" in str(e).lower() or "navigat" in str(e).lower():
                        await self.open()
                    continue
                self.nreq += len(todo); self.since_prune += len(todo)
            again, challenge, slow, big = [], False, False, False
            for i, r in zip(todo, res):
                st, txt = r[0], r[1]
                self.status[st] += 1
                head = txt[:4000].lower() if txt else ""
                if st in (403, 503) and ("just a moment" in head or "challenge" in head):
                    challenge = True; again.append(i)
                elif st == 400 and "too large" in head:
                    big = True; again.append(i)
                elif st == 429 or st == 0 or st >= 500:
                    slow = True; again.append(i)
                else:
                    out[i] = (st, txt, r[2])
            if big:
                self.errors["cookie_too_large"] += 1
                async with self.lock:
                    await self.prune()
            if challenge:
                await self.open()
            if slow:
                await asyncio.sleep(15 * (attempt + 1))
            todo = again
            if pace:
                await asyncio.sleep(pace)
        for i in todo:
            out[i] = (-1, "", full[i])
        return out

    async def get(self, url, pace=0.0):
        return (await self.get_many([url], pace))[0]


async def start(p, profile="/tmp/mlbd-prof"):
    base = "https://" + current_host()
    ctx = await p.chromium.launch_persistent_context(profile, channel="chromium", headless=False, no_viewport=True)
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    s = Session(ctx, page, base)
    ok = await s.open()
    return s, ok
