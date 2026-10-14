"""Browser-pass diagnosis (Viktor 2026-10-14, throwaway): why does the GitHub runner get no buttons?
Opens known file pages one by one in one tab, logs a compact state line every 3 s (counts and flags only, the
log is public) and saves a screenshot per page to /tmp/out (1-day artifact)."""
import asyncio, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cinefreak as cf  # noqa: E402
from patchright.async_api import async_playwright  # noqa: E402

OUT = "/tmp/out"; os.makedirs(OUT, exist_ok=True)
FIDS = sys.argv[1].split(",")
WAIT = int(os.environ.get("DIAG_WAIT", "150"))


async def state(page):
    js = """() => {
      const e = document.getElementById('download-buttons-container');
      const c = e ? e.innerHTML : '';
      return {t: (document.title||'').slice(0,40), cont: c.length, box: c.includes('cf-ts-box') ? 1 : 0,
              a: e ? e.querySelectorAll('a').length : -1, inp: document.querySelectorAll('input[name=cf-turnstile-response]').length,
              val: Array.from(document.querySelectorAll('input[name=cf-turnstile-response]')).map(x=>x.value.length).join('/')};
    }"""
    try:
        s = await page.evaluate(js)
    except Exception as e:  # noqa: BLE001
        s = {"err": type(e).__name__}
    s["fr"] = sum(1 for f in page.frames if "challenges.cloudflare.com" in f.url)
    return s


async def click_ts(page):
    n = 0
    for fr in page.frames:
        if "challenges.cloudflare.com" in fr.url:
            try:
                box = await (await fr.frame_element()).bounding_box()
                if box and box["width"] > 0:
                    await page.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2); n += 1
            except Exception:  # noqa: BLE001
                pass
    return n


async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context("/tmp/diag-prof", channel="chromium", headless=False, no_viewport=True)

        async def block(route):
            await route.abort()
        await ctx.route(cf.BR_ADS, block)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        for k, fid in enumerate(FIDS):
            t0 = time.time()
            try:
                r = await page.goto(f"{cf.BR_HOST}/f/{fid}", timeout=90000)
                print(f"[{k}] goto {r.status if r else None} {time.time()-t0:.0f}s", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"[{k}] goto failed {type(e).__name__}", flush=True)
            got, last = None, None
            clicks = 0
            while time.time() - t0 < WAIT:
                s = await state(page)
                if s != last:
                    print(f"[{k}] {time.time()-t0:5.0f}s {s}", flush=True); last = s
                if s.get("cont") and not s.get("box"):
                    got = s; break
                el = time.time() - t0
                if s.get("fr") and (clicks == 0 and el > 6 or clicks == 1 and el > 20 or clicks == 2 and el > 40 or clicks == 3 and el > 70):
                    clicks += 1
                    print(f"[{k}] click#{clicks} hit {await click_ts(page)}", flush=True)
                await asyncio.sleep(3)
            await page.screenshot(path=f"{OUT}/p{k}.png")
            if got:
                c = await page.evaluate("() => document.getElementById('download-buttons-container').innerHTML")
                btns = cf._buttons(c)
                cls = cf._br_class(btns)
                print(f"[{k}] PASS {time.time()-t0:.0f}s buttons {len(btns)} class {cls.get('host')}", flush=True)
            else:
                print(f"[{k}] FAIL after {time.time()-t0:.0f}s", flush=True)
        await ctx.close()


asyncio.run(main())
