"""Decode movielinkbd's obfuscated download hrefs (a[data-dl], data-ds seed hex, data-dr rounds): LCG-XOR, as in the page's own dec()."""
import base64
def dec(b: str, s: str, r: int) -> str:
    x = int(s, 16) & 0xFFFFFFFF if s else 0
    for _ in range(r):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
    raw = base64.b64decode(b); out = []
    for c in raw:
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out.append(chr(c ^ ((x >> 16) & 255)))
    return "".join(out)
if __name__ == "__main__":
    import re, sys, html
    s = open(sys.argv[1]).read()
    for b, ds, dr in re.findall(r'data-dl="([^"]+)" data-ds="([^"]*)" data-dr="(\d+)"', s):
        print(dec(html.unescape(b), ds, int(dr)))
