"""do_relink / do_push wiring (2026-10-25): only the changed links go to /relink (one row per file);
a full /ingest runs only when the title's page hash really changed. Run: python3 mlbd/test_relink.py"""
import json, os, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
os.environ["MLBD_DB"] = "/tmp/test-relink.sqlite"
if os.path.exists(os.environ["MLBD_DB"]):
    os.remove(os.environ["MLBD_DB"])

import mlbd  # noqa: E402

calls = []


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))))
        calls.append({"path": self.path, "body": body})
        if self.path == "/relink":
            out = {"ok": True, "files": len(body.get("files", [])), "updated": len(body.get("files", [])),
                   "shown": 0, "hidden": 0, "rows_written": len(body.get("files", []))}
        else:
            n = sum(len(t.get("files", [])) + 1 + len(t.get("episodes", [])) for t in body.get("titles", []))
            out = {"ok": True, "written": n}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


srv = HTTPServer(("127.0.0.1", 0), Fake)
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{srv.server_address[1]}"

conn = mlbd.db()
NOW = int(time.time())
# t1: already fully pushed, page unchanged, only its links were re-resolved
conn.execute("INSERT INTO titles(path,cid,kind,title,year,badge,language,quality,rating,genres,released,overview,poster,status,hash,pushed_hash,fetched_at) VALUES('/movie/t1',9,'movie','T1',2024,NULL,'Bangla',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'h1','h1:100',?)", (NOW,))
conn.execute("INSERT INTO files VALUES('f1','/movie/t1',NULL,NULL,NULL,720,0,700,'L1','Bangla','a1','r2','https://fsldownload.com/d/new1',700,'n1',?,NULL,NULL)", (NOW,))
conn.execute("UPDATE files SET pushed_url='https://fsldownload.com/d/old1' WHERE fid='f1'")
conn.execute("INSERT INTO files VALUES('f2','/movie/t1',NULL,NULL,NULL,480,0,400,'L2','Bangla','a2','r2','https://fsldownload.com/d/new2',400,'n2',?,NULL,NULL)", (NOW,))
# t2: never pushed
conn.execute("INSERT INTO titles(path,cid,kind,title,year,badge,language,quality,rating,genres,released,overview,poster,status,hash,pushed_hash,fetched_at) VALUES('/movie/t2',9,'movie','T2',2024,NULL,'Bangla',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'h2',NULL,?)", (NOW,))
conn.execute("INSERT INTO files VALUES('f3','/movie/t2',NULL,NULL,NULL,720,0,800,'L3','Bangla','a3','r2','https://fsldownload.com/d/n3',800,'n3',?,NULL,NULL)", (NOW,))
conn.commit()

# 1) relink: t1's two changed links, nothing else
n = mlbd.do_relink(conn, BASE, "k")
conn.commit()
assert n == 2, n
relink = [c for c in calls if c["path"] == "/relink"]
assert len(relink) == 1, relink
assert sorted(f["fid"] for f in relink[0]["body"]["files"]) == ["f1", "f2"]
for fid, url in (("f1", "https://fsldownload.com/d/new1"), ("f2", "https://fsldownload.com/d/new2")):
    got = conn.execute("SELECT pushed_url FROM files WHERE fid=?", (fid,)).fetchone()[0]
    assert got == url, (fid, got)
print("relink sends only changed links, one row per file: ok")

# 2) push: t2 ingested (first push), t1 skipped because its page hash is unchanged
calls.clear()
n = mlbd.do_push(conn, BASE, "k")
conn.commit()
ingest = [c for c in calls if c["path"] == "/ingest"]
assert len(ingest) == 1, ingest
assert len(ingest[0]["body"]["titles"]) == 1 and ingest[0]["body"]["titles"][0]["title"] == "T2"
ph = conn.execute("SELECT pushed_hash FROM titles WHERE path='/movie/t2'").fetchone()[0]
assert ph and ph.startswith("h2:"), ph
assert conn.execute("SELECT pushed_url FROM files WHERE fid='f3'").fetchone()[0].endswith("n3")
print("unchanged title is not re-ingested; first push marks the title and its links: ok")

# 3) nothing left to relink
assert mlbd.do_relink(conn, BASE, "k") == 0
print("second relink is a no-op: ok")

# 4) the page hash changed -> full ingest again
conn.execute("UPDATE titles SET hash='h1b' WHERE path='/movie/t1'")
conn.commit()
calls.clear()
mlbd.do_push(conn, BASE, "k")
ingest = [c for c in calls if c["path"] == "/ingest"]
assert len(ingest) == 1 and len(ingest[0]["body"]["titles"]) == 1 and ingest[0]["body"]["titles"][0]["title"] == "T1", ingest
print("a changed page still goes through the full ingest: ok")

# 5) relink keeps its own real-rows budget (the push estimate is ~1.5x the real D1 count and
# must not starve the renewals): with a spent relink counter relink refuses, below it relinks.
conn.execute("INSERT INTO meta VALUES('relinkrows:2099-01-01','99999')")
conn.execute("UPDATE files SET pushed_url=NULL WHERE fid='f1'")
conn.commit()
# force the same day label by monkeypatching strftime is overkill; exhaust the day's counter directly
rl_before = int(conn.execute("SELECT v FROM meta WHERE k='relinkrows:%s'" % time.strftime("%Y-%m-%d", time.gmtime())).fetchone()[0])
conn.execute("UPDATE meta SET v='99999' WHERE k='relinkrows:%s'" % time.strftime("%Y-%m-%d", time.gmtime()))
conn.commit()
calls.clear()
mlbd.do_relink(conn, BASE, "k")
assert not [c for c in calls if c["path"] == "/relink"], "relink budget exhausted but relink still called"
# a mostly-free relink budget relinks again (the shared push estimate is irrelevant here)
conn.execute("UPDATE meta SET v='100' WHERE k='relinkrows:%s'" % time.strftime("%Y-%m-%d", time.gmtime()))
conn.commit()
calls.clear()
n = mlbd.do_relink(conn, BASE, "k")
assert n == 1 and any(c["path"] == "/relink" for c in calls), (n, calls)
conn.execute("UPDATE meta SET v=? WHERE k='relinkrows:%s'" % time.strftime("%Y-%m-%d", time.gmtime()), (rl_before,))
conn.commit()
print("relink keeps its own real-rows budget and still relinks under the push estimate: ok")

print("PASS test_relink.py")
