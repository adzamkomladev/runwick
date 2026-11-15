#!/usr/bin/env python3
"""Server-side ceiling without the .NET client: C connections each send singles or groups of G back to back for T seconds against
the production Lua on a disposable AOF-always Redis. Usage: rawthroughput.py --port P [--conns 8] [--group 8] [--seconds 10]"""
import argparse, json, socket, threading, time
from pathlib import Path
from uuid import uuid4

LUA = Path(__file__).resolve().parents[3] / "src/Runwick.Redis/Lua"
CAP, FLOOR = 600_000, 86_400_000


class Redis:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile("rb")

    def read(self):
        line = self.f.readline()
        kind, value = line[:1], line[1:-2]
        if kind == b"+": return value
        if kind == b"-": raise RuntimeError(value.decode())
        if kind == b":": return int(value)
        size = int(value)
        if size < 0: return None
        if kind == b"$":
            data = self.f.read(size); self.f.read(2); return data
        return [self.read() for _ in range(size)]

    def call(self, *values):
        enc = [v if isinstance(v, bytes) else str(v).encode() for v in values]
        self.s.sendall(b"*%d\r\n" % len(enc) + b"".join(b"$%d\r\n%s\r\n" % (len(v), v) for v in enc))
        return self.read()


def script(text):
    return "\n".join([(LUA / "prelude.lua").read_text(), (LUA / "jobs.lua").read_text(), text])


def run(port, conns, group, seconds, payload):
    store = uuid4().hex
    prefix = "rw:{" + b"raw".hex() + uuid4().hex[:8] + "}:"
    meta = prefix + "meta"
    db = Redis(port)
    db.call("HSET", meta, "store", store, "mode", "normal", "layout", 1, "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", prefix + "queues", "default", "{}")
    single = db.call("SCRIPT", "LOAD", script((LUA / "ops/dispatch.lua").read_text()))
    grouped = db.call("SCRIPT", "LOAD", script((LUA / "ops/dispatch_group.lua").read_text()))
    sec, usec = map(int, db.call("TIME"))
    deadline = sec * 1000 + usec // 1000 + 600_000
    policy = dict(maxExec=3, maxUn=3, retry="{}", uretry="{}", timeout=None)

    def item():
        body = dict(jobId=uuid4().hex, name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=[], rates=[])
        return uuid4().hex, uuid4().hex + uuid4().hex, json.dumps(body), payload

    done = [0] * conns
    stop = time.time() + seconds

    def worker(i):
        c = Redis(port)
        while time.time() < stop:
            if group == 1:
                cmd, fp, body, pay = item()
                r = c.call("EVALSHA", single, 1, meta, 0, store, "Dispatch", cmd, fp, deadline, CAP, FLOOR, "durable", "c", "", 0, 0, "initial", "", 1, body, pay)
                assert r[1] == b"Applied", r
                done[i] += 1
            else:
                args = []
                for _ in range(group):
                    cmd, fp, body, pay = item()
                    args += [cmd, fp, deadline, body, pay]
                r = c.call("EVALSHA", grouped, 1, meta, 0, store, "DispatchGroup", uuid4().hex, "", deadline, CAP, FLOOR, "", "c", "", group, *args)
                assert r[1] == b"Applied", r
                done[i] += group

    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(conns)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    el = time.time() - t0
    info = db.call("INFO", "commandstats").decode()
    line = [l for l in info.splitlines() if l.startswith("cmdstat_evalsha")][0]
    return sum(done) / el, line


ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--conns", type=int, default=8)
ap.add_argument("--group", type=int, default=8)
ap.add_argument("--seconds", type=int, default=10)
ap.add_argument("--payload", type=int, default=1024)
a = ap.parse_args()
payload = b"x" * a.payload
for label, g in (("single", 1), (f"group{a.group}", a.group), ("single", 1), (f"group{a.group}", a.group)):
    rate, line = run(a.port, a.conns, g, a.seconds, payload)
    print(f"{label:8s} conns={a.conns}: {rate:7.0f} jobs/s   {line}")
