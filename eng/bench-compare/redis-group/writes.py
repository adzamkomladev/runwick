#!/usr/bin/env python3
"""What does each worker command write? Runs claim (ReserveAndBegin), FinishMany, Finish and FinishAndClaim calls of k jobs through
the checkout's Lua against a disposable AOF-always Redis whose AOF directory is mounted on the host, reads the AOF bytes each call
appended, and prints per-job bytes, write commands and fields by key kind. Usage:
  writes.py --port P --aof DIR/appendonlydir [--lua DIR] [--k 8] [--calls 20]"""
import argparse, collections, json, os, re, socket
from pathlib import Path
from uuid import uuid4

STORE = uuid4().hex
PREFIX = "rw:{" + b"writes".hex() + "}:"
META = PREFIX + "meta"


class Redis:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=120)
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


def resp_commands(buf):
    """Yields (argv, bytes) for each RESP array in an AOF slice."""
    i = 0
    while i < len(buf):
        start = i
        assert buf[i:i + 1] == b"*", buf[i:i + 40]
        j = buf.index(b"\r\n", i); n = int(buf[i + 1:j]); i = j + 2
        argv = []
        for _ in range(n):
            j = buf.index(b"\r\n", i); size = int(buf[i + 1:j]); i = j + 2
            argv.append(buf[i:i + size]); i += size + 2
        yield argv, i - start


def kind(key):
    s = key.decode("latin1")[len(PREFIX):]
    if s == "meta": return "meta"
    for p, name in (("job:", "job hash"), ("rcpt:exp:", "receipt expiry index"), ("rcpt:", "receipt"), ("events:data", "event row"),
                    ("events", "event index"), ("ready:", "ready index"), ("leases", "lease index"), ("idx:created:s:", "state index"),
                    ("idx:terminal", "terminal index"), ("attempts", "attempt->job"), ("pay:", "payload")):
        if s.startswith(p): return name
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--aof", required=True, help="the host path of the server's appendonlydir")
    ap.add_argument("--lua", default=str(Path(__file__).resolve().parents[3] / "src/Runwick.Redis/Lua"))
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--calls", type=int, default=20)
    a = ap.parse_args()
    lua, k = Path(a.lua), a.k
    db = Redis(a.port)
    db.call("FLUSHALL")
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", 1, "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    pre = (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n"
    load = lambda op: db.call("SCRIPT", "LOAD", pre + (lua / f"ops/{op}.lua").read_text())
    dispatch, rb, fm, fin, fc = (load(x) for x in ("dispatch", "reserve_and_begin", "finish_many", "finish", "finish_and_claim"))

    def aof_file():
        d = Path(a.aof)
        return max(d.glob("*.incr.aof"), key=lambda p: int(re.search(r"\.(\d+)\.incr", p.name).group(1)))

    def dl(ahead):
        sec, usec = map(int, db.call("TIME"))
        return sec * 1000 + usec // 1000 + ahead

    for _ in range(k * (3 * a.calls + 4)):
        policy = dict(maxExec=3, maxUn=3, retry='{"initialDelay":100,"maximumDelay":1000}', uretry='{"initialDelay":100,"maximumDelay":1000}', timeout=None)
        body = dict(jobId=uuid4().hex, name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=[], rates=[])
        r = db.call("EVALSHA", dispatch, 1, META, 0, STORE, "Dispatch", uuid4().hex, uuid4().hex * 2, dl(300_000), 600_000, 86_400_000,
                    "durable", "c", "", 0, 0, "initial", "", 1, json.dumps(body), b"x" * 1024)
        assert r[1] == b"Applied", r

    claim_args = lambda n: ["", uuid4().hex, "default", "medium", 600_000, json.dumps([uuid4().hex for _ in range(n)]), "", "",
                            json.dumps([["test", 1, "json", 1]]), n, json.dumps([uuid4().hex[:20] for _ in range(n)]), json.dumps([0] * n)]

    def claim(n):
        r = db.call("EVALSHA", rb, 1, META, 0, STORE, "ReserveAndBegin", uuid4().hex, uuid4().hex * 2, dl(30_000), 60_000, 600_000, "ownership", "c",
                    *claim_args(n))
        assert r[1] == b"Applied", r
        return [i["begin"] for i in json.loads(r[7])["items"]]

    def fbody(b):
        lease = dict(jobId=b["jobId"], attemptId=b["attemptId"], workerInstanceId=b["workerInstanceId"], ownershipToken=b["ownershipToken"],
                     generation=b["generation"])
        return json.dumps(dict(lease=lease, outcome="succeeded", elapsed=1, requested=None, error=None, usage=None))

    def fitems(begun):
        out = [len(begun)]
        for b in begun: out += [uuid4().hex, uuid4().hex * 2, dl(30_000), fbody(b), 0.5]
        return out

    stats = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0, 0]))  # op -> kind -> [bytes, commands, args]
    totals = collections.Counter()

    def measured(op, jobs, fn):
        f = aof_file(); before = f.stat().st_size
        out = fn()
        f2 = aof_file(); assert f2 == f
        with open(f, "rb") as h:
            h.seek(before); buf = h.read()
        totals[op, "bytes"] += len(buf); totals[op, "jobs"] += jobs; totals[op, "calls"] += 1
        for argv, size in resp_commands(buf):
            c = argv[0].upper()
            if c in (b"MULTI", b"EXEC", b"SELECT"):
                key = "(MULTI/EXEC/SELECT)"
            else:
                key = kind(argv[1])
                if key == "meta" and b"intent" in argv[2:] and len(argv) > 4:
                    i = argv.index(b"intent")
                    key = "meta: guard + intent" if argv[i + 1] else "meta: publication"
            s = stats[op][key]
            s[0] += size; s[1] += 1; s[2] += max(0, (len(argv) - 2) // 2)
        return out

    for _ in range(a.calls):
        begun = measured("claim (ReserveAndBegin)", k, lambda: claim(k))
        measured("FinishMany", k, lambda: db.call("EVALSHA", fm, 1, META, 0, STORE, "FinishMany", uuid4().hex, "", dl(30_000), 60_000, 0, "", "d", "",
                                                   *fitems(begun)))
        begun = claim(1)
        measured("Finish (single)", 1, lambda: db.call("EVALSHA", fin, 1, META, 0, STORE, "Finish", uuid4().hex, uuid4().hex * 2, dl(30_000), 60_000,
                                                       86_400_000, "durable", "d", "", fbody(begun[0]), 0.5))
    begun = claim(k)
    for _ in range(a.calls):
        def comb():
            r = db.call("EVALSHA", fc, 1, META, 0, STORE, "ReserveAndBegin", uuid4().hex, uuid4().hex * 2, dl(30_000), 60_000, 600_000, "ownership",
                        "c", *claim_args(k), *fitems(begun))
            assert r[1] == b"Applied", r
            return [i["begin"] for i in json.loads(r[7])["items"]]
        begun = measured("FinishAndClaim (k finishes + claim of k)", k, comb)

    out = {}
    for op, kinds in stats.items():
        jobs = totals[op, "jobs"]
        print(f"\n{op}: {totals[op, 'calls']} calls, AOF {totals[op, 'bytes'] / jobs:,.0f} bytes per job")
        print(f"  {'key kind':38s} {'bytes/job':>10s} {'cmds/job':>9s} {'fields/job':>11s}")
        for key, (b, c, f) in sorted(kinds.items(), key=lambda kv: -kv[1][0]):
            print(f"  {key:38s} {b / jobs:10.0f} {c / jobs:9.2f} {f / jobs:11.1f}")
        out[op] = {"bytesPerJob": totals[op, "bytes"] / jobs, "kinds": {key: [v / jobs for v in s] for key, s in kinds.items()}}
    if os.environ.get("JSON_OUT"): Path(os.environ["JSON_OUT"]).write_text(json.dumps(out, indent=1))


main()
