#!/usr/bin/env python3
"""Where do the worker-side scripts (ReserveAndBegin, FinishMany) spend CPU? Runs the checkout's production Lua with os.clock marks
against a disposable Redis and prints median microseconds per phase, per call and per job, for claim/finish batches of k.
os.clock is process CPU time, so it is not distorted by other load on the host. Usage: profile_worker.py --port P [--lua DIR] [--k 1,4,16] [--calls 40]"""
import argparse, json, os, socket, statistics
from pathlib import Path
from uuid import uuid4

STORE = uuid4().hex
PREFIX = "rw:{" + b"prof".hex() + uuid4().hex[:8] + "}:"
META = PREFIX + "meta"
FLOOR_OWN, FLOOR_DUR = 600_000, 86_400_000


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


def patched(lua, op):
    prelude = (lua / "prelude.lua").read_text()

    def rep(old, new):
        nonlocal prelude
        assert prelude.count(old) == 1, old
        prelude = prelude.replace(old, new)

    rep("local NOWS = U.S(NOW)", """local NOWS = U.S(NOW)
local PM = {}
local C0 = os.clock()
local function mark(n) PM[#PM + 1] = n .. ':' .. math.floor((os.clock() - C0) * 1000000) end
U.ACC = {}
function U.wrap(name)
  local f = U[name]
  U[name] = function(...)
    local c = os.clock()
    local a = f(...)
    U.ACC[name] = (U.ACC[name] or 0) + (os.clock() - c)
    return a
  end
end""")
    rep("  local ok, r = pcall(M.decide)", "  mark('pre_decide')\n  local ok, r = pcall(M.decide)\n  mark('decide')")
    rep("  flush_events(m)\n  local ops, keys = build_ops()", "  mark('commit_start')\n  flush_events(m)\n  mark('events')\n  local ops, keys = build_ops()\n  mark('build_ops')")
    rep("  local bytes = cmsgpack.pack(intent)", "  local bytes = cmsgpack.pack(intent)\n  mark('pack')")
    rep("  local more = U.project(intent, 1, budget)", "  mark('guard_write')\n  local more = U.project(intent, 1, budget)\n  mark('project')")
    rep("  if c and c.pending then return { 'P', CMD } end\n  return with_payloads(reply_array(r, false), r.payloads)",
        "  if c and c.pending then return { 'P', CMD } end\n  mark('end')\n  for k, v in pairs(U.ACC) do PM[#PM + 1] = 'in_' .. k .. ':' .. math.floor(v * 1000000) end\n"
        "  redis.call('SET', 'prof:last', table.concat(PM, ','))\n  local pa = with_payloads(reply_array(r, false), r.payloads)\n  return pa")
    jobs = (lua / "jobs.lua").read_text()
    wraps = "\nfor _, n in ipairs({'candidates', 'reserve_job', 'begin_job', 'finish_job'}) do U.wrap(n) end\n"
    return "\n".join([prelude, jobs, wraps, op])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--lua", default=str(Path(__file__).resolve().parents[3] / "src/Runwick.Redis/Lua"))
    ap.add_argument("--k", default="1,4,16")
    ap.add_argument("--calls", type=int, default=40)
    ap.add_argument("--payload", type=int, default=1024)
    a = ap.parse_args()
    lua = Path(a.lua)
    db = Redis(a.port)
    db.call("FLUSHALL")
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", 1, "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    dispatch = db.call("SCRIPT", "LOAD", (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n" + (lua / "ops/dispatch.lua").read_text())
    rb = db.call("SCRIPT", "LOAD", patched(lua, (lua / "ops/reserve_and_begin.lua").read_text()))
    fm = db.call("SCRIPT", "LOAD", patched(lua, (lua / "ops/finish_many.lua").read_text()))
    payload = b"x" * a.payload
    def dl(ahead):
        sec, usec = map(int, db.call("TIME"))
        return sec * 1000 + usec // 1000 + ahead

    def seed(n):
        # created in one DispatchMany-free pass: single dispatch per job (profile cost is not measured here)
        for _ in range(n):
            policy = dict(maxExec=3, maxUn=3, retry="{}", uretry="{}", timeout=None)
            body = dict(jobId=uuid4().hex, name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                        akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                        scopes=[], rates=[])
            r = db.call("EVALSHA", dispatch, 1, META, 0, STORE, "Dispatch", uuid4().hex, uuid4().hex + uuid4().hex, dl(300_000), 600_000, FLOOR_DUR,
                        "durable", "c", "", 0, 0, "initial", "", 1, json.dumps(body), payload)
            assert r[0] == b"R" and r[1] == b"Applied", r

    def marks():
        out = {}
        for m in db.call("GET", "prof:last").decode().split(","):
            k, v = m.split(":")
            out[k] = int(v)
        return out

    def claim(k):
        toks = json.dumps([uuid4().hex for _ in range(k)])
        ents = json.dumps([uuid4().hex[:20] for _ in range(k)])
        jit = json.dumps([0] * k)
        worker = uuid4().hex
        r = db.call("EVALSHA", rb, 1, META, 0, STORE, "ReserveAndBegin", uuid4().hex, uuid4().hex + uuid4().hex, dl(30_000), 60_000, FLOOR_OWN,
                    "ownership", "c", "", worker, "default", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), k, ents, jit)
        assert r[0] == b"R" and r[1] == b"Applied", r
        items = json.loads(r[7])["items"]
        assert len(items) == k, (len(items), k)
        return [i["begin"] for i in items], marks(), len(r[7])

    def finish(begun):
        args = [len(begun)]
        for b in begun:
            lease = dict(jobId=b["jobId"], attemptId=b["attemptId"], workerInstanceId=b["workerInstanceId"], ownershipToken=b["ownershipToken"],
                         generation=b["generation"])
            body = dict(lease=lease, outcome="succeeded", elapsed=1, requested=None, error=None, usage=None)
            args += [uuid4().hex, uuid4().hex + uuid4().hex, dl(30_000), json.dumps(body), 0.5]
        r = db.call("EVALSHA", fm, 1, META, 0, STORE, "FinishMany", uuid4().hex, "", dl(30_000), 60_000, 0, "", "d", "", *args)
        assert r[0] == b"R" and r[1] == b"Applied", r
        return marks(), len(r[7])

    def show(name, rows, k):
        keys = list(rows[0])
        prev = 0
        print(f"{name} k={k}: calls={len(rows)}")
        for key in keys:
            if key.startswith("in_"):
                continue
            m = statistics.median(r[key] for r in rows)
            print(f"  {key:12s} at {m:7.0f} us  (+{m - prev:6.0f})")
            prev = m
        for key in keys:
            if key.startswith("in_"):
                m = statistics.median(r[key] for r in rows)
                print(f"  {key:22s} total {m:7.0f} us")
        total = statistics.median(r["end"] for r in rows)
        print(f"  per job: {total / k:.0f} us (in-script total {total:.0f} us)")

    for k in [int(x) for x in a.k.split(",")]:
        seed(k * (a.calls + 5))
        for _ in range(3):
            finish(claim(k)[0])
        cm, fmk, creply, freply = [], [], [], []
        def evalsha_usec():
            for line in db.call("INFO", "commandstats").decode().splitlines():
                if line.startswith("cmdstat_evalsha:"):
                    f = dict(x.split("=") for x in line.split(":")[1].split(","))
                    return float(f["usec_per_call"]), int(f["calls"])
        db.call("CONFIG", "RESETSTAT")
        pending = []
        for _ in range(a.calls):
            begun, m1, n1 = claim(k)
            cm.append(m1)
            creply.append(n1)
            pending.append(begun)
        claim_stat = evalsha_usec()
        db.call("CONFIG", "RESETSTAT")
        for begun in pending:
            m2, n2 = finish(begun)
            fmk.append(m2)
            freply.append(n2)
        finish_stat = evalsha_usec()
        show("claim (ReserveAndBegin)", cm, k)
        print(f"  reply bytes per call {statistics.median(creply):.0f}; server evalsha {claim_stat[0]:.0f} us per call = {claim_stat[0] / k:.0f} us per job")
        show("finish (FinishMany)", fmk, k)
        print(f"  reply bytes per call {statistics.median(freply):.0f}; server evalsha {finish_stat[0]:.0f} us per call = {finish_stat[0] / k:.0f} us per job")


main()
