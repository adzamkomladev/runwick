#!/usr/bin/env python3
"""Where does the dispatch script spend its time? Runs the production Lua (checkout) with TIME marks between phases against a disposable
Redis (AOF always) and prints mean microseconds per phase for single dispatch and for groups. Usage: profile.py --port P [--n 300] [--group 6]"""
import argparse, json, socket, statistics
from pathlib import Path
from luascript import layout
from uuid import uuid4

import os
# LUA_DIR points at another checkout's Lua folder, so a before and an after can be profiled back to back under the same load.
LUA = Path(os.environ.get("LUA_DIR") or Path(__file__).resolve().parents[3] / "src/Runwick.Redis/Lua")
STORE = uuid4().hex
PREFIX = "rw:{" + b"prof".hex() + uuid4().hex[:8] + "}:"
META = PREFIX + "meta"
CAP, FLOOR = 600_000, 86_400_000


class Redis:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=30)
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


def patched(op):
    prelude = (LUA / "prelude.lua").read_text()

    def rep(old, new):
        nonlocal prelude
        assert prelude.count(old) == 1, old
        prelude = prelude.replace(old, new)

    rep("local NOWS = U.S(NOW)", """local NOWS = U.S(NOW)
local PM = {}
local C0 = os.clock()
local function mark(n) PM[#PM + 1] = n .. ':' .. math.floor((os.clock() - C0) * 1000000) end""")
    rep("  local ok, r = pcall(M.decide)", "  mark('pre_decide')\n  local ok, r = pcall(M.decide)\n  mark('decide')")
    rep("  flush_events(m)\n  local ops, keys = build_ops()", "  mark('commit_start')\n  flush_events(m)\n  mark('events')\n  local ops, keys = build_ops()\n  mark('build_ops')")
    rep("  local bytes = cmsgpack.pack(intent)", "  local bytes = cmsgpack.pack(intent)\n  mark('pack')")
    rep("  local more = U.project(intent, 1, budget)", "  mark('guard_write')\n  local more = U.project(intent, 1, budget)\n  mark('project')")
    rep("  if c and c.pending then return { 'P', CMD } end\n  return with_payloads(reply_array(r, false), r.payloads)",
        "  if c and c.pending then return { 'P', CMD } end\n  mark('end')\n  redis.call('SET', 'prof:last', table.concat(PM, ','))\n  return with_payloads(reply_array(r, false), r.payloads)")
    import os
    if os.environ.get("SORT_FAST"):
        assert prelude.count("table.sort(k, U.blt)") == 1
        prelude = prelude.replace("table.sort(k, U.blt)", "table.sort(k)")
    jobs = (LUA / "jobs.lua").read_text()
    if os.environ.get("DEEP"):
        # Finer marks: meta read, gates and receipt lookup before the decision; inside the dispatch core during it.
        rep("function U.run(M)\n", "function U.run(M)\n  mark('run_start')\n")
        rep("  local g = gates(m)\n", "  mark('meta')\n  local g = gates(m)\n  mark('gates')\n")
        rep("  if NOW >= NOT_AFTER then", "  mark('receipt')\n  if NOW >= NOT_AFTER then")
        for old, new in [("  local hasRates = false\n", "  mark('dc_start')\n  local hasRates = false\n"),
                         ("  -- Key writes in sorted order.\n", "  mark('dc_items')\n  -- Key writes in sorted order.\n"),
                         ("  for _, it in ipairs(byJob) do\n    local id = it.jobId\n", "  mark('dc_scopes')\n  for _, it in ipairs(byJob) do\n    local id = it.jobId\n"),
                         ("    U.sset('pay:' .. id, it.payload)\n", "    mark('dc_fields')\n    U.sset('pay:' .. id, it.payload)\n"),
                         ("  for _, i in ipairs(created) do\n    local it = items[i]\n    U.event", "  mark('dc_jobs')\n  for _, i in ipairs(created) do\n    local it = items[i]\n    U.event")]:
            assert jobs.count(old) == 1, old
            jobs = jobs.replace(old, new)
    if os.environ.get("DEEP"):
        return "\n".join([prelude, "mark('prelude_end')", jobs, "mark('jobs_end')", op])
    return "\n".join([prelude, jobs, op])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--group", type=int, default=6)
    ap.add_argument("--payload", type=int, default=1024)
    a = ap.parse_args()
    db = Redis(a.port)
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", layout(LUA), "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    single = db.call("SCRIPT", "LOAD", patched((LUA / "ops/dispatch.lua").read_text()))
    grouped = db.call("SCRIPT", "LOAD", patched((LUA / "ops/dispatch_group.lua").read_text()))
    payload = b"x" * a.payload
    sec, usec = map(int, db.call("TIME"))
    deadline = sec * 1000 + usec // 1000 + 300_000

    def item():
        policy = dict(maxExec=3, maxUn=3, retry="{}", uretry="{}", timeout=None)
        body = dict(jobId=uuid4().hex, name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=[], rates=[])
        return uuid4().hex, uuid4().hex + uuid4().hex, json.dumps(body), payload

    def one():
        cmd, fp, body, pay = item()
        return db.call("EVALSHA", single, 1, META, 0, STORE, "Dispatch", cmd, fp, deadline, CAP, FLOOR, "durable", "c", "", 0, 0, "initial", "", 1, body, pay)

    def group():
        items = [item() for _ in range(a.group)]
        args = []
        for cmd, fp, body, pay in items:
            args += [cmd, fp, deadline, body, pay]
        return db.call("EVALSHA", grouped, 1, META, 0, STORE, "DispatchGroup", uuid4().hex, "", deadline, CAP, FLOOR, "", "c", "", a.group, *args)

    def collect(fn, n):
        rows = []
        for _ in range(n):
            r = fn()
            assert r[0] == b"R" and r[1] == b"Applied", r
            marks = [m.split(":") for m in db.call("GET", "prof:last").decode().split(",")]
            rows.append({k: int(v) for k, v in marks})
        return rows

    def show(name, rows, per):
        keys = list(rows[0])
        prev = 0
        print(f"{name}: calls={len(rows)}, jobs per call={per}")
        for k in keys:
            m = statistics.median(r[k] for r in rows)
            print(f"  {k:12s} at {m:7.0f} us  (+{m - prev:6.0f})")
            prev = m
        total = statistics.median(r["end"] for r in rows)
        print(f"  per job: {total / per:.0f} us (in-script total {total:.0f} us)")

    collect(one, 20)
    collect(group, 10)
    show("single", collect(one, a.n), 1)
    show(f"group of {a.group}", collect(group, a.n // a.group), a.group)


main()
