#!/usr/bin/env python3
"""Where do the worker-side scripts (ReserveAndBegin, FinishMany) spend CPU? Runs the checkout's production Lua with os.clock marks
against a disposable Redis and prints median microseconds per phase, per call and per job, for claim/finish batches of k.
os.clock is process CPU time, so it is not distorted by other load on the host. Usage: profile_worker.py --port P [--lua DIR] [--k 1,4,16] [--calls 40]"""
import argparse, json, os, socket, statistics
from luascript import compose, hint, layout
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
local DEPTH = {}
function U.wrap(name)
  local f = U[name]
  DEPTH[name] = 0
  U[name] = function(...)
    if DEPTH[name] > 0 then return f(...) end -- a recursive call (U.enc) is counted once, in its outermost call
    DEPTH[name] = 1
    local c = os.clock()
    local a, b = f(...)
    U.ACC[name] = (U.ACC[name] or 0) + (os.clock() - c)
    DEPTH[name] = 0
    return a, b
  end
end""")
    def opt(old, new):  # a finer mark, where this version of the prelude has the line
        nonlocal prelude
        if prelude.count(old) == 1:
            prelude = prelude.replace(old, new)

    # setup: running the chunk's definitions; meta_gates: HGETALL meta and the gates; rcpt_lookup: the receipt check
    opt("function U.run(M)\n", "function U.run(M)\n  mark('setup')\n")
    opt("  U.meta = m\n\n  if FP ~= '' then", "  U.meta = m\n  mark('meta_gates')\n\n  if FP ~= '' then")
    opt("  if NOW >= NOT_AFTER then return reply_array(U.noeffect('CommandExpired'", "  mark('rcpt_lookup')\n  if NOW >= NOT_AFTER then return reply_array(U.noeffect('CommandExpired'")
    rep("  local ok, r = pcall(M.decide)", "  mark('pre_decide')\n  local ok, r = pcall(M.decide)\n  mark('decide')")
    opt("  local ok2, c = pcall(commit, m, r)", "  mark('receipt')\n  local ok2, c = pcall(commit, m, r)")
    rep("  flush_events(m)\n  local ops, keys = build_ops()", "  mark('commit_start')\n  flush_events(m)\n  mark('events')\n  local ops, keys = build_ops()\n  mark('build_ops')")
    opt("  local reply = reply_array(r, false)\n", "  local reply = reply_array(r, false)\n  mark('reply_encode')\n")
    if "  local bytes = cmsgpack.pack(intent)" in prelude:  # scripts that store every intent before projecting
        rep("  local bytes = cmsgpack.pack(intent)", "  local bytes = cmsgpack.pack(intent)\n  mark('pack')")
        rep("  local more = U.project(intent, 1, budget)", "  mark('guard_write')\n  local more = U.project(intent, 1, budget)\n  mark('project')")
    else:  # direct projection: the intent is stored only when a write fails partway
        rep("  local budget = inline_budget(ops, keys, size(BATCH_INLINE_BYTES))", "  local budget = inline_budget(ops, keys, size(BATCH_INLINE_BYTES))\n  mark('bound')")
        rep("  if FAIL > 0 and writes >= FAIL then error('INJECTED reply lost", "  mark('project')\n  if FAIL > 0 and writes >= FAIL then error('INJECTED reply lost")
    # payloads: the reply array and the payload reads after the commit (with_payloads); the profile row is written last
    tail = "  if c and c.pending then return { 'P', CMD } end\n  return with_payloads(reply_array(r, false), r.payloads"
    args = prelude[prelude.index(tail) + len(tail):].split("\n", 1)[0]  # ")" or ", r.known)"
    rep(tail + args, "  if c and c.pending then return { 'P', CMD } end\n  mark('end')\n  local pa = with_payloads(reply_array(r, false), r.payloads" + args
        + "\n  mark('payloads')\n  for k, v in pairs(U.ACC) do PM[#PM + 1] = 'in_' .. k .. ':' .. math.floor(v * 1000000) end\n"
        "  redis.call('SET', 'prof:last', table.concat(PM, ','))\n  return pa")
    jobs = (lua / "jobs.lua").read_text()
    names = ['candidates', 'reserve_job', 'begin_job', 'finish_job'] + [n for n in os.environ.get("WRAPS", "").split(",") if n]
    # WRAPS=enc,sorted,... also times those helpers (helpers that prelude locals captured before this line are not affected)
    wraps = "\nfor _, n in ipairs({" + ", ".join("'%s'" % n for n in names) + "}) do U.wrap(n) end\n"
    return "\n".join([prelude, jobs, wraps, op])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--lua", default=str(Path(__file__).resolve().parents[3] / "src/Runwick.Redis/Lua"))
    ap.add_argument("--k", default="1,4,16")
    ap.add_argument("--calls", type=int, default=40)
    ap.add_argument("--payload", type=int, default=1024)
    ap.add_argument("--combined", action="store_true", help="also time finish_and_claim (k finishes + a claim of k) against the two calls")
    ap.add_argument("--plain", action="store_true", help="load the scripts as the binary composes them (lean paths included, luascript.py) "
                    "and report only Redis time per call (commandstats); the phase marks need the generic frame")
    a = ap.parse_args()
    lua = Path(a.lua)
    db = Redis(a.port)
    db.call("FLUSHALL")
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", layout(lua), "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    dispatch = db.call("SCRIPT", "LOAD", (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n" + (lua / "ops/dispatch.lua").read_text())
    # plain: the whole script timed in-script as one block (os.clock is CPU time, so a preempted server does not inflate it)
    timed = lambda text: ("local C0 = os.clock()\nlocal R = (function()\n" + text + "\nend)()\n"
                          "redis.call('SET', 'prof:last', 'payloads:' .. math.floor((os.clock() - C0) * 1000000))\nreturn R")
    load = (lambda op: timed(compose(lua, op))) if a.plain else (lambda op: patched(lua, (lua / f"ops/{op}.lua").read_text()))
    rb = db.call("SCRIPT", "LOAD", load("reserve_and_begin"))
    fm = db.call("SCRIPT", "LOAD", load("finish_many"))
    fc = db.call("SCRIPT", "LOAD", patched(lua, (lua / "ops/finish_and_claim.lua").read_text())) if a.combined else None
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

    def finish_args(begun):
        args = [len(begun)]
        for b in begun:
            lease = dict(jobId=b["jobId"], attemptId=b["attemptId"], workerInstanceId=b["workerInstanceId"], ownershipToken=b["ownershipToken"],
                         generation=b["generation"])
            body = dict(lease=lease, outcome="succeeded", elapsed=1, requested=None, error=None, usage=None)
            args += [uuid4().hex, uuid4().hex + uuid4().hex, dl(30_000), json.dumps(body), 0.5, *hint(lua)]
        return args

    def combined(begun, k):
        toks = json.dumps([uuid4().hex for _ in range(k)])
        ents = json.dumps([uuid4().hex[:20] for _ in range(k)])
        r = db.call("EVALSHA", fc, 1, META, 0, STORE, "ReserveAndBegin", uuid4().hex, uuid4().hex + uuid4().hex, dl(30_000), 60_000, FLOOR_OWN,
                    "ownership", "c", "", uuid4().hex, "default", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), k, ents,
                    json.dumps([0] * k), *finish_args(begun))
        assert r[0] == b"R" and r[1] == b"Applied", r
        res = json.loads(r[7])
        assert len(res["items"]) == k and len(res["finished"]) == len(begun), res
        return [i["begin"] for i in res["items"]], marks(), len(r[7])

    def finish(begun):
        args = finish_args(begun)
        r = db.call("EVALSHA", fm, 1, META, 0, STORE, "FinishMany", uuid4().hex, "", dl(30_000), 60_000, 0, "", "d", "", *args)
        assert r[0] == b"R" and r[1] == b"Applied", r
        return marks(), len(r[7])

    def show(name, rows, k):
        if a.plain:
            total = statistics.median(r["payloads"] for r in rows)
            print(f"{name} k={k}: calls={len(rows)}\n  per job: {total / k:.0f} us (in-script total {total:.0f} us)")
            return
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
        total = statistics.median(r["payloads"] for r in rows)
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
        def main_cpu():  # seconds of Redis main-thread CPU (user + system): not inflated when the host preempts the server
            f = dict(l.split(":", 1) for l in db.call("INFO", "cpu").decode().splitlines() if ":" in l)
            return float(f["used_cpu_user_main_thread"]) + float(f["used_cpu_sys_main_thread"])
        db.call("CONFIG", "RESETSTAT")
        c0 = main_cpu()
        pending = []
        for _ in range(a.calls):
            begun, m1, n1 = claim(k)
            cm.append(m1)
            creply.append(n1)
            pending.append(begun)
        claim_stat = evalsha_usec()
        c1 = main_cpu()
        db.call("CONFIG", "RESETSTAT")
        for begun in pending:
            m2, n2 = finish(begun)
            fmk.append(m2)
            freply.append(n2)
        finish_stat = evalsha_usec()
        c2 = main_cpu()
        show("claim (ReserveAndBegin)", cm, k)
        print(f"  reply bytes per call {statistics.median(creply):.0f}; server evalsha {claim_stat[0]:.0f} us per call = {claim_stat[0] / k:.0f} us per job; main-thread CPU {(c1 - c0) * 1e6 / a.calls / k:.0f} us per job")
        show("finish (FinishMany)", fmk, k)
        print(f"  reply bytes per call {statistics.median(freply):.0f}; server evalsha {finish_stat[0]:.0f} us per call = {finish_stat[0] / k:.0f} us per job; main-thread CPU {(c2 - c1) * 1e6 / a.calls / k:.0f} us per job")
        if fc:
            seed(k * (a.calls + 2))
            begun = claim(k)[0]
            rows = []
            db.call("CONFIG", "RESETSTAT")
            for _ in range(a.calls):
                begun, m3, _n = combined(begun, k)
                rows.append(m3)
            comb_stat = evalsha_usec()
            finish(begun)
            show("combined (finish_and_claim, k finishes + claim of k)", rows, k)
            print(f"  server evalsha {comb_stat[0]:.0f} us per call = {comb_stat[0] / k:.0f} us per job (finish + claim);"
                  f" separate calls {claim_stat[0] + finish_stat[0]:.0f} us")


main()
