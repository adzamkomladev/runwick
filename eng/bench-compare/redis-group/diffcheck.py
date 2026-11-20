#!/usr/bin/env python3
"""Do the changed scripts store byte-identical data and reply byte-identical text? Runs the same dispatch / claim / finish / finish-and-claim sequence (fixed ids, tokens, entropy, draws) through the
baseline Lua and the checkout's Lua on two disposable Redis databases, dumps every key and compares them, with 13-digit millisecond times
replaced by T. Receipt results are compared as replied (a compact result, 22 §3.2, expanded with the checkout's decoder; a missing code or
detail field reads as empty). Also covers a claim whose items are not plain Applied ones (fenced, capacity blocked, cancelled before start,
budget exhausted), a single Reserve, Begin and Finish, and a resend (replay) of every command. Usage: diffcheck.py --port P [--base DIR] [--new DIR]"""
import argparse, json, re, socket, sys
from pathlib import Path
from luascript import compose

STORE = "11" * 16
PREFIX = "rw:{" + b"diffapp".hex() + "}:"
META = PREFIX + "meta"
HERE = Path(__file__).resolve().parent


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


def hexid(n, kind):
    return f"{kind:04x}{n:028x}"


def run(lua, port, decoder):
    db = Redis(port)
    db.call("FLUSHALL")
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", 1, "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    load = lambda op: db.call("SCRIPT", "LOAD", compose(lua, op))
    dispatch, rb, fm, fin, fc, ren = (load(op) for op in ("dispatch", "reserve_and_begin", "finish_many", "finish", "finish_and_claim", "renew"))
    replies = []

    def dl(ahead):
        sec, usec = map(int, db.call("TIME"))
        return sec * 1000 + usec // 1000 + ahead

    n = 8
    for i in range(n):
        policy = dict(maxExec=3, maxUn=3, retry='{"initialDelay":100,"maximumDelay":1000}', uretry='{"initialDelay":100,"maximumDelay":1000}', timeout=None)
        body = dict(jobId=hexid(i, 1), name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey="k0" if i == 0 else None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=[], rates=[])
        r = db.call("EVALSHA", dispatch, 1, META, 0, STORE, "Dispatch", hexid(i, 2), hexid(i, 3) * 2, dl(300_000), 600_000, 86_400_000,
                    "durable", "c", "", 0, 0, "initial", "", 1, json.dumps(body), b"payload-%d" % i)
        assert r[1] == b"Applied", r
        replies.append(r)
    toks = json.dumps([hexid(i, 4) for i in range(5)])
    ents = json.dumps([f"{i:020x}" for i in range(5)])
    claim = ["EVALSHA", rb, 1, META, 0, STORE, "ReserveAndBegin", hexid(0, 5), hexid(0, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "",
             hexid(0, 7), "default", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), 5, ents, json.dumps([0] * 5)]
    r = db.call(*claim)
    assert r[1] == b"Applied", r
    replies.append(r)
    items = json.loads(r[7])["items"]
    lease = lambda b: dict(jobId=b["jobId"], attemptId=b["attemptId"], workerInstanceId=b["workerInstanceId"], ownershipToken=b["ownershipToken"],
                           generation=b["generation"])
    fbody = lambda i, outcome, **kw: json.dumps(dict(dict(lease=lease(items[i]["begin"]), outcome=outcome, elapsed=1, requested=None, error=None,
                                                         usage=None), **kw))
    # Renewals: RenewMany of two owned leases and a stale one, a single Renew, a single Renew of the stale lease (no effect), each resent.
    stale = dict(lease(items[2]["begin"]), ownershipToken="ff" * 16)
    twice = dict(lease(items[3]["begin"]), ownershipToken="ff" * 16)  # the same job twice in one RenewMany
    for many, leases in (("1", [lease(items[3]["begin"]), lease(items[4]["begin"]), stale]), ("0", [lease(items[3]["begin"])]), ("0", [stale]),
                         ("1", [stale]), ("1", [lease(items[3]["begin"]), twice])):
        call = ["EVALSHA", ren, 1, META, 0, STORE, "RenewMany" if many == "1" else "Renew", hexid(len(replies), 13), "", dl(30_000), 60_000, 0, "",
                "d", "", 600_000, many, json.dumps(leases)]
        for _ in range(2):
            replies.append(db.call(*call))
    args = [3]
    kws = [dict(), dict(usage='{"cpuMs":1}'), dict(error='{"code":"x","message":"m\\n\\"q\\""}', requested=50)]
    for i in range(3):
        args += [hexid(i, 8), hexid(i, 9) * 2, dl(30_000), fbody(i, "retryable_failure" if i == 2 else "succeeded", **kws[i]), 0.5]
    many = ["EVALSHA", fm, 1, META, 0, STORE, "FinishMany", hexid(0, 10), "", dl(30_000), 60_000, 0, "", "d", "", *args]
    r = db.call(*many)
    assert r[1] == b"Applied", r
    replies.append(r)
    single = ["EVALSHA", fin, 1, META, 0, STORE, "Finish", hexid(3, 8), hexid(3, 9) * 2, dl(30_000), 60_000, 86_400_000, "durable", "d", "",
              fbody(3, "retryable_failure"), 0.5]
    r = db.call(*single)
    assert r[1] == b"Applied", r
    replies.append(r)
    # Lost replies: each resend replays from its receipt.
    for call in (claim, many, single):
        r = db.call(*call)
        assert r[1] == b"Applied", r
        replies.append(r)
    # FinishAndClaim: the last begun job finishes and three new jobs are claimed in one call; then a resend of the same call replays.
    toks = json.dumps([hexid(i, 11) for i in range(3)])
    ents = json.dumps([f"{i + 100:020x}" for i in range(3)])
    fargs = [1, hexid(4, 8), hexid(4, 9) * 2, dl(30_000), fbody(4, "permanent_failure"), 0.5]
    call = ["EVALSHA", fc, 1, META, 0, STORE, "ReserveAndBegin", hexid(1, 5), hexid(1, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "",
            hexid(1, 7), "default", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), 3, ents, json.dumps([0] * 3), *fargs]
    r = db.call(*call)
    assert r[1] == b"Applied" and len(json.loads(r[7])["items"]) == 3, r
    replies.append(r)
    r = db.call(*call)
    assert r[3] == b"1", r
    replies.append(r)

    # Claim items that are not plain Applied ones (each kept as replied text): a fenced resource scope, a second job of that scope
    # (capacity blocked), a job with a cancel request (cancelled before start) and one with its execution budget used up; then a
    # single Reserve, Begin and Finish on a fifth job. Each is resent once (a lost reply).
    db.call("HSET", PREFIX + "queues", "q2", "{}")
    scope = [dict(kind="resource", key="r1", id=hexid(0, 20), capacity=1)]
    for i in range(10, 15):
        policy = dict(maxExec=3, maxUn=3, retry='{"initialDelay":100,"maximumDelay":1000}', uretry='{"initialDelay":100,"maximumDelay":1000}', timeout=5000)
        body = dict(jobId=hexid(i, 1), name="test", queue="q2", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=scope if i in (10, 11) else [], rates=[])
        r = db.call("EVALSHA", dispatch, 1, META, 0, STORE, "Dispatch", hexid(i, 2), hexid(i, 3) * 2, dl(300_000), 600_000, 86_400_000,
                    "durable", "c", "", 0, 0, "initial", "", 1, json.dumps(body), b"payload-%d" % i)
        assert r[1] == b"Applied", r
    db.call("HSET", PREFIX + "job:" + hexid(12, 1), "cancelat", "1")
    db.call("HSET", PREFIX + "job:" + hexid(13, 1), "g:1:started", "3")
    db.call("ZADD", PREFIX + "ready:q2:medium", 1, hexid(10, 1), 2, hexid(11, 1), 3, hexid(12, 1), 4, hexid(13, 1), 5, hexid(14, 1))
    toks = json.dumps([hexid(i, 12) for i in range(4)])
    ents = json.dumps([f"{i + 200:020x}" for i in range(4)])
    mixed = ["EVALSHA", rb, 1, META, 0, STORE, "ReserveAndBegin", hexid(2, 5), hexid(2, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "",
             hexid(2, 7), "q2", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), 4, ents, json.dumps([0] * 4)]
    reserve, begin = load("reserve"), load("begin")
    one = ["EVALSHA", reserve, 1, META, 0, STORE, "Reserve", hexid(3, 5), hexid(3, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "",
           hexid(3, 7), "q2", "medium", 600_000, hexid(3, 12), "4", hexid(13, 1), json.dumps([["test", 1, "json", 1]]), f"{300:020x}"]
    for c in (mixed, mixed, one, one):
        r = db.call(*c)
        assert r[1] == b"Applied", r
        replies.append(r)
        if c is mixed:
            items = json.loads(r[7])["items"]
            assert [(i["status"], bool(i.get("reserve", {}).get("fences"))) for i in items] == [
                ("Applied", True), ("CapacityBlocked", False), ("InvalidState", False), ("BudgetExhausted", False)], items
    res = json.loads(r[7])
    lease = json.dumps(dict(jobId=res["jobId"], attemptId=res["attemptId"], workerInstanceId=res["workerInstanceId"], ownershipToken=res["ownershipToken"],
                            generation=res["generation"]))
    beg = ["EVALSHA", begin, 1, META, 0, STORE, "Begin", hexid(4, 5), hexid(4, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "", lease, 0]
    fin2 = ["EVALSHA", fin, 1, META, 0, STORE, "Finish", hexid(14, 8), hexid(14, 9) * 2, dl(30_000), 60_000, 86_400_000, "durable", "d", "",
            json.dumps(dict(lease=json.loads(lease), outcome="succeeded", elapsed=2, requested=None, error=None, usage=None)), 0.5]
    for c in (beg, beg, fin2, fin2):
        r = db.call(*c)
        assert r[1] == b"Applied", r
        replies.append(r)

    expand = db.call("SCRIPT", "LOAD", decoder + "return U.cdec(ARGV[12])")
    dump = {}
    for key in db.call("KEYS", PREFIX + "*"):
        key = key.decode()
        t = db.call("TYPE", key).decode()
        if t == "hash":
            flat = db.call("HGETALL", key)
            val = {flat[i].decode(): flat[i + 1].decode("latin1") for i in range(0, len(flat), 2)}
            if ":rcpt:" in key and not key.endswith(("exp:durable", "exp:ownership")):
                val.setdefault("code", ""); val.setdefault("detail", "")
                if "result" in val:
                    val["result"] = db.call("EVALSHA", expand, 1, META, *[""] * 11, val["result"].encode("latin1")).decode("latin1")
        elif t == "zset":
            flat = db.call("ZRANGE", key, 0, -1, "WITHSCORES")
            val = {flat[i].decode(): flat[i + 1].decode() for i in range(0, len(flat), 2)}
        else:
            val = (db.call("GET", key) or b"").decode("latin1")
        dump[key] = val
    dump["(replies)"] = [[x.decode("latin1") if isinstance(x, bytes) else x for x in r] for r in replies]
    return dump


def norm(x):
    s = json.dumps(x, sort_keys=True)
    s = re.sub(r"01a1[0-9a-f]{8}7", "A7", s)  # the 12 hex digits of a UUIDv7 attempt id are the decision time
    return re.sub(r"\d{13}", "T", s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--base", required=True, help="a copy of the baseline Lua directory (prelude.lua, jobs.lua, ops/)")
    ap.add_argument("--new", default=str(HERE.parents[2] / "src/Runwick.Redis/Lua"))
    a = ap.parse_args()
    lua = Path(a.new)
    decoder = (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n"
    old = run(Path(a.base), a.port, decoder)
    new = run(lua, a.port, decoder)
    bad = 0
    for k in sorted(set(old) | set(new)):
        o, n = old.get(k), new.get(k)
        if o is None or n is None:
            print("only in", "base" if n is None else "new", k)
            bad += 1
        elif k == "(replies)":
            for i, (ro, rn) in enumerate(zip(o, n)):
                if norm(ro) != norm(rn):  # the exact reply text, byte for byte apart from times
                    print("DIFF reply", i, "\n  base:", norm(ro)[:400], "\n  new: ", norm(rn)[:400])
                    bad += 1
        elif isinstance(o, dict) and isinstance(n, dict):
            on = {norm(f): norm(v) for f, v in o.items()}
            nn = {norm(f): norm(v) for f, v in n.items()}
            for f in sorted(set(on) | set(nn)):
                if on.get(f) != nn.get(f):
                    print("DIFF", k, f, "\n  base:", (on.get(f) or "null")[:300], "\n  new: ", (nn.get(f) or "null")[:300])
                    bad += 1
        elif norm(o) != norm(n):
            print("DIFF", k)
            bad += 1
    print(f"{len(old)} keys compared; {bad} differences")
    sys.exit(1 if bad else 0)


main()
