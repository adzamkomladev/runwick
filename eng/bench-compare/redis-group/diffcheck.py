#!/usr/bin/env python3
"""Do the changed scripts store byte-identical data and reply byte-identical text? Runs the same dispatch / claim / finish / finish-and-claim sequence (fixed ids, tokens, entropy, draws) through the
baseline Lua and the checkout's Lua on two disposable Redis databases, dumps every key and compares them, with 13-digit millisecond times
replaced by T. Usage: diffcheck.py --port P [--base DIR] [--new DIR]"""
import argparse, json, re, socket, sys
from pathlib import Path

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


def run(lua, port):
    db = Redis(port)
    db.call("FLUSHALL")
    db.call("HSET", META, "store", STORE, "mode", "normal", "layout", 1, "epoch", 1, "admission", "open", "bf", "done")
    db.call("HSET", PREFIX + "queues", "default", "{}")
    prelude = (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n"
    dispatch = db.call("SCRIPT", "LOAD", prelude + (lua / "ops/dispatch.lua").read_text())
    rb = db.call("SCRIPT", "LOAD", prelude + (lua / "ops/reserve_and_begin.lua").read_text())
    fm = db.call("SCRIPT", "LOAD", prelude + (lua / "ops/finish_many.lua").read_text())
    fin = db.call("SCRIPT", "LOAD", prelude + (lua / "ops/finish.lua").read_text())
    fc = db.call("SCRIPT", "LOAD", prelude + (lua / "ops/finish_and_claim.lua").read_text())
    replies = []

    def dl(ahead):
        sec, usec = map(int, db.call("TIME"))
        return sec * 1000 + usec // 1000 + ahead

    n = 8
    for i in range(n):
        policy = dict(maxExec=3, maxUn=3, retry='{"initialDelay":100,"maximumDelay":1000}', uretry='{"initialDelay":100,"maximumDelay":1000}', timeout=None)
        body = dict(jobId=hexid(i, 1), name="test", queue="default", priority="medium", pver=1, profile="json", profver=1, notBefore=None,
                    akey=None, wkey=None, wms=None, biz="0" * 64, pol="0" * 64, headers='{"business":{},"diagnostic":{}}', policy=policy,
                    scopes=[], rates=[])
        r = db.call("EVALSHA", dispatch, 1, META, 0, STORE, "Dispatch", hexid(i, 2), hexid(i, 3) * 2, dl(300_000), 600_000, 86_400_000,
                    "durable", "c", "", 0, 0, "initial", "", 1, json.dumps(body), b"payload-%d" % i)
        assert r[1] == b"Applied", r
        replies.append(r)
    toks = json.dumps([hexid(i, 4) for i in range(5)])
    ents = json.dumps([f"{i:020x}" for i in range(5)])
    r = db.call("EVALSHA", rb, 1, META, 0, STORE, "ReserveAndBegin", hexid(0, 5), hexid(0, 6) * 2, dl(30_000), 60_000, 600_000, "ownership", "c", "",
                hexid(0, 7), "default", "medium", 600_000, toks, "", "", json.dumps([["test", 1, "json", 1]]), 5, ents, json.dumps([0] * 5))
    assert r[1] == b"Applied", r
    replies.append(r)
    items = json.loads(r[7])["items"]
    lease = lambda b: dict(jobId=b["jobId"], attemptId=b["attemptId"], workerInstanceId=b["workerInstanceId"], ownershipToken=b["ownershipToken"],
                           generation=b["generation"])
    fbody = lambda i, outcome: json.dumps(dict(lease=lease(items[i]["begin"]), outcome=outcome, elapsed=1, requested=None, error=None, usage=None))
    args = [3]
    for i in range(3):
        args += [hexid(i, 8), hexid(i, 9) * 2, dl(30_000), fbody(i, "succeeded"), 0.5]
    r = db.call("EVALSHA", fm, 1, META, 0, STORE, "FinishMany", hexid(0, 10), "", dl(30_000), 60_000, 0, "", "d", "", *args)
    assert r[1] == b"Applied", r
    replies.append(r)
    r = db.call("EVALSHA", fin, 1, META, 0, STORE, "Finish", hexid(3, 8), hexid(3, 9) * 2, dl(30_000), 60_000, 86_400_000, "durable", "d", "",
                fbody(3, "retryable_failure"), 0.5)
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

    dump = {}
    for key in db.call("KEYS", PREFIX + "*"):
        key = key.decode()
        t = db.call("TYPE", key).decode()
        if t == "hash":
            flat = db.call("HGETALL", key)
            val = {flat[i].decode(): flat[i + 1].decode("latin1") for i in range(0, len(flat), 2)}
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
    old = run(Path(a.base), a.port)
    new = run(Path(a.new), a.port)
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
