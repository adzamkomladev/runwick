#!/usr/bin/env python3
"""Per-operation wall time from SLOWLOG (threshold 0): calls, total ms, us/call, us per drained job. usage: slowparse.py <port> [jobs]"""
import socket, sys, statistics, collections


class R:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=300)
        self.f = self.s.makefile("rb")

    def read(self):
        line = self.f.readline()
        k, v = line[:1], line[1:-2]
        if k == b"+": return v
        if k == b"-": raise RuntimeError(v)
        if k == b":": return int(v)
        n = int(v)
        if n < 0: return None
        if k == b"$":
            d = self.f.read(n); self.f.read(2); return d
        return [self.read() for _ in range(n)]

    def call(self, *a):
        enc = [x if isinstance(x, bytes) else str(x).encode() for x in a]
        self.s.sendall(b"*%d\r\n" % len(enc) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in enc))
        return self.read()


port = int(sys.argv[1]); jobs = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
r = R(port)
log = r.call("SLOWLOG", "GET", "-1")
by = collections.defaultdict(list)
items = collections.defaultdict(int)
for e in log:
    dur, args = e[2], e[3]
    cmd = args[0].decode().upper()
    if cmd in ("EVALSHA", "EVAL") and len(args) > 6:
        op = args[6].decode()
        if op == "ReserveAndBegin" and len(args) > 26: op = "FinishAndClaim"
        if op == "FinishMany" and len(args) > 15: items[op] += int(args[15])
        if op == "FinishAndClaim" and len(args) > 26: items[op] += int(args[26])
        key = op
    else:
        key = cmd
    by[key].append(dur)
tot = sum(sum(v) for v in by.values())
print(f"entries {len(log)} total_ms {tot/1000:.0f} per_job_us {tot/jobs:.0f}")
print(f"{'op':22s} {'calls':>7s} {'total_ms':>9s} {'us/call':>8s} {'p50':>7s} {'p95':>7s} {'us/job':>7s} items")
for k, v in sorted(by.items(), key=lambda kv: -sum(kv[1])):
    v.sort()
    print(f"{k:22s} {len(v):7d} {sum(v)/1000:9.0f} {sum(v)/len(v):8.0f} {v[len(v)//2]:7d} {v[int(len(v)*.95)]:7d} {sum(v)/jobs:7.0f} {items.get(k, '')}")
