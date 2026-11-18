#!/usr/bin/env python3
"""Redis commands issued inside each script, from a MONITOR capture: per operation, calls, nested commands per call by type. usage: monparse.py <file> <jobs>"""
import re, sys, collections

tok = re.compile(r'"((?:[^"\\]|\\.)*)"')
calls = collections.Counter()
nested = collections.defaultdict(collections.Counter)
cur = None
for line in open(sys.argv[1], errors="replace"):
    m = re.match(r"^[\d.]+ \[\d+ ([^\]]+)\] (.*)$", line.rstrip("\n"))
    if not m:
        continue
    who, rest = m.groups()
    t = tok.findall(rest[:4000])
    if not t:
        continue
    cmd = t[0].upper()
    if who != "lua":
        if cmd in ("EVALSHA", "EVAL") and len(t) > 6:
            op = t[6]
            if op == "ReserveAndBegin" and len(t) > 26:
                op = "FinishAndClaim"
            cur = op
            calls[op] += 1
        else:
            cur = None
            calls[cmd] += 1
    elif cur:
        nested[cur][cmd] += 1
jobs = int(sys.argv[2])
for op, n in calls.most_common():
    if op not in nested:
        continue
    c = nested[op]
    tot = sum(c.values())
    print(f"{op}: {n} calls, {tot / n:.1f} commands per call, {tot / jobs:.2f} per drained job: " +
          ", ".join(f"{k} {v / n:.1f}" for k, v in c.most_common()))
