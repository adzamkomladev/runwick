#!/usr/bin/env python3
"""python3 summarize.py DIR -> DIR/summary.md: per database and test, arm medians (min-max) and the median paired Laravel/Runwick ratio."""
import glob, json, os, re, statistics as st, sys
d = sys.argv[1]; runs = {}
for f in glob.glob(f"{d}/r*-*.json"):
    m = re.match(r"r(\d+)-(sqlite|pg)-(drain|steady-\d+)-(runwick|laravel)\.json$", os.path.basename(f))
    if m and not json.load(open(f)).get("failed"): runs[(int(m[1]), m[2], m[3], m[4])] = json.load(open(f))  # a run with failed jobs is not a measurement
def med(xs): return "n/a" if not xs else f"{st.median(xs):,.1f} ({min(xs):,.1f}-{max(xs):,.1f})"
def get(j, path):
    for k in path.split("."):
        j = (j or {}).get(k)
    return j
out = ["| Database | Test | Metric | Runwick median (range) | Laravel median (range) | Paired Laravel/Runwick median (range) | Pairs |", "|---|---|---|---|---|---|---|"]
for db in ("sqlite", "pg"):
    for test in sorted({k[2] for k in runs}, key=lambda t: (t != "drain", t)):
        metrics = [("completions/s", "completionsPerSec")] + ([] if test == "drain" else [("due-to-done p50 ms", "latencyMs.p50"), ("due-to-done p99 ms", "latencyMs.p99")])
        for label, path in metrics:
            rounds = sorted({k[0] for k in runs if k[1:3] == (db, test)})
            a = {r: get(runs.get((r, db, test, "runwick")), path) for r in rounds}; b = {r: get(runs.get((r, db, test, "laravel")), path) for r in rounds}
            pairs = [b[r] / a[r] for r in rounds if a[r] and b[r]]
            if not (a or b): continue
            out.append(f"| {db} | {test} | {label} | {med([v for v in a.values() if v is not None])} | {med([v for v in b.values() if v is not None])} | {med(pairs) if pairs else 'n/a'} | {len(pairs)} |")
open(f"{d}/summary.md", "w").write("\n".join(out) + "\n"); print("\n".join(out))
