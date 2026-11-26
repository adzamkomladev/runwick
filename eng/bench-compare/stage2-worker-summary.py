#!/usr/bin/env python3
"""Summarize a directory of `stage2 --part worker` results: per run kind, jobs/s median (range) over rounds, client and store CPU per job, handler-return-to-confirm p99,
mean claim and finish batch sizes, share of samples with an idle slot and unclaimed backlog (direct arm only)."""
import glob, json, os, re, statistics as st, sys
d = sys.argv[1]; kinds = {}
for f in sorted(glob.glob(f"{d}/*.json")):
    name = os.path.basename(f)[:-5]; kind = re.sub(r"-r\d+$", "", name)
    try: j = json.load(open(f))
    except Exception: continue
    kinds.setdefault(kind, []).append(j)
print(f"{'run kind':34} {'n':>2} {'jobs/s median (range)':26} {'cli cpu':>7} {'store cpu':>9} {'p99 ret→conf ms':>15} {'claim sz':>8} {'fin sz':>6} {'idle+backlog':>12} {'idle%':>5}")
for k, js in kinds.items():
    ok = [j for j in js if j.get("confirmed") == j.get("jobs")]
    v = [j["jobsPerSec"] for j in ok] or [0]
    L = lambda j, key: j["storeCallLatencies"].get(key, {}).get("meanMs")
    med = lambda xs: f"{st.median(xs):.1f}" if xs else "-"
    idle = [j["idleWithBacklog"]["idleSlotWithBacklogShare"] for j in ok if j["idleWithBacklog"]["idleSlotWithBacklogShare"] is not None]
    print(f"{k:34} {len(ok):2} {st.median(v):8.0f} ({min(v):.0f}-{max(v):.0f}){'':5} {st.median([j['clientCpuMsPerJob'] for j in ok] or [0]):7.2f} {st.median([j['storeCpuMsPerJob'] for j in ok] or [0]):9.2f} "
          f"{st.median([j['handlerReturnToConfirmMs']['p99'] for j in ok] or [0]):15.1f} {med([x for x in (L(j, 'reserveAndBeginSize') for j in ok) if x]):>8} {med([x for x in (L(j, 'finishManySize') for j in ok) if x]):>6} "
          f"{(f'{st.median(idle):.2f}' if idle else '-'):>12} {st.median([j['cpuIdlePctBefore'] for j in js]):5.0f}")
