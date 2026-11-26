#!/usr/bin/env python3
"""Summarize a `stage2 --part dispatch` result: per arm median and range over rounds (ms/op, client CPU ms/op, KiB/op, store CPU ms/op, store commands/op)."""
import json, statistics as st, sys
d = json.load(open(sys.argv[1]))
print(f"{d['store']}  payload {d['payloadBytes']} B, {d['opsPerSingleArm']} single ops, {d['batchesPerArm']} batches of {d['batchItems']}, {d['rounds']} rounds, tmpdir {d['tmpdir']}")
arms = {}
for x in d['arms']: arms.setdefault(x['arm'], []).append(x)
def med(k, xs): v = [x[k] for x in xs]; return f"{st.median(v):.3f} ({min(v):.3f}-{max(v):.3f})"
print(f"{'arm':16} {'ms/op median (range)':26} {'client cpu ms/op':26} {'KiB/op':>8} {'store cpu ms/op':26} cmds/op")
for a, xs in arms.items():
    cmds = [x['storeCountersPerOp'].get('commandCalls') or x['storeCountersPerOp'].get('transactions') for x in xs if x['storeCountersPerOp']]
    print(f"{a:16} {med('meanMs', xs):26} {med('clientCpuMsPerOp', xs):26} {st.median([x['allocBytesPerOp'] for x in xs])/1024:8.1f} {med('storeCpuMsPerOp', xs):26} {st.median(cmds) if cmds else '-'}")
print("preparation split (ms per job, median):", {k: round(st.median([s[k] for s in d['preparationSplit']]), 4) for k in ('serializePayloadMs', 'prepareItemMs', 'digestsMs', 'prepareSingleMs')})
