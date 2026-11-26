#!/usr/bin/env python3
"""Summarize `stage2 --part concurrent`: per caller count and arm, median and range over rounds."""
import json, statistics as st, sys
d = json.load(open(sys.argv[1]))
print(f"{d['store']}: {d['opsPerArm']} ops per arm, {d['rounds']} rounds, tmpdir {d['tmpdir']}")
keys = sorted({(r['callers'], r['arm']) for r in d['results']}, key=lambda k: (k[0], ['public', 'prepared', 'store'].index(k[1])))
print(f"{'callers':>7} {'arm':9} {'ops/s median (range)':26} {'client cpu ms/op':>17} {'KiB/op':>7} {'store cpu ms/op':>16} {'gen0/gen2 per arm':>18}")
for c, arm in keys:
    xs = [r for r in d['results'] if r['callers'] == c and r['arm'] == arm]
    v = [x['opsPerSec'] for x in xs]
    print(f"{c:7} {arm:9} {st.median(v):9.0f} ({min(v):.0f}-{max(v):.0f}){'':6} {st.median([x['clientCpuMsPerOp'] for x in xs]):17.3f} {st.median([x['allocBytesPerOp'] for x in xs])/1024:7.1f} {st.median([x['storeCpuMsPerOp'] for x in xs]):16.3f} {st.median([x['gcGen0'] for x in xs]):9.0f}/{st.median([x['gcGen2'] for x in xs]):.0f}")
