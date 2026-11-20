#!/usr/bin/env python3
"""Summarizes profile_worker.py outputs (several variants, several rounds appended to one file per variant, each round headed
"== <variant> round <n> load <x>"): in-script CPU µs per job and the fixed part (setup + meta + receipt lookup) per call, for claim,
finish and finish_and_claim at each k, as the median over rounds. usage: profsum.py prof-main.txt prof-fr.txt ..."""
import collections, re, statistics, sys

rows = collections.defaultdict(lambda: collections.defaultdict(list))  # variant -> (step, k) -> [per job]
fixed = collections.defaultdict(lambda: collections.defaultdict(list))
order = []
for path in sys.argv[1:]:
    variant, step, k = None, None, None
    for line in open(path):
        m = re.match(r"== (\S+) round", line)
        if m:
            variant = m.group(1)
            if variant not in order: order.append(variant)
            continue
        m = re.match(r"(claim|finish|combined) .*k=(\d+)", line)
        if m:
            step, k = m.group(1), int(m.group(2)); continue
        m = re.match(r"  rcpt_lookup\s+at\s+(\d+)", line)
        if m and step: fixed[variant][step, k].append(int(m.group(1)))
        m = re.match(r"  per job: (\d+) us", line)
        if m and step: rows[variant][step, k].append(int(m.group(1)))
cols = sorted({c for v in rows.values() for c in v}, key=lambda c: (c[1], c[0]))
print(f"{'variant':10s}" + "".join(f"{s + ' k=' + str(k):>16s}" for s, k in cols) + "   (in-script CPU us per job, median of rounds)")
for v in order:
    print(f"{v:10s}" + "".join(f"{statistics.median(rows[v][c]):16.0f}" if rows[v][c] else f"{'-':>16s}" for c in cols))
print(f"\n{'variant':10s}" + "".join(f"{s + ' k=' + str(k):>16s}" for s, k in cols) + "   (fixed part per call: setup, meta and gates, receipt lookup)")
for v in order:
    print(f"{v:10s}" + "".join(f"{statistics.median(fixed[v][c]):16.0f}" if fixed[v][c] else f"{'-':>16s}" for c in cols))
