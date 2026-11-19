#!/usr/bin/env python3
"""Per-arm drain summary of pairs-combined.sh results: jobs/s, Redis script time per job (INFO commandstats, wall clock inside Redis),
Redis main-thread CPU per job, AOF bytes per job (insert included), and the load average at the start of each run. usage: summarize_drain.py results/<tag> [jobs]"""
import collections, json, pathlib, re, statistics, sys

d = pathlib.Path(sys.argv[1]); jobs = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
rows = collections.defaultdict(list)
for j in sorted(d.glob("*.json")):
    rnd, arm = j.stem.split("-", 1)
    data = json.loads(j.read_text())
    rate = data.get("completionsPerSec") or data.get("jobsPerSec") or data.get("throughput")
    red = (d / f"{j.stem}.redis").read_text() if (d / f"{j.stem}.redis").exists() else ""
    us = sum(int(m) for m in re.findall(r"cmdstat_(?:evalsha|eval|fcall|evalsha_ro|fcall_ro):calls=\d+,usec=(\d+)", red))
    cpu = sum(float(m) for m in re.findall(r"used_cpu_(?:user|sys)_main_thread:([\d.]+)", red))
    aof = sum(int(m) for m in re.findall(r"aof_current_size:(\d+)", red))
    load = (d / f"{j.stem}.load").read_text().split("average:")[-1].split(",")[0].strip() if (d / f"{j.stem}.load").exists() else "?"
    rows[arm].append((int(rnd), rate, us / jobs / 1000, cpu / jobs * 1000, load, aof / jobs))
print(f"{'arm':8s} {'jobs/s per round':28s} {'median':>7s} {'script ms/job':>14s} {'cpu ms/job':>11s} {'AOF B/job':>10s}  load")
for arm, r in rows.items():
    r.sort()
    print(f"{arm:8s} {', '.join(f'{x[1]:.0f}' for x in r):28s} {statistics.median(x[1] for x in r):7.0f} "
          f"{statistics.median(x[2] for x in r):14.2f} {statistics.median(x[3] for x in r):11.2f} {statistics.median(x[5] for x in r):10.0f}  {', '.join(x[4] for x in r)}")
