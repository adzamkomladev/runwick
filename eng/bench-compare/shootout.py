#!/usr/bin/env python3
"""Queue shootout runner (docs/reports/queue-shootout.md). ONE system config at a time, never two at once.
  shootout.py --out DIR --rounds 3 [--configs a,b,c] [--tests t1,t2]     rounds rotate the config order
  shootout.py --out DIR --one CONFIG --round 1                            a single config, one round
Configs: runwick-pg runwick-sqlite runwick-redis river pgboss fastendpoints bullmq-everysec bullmq-always horizon-everysec horizon-always laravel-sqlite laravel-pg
Needs: runwick-pg (127.0.0.1:55499; laravel-pg uses it too) and two throwaway Redis containers, bench-redis-everysec (56391) and bench-redis-always (56392).
Each run: waits up to --quiet-wait seconds for 1-minute load < 8, records nproc/load/top processes before and after, flags load > 8 at start."""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True); ap.add_argument("--rounds", type=int, default=3); ap.add_argument("--one"); ap.add_argument("--round", type=int, default=1)
ap.add_argument("--configs", default="runwick-pg,runwick-sqlite,runwick-redis,river,pgboss,fastendpoints,bullmq-everysec,bullmq-always,horizon-everysec,horizon-always,laravel-sqlite,laravel-pg")
ap.add_argument("--tests", default="enqueue-c1,enqueue-c16,bulk,drain-w1,drain-w16,drain-w64,steady-100,steady-500")
ap.add_argument("--scale", type=int, default=1, help="divide job counts (smoke test only)"); ap.add_argument("--quiet-wait", type=int, default=300); ap.add_argument("--steady-seconds", type=int, default=30)
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
RUNWICK = ["dotnet", f"{ROOT}/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll", "bench"]
REDIS = {"everysec": "127.0.0.1:56391", "always": "127.0.0.1:56392"}
N = {"enqueue-c1": 2000, "enqueue-c16": 10000, "bulk": 20000}
MAXS = {"drain-w1": 120, "drain-w16": 300, "drain-w64": 300}  # a drain still running at the cap is reported as finished/seconds so far

def cmd(cfg, test, outf):
    kind, _, variant = cfg.partition("-")
    mode = test.split("-")[0]; k = int(test.split("-c")[1]) if test.startswith("enqueue") else None
    w = int(test.split("-w")[1]) if test.startswith("drain") else 16
    rate = int(test.split("-")[1]) if mode == "steady" else None
    n = N.get(test, 20000) // a.scale; secs = a.steady_seconds
    if kind == "runwick":
        store = {"pg": "postgres", "sqlite": "sqlite", "redis": "redis"}[variant]
        c = RUNWICK + ["--store", store, "--mode", mode, "--n", str(n), "--workers", str(w), "--out", outf]
        if store == "redis": c += ["--redis", REDIS["always"]]
        if mode == "enqueue": c += ["--calls", str(k)]
        if mode == "bulk": c += ["--calls", "1"]
        if mode == "drain": c += ["--calls", "1" if store == "redis" else "8", "--max-seconds", str(MAXS[test])]  # 8 concurrent DispatchMany callers on Redis end in "outcome unknown" (see report)
        if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs), "--calls", "16"]
        return c
    if kind == "river":
        c = [f"{HERE}/river/bench-river", "-mode", mode, "-n", str(n), "-workers", str(w), "-out", outf]
        if mode == "enqueue": c += ["-calls", str(k)]
        if mode == "drain": c += ["-cooldown-ms", "1", "-max-seconds", str(MAXS[test])]
        if mode == "steady": c += ["-rate", str(rate), "-seconds", str(secs), "-calls", "16", "-cooldown-ms", "1"]  # same throughput setting as the drains; default 100 ms caps 16 workers near 160 jobs/s
        return c
    if kind == "pgboss":
        c = ["node", f"{HERE}/pgboss/bench.mjs", "--mode", mode, "--n", str(n), "--out", outf]
        if mode == "enqueue": c += ["--calls", str(k), "--concurrency", str(k)]
        if mode == "bulk": c += ["--concurrency", "1"]
        if mode == "drain": c += ["--concurrency", "1", "--batch", str(w), "--burst", "--max-seconds", str(MAXS[test])]
        if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs), "--concurrency", "1", "--batch", str(rate), "--burst", "--calls", "16"]  # polls every 0.5 s (its minimum), so the batch must hold one second of arrivals; batch 1 x 16 workers = 32 jobs/s at most
        return c
    if kind == "fastendpoints":
        if mode == "bulk": return None  # FastEndpoints has no bulk-queue API
        c = ["dotnet", f"{HERE}/fastendpoints/bin/Release/net10.0/bench-fe.dll", "--mode", mode, "--n", str(n), "--workers", str(w), "--out", outf]
        if mode == "enqueue": c += ["--calls", str(k)]
        if mode == "drain": c += ["--max-seconds", str(MAXS[test])]
        if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs)]
        return c
    if kind in ("bullmq", "horizon"):
        r = REDIS[variant]
        if kind == "bullmq":
            c = ["node", f"{HERE}/bullmq/bench.mjs", "--redis", r, "--mode", mode, "--n", str(n), "--concurrency", str(w), "--out", outf]
            if mode == "enqueue": c += ["--calls", str(k)]
            if mode == "drain": c += ["--max-seconds", str(MAXS[test])]
            if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs)]
        else:
            c = ["python3", f"{HERE}/horizon/bench.py", "--redis", r, "--mode", mode, "--n", str(n), "--workers", str(w), "--out", outf]
            if mode == "enqueue": c += ["--calls", str(k)]
            if mode == "drain": c += ["--max-seconds", str(MAXS[test])]
            if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs)]
        return c
    if kind == "laravel":  # database queue driver, `queue:work` x16; run laravel/setup.sh once first
        c = ["python3", f"{HERE}/laravel/bench.py", "--db", variant, "--mode", mode, "--n", str(n), "--workers", str(w), "--out", outf]
        if mode == "enqueue": c += ["--calls", str(k)]
        if mode == "drain": c += ["--max-seconds", str(MAXS[test])]
        if mode == "steady": c += ["--rate", str(rate), "--seconds", str(secs)]
        return c
    raise SystemExit("unknown config " + cfg)

def snap():
    top = subprocess.run("ps -eo pcpu=,comm= --sort=-pcpu | head -6", shell=True, capture_output=True, text=True).stdout.strip().splitlines()
    return {"cores": os.cpu_count(), "load": open("/proc/loadavg").read().split()[:3], "top": [t.strip() for t in top]}

def run_one(cfg, test, rnd):
    name = f"r{rnd}-{test}-{cfg}"; outf = f"{a.out}/{name}.json"
    c = cmd(cfg, test, outf)
    if c is None: return
    waited = 0
    while float(open("/proc/loadavg").read().split()[0]) >= 8 and waited < a.quiet_wait: time.sleep(10); waited += 10
    before = snap(); t0 = time.time(); print(time.strftime("%H:%M:%S"), name, "load", before["load"][0], flush=True)
    env = dict(os.environ, PATH=f"{os.path.expanduser('~/.dotnet')}:{os.path.expanduser('~/sdk/go/bin')}:" + os.environ["PATH"], DOTNET_ROOT=os.path.expanduser("~/.dotnet"), DOTNET_NOLOGO="1")
    p = subprocess.run(c, capture_output=True, text=True, env=env, timeout=1500)
    if p.returncode != 0:  # one retry, e.g. Runwick-Redis answers store_recovering for the first calls after a fresh start
        print('  retry', name, p.stderr[-200:].replace(chr(10), ' '), flush=True); p = subprocess.run(c, capture_output=True, text=True, env=env, timeout=1500)
    open(f"{a.out}/{name}.log", "w").write(p.stdout[-4000:] + "\n--- stderr ---\n" + p.stderr[-4000:])
    if p.returncode != 0: print("  FAILED", name, p.stderr[-300:], flush=True)
    json.dump({"before": before, "after": snap(), "heavyAtStart": float(before["load"][0]) > 8, "waitedQuietSeconds": waited, "seconds": round(time.time() - t0, 1), "cmd": c}, open(f"{a.out}/{name}.load.json", "w"))

tests = a.tests.split(","); configs = a.configs.split(",")
rounds = [a.round] if a.one else range(1, a.rounds + 1)
for rnd in rounds:
    order = [a.one] if a.one else configs[(rnd - 1) % len(configs):] + configs[:(rnd - 1) % len(configs)]
    for cfg in order:
        for t in tests: run_one(cfg, t, rnd)
