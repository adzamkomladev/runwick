#!/usr/bin/env python3
"""Claim-ahead qualification on Redis (experiment branch exp/finish-batch-64, claim-ahead limit raised to 64).
Every arm: 16 slots, buffered finish, 2 claims in flight, finish cap 32; only --claim-ahead changes (0, 16, 64).
  steady   one worker, 500 jobs/s offered for 30 s by 4 callers, no-op jobs: add-to-finish latency from the harness.
  pair     two workers on one queue, 300 jobs/s offered for 30 s by worker A (4 callers); jobs take 20 ms except every
           20th, which takes 1 s. Pickup (accepted -> handler start) and each worker's share, from the ledgers.
  crash    3,000 jobs of 100 ms seeded; worker A runs 4 s and is SIGKILLed; worker B starts at once.
           Jobs whose first start in B came 20 s or more after the kill waited for A's leases to expire (60 s default).
Usage: qual.py <outdir> <test> <reps>"""
import json, os, signal, subprocess, sys, time, statistics as st

OUT, TEST, REPS = sys.argv[1], sys.argv[2], int(sys.argv[3])
WT = "/root/work/Spindle-exp-finish64"
DLL = f"{WT}/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll"
REDIS = "127.0.0.1:56393"
ARMS = [int(x) for x in os.environ.get("ARMS", "0,16,64").split(",")]
os.makedirs(OUT, exist_ok=True)


def env(extra=None):
    e = dict(os.environ, DOTNET_gcServer="1", DOTNET_PROCESSOR_COUNT="4")
    e.update(extra or {})
    return e


def bench(ca, *args):
    return ["dotnet", DLL, "bench", "--store", "redis", "--redis", REDIS, "--workers", "16", "--buffered-finish",
            "--claim-ahead", str(ca), "--claims-in-flight", "2", *args]


def start(cmd, log, extra=None):
    return subprocess.Popen(cmd, stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env(extra), cwd=WT)


def ledger(path):
    rows = []
    if os.path.exists(path):
        for line in open(path):
            p = line.split()
            if len(p) == 3:
                rows.append((p[0], p[1], int(p[2])))
    return rows


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, max(0, int(p * len(xs)) - 1))], 1) if xs else None


def steady(ca, tag):
    out = f"{OUT}/{tag}.json"
    subprocess.run(bench(ca, "--mode", "steady", "--rate", "500", "--calls", "4", "--seconds", "30", "--out", out),
                   stdout=open(f"{OUT}/{tag}.log", "w"), stderr=subprocess.STDOUT, env=env(), cwd=WT, timeout=300)
    d = json.load(open(out))
    return {"done/s": d.get("completionsPerSec"), "p50": d["latencyMs"]["p50"], "p99": d["latencyMs"]["p99"],
            "max": d["latencyMs"]["max"], "refused": d.get("refused") or d.get("refusals")}


def pair(ca, tag):
    la, lb = f"{OUT}/{tag}-a.ledger", f"{OUT}/{tag}-b.ledger"
    for f in (la, lb):
        if os.path.exists(f): os.remove(f)
    slow = {"BENCH_SLOW_EVERY": "20", "BENCH_SLOW_MS": "1000"}
    a = start(bench(ca, "--mode", "steady", "--rate", "300", "--calls", "4", "--seconds", "30", "--job-ms", "20",
                    "--ledger", la, "--out", f"{OUT}/{tag}-a.json"), f"{OUT}/{tag}-a.log", slow)
    while not any(k == "a" for k, _, _ in ledger(la)):  # A has flushed and migrated the store
        time.sleep(0.05)
    b = start(bench(ca, "--mode", "work", "--redis-reuse", "--n", "100000000", "--seconds", "60", "--job-ms", "20",
                    "--ledger", lb), f"{OUT}/{tag}-b.log", slow)
    a.wait(timeout=200)
    b.kill(); b.wait()
    A, B = ledger(la), ledger(lb)
    acc = {i: ms for k, i, ms in A if k == "a"}
    starts = {}
    for who, rows in (("a", A), ("b", B)):
        for k, i, ms in rows:
            if k == "i" and (i not in starts or ms < starts[i][1]):
                starts[i] = (who, ms)
    b_first = min((ms for k, _, ms in B if k == "i"), default=None)
    # Count only jobs accepted once both workers were running.
    ids = [i for i, ms in acc.items() if b_first and ms >= b_first + 1000]
    pick = [starts[i][1] - acc[i] for i in ids if i in starts]
    share_b = sum(1 for i in ids if i in starts and starts[i][0] == "b") / max(1, len(ids))
    return {"jobs": len(ids), "started": len(pick), "pickup p50": pct(pick, .5), "p95": pct(pick, .95), "p99": pct(pick, .99),
            "max": pct(pick, 1), "share A": round(1 - share_b, 3), "share B": round(share_b, 3)}


def crash(ca, tag):
    la, lb = f"{OUT}/{tag}-a.ledger", f"{OUT}/{tag}-b.ledger"
    for f in (la, lb):
        if os.path.exists(f): os.remove(f)
    subprocess.run(bench(ca, "--mode", "bulk", "--n", "3000", "--out", f"{OUT}/{tag}-seed.json"),
                   stdout=open(f"{OUT}/{tag}-seed.log", "w"), stderr=subprocess.STDOUT, env=env(), cwd=WT, timeout=300)
    a = start(bench(ca, "--mode", "work", "--redis-reuse", "--n", "100000000", "--seconds", "600", "--job-ms", "100",
                    "--ledger", la), f"{OUT}/{tag}-a.log")
    while not any(k == "i" for k, _, _ in ledger(la)):
        time.sleep(0.05)
    time.sleep(4)
    a.send_signal(signal.SIGKILL); a.wait()
    kill = int(time.time() * 1000)
    b = start(bench(ca, "--mode", "work", "--redis-reuse", "--n", "100000000", "--seconds", "600", "--job-ms", "100",
                    "--ledger", lb), f"{OUT}/{tag}-b.log")
    deadline = time.time() + 200
    while time.time() < deadline:
        done = {i for k, i, _ in ledger(la) + ledger(lb) if k == "f"}
        if len(done) >= 3000: break
        time.sleep(1)
    b.kill(); b.wait()
    states = store_states()
    A, B = ledger(la), ledger(lb)
    a_started = {i for k, i, _ in A if k == "i"}
    a_done = {i for k, i, _ in A if k == "f"}
    b_start = {}
    for k, i, ms in B:
        if k == "i" and i not in b_start: b_start[i] = ms
    done = a_done | {i for k, i, _ in B if k == "f"}
    late = [i for i, ms in b_start.items() if ms - kill >= 20000]
    return {"finished": len(done), "A finished": len(a_done), "A running at kill": len(a_started - a_done),
            "waited for lease": len(late), "of which never started by A": len([i for i in late if i not in a_started]),
            "lease wait s": round(min((b_start[i] - kill for i in late), default=0) / 1000, 1),
            "kill to all done s": round((max(ms for k, _, ms in A + B if k == "f") - kill) / 1000, 1), "store states": states}


def store_states():
    """Job count by state, read from Redis at the end (catches jobs a killed worker finished but never logged)."""
    lua = ("local c={} local cur='0' repeat local r=redis.call('SCAN',cur,'MATCH','*job:*','COUNT',1000) cur=r[1] "
           "for _,k in ipairs(r[2]) do if redis.call('TYPE',k).ok=='hash' then local s=redis.call('HGET',k,'state') or 'nil' "
           "c[s]=(c[s] or 0)+1 end end until cur=='0' return cjson.encode(c)")
    r = subprocess.run(["docker", "exec", "exp-redis", "redis-cli", "EVAL", lua, "0"], capture_output=True, text=True)
    return r.stdout.strip()


fn = {"steady": steady, "pair": pair, "crash": crash}[TEST]
res = {}
for r in range(REPS):
    order = ARMS if r % 2 == 0 else ARMS[::-1]
    for ca in order:
        tag = f"{TEST}-ca{ca}-r{r + 1}"
        subprocess.run(["docker", "exec", "exp-redis", "redis-cli", "FLUSHALL"], capture_output=True)
        res[tag] = fn(ca, tag)
        print(tag, json.dumps(res[tag]), flush=True)
json.dump(res, open(f"{OUT}/{TEST}-summary.json", "w"), indent=1)
