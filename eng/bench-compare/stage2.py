#!/usr/bin/env python3
"""Stage 2 driver (docs/plans/queue-throughput-engineering-plan.md): runs the `stage2` modes of Runwick.Workloads on this machine, one run at a time,
logging CPU idle before each run, arms in alternating order, one fresh store per run.

  stage2.py dispatch --store redis|postgres|sqlite --out DIR [--rounds 5]
  stage2.py concurrent --store ... --out DIR [--rounds 5]
  stage2.py worker   --store ... --slots 16 --out DIR [--rounds 5] [--n 20000]

Stores are the throwaway containers bench-pg (55499) and bench-redis (56392), started like eng/bench-compare/ci/run.py reset_store; --tmpfs puts their data
and the SQLite file in memory. Nothing here talks to GitHub.
"""
import argparse, json, os, subprocess, sys, time

p = argparse.ArgumentParser()
p.add_argument("part"); p.add_argument("--store", required=True); p.add_argument("--out", required=True)
p.add_argument("--rounds", type=int, default=5); p.add_argument("--slots", type=int, default=16); p.add_argument("--n", type=int, default=20000)
p.add_argument("--tmpfs", action="store_true"); p.add_argument("--variants", default="claim-ahead,claims-in-flight,flush")
a = p.parse_args()
DLL = os.path.join(os.path.dirname(__file__), "../../tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll")
ENV = dict(os.environ, DOTNET_gcServer="1", DOTNET_NOLOGO="1", PATH=os.path.expanduser("~/.dotnet") + ":" + os.environ["PATH"], DOTNET_ROOT=os.path.expanduser("~/.dotnet"))
if a.tmpfs: os.makedirs("/dev/shm/s2", exist_ok=True); ENV["TMPDIR"] = "/dev/shm/s2"
os.makedirs(a.out, exist_ok=True)
CONTAINER = {"redis": "bench-redis", "postgres": "bench-pg"}.get(a.store)

def idle():
    out = subprocess.run(["vmstat", "3", "2"], capture_output=True, text=True).stdout.strip().splitlines()[-1].split()
    return int(out[14])

def wait_idle(limit=1500):
    """The machine is shared (other sessions compile and test here). Wait for CPU idle >= 80% before each run; give up after `limit` seconds and flag it."""
    start = time.time()
    while True:
        i = idle()
        if i >= 80: return i, False
        if time.time() - start > limit: return i, True
        time.sleep(15)

def run(name, args):
    path = f"{a.out}/{name}.json"
    if os.path.exists(path):  # a finished run is kept, so re-running the script only fills the gaps
        j = json.load(open(path))
        if j.get("returncode") == 0 and j.get("confirmed", j.get("jobs")) == j.get("jobs"): return j
    i, gated_out = wait_idle()
    t = time.time()
    cmd = ["dotnet", DLL, "stage2", *args, "--store", a.store, "--out", path] + (["--container", CONTAINER] if CONTAINER else [])
    r = subprocess.run(cmd, env=ENV, capture_output=True, text=True)
    open(f"{a.out}/{name}.log", "w").write(r.stdout + r.stderr)
    ok = os.path.exists(path) and r.returncode == 0
    print(f"{time.strftime('%H:%M:%S')} {name} idle={i}%{' (GATE TIMED OUT)' if gated_out else ''} rc={r.returncode} {time.time()-t:.0f}s", flush=True)
    if os.path.exists(path):
        j = json.load(open(path)); j["cpuIdlePctBefore"] = i; j["idleGateTimedOut"] = gated_out; j["returncode"] = r.returncode
        json.dump(j, open(path, "w"), indent=1)
    return json.load(open(path)) if os.path.exists(path) else None

if a.part == "concurrent":
    run(f"concurrent-{a.store}", ["--part", "concurrent", "--rounds", str(a.rounds)])
    sys.exit()
if a.part == "dispatch":
    run(f"dispatch-{a.store}", ["--part", "dispatch", "--rounds", str(a.rounds)] + (["--trace"] if "trace" in a.out else []))
    sys.exit()

# worker: calibration run of the public worker, then matched pairs, then one-knob variants
S, N = str(a.slots), str(a.n)
base = ["--part", "worker", "--slots", S, "--n", N, "--producer-calls", "8"]  # seeding uses 4 callers; two producer slots made it fail now and then
cal = run(f"w{S}-calibration-public", base + ["--arm", "public"])
if not cal: sys.exit("calibration failed")
lat = cal["storeCallLatencies"]
claim = round(lat.get("reserveAndBeginSize", {}).get("meanMs", 1)) or 1      # sizes are recorded through the latency reservoir (count, not ms)
fin = round(lat.get("finishManySize", {}).get("meanMs", 1)) or 1
buf = cal["buffered"]
print(f"observed mean claim size {claim}, mean finish size {fin}, buffered={buf}", flush=True)
matched = ["--claim-size", str(max(1, claim)), "--finish-size", str(max(1, fin)), "--buffered", str(buf).lower(), "--claims-in-flight", str(cal["claimsInFlightSetting"]), "--flush-ms", "5"]
for r in range(1, a.rounds + 1):
    for arm in (["public", "direct"] if r % 2 else ["direct", "public"]):
        run(f"w{S}-pair1-{arm}-r{r}", base + ["--arm", arm] + (matched if arm == "direct" else []))
# pair 2: one knob at a time, same slot budget and durability, 3 rounds, control = the matched arm of the same kind
rr = min(3, a.rounds)
v = a.variants.split(",")
for r in range(1, rr + 1):
    if "claim-ahead" in v: run(f"w{S}-pair2-public-claimahead{min(32, a.slots)}-r{r}", base + ["--arm", "public", "--claim-ahead", str(min(32, a.slots))])  # the option's maximum is 32
    if "claims-in-flight" in v: run(f"w{S}-pair2-public-inflight2-r{r}", base + ["--arm", "public", "--claims-in-flight", "2"])
    if "flush" in v:
        for ms in ("0", "20"):
            run(f"w{S}-pair2-direct-flush{ms}-r{r}", base + ["--arm", "direct"] + [x if x != "5" else ms for x in matched])
    if "claim-size" in v:
        for c in (max(1, claim // 2), min(32, claim * 2)):
            run(f"w{S}-pair2-direct-claim{c}-r{r}", base + ["--arm", "direct"] + [str(c) if x == str(max(1, claim)) else x for x in matched])
