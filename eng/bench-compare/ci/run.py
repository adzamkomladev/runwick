#!/usr/bin/env python3
"""One library, one workload, every test, several rounds, each run wrapped in a resource sampler (ci/README.md).
  run.py --system runwick-pg --workload normal|cpu --out DIR [--rounds 3] [--scale 1]
Writes DIR/<system>--<workload>--<test>--r<round>.json = {"result": <the harness's own JSON>, "resources": {...}, "derived": {...}}
plus DIR/host.json. Expects PostgreSQL on 127.0.0.1:55499 (user postgres, password pg) in container bench-pg and
Redis on 127.0.0.1:56392 (appendfsync always) in container bench-redis."""
import argparse, glob, json, os, platform, resource, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__)); BC = os.path.dirname(HERE); ROOT = os.path.abspath(os.path.join(BC, "..", ".."))
ap = argparse.ArgumentParser()
ap.add_argument("--system", required=True); ap.add_argument("--workload", choices=["normal", "cpu"], required=True)
ap.add_argument("--out", required=True); ap.add_argument("--rounds", type=int, default=3); ap.add_argument("--scale", type=int, default=1, help="divide job counts and seconds (smoke tests)")
ap.add_argument("--tests", default="", help="comma list; default all for the workload")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)

CPU_ITERS = 50000  # ~0.1-0.5 s per job depending on the language (README)
REDIS = "127.0.0.1:56392"
# test -> (mode, jobs, workers, rate, seconds). Normal = 1 KiB no-op job; cpu = the same job doing CPU_ITERS SHA-256 rounds.
TESTS = {
    "normal": {"enqueue-c1": ("enqueue", 2000, 16, 0, 0), "enqueue-c16": ("enqueue", 10000, 16, 0, 0), "bulk": ("bulk", 20000, 16, 0, 0),
               "drain-w16": ("drain", 20000, 16, 0, 0), "drain-w64": ("drain", 20000, 64, 0, 0),
               "steady-100": ("steady", 0, 16, 100, 30), "steady-500": ("steady", 0, 16, 500, 30)},
    "cpu": {"drain-w16": ("drain", 400, 16, 0, 0), "steady-4": ("steady", 0, 16, 4, 60)},
}
STORE = {"runwick-pg": "bench-pg", "river": "bench-pg", "pgboss": "bench-pg", "bullmq-dotnet-pg": "bench-pg", "wolverine-pg": "bench-pg",
         "runwick-redis": "bench-redis", "bullmq": "bench-redis", "horizon": "bench-redis", "celery": "bench-redis",
         "bullmq-dotnet-redis": "bench-redis", "wolverine-redis": "bench-redis",
         "runwick-sqlite": None, "solidqueue": None, "wolverine-sqlite": None}
MAX_SECONDS = 600


def cmd(system, test):
    mode, n, w, rate, secs = TESTS[a.workload][test]
    n //= a.scale; secs = max(5, secs // a.scale)
    k = int(test.split("-c")[1]) if mode == "enqueue" else 1
    if system.startswith("runwick"):
        store = {"runwick-pg": "postgres", "runwick-sqlite": "sqlite", "runwick-redis": "redis"}[system]
        c = ["dotnet", f"{ROOT}/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll", "bench", "--store", store, "--mode", mode, "--n", str(n), "--workers", str(w)]
        if store == "redis": c += ["--redis", REDIS]
        c += {"enqueue": ["--calls", str(k)], "bulk": ["--calls", "1"], "drain": ["--calls", "1" if store == "redis" else "8", "--max-seconds", str(MAX_SECONDS)],
              "steady": ["--rate", str(rate), "--seconds", str(secs), "--calls", "16"]}[mode]
        return c, None
    if system == "river":
        c = [f"{BC}/river/bench-river", "-mode", mode, "-n", str(n), "-workers", str(w)]
        c += {"enqueue": ["-calls", str(k)], "bulk": [], "drain": ["-cooldown-ms", "1", "-max-seconds", str(MAX_SECONDS)],
              "steady": ["-rate", str(rate), "-seconds", str(secs), "-calls", "16", "-cooldown-ms", "1"]}[mode]
        return c, None
    if system == "pgboss":  # polls every 0.5 s at best, so a drain/steady fetch batch must cover the worker count / one second of arrivals
        c = ["node", f"{BC}/pgboss/bench.mjs", "--mode", mode, "--n", str(n)]
        c += {"enqueue": ["--calls", str(k), "--concurrency", str(k)], "bulk": ["--concurrency", "1"],
              "drain": ["--concurrency", "1", "--batch", str(w), "--burst", "--max-seconds", str(MAX_SECONDS)],
              "steady": ["--rate", str(rate), "--seconds", str(secs), "--concurrency", "1", "--batch", str(max(int(rate), 1)), "--burst", "--calls", "16"]}[mode]
        return c, None
    common = {"enqueue": ["--calls", str(k)], "bulk": [], "drain": ["--max-seconds", str(MAX_SECONDS)], "steady": ["--rate", str(rate), "--seconds", str(secs)]}[mode]
    if system == "bullmq":
        return ["node", f"{BC}/bullmq/bench.mjs", "--redis", REDIS, "--mode", mode, "--n", str(n), "--concurrency", str(w)] + common, None
    if system == "horizon":
        return ["python3", f"{BC}/horizon/bench.py", "--redis", REDIS, "--mode", mode, "--n", str(n), "--workers", str(w)] + common, None
    if system == "celery":
        return [sys.executable, f"{BC}/celery/bench.py", "--redis", REDIS, "--mode", mode, "--n", str(n), "--workers", str(w)] + common, None
    if system.startswith("bullmq-dotnet"):
        backend = {"bullmq-dotnet-redis": "redis", "bullmq-dotnet-pg": "postgres"}[system]
        return ["dotnet", f"{BC}/bullmq-dotnet/bin/Release/net10.0/bench-bullmq-dotnet.dll", "--backend", backend, "--redis", REDIS,
                "--mode", mode, "--n", str(n), "--workers", str(w)] + common, None
    if system.startswith("wolverine"):
        transport = {"wolverine-pg": "postgres", "wolverine-redis": "redis", "wolverine-sqlite": "sqlite"}[system]
        return ["dotnet", f"{BC}/wolverine/bin/Release/net10.0/bench-wolverine.dll", "--transport", transport, "--redis", REDIS,
                "--mode", mode, "--n", str(n), "--workers", str(w)] + common, None
    if system == "solidqueue":
        return ["bin/rails", "runner", "../bench.rb", "--mode", mode, "--n", str(n), "--workers", str(w)] + common, f"{BC}/solidqueue/app"
    raise SystemExit("unknown system " + system)


# ---- resource sampling -------------------------------------------------------------------------------------------
TICK = os.sysconf("SC_CLK_TCK")


def read(p):
    try:
        with open(p) as f: return f.read()
    except OSError: return ""


def tree(root):
    kids = {}
    for d in glob.glob("/proc/[0-9]*/stat"):
        s = read(d)
        if s: kids.setdefault(int(s.rsplit(")", 1)[1].split()[1]), []).append(int(d.split("/")[2]))
    out, todo = [], [root]
    while todo:
        p = todo.pop(); out.append(p); todo += kids.get(p, [])
    return out


def proc_sample(pids):
    cpu = mem = rb = wb = 0
    for p in pids:
        s = read(f"/proc/{p}/stat")
        if not s: continue
        f = s.rsplit(")", 1)[1].split()
        cpu += sum(int(x) for x in f[11:15]) / TICK  # utime stime cutime cstime: reaped children count once, in their parent
        sm = read(f"/proc/{p}/smaps_rollup")  # PSS: shared pages split between processes, so prefork pools are not overcounted
        mem += next((int(l.split()[1]) * 1024 for l in sm.splitlines() if l.startswith("Pss:")), 0) or int((read(f"/proc/{p}/statm").split() or [0, 0])[1]) * 4096
        for l in read(f"/proc/{p}/io").splitlines():
            if l.startswith("read_bytes:"): rb += int(l.split()[1])
            elif l.startswith("write_bytes:"): wb += int(l.split()[1])
    return cpu, mem, rb, wb


class Container:
    def __init__(s, name):
        s.ok = False
        if not name: return
        try: pid = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", name], capture_output=True, text=True).stdout.strip()
        except OSError: pid = ""  # no docker CLI: store figures are left out
        cg = next((l.split("::", 1)[1] for l in read(f"/proc/{pid}/cgroup").splitlines() if l.startswith("0::")), None) if pid else None
        s.pid, s.cg = pid, f"/sys/fs/cgroup{cg}" if cg else None
        s.ok = bool(s.cg and os.path.exists(s.cg))

    def sample(s):
        cpu = next((int(l.split()[1]) / 1e6 for l in read(f"{s.cg}/cpu.stat").splitlines() if l.startswith("usage_usec")), 0)
        mem = int(read(f"{s.cg}/memory.current") or 0)
        rd = wr = 0
        for l in read(f"{s.cg}/io.stat").splitlines():
            kv = dict(x.split("=") for x in l.split()[1:] if "=" in x); rd += int(kv.get("rbytes", 0)); wr += int(kv.get("wbytes", 0))
        rx = tx = 0
        for l in read(f"/proc/{s.pid}/net/dev").splitlines()[2:]:
            name, v = l.split(":", 1); v = v.split()
            if name.strip() != "lo": rx += int(v[0]); tx += int(v[8])
        return cpu, mem, rd, wr, rx, tx


def run_sampled(c, cwd, env, store):
    p = subprocess.Popen(c, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    stop = threading.Event(); app_s, st_s = [], []
    def loop():
        while not stop.is_set():
            t = time.time(); app_s.append((t, *proc_sample(tree(p.pid))))
            if store.ok: st_s.append((t, *store.sample()))
            stop.wait(0.5)
    st0 = store.sample() if store.ok else None
    th = threading.Thread(target=loop, daemon=True); th.start()
    killer = threading.Timer(MAX_SECONDS + 600, lambda: os.killpg(p.pid, signal.SIGKILL)); killer.start()
    out, err = p.communicate(); t1 = time.time(); killer.cancel(); stop.set(); th.join()
    if store.ok: st_s.append((t1, *store.sample()))  # closing reading: a run shorter than one interval would otherwise have none after its work
    ru = resource.getrusage(resource.RUSAGE_CHILDREN)  # communicate() reaped it: its CPU, and its reaped children's, is now in ours
    return p.returncode, out, err, app_s, st_s, st0, t1, ru


def summarize(app_s, st_s, st0, t1, ru_before, ru_after):
    secs = max(1e-9, t1 - app_s[0][0]) if app_s else 1e-9
    cpu_total = (ru_after.ru_utime + ru_after.ru_stime) - (ru_before.ru_utime + ru_before.ru_stime)  # exact: everything the run started, reaped
    pct = [(b[1] - a_[1]) / (b[0] - a_[0]) * 100 for a_, b in zip(app_s, app_s[1:]) if b[0] > a_[0] and b[1] >= a_[1]]
    app = {"seconds": round(secs, 2), "cpuSeconds": round(cpu_total, 2), "cpuAvgPct": round(cpu_total / secs * 100, 1),
           "cpuPeakPct": round(max(pct, default=0), 1), "memPeakMB": round(max((x[2] for x in app_s), default=0) / 2**20, 1),
           "memAvgMB": round(sum(x[2] for x in app_s) / max(1, len(app_s)) / 2**20, 1),
           "diskReadBytes": max((x[3] for x in app_s), default=0), "diskWriteBytes": max((x[4] for x in app_s), default=0)}
    store = None
    if st_s and st0:
        last = st_s[-1]; d = [last[i + 1] - st0[i] for i in range(6)]
        spct = [(b[1] - a_[1]) / (b[0] - a_[0]) * 100 for a_, b in zip(st_s, st_s[1:]) if b[0] > a_[0]]
        store = {"cpuSeconds": round(d[0], 2), "cpuAvgPct": round(d[0] / secs * 100, 1), "cpuPeakPct": round(max(spct, default=0), 1),
                 "memPeakMB": round(max(x[2] for x in st_s) / 2**20, 1), "diskReadBytes": d[2], "diskWriteBytes": d[3], "netRxBytes": d[4], "netTxBytes": d[5]}
    return {"app": app, "store": store}


def host():
    cpu = next((l.split(":", 1)[1].strip() for l in read("/proc/cpuinfo").splitlines() if l.startswith("model name")), platform.processor())
    mem = next((int(l.split()[1]) // 1024 for l in read("/proc/meminfo").splitlines() if l.startswith("MemTotal")), 0)
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    return {"cpu": cpu, "cores": os.cpu_count(), "memMB": mem, "kernel": platform.release(), "commit": commit, "cpuIters": CPU_ITERS,
            "runner": os.environ.get("RUNNER_NAME"), "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def lib_version(system, result):
    """The library and version this run measured: the harness's own report, or for Runwick the commit under test."""
    if system.startswith("runwick"):
        d = subprocess.run(["git", "describe", "--tags", "--always"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
        return f"runwick {d}"
    return (result or {}).get("system")


def main():
    tag = f"{a.system}--{a.workload}"
    json.dump(host(), open(f"{a.out}/{tag}--host.json", "w"), indent=2)
    # .NET contenders run with server GC (throughput GC, one heap per core), the setting a production worker service would use.
    env = dict(os.environ, BENCH_CPU_ITERS=str(CPU_ITERS if a.workload == "cpu" else 0), DOTNET_NOLOGO="1", DOTNET_gcServer="1")
    store = Container(STORE[a.system])
    tests = a.tests.split(",") if a.tests else list(TESTS[a.workload])
    failures = 0
    for rnd in range(1, a.rounds + 1):
        for test in tests:
            c, cwd = cmd(a.system, test)
            name = f"{tag}--{test}--r{rnd}"; outf = os.path.abspath(f"{a.out}/{name}.raw.json")
            if os.path.exists(outf): os.remove(outf)
            print(time.strftime("%H:%M:%S"), name, flush=True)
            ru0 = resource.getrusage(resource.RUSAGE_CHILDREN)
            rc, out, err, app_s, st_s, st0, t1, ru1 = run_sampled(c + ["-out" if a.system == "river" else "--out", outf], cwd, env, store)
            open(f"{a.out}/{name}.log", "w").write(" ".join(c) + "\n" + out[-6000:] + "\n--- stderr ---\n" + err[-6000:])
            try: result = json.load(open(outf))
            except (OSError, ValueError): result = None
            if rc != 0 or result is None:
                failures += 1; print(f"  FAILED rc={rc}: {err[-400:]}", flush=True)
            res = summarize(app_s, st_s, st0, t1, ru0, ru1)
            jobs = (result or {}).get("finished") or (result or {}).get("completed") or (result or {}).get("jobs") or 0
            cpu = res["app"]["cpuSeconds"] + ((res["store"] or {}).get("cpuSeconds") or 0)
            wire = ((res["store"] or {}).get("netRxBytes") or 0) + ((res["store"] or {}).get("netTxBytes") or 0)
            derived = {"jobs": jobs, "cpuMsPerJob": round(cpu * 1000 / jobs, 3) if jobs else None, "wireBytesPerJob": round(wire / jobs) if jobs and wire else None,
                       "storeWriteBytesPerJob": round(((res["store"] or {}).get("diskWriteBytes") or res["app"]["diskWriteBytes"]) / jobs) if jobs else None}
            json.dump({"system": a.system, "workload": a.workload, "test": test, "round": rnd, "ok": rc == 0 and result is not None,
                       "version": lib_version(a.system, result), "result": result, "resources": res, "derived": derived}, open(f"{a.out}/{name}.json", "w"), indent=2)
    print(f"{failures} failed run(s)")


main()
