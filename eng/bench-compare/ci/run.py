#!/usr/bin/env python3
"""One or more libraries, one workload, every test, several rounds, each run wrapped in a resource sampler (ci/README.md).
  run.py --system runwick-pg --workload normal|cpu --out DIR [--rounds 3] [--scale 1]
  run.py --systems runwick-pg,river,pgboss --fresh-stores --out DIR      same host, one library after another, order reversed on even rounds
Writes DIR/<system>--<workload>--<test>--r<round>.json = {"result": <the harness's own JSON>, "resources": {...}, "derived": {...},
"phases": {...}, "outcome": {...}, "complete": bool}, DIR/<system>--<workload>--host.json and DIR/manifest.json.
Harnesses print "@@PHASE <name> <unix ms>" lines on stderr; at each one this driver reads the process tree, the store container and the store's own
counters, so CPU, wire and disk figures per job cover the timed phase only (seeding, startup and shutdown are left out).
Expects PostgreSQL on 127.0.0.1:55499 (user postgres, password pg) in container bench-pg and
Redis on 127.0.0.1:56392 (appendfsync always) in container bench-redis; --fresh-stores (re)creates both, pinned, before every library."""
import argparse, atexit, glob, json, os, platform, re, resource, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__)); BC = os.path.dirname(HERE); ROOT = os.path.abspath(os.path.join(BC, "..", ".."))
ap = argparse.ArgumentParser()
ap.add_argument("--system", default=""); ap.add_argument("--systems", default="", help="comma list, run one after another on this host"); ap.add_argument("--fresh-stores", action="store_true", help="recreate bench-pg / bench-redis before each library (pinned images and settings)"); ap.add_argument("--store-cpus", default="", help="docker --cpus for the store containers"); ap.add_argument("--store-tmpfs", action="store_true", help="keep the store container's data on tmpfs (a second, fast storage class); SQLite libraries use TMPDIR, set it to a tmpfs path"); ap.add_argument("--trace-round", action="store_true", help="one extra --trace run of each Runwick test, kept out of the timing rounds"); ap.add_argument("--workload", choices=["normal", "cpu"], required=True)
ap.add_argument("--out", required=True); ap.add_argument("--rounds", type=int, default=3); ap.add_argument("--scale", type=int, default=1, help="divide job counts and seconds (smoke tests)")
ap.add_argument("--first-round", type=int, default=1, help="number the rounds from here (re-run one round of some cells without touching the others)"); ap.add_argument("--tests", default="", help="comma list; default all for the workload")
a = ap.parse_args()
SYSTEMS = [x for x in (a.systems or a.system).split(",") if x]
if not SYSTEMS: ap.error("--system or --systems is required")
os.makedirs(a.out, exist_ok=True)

CPU_ITERS = 50000  # ~0.1-0.5 s per job depending on the language (README)
REDIS = "127.0.0.1:56392"
# test -> (mode, jobs, workers, rate, seconds). Normal = 1 KiB no-op job; cpu = the same job doing CPU_ITERS SHA-256 rounds.
TESTS = {
    "normal": {"enqueue-c1": ("enqueue", 2000, 16, 0, 0), "enqueue-c16": ("enqueue", 10000, 16, 0, 0), "enqueue-c64": ("enqueue", 10000, 16, 0, 0),
               "bulk": ("bulk", 20000, 16, 0, 0), "bulk-100": ("bulk", 20000, 16, 0, 0),  # bulk = each library's native chunk; bulk-100 = 100 items per call for every library
               "floor-c1": ("writefloor", 2000, 1, 0, 0), "floor-c16": ("writefloor", 4000, 16, 0, 0),  # writefloor-* systems only: the cheapest durable write, no queue code
               "drain-w16": ("drain", 20000, 16, 0, 0), "drain-w64": ("drain", 20000, 64, 0, 0),
               "steady-100": ("steady", 0, 16, 100, 30), "steady-500": ("steady", 0, 16, 500, 30)},
    "cpu": {"drain-w16": ("drain", 400, 16, 0, 0), "steady-4": ("steady", 0, 16, 4, 60)},
}
STORE = {"writefloor-pg": "bench-pg", "writefloor-redis": "bench-redis", "writefloor-sqlite": None, "runwick-redis-tuned": "bench-redis", "runwick-pg": "bench-pg", "river": "bench-pg", "pgboss": "bench-pg", "bullmq-dotnet-pg": "bench-pg", "wolverine-pg": "bench-pg",
         "runwick-redis": "bench-redis", "bullmq": "bench-redis", "horizon": "bench-redis", "celery": "bench-redis",
         "bullmq-dotnet-redis": "bench-redis", "wolverine-redis": "bench-redis",
         "runwick-sqlite": None, "solidqueue": None, "wolverine-sqlite": None}
MAX_SECONDS = 600
MANIFEST = f"{a.out}/manifest-{SYSTEMS[0]}-{a.workload}.json"  # one per job: merged artifact folders must not collide (and no "--r", which report.py reads as a run)


TUNED = ["--buffered-finish", "--claim-ahead", "16", "--claims-in-flight", "2"]  # the qualified short-job Redis profile (plan, Stage 4)


def cmd(system, test, trace=False):
    mode, n, w, rate, secs = TESTS[a.workload][test]
    n //= a.scale; secs = max(5, secs // a.scale)
    k = int(test.split("-c")[1]) if mode in ("enqueue", "writefloor") else 1
    chunk = test == "bulk-100"
    if system.startswith("writefloor-"):
        store = {"writefloor-pg": "postgres", "writefloor-redis": "redis", "writefloor-sqlite": "sqlite"}[system]
        return ["dotnet", f"{ROOT}/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll", "writefloor", "--store", store, "--callers", str(k), "--n", str(max(100, n)), "--redis", REDIS], None
    if system.startswith("runwick"):
        store = {"runwick-pg": "postgres", "runwick-sqlite": "sqlite", "runwick-redis": "redis", "runwick-redis-tuned": "redis"}[system]
        c = ["dotnet", f"{ROOT}/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll", "bench", "--store", store, "--mode", mode, "--n", str(n), "--workers", str(w)]
        if store == "redis": c += ["--redis", REDIS]
        c += {"enqueue": ["--calls", str(k)], "bulk": ["--calls", "1"], "drain": ["--calls", "1" if store == "redis" else "8", "--max-seconds", str(MAX_SECONDS)],
              "steady": ["--rate", str(rate), "--seconds", str(secs), "--calls", "16"]}[mode]
        if system == "runwick-redis-tuned" and mode in ("drain", "steady"): c += TUNED
        if chunk: c += ["--batch-items", "100"]
        if trace: c += ["--trace"]
        return c, None
    if system == "river":
        c = [f"{BC}/river/bench-river", "-mode", mode, "-n", str(n), "-workers", str(w)]
        c += {"enqueue": ["-calls", str(k)], "bulk": ["-batch", "100"] if chunk else [], "drain": ["-cooldown-ms", "1", "-max-seconds", str(MAX_SECONDS)],
              "steady": ["-rate", str(rate), "-seconds", str(secs), "-calls", "16", "-cooldown-ms", "1"]}[mode]
        return c, None
    if system == "pgboss":  # polls every 0.5 s at best, so a drain/steady fetch batch must cover the worker count / one second of arrivals
        c = ["node", f"{BC}/pgboss/bench.mjs", "--mode", mode, "--n", str(n)]
        c += {"enqueue": ["--calls", str(k), "--concurrency", str(k)], "bulk": ["--concurrency", "1"] + (["--chunk", "100"] if chunk else []),
              "drain": ["--concurrency", "1", "--batch", str(w), "--burst", "--max-seconds", str(MAX_SECONDS)],
              "steady": ["--rate", str(rate), "--seconds", str(secs), "--concurrency", "1", "--batch", str(max(int(rate), 1)), "--burst", "--calls", "16"]}[mode]
        return c, None
    common = {"enqueue": ["--calls", str(k)], "bulk": ["--batch", "100"] if chunk and system in ("bullmq", "bullmq-dotnet-redis", "bullmq-dotnet-pg", "solidqueue") else [], "drain": ["--max-seconds", str(MAX_SECONDS)], "steady": ["--rate", str(rate), "--seconds", str(secs)]}[mode]
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


PHASE_RE = re.compile(r"^@@PHASE (\S+) (\d+)")


def sh(c, timeout=20):
    try: return subprocess.run(c, capture_output=True, text=True, timeout=timeout).stdout.strip()
    except (OSError, subprocess.SubprocessError): return ""


class Pipe:
    """A long-lived `docker exec -i <store> <client>` that answers a query in about a millisecond, so counters are read at the phase marker, not a docker exec later."""
    def __init__(s, kind):
        s.lock = threading.Lock(); s.p = None
        argv = {"bench-pg": ["psql", "-U", "postgres", "-At", "-q"], "bench-redis": ["redis-cli"]}.get(kind)
        if argv:
            s.p = subprocess.Popen(["docker", "exec", "-i", kind, *argv], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
        s.kind = kind

    def ask(s, *queries):
        if not s.p or s.p.poll() is not None: return []
        sentinel = "@@END"
        with s.lock:
            for q in queries: s.p.stdin.write(q + "\n")
            s.p.stdin.write(("SELECT '@@END';" if s.kind == "bench-pg" else "ECHO @@END") + "\n"); s.p.stdin.flush()
            out = []
            for line in s.p.stdout:
                if line.strip() == sentinel: break
                out.append(line.rstrip("\n"))
            return out

    def close(s):
        if s.p and s.p.poll() is None: s.p.terminate()


def store_counters(pipe):
    """The store's own cumulative counters, read at a phase marker. PostgreSQL: WAL bytes (exact, from the insert LSN), plus records, full-page images and transactions from
    the statistics views, which backends flush within about a second. Redis: AOF size, commands and network bytes."""
    if pipe.kind == "bench-pg":
        r = pipe.ask("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0') || '|' || (SELECT wal_records || '|' || wal_fpi FROM pg_stat_wal) || '|' || (SELECT sum(xact_commit + xact_rollback) FROM pg_stat_database);")
        v = r[0].split("|") if r else []
        return dict(zip(["walBytes", "walRecords", "walFpi", "transactions"], (float(x) for x in v))) if len(v) == 4 and all(v) else {}
    if pipe.kind == "bench-redis":
        info = dict(l.rstrip("\r").split(":", 1) for l in pipe.ask("INFO") if ":" in l)
        return {k: float(info[k]) for k in ("total_commands_processed", "aof_current_size", "total_net_input_bytes", "total_net_output_bytes") if k in info}
    return {}


def run_sampled(c, cwd, env, store, pipe):
    p = subprocess.Popen(c, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    stop = threading.Event(); app_s, st_s, phases, out_l, err_l = [], [], {}, [], []
    def loop():
        while not stop.is_set():
            t = time.time(); app_s.append((t, *proc_sample(tree(p.pid))))
            if store.ok: st_s.append((t, *store.sample()))
            stop.wait(0.5)
    def read_err():
        for line in p.stderr:
            err_l.append(line)
            m = PHASE_RE.match(line)
            if m:  # process and container readings first (microseconds), then the store's own counters (a docker exec)
                snap = {"harnessMs": int(m.group(2)), "wall": time.time(), "app": proc_sample(tree(p.pid)), "store": store.sample() if store.ok else None}
                snap["counters"] = store_counters(pipe)
                phases[m.group(1)] = snap
    def read_out():
        for line in p.stdout: out_l.append(line)
    st0 = store.sample() if store.ok else None
    ths = [threading.Thread(target=f, daemon=True) for f in (loop, read_err, read_out)]
    for th in ths: th.start()
    killer = threading.Timer(MAX_SECONDS + 600, lambda: os.killpg(p.pid, signal.SIGKILL)); killer.start()
    p.wait(); t1 = time.time(); killer.cancel(); stop.set()
    for th in ths: th.join()
    if store.ok: st_s.append((t1, *store.sample()))  # closing reading: a run shorter than one interval would otherwise have none after its work
    ru = resource.getrusage(resource.RUSAGE_CHILDREN)  # wait() reaped it: its CPU, and its reaped children's, is now in ours
    return p.returncode, "".join(out_l), "".join(err_l), app_s, st_s, st0, t1, ru, phases


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


def timed(phases, jobs):
    """Per-job cost of the timed phase only: from the timed-start marker to timed-end. None when a harness gave no markers."""
    a_, b_ = phases.get("timed-start"), phases.get("timed-end")
    if not a_ or not b_ or not jobs: return None
    secs = b_["wall"] - a_["wall"]
    app_cpu = b_["app"][0] - a_["app"][0]
    out = {"seconds": round(secs, 3), "appCpuSeconds": round(app_cpu, 3), "jobs": jobs}
    cpu = app_cpu
    if a_["store"] and b_["store"]:
        d = [b_["store"][i] - a_["store"][i] for i in range(6)]
        out |= {"storeCpuSeconds": round(d[0], 3), "storeDiskWriteBytes": d[3], "storeNetRxBytes": d[4], "storeNetTxBytes": d[5]}
        cpu += d[0]
        out["wireBytesPerJob"] = round((d[4] + d[5]) / jobs)
        out["storeWriteBytesPerJob"] = round(d[3] / jobs)
    out["cpuMsPerJob"] = round(cpu * 1000 / jobs, 3)
    ca, cb = a_["counters"], b_["counters"]
    out["counters"] = {k: cb[k] - ca[k] for k in cb if k in ca}
    for k, name in (("walBytes", "walBytesPerJob"), ("walFpi", "walFpiPerJob"), ("transactions", "transactionsPerJob"), ("total_commands_processed", "redisCommandsPerJob"), ("aof_current_size", "aofBytesPerJob")):
        if k in out["counters"]: out[name] = round(out["counters"][k] / jobs, 3)
    return out


def num(d, *keys):
    return sum(v for k, v in (d or {}).items() if k in keys and isinstance(v, (int, float)))


def normalize(system, mode, r):
    """What the store holds, in one shape: accepted (rows stored), succeeded, failed, unfinished. None where the store gives no such figure."""
    o = (r or {}).get("outcome") or {}
    if not o or "error" in o: return None
    if system.startswith("runwick"):
        done = {"succeeded": o.get("succeeded", 0), "failed": o.get("failed", 0) + o.get("cancelled", 0)}
        unf = num(o, "pending", "reserved", "running", "retrying")
    elif system == "river":
        done = {"succeeded": o.get("completed", 0), "failed": o.get("discarded", 0) + o.get("cancelled", 0)}
        unf = num(o, "available", "running", "retryable", "scheduled", "pending")
    elif system == "pgboss":
        done = {"succeeded": o.get("completed", 0), "failed": o.get("failed", 0) + o.get("cancelled", 0)}
        unf = num(o, "created", "retry", "active")
    elif system.startswith("bullmq"):  # completed jobs are trimmed by removeOnComplete: the worker's Completed events stand in
        done = {"succeeded": o.get("completedEvents", 0), "failed": o.get("failed", 0)}
        unf = num(o, "waiting", "active", "delayed")
    elif system.startswith("wolverine"):  # handler returns are Wolverine's own count; the store says what is still queued
        done = {"succeeded": ((r.get("clocks") or {}).get("handlerReturns", r.get("completed", 0))), "failed": o.get("deadLetters", 0)}
        unf = (num(o, "pending") + max(0, o.get("lag", 0))) if "streamEntries" in o else sum(v for k, v in o.items() if k != "deadLetters" and isinstance(v, (int, float)))
    elif system == "solidqueue":
        done = {"succeeded": o.get("finished", 0) - o.get("failed", 0), "failed": o.get("failed", 0)}
        unf = o.get("unfinished", 0) - o.get("failed", 0)
    else:
        return None
    stored = done["succeeded"] + done["failed"] + unf
    if mode in ("enqueue", "bulk"):  # nothing runs: everything stored is an accepted job (Wolverine's Redis stream and BullMQ count entries differently)
        stored = o.get("streamEntries", num(o, "waiting") if system.startswith("bullmq") else stored)
    return {"accepted": stored, **done, "unfinished": unf}


def reconcile(system, mode, r, norm, rc):
    """Complete = the store agrees with the harness: every offered job accepted (add tests) or every accepted job succeeded or failed with none left (drain, steady)."""
    if rc != 0 or r is None: return False, "harness failed"
    if r.get("skipped"): return True, "skipped"
    if mode == "writefloor": return True, ""
    if norm is None: return False, "no store counts"
    n = r.get("jobs") or 0
    if mode in ("enqueue", "bulk"):
        ok = norm["accepted"] >= n - (r.get("failedDispatch") or 0)
        return ok, "" if ok else f"store holds {norm['accepted']} of {n} offered"
    expected = r.get("inserted") if mode == "steady" else n - (r.get("failedDispatch") or 0)
    ok = norm["unfinished"] == 0 and norm["succeeded"] + norm["failed"] >= (expected or 0)
    return ok, "" if ok else f"succeeded {norm['succeeded']} failed {norm['failed']} unfinished {norm['unfinished']} of {expected}"


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


def gitq(*c):
    return subprocess.run(["git", *c], cwd=ROOT, capture_output=True, text=True)


def reset_store(kind):
    """Recreate a store container exactly as the workflow does (pinned image tag, the Runwick Redis profile), so each library starts on a fresh server."""
    sh(["docker", "rm", "-f", kind]); cpus = ["--cpus", a.store_cpus] if a.store_cpus else []
    cpus += ["--tmpfs", "/var/lib/postgresql" if kind == "bench-pg" else "/data"] if a.store_tmpfs else []
    if kind == "bench-pg":
        sh(["docker", "run", "-d", "--name", kind, *cpus, "-p", "127.0.0.1:55499:5432", "-e", "POSTGRES_PASSWORD=pg", "postgres:18"])
        while subprocess.run(["docker", "exec", kind, "pg_isready", "-U", "postgres", "-q"], capture_output=True).returncode: time.sleep(1)
    else:
        sh(["docker", "run", "-d", "--name", kind, *cpus, "-p", "127.0.0.1:56392:6379", "redis:8.2-alpine", "redis-server", "--appendonly", "yes", "--appendfsync", "always",
            "--no-appendfsync-on-rewrite", "no", "--maxmemory-policy", "noeviction", "--aof-load-truncated", "no", "--save", ""])
        while "PONG" not in sh(["docker", "exec", kind, "redis-cli", "ping"]): time.sleep(1)


def store_manifest(kind):
    """The store's image digest and the durability settings it reports about itself."""
    if not kind: return None
    m = {"container": kind, "image": sh(["docker", "inspect", "-f", "{{.Config.Image}}", kind]),
         "imageId": sh(["docker", "inspect", "-f", "{{.Image}}", kind]), "cpus": sh(["docker", "inspect", "-f", "{{.HostConfig.NanoCpus}}", kind])}
    m["repoDigest"] = sh(["docker", "image", "inspect", "-f", "{{index .RepoDigests 0}}", m["image"]])
    if kind == "bench-pg":
        m["settings"] = {k: sh(["docker", "exec", kind, "psql", "-U", "postgres", "-Atc", f"SHOW {k}"]) for k in
                         ("server_version", "fsync", "synchronous_commit", "full_page_writes", "wal_sync_method", "wal_level", "shared_buffers", "max_wal_size", "checkpoint_timeout")}
    else:
        m["settings"] = {k: sh(["docker", "exec", kind, "redis-cli", "CONFIG", "GET", k]).split("\n")[-1] for k in ("appendonly", "appendfsync", "save", "maxmemory-policy", "no-appendfsync-on-rewrite")}
        m["settings"]["redis_version"] = next((l.split(":")[1] for l in sh(["docker", "exec", kind, "redis-cli", "INFO", "server"]).splitlines() if l.startswith("redis_version")), "")
    return m


def manifest(libs):
    quota = read("/sys/fs/cgroup/cpu.max").strip()
    mount = sh(["findmnt", "-no", "SOURCE,FSTYPE", "-T", a.out])
    return {"host": host(), "systems": SYSTEMS, "workload": a.workload, "rounds": a.rounds, "scale": a.scale, "freshStores": a.fresh_stores, "storeTmpfs": a.store_tmpfs, "tmpdir": os.environ.get("TMPDIR"),
            "runwick": {"describe": gitq("describe", "--tags", "--always").stdout.strip(), "commit": gitq("rev-parse", "HEAD").stdout.strip(),
                        "srcDirty": bool(gitq("status", "--porcelain", "--", "src").stdout.strip()),
                        "srcIdenticalToV0_1_5": gitq("diff", "--quiet", "v0.1.5", "HEAD", "--", "src").returncode == 0},
            "libraries": libs, "stores": {k: store_manifest(k) for k in sorted({STORE[s] for s in SYSTEMS if STORE.get(s)})},
            "cpuQuota": quota, "outDirMount": mount, "gc": {"DOTNET_gcServer": "1"}, "orderPolicy": "reversed on even rounds"}


def main():
    os.makedirs(a.out, exist_ok=True)
    libs = {}; failures = incomplete = 0
    env = dict(os.environ, BENCH_CPU_ITERS=str(CPU_ITERS if a.workload == "cpu" else 0), DOTNET_NOLOGO="1", DOTNET_gcServer="1")
    tests = a.tests.split(",") if a.tests else list(TESTS[a.workload])
    for rnd in range(a.first_round, a.first_round + a.rounds):
        order = SYSTEMS if rnd % 2 else SYSTEMS[::-1]
        for system in order:
            tag = f"{system}--{a.workload}"
            kind = STORE[system]
            if a.fresh_stores and kind: reset_store(kind)
            store = Container(kind); pipe = Pipe(kind); atexit.register(pipe.close)
            if not os.path.exists(MANIFEST):
                json.dump(manifest({}), open(MANIFEST, "w"), indent=2)
            json.dump(host(), open(f"{a.out}/{tag}--host.json", "w"), indent=2)
            for test in tests:
                if test not in TESTS[a.workload] or (TESTS[a.workload][test][0] == "writefloor") != system.startswith("writefloor-"): continue
                for trace in ([False, True] if (a.trace_round and rnd == a.first_round + a.rounds - 1 and system.startswith("runwick")) else [False]):
                    c, cwd = cmd(system, test, trace)
                    name = f"{tag}--{test}--" + ("trace" if trace else f"r{rnd}"); outf = os.path.abspath(f"{a.out}/{name}.raw.json")
                    if os.path.exists(outf): os.remove(outf)
                    print(time.strftime("%H:%M:%S"), name, flush=True)
                    ru0 = resource.getrusage(resource.RUSAGE_CHILDREN)
                    full = c + ["-out" if system == "river" else "--out", outf]
                    rc, out, err, app_s, st_s, st0, t1, ru1, phases = run_sampled(full, cwd, env, store, pipe)
                    open(f"{a.out}/{name}.log", "w").write(" ".join(full) + "\n" + out[-6000:] + "\n--- stderr ---\n" + err[-6000:])
                    try: result = json.load(open(outf))
                    except (OSError, ValueError): result = None
                    mode = TESTS[a.workload][test][0]
                    norm = normalize(system, mode, result); complete, why = reconcile(system, mode, result, norm, rc)
                    if rc != 0 or result is None:
                        failures += 1; print(f"  FAILED rc={rc}: {err[-400:]}", flush=True)
                    elif not complete:
                        incomplete += 1; print(f"  INCOMPLETE: {why}", flush=True)
                    res = summarize(app_s, st_s, st0, t1, ru0, ru1)
                    jobs = (result or {}).get("finished") or (result or {}).get("completed") or (result or {}).get("jobs") or 0
                    if mode in ("enqueue", "bulk") and result: jobs = max(0, (result.get("jobs") or 0) - (result.get("failedDispatch") or 0))
                    cpu = res["app"]["cpuSeconds"] + ((res["store"] or {}).get("cpuSeconds") or 0)
                    wire = ((res["store"] or {}).get("netRxBytes") or 0) + ((res["store"] or {}).get("netTxBytes") or 0)
                    derived = {"jobs": jobs, "cpuMsPerJob": round(cpu * 1000 / jobs, 3) if jobs else None, "wireBytesPerJob": round(wire / jobs) if jobs and wire else None,
                               "storeWriteBytesPerJob": round(((res["store"] or {}).get("diskWriteBytes") or res["app"]["diskWriteBytes"]) / jobs) if jobs else None}
                    # Whole-run figures above include startup, seeding and shutdown (kept for continuity); "timed" is the measured phase alone.
                    derived["timed"] = timed(phases, jobs)
                    if norm and mode == "drain" and result:  # rate from the store-confirmed count, on both clocks the harness reports
                        for key, secs in (("workerStart", (result.get("clocks") or {}).get("workerStartSeconds")), ("firstClaim", (result.get("clocks") or {}).get("firstClaimSeconds") or result.get("drainSeconds"))):
                            if secs: derived[f"confirmedPerSec_{key}"] = round(norm["succeeded"] / secs, 1)
                    if result: libs[system] = lib_version(system, result)
                    json.dump({"system": system, "workload": a.workload, "test": test, "round": rnd, "trace": trace, "ok": rc == 0 and result is not None, "complete": complete,
                               "incompleteReason": why, "version": lib_version(system, result), "command": full, "result": result, "outcome": norm, "resources": res,
                               "derived": derived, "phases": {k: {"harnessMs": v["harnessMs"], "counters": v["counters"]} for k, v in phases.items()}},
                              open(f"{a.out}/{name}.json", "w"), indent=2)
    json.dump(manifest(libs), open(MANIFEST, "w"), indent=2)
    print(f"{failures} failed run(s), {incomplete} incomplete")


main()
