#!/usr/bin/env python3
"""Laravel Horizon benchmark driver (PHP 8.3, laravel/framework 13.40.0, laravel/horizon 5.50.1, phpredis).
  bench.py --redis 127.0.0.1:56391 --mode enqueue|bulk|drain|steady [--n 20000] [--calls 1] [--workers 16] [--rate 100] [--seconds 30] [--out f.json]
Producers are `php artisan bench:produce` processes (one per concurrent caller). Consumers are real Horizon: `php artisan horizon`
(master supervisor + one supervisor with --workers PHP worker processes, balance=false). The Redis instance is FLUSHALLed: dedicated, throwaway only."""
import argparse, json, os, socket, subprocess, sys, time

ap = argparse.ArgumentParser()
for k, d in [("redis", "127.0.0.1:56391"), ("mode", "drain"), ("n", 20000), ("calls", 1), ("workers", 16), ("rate", 100.0), ("seconds", 30.0), ("max-seconds", 1800.0), ("out", "")]:
    ap.add_argument("--" + k, default=d, type=type(d))
a = ap.parse_args()
host, port = a.redis.split(":"); port = int(port)
APP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app")
ENV = dict(os.environ, REDIS_HOST=host, REDIS_PORT=str(port), BENCH_WORKERS=str(a.workers))

class R:
    def __init__(s): s.f = socket.create_connection((host, port)).makefile("rwb", buffering=0) if False else None; s.s = socket.create_connection((host, port)); s.b = s.s.makefile("rb")
    def cmd(s, *args):
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(x := str(y).encode()), x) for y in args); s.s.sendall(out); return s.read()
    def read(s):
        l = s.b.readline(); t, v = l[:1], l[1:-2]
        if t in b"+-": return v.decode()
        if t == b":": return int(v)
        if t == b"$": n = int(v); return None if n < 0 else s.b.read(n + 2)[:-2].decode()
        return [s.read() for _ in range(int(v))]
r = R()
r.cmd("FLUSHALL")
fsync = r.cmd("CONFIG", "GET", "appendfsync")[1]

def counts():
    keys = r.cmd("KEYS", "*queues:default*")
    n = 0
    for k in keys:
        if k.endswith("queues:default"): n += r.cmd("LLEN", k)
        elif k.endswith(("queues:default:reserved", "queues:default:delayed")): n += r.cmd("ZCARD", k)
    return n, keys

def producers(k, n_each, mode="single", extra=()):
    start = time.time() + 2.5  # php boot is ~0.3 s; everyone waits for this instant
    ps = [subprocess.Popen(["php", "artisan", "bench:produce", f"--n={n_each}", f"--mode={mode}", f"--start-at={start}", *extra], cwd=APP, env=ENV, stdout=subprocess.PIPE, text=True) for _ in range(k)]
    return [json.loads(p.communicate()[0].strip().splitlines()[-1]) for p in ps]

def pct(xs, p): return round(sorted(xs)[max(0, min(len(xs) - 1, int(p * len(xs)) - 1))], 2) if xs else 0

def start_horizon():
    return subprocess.Popen(["php", "artisan", "horizon"], cwd=APP, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)

def stop_horizon(h):
    subprocess.run(["php", "artisan", "horizon:terminate"], cwd=APP, env=ENV, capture_output=True)
    try: h.wait(timeout=30)
    except subprocess.TimeoutExpired: os.killpg(h.pid, 9)

res = {"system": "laravel-horizon 5.50.1 (framework 13.40.0, php 8.3, phpredis)", "mode": a.mode, "workers": a.workers, "redisAppendfsync": fsync, "phpProcessesPerWorker": 1}
if a.mode in ("enqueue", "bulk"):
    k = a.calls if a.mode == "enqueue" else 1
    out = producers(k, a.n // k, "single" if a.mode == "enqueue" else "bulk")
    el = max(o["end"] for o in out) - min(o["start"] for o in out); lat = [x for o in out for x in o["lat"]]
    res.update(jobs=a.n, calls=k, insertsPerSec=round(sum(o["n"] for o in out) / el, 1))
    if lat: res["insertLatencyMs"] = {"p50": pct(lat, .5), "p99": pct(lat, .99)}
elif a.mode == "drain":
    producers(1, a.n, "bulk")  # backlog written before Horizon exists; not timed
    assert counts()[0] == a.n, counts()
    t0 = time.time(); h = start_horizon(); first = None
    while time.time() - t0 < a.max_seconds:
        left, _ = counts()
        if first is None and left < a.n: first = time.time()
        if left == 0: break
        time.sleep(0.02)
    t1 = time.time(); left, _ = counts(); stop_horizon(h)
    done = a.n - left
    res.update(jobs=a.n, finished=done, failedInRedis=r.cmd("ZCARD", "failed_jobs") if False else None, drainSeconds=round(t1 - t0, 3), completionsPerSec=round(done / (t1 - t0), 1),
               startupSeconds=round((first or t1) - t0, 2), completionsPerSecExcludingStartup=round(done / max(1e-9, t1 - (first or t0)), 1))
else:
    h = start_horizon(); time.sleep(10 + a.workers * 0.1)  # let the master and its workers come up
    out = producers(1, 0, "single", (f"--rate={a.rate}", f"--seconds={a.seconds}"))[0]
    dl = time.time() + 60
    while counts()[0] > 0 and time.time() < dl: time.sleep(0.05)
    el = time.time() - out["start"]
    time.sleep(1); stop_horizon(h)
    # Horizon's own job records hold created_at (set at push) and completed_at (unix seconds as text): latency = completed_at - pushed_at.
    lat, cur = [], "0"
    while True:
        cur, ks = r.cmd("SCAN", cur, "MATCH", "*horizon:*", "COUNT", 1000); ks = [k for k in ks]
        for k in ks:
            if r.cmd("TYPE", k) != "hash": continue
            p, c = r.cmd("HMGET", k, "created_at", "completed_at")
            if p and c: lat.append((float(c) - float(p)) * 1000)
        if cur == "0": break
    res.update(offeredPerSec=a.rate, inserted=out["n"], completed=len(lat), completionsPerSec=round(len(lat) / el, 1),
               latencyMs={"p50": pct(lat, .5), "p95": pct(lat, .95), "p99": pct(lat, .99), "max": pct(lat, 1)})
txt = json.dumps(res, indent=2); print(txt)
if a.out: open(a.out, "w").write(txt)
