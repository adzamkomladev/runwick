#!/usr/bin/env python3
"""Celery benchmark driver (Redis broker, prefork pool). Same modes and JSON fields as the other harnesses.
  bench.py --redis 127.0.0.1:56392 --mode enqueue|bulk|drain|steady [--n 20000] [--calls 1] [--workers 16] [--rate 100] [--seconds 30] [--out f.json]
The Redis instance is FLUSHALLed: dedicated, throwaway only. Consumers are `celery worker --pool prefork -c <workers>` (one OS process per slot).
Celery has no bulk-enqueue API, so bulk reports skipped and drain seeds its backlog with 8 producer processes (not timed)."""
import argparse, json, multiprocessing as mp, os, signal, subprocess, sys, time
import celery, redis

ap = argparse.ArgumentParser()
for k, d in [("redis", "127.0.0.1:56392"), ("mode", "drain"), ("n", 20000), ("calls", 1), ("workers", 16), ("rate", 100.0), ("seconds", 30.0), ("max-seconds", 1800.0), ("out", "")]:
    ap.add_argument("--" + k, default=d, type=type(d))
a = ap.parse_args()
HERE = os.path.dirname(os.path.abspath(__file__))
URL = f"redis://{a.redis}/0"
os.environ["BENCH_BROKER"] = URL
sys.path.insert(0, HERE)
r = redis.Redis.from_url(URL, decode_responses=True)
r.flushall()
PAD = "x" * (1024 - 32)
res = {"system": f"celery {celery.__version__} (redis broker, prefork)", "mode": a.mode, "workers": a.workers,
       "redisAppendfsync": r.config_get("appendfsync").get("appendfsync")}

def pct(xs, p): return round(sorted(xs)[max(0, min(len(xs) - 1, int(p * len(xs)) - 1))], 2) if xs else 0

def produce(args):
    n, start_at, timed = args
    from tasks import noop
    noop.apply_async(args=("warm",), queue="warmup")  # open the producer connection before the clock starts
    while time.time() < start_at: time.sleep(0.001)
    lat, t0 = [], time.time()
    for _ in range(n):
        t = time.perf_counter(); noop.apply_async(args=(PAD,))
        if timed: lat.append((time.perf_counter() - t) * 1000)
    return {"n": n, "start": t0, "end": time.time(), "lat": lat}

def producers(k, n, timed):
    with mp.get_context("fork").Pool(k) as pool:
        out = pool.map(produce, [(n // k + (1 if i < n % k else 0), time.time() + 1.5, timed) for i in range(k)])
    r.delete("warmup")
    return out

def start_worker():
    return subprocess.Popen([sys.executable, "-m", "celery", "-A", "tasks", "worker", "--pool", "prefork", "-c", str(a.workers), "-Q", "celery",
                             "--without-gossip", "--without-mingle", "--without-heartbeat", "--loglevel", "WARNING"],
                            cwd=HERE, env=dict(os.environ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)

def stop_worker(w):
    os.killpg(w.pid, signal.SIGTERM)
    try: w.wait(timeout=60)
    except subprocess.TimeoutExpired: os.killpg(w.pid, 9); w.wait()

def done(): return int(r.get("bench:done") or 0)

if a.mode == "bulk":
    res.update(skipped="Celery has no bulk-enqueue API")
elif a.mode == "enqueue":
    out = producers(a.calls, a.n, True)
    el = max(o["end"] for o in out) - min(o["start"] for o in out); lat = [x for o in out for x in o["lat"]]
    res.update(jobs=a.n, calls=a.calls, accepted=r.llen("celery"), insertsPerSec=round(a.n / el, 1),
               insertLatencyMs={"p50": pct(lat, .5), "p95": pct(lat, .95), "p99": pct(lat, .99)})
elif a.mode == "drain":
    producers(8, a.n, False)
    assert r.llen("celery") == a.n, r.llen("celery")
    t0 = time.time(); w = start_worker(); first = None
    while time.time() - t0 < a.max_seconds:
        d = done()
        if first is None and d > 0: first = time.time()
        if d >= a.n: break
        time.sleep(0.02)
    t1 = time.time(); d = done(); stop_worker(w)
    res.update(jobs=a.n, finished=d, drainSeconds=round(t1 - t0, 3), completionsPerSec=round(d / (t1 - t0), 1),
               startupSeconds=round((first or t1) - t0, 2), completionsPerSecExcludingStartup=round(d / max(1e-9, t1 - (first or t0)), 1))
else:
    from tasks import noop
    w = start_worker()
    noop.apply_async(args=(PAD,))  # probe: the worker is up once it finishes one job
    dl = time.time() + 120
    while done() < 1 and time.time() < dl: time.sleep(0.05)
    r.delete("bench:done")
    t0 = time.time(); stop = t0 + a.seconds; nxt = t0; inserted = 0
    while time.time() < stop:
        noop.apply_async(args=(PAD, time.time() * 1000)); inserted += 1
        nxt += 1 / a.rate; s = nxt - time.time()
        if s > 0: time.sleep(s)
    dl = time.time() + 60
    while done() < inserted and time.time() < dl: time.sleep(0.02)
    el = time.time() - t0; stop_worker(w)
    lat = [float(x) for x in r.lrange("bench:lat", 0, -1)]
    res.update(offeredPerSec=a.rate, inserted=inserted, completed=done(), completionsPerSec=round(done() / el, 1),
               latencyMs={"p50": pct(lat, .5), "p95": pct(lat, .95), "p99": pct(lat, .99), "max": pct(lat, 1)})
txt = json.dumps(res, indent=2); print(txt)
if a.out: open(a.out, "w").write(txt)
