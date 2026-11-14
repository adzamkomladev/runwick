#!/usr/bin/env python3
"""Laravel database-queue benchmark driver (PHP 8.3, laravel/framework 13.x, `queue:work database`, 1 KiB no-op jobs).
  bench.py --db sqlite|pg --mode drain|steady|enqueue|bulk [--n 20000] [--workers 16] [--rate 100] [--seconds 30] [--out f.json]
  PostgreSQL: --pg host:port (superuser postgres/pg by default; PGPASSWORD overrides); a fresh database is created and dropped per run.
  SQLite: a fresh file under --sqlite-dir (default system temp), journal_mode WAL, synchronous FULL, busy_timeout 5000, BEGIN IMMEDIATE:
  the same durability as Runwick's SQLite default. PostgreSQL keeps the server defaults (fsync and synchronous_commit on).
Producers are `php artisan bench:produce`; consumers are N `php artisan queue:work database --sleep=1` processes. The database driver polls, so an
idle worker notices a new job within --sleep seconds (1 s is the smallest useful setting; the stock default is 3). The result JSON uses the
River/pg-boss/Horizon shape: completionsPerSec, latencyMs {p50,p95,p99,max} (steady), insertsPerSec."""
import argparse, glob, json, os, shutil, signal, sqlite3, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
for k, d in [("db", "sqlite"), ("mode", "drain"), ("n", 20000), ("calls", 1), ("workers", 16), ("rate", 100.0), ("seconds", 30.0), ("max-seconds", 1800.0),
             ("pg", "127.0.0.1:55499"), ("sqlite-dir", ""), ("sleep", 1), ("php-image", ""), ("out", "")]:
    ap.add_argument("--" + k, default=d, type=type(d))
ap.add_argument("--allow-deferred", action="store_true", help="report SQLite even when BEGIN IMMEDIATE is not in effect (PHP < 8.4)")
a = ap.parse_args()
APP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app")
tmp = tempfile.mkdtemp(prefix="laravel-bench-", dir=a.sqlite_dir or None)
lat_dir = os.path.join(tmp, "lat"); os.makedirs(lat_dir)
CFG = {"DB_CONNECTION": "sqlite" if a.db == "sqlite" else "pgsql"}  # what the app needs; also what is passed into a container
if a.mode == "steady": CFG["BENCH_LAT_DIR"] = lat_dir

if a.db == "sqlite":
    dbfile = os.path.join(tmp, "queue.sqlite"); open(dbfile, "w").close()
    CFG.update(DB_DATABASE=dbfile, DB_JOURNAL_MODE="WAL", DB_SYNCHRONOUS="FULL", DB_BUSY_TIMEOUT="5000", DB_TRANSACTION_MODE="IMMEDIATE")
    mon = sqlite3.connect(f"file:{dbfile}?mode=ro", uri=True, timeout=5, isolation_level=None)
    def q(sql): return mon.execute(sql).fetchone()[0]
    def failure_samples(): return [f"{n} x {e}" for e, n in mon.execute("SELECT substr(exception, 1, 160) e, count(*) FROM failed_jobs GROUP BY e ORDER BY 2 DESC LIMIT 5")]
    def cleanup(): mon.close()
    system = "laravel database queue (sqlite, WAL, synchronous=FULL)"
else:
    host, port = a.pg.split(":"); pw = os.environ.get("PGPASSWORD", "pg"); dbname = f"laravel_bench_{os.getpid()}"
    penv = dict(os.environ, PGPASSWORD=pw)
    def psql(sql, db="postgres"):
        p = subprocess.run(["psql", "-X", "-A", "-t", "-h", host, "-p", port, "-U", "postgres", "-d", db, "-c", sql], capture_output=True, text=True, env=penv)
        if p.returncode: raise RuntimeError(p.stderr)
        return p.stdout.strip()
    psql(f"CREATE DATABASE {dbname}")
    CFG.update(DB_HOST=host, DB_PORT=port, DB_DATABASE=dbname, DB_USERNAME="postgres", DB_PASSWORD=pw)
    def q(sql): return int(psql(sql, dbname))
    def failure_samples(): return psql("SELECT count(*) || ' x ' || substr(exception, 1, 160) FROM failed_jobs GROUP BY substr(exception, 1, 160) ORDER BY count(*) DESC LIMIT 5", dbname).splitlines()
    def cleanup(): psql(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)")
    system = "laravel database queue (postgresql, server defaults: fsync and synchronous_commit on)"

ENV = dict(os.environ, **CFG); containers = []
def launch(*args, **kw):
    """Popen an `artisan` command natively, or inside --php-image (host network, app and temp dir mounted at the same paths, tini as init)."""
    if not a.php_image: return subprocess.Popen(["php", "artisan", *args], cwd=APP, env=ENV, **kw)
    name = f"laravel-bench-{os.getpid()}-{len(containers)}"; containers.append(name)
    env = [x for k, v in CFG.items() for x in ("-e", f"{k}={v}")]
    return subprocess.Popen(["docker", "run", "--rm", "--init", "--network", "host", "--name", name, "-v", f"{APP}:{APP}", "-v", f"{tmp}:{tmp}", "-w", APP, *env,
                             a.php_image, "php", "artisan", *args], **kw)
def php(*args):
    p = launch(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True); o, e = p.communicate()
    return subprocess.CompletedProcess(args, p.returncode, o, e)
def pct(xs, p): return round(sorted(xs)[max(0, min(len(xs) - 1, int(p * len(xs)) - 1))], 2) if xs else 0

def producers(k, n_each, mode="single", extra=()):
    start = time.time() + 2.5  # php boot is ~0.3 s; everyone waits for this instant
    ps = [launch("bench:produce", f"--n={n_each}", f"--mode={mode}", f"--start-at={start}", *extra, stdout=subprocess.PIPE, text=True) for _ in range(k)]
    return [json.loads(p.communicate()[0].strip().splitlines()[-1]) for p in ps]

def start_workers():  # worker stderr goes to files: Laravel reports the real exception there, while failed_jobs only says "attempted too many times"
    return [launch("queue:work", "database", "--queue=default", f"--sleep={a.sleep}", "--tries=1", "--no-interaction",
                   stdout=subprocess.DEVNULL, stderr=open(os.path.join(tmp, f"worker-{i}.err"), "w"), start_new_session=True) for i in range(a.workers)]

def stop_workers(ws):
    for w in ws: os.killpg(w.pid, signal.SIGTERM)  # a worker finishes its current job, then exits
    for w in ws:
        try: w.wait(timeout=30)
        except subprocess.TimeoutExpired: os.killpg(w.pid, signal.SIGKILL)

res = {"phpImage": a.php_image or "native", "system": system, "mode": a.mode, "workers": a.workers, "workerSleepSeconds": a.sleep, "phpProcessesPerWorker": 1}
try:
    m = php("migrate", "--force")
    if m.returncode: sys.exit("migrate failed: " + m.stdout[-500:] + m.stderr[-500:])
    def backlog(): return q("SELECT count(*) FROM jobs")
    def failed(): return q("SELECT count(*) FROM failed_jobs")
    if a.db == "sqlite":  # prove the settings reached Laravel's own connection, not just the file (synchronous is per connection)
        jm = q("PRAGMA journal_mode")
        php_version = php("tinker", "--execute=echo PHP_VERSION;").stdout.strip()
        immediate = tuple(int(x) for x in php_version.split(".")[:2]) >= (8, 4)  # Laravel reads transaction_mode only on PHP 8.4+; before that every transaction is DEFERRED
        sy, bt = php("tinker", "--execute=echo DB::selectOne('PRAGMA synchronous')->synchronous, ' ', DB::selectOne('PRAGMA busy_timeout')->timeout;").stdout.split()
        res["sqliteVerified"] = {"journal_mode": jm, "synchronous": sy, "busy_timeout": bt, "php": php_version, "beginImmediate": immediate}
        if not immediate and not a.allow_deferred: sys.exit(f"PHP {php_version}: Laravel ignores transaction_mode before 8.4, so BEGIN IMMEDIATE is not in effect (use --php-image php:8.4-cli, or --allow-deferred)")
        if jm != "wal" or sy != "2" or bt != "5000": sys.exit(f"sqlite settings not applied: journal_mode={jm}, synchronous={sy}, busy_timeout={bt}")
    if a.mode in ("enqueue", "bulk"):
        k = a.calls if a.mode == "enqueue" else 1
        out = producers(k, a.n // k, "single" if a.mode == "enqueue" else "bulk")
        el = max(o["end"] for o in out) - min(o["start"] for o in out); lat = [x for o in out for x in o["lat"]]
        res.update(jobs=a.n, calls=k, insertsPerSec=round(sum(o["n"] for o in out) / el, 1), rowsInTable=backlog())
        if lat: res["insertLatencyMs"] = {"p50": pct(lat, .5), "p99": pct(lat, .99)}
    elif a.mode == "drain":
        producers(1, a.n, "bulk")  # backlog written before any worker exists; not timed
        assert backlog() == a.n, backlog()
        t0 = time.time(); ws = start_workers(); first = None
        while time.time() - t0 < a.max_seconds:
            left = backlog()
            if first is None and left < a.n: first = time.time()
            if left == 0: break
            time.sleep(0.05)
        t1 = time.time(); left = backlog(); stop_workers(ws); bad = failed()
        done = a.n - left - bad
        res.update(jobs=a.n, finished=done, failed=bad, drainSeconds=round(t1 - t0, 3), completionsPerSec=round(done / (t1 - t0), 1),
                   startupSeconds=round((first or t1) - t0, 2), completionsPerSecExcludingStartup=round(done / max(1e-9, t1 - (first or t0)), 1))
    else:
        ws = start_workers(); time.sleep(10 + a.workers * 0.1)  # let the workers boot
        out = producers(1, 0, "single", (f"--rate={a.rate}", f"--seconds={a.seconds}"))[0]
        dl = time.time() + 60
        while backlog() > 0 and time.time() < dl: time.sleep(0.05)
        el = time.time() - out["start"]
        time.sleep(1); stop_workers(ws)
        lat = []
        for f in glob.glob(lat_dir + "/*.lat"):
            for line in open(f):
                t, d = line.split(); lat.append((float(d) - float(t)) * 1000)
        res.update(offeredPerSec=a.rate, inserted=out["n"], completed=len(lat), failed=failed(), completionsPerSec=round(len(lat) / el, 1),
                   insertLatencyMs={"p50": pct(out["lat"], .5), "p99": pct(out["lat"], .99)},
                   latencyMs={"p50": pct(lat, .5), "p95": pct(lat, .95), "p99": pct(lat, .99), "max": pct(lat, 1)})
    if res.get("failed"):
        res["failureSamples"] = failure_samples()  # first 160 chars of each distinct failed_jobs exception, most common first
        errs = {}
        for f in glob.glob(tmp + "/worker-*.err"):
            for line in open(f, errors="replace"):
                if "SQLSTATE" in line or "Exception" in line: errs[line.strip()[:200]] = errs.get(line.strip()[:200], 0) + 1
        res["workerErrorSamples"] = [f"{n} x {e}" for e, n in sorted(errs.items(), key=lambda x: -x[1])[:5]]
finally:
    for c in containers: subprocess.run(["docker", "rm", "-f", c], capture_output=True)
    cleanup(); shutil.rmtree(tmp, ignore_errors=True)
txt = json.dumps(res, indent=2); print(txt)
if a.out: open(a.out, "w").write(txt)
