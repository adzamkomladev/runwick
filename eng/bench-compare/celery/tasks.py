"""Celery app for the benchmark: Redis broker, no result backend (task_ignore_result), otherwise Celery defaults
(acks_late off, prefetch multiplier 4, prefork pool). Each task records its own completion with one pipelined
INCR (+ RPUSH of its enqueue-to-finish latency in steady mode) on the same Redis, so the driver can count finished jobs."""
import hashlib, os, time
import redis
from celery import Celery

BROKER = os.environ.get("BENCH_BROKER", "redis://127.0.0.1:56392/0")
app = Celery("bench", broker=BROKER)
app.conf.update(task_ignore_result=True, broker_connection_retry_on_startup=True, worker_hijack_root_logger=False)
ITERS = int(os.environ.get("BENCH_CPU_ITERS") or 0)
_r = None


@app.task(name="noop")
def noop(pad, t=None):
    global _r
    if ITERS:  # shared CPU-heavy job (ci/README.md)
        b = bytearray(pad.encode()[:1024].ljust(1024, b"x"))
        for _ in range(ITERS):
            b[0:32] = hashlib.sha256(b).digest()
    if _r is None:
        _r = redis.Redis.from_url(BROKER)  # per worker process, created after fork
    p = _r.pipeline(transaction=False)
    p.incr("bench:done")
    if t is not None:
        p.rpush("bench:lat", time.time() * 1000 - t)
    p.execute()
