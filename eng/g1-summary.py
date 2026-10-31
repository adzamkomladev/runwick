#!/usr/bin/env python3
"""G-1 workload summary (07 §14.6): reads <perf-dir>/<ts>-<workload>/{result.json,env.json,<run>/result.json}
and prints markdown tables of the recorded measurements and the D-14 threshold comparisons.
Usage: eng/g1-summary.py artifacts/g1/<run-id>/perf > summary.md"""
import glob
import json
import os
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def g(o, *path, default=None):
    for p in path:
        if o is None:
            return default
        o = o.get(p) if isinstance(o, dict) else None
    return default if o is None else o


def fmt(v, digits=1):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def lat(o, name):
    """p50/p95/p99/max (n) of a latency block."""
    l = g(o, "latencies", name)
    if not l or not l.get("count"):
        return "—"
    return f"{fmt(l['p50Ms'], 0)} / {fmt(l['p95Ms'], 0)} / {fmt(l['p99Ms'], 0)} / {fmt(l['maxMs'], 0)} (n={l['count']})"


def worker_sum(run, key):
    return sum(g(w, "counters", key, default=0) for w in (w for w in run.get("workers") or [] if w))


def events(run, prefix):
    out = {}
    for w in (w for w in run.get("workers") or [] if w):
        for k, v in (w.get("counters") or {}).items():
            if k.startswith("event:" + prefix):
                out[k[len("event:"):]] = out.get(k[len("event:"):], 0) + v
    return out


def worker_lat_merged(run, name):
    """Worst (max over workers) p99 and min over workers of min, with the summed count."""
    ws = [g(w, "latencies", name) for w in (w for w in run.get("workers") or [] if w)]
    ws = [l for l in ws if l and l.get("count")]
    if not ws:
        return None
    return {"count": sum(l["count"] for l in ws), "minMs": min(l["minMs"] for l in ws), "p01Ms": min(l["p01Ms"] for l in ws),
            "p50Ms": max(l["p50Ms"] for l in ws), "p95Ms": max(l["p95Ms"] for l in ws),
            "p99Ms": max(l["p99Ms"] for l in ws), "maxMs": max(l["maxMs"] for l in ws), "meanMs": max(l["meanMs"] for l in ws)}


def pmm(l):
    if not l:
        return "—"
    return f"{fmt(l['p50Ms'], 0)} / {fmt(l['p95Ms'], 0)} / {fmt(l['p99Ms'], 0)} / {fmt(l['maxMs'], 0)} (n={l['count']})"


def verdict(ok):
    return "—" if ok is None else ("pass" if ok else "**miss**")


def run_rows(name, run):
    w = run.get("window") or {}
    pw = run.get("producerWindow") or {}
    c = run.get("ceilings") or {}
    corr = run.get("correctness") or {}
    disp = g(run, "producer", "latencies", "dispatch")
    due = worker_lat_merged(run, "dueToBegin")
    pipe = worker_lat_merged(run, "pipelineToFinish")
    ctl = {op: worker_lat_merged(run, op) for op in ("reserve", "begin", "renew", "finish")}
    cw = worker_lat_merged(run, "controlWaitSampled")
    margin = worker_lat_merged(run, "renewalMargin")
    losses = events(run, "lost:")
    stops = events(run, "pressureStop:")
    rows = [
        ("label / database", f"{run.get('label')} / {run.get('database')}"),
        ("run failure", run.get("failure") or "none"),
        ("measured window", f"{fmt(w.get('seconds'))} s"),
        ("offered / accepted / failed per s (producer)", f"{fmt(pw.get('offeredPerSec'), 2)} / {fmt(pw.get('acceptedPerSec'), 2)} / {fmt(pw.get('failedPerSec'), 2)}"),
        ("producer failures by reason", ", ".join(f"{k[7:]} {v}" for k, v in sorted((g(run, 'producer', 'counters') or {}).items()) if k.startswith("failed:")) or "—"),
        ("confirmed finishes/s", fmt(w.get("confirmedFinishesPerSec"), 2)),
        ("backlog start → end (max, slope/s)", f"{w.get('backlogStart')} → {w.get('backlogEnd')} (max {w.get('backlogMax')}, {fmt(w.get('backlogSlopePerSec'), 3)}/s)"),
        ("dispatch p50/p95/p99/max ms", pmm(disp)),
        ("due-to-Begin p50/p95/p99/max ms", pmm(due)),
        ("pipeline end → confirmed Finish p50/p95/p99/max ms", pmm(pipe)),
        *((f"{op} p50/p95/p99/max ms", pmm(l)) for op, l in ctl.items()),
        ("control-I/O wait (sampled) p50/p95/p99/max ms", pmm(cw)),
        ("renewal margin min / p01 / p50 ms", "—" if not margin else f"{fmt(margin['minMs'], 0)} / {fmt(margin['p01Ms'], 0)} / {fmt(margin['p50Ms'], 0)} (n={margin['count']})"),
        ("missed renewal margins", worker_sum(run, "renewalMarginMissed")),
        ("lease-safety / ownership losses", ", ".join(f"{k} {v}" for k, v in sorted(losses.items())) or "0"),
        ("pressure stops by reason (stopped s)", (", ".join(f"{k[13:]} {v}" for k, v in sorted(stops.items())) or "0") + f" ({worker_sum(run, 'pressureStoppedSeconds')} s)"),
        ("WAL bytes/job; transactions/s; receipt writes/s", f"{fmt(w.get('walBytesPerJob'), 0)}; {fmt(w.get('transactionsPerSec'), 2)}; {fmt(w.get('receiptWritesPerSec'), 2)}"),
        ("lock waiters max / mean", f"{w.get('lockWaitersMax')} / {fmt(w.get('lockWaitersMean'), 2)}"),
        ("connections max by application:role", ", ".join(f"{k} {v}" for k, v in sorted((run.get("connectionsMax") or {}).items()))),
        ("t_reserve / t_begin / t_finish mean (p99) ms", f"{fmt(c.get('tReserveMeanMs'), 0)} ({fmt(c.get('tReserveP99Ms'), 0)}) / {fmt(c.get('tBeginMeanMs'), 0)} ({fmt(c.get('tBeginP99Ms'), 0)}) / {fmt(c.get('tFinishMeanMs'), 0)} ({fmt(c.get('tFinishP99Ms'), 0)})"),
        ("store-time floor; slot-bound ceiling; C_claim", f"{fmt(c.get('storeTimeFloorMs'), 0)} ms; {fmt(c.get('slotBoundCeilingPerSec'), 1)}/s; {fmt(c.get('serialClaimCeilingPerSec'), 2)}/s"),
    ]
    for i, wk in enumerate((w for w in run.get("workers") or [] if w), 1):
        p = wk.get("process") or {}
        rows.append((f"worker{i} CPU s, GC pause %, alloc/job KiB, RSS MiB, timer lag max ms",
                     f"{fmt((p.get('cpuMs') or 0) / 1000, 0)}, {fmt(p.get('gcPausePercent'), 2)}, "
                     f"{fmt((p.get('allocatedBytes') or 0) / 1024 / max(1, wk.get('invocations') or 1), 1)}, "
                     f"{fmt((p.get('rssBytes') or 0) / 1048576, 0)}, {p.get('timerLagAllMaxMs')}"))
        adm = wk.get("admission") or {}
        if adm.get("secondsFromRestorationToReady") is not None:
            rows.append((f"worker{i} restoration → ready", f"{adm['secondsFromRestorationToReady']} s"))
        if wk.get("maxima"):
            rows.append((f"worker{i} maxima", ", ".join(f"{k} {v}" for k, v in sorted(wk["maxima"].items()))))
    if name == "G1-M1":
        probes = worker_sum(run, "probe:empty") + worker_sum(run, "probe:ready")
        secs = w.get("seconds") or 0
        rows.append(("idle probes (whole run) / measured tx/s", f"{probes} / {fmt(w.get('transactionsPerSec'), 2)}"))
    rows.append(("correctness", f"{corr.get('violationCount')} violation(s); jobs {corr.get('jobs')}, succeeded {corr.get('succeededJobs')}, open {corr.get('openJobs')}, "
                                f"invoked more than once {corr.get('jobsInvokedMoreThanOnce')}, >1 execution {corr.get('jobsWithMoreThanOneExecution')}"
                                + ("; " + "; ".join(corr.get("violations")) if corr.get("violations") else "")))
    thresholds = []
    if name == "P-01":
        thresholds = [
            ("throughput ≥ 100 finishes/s, no sustained backlog growth", verdict((w.get("confirmedFinishesPerSec") or 0) >= 100 and (w.get("backlogSlopePerSec") or 0) <= 0.05)),
            ("dispatch p95 ≤ 50 ms / p99 ≤ 200 ms", verdict(bool(disp) and disp["p95Ms"] <= 50 and disp["p99Ms"] <= 200)),
            ("due-to-Begin p99 ≤ 2 s", verdict(bool(due) and due["p99Ms"] <= 2000)),
            ("pipeline → confirmed Finish p99 ≤ 500 ms", verdict(bool(pipe) and pipe["p99Ms"] <= 500)),
        ]
    if name in ("P-01", "P-02", "G1-M2", "G1-M3"):
        worst = max((l["p99Ms"] for l in ctl.values() if l), default=None)
        thresholds += [
            ("healthy control I/O p99 ≤ 100 ms incl. admission", verdict(None if worst is None else worst <= 100)),
        ]
    if name in ("P-01", "P-02", "G1-M2"):
        thresholds += [("zero lease-safety losses", verdict(sum(losses.values()) == 0))]
    if name.startswith("P-08"):
        ready = [g(wk, "admission", "secondsFromRestorationToReady") for wk in (w for w in run.get("workers") or [] if w)]
        ready = [r for r in ready if r is not None]
        conn_ok = all(v <= (6 if k.endswith(":control") else 8) for k, v in (run.get("connectionsMax") or {}).items() if k.startswith("wl-worker"))
        thresholds += [
            ("resource counts within declared caps (connections control 6 / work 8; entries ≤ 16)",
             verdict(conn_ok and all((g(wk, "maxima", "entries") or 0) <= 16 for wk in (w for w in run.get("workers") or [] if w)))),
            ("admission pauses", verdict(sum(stops.values()) > 0)),
        ]
        if name == "P-08c":
            thresholds += [("readiness ≤ 30 s after restoration", verdict(bool(ready) and max(ready) <= 30))]
    thresholds += [("correctness: zero violations", verdict(corr.get("violationCount") == 0))]
    return rows, thresholds


def main():
    root = sys.argv[1]
    for d in sorted(glob.glob(os.path.join(root, "*-*"))):
        if not os.path.isdir(d) or not os.path.exists(os.path.join(d, "result.json")):
            continue
        top = load(os.path.join(d, "result.json"))
        name = top["workload"]
        print(f"### {name}\n")
        print(f"Result directory `{os.path.relpath(d, os.path.dirname(os.path.dirname(root)))}`; "
              f"{top.get('startedAt', '')[:19]} → {top.get('finishedAt', '')[:19]} UTC; load average at end {top.get('loadAverageAtEnd')}; "
              f"correctness violations {top.get('correctnessViolations')}.")
        extra = {k: v for k, v in top.items() if k not in ("workload", "startedAt", "finishedAt", "loadAverageAtEnd", "correctnessViolations", "runs")}
        if extra:
            print("Summary: " + "; ".join(f"{k} = {v}" for k, v in extra.items()) + ".")
        print()
        runs = [load(p) for p in sorted(glob.glob(os.path.join(d, "*", "result.json")))]
        for run in runs:
            ex = run.get("extra") or {}
            if ex.get("steps"):
                print(f"Ramp `{run.get('label')}` steps:\n")
                print("| nominal/s | offered/s | accepted/s | finishes/s | backlog | saturated |")
                print("|---|---|---|---|---|---|")
                for s in ex["steps"]:
                    print(f"| {s['nominalRate']} | {fmt(s['offeredPerSec'], 2)} | {fmt(s['acceptedPerSec'], 2)} | {fmt(s['confirmedFinishesPerSec'], 2)} | "
                          f"{s['backlogStart']} → {s['backlogEnd']} | {s['saturated']} |")
                print()
        tables = [(run.get("label"), *run_rows(name, run)) for run in runs if not (run.get("extra") or {}).get("steps")]
        if not tables:
            continue
        labels = [t[0] for t in tables]
        print("| Measurement | " + " | ".join(labels) + " |")
        print("|---|" + "---|" * len(labels))
        for i, (k, _) in enumerate(tables[0][1]):
            vals = [str(t[1][i][1]) if i < len(t[1]) and t[1][i][0] == k else "—" for t in tables]
            print(f"| {k} | " + " | ".join(v.replace("|", "\\|") for v in vals) + " |")
        print()
        print("| D-14 threshold | " + " | ".join(labels) + " |")
        print("|---|" + "---|" * len(labels))
        for i, (k, _) in enumerate(tables[0][2]):
            print(f"| {k} | " + " | ".join(t[2][i][1] if i < len(t[2]) else "—" for t in tables) + " |")
        print()
        for run in runs:
            plans = g(run, "store", "plans") or {}
            if plans:
                print(f"<details><summary>Query plans at end of {run.get('label')} (EXPLAIN ANALYZE, BUFFERS)</summary>\n")
                for k, v in plans.items():
                    print(f"`{k}`\n\n```\n{v}\n```\n")
                print("</details>\n")
                break


if __name__ == "__main__":
    main()
