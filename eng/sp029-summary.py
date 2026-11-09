#!/usr/bin/env python3
"""SP-029 summary: reads <out>/perf/*-<case>/result.json written by eng/run-sp029.sh and writes <out>/summary.md with
one verdict per performance-plan threshold (docs/architecture/performance-acceptance-plan.md).

Verdicts are PASS / FAIL only with --reference (a dedicated, quiet reference host, full profile, 24 h soak). Otherwise
each is INFORMATIONAL with what it would have been ("would pass" / "would miss"). A threshold without data is NO DATA.
Usage: sp029-summary.py <out-dir> [--reference] [--profile short|full]
"""
import glob
import json
import os
import sys

out = sys.argv[1]
reference = "--reference" in sys.argv
profile = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "?"


def load(case):
    files = sorted(glob.glob(os.path.join(out, "perf", f"*-{case}", "result.json")))
    return json.load(open(files[-1])) if files else None


def get(node, *path):
    for p in path:
        if node is None:
            return None
        node = node[p] if isinstance(node, list) and isinstance(p, int) and p < len(node) else node.get(p) if isinstance(node, dict) else None
    return node


def runs(result):
    return result["runs"] if result else []


def nums(values):
    return [v for v in values if isinstance(v, (int, float))]


rows = []  # (threshold, measured, rule, ok or None, note)


def verdict(ok):
    if ok is None:
        return "NO DATA"
    if reference:
        return "PASS" if ok else "FAIL"
    return f"INFORMATIONAL (would {'pass' if ok else 'miss'})"


def add(name, measured, rule, ok, note=""):
    rows.append((name, measured, rule, ok, note))


cases = ["P-01", "P-01t", "P-02", "P-03", "P-04", "P-05", "P-06r", "P-06s", "P-07", "P-08a", "P-08b", "P-08c", "P-09", "P-10"]
results = {c: load(c) for c in cases}

# Correctness under every workload.
ran = {c: r for c, r in results.items() if r}
violations = sum(r.get("correctnessViolations", 0) for r in ran.values())
failures = {c: sum(1 for x in runs(r) if x.get("failure")) for c, r in ran.items()}
add("Correctness under every workload", f"{violations} violation(s) over {len(ran)} case(s); run failures: "
    + (", ".join(f"{c} {n}" for c, n in failures.items() if n) or "none"),
    "zero violated invariants, no false durable success", violations == 0 if ran else None,
    "a run failure (for example a backlog not drained in time) is reported, not counted as a violation")

# P-01 throughput and latency.
p01 = runs(results["P-01"])
fin = nums(get(x, "window", "confirmedFinishesPerSec") for x in p01)
slope = nums(get(x, "window", "backlogSlopePerSec") for x in p01)
add("P-01 single-worker throughput", f"{', '.join(map(str, fin))} finishes/s; backlog slope {', '.join(map(str, slope))}/s",
    "≥ 100/s steady, no sustained backlog growth", (min(fin) >= 100 and max(slope) <= 0.1) if fin and slope else None)
d95 = nums(get(x, "producer", "latencies", "dispatchCall", "p95Ms") for x in p01)
d99 = nums(get(x, "producer", "latencies", "dispatchCall", "p99Ms") for x in p01)
due = nums(get(x, "store", "dueToBeginFirstExecution", "p99Ms") for x in p01)
pipe = nums(get(x, "workers", 0, "latencies", "pipelineToFinish", "p99Ms") for x in p01)
lat_ok = None if not (d95 and d99 and due and pipe) else max(d95) <= 50 and max(d99) <= 200 and max(due) <= 2000 and max(pipe) <= 500
add("P-01 latency", f"dispatch p95 {max(d95, default='-')} / p99 {max(d99, default='-')} ms; due-to-Begin p99 {max(due, default='-')} ms; "
    f"pipeline-to-Finish p99 {max(pipe, default='-')} ms (worst repetition)",
    "dispatch p95 ≤ 50 / p99 ≤ 200 ms; due-to-Begin p99 ≤ 2 s; pipeline-to-Finish p99 ≤ 500 ms", lat_ok)
ctl = nums(get(x, "ceilings", k) for x in p01 for k in ("tReserveP99Ms", "tBeginP99Ms", "tFinishP99Ms", "tRenewP99Ms"))
lost = sum(v for x in p01 for w in (x.get("workers") or []) if w for k, v in (w.get("counters") or {}).items() if k.startswith("event:lost:"))
add("Healthy control I/O", f"worst store-call p99 {max(ctl, default='-')} ms; {lost} lease loss(es)",
    "p99 ≤ 100 ms; zero lease-safety losses", (max(ctl) <= 100 and lost == 0) if ctl else None)

# Instrumentation cost.
t = get(results["P-01t"], "summary") or {}
on = [v for k, v in t.items() if k.startswith("on-")]
off = [v for k, v in t.items() if k.startswith("off-")]


def mean(xs):
    xs = nums(xs)
    return sum(xs) / len(xs) if xs else None


on_tp, off_tp = mean(x.get("confirmedFinishesPerSec") for x in on), mean(x.get("confirmedFinishesPerSec") for x in off)
on_al, off_al = mean(x.get("workerAllocatedBytesPerJob") for x in on), mean(x.get("workerAllocatedBytesPerJob") for x in off)
if None not in (on_tp, off_tp, on_al, off_al) and off_tp and off_al:
    deg, inc = 100 * (off_tp - on_tp) / off_tp, 100 * (on_al - off_al) / off_al
    add("Instrumentation cost (P-01)", f"throughput {off_tp:.1f} → {on_tp:.1f}/s ({deg:+.1f} % loss); allocations/job {off_al:.0f} → {on_al:.0f} B ({inc:+.1f} %)",
        "throughput loss ≤ 10 %, allocations/job increase ≤ 15 %", deg <= 10 and inc <= 15, "raw totals in perf/*-P-01t/result.json")
else:
    add("Instrumentation cost (P-01)", "-", "throughput loss ≤ 10 %, allocations/job increase ≤ 15 %", None)

# P-02 and P-03: measurements.
p02 = results["P-02"]
add("P-02 store overhead (measurement)", f"sustainable {get(p02, 'sustainableRate')}/s" if p02 else "-", "no threshold: maximum and percentiles recorded",
    True if p02 else None)
p03 = get(results["P-03"], "summary")
if p03:
    lim = p03.get("limitCheck") or {}
    add("P-03 payload limit", f"1 MiB + 1 B refused with {lim.get('oversizeRejectedWith')}, {lim.get('jobsStored')} stored",
        "over-limit payload refused (payload_too_large), nothing stored", bool(lim.get("passed")))
    for label in ("64k", "1m"):
        for i, r in enumerate(p03.get(label) or []):
            add(f"P-03 {label} rep{i + 1} (measurement)",
                f"{r.get('acceptedPerSec')} accepted/s ({r.get('ingressBytesPerSec')} B/s in), {r.get('confirmedFinishesPerSec')} finishes/s; "
                f"worker {r.get('workerAllocatedBytesPerJob')} B/job, producer {r.get('producerAllocatedBytesPerJob')} B/job allocated; "
                f"WAL {r.get('walBytesPerJob')} B/job; dispatch p99 {r.get('dispatchP99Ms')} ms",
                "no threshold: allocation cost and ingress recorded", r.get("violations") == 0)
else:
    add("P-03 payload pressure", "-", "limit check and allocation cost", None)

# P-04 fairness.
p04 = get(results["P-04"], "summary") or {}
fair = [(k, v) for k, v in p04.items() if k in ("a", "b")]
if fair:
    ok = all(isinstance(v.get("maxDeviationPp"), (int, float)) and v["maxDeviationPp"] <= 2 and (v.get("admissions") or 0) >= 10000 for _, v in fair)
    add("P-04 fairness (aging off)", "; ".join(f"{k}: {v.get('maxDeviationPp')} pp over {v.get('admissions')} admissions" for k, v in fair),
        "shares within 2 pp over ≥ 10,000 admissions", ok, "variant c (aging) and d (CPU) are reported in the result file")
else:
    add("P-04 fairness (aging off)", "-", "shares within 2 pp over ≥ 10,000 admissions", None)

# P-05 efficiency.
ratio = get(results["P-05"], "summary", "unconstrained", "threshold", "ratio3v1")
add("P-05 efficiency", f"3 replicas / 1 replica = {ratio}", "≥ 2× before a measured shared bottleneck",
    ratio >= 2 if isinstance(ratio, (int, float)) else None, "constrained/hot-key bottleneck is reported, not judged")

# P-06 scheduler and rate.
p06s = get(results["P-06s"], "summary") or {}
lag = p06s.get("lagP99S")
bound = p06s.get("lagBoundS") or 13  # owner decision 2026-11-09: 1,000 schedules ÷ 100/s + 3 s (older results lack lagBoundS)
catch = p06s.get("catchUp") or {}
p06r_v = get(results["P-06r"], "correctnessViolations")
add("P-06 scheduler", f"materialization lag p99 {lag} s; catch-up {catch.get('occurrencesEmitted')} of {catch.get('occurrencesRequested')} emitted; "
    f"lease losses {p06s.get('leaseLosses')}; rate-window violations {p06r_v}",
    f"lag p99 ≤ schedules ÷ JobsPerSecond + 3 s ({bound} s), distinct occurrences, bounded catch-up, no lease loss",
    (lag <= bound and p06s.get("leaseLosses") == 0 and p06r_v == 0 and results["P-06s"]["correctnessViolations"] == 0) if isinstance(lag, (int, float)) and p06r_v is not None else None)

# P-07 management.
p07 = [v for v in (get(results["P-07"], "summary") or {}).values() if v]
if p07:
    m = p07[-1]
    allp = get(m, "latencies", "all") or {}
    rows_ = m.get("jobRows") or 0
    seq = m.get("extraSeqRowsReadPerPoll")
    deg = m.get("throughputDegradationPercent")
    ok = None if not allp else (allp.get("p95Ms", 1e9) <= 250 and allp.get("p99Ms", 1e9) <= 1000 and (deg is not None and deg <= 20)
                               and (seq is not None and seq < 0.1 * rows_) and m.get("errors") == 0)
    add("P-07 management", f"{m.get('viewers')} viewers, {m.get('polls')} polls, {m.get('errors')} errors; list p95 {allp.get('p95Ms')} / p99 {allp.get('p99Ms')} ms; "
        f"worker {m.get('baselineFinishesPerSec')} → {m.get('withViewersFinishesPerSec')} finishes/s ({deg} % loss); "
        f"extra sequential rows read per poll {seq} of {rows_} store rows",
        "list p95 ≤ 250 ms / p99 ≤ 1 s; worker throughput loss ≤ 20 %; no unbounded exact-count scan", ok)
else:
    add("P-07 management", "-", "list p95 ≤ 250 ms / p99 ≤ 1 s; worker throughput loss ≤ 20 %; no unbounded exact-count scan", None)

# P-08 outage.
p08c = runs(results["P-08c"])
ready = nums(get(w, "admission", "secondsFromRestorationToReady") for x in p08c for w in (x.get("workers") or []) if w)
paused = any("closed" in (t.get("state") or "") for x in p08c for w in (x.get("workers") or []) if w for t in (get(w, "admission", "transitions") or []))
add("P-08 outage", f"readiness {max(ready, default='-')} s after restoration; admission paused: {paused}; violations {get(results['P-08c'], 'correctnessViolations')}",
    "caps held, admission pauses, ready ≤ 30 s after restoration",
    (max(ready) <= 30 and paused and results["P-08c"]["correctnessViolations"] == 0) if ready else None,
    "P-08a/P-08b (1 s / 5 s control latency) are recorded in their result files")

# P-09 relay.
ph = get(results["P-09"], "summary", "phase1") or {}
add("P-09 relay", f"{ph.get('confirmedDeliveriesPerSec')} deliveries/s; ready-to-confirm p99 {ph.get('readyToConfirmP99Ms')} ms; violations {get(results['P-09'], 'correctnessViolations')}",
    "≥ 100 deliveries/s; p99 ≤ 2 s; lost marks never add a second mapping",
    (ph["confirmedDeliveriesPerSec"] >= 100 and isinstance(ph.get("readyToConfirmP99Ms"), (int, float)) and ph["readyToConfirmP99Ms"] <= 2000
     and results["P-09"]["correctnessViolations"] == 0) if isinstance(ph.get("confirmedDeliveriesPerSec"), (int, float)) else None)

# P-10 bounded growth.
g = get(results["P-10"], "summary", "growth") or {}
dur = None
r10 = runs(results["P-10"])
if r10:
    marks = r10[0].get("marks") or {}
    if "measureStart" in marks and "measureEnd" in marks:
        from datetime import datetime
        dur = (datetime.fromisoformat(marks["measureEnd"]) - datetime.fromisoformat(marks["measureStart"])).total_seconds() / 3600
if "heapRatio" in g:
    add("P-10 bounded growth", f"{dur:.2f} h measured; post-GC heap last/first quarter {g['heapRatio']} (RSS {g.get('rssRatio')}); "
        f"attempt entries {get(g, 'firstQuarter', 'attemptEntriesMean')} → {get(g, 'lastQuarter', 'attemptEntriesMean')}; "
        f"scope map {get(g, 'firstQuarter', 'scopeMapMean')} → {get(g, 'lastQuarter', 'scopeMapMean')}; "
        f"finishes {get(g, 'firstQuarter', 'finishesAtEnd')} → {get(g, 'lastQuarter', 'finishesAtEnd')}",
        "no cache growth with completed jobs; heap last 6 h ≤ 110 % of first steady 6 h (24 h run)",
        bool(g.get("heapBounded")) and bool(g.get("cachesBounded")) and (results["P-10"]["correctnessViolations"] == 0),
        "" if dur and dur >= 24 else "shorter than 24 h: a smoke run, never a qualification result")
else:
    add("P-10 bounded growth", g.get("error", "-"), "no cache growth; heap last 6 h ≤ 110 % of first 6 h", None)

env = None
for r in ran:
    files = sorted(glob.glob(os.path.join(out, "perf", f"*-{r}", "env.json")))
    if files:
        env = json.load(open(files[-1]))
        break

with open(os.path.join(out, "summary.md"), "w") as f:
    f.write(f"# SP-029 qualification summary\n\nProfile: {profile}. Host: {'reference' if reference else 'NON-REFERENCE (every verdict informational)'}.\n\n")
    if env:
        f.write(f"Commit {env.get('commit', env.get('sourceCommit', '?'))}; CPU {env.get('cpuModel', env.get('cpu', '?'))}; "
                f"load at start {env.get('loadAverageAtStart', '?')}.\n\n")
    f.write("| Threshold | Measured | Rule | Verdict | Note |\n|---|---|---|---|---|\n")
    for name, measured, rule, ok, note in rows:
        f.write(f"| {name} | {measured} | {rule} | {verdict(ok)} | {note} |\n")
for name, measured, rule, ok, note in rows:
    print(f"{verdict(ok):32} {name}: {measured}")
print(f"summary: {os.path.join(out, 'summary.md')}")
