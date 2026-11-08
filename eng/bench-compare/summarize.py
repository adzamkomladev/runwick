"""Summarize an eng/bench-compare/run.sh output folder: median and spread per test and system, and host load.
A run counts as "heavy" when the 1-minute load average at its start was above the core count."""
import glob, json, os, re, statistics, sys

out = sys.argv[1]
groups = {}
for f in sorted(glob.glob(os.path.join(out, "r*-*.json"))):
    if f.endswith(".load.json"):
        continue
    m = re.match(r"r(\d+)-(.+)-(\w+)\.json$", os.path.basename(f))
    j = json.load(open(f))
    ld = json.load(open(f[:-5] + ".load.json")) if os.path.exists(f[:-5] + ".load.json") else None
    load1 = float(ld["before"]["load"].split()[0]) if ld else None
    cores = ld["before"]["cores"] if ld else 8
    rate = j.get("insertsPerSec", j.get("completionsPerSec", 0))
    lat = j.get("latencyMs") or j.get("insertLatencyMs") or {}
    extra = f"{j.get('inserted', '')}/{j.get('completed', '')}" if "steady" in m[2] else ""
    label = {"a": "Runwick main", "b": "Runwick prototype"}.get(m[3])  # ab.sh variants
    groups.setdefault((m[2], m[3]), []).append((int(m[1]), rate, lat.get("p50"), lat.get("p99"), load1, load1 is not None and load1 > cores, extra, label or j.get("system", "")))

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None

def fmt(x):
    return "" if x is None else (f"{x:.0f}" if x >= 100 else f"{x:.1f}")

lines = ["| Test | System | Rate/s median | min to max | p50 ms | p99 ms | Load at start | Heavy runs | Per round (rate, inserted/completed) |",
         "|---|---|---|---|---|---|---|---|---|"]
for (test, system), rows in sorted(groups.items()):
    rates = [r[1] for r in rows]
    loads = [r[4] for r in rows if r[4] is not None]
    per = "; ".join(f"r{r[0]} {fmt(r[1])}{' ' + r[6] if r[6] else ''}{' *' if r[5] else ''}" for r in sorted(rows))
    lines.append(f"| {test} | {rows[0][7] or system} | {fmt(med(rates))} | {fmt(min(rates))} to {fmt(max(rates))} | {fmt(med([r[2] for r in rows]))} | {fmt(med([r[3] for r in rows]))} | "
                 f"{fmt(min(loads)) if loads else ''} to {fmt(max(loads)) if loads else ''} | {sum(r[5] for r in rows)} of {len(rows)} | {per} |")
text = "\n".join(lines) + "\n\nRate is inserts/s for enqueue tests and completions/s otherwise. Latency is per insert call for enqueue and insert-to-finish for steady. * = heavy load at start (1-minute load average above core count).\n"
open(os.path.join(out, "summary.md"), "w").write(text)
print(text)
