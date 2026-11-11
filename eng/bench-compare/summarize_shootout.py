#!/usr/bin/env python3
"""Summarize shootout.py output: python3 summarize_shootout.py DIR  -> DIR/summary.md (median, min-max over rounds, heavy-load count per cell)."""
import glob, json, os, re, statistics as st, sys
d = sys.argv[1]; cells = {}
for f in glob.glob(f"{d}/r*-*.json"):
    if f.endswith(".load.json"): continue
    m = re.match(r"r(\d+)-(.+?)-((?:runwick|river|pgboss|fastend|bullmq|horizon).*)\.json$", os.path.basename(f))
    j = json.load(open(f)); ld = json.load(open(f[:-5] + ".load.json"))
    t = m[2]; mode = t.split("-")[0]
    v = j.get("insertsPerSec") if mode in ("enqueue", "bulk") else j.get("completionsPerSec")
    if v is None: continue
    extra = {"p50": (j.get("insertLatencyMs") or j.get("latencyMs") or {}).get("p50"), "p99": (j.get("insertLatencyMs") or j.get("latencyMs") or {}).get("p99"),
             "frac": (j.get("finished", j["jobs"] - j.get("failed", 0) if "failed" in j else j["jobs"]) / j["jobs"]) if mode == "drain" and j.get("jobs") else (j.get("completed", 0) / j["inserted"] if mode == "steady" and j.get("inserted") else None),
             "ins": j.get("inserted")}
    cells.setdefault((m[3], t), []).append((int(m[1]), v, ld["heavyAtStart"], float(ld["before"]["load"][0]), extra))
def f(x): return "–" if x is None else (f"{x:,.0f}" if x >= 100 else f"{x:.1f}")
def cell(c, lat=False):
    r = cells.get(c)
    if not r: return "n/a"
    vs = [x[1] for x in r]; s = f"{f(st.median(vs))} ({f(min(vs))}–{f(max(vs))})"
    p50 = [x[4]["p50"] for x in r if x[4]["p50"] is not None]; p99 = [x[4]["p99"] for x in r if x[4]["p99"] is not None]
    if lat and p50: s += f" · p50 {f(st.median(p50))} / p99 {f(st.median(p99))} ms"
    if c[1].startswith("steady"):
        sent = [x[4]["ins"] / (int(c[1].split("-")[1]) * 30) for x in r if x[4]["ins"]]
        if sent and st.median(sent) < 0.95: s += f" [sent only {st.median(sent)*100:.0f}% of offered]"
    fr = [x[4]["frac"] for x in r if x[4]["frac"] is not None]
    if fr and st.median(fr) < 0.99: s += f" [only {st.median(fr)*100:.0f}% done]"
    if any(x[2] for x in r): s += f" ⚑{sum(x[2] for x in r)}/{len(r)}"
    return s
cfgs = sorted({c for c, _ in cells}); order = ["runwick-pg","runwick-sqlite","runwick-redis","river","pgboss","fastendpoints","bullmq-everysec","bullmq-always","horizon-everysec","horizon-always"]
cfgs = [c for c in order if c in cfgs]
out = []
for title, tests, lat in [("Push (jobs added per second)", ["enqueue-c1","enqueue-c16","bulk"], False), ("Process (20,000-job backlog drained, jobs/s)", ["drain-w1","drain-w16","drain-w64"], False),
                          ("Steady arrivals: jobs finished per second, enqueue-to-finished latency", ["steady-100","steady-500"], True)]:
    out += [f"### {title}", "", "| System | " + " | ".join(tests) + " |", "|---|" + "---|" * len(tests)]
    out += [f"| {c} | " + " | ".join(cell((c, t), lat or t == "enqueue-c16") for t in tests) + " |" for c in cfgs] + [""]
out.append("Cells: median (min–max) over rounds. ⚑n/m = n of m runs started above load 8. [only x% done] = the run hit its time cap or could not keep up.")
open(f"{d}/summary.md", "w").write("\n".join(out)); print("\n".join(out))
