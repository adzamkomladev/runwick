#!/usr/bin/env python3
"""Collect ci/run.py output into one report.  report.py RESULTS_DIR OUT_DIR
  OUT_DIR/report.md   tables + Mermaid bar charts (GitHub renders both; the workflow appends it to the run summary)
  OUT_DIR/report.html the same, standalone, with SVG bar charts
  OUT_DIR/results.json every run, flattened
  OUT_DIR/rows.json    medians, one row per library x test, tagged with the run
Each cell is the median over rounds."""
import glob, html, json, os, statistics as st, sys

src, dst = sys.argv[1], sys.argv[2]
os.makedirs(dst, exist_ok=True)
ORDER = ["runwick-pg", "runwick-sqlite", "runwick-redis", "wolverine-pg", "wolverine-sqlite", "wolverine-redis", "bullmq-dotnet-pg", "bullmq-dotnet-redis",
         "river", "pgboss", "solidqueue", "bullmq", "horizon", "celery"]
LABEL = {"runwick-pg": "Runwick PostgreSQL", "runwick-sqlite": "Runwick SQLite", "runwick-redis": "Runwick Redis", "river": "River (Go, PG)",
         "pgboss": "pg-boss (Node, PG)", "solidqueue": "Solid Queue (Rails, SQLite)",
         "wolverine-pg": "Wolverine (.NET, PG)", "wolverine-sqlite": "Wolverine (.NET, SQLite)", "wolverine-redis": "Wolverine (.NET, Redis)",
         "bullmq-dotnet-pg": "BullMQ .NET (PG)", "bullmq-dotnet-redis": "BullMQ .NET (Redis)", "bullmq": "BullMQ (Node, Redis)",
         "horizon": "Laravel Horizon (PHP, Redis)", "celery": "Celery (Python, Redis)"}
runs = [json.load(open(f)) for f in glob.glob(f"{src}/**/*.json", recursive=True)
        if "--r" in os.path.basename(f) and not f.endswith(".raw.json")]
hosts = {os.path.basename(f).rsplit("--host", 1)[0]: json.load(open(f)) for f in glob.glob(f"{src}/**/*--host.json", recursive=True)}
json.dump(runs, open(f"{dst}/results.json", "w"))


def g(d, path):
    for k in path.split("."):
        d = (d or {}).get(k)
    return d


def med(xs):
    xs = [x for x in xs if isinstance(x, (int, float))]
    return st.median(xs) if xs else None


def done_pct(j):  # share of the offered jobs that finished: drains report finished (or failed, River), steady runs completed/inserted
    if j.get("inserted"): return 100 * j.get("completed", 0) / j["inserted"]
    if j.get("mode") != "drain" or not j.get("jobs"): return None
    return 100 * (j["finished"] if "finished" in j else j["jobs"] - j.get("failed", 0)) / j["jobs"]


# (key, label, getter, unit, higher is better)
METRICS = [
    ("thru", "Jobs/s", lambda r: g(r, "result.insertsPerSec") or g(r, "result.completionsPerSec"), "", True),
    ("p50", "p50 ms", lambda r: g(r, "result.latencyMs.p50") or g(r, "result.insertLatencyMs.p50"), "", False),
    ("p99", "p99 ms", lambda r: g(r, "result.latencyMs.p99") or g(r, "result.insertLatencyMs.p99"), "", False),
    ("done", "Done %", lambda r: done_pct(r["result"]), "", True),
    ("acpu", "App CPU %", lambda r: g(r, "resources.app.cpuAvgPct"), "", False),
    ("amem", "App mem MB", lambda r: g(r, "resources.app.memPeakMB"), "", False),
    ("scpu", "Store CPU %", lambda r: g(r, "resources.store.cpuAvgPct"), "", False),
    ("smem", "Store mem MB", lambda r: g(r, "resources.store.memPeakMB"), "", False),
    ("cpj", "CPU ms/job", lambda r: g(r, "derived.cpuMsPerJob"), "", False),
    ("bpj", "Wire B/job", lambda r: g(r, "derived.wireBytesPerJob"), "", False),
    ("wpj", "Disk write B/job", lambda r: g(r, "derived.storeWriteBytesPerJob"), "", False),
]
cells = {}
for r in runs:
    if not r.get("ok") or g(r, "result.skipped"): continue
    cells.setdefault((r["workload"], r["test"], r["system"]), []).append(r)
agg = {k: {m[0]: med([m[2](r) for r in v]) for m in METRICS} | {"n": len(v)} for k, v in cells.items()}
version = {r["system"]: r.get("version") or g(r, "result.system") for r in runs if r.get("ok")}
# rows.json: one flat row per library x test with this run's medians, for the results dashboard (append each run's file).
run_id = os.environ.get("GITHUB_RUN_ID", "local")
COLNAME = {"thru": "jobsPerSec", "p50": "p50Ms", "p99": "p99Ms", "done": "donePct", "acpu": "appCpuPct", "amem": "appMemMB", "scpu": "storeCpuPct",
           "smem": "storeMemMB", "cpj": "cpuMsPerJob", "bpj": "wireBytesPerJob", "wpj": "diskWriteBytesPerJob"}
rows = []
for (wl, t, s), m in sorted(agg.items()):
    h = hosts.get(f"{s}--{wl}", {})
    rows.append({"run": run_id, "date": min((x.get("when", "") for x in hosts.values()), default="")[:10], "commit": h.get("commit"),
                 "workload": wl, "test": t, "system": s, "library": LABEL[s], "version": version.get(s), "rounds": m["n"], "hostCpu": h.get("cpu"),
                 **{COLNAME[k]: (round(v, 3) if isinstance(v, float) else v) for k, v in m.items() if k != "n"}})
json.dump(rows, open(f"{dst}/rows.json", "w"), indent=0)
failed = sorted({(r["system"], r["workload"], r["test"]) for r in runs if not r.get("ok")})
skipped = sorted({(r["system"], r["test"]) for r in runs if g(r, "result.skipped")})

TESTS = {"normal": ["enqueue-c1", "enqueue-c16", "bulk", "drain-w16", "drain-w64", "steady-100", "steady-500"], "cpu": ["drain-w16", "steady-4"]}
DESC = {"enqueue-c1": "Push one job at a time, 1 caller (jobs added/s, add latency)", "enqueue-c16": "Push one job at a time, 16 concurrent callers",
        "bulk": "Push 20,000 jobs with the library's bulk API", "drain-w16": "Process a ready backlog with 16 worker slots (jobs finished/s)",
        "drain-w64": "Process a ready backlog with 64 worker slots", "steady-100": "100 jobs/s arriving for 30 s: finished/s and enqueue-to-finished latency",
        "steady-500": "500 jobs/s arriving for 30 s", "steady-4": "4 CPU-heavy jobs/s arriving for 60 s: finished/s and enqueue-to-finished latency"}
COLS = {"enqueue": ["thru", "p50", "p99", "acpu", "amem", "scpu", "cpj", "bpj", "wpj"], "bulk": ["thru", "acpu", "amem", "scpu", "cpj", "bpj", "wpj"],
        "drain": ["thru", "done", "acpu", "amem", "scpu", "smem", "cpj", "bpj", "wpj"], "steady": ["thru", "p50", "p99", "done", "acpu", "amem", "scpu", "smem", "cpj", "bpj"]}
MLAB = {m[0]: m[1] for m in METRICS}


def fmt(x):
    if x is None: return "–"
    return f"{x:,.0f}" if abs(x) >= 100 else f"{x:.1f}" if abs(x) >= 1 else f"{x:.2f}"


def sections():
    for wl in ("normal", "cpu"):
        for t in TESTS[wl]:
            rows = [(s, agg[(wl, t, s)]) for s in ORDER if (wl, t, s) in agg]
            if rows: yield wl, t, rows


md = ["# Queue library benchmark", "",
      "Every library ran on its own fresh GitHub-hosted runner (ubuntu-24.04), against its own PostgreSQL 18 or Redis 8.2 (AOF, appendfsync always) container, or a local SQLite file. "
      "**normal** = a 1 KiB no-op job; **cpu** = the same job doing 50,000 SHA-256 rounds over 1 KiB (≈0.1–0.5 s of one core, depending on the language). "
      "Cells are medians over rounds. *App* = every process the benchmark started (producers + workers + driver); *Store* = the database container. "
      "*CPU ms/job* = app + store CPU time per finished job. *Wire B/job* = bytes in and out of the store container per job. Runners can differ in CPU model: see Hosts.", ""]
for wl, t, rows in sections():
    cols = COLS[t.split("-")[0]]
    best = max(r[1]["thru"] or 0 for r in rows)
    md += [f"## {wl} · {t}", "", DESC[t], "", "```mermaid", "xychart-beta",
           f'  title "{wl} {t}: jobs/s"', "  x-axis [" + ", ".join(f'"{s}"' for s, _ in rows) + "]",
           f'  y-axis "jobs/s" 0 --> {max(1, round(best * 1.1))}', "  bar [" + ", ".join(str(round(r["thru"] or 0, 1)) for _, r in rows) + "]", "```", "",
           "| Library | " + " | ".join(MLAB[c] for c in cols) + " |", "|---|" + "---:|" * len(cols)]
    md += [f"| {LABEL[s]} | " + " | ".join(fmt(r[c]) for c in cols) + " |" for s, r in rows] + [""]
if skipped: md += ["**Not applicable:** " + ", ".join(f"{s} {t}" for s, t in skipped) + " (no bulk-enqueue API).", ""]
if failed: md += ["**Failed runs** (see that job's artifact logs): " + ", ".join(f"{s}/{w}/{t}" for s, w, t in failed), ""]
md += ["## Versions and hosts", "", "| Run | Library version | CPU | Cores | RAM MB | Commit |", "|---|---|---|---:|---:|---|"]
md += [f"| {k} | {version.get(k.rsplit('--', 1)[0], '–')} | {h['cpu']} | {h['cores']} | {h['memMB']:,} | {h['commit']} |" for k, h in sorted(hosts.items())]
open(f"{dst}/report.md", "w").write("\n".join(md) + "\n")

# ---- HTML: same tables, SVG bars for every metric that has data -------------------------------------------------------
COLORS = {s: c for s, c in zip(ORDER, ["#2563eb", "#3b82f6", "#60a5fa", "#9333ea", "#a855f7", "#c084fc", "#db2777", "#f472b6", "#059669", "#10b981", "#dc2626", "#d97706", "#7c3aed", "#0891b2"])}


def bars(rows, key, title, higher):
    vals = [(s, r[key]) for s, r in rows if r[key] is not None]
    if not vals: return ""
    top = max(v for _, v in vals) or 1; h = 22 * len(vals) + 30
    out = [f'<figure><figcaption>{html.escape(title)} <small>({"higher" if higher else "lower"} is better)</small></figcaption>',
           f'<svg viewBox="0 0 640 {h}" width="100%" role="img" aria-label="{html.escape(title)}">']
    for i, (s, v) in enumerate(sorted(vals, key=lambda x: -x[1] if higher else x[1])):
        y = 10 + i * 22; w = max(1, 380 * v / top)
        out += [f'<text x="200" y="{y + 14}" text-anchor="end" font-size="12">{html.escape(LABEL[s])}</text>',
                f'<rect x="206" y="{y}" width="{w:.1f}" height="17" rx="3" fill="{COLORS[s]}"/>',
                f'<text x="{210 + w:.1f}" y="{y + 13}" font-size="11">{fmt(v)}</text>']
    return "\n".join(out + ["</svg></figure>"])


parts = []
for wl, t, rows in sections():
    cols = COLS[t.split("-")[0]]
    tbl = "<table><tr><th>Library</th>" + "".join(f"<th>{MLAB[c]}</th>" for c in cols) + "</tr>" + "".join(
        f"<tr><td>{LABEL[s]}</td>" + "".join(f"<td>{fmt(r[c])}</td>" for c in cols) + "</tr>" for s, r in rows) + "</table>"
    figs = "".join(bars(rows, c, MLAB[c], next(m[4] for m in METRICS if m[0] == c)) for c in cols)
    parts.append(f'<section><h2>{wl} · {t}</h2><p>{html.escape(DESC[t])}</p>{tbl}<div class="grid">{figs}</div></section>')
intro = html.escape(md[2]).replace("**", "")
hosts_tbl = "<table><tr><th>Run</th><th>CPU</th><th>Cores</th><th>RAM MB</th><th>Commit</th></tr>" + "".join(
    f"<tr><td>{html.escape(k)}</td><td>{html.escape(h['cpu'])}</td><td>{h['cores']}</td><td>{h['memMB']:,}</td><td>{h['commit']}</td></tr>" for k, h in sorted(hosts.items())) + "</table>"
notes = ("<p><b>Failed:</b> " + html.escape(", ".join(f"{s}/{w}/{t}" for s, w, t in failed)) + "</p>") if failed else ""
open(f"{dst}/report.html", "w").write(f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Queue library benchmark</title><style>
body{{font:14px/1.5 system-ui,sans-serif;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#111}}
table{{border-collapse:collapse;margin:.5rem 0 1rem;font-variant-numeric:tabular-nums}}td,th{{border-bottom:1px solid #ddd;padding:4px 10px;text-align:right}}
td:first-child,th:first-child{{text-align:left}}.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:1rem}}
figure{{margin:0}}figcaption{{font-weight:600}}section{{border-top:2px solid #eee;padding-top:.5rem;margin-top:1.5rem}}svg text{{fill:currentColor}}
@media (prefers-color-scheme:dark){{body{{background:#111;color:#eee}}td,th{{border-color:#333}}section{{border-color:#333}}}}
</style><h1>Queue library benchmark</h1><p>{intro}</p>{notes}{''.join(parts)}<section><h2>Hosts</h2>{hosts_tbl}</section></html>""")
print("\n".join(md))
