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
ORDER = ["runwick-pg", "runwick-sqlite", "runwick-redis", "runwick-redis-tuned", "writefloor-pg", "writefloor-sqlite", "writefloor-redis", "wolverine-pg", "wolverine-sqlite", "wolverine-redis", "bullmq-dotnet-pg", "bullmq-dotnet-redis",
         "river", "pgboss", "solidqueue", "bullmq", "horizon", "celery"]
LABEL = {"runwick-redis-tuned": "Runwick Redis (tuned worker)", "writefloor-pg": "Floor: one INSERT (PG)", "writefloor-sqlite": "Floor: one INSERT (SQLite FULL)", "writefloor-redis": "Floor: one XADD (Redis AOF always)",
         "runwick-pg": "Runwick PostgreSQL", "runwick-sqlite": "Runwick SQLite", "runwick-redis": "Runwick Redis", "river": "River (Go, PG)",
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
    ("thru", "Jobs/s", lambda r: g(r, "result.insertsPerSec") or g(r, "result.completionsPerSec") or g(r, "result.writesPerSec"), "", True),
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
    # Stage 1: the timed phase only (seeding, startup and shutdown left out), and the store-confirmed drain rate on both clocks.
    ("cfc", "Confirmed/s, first claim", lambda r: g(r, "derived.confirmedPerSec_firstClaim"), "", True),
    ("cfs", "Confirmed/s, from start", lambda r: g(r, "derived.confirmedPerSec_workerStart"), "", True),
    ("tcpj", "Timed CPU ms/job", lambda r: g(r, "derived.timed.cpuMsPerJob"), "", False),
    ("twire", "Timed wire B/job", lambda r: g(r, "derived.timed.wireBytesPerJob"), "", False),
    ("twal", "Timed WAL B/job", lambda r: g(r, "derived.timed.walBytesPerJob"), "", False),
    ("tcmd", "Timed Redis cmds/job", lambda r: g(r, "derived.timed.redisCommandsPerJob"), "", False),
    ("mean", "Mean µs/write", lambda r: g(r, "result.meanMicrosPerWrite"), "", False),
]
cells = {}
every = {}  # all rounds of a cell, complete or not
for r in runs:
    if r.get("trace") or g(r, "result.skipped"): continue
    # Artifacts from before the result contract have no "complete" flag: an ok run counts as complete there.
    r.setdefault("complete", bool(r.get("ok")))
    every.setdefault((r["workload"], r["test"], r["system"]), []).append(r)
    if r.get("ok") and r["complete"]: cells.setdefault((r["workload"], r["test"], r["system"]), []).append(r)
agg = {k: {m[0]: med([m[2](r) for r in v]) for m in METRICS} | {"n": len(v)} for k, v in cells.items()}
for k, v in cells.items():
    xs = [x for x in (METRICS[0][2](r) for r in v) if x is not None]
    agg[k]["lo"], agg[k]["hi"] = (min(xs), max(xs)) if xs else (None, None)
    agg[k]["rounds"] = len(every[k]); agg[k]["incomplete"] = len(every[k]) - len(v)
version = {r["system"]: r.get("version") or g(r, "result.system") for r in runs if r.get("ok")}
# rows.json: one flat row per library x test with this run's medians, for the results dashboard (append each run's file).
run_id = os.environ.get("GITHUB_RUN_ID", "local")
COLNAME = {"cfc": "confirmedPerSecFirstClaim", "cfs": "confirmedPerSecFromStart", "tcpj": "timedCpuMsPerJob", "twire": "timedWireBytesPerJob", "twal": "timedWalBytesPerJob",
           "tcmd": "timedRedisCmdsPerJob", "mean": "meanMicrosPerWrite", "thru": "jobsPerSec", "p50": "p50Ms", "p99": "p99Ms", "done": "donePct", "acpu": "appCpuPct", "amem": "appMemMB", "scpu": "storeCpuPct",
           "smem": "storeMemMB", "cpj": "cpuMsPerJob", "bpj": "wireBytesPerJob", "wpj": "diskWriteBytesPerJob"}
rows = []
for (wl, t, s), m in sorted(agg.items()):
    h = hosts.get(f"{s}--{wl}", {})
    rows.append({"run": run_id, "date": min((x.get("when", "") for x in hosts.values()), default="")[:10], "commit": h.get("commit"),
                 "workload": wl, "test": t, "system": s, "library": LABEL[s], "version": version.get(s), "rounds": m["n"], "hostCpu": h.get("cpu"),
                 **{COLNAME[k]: (round(v, 3) if isinstance(v, float) else v) for k, v in m.items() if k in COLNAME}, "completeRounds": m["n"], "incompleteRounds": m["incomplete"],
                 "minJobsPerSec": m["lo"], "maxJobsPerSec": m["hi"]})
json.dump(rows, open(f"{dst}/rows.json", "w"), indent=0)
failed = sorted({(r["system"], r["workload"], r["test"]) for r in runs if not r.get("ok") and not r.get("trace")})
incomplete = [r for r in runs if r.get("ok") and not r["complete"] and not r.get("trace")]
skipped = sorted({(r["system"], r["test"]) for r in runs if g(r, "result.skipped")})

TESTS = {"normal": ["floor-c1", "floor-c16", "enqueue-c1", "enqueue-c16", "enqueue-c64", "bulk", "bulk-100", "drain-w16", "drain-w64", "steady-100", "steady-500"], "cpu": ["drain-w16", "steady-4"]}
DESC = {"floor-c1": "The cheapest durable 1 KiB write the store accepts, one caller, no queue code (mean µs per write is the budget)", "floor-c16": "The same write from 16 concurrent callers",
        "enqueue-c64": "Push one job at a time, 64 concurrent callers", "bulk-100": "Push 20,000 jobs, 100 per call for every library that takes a chunk size",
        "enqueue-c1": "Push one job at a time, 1 caller (jobs added/s, add latency)", "enqueue-c16": "Push one job at a time, 16 concurrent callers",
        "bulk": "Push 20,000 jobs with the library's bulk API", "drain-w16": "Process a ready backlog with 16 worker slots (jobs finished/s)",
        "drain-w64": "Process a ready backlog with 64 worker slots", "steady-100": "100 jobs/s arriving for 30 s: finished/s and enqueue-to-finished latency",
        "steady-500": "500 jobs/s arriving for 30 s", "steady-4": "4 CPU-heavy jobs/s arriving for 60 s: finished/s and enqueue-to-finished latency"}
COLS = {"floor": ["thru", "mean", "p50", "p99", "tcpj", "twal", "tcmd"], "enqueue": ["thru", "p50", "p99", "tcpj", "twire", "twal", "tcmd", "acpu", "scpu", "cpj", "bpj", "wpj"],
        "bulk": ["thru", "tcpj", "twire", "twal", "tcmd", "acpu", "scpu", "cpj", "bpj", "wpj"],
        "drain": ["thru", "cfc", "cfs", "done", "tcpj", "twire", "twal", "tcmd", "acpu", "scpu", "smem", "cpj", "bpj", "wpj"], "steady": ["thru", "p50", "p99", "done", "acpu", "amem", "scpu", "smem", "cpj", "bpj"]}
MLAB = {m[0]: m[1] for m in METRICS}


def fmt(x):
    if x is None: return "–"
    return f"{x:,.0f}" if abs(x) >= 100 else f"{x:.1f}" if abs(x) >= 1 else f"{x:.2f}"


def rounds_cell(r):  # "3" when every round was complete, "3 of 5" when some were not (those are listed under Incomplete rounds)
    return str(r["n"]) if not r["incomplete"] else f"{r['n']} of {r['rounds']}"


GROUPS = {"PostgreSQL": ["runwick-pg", "river", "pgboss", "wolverine-pg", "bullmq-dotnet-pg"], "Redis": ["runwick-redis", "wolverine-redis", "bullmq-dotnet-redis", "bullmq"],
          "SQLite": ["runwick-sqlite", "wolverine-sqlite", "solidqueue"]}
FLOOR = {"PostgreSQL": "writefloor-pg", "Redis": "writefloor-redis", "SQLite": "writefloor-sqlite"}
TARGET_COLS = [("Single add, 1 caller", "enqueue-c1"), ("Single add, 16 callers", "enqueue-c16"), ("Single add, 64 callers", "enqueue-c64"), ("Bulk add (native chunk)", "bulk"),
               ("Bulk add (100 per call)", "bulk-100"), ("Drain, 16 slots", "drain-w16"), ("Drain, 64 slots", "drain-w64")]


def targets():
    """baseline -> target per store and test, from this run: target = 90% of the fastest other same-store library in the same column (complete rounds only)."""
    out = {}
    for store, systems in GROUPS.items():
        me = systems[0]
        for label, t in TARGET_COLS:
            base = agg.get(("normal", t, me), {}).get("thru")
            rivals = [(agg[("normal", t, s)]["thru"], s) for s in systems[1:] if ("normal", t, s) in agg and agg[("normal", t, s)]["thru"]]
            if base is None and not rivals: continue
            lead = max(rivals, default=None)
            out[(store, label)] = (base, lead[0] * 0.9 if lead else None, lead[1] if lead else None, t)
    return out


def budgets(tg):
    """Time one synchronous caller has per accepted job at the target rate, against what the bare store write costs on this disk (floor-c1)."""
    rows = []
    for store, fs in FLOOR.items():
        tgt = tg.get((store, "Single add, 1 caller"))
        fl = agg.get(("normal", "floor-c1", fs), {}).get("mean")
        if tgt and tgt[1] and fl: rows.append((store, 1e6 / tgt[1], fl))
    return rows


def sections():
    for wl in ("normal", "cpu"):
        for t in TESTS[wl]:
            rows = [(s, agg[(wl, t, s)]) for s in ORDER if (wl, t, s) in agg]
            if rows: yield wl, t, rows


md = ["# Queue library benchmark", "",
      "Every library ran on a fresh GitHub-hosted runner (ubuntu-24.04), either one runner per library or, in same-host mode, one runner per store with the libraries one after another and the store container recreated for each, against its own PostgreSQL 18 or Redis 8.2 (AOF, appendfsync always) container, or a local SQLite file. "
      "**normal** = a 1 KiB no-op job; **cpu** = the same job doing 50,000 SHA-256 rounds over 1 KiB (≈0.1–0.5 s of one core, depending on the language). "
      "Cells are medians over rounds. *App* = every process the benchmark started (producers + workers + driver); *Store* = the database container. "
      "*CPU ms/job* = app + store CPU time per finished job. *Wire B/job* = bytes in and out of the store container per job. Runners can differ in CPU model: see Hosts.", ""]
for wl, t, rows in sections():
    cols = COLS[t.split("-")[0]]
    best = max(r[1]["thru"] or 0 for r in rows)
    md += [f"## {wl} · {t}", "", DESC[t], "", "```mermaid", "xychart-beta",
           f'  title "{wl} {t}: jobs/s"', "  x-axis [" + ", ".join(f'"{s}"' for s, _ in rows) + "]",
           f'  y-axis "jobs/s" 0 --> {max(1, round(best * 1.1))}', "  bar [" + ", ".join(str(round(r["thru"] or 0, 1)) for _, r in rows) + "]", "```", "",
           "| Library | Rounds | Jobs/s range | " + " | ".join(MLAB[c] for c in cols) + " |", "|---|---:|---:|" + "---:|" * len(cols)]
    md += [f"| {LABEL[s]} | {rounds_cell(r)} | {fmt(r['lo'])}–{fmt(r['hi'])} | " + " | ".join(fmt(r[c]) for c in cols) + " |" for s, r in rows] + [""]
tg = targets()
if tg:
    md += ["## Baseline → target", "", "Jobs/s, Runwick's median over complete rounds → 90% of the fastest other library in the same store and column on this host (leader in brackets). "
           "Sizes the engineering problem; the release decision uses paired runs.", "", "| Store | " + " | ".join(l for l, _ in TARGET_COLS) + " |", "|---|" + "---:|" * len(TARGET_COLS)]
    for store in GROUPS:
        cells_ = []
        for l, _ in TARGET_COLS:
            b, t_, who, _t = tg.get((store, l), (None, None, None, None))
            cells_.append("–" if b is None and t_ is None else f"{fmt(b)} → {fmt(t_)} ({LABEL[who].split(' (')[0]})" if t_ else f"{fmt(b)} → –")
        md.append(f"| {store} | " + " | ".join(cells_) + " |")
    md.append("")
bg = budgets(tg)
if bg:
    md += ["## Cost budgets", "", "Microseconds one synchronous caller has per accepted job at the single-caller target, against the mean cost of the bare store write (floor-c1) on this disk. "
           "If the floor already uses the budget, the target is out of reach on this hardware.", "", "| Store | Budget µs/job | Bare write µs | Left for Runwick µs |", "|---|---:|---:|---:|"]
    md += [f"| {st_} | {fmt(b_)} | {fmt(f_)} | {fmt(b_ - f_)} |" for st_, b_, f_ in bg] + [""]
if incomplete:
    md += ["## Incomplete rounds", "", "Reported, never counted in a median: the store did not agree with the harness that every accepted job finished.", "", "| Library | Test | Round | What the store holds | How far it got |", "|---|---|---:|---|---|"]
    for r in sorted(incomplete, key=lambda r: (r["system"], r["test"], r["round"])):
        prog = g(r, "result.progress"); far = f"{prog[-1][1]:,.0f} at {prog[-1][0]} s" if prog else "–"
        md.append(f"| {LABEL.get(r['system'], r['system'])} | {r['workload']} {r['test']} | {r['round']} | {r.get('incompleteReason') or '–'} | {far} |")
    md.append("")
if skipped: md += ["**Not applicable:** " + ", ".join(f"{s} {t}" for s, t in skipped) + " (no bulk-enqueue API).", ""]
if failed: md += ["**Failed runs** (see that job's artifact logs): " + ", ".join(f"{s}/{w}/{t}" for s, w, t in failed), ""]
manifests = [json.load(open(f)) for f in sorted(glob.glob(f"{src}/**/manifest-*.json", recursive=True))]
if manifests:
    md += ["## Run manifests", "", "| Systems | Runwick | src identical to v0.1.5 | Store | Image digest | Durability settings |", "|---|---|---|---|---|---|"]
    for m in manifests:
        for k, st_ in (m.get("stores") or {"-": None}).items():
            rw = m["runwick"]
            md.append(f"| {', '.join(m['systems'])} | {rw['describe']}{' (dirty src)' if rw['srcDirty'] else ''} | {'yes' if rw['srcIdenticalToV0_1_5'] else '**NO**'} | {k} | {(st_ or {}).get('repoDigest') or '–'} | "
                      + ", ".join(f"{a_}={b_}" for a_, b_ in ((st_ or {}).get('settings') or {}).items()) + " |")
    md.append("")
md += ["## Versions and hosts", "", "| Run | Library version | CPU | Cores | RAM MB | Commit |", "|---|---|---|---:|---:|---|"]
md += [f"| {k} | {version.get(k.rsplit('--', 1)[0], '–')} | {h['cpu']} | {h['cores']} | {h['memMB']:,} | {h['commit']} |" for k, h in sorted(hosts.items())]
open(f"{dst}/report.md", "w").write("\n".join(md) + "\n")

# ---- HTML: same tables, SVG bars for every metric that has data -------------------------------------------------------
COLORS = {s: c for s, c in zip(ORDER, ["#2563eb", "#3b82f6", "#60a5fa", "#1d4ed8", "#6b7280", "#6b7280", "#6b7280", "#9333ea", "#a855f7", "#c084fc", "#db2777", "#f472b6", "#059669", "#10b981", "#dc2626", "#d97706", "#7c3aed", "#0891b2"])}
COLORS.setdefault("x", "#888")


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
extra_html = ""
if tg:
    extra_html += "<section><h2>Baseline → target</h2><table><tr><th>Store</th>" + "".join(f"<th>{html.escape(l)}</th>" for l, _ in TARGET_COLS) + "</tr>" + "".join(
        f"<tr><td>{store}</td>" + "".join("<td>" + html.escape("–" if not tg.get((store, l)) or (tg[(store, l)][0] is None and tg[(store, l)][1] is None) else f"{fmt(tg[(store, l)][0])} → {fmt(tg[(store, l)][1])}" + (f" ({LABEL[tg[(store, l)][2]].split(' (')[0]})" if tg[(store, l)][2] else "")) + "</td>" for l, _ in TARGET_COLS) + "</tr>"
        for store in GROUPS) + "</table></section>"
if bg:
    extra_html += "<section><h2>Cost budgets</h2><table><tr><th>Store</th><th>Budget µs/job</th><th>Bare write µs</th><th>Left µs</th></tr>" + "".join(
        f"<tr><td>{st_}</td><td>{fmt(b_)}</td><td>{fmt(f_)}</td><td>{fmt(b_ - f_)}</td></tr>" for st_, b_, f_ in bg) + "</table></section>"
if incomplete:
    extra_html += "<section><h2>Incomplete rounds</h2><table><tr><th>Library</th><th>Test</th><th>Round</th><th>Store holds</th></tr>" + "".join(
        f"<tr><td>{html.escape(LABEL.get(r['system'], r['system']))}</td><td>{r['test']}</td><td>{r['round']}</td><td>{html.escape(r.get('incompleteReason') or '–')}</td></tr>" for r in incomplete) + "</table></section>"
open(f"{dst}/report.html", "w").write(f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Queue library benchmark</title><style>
body{{font:14px/1.5 system-ui,sans-serif;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#111}}
table{{border-collapse:collapse;margin:.5rem 0 1rem;font-variant-numeric:tabular-nums}}td,th{{border-bottom:1px solid #ddd;padding:4px 10px;text-align:right}}
td:first-child,th:first-child{{text-align:left}}.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:1rem}}
figure{{margin:0}}figcaption{{font-weight:600}}section{{border-top:2px solid #eee;padding-top:.5rem;margin-top:1.5rem}}svg text{{fill:currentColor}}
@media (prefers-color-scheme:dark){{body{{background:#111;color:#eee}}td,th{{border-color:#333}}section{{border-color:#333}}}}
</style><h1>Queue library benchmark</h1><p>{intro}</p>{notes}{''.join(parts)}{extra_html}<section><h2>Hosts</h2>{hosts_tbl}</section></html>""")
print("\n".join(md))
