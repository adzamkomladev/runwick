#!/usr/bin/env bash
# Comparison benchmark (perf plan PF-01 / PQ4): River and pg-boss against the same PostgreSQL as Runwick
# (runwick-pg, 127.0.0.1:55499, through docker-proxy like the Runwick workloads). No-op jobs, 1 KiB payload.
# Usage: eng/bench-compare/run.sh [out-dir]; BENCH_N (drain jobs, default 20000), BENCH_SECONDS (steady, default 120).
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/sdk/go/bin:$PATH" GOPATH="${GOPATH:-$HOME/gopath}"
out="${1:-../../artifacts/bench-compare/$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$out"
n="${BENCH_N:-20000}"; secs="${BENCH_SECONDS:-120}"
(cd river && go build -o bench-river .)
(cd pgboss && [[ -d node_modules ]] || npm ci)
uptime > "$out/load-start.txt"
run() { local name="$1"; shift; echo "$(date -u +%H:%M:%S) $name"; "$@" --out "$out/$name.json" > "$out/$name.log" 2>&1 || echo "  $name failed"; uptime >> "$out/load.txt"; }
run river-drain-default   river/bench-river -mode drain -n "$n" -workers 16
run river-drain-tuned     river/bench-river -mode drain -n "$n" -workers 16 -cooldown-ms 1
run river-steady-100      river/bench-river -mode steady -rate 100 -seconds "$secs" -workers 16
run pgboss-drain-b1       node pgboss/bench.mjs --mode drain --n "$n" --concurrency 16 --batch 1
run pgboss-drain-b16      node pgboss/bench.mjs --mode drain --n "$n" --concurrency 1 --batch 16 --burst
run pgboss-steady-100     node pgboss/bench.mjs --mode steady --rate 100 --seconds "$secs" --concurrency 16 --batch 1
python3 - "$out" <<'PY'
import json,glob,os,sys
rows=[]
for f in sorted(glob.glob(os.path.join(sys.argv[1],'*.json'))):
    j=json.load(open(f)); l=j.get('latencyMs',{})
    rows.append(f"| {os.path.basename(f)[:-5]} | {j['system']} | {j['mode']} | {j.get('completionsPerSec',0):.1f} | {l.get('p50','')} | {l.get('p99','')} |")
open(os.path.join(sys.argv[1],'summary.md'),'w').write("| Run | System | Mode | Completions/s | p50 ms | p99 ms |\n|---|---|---|---|---|---|\n"+"\n".join(rows)+"\n")
print(open(os.path.join(sys.argv[1],'summary.md')).read())
PY
