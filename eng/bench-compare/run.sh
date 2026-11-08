#!/usr/bin/env bash
# Comparison benchmark: River, pg-boss and Runwick against the same PostgreSQL (runwick-pg, 127.0.0.1:55499),
# each in its own freshly created database. No-op jobs, 1 KiB payload.
# Every test runs all three systems back to back, and the whole set repeats BENCH_ROUNDS times, with the order of
# systems rotated each round, so host noise spreads over all systems. Each run gets a load snapshot next to it.
# Usage: eng/bench-compare/run.sh [out-dir]
#   BENCH_ROUNDS (default 3), BENCH_N (drain jobs, default 20000), BENCH_SECONDS (steady, default 60),
#   BENCH_SYSTEMS (subset of "river pgboss runwick"), BENCH_TESTS (subset of "enqueue drain steady").
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/sdk/go/bin:$HOME/.dotnet:$PATH" GOPATH="${GOPATH:-$HOME/gopath}"
export DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
out="${1:-../../artifacts/bench-compare/$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$out"
rounds="${BENCH_ROUNDS:-3}"; n="${BENCH_N:-20000}"; secs="${BENCH_SECONDS:-60}"
systems=(${BENCH_SYSTEMS:-river pgboss runwick}); tests=" ${BENCH_TESTS:-enqueue drain steady} "
has() { [[ " ${systems[*]} " == *" $1 "* ]]; }
if has river; then (cd river && go build -o bench-river .); fi
if has pgboss; then (cd pgboss && { [[ -d node_modules ]] || npm ci; }); fi
workloads="$(cd ../.. && pwd)/tests/Runwick.Workloads"
if has runwick; then
  dotnet build "$workloads/Runwick.Workloads.csproj" -c Release > "$out/runwick-build.log" 2>&1 || { echo "Runwick.Workloads build failed (see $out/runwick-build.log)"; exit 1; }
fi
(cd ../.. && git rev-parse --short HEAD) > "$out/commit.txt"
runwick() { dotnet "$workloads/bin/Release/net10.0/Runwick.Workloads.dll" bench "$@"; }

# Host load around one run: cores, load average before and after, and the busiest processes at the start.
snap() { echo "{\"cores\": $(nproc), \"load\": \"$(cut -d' ' -f1-3 /proc/loadavg)\", \"top\": $(ps -eo pcpu=,comm= --sort=-pcpu | head -5 | python3 -c 'import sys,json; print(json.dumps([l.split(None,1)[0]+" "+l.split(None,1)[1].strip() for l in sys.stdin]))')}"; }
run() {
  local name="$1"; shift
  local before; before="$(snap)"
  echo "$(date -u +%H:%M:%S) $name (load $(cut -d' ' -f1 /proc/loadavg))"
  "$@" --out "$out/$name.json" > "$out/$name.log" 2>&1 || echo "  $name failed"
  echo "{\"before\": $before, \"after\": $(snap)}" > "$out/$name.load.json"
}

# One test = one command per system. $1 test id, $2 round; the rest are "system:command" strings.
cases() {
  local id="$1" r="$2"; shift 2
  local list=("$@") k=${#@}
  for ((i = 0; i < k; i++)); do
    local c="${list[$(((i + r) % k))]}"; local sys="${c%%:*}"
    has "$sys" && run "r$r-$id-$sys" ${c#*:}
  done
}

for ((r = 1; r <= rounds; r++)); do
  if [[ "$tests" == *" enqueue "* ]]; then
    cases enqueue-c1 $r "river:river/bench-river -mode enqueue -n 2000 -calls 1" \
      "pgboss:node pgboss/bench.mjs --mode enqueue --n 2000 --calls 1 --concurrency 1" \
      "runwick:runwick --mode enqueue --n 2000 --calls 1"
    cases enqueue-c16 $r "river:river/bench-river -mode enqueue -n 10000 -calls 16" \
      "pgboss:node pgboss/bench.mjs --mode enqueue --n 10000 --calls 16 --concurrency 16" \
      "runwick:runwick --mode enqueue --n 10000 --calls 16"
  fi
  if [[ "$tests" == *" drain "* ]]; then
    cases drain-w16 $r "river:river/bench-river -mode drain -n $n -workers 16" \
      "pgboss:node pgboss/bench.mjs --mode drain --n $n --concurrency 1 --batch 16 --burst" \
      "runwick:runwick --mode drain --n $n --workers 16 --calls 8"
    cases drain-w64 $r "river:river/bench-river -mode drain -n $n -workers 64 -cooldown-ms 1" \
      "runwick:runwick --mode drain --n $n --workers 64 --calls 8"
  fi
  if [[ "$tests" == *" steady "* ]]; then
    cases steady-20 $r "river:river/bench-river -mode steady -rate 20 -seconds $secs -workers 16" \
      "pgboss:node pgboss/bench.mjs --mode steady --rate 20 --seconds $secs --concurrency 16 --batch 1" \
      "runwick:runwick --mode steady --rate 20 --seconds $secs --workers 16"
    cases steady-100 $r "river:river/bench-river -mode steady -rate 100 -seconds $secs -workers 16" \
      "pgboss:node pgboss/bench.mjs --mode steady --rate 100 --seconds $secs --concurrency 16 --batch 1" \
      "runwick:runwick --mode steady --rate 100 --seconds $secs --workers 16"
  fi
done
python3 summarize.py "$out"
