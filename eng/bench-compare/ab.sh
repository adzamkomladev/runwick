#!/usr/bin/env bash
# A/B of two Runwick builds on the comparison workloads, alternating builds within each test and round
# (order flipped every round), with a load snapshot per run. Summarize with summarize.py.
# Usage: eng/bench-compare/ab.sh <a-Runwick.Workloads.dll> <b-Runwick.Workloads.dll> <out-dir>
#   BENCH_ROUNDS (default 3), BENCH_N (drain jobs, default 20000), BENCH_SECONDS (steady, default 60),
#   BENCH_TESTS (subset of "enqueue-c1 enqueue-c16 drain-w16 steady-100", default all). a is labelled "main", b "prototype".
#   BENCH_STORE (postgres or sqlite, default postgres), BENCH_TRACE=1 to capture store phase timings.
#   BENCH_STEADY_CALLS (default 1, set above 1 for open-loop arrivals).
set -euo pipefail
a="$1"; b="$2"; out="$3"; mkdir -p "$out"
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
rounds="${BENCH_ROUNDS:-3}"; n="${BENCH_N:-20000}"; secs="${BENCH_SECONDS:-60}"
steady_calls="${BENCH_STEADY_CALLS:-1}"
bench_args=(--store "${BENCH_STORE:-postgres}")
if [[ "${BENCH_TRACE:-0}" == 1 ]]; then bench_args+=(--trace); fi
snap() { echo "{\"cores\": $(nproc), \"load\": \"$(cut -d' ' -f1-3 /proc/loadavg)\", \"top\": $(ps -eo pcpu=,comm= --sort=-pcpu | head -5 | python3 -c 'import sys,json; print(json.dumps([" ".join(l.split()) for l in sys.stdin]))')}"; }
one() { # round test variant dll args...
  local r="$1" t="$2" v="$3" dll="$4"; shift 4
  local before; before="$(snap)"; local name="r$r-$t-$v"
  echo "$(date -u +%H:%M:%S) $name (load $(cut -d' ' -f1 /proc/loadavg))"
  dotnet "$dll" bench "${bench_args[@]}" --db "bench_ab_$v" "$@" --out "$out/$name.json" > "$out/$name.log" 2>&1 || echo "  $name failed"
  echo "{\"before\": $before, \"after\": $(snap)}" > "$out/$name.load.json"
}
for ((r = 1; r <= rounds; r++)); do
  for t in "enqueue-c1|--mode enqueue --n 2000 --calls 1" "enqueue-c16|--mode enqueue --n 10000 --calls 16" \
           "drain-w16|--mode drain --n $n --workers 16 --calls 8" "steady-100|--mode steady --rate 100 --seconds $secs --workers 16 --calls $steady_calls"; do
    [[ " ${BENCH_TESTS:-enqueue-c1 enqueue-c16 drain-w16 steady-100} " == *" ${t%%|*} "* ]] || continue
    if ((r % 2)); then order="a b"; else order="b a"; fi
    for v in $order; do
      dll="$a"; [[ $v == b ]] && dll="$b"
      # shellcheck disable=SC2086
      one "$r" "${t%%|*}" "$v" "$dll" ${t#*|}
    done
  done
done
python3 "$(dirname "$0")/summarize.py" "$out"
