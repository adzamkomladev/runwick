#!/usr/bin/env bash
# SP-029 qualification run (docs/architecture/performance-acceptance-plan.md, P-01 to P-10 and the D-14 thresholds).
# Runs every P-case in order, each on its own Testcontainers PostgreSQL server (RUNWICK_TEST_PG is ignored), then writes
# <out>/summary.md with one PASS / FAIL / INFORMATIONAL verdict per threshold (eng/sp029-summary.py).
#
# Usage: eng/run-sp029.sh [out-dir]   (default artifacts/sp029/<utc timestamp>)
# Environment:
#   SP029_PROFILE   short (default; fits a shared host: 1 repetition, 1 min warm-up, 3 min measured, smaller P-07 seed)
#                   or full (the plan's sizes: 5 min warm-up, 15 min measured, 3 repetitions, 1 M terminal jobs).
#   SP029_CASES     cases to run, in order (default: all). P-01t is the instrumentation-cost pair on P-01.
#   SP029_SOAK_MIN  P-10 duration in minutes (default 1440 = 24 h; 10 for a smoke run).
#   SP029_SOAK_RATE P-10 offered rate; default 70 % of the P-02 sustainable rate from this run, else 35/s.
#   SP029_REFERENCE 1 only on a dedicated, quiet reference host: verdicts become PASS/FAIL instead of INFORMATIONAL.
#   SP029_SKIP_BUILD 1 to reuse the current Release build.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
unset RUNWICK_TEST_PG

profile="${SP029_PROFILE:-short}"
out="${1:-artifacts/sp029/$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$out/perf"
out="$(cd "$out" && pwd)"
echo "SP-029 ($profile) into $out; commit $(git rev-parse HEAD) ($(git status --porcelain | wc -l) uncommitted paths); load $(cat /proc/loadavg)"

if [[ -z "${SP029_SKIP_BUILD:-}" ]]; then
  dotnet build Runwick.slnx -c Release 2>&1 | tail -3
fi
# The runner is copied so a later build cannot replace binaries under a running workload.
rm -rf "$out/bin" && cp -r tests/Runwick.Workloads/bin/Release/net10.0 "$out/bin"
wl="$out/bin/Runwick.Workloads"

# Per-case arguments. Workload defaults are already the plan's sizes, so "full" mostly adds drain bounds.
args_for() {
  local name=$1
  if [[ "$profile" == full ]]; then
    case "$name" in
      P-01|P-01t) echo "--drain-min 90" ;;
      P-08a|P-08b|P-08c) echo "--reps 1" ;;
      P-06r) echo "--drain-min 90" ;;
      P-07) echo "--reps 3 --drain-min 90" ;;
      P-09) echo "--rate 100 --drain-min 90" ;;
      *) echo "" ;;
    esac
    return
  fi
  local m="--reps 1 --warmup-min 1 --measure-min 3"
  case "$name" in
    P-01) echo "$m --drain-min 30" ;;
    P-01t) echo "$m --drain-min 30" ;;
    P-02) echo "--reps 1 --step-settle-min 0.5 --step-measure-min 1 --confirm-warmup-min 1 --measure-min 3" ;;
    P-03) echo "$m --drain-min 30" ;;
    P-04) echo "--warmup-min 1 --admissions 2000" ;;
    P-05) echo "--warmup-min 1 --measure-min 3 --replicas 1,3" ;;
    P-06r) echo "--warmup-min 1 --measure-min 3 --drain-min 30" ;;
    P-06s) echo "--warmup-min 1 --measure-min 3 --catchup-occurrences 300" ;;
    P-07) echo "--reps 1 --warmup-min 2 --measure-min 3 --terminal-jobs 100000 --drain-min 30" ;;
    P-08a|P-08b) echo "--reps 1 --warmup-min 1 --phase-min 2" ;;
    P-08c) echo "--reps 1 --warmup-min 1 --observe-min 3" ;;
    P-09) echo "--warmup-min 1 --measure-min 2 --rate 100 --drain-min 30" ;;
    *) echo "" ;;
  esac
}

for name in ${SP029_CASES:-P-01 P-01t P-02 P-03 P-04 P-05 P-06r P-06s P-07 P-08a P-08b P-08c P-09 P-10}; do
  # shellcheck disable=SC2207
  args=($(args_for "$name"))
  if [[ "$name" == P-10 ]]; then
    rate="${SP029_SOAK_RATE:-$(python3 -c "import json,glob;f=sorted(glob.glob('$out/perf/*-P-02/result.json'))[-1];r=json.load(open(f))['sustainableRate'];print(round(0.7*r,2) if r else 35)" 2>/dev/null || echo 35)}"
    soak="${SP029_SOAK_MIN:-1440}"
    gc=$(( soak >= 120 ? 300 : 30 ))
    args+=(--rate "$rate" --duration-min "$soak" --gc-interval-s "$gc")
    [[ "$profile" == short ]] && args+=(--warmup-min 1 --drain-min 30)
  fi
  start=$(date +%s)
  "$wl" run "$name" --out "$out/perf/$(date -u +%Y%m%d-%H%M%S)-$name" "${args[@]}" > "$out/perf/$name.log" 2>&1
  echo "$name: exit $? in $(( $(date +%s) - start )) s; $(tail -1 "$out/perf/$name.log")"
done

python3 eng/sp029-summary.py "$out" --profile "$profile" ${SP029_REFERENCE:+--reference}
