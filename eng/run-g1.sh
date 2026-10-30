#!/usr/bin/env bash
# G-1 run (07 §15): the full Core and Postgres suites on Docker (RUNWICK_TEST_PG must be unset, so every
# RequiresDocker test executes), then every SP-010 workload, then the case table from the xunit XML results.
# Usage: eng/run-g1.sh [out-dir]   (default artifacts/g1/<utc timestamp>)
# Environment: G1_SKIP_TESTS=1 or G1_SKIP_WORKLOADS=1 to run one half; G1_WORKLOADS to choose workloads.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
unset RUNWICK_TEST_PG

out="${1:-artifacts/g1/$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$out/results" "$out/perf"
out="$(cd "$out" && pwd)"
echo "G-1 run into $out; commit $(git rev-parse HEAD) ($(git status --porcelain | wc -l) uncommitted paths)"

dotnet build Runwick.slnx -c Release 2>&1 | tail -3
# The workload runner is copied so later builds cannot replace binaries under a running workload.
rm -rf "$out/bin" && cp -r tests/Runwick.Workloads/bin/Release/net10.0 "$out/bin"

if [[ -z "${G1_SKIP_TESTS:-}" ]]; then
  for project in Runwick.Core.Tests Runwick.Postgres.Tests; do
    start=$(date +%s)
    "tests/$project/bin/Release/net10.0/$project" -noLogo -noColor -result-xml "$out/results/$project.xml" \
      > "$out/results/$project.log" 2>&1
    echo "$project: exit $? in $(( $(date +%s) - start )) s; $(grep -E 'Total: ' "$out/results/$project.log" | tail -1)"
  done
fi

if [[ -z "${G1_SKIP_WORKLOADS:-}" ]]; then
  wl="$out/bin/Runwick.Workloads"
  for name in ${G1_WORKLOADS:-G1-M1 G1-M2 P-02 G1-M3 P-01 P-08a P-08b P-08c}; do
    args=()
    # 07 §14.3 asks for 3 repetitions of P-01 and P-02 only; the P-08 variants run once. P-01's drain of everything
    # accepted is bounded at 90 min (a backlog left after that is a run failure, never silently dropped).
    case "$name" in
      P-08*) args+=(--reps 1) ;;
      P-01) args+=(--drain-min 90) ;;
    esac
    if [[ "$name" == "G1-M3" ]]; then
      rate=$(python3 -c "import json,glob;f=sorted(glob.glob('$out/perf/*-P-02/result.json'))[-1];print(json.load(open(f))['sustainableRate'])" 2>/dev/null || echo 1)
      args+=(--rate "$rate")
    fi
    start=$(date +%s)
    "$wl" run "$name" --out "$out/perf/$(date -u +%Y%m%d-%H%M%S)-$name" "${args[@]}" > "$out/perf/$name.log" 2>&1
    echo "$name: exit $? in $(( $(date +%s) - start )) s; $(tail -1 "$out/perf/$name.log")"
  done
fi

"$out/bin/Runwick.Workloads" report --results "$out/results" --workloads "$out/perf" --out "$out/case-table.md" \
  ${G1_REASONS:+--reasons "$G1_REASONS"}
echo "case table: $out/case-table.md"
