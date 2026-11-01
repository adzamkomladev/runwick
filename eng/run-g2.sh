#!/usr/bin/env bash
# G-2 run (15 §12): the full Core, Outbox and Postgres suites on Docker (RUNWICK_TEST_PG must be unset, so every
# RequiresDocker test executes), then the 15 §11 workloads, then the G-2 case table and checklist from the results.
# Usage: eng/run-g2.sh [out-dir]   (default artifacts/g2/<utc timestamp>)
# Environment: G2_SKIP_TESTS=1 or G2_SKIP_WORKLOADS=1 to run one half; G2_WORKLOADS to choose workloads;
# G2_REASONS for the human-added reasons file (CASE<TAB>reason or "CASE ITEM"<TAB>reason).
set -uo pipefail
cd "$(dirname "$0")/.."
# node runs the independent vector encoder (tests/vectors/tools/canonical.mjs); a missing node skips WireVectorTests.
command -v node >/dev/null || export PATH="$HOME/.local/share/vite-plus/bin:$PATH"
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
unset RUNWICK_TEST_PG

out="${1:-artifacts/g2/$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$out/results" "$out/perf"
out="$(cd "$out" && pwd)"
echo "G-2 run into $out; commit $(git rev-parse HEAD) ($(git status --porcelain | wc -l) uncommitted paths)"

dotnet build Runwick.slnx -c Release 2>&1 | tail -3
# The workload runner is copied so later builds cannot replace binaries under a running workload.
rm -rf "$out/bin" && cp -r tests/Runwick.Workloads/bin/Release/net10.0 "$out/bin"

if [[ -z "${G2_SKIP_TESTS:-}" ]]; then
  for project in Runwick.Core.Tests Runwick.Outbox.Tests Runwick.Postgres.Tests; do
    start=$(date +%s)
    "tests/$project/bin/Release/net10.0/$project" -noLogo -noColor -result-xml "$out/results/$project.xml" \
      > "$out/results/$project.log" 2>&1
    echo "$project: exit $? in $(( $(date +%s) - start )) s; $(grep -E 'Total: ' "$out/results/$project.log" | tail -1)"
  done
fi

if [[ -z "${G2_SKIP_WORKLOADS:-}" ]]; then
  wl="$out/bin/Runwick.Workloads"
  # 15 §11: the G-1 reruns (G1-M3 at the G-1 sustainable rate unless G2_G1M3_RATE says otherwise), then the M3 workloads.
  for name in ${G2_WORKLOADS:-G1-M1 G1-M3 P-04 P-05 P-06r P-06s P-09}; do
    args=()
    case "$name" in
      G1-M3) args+=(--rate "${G2_G1M3_RATE:-1}") ;;
    esac
    start=$(date +%s)
    "$wl" run "$name" --out "$out/perf/$(date -u +%Y%m%d-%H%M%S)-$name" "${args[@]}" > "$out/perf/$name.log" 2>&1
    echo "$name: exit $? in $(( $(date +%s) - start )) s; $(tail -1 "$out/perf/$name.log")"
  done
fi

"$out/bin/Runwick.Workloads" report --gate g2 --results "$out/results" --workloads "$out/perf" --out "$out/case-table.md" \
  ${G2_REASONS:+--reasons "$G2_REASONS"}
echo "case table: $out/case-table.md"
