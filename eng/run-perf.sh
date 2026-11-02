#!/usr/bin/env bash
# Performance run (performance plan PF-01): the store-call floor (client CPU and allocation per job, allocation profile),
# the idle cost per worker / scheduler / relay, and a shortened P-01, all against one dedicated Testcontainers server
# reached at the container IP, so docker-proxy is not in the path. RUNWICK_TEST_PG is ignored (unset).
# Usage: eng/run-perf.sh [out-dir]   (default artifacts/perf/<utc date>; results in <out-dir>/summary.md)
# Environment: PERF_SKIP_BUILD=1 to reuse the current Release build; PERF_ARGS for extra `perf` arguments, e.g.
#   PERF_ARGS="--n 200 --cost-jobs 500 --idle-s 120 --p01-measure-min 5" or PERF_ARGS="--skip-p01".
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}" DOTNET_NOLOGO=1
unset RUNWICK_TEST_PG

out="${1:-artifacts/perf/$(date -u +%Y-%m-%d)}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
echo "perf run into $out; commit $(git rev-parse HEAD) ($(git status --porcelain | wc -l) uncommitted paths); load $(cat /proc/loadavg)"

if [[ -z "${PERF_SKIP_BUILD:-}" ]]; then
  dotnet build Runwick.slnx -c Release 2>&1 | tail -3
fi
# The runner is copied so a later build cannot replace binaries under a running measurement.
rm -rf "$out/bin" && cp -r tests/Runwick.Workloads/bin/Release/net10.0 "$out/bin"

# shellcheck disable=SC2086
"$out/bin/Runwick.Workloads" perf --out "$out" ${PERF_ARGS:-} 2>&1 | tee "$out/perf.log"
status=${PIPESTATUS[0]}
echo "perf: exit $status; summary: $out/summary.md"
exit "$status"
