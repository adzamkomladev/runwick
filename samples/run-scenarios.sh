#!/usr/bin/env bash
# Runs the executable scenarios of docs/scenarios.md and prints one PASS/FAIL line each.
#   samples/run-scenarios.sh              all scenarios
#   samples/run-scenarios.sh SCN-02 ...   only these
# Needs: dotnet on PATH; PostgreSQL (RUNWICK_TEST_PG, default 127.0.0.1:55499 postgres/pg: each scenario makes and drops its own database);
# Redis with the AOF-always profile (RUNWICK_TEST_REDIS, default 127.0.0.1:56392: each scenario uses and flushes one numbered database).
# SQLite needs nothing: each scenario uses a temporary file. Exit code is the number of failed scenarios (capped at 99).
set -u
cd "$(dirname "$0")/.."
export RUNWICK_TEST_PG="${RUNWICK_TEST_PG:-Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres}"
export RUNWICK_TEST_REDIS="${RUNWICK_TEST_REDIS:-127.0.0.1:56392}"

dotnet build samples/Runwick.Scenarios -c Release -nologo -v q -warnaserror >/dev/null || { echo "FAIL build"; exit 99; }
dll=$(ls samples/Runwick.Scenarios/bin/Release/net10.0/Runwick.Scenarios.dll)
export RUNWICK_DASH_DLL
RUNWICK_DASH_DLL=$(ls samples/Runwick.Dashboard.Host/bin/*/net10.0/Runwick.Dashboard.Host.dll | head -1)

ids=("$@")
if [ ${#ids[@]} -eq 0 ]; then mapfile -t ids < <(dotnet "$dll" list | cut -f1); fi

failed=0
for id in "${ids[@]}"; do
  line=$(timeout 300 dotnet "$dll" run "$id" 2>&1 | tail -1) || true
  case "$line" in PASS*) ;; *) failed=$((failed + 1)); [[ "$line" == FAIL* ]] || line="FAIL $id (no result line: ${line:-timeout or crash})" ;; esac
  echo "$line"
done
echo "${#ids[@]} scenarios, $failed failed"
exit $((failed > 99 ? 99 : failed))
