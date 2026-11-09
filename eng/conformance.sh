#!/usr/bin/env bash
# Driver conformance run (SP-028, gate G-4). Runs every test project against each driver, then writes the per-driver reports.
#
#   eng/conformance.sh                   all three drivers, then the reports
#   eng/conformance.sh sqlite redis      only these drivers (reports for the others are rewritten from older results, if any)
#   eng/conformance.sh --report-only     skip the test runs and rebuild the reports from artifacts/conformance
#   eng/conformance.sh --rerun postgres 'Runwick.Postgres.Tests' 'FullyQualifiedName~Foo'   rerun one project alone (filter optional);
#                                        its results replace an earlier failure and the report says so
#
# Environment (nothing is started for you):
#   RUNWICK_TEST_PG        PostgreSQL admin connection string (default: the local runwick-pg container on 55499;
#                          set it empty for a --rerun of RequiresDocker=true cases, which start their own container)
#   RUNWICK_TEST_TEMPLATE  template database name, unique per concurrent run (default rw_template_cnf)
#   RUNWICK_TEST_REDIS     Redis host:port of an AOF-always server (22 §2), required for the redis rows
#   CONFORMANCE_OUT        result directory (default artifacts/conformance)
# Test failures do not stop the run; the report tool decides the verdict. Exit code is the tool's: 0 only if G-4 passes for all drivers.
set -u
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_NOLOGO=1
export RUNWICK_TEST_PG="${RUNWICK_TEST_PG-Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres}"
export RUNWICK_TEST_TEMPLATE="${RUNWICK_TEST_TEMPLATE:-rw_template_cnf}"
OUT="${CONFORMANCE_OUT:-artifacts/conformance}"
export CONFORMANCE_OUT="$OUT"
KINDS=(postgres:Postgres sqlite:Sqlite redis:Redis)

run_project() { # dir-under-OUT project [stores] [filter]
  local dir="$1" proj="$2" stores="${3:-}" filter="${4:-}"
  # A test that runs longer than CONFORMANCE_HANG_TIMEOUT (default 60m; the REDIS-01 kill -9 theory takes about 11 min alone and passed 30 min under full-run load) is
  # killed with a mini dump; the TRX keeps the finished results and Sequence_*.xml under the results directory names the hung test.
  local args=(test "tests/$proj" --no-build --logger trx --results-directory "$OUT/$dir/$proj"
    --blame-hang-timeout "${CONFORMANCE_HANG_TIMEOUT:-60m}" --blame-hang-dump-type mini)
  [ -n "$filter" ] && args+=(--filter "$filter")
  echo "== $dir: $proj ${stores:+(stores=$stores)}"
  if [ -n "$stores" ]; then RUNWICK_TEST_STORES="$stores" dotnet "${args[@]}"; else env -u RUNWICK_TEST_STORES dotnet "${args[@]}"; fi
}

meta() { # driver
  local d="$1"; mkdir -p "$OUT/$d"
  { echo "commit=$(git rev-parse --short HEAD)"; echo "date=$(date -u +%Y-%m-%d)"; echo "host=$(uname -srm), $(nproc) cpus"
    echo "dotnet=$(dotnet --version)"; echo "started=$(date -u +%H:%M:%SZ)"; echo "start_epoch=$(date +%s)"; } > "$OUT/$d/meta.txt"
}
finish_meta() { # driver server-description
  local d="$1" start; start=$(grep '^start_epoch=' "$OUT/$d/meta.txt" | cut -d= -f2)
  { echo "finished=$(date -u +%H:%M:%SZ)"; echo "seconds=$(( $(date +%s) - start ))"; [ -n "${2:-}" ] && echo "server=$2"; } >> "$OUT/$d/meta.txt"
}

if [ "${1:-}" = "--report-only" ]; then exec python3 eng/conformance.py; fi

if [ "${1:-}" = "--rerun" ]; then
  d="$2"; proj="$3"; filter="${4:-}"; stores=""
  for k in "${KINDS[@]}"; do [ "${k%%:*}" = "$d" ] && stores="${k##*:}"; done
  [ "$proj" = "Runwick.Contract.Tests" ] || stores=""
  run_project "$d/rerun-$proj" "$proj" "$stores" "$filter"
  exec python3 eng/conformance.py
fi

drivers=("$@"); [ ${#drivers[@]} -eq 0 ] && drivers=(postgres sqlite redis)
[ -z "${CONFORMANCE_NO_BUILD:-}" ] && { dotnet build Runwick.slnx || { echo "build failed"; exit 3; }; }

# Driver-independent engine tests and the host projects: run once, shared by all reports ("common").
meta common
for p in Runwick.Core.Tests Runwick.Cli.Tests Runwick.Outbox.Tests Runwick.Dashboard.Tests Runwick.AspNetCore.Tests; do run_project common "$p"; done
finish_meta common

for d in "${drivers[@]}"; do
  stores=""; own=""
  for k in "${KINDS[@]}"; do [ "${k%%:*}" = "$d" ] && stores="${k##*:}"; done
  case "$d" in postgres) own=Runwick.Postgres.Tests;; sqlite) own=Runwick.Sqlite.Tests;; redis) own=Runwick.Redis.Tests;; esac
  meta "$d"
  run_project "$d" Runwick.Contract.Tests "$stores"
  if [ "$d" = postgres ]; then
    # Restart/fault cases restart their own Testcontainers server, so they cannot use the shared RUNWICK_TEST_PG.
    run_project "$d" "$own" "$stores" "RequiresDocker!=true"
    RUNWICK_TEST_PG= run_project "$d/docker" "$own" "$stores" "RequiresDocker=true"
  else
    run_project "$d" "$own" "$stores"
  fi
  case "$d" in
    postgres) srv=$(docker exec runwick-pg psql -U postgres -tAc 'select version()' 2>/dev/null | cut -c1-60);;
    sqlite) srv="Microsoft.Data.Sqlite bundled engine: $(grep -h -o 'sqlite_version[^,]*' "$OUT"/sqlite/*/*.trx 2>/dev/null | head -1)";;
    redis) srv=$(docker inspect runwick-redis-cnf --format 'redis:8.2-alpine {{.Image}}' 2>/dev/null)
           srv="$srv, $(docker exec runwick-redis-cnf redis-server --version 2>/dev/null | cut -c1-60), appendonly yes, appendfsync always, noeviction";;
  esac
  finish_meta "$d" "${srv:-}"
done
python3 eng/conformance.py
