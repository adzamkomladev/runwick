#!/usr/bin/env bash
# Builds the two retained-data templates for the 0029 paired drains (docs/reports/fragments/pg-index.md).
#   pgi_ret   : schema revision 26 (built with BASE_BIN), 205,000 succeeded jobs, analyzed.
#   pgi_ret_c : a copy of pgi_ret migrated to revision 29 by CAND_CLI, analyzed.
# usage: setup.sh <base-workloads-bin-dir> <cand-cli-path> <workdir>
# Needs a PostgreSQL 18 server on PORT (default 55631, user postgres, password pg), for example:
#   docker run -d --name runwick-pgidx-<you> -e POSTGRES_PASSWORD=pg -p 55631:5432 postgres:18 \
#     -c shared_buffers=512MB -c max_connections=300 -c shared_preload_libraries=auto_explain,pg_stat_statements
set -euo pipefail
export PGPASSWORD=pg PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
BASE_BIN=$1; CAND_CLI=$2; OUT=$3; PORT=${PORT:-55631}; HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"; cd "$OUT"
Q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d "$1" -Atqc "$2"; }
ADM="Host=127.0.0.1;Port=$PORT;Username=postgres;Password=pg;Database=postgres"
flock /tmp/runwick-bench.lock "$BASE_BIN/Runwick.Workloads" bench --mode drain --store postgres --n 5000 --payload 512 --db pgi_ret --conn "$ADM" --out seed-drain.json > seed-drain.log 2>&1
python3 -I "$HERE/seed.py" pgi_ret 205000 "$PORT"
Q pgi_ret "VACUUM ANALYZE"
Q postgres "DROP DATABASE IF EXISTS pgi_ret_c WITH (FORCE)"
Q postgres "CREATE DATABASE pgi_ret_c TEMPLATE pgi_ret STRATEGY file_copy"
echo "Host=127.0.0.1;Port=$PORT;Username=postgres;Password=pg;Database=pgi_ret_c" | "$CAND_CLI" migrate --provider postgres --connection-stdin
Q pgi_ret_c "VACUUM ANALYZE"
for d in pgi_ret pgi_ret_c; do echo "$d jobs indexes: $(Q $d "SELECT pg_size_pretty(pg_indexes_size('runwick.jobs'))")"; done
