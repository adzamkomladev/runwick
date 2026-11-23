#!/usr/bin/env bash
# Part of the 0029 jobs index diet measurements (docs/reports/fragments/pg-index.md). Server: PORT (default 55631), user postgres, password pg.
# WAL attributed to each jobs index during a short drain on a copy of a template (pg_walinspect). usage: walattr.sh <template> <bin> <n>
set -euo pipefail
export PGPASSWORD=pg PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
T=$1; BIN=$2; N=$3; DB=${T}_w
Q() { psql -h 127.0.0.1 -p "${PORT:-55631}" -U postgres -d "$1" -Atqc "$2"; }
Q postgres "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
Q postgres "CREATE DATABASE $DB TEMPLATE $T STRATEGY file_copy"
Q $DB "CREATE EXTENSION IF NOT EXISTS pg_walinspect"
Q postgres "CHECKPOINT"
L0=$(Q postgres "SELECT pg_current_wal_lsn()")
ADM="Host=127.0.0.1;Port=${PORT:-55631};Username=postgres;Password=pg;Database=postgres"
flock /tmp/runwick-bench.lock "$BIN/Runwick.Workloads" bench --mode drain --store postgres --n "$N" --payload 512 --db "$DB" --db-reuse --conn "$ADM" --out "walattr-$T.json" > "walattr-$T.log" 2>&1
L1=$(Q postgres "SELECT pg_current_wal_lsn()")
echo "total WAL bytes: $(Q postgres "SELECT pg_wal_lsn_diff('$L1', '$L0')")  jobs: $N"
Q $DB "
WITH named AS (
  SELECT coalesce(c.relname, 'other:' || b.relfilenode) AS rel, count(*) AS blocks, sum(b.block_fpi_length) AS fpi,
         sum(b.block_data_length) AS data
  FROM pg_get_wal_block_info('$L0', '$L1', true) AS b
  LEFT JOIN pg_class c ON c.relfilenode = b.relfilenode
  GROUP BY 1)
SELECT rel, blocks, data, fpi, round((data + fpi)::numeric / $N, 1) AS bytes_per_job FROM named
WHERE rel LIKE 'jobs%' OR rel LIKE 'job_payloads%' ORDER BY data + fpi DESC" | column -t -s'|'
grep -o '"diagnostics": \[[^]]*\]' "walattr-$T.json" || true
Q postgres "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
