#!/usr/bin/env bash
# Receipt cost per drained job on PostgreSQL (docs/reports/fragments/pg-receipts.md).
# Creates database DB with BIN (a tiny warm-up drain migrates it), empties the receipts, checkpoints, drains N jobs with
# --db-reuse, then prints total server WAL, WAL attributed to each receipt relation (pg_walinspect), receipt rows and bytes
# per operation, and relation sizes.
# FF=<n> sets the receipt partitions' fill factor before the drain (probe).
# usage: measure.sh <workloads-bin-dir> <db> [n]   Server: PORT (default 55641; a private server, because WAL totals and pg_walinspect need no other writers), user postgres, password pg.
set -euo pipefail
export PGPASSWORD=pg PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
BIN=$1; DB=$2; N=${3:-2000}; PORT=${PORT:-55641}
Q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d "$1" -Atqc "$2"; }
ADM="Host=127.0.0.1;Port=$PORT;Username=postgres;Password=pg;Database=postgres"
run() { flock /tmp/runwick-bench.lock "$BIN/Runwick.Workloads" bench --mode drain --store postgres --payload 512 --db "$DB" --conn "$ADM" "$@"; }
Q postgres "DROP DATABASE IF EXISTS $DB WITH (FORCE)"
run --n 20 --out "$DB-warm.json" > "$DB-warm.log" 2>&1
Q "$DB" "CREATE EXTENSION IF NOT EXISTS pg_walinspect"
Q "$DB" "TRUNCATE runwick.storage_commands"
if [ -n "${FF:-}" ]; then # probe: a receipt partition fill factor other than the migrated one
  for part in storage_commands_durable storage_commands_ownership; do Q "$DB" "ALTER TABLE runwick.$part SET (fillfactor = $FF)"; done
fi
Q postgres "CHECKPOINT"
L0=$(Q postgres "SELECT pg_current_wal_lsn()")
run --n "$N" --db-reuse --out "$DB.json" > "$DB.log" 2>&1
L1=$(Q postgres "SELECT pg_current_wal_lsn()")
echo "jobs/s: $(grep -o '"completionsPerSec": *[0-9.]*' "$DB.json" | head -1 | grep -o '[0-9.]*$')  drained: $(grep -o '"drained": *[a-z]*' "$DB.json" | grep -o '[a-z]*$')"
echo "total WAL bytes/job: $(Q postgres "SELECT round(pg_wal_lsn_diff('$L1', '$L0') / $N)")"
echo "-- WAL by relation, bytes per job: block data, full-page images, whole records (headers included) touching it"
Q "$DB" "
WITH b AS (
  SELECT b.start_lsn, b.relfilenode, b.block_data_length, b.block_fpi_length
  FROM pg_get_wal_block_info('$L0', '$L1', true) AS b),
r AS (SELECT start_lsn, record_length FROM pg_get_wal_records_info('$L0', '$L1')),
per AS (
  SELECT coalesce(c.relname, 'other:' || b.relfilenode) AS rel, count(*) AS blocks,
         sum(b.block_data_length) AS data, sum(b.block_fpi_length) AS fpi
  FROM b LEFT JOIN pg_class c ON c.relfilenode = b.relfilenode GROUP BY 1),
rec AS (
  SELECT coalesce(c.relname, 'other:' || x.relfilenode) AS rel, sum(r.record_length) AS rec
  FROM (SELECT DISTINCT start_lsn, relfilenode FROM b) x JOIN r USING (start_lsn)
  LEFT JOIN pg_class c ON c.relfilenode = x.relfilenode GROUP BY 1)
SELECT per.rel, blocks, round(data::numeric / $N, 1), round(fpi::numeric / $N, 1), round(rec::numeric / $N, 1)
FROM per JOIN rec USING (rel)
WHERE per.rel LIKE 'storage_commands%' OR per.rel LIKE 'command_%'
ORDER BY data + fpi DESC" | column -t -s'|'
echo "-- receipts per operation: class, operation, rows per job, avg row bytes, avg result bytes (stored), avg result text"
Q "$DB" "SELECT retention_class, operation, round(count(*)::numeric / $N, 2), round(avg(pg_column_size(c.*))),
       round(avg(pg_column_size(result))), round(avg(length(result::text)))
       FROM runwick.storage_commands c GROUP BY 1, 2 ORDER BY 1, 2" | column -t -s'|'
echo "-- relation sizes, bytes"
Q "$DB" "SELECT c.relname, pg_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'runwick' AND c.relkind IN ('r', 'i')
         AND (c.relname LIKE 'storage_commands_%' OR c.relname LIKE 'command_%') ORDER BY 1" | column -t -s'|'
echo "-- inserts, updates, HOT updates"
Q "$DB" "SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables
       WHERE relname LIKE 'storage_commands_%' ORDER BY 1" | column -t -s'|'
