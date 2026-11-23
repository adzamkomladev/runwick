#!/usr/bin/env bash
# Paired alternating drains for migration 0030_receipt_by_key (docs/reports/fragments/pg-receipts.md).
# Each arm: a fresh database created and migrated by its own binary (a 20-job warm-up drain), CHECKPOINT, then an N-job
# drain with --db-reuse. Per arm: jobs/s, server WAL bytes per job, receipt relations' size growth.
# Base arm: BASE_BIN (schema revision 29). Candidate arm: CAND_BIN (revision 30).
# Run against a private server so the WAL total is this drain's alone, for example:
#   docker run -d --name runwick-rcpt-pg -e POSTGRES_PASSWORD=pg -p 127.0.0.1:55641:5432 postgres:18 \
#     -c shared_buffers=512MB -c max_connections=300 -c max_wal_size=8GB
# usage: pairs.sh <base-workloads-bin-dir> <cand-workloads-bin-dir> <outdir> <pairs> [n]   Server: PORT (default 55641).
set -uo pipefail
export PGPASSWORD=pg PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
BASE_BIN=$1; CAND_BIN=$2; OUT=$3; PAIRS=$4; N=${5:-5000}; PORT=${PORT:-55641}
mkdir -p "$OUT"; cd "$OUT"
Q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d "$1" -Atqc "$2"; }
ADM="Host=127.0.0.1;Port=$PORT;Username=postgres;Password=pg;Database=postgres"
SIZE="SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'runwick' AND c.relkind = 'r' AND c.relname LIKE 'storage_commands_%'"
for pair in $(seq 1 "$PAIRS"); do
  if (( pair % 2 )); then order="base cand"; else order="cand base"; fi
  for arm in $order; do
    tag="p${pair}-${arm}"; db="rcptab_${pair}_${arm}"
    if [ "$arm" = base ]; then bin=$BASE_BIN; else bin=$CAND_BIN; fi
    Q postgres "DROP DATABASE IF EXISTS $db WITH (FORCE)"
    flock /tmp/runwick-bench.lock "$bin/Runwick.Workloads" bench --mode drain --store postgres --n 20 --payload 512 --db "$db" --conn "$ADM" --out "$tag-warm.json" > "$tag-warm.log" 2>&1
    Q postgres "CHECKPOINT"
    size0=$(Q "$db" "$SIZE")
    wal0=$(Q postgres "SELECT pg_current_wal_lsn()")
    flock /tmp/runwick-bench.lock "$bin/Runwick.Workloads" bench --mode drain --store postgres --n "$N" --payload 512 --db "$db" --db-reuse --conn "$ADM" --out "$tag.json" > "$tag.log" 2>&1
    echo "exit $?" >> "$tag.log"
    echo "$(( $(Q postgres "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '$wal0')::bigint") / N )) $(( $(Q "$db" "$SIZE") - size0 ))" > "$tag.wal-size"
    Q postgres "DROP DATABASE IF EXISTS $db WITH (FORCE)"
  done
done
echo "per arm: <tag> <jobs/s> <WAL bytes per job> <receipt relations growth bytes>"
for f in p*-base.json p*-cand.json; do
  t=${f%.json}
  printf '%s %s %s drained=%s\n' "$t" "$(grep -o '"completionsPerSec": *[0-9.]*' "$f" | head -1 | grep -o '[0-9.]*$')" "$(cat "$t.wal-size")" "$(grep -o '"drained": *[a-z]*' "$f" | grep -o '[a-z]*$')"
done
