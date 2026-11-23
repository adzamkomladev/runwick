#!/usr/bin/env bash
# Paired alternating drains on copies of the retained-data templates built by setup.sh.
# Base arm: BASE_BIN on a copy of pgi_ret (revision 26). Candidate arm: CAND_BIN on a copy of pgi_ret_c (already at 29, so
# the migration is not in the measured WAL). Per arm: jobs/s from the bench JSON, server WAL bytes, jobs index growth.
# usage: pairs.sh <base-workloads-bin-dir> <cand-workloads-bin-dir> <outdir> <pairs> [n]
set -uo pipefail
export PGPASSWORD=pg PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
BASE_BIN=$1; CAND_BIN=$2; OUT=$3; PAIRS=$4; N=${5:-5000}; PORT=${PORT:-55631}
mkdir -p "$OUT"; cd "$OUT"
Q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d "$1" -Atqc "$2"; }
ADM="Host=127.0.0.1;Port=$PORT;Username=postgres;Password=pg;Database=postgres"
for pair in $(seq 1 "$PAIRS"); do
  if (( pair % 2 )); then order="base cand"; else order="cand base"; fi
  for arm in $order; do
    tag="p${pair}-${arm}"; db="pgiab_${pair}_${arm}"
    if [ "$arm" = base ]; then tpl=pgi_ret; bin=$BASE_BIN; else tpl=pgi_ret_c; bin=$CAND_BIN; fi
    Q postgres "DROP DATABASE IF EXISTS $db WITH (FORCE)"
    Q postgres "CREATE DATABASE $db TEMPLATE $tpl STRATEGY file_copy"
    Q postgres "CHECKPOINT"
    idx0=$(Q $db "SELECT pg_indexes_size('runwick.jobs')")
    wal0=$(Q postgres "SELECT pg_current_wal_lsn()")
    flock /tmp/runwick-bench.lock "$bin/Runwick.Workloads" bench --mode drain --store postgres --n "$N" --payload 512 --db "$db" --db-reuse --conn "$ADM" --out "$tag.json" > "$tag.log" 2>&1
    echo "exit $?" >> "$tag.log"
    echo "$(Q postgres "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '$wal0')") $(( $(Q $db "SELECT pg_indexes_size('runwick.jobs')") - idx0 ))" > "$tag.wal-idx"
    Q postgres "DROP DATABASE IF EXISTS $db WITH (FORCE)"
  done
done
echo "per arm: <tag> <jobs/s> <WAL bytes> <jobs index growth bytes>"
for f in p*-*.json; do
  t=${f%.json}
  printf '%s %s %s drained=%s\n' "$t" "$(grep -o '"completionsPerSec": *[0-9.]*' "$f" | head -1 | grep -o '[0-9.]*$')" "$(cat "$t.wal-idx")" "$(grep -o '"drained": *[a-z]*' "$f" | grep -o '[a-z]*$')"
done
