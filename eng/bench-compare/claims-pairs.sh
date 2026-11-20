#!/usr/bin/env bash
# Claims in flight (05 §9.7) on PostgreSQL and SQLite: paired drains of a base build against the candidate at 1 (base), 2 and 4 claims
# in flight, arm order rotated each round, every round under the shared bench lock, load recorded. PostgreSQL runs in its own
# container (rw-cp-pg, removed at the end) with a fresh database per run; SQLite uses a fresh file per run.
# usage: claims-pairs.sh <postgres|sqlite> <rounds> <base-bin-dir> <cand-bin-dir> <out-dir> [extra bench flags...]
set -uo pipefail
store=$1; rounds=$2; base=$3; cand=$4; out=$5; shift 5
extra=("$@")
export PATH="/root/.dotnet:$PATH" DOTNET_ROOT=/root/.dotnet
mkdir -p "$out"
conn=()
if [[ $store == postgres ]]; then
  port=${PG_PORT:-55731}
  docker run -d --name rw-cp-pg -e POSTGRES_PASSWORD=pg -p "127.0.0.1:$port:5432" postgres:18 >/dev/null
  for i in $(seq 1 120); do docker exec rw-cp-pg pg_isready -U postgres >/dev/null 2>&1 && break; sleep .5; done
  sleep 2
  conn=(--conn "Host=127.0.0.1;Port=$port;Username=postgres;Password=pg;Database=postgres")
fi
arms=(base cif2 cif4)
exec 9>/tmp/runwick-bench.lock
for round in $(seq 1 "$rounds"); do
  flock 9
  n=${#arms[@]}
  for i in $(seq 0 $((n - 1))); do
    arm=${arms[$(( (i + round - 1) % n ))]}
    bin=$cand; flags=()
    case $arm in base) bin=$base ;; cif*) flags=(--claims-in-flight "${arm#cif}") ;; esac
    uptime > "$out/$round-$arm.load"
    echo "$(date -Is) $store round $round $arm"
    "$bin/Runwick.Workloads" bench --store "$store" "${conn[@]}" --db "bench_cp_$arm" --mode drain --n 20000 --workers 16 --calls 1 \
      "${extra[@]}" "${flags[@]}" --out "$out/$round-$arm.json" > "$out/$round-$arm.log" 2>&1
    echo "exit $?" >> "$out/$round-$arm.log"
  done
  flock -u 9
done
[[ $store == postgres ]] && docker rm -fv rw-cp-pg >/dev/null
echo "DONE $store $(date -Is)"
