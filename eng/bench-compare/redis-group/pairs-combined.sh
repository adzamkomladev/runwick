#!/usr/bin/env bash
# Combined finish-and-claim pairs: one Runwick bench build run with and without the option (arms base, comb; buf = buffered finish, both = both), and BullMQ, alternating order, a fresh Redis 8.2 per run, each round under the shared bench lock.
# usage: pairs-combined.sh <workload> <rounds> <bench-bin-dir> [arms...]    workloads: drain steady500 delayed100 steady100
# Arms cif1 to cif4 run the candidate with buffered finish and that many claims in flight per pool (05 §9.7); ucif1 to ucif4 the same without buffered finish.
# Arms caN (ca8, ca16, ca32) run the candidate with buffered finish and claim ahead N (05 §9.8: N jobs claimed beyond the 16 slots, still 16 handlers);
# caNcif2 the same with 2 claims in flight.
# Arms old and oldbuf run a second build given by BASE_BIN (without and with buffered finish), for base-against-change pairs; midbuf runs a
# third build given by MID_BIN with buffered finish (two changes stacked on one base, measured in the same rounds).
# APPENDFSYNC=everysec runs every Redis with appendfsync everysec and the Runwick arms with --redis-everysec (G4); default always.
# FSYNC=tmpfs puts the AOF on memory (fsync costs almost nothing): the CPU-bound view. Default is the host disk (fsync about 7 ms here).
set -uo pipefail
cd "$(dirname "$0")"
export PATH="/root/.dotnet:$PATH" DOTNET_ROOT=/root/.dotnet
workload=$1; rounds=$2; cand=$3; shift 3
arms=("$@"); [[ ${#arms[@]} -eq 0 ]] && arms=(base comb bullmq)
bullmq_dir="$(pwd)"
image=redis:8.2-alpine
tag="$workload${RESULT_TAG:-}"
mkdir -p "results/$tag"

run_arm() {
  local round=$1 arm=$2
  local prefix="results/$tag/$round-$arm"
  local fixture="${NAME_PREFIX:-runwick-diet-run}-$$-$round-$arm"   # NAME_PREFIX: container name prefix
  local mounts=()
  [[ "${FSYNC:-disk}" == tmpfs ]] && mounts=(--mount type=tmpfs,destination=/data,tmpfs-mode=1777)
  docker run --pull=never -d --name "$fixture" "${mounts[@]}" -p 127.0.0.1::6379 "$image" redis-server --appendonly yes --appendfsync "${APPENDFSYNC:-always}" --aof-load-truncated no --maxmemory-policy noeviction --save '' --auto-aof-rewrite-percentage 0 > "$prefix.container"
  for i in $(seq 1 60); do [[ "$(docker exec "$fixture" redis-cli ping 2>/dev/null || true)" == PONG ]] && break; sleep .25; done
  local address; address="$(docker port "$fixture" 6379/tcp | head -1 | sed 's/0.0.0.0/127.0.0.1/')"
  uptime > "$prefix.load"
  local flags=()
  local bin=$cand workers=16
  case "$arm" in comb) flags=(--combined-finish-claim) ;; buf) flags=(--buffered-finish) ;; both) flags=(--buffered-finish --combined-finish-claim) ;;
    ca[0-9]*cif2) n=${arm#ca}; flags=(--buffered-finish --claim-ahead "${n%cif2}" --claims-in-flight 2) ;;
    ca[0-9]*) flags=(--buffered-finish --claim-ahead "${arm#ca}") ;;
    cif[1-4]) flags=(--buffered-finish --claims-in-flight "${arm#cif}") ;; ucif[1-4]) flags=(--claims-in-flight "${arm#ucif}") ;;
    old) bin=$BASE_BIN ;; oldbuf) bin=$BASE_BIN; flags=(--buffered-finish) ;; midbuf) bin=$MID_BIN; flags=(--buffered-finish) ;; oldbuf32) bin=$BASE_BIN; flags=(--buffered-finish); workers=32 ;; esac
  [[ "${APPENDFSYNC:-always}" == everysec ]] && flags+=(--redis-everysec)
  local rw=("$bin/Runwick.Workloads" bench --store redis --redis "$address" --out "$prefix.json" "${flags[@]}")
  local cmd=()
  case "$workload:$arm" in
    drain:bullmq)      cmd=(node "$bullmq_dir/bench-base.mjs" --redis "$address" --mode drain --n 20000 --concurrency 16 --out "$prefix.json") ;;
    drain:*)           cmd=("${rw[@]}" --mode drain --n 20000 --workers "$workers" --calls 1) ;;
    steady500:bullmq)  cmd=(node "$bullmq_dir/bench-steady.mjs" --concurrency 16 --redis "$address" --mode steady --calls 16 --seconds 30 --rate 500 --out "$prefix.json") ;;
    steady500:*)       cmd=("${rw[@]}" --trace --workers 16 --store-warmup-seconds 12 --mode steady --calls 16 --seconds 30 --rate 500) ;;
    steady100:bullmq)  cmd=(node "$bullmq_dir/bench-steady.mjs" --concurrency 16 --redis "$address" --mode steady --calls 16 --seconds 30 --rate 100 --out "$prefix.json") ;;
    steady100:*)       cmd=("${rw[@]}" --trace --workers 16 --store-warmup-seconds 12 --mode steady --calls 16 --seconds 30 --rate 100) ;;
    delayed100:bullmq) cmd=(node "$bullmq_dir/bench-delayed.mjs" --concurrency 16 --redis "$address" --mode steady --calls 16 --seconds 30 --rate 100 --out "$prefix.json") ;;
    delayed100:*)      cmd=("${rw[@]}" --trace --workers 16 --store-warmup-seconds 12 --mode steady --calls 16 --seconds 30 --rate 100 --delay-ms 5000) ;;
    *) echo "unknown $workload:$arm"; docker rm -fv "$fixture" >/dev/null; return 1 ;;
  esac
  echo "$(date -Is) $workload round $round $arm"
  "${cmd[@]}" > "$prefix.log" 2>&1
  echo "exit $?" >> "$prefix.log"
  docker exec "$fixture" redis-cli INFO cpu persistence commandstats > "$prefix.redis" 2>/dev/null
  docker rm -fv "$fixture" >/dev/null 2>&1
}

exec 9>/tmp/runwick-bench.lock
for round in $(seq 1 "$rounds"); do
  n=${#arms[@]}
  flock 9   # the whole round (all arms, alternating order) holds the shared bench lock, so its arms see the same neighbours
  for i in $(seq 0 $((n - 1))); do run_arm "$round" "${arms[$(( (i + round - 1) % n ))]}"; done
  flock -u 9
done
echo "DONE $workload $(date -Is)"
