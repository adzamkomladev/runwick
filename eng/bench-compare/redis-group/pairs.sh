#!/usr/bin/env bash
# Redis F: grouped dispatch off/on and BullMQ, alternating order, fresh AOF-always Redis 8.2 per run, every timing run under the shared bench lock.
# usage: pairs.sh <workload> <rounds> [arms...]   workloads: enq16 enq64 steady500 delayed100 drain
set -uo pipefail
cd "$(dirname "$0")"
export PATH="/root/.dotnet:$PATH" DOTNET_ROOT=/root/.dotnet
workload=$1; rounds=$2; shift 2
arms=("$@"); [[ ${#arms[@]} -eq 0 ]] && arms=(off on bullmq)
dll="$(cd ../../.. && pwd)/tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll"
image=redis:8.2-alpine
tag="$workload${RESULT_TAG:-}"
mkdir -p "results/$tag"

run_arm() {
  local round=$1 arm=$2
  local prefix="results/$tag/$round-$arm"
  local fixture="runwick-fork-bench-$$-$round-$arm"
  docker run --pull=never -d --name "$fixture" -p 127.0.0.1::6379 "$image" redis-server --appendonly yes --appendfsync always --aof-load-truncated no --maxmemory-policy noeviction --save '' > "$prefix.container"
  for i in $(seq 1 60); do [[ "$(docker exec "$fixture" redis-cli ping 2>/dev/null || true)" == PONG ]] && break; sleep .25; done
  local address; address="$(docker port "$fixture" 6379/tcp | head -1 | sed 's/0.0.0.0/127.0.0.1/')"
  uptime > "$prefix.load"
  local cmd=()
  local rw=(dotnet "$dll" bench --store redis --redis "$address" --out "$prefix.json")
  [[ $arm == base ]] && rw[1]="${BASE_DLL:?arm base needs BASE_DLL, the workload binary of the tree before the change}"
  [[ $arm == on ]] && rw+=(--redis-group)
  [[ $arm == on4 ]] && rw+=(--redis-group --group-slots 4)
  [[ $arm == on8 ]] && rw+=(--redis-group --group-slots 8)
  [[ $arm == on16 ]] && rw+=(--redis-group --group-slots 16 --group-items 4)
  case "$workload:$arm" in
    enq16:bullmq)     cmd=(node bench-base.mjs --redis "$address" --mode enqueue --n 10000 --calls 16 --warmup-jobs 500 --out "$prefix.json") ;;
    enq16:*)          cmd=("${rw[@]}" --mode enqueue --n 10000 --calls 16 --warmup-jobs 500) ;;
    enq64:bullmq)     cmd=(node bench-base.mjs --redis "$address" --mode enqueue --n 10000 --calls 64 --warmup-jobs 500 --out "$prefix.json") ;;
    enq64:*)          cmd=("${rw[@]}" --mode enqueue --n 10000 --calls 64 --warmup-jobs 500) ;;
    steady500:bullmq) cmd=(node bench-steady.mjs --concurrency 16 --redis "$address" --mode steady --calls 16 --seconds 30 --rate 500 --out "$prefix.json") ;;
    steady500:*)      cmd=("${rw[@]}" --trace --workers 16 --store-warmup-seconds 12 --mode steady --calls 16 --seconds 30 --rate 500) ;;
    delayed100:bullmq) cmd=(node bench-delayed.mjs --concurrency 16 --redis "$address" --mode steady --calls 16 --seconds 30 --rate 100 --out "$prefix.json") ;;
    delayed100:*)     cmd=("${rw[@]}" --trace --workers 16 --store-warmup-seconds 12 --mode steady --calls 16 --seconds 30 --rate 100 --delay-ms 5000) ;;
    drain:bullmq)     cmd=(node bench-base.mjs --redis "$address" --mode drain --n 20000 --concurrency 16 --out "$prefix.json") ;;
    drain:*)          cmd=("${rw[@]}" --mode drain --n 20000 --workers 16 --calls 1) ;;
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
