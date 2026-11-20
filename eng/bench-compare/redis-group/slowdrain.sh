#!/usr/bin/env bash
# usage: slowdrain.sh <bin-dir> <outprefix> [bench flags...]  -- one drain with SLOWLOG capturing every command. Hold /tmp/runwick-bench.lock (flock) around it.
set -uo pipefail
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
here="$(cd "$(dirname "$0")" && pwd)"
bin=$1; out=$2; shift 2
name="${NAME_PREFIX:-rwcost}-$$"   # NAME_PREFIX: container name prefix (default rwcost)
mounts=(); [[ "${FSYNC:-disk}" == tmpfs ]] && mounts=(--mount type=tmpfs,destination=/data,tmpfs-mode=1777)
docker run --pull=never -d --name "$name" "${mounts[@]}" -p 127.0.0.1::6379 ${IMAGE:-redis:8.2-alpine} redis-server --appendonly yes --appendfsync always --aof-load-truncated no --maxmemory-policy noeviction --save '' --auto-aof-rewrite-percentage 0 --slowlog-log-slower-than ${SLOW:-0} --slowlog-max-len 400000 >/dev/null
for i in $(seq 1 60); do [[ "$(docker exec "$name" redis-cli ping 2>/dev/null)" == PONG ]] && break; sleep .25; done
addr="$(docker port "$name" 6379/tcp | head -1 | sed 's/0.0.0.0/127.0.0.1/')"
uptime > "$out.load"
"$bin/Runwick.Workloads" bench --store redis --redis "$addr" --out "$out.json" --mode drain --n ${N:-20000} --workers 16 --calls 1 "$@" > "$out.log" 2>&1
echo "exit $?" >> "$out.log"
uptime >> "$out.load"
docker exec "$name" redis-cli INFO all > "$out.redis"
python3 "$here/slowparse.py" "$(docker port "$name" 6379/tcp | head -1 | sed 's/.*://')" "${N:-20000}" > "$out.slow"
docker rm -fv "$name" >/dev/null
