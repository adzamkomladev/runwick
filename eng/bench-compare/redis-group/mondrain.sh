#!/usr/bin/env bash
# usage: mondrain.sh <bin-dir> <outprefix> [bench flags...]  -- a small drain with MONITOR on, to count the Redis commands each script issues. Hold /tmp/runwick-bench.lock (flock) around it.
set -uo pipefail
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
here="$(cd "$(dirname "$0")" && pwd)"
bin=$1; out=$2; shift 2
name="rwcost-mon-$$"
docker run --pull=never -d --name "$name" -p 127.0.0.1::6379 redis:8.2-alpine redis-server --appendonly yes --appendfsync always --aof-load-truncated no --maxmemory-policy noeviction --save '' >/dev/null
for i in $(seq 1 60); do [[ "$(docker exec "$name" redis-cli ping 2>/dev/null)" == PONG ]] && break; sleep .25; done
addr="$(docker port "$name" 6379/tcp | head -1 | sed 's/0.0.0.0/127.0.0.1/')"
docker exec "$name" redis-cli MONITOR > "$out.mon" &
mp=$!
sleep 1
"$bin/Runwick.Workloads" bench --store redis --redis "$addr" --out "$out.json" --mode drain --n ${N:-2000} --workers 16 --calls 1 "$@" > "$out.log" 2>&1
sleep 1
kill $mp; wait $mp 2>/dev/null
docker rm -fv "$name" >/dev/null
python3 "$here/monparse.py" "$out.mon" ${N:-2000} > "$out.cmds"
