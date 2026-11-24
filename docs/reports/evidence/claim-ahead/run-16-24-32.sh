#!/bin/bash
# Redis claim-ahead sweep: 16, 24, 32 (Runwick's maximum), 2 claims in flight, finish cap 32. Usage: run-16-24-32.sh <outdir>
S=$1; cd /root/work/Spindle-exp-finish64
one() { # ahead tag
  docker exec exp-redis redis-cli FLUSHALL >/dev/null
  DOTNET_gcServer=1 DOTNET_PROCESSOR_COUNT=4 timeout 600 dotnet tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll bench \
    --store redis --redis 127.0.0.1:56393 --mode drain --n 20000 --workers 16 --calls 1 --buffered-finish --claim-ahead $1 --claims-in-flight 2 \
    --out $S/$2.json > $S/$2.log 2>&1; echo "$2 rc=$?"
}
i=0
for order in "16 24 32" "32 24 16" "24 32 16" "16 32 24"; do i=$((i+1)); for c in $order; do one $c a$c-p$i; done; done
