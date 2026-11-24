#!/bin/bash
# Redis claims-in-flight sweep: 1..4, finish cap 32 (default), everything else as the finish-batch experiment. Usage: run.sh <outdir>
S=$1; cd /root/work/Spindle-exp-finish64
one() { # cif tag
  docker exec exp-redis redis-cli FLUSHALL >/dev/null
  DOTNET_gcServer=1 DOTNET_PROCESSOR_COUNT=4 timeout 600 dotnet tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll bench \
    --store redis --redis 127.0.0.1:56393 --mode drain --n 20000 --workers 16 --calls 1 --buffered-finish --claim-ahead 16 --claims-in-flight $1 \
    --out $S/$2.json > $S/$2.log 2>&1; echo "$2 rc=$?"
}
i=0
for order in "1 2 3 4" "4 3 2 1" "2 4 1 3" "3 1 4 2"; do i=$((i+1)); for c in $order; do one $c c$c-p$i; done; done
