#!/bin/bash
# Claim-ahead 64 (limit raised on the branch), 2 claims in flight: finish cap 32 vs 64. Usage: run-combo.sh <outdir>
S=$1; cd /root/work/Spindle-exp-finish64
one() { # finishcap tag
  docker exec exp-redis redis-cli FLUSHALL >/dev/null
  DOTNET_gcServer=1 DOTNET_PROCESSOR_COUNT=4 RUNWICK_EXPERIMENT_FINISH_BATCH=$1 timeout 600 dotnet tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll bench \
    --store redis --redis 127.0.0.1:56393 --mode drain --n 20000 --workers 16 --calls 1 --buffered-finish --claim-ahead 64 --claims-in-flight 2 \
    --out $S/$2.json > $S/$2.log 2>&1; echo "$2 rc=$?"
}
i=0
for order in "32 64" "64 32" "32 64" "64 32"; do i=$((i+1)); for c in $order; do one $c f$c-ca64-p$i; done; done
