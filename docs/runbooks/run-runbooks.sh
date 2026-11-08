#!/usr/bin/env bash
# Runs docs/runbooks/upgrade.md and restore.md against a scratch PostgreSQL in the container runwick-pg (port 55499) and prints each command with its output.
# Needs: dotnet on PATH, `dotnet build src/Runwick.Cli samples/Runwick.Samples.Host` done, docker. Output of the 2026-11-08 run: transcript-2026-11-08.txt.
set -u
W=$(cd "$(dirname "$0")/../.." && pwd)
export DOTNET_NOLOGO=1
CLI="dotnet $W/src/Runwick.Cli/bin/Debug/net10.0/Runwick.Cli.dll"
HOST="dotnet $W/samples/Runwick.Samples.Host/bin/Debug/net10.0/Runwick.Samples.Host.dll"
PSQL="docker exec -i runwick-pg psql -U postgres -v ON_ERROR_STOP=1 -q"
run() { echo; echo "\$ $*"; "$@" 2>&1; echo "[exit $?]"; }

$PSQL -c "DROP DATABASE IF EXISTS rw_runbook WITH (FORCE)" -c "DROP DATABASE IF EXISTS rw_runbook_restored WITH (FORCE)" -c "DROP DATABASE IF EXISTS rw_runbook_clone WITH (FORCE)" >/dev/null
$PSQL -c "CREATE DATABASE rw_runbook" >/dev/null
export RUNWICK_STORE='Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=rw_runbook'
export RUNWICK_CONN="$RUNWICK_STORE"

echo "### interrupted first install: kill the migrator's session once the ledger has 20 rows"
( for i in $(seq 1 400); do
    n=$(docker exec runwick-pg psql -U postgres -d rw_runbook -Atc "select count(*) from runwick.schema_migrations" 2>/dev/null || echo 0)
    if [ "${n:-0}" -ge 20 ]; then docker exec runwick-pg psql -U postgres -Atc "select pg_terminate_backend(pid) from pg_stat_activity where application_name='runwick:migrate'" >/dev/null; break; fi
    sleep 0.05
  done ) &
run $CLI --agent migrate --provider postgres --connection-env RUNWICK_STORE | cut -c1-300
wait
echo; $PSQL -d rw_runbook -Atc "select 'revision after interruption: ' || schema_revision || ', mode ' || migration_mode from runwick.store_metadata"

echo; echo "### upgrade.md step 4: migrate again (resumes from the ledger)"
run $CLI migrate --provider postgres --connection-env RUNWICK_STORE | cut -c1-300
run $CLI migrate --provider postgres --connection-env RUNWICK_STORE

echo; echo "### the application runs 3 jobs (registers application 'samples')"
run $HOST --dispatch 3

echo; echo "### upgrade.md step 1: back up"
docker exec runwick-pg pg_dump -U postgres -Fc rw_runbook > /tmp/rw_runbook.dump && ls -l /tmp/rw_runbook.dump
echo "\$ pg_dump -Fc rw_runbook  [exit $?]"

echo; echo "### upgrade.md step 2: status, drain, status"
run $CLI store status --provider postgres --connection-env RUNWICK_STORE
run $CLI store admission --mode maintenance --provider postgres --connection-env RUNWICK_STORE
run $CLI store admission --mode drain --provider postgres --connection-env RUNWICK_STORE
echo; echo "### while drained: dispatch is refused, nothing runs"
run $HOST --dispatch 1
run $CLI store status --provider postgres --connection-env RUNWICK_STORE

echo; echo "### upgrade.md step 6: reopen, new binary works"
run $CLI store admission --mode open --provider postgres --connection-env RUNWICK_STORE
run $HOST --dispatch 2

echo; echo "### restore.md: restore the dump as a different database and give it a new identity"
docker exec -i runwick-pg createdb -U postgres rw_runbook_restored
docker exec -i runwick-pg pg_restore -U postgres -d rw_runbook_restored --no-owner < /tmp/rw_runbook.dump; echo "\$ pg_restore  [exit $?]"
export RUNWICK_RESTORED='Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=rw_runbook_restored'
run $CLI store status --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store reidentify --confirm --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store admission --mode drain --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store admission --mode maintenance --provider postgres --connection-env RUNWICK_RESTORED
echo; echo "### the dump holds the presence rows of the processes that were running; wait for them to expire (they prove nothing)"
for i in $(seq 1 120); do n=$(docker exec runwick-pg psql -U postgres -d rw_runbook_restored -Atc "select count(*) from runwick.worker_instances where expires_at > clock_timestamp()"); [ "$n" = 0 ] && break; sleep 1; done; echo "live presence rows after ${i}s: $n"
run $CLI store status --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store reidentify --confirm --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store admission --mode open --provider postgres --connection-env RUNWICK_RESTORED
run $CLI store status --provider postgres --connection-env RUNWICK_RESTORED
echo; echo "### the original keeps its own identity"
run $CLI store status --provider postgres --connection-env RUNWICK_STORE
echo; echo "### a process holding the restored database's OLD identity would be refused; a new process learns the new one"
echo; echo "### an application run against the restored database works under its new identity"
RUNWICK_CONN="$RUNWICK_RESTORED" run $HOST --dispatch 1

echo; echo "### SQLite: the same commands on a file"
D=$(mktemp -d); export SQ="Data Source=$D/runwick.db" SQ2="Data Source=$D/copy.db"
run $CLI migrate --provider sqlite --connection-env SQ
run $CLI store status --provider sqlite --connection-env SQ
cp "$D/runwick.db" "$D/copy.db"; echo "\$ cp runwick.db copy.db  (the store is idle: no process has the file open)"
run $CLI store reidentify --confirm --provider sqlite --connection-env SQ2
run $CLI store status --provider sqlite --connection-env SQ
