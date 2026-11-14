#!/usr/bin/env bash
# Runwick vs Laravel's database queue driver on the same database engine, alternating arms (order flips each round), fresh database per run.
#   pairs.sh <Runwick.Workloads.dll> <out-dir>      env: LARAVEL_ROUNDS (5), LARAVEL_DBS ("sqlite pg"), LARAVEL_TESTS ("drain steady-40 steady-100"),
#   LARAVEL_DRAIN_N (20000), RUNWICK_TEST_PG_IMAGE (pinned PostgreSQL image, required when "pg" is in LARAVEL_DBS)
# Laravel runs natively on PHP 8.3 for PostgreSQL and in the pinned php:8.4-cli image for SQLite (see below).
# Both arms: 16 workers, 1 KiB no-op jobs. SQLite: WAL + synchronous FULL (both). PostgreSQL: a fresh container per run, server defaults
# (fsync and synchronous_commit on), max_connections 200. Run laravel/setup.sh once first. Timing runs one at a time; keep the host otherwise idle.
set -euo pipefail
dll="$(realpath "$1")"; here="$(cd "$(dirname "$0")" && pwd)"; out="$(mkdir -p "$2" && realpath "$2")"
export PATH="/root/.dotnet:$PATH" DOTNET_ROOT=/root/.dotnet
rounds="${LARAVEL_ROUNDS:-5}"; n="${LARAVEL_DRAIN_N:-20000}"
run() (
  round=$1 db=$2 test=$3 arm=$4
  prefix="$out/r$round-$db-$test-$arm"; fixture=
  trap '[[ -z "$fixture" ]] || docker rm -fv "$fixture" >/dev/null 2>&1 || true' EXIT
  if [[ "$db" == pg ]]; then
    fixture="laravel-pair-$$-$round-$test-$arm"
    docker run --pull=never -d --name "$fixture" -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=pg "${RUNWICK_TEST_PG_IMAGE:?}" -c max_connections=200 > "$prefix.container"
    for _ in $(seq 1 120); do [[ "$(docker logs "$fixture" 2>&1 | grep -c 'ready to accept connections')" -ge 2 ]] && break; sleep .5; done  # the image restarts once after init
    port="$(docker port "$fixture" 5432/tcp | head -1)"; port="${port##*:}"
  fi
  args=(--mode "${test%%-*}" --workers 16)
  if [[ "$test" == drain ]]; then args+=(--n "$n"); else args+=(--rate "${test#steady-}" --seconds 30); fi
  uptime > "$prefix.load"
  if [[ "$arm" == runwick ]]; then
    c=(dotnet "$dll" bench --trace --calls "$([[ "$test" == drain ]] && echo 8 || echo 16)")
    if [[ "$db" == pg ]]; then c+=(--store postgres --conn "Host=127.0.0.1;Port=$port;Username=postgres;Password=pg;Database=postgres"); else c+=(--store sqlite); fi
  else
    c=(python3 "$here/bench.py")
    # SQLite needs PHP 8.4: Laravel applies transaction_mode (BEGIN IMMEDIATE) only from 8.4, and bench.py refuses to run SQLite without it.
    if [[ "$db" == pg ]]; then c+=(--db pg --pg "127.0.0.1:$port"); else c+=(--db sqlite --php-image "${LARAVEL_PHP_IMAGE:-php@sha256:d63e8b80cd4c6cfd01cfa7c910522e299cdc7ca1a2b6d6563db48df3c17e6c55}"); fi
  fi
  "${c[@]}" "${args[@]}" --out "$prefix.json" > "$prefix.log" 2>&1
  if [[ "$test" == drain ]]; then jq -e '(.finished // .jobs) == .jobs and (.failed // 0) == 0' "$prefix.json" >/dev/null
  else jq -e '.inserted == .completed and (.failed // 0) == 0' "$prefix.json" >/dev/null; fi
)
for round in $(seq 1 "$rounds"); do
  for db in ${LARAVEL_DBS:-sqlite pg}; do
    for test in ${LARAVEL_TESTS:-drain steady-40 steady-100}; do
      order=(runwick laravel); if (( round % 2 == 0 )); then order=(laravel runwick); fi
      for arm in "${order[@]}"; do
        echo "$(date -u +%T) round $round $db $test $arm"
        run "$round" "$db" "$test" "$arm" || echo "  FAILED $round $db $test $arm (see $out/r$round-$db-$test-$arm.log)"
      done
    done
  done
done
python3 "$here/summarize.py" "$out"
