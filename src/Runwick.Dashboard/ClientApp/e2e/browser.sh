#!/usr/bin/env bash
# SP-024 browser runs with playwright-cli: Chromium runs every flow; Firefox and WebKit run the smoke flow.
# Usage, after `dotnet build samples/Runwick.Dashboard.Host`:
#   RUNWICK_DASH_PG='Host=...;Database=...' e2e/browser.sh [chromium] [firefox] [webkit]
# RUNWICK_DASH_PORT sets the port (default 5791); DOTNET the dotnet executable (default: dotnet on PATH). Each *.js file is one `playwright-cli run-code` function that returns pass/FAIL lines.
set -uo pipefail
PLAYWRIGHT_CLI_VERSION=0.1.22 # the version these flows were written against
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../../.." && pwd)
dll=$repo/samples/Runwick.Dashboard.Host/bin/Debug/net10.0/Runwick.Dashboard.Host.dll
port=${RUNWICK_DASH_PORT:-5791}
work=$(mktemp -d)
host=
results=()
sessions=()

have=$(playwright-cli --version 2>/dev/null)
if [ "$have" != "$PLAYWRIGHT_CLI_VERSION" ]; then
  echo "playwright-cli $PLAYWRIGHT_CLI_VERSION required (found '${have:-none}'): npm i -g @playwright/cli@$PLAYWRIGHT_CLI_VERSION" >&2
  exit 2
fi
: "${RUNWICK_DASH_PG:?set RUNWICK_DASH_PG to a PostgreSQL connection string}"

# The flows name port 5791; copy them with the chosen port.
for f in "$here"/*.js; do sed "s/127\.0\.0\.1:5791/127.0.0.1:$port/g" "$f" > "$work/$(basename "$f")"; done

start_host() {
  RUNWICK_DASH_PORT=$port "${DOTNET:-dotnet}" "$dll" "$@" > "$work/host.log" 2>&1 &
  host=$!
  for _ in $(seq 1200); do grep -q '^ready' "$work/host.log" 2>/dev/null && return; sleep 0.1; done
  echo "host did not start:" >&2
  cat "$work/host.log" >&2
  exit 2
}
stop_host() { if [ -n "$host" ]; then kill "$host" 2>/dev/null; wait "$host" 2>/dev/null; host=; fi; }
cleanup() { stop_host; for s in "${sessions[@]}"; do (cd "$work" && playwright-cli -s="$s" close >/dev/null 2>&1); done; rm -rf "$work"; }
trap cleanup EXIT

open_session() { # session browserName
  local launch='{}'
  [ "$2" = chromium ] && launch='{"channel":"chromium"}' # the full build in new headless mode
  printf '{"browser":{"browserName":"%s","launchOptions":%s}}' "$2" "$launch" > "$work/$1.json"
  sessions+=("$1")
  (cd "$work" && playwright-cli -s="$1" open --config="$work/$1.json" > "$work/$1.out" 2>&1) && ! grep -q '^### Error' "$work/$1.out"
}
run() { # session file
  local out
  out=$(cd "$work" && playwright-cli -s="$1" --raw run-code --filename="$work/$2" 2>&1)
  if [[ $out == \[* ]]; then
    mapfile -t lines < <(node -e 'for (const l of JSON.parse(process.argv[1])) console.log(l)' "$out")
    results+=("${lines[@]}")
  else
    results+=("FAIL  $1 $2: ${out%%$'\n'*}")
  fi
}

browsers=("$@")
[ $# -eq 0 ] && browsers=(chromium firefox webkit)
start_host
for browser in "${browsers[@]}"; do
  if ! open_session "$browser" "$browser"; then
    results+=("FAIL  $browser: did not launch: $(grep -m1 -oE 'Host system is missing dependencies[^║]*|Executable doesn.t exist[^ ]*' "$work/$browser.out" || echo 'see playwright-cli open output')")
    continue
  fi
  if [ "$browser" = chromium ]; then
    run chromium full.js
    stop_host
    start_host --build v2 # stands in for a deploy
    run chromium upgrade.js
    open_session viewer chromium && run viewer viewer.js
    stop_host
    start_host # fresh data for the smoke runs
  else
    run "$browser" smoke.js
  fi
done

printf '%s\n' "${results[@]}"
! printf '%s\n' "${results[@]}" | grep -q '^FAIL'
