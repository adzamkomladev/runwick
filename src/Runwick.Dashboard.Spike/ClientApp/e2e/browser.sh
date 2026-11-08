#!/usr/bin/env bash
# SP-023 browser coverage with playwright-cli: Chromium runs every step; Firefox and WebKit run the smoke step.
# Usage, after `dotnet build samples/Runwick.Dashboard.SpikeHost`:  e2e/browser.sh [chromium] [firefox] [webkit]
# Each *.js file here is one `playwright-cli run-code` function that returns its pass/FAIL lines.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../../.." && pwd)
dll=$repo/samples/Runwick.Dashboard.SpikeHost/bin/Debug/net10.0/Runwick.Dashboard.SpikeHost.dll
port=5791 # the *.js files use this port
work=$(mktemp -d)
host=
results=()

start_host() {
  ASPNETCORE_URLS=http://127.0.0.1:$port dotnet "$dll" "$@" >/dev/null 2>&1 &
  host=$!
  for _ in $(seq 100); do curl -sf "http://127.0.0.1:$port/ops/healthz" >/dev/null && return; sleep 0.1; done
  echo "host did not start" >&2
  exit 2
}
stop_host() { if [ -n "$host" ]; then kill "$host" 2>/dev/null; wait "$host" 2>/dev/null; host=; fi; }
cleanup() { stop_host; for s in "${sessions[@]}"; do (cd "$work" && playwright-cli -s="$s" close >/dev/null 2>&1); done; rm -rf "$work"; }
sessions=()
trap cleanup EXIT

# One session per browser and user; the Basic credentials go in the context options.
open_session() { # session browserName user
  local launch='{}'
  [ "$2" = chromium ] && launch='{"channel":"chromium"}' # the full build in new headless mode
  printf '{"browser":{"browserName":"%s","launchOptions":%s,"contextOptions":{"httpCredentials":{"username":"%s","password":"pw-%s"}}}}' \
    "$2" "$launch" "$3" "$3" > "$work/$1.json"
  sessions+=("$1")
  (cd "$work" && playwright-cli -s="$1" open --config="$work/$1.json" > "$work/$1.out" 2>&1) && ! grep -q '^### Error' "$work/$1.out"
}
run() { # session file
  local out
  out=$(cd "$work" && playwright-cli -s="$1" --raw run-code --filename="$here/$2" 2>&1)
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
  if ! open_session "$browser" "$browser" alice; then
    results+=("FAIL  $browser: did not launch: $(grep -m1 -oE 'Host system is missing dependencies[^║]*|Executable doesn.t exist[^ ]*' "$work/$browser.out" || echo 'see playwright-cli open output')")
    continue
  fi
  if [ "$browser" = chromium ]; then
    run chromium full.js
    stop_host
    start_host --build v2 # stands in for a deploy
    run chromium upgrade.js
    open_session viewer chromium victor && run viewer viewer.js
  else
    run "$browser" smoke.js
  fi
done

printf '%s\n' "${results[@]}"
! printf '%s\n' "${results[@]}" | grep -q '^FAIL'
