#!/usr/bin/env bash
# SP-032 smoke test (19 §1): publish runwick as a self-contained single file for linux-x64, then run it with no SDK
# involved: version, help, and a SQLite migrate + doctor (proves the native SQLite library is extracted and loads).
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
dotnet publish src/Runwick.Cli -c Release -r linux-x64 -o "$out" --nologo -v quiet
bin="$out/Runwick.Cli"
test "$(find "$out" -maxdepth 1 -name '*.dll' | wc -l)" -eq 0 || { echo "not single-file: dlls next to the binary"; exit 1; }
"$bin" version | grep -q '^version: '
"$bin" --help | grep -q '^runwick <command>'
export SMOKE_CS="Data Source=$out/smoke.db"
"$bin" --agent migrate --provider sqlite --connection-env SMOKE_CS | grep -q '"ok":true'
"$bin" --agent doctor --provider sqlite --connection-env SMOKE_CS | grep -q '"ok":true'
echo "single-file publish smoke OK ($(du -h "$bin" | cut -f1))"
