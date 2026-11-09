#!/usr/bin/env bash
# SP-032 smoke test (19 §1): publish runwick as a self-contained single file, then run it with no SDK involved:
# version, help, and a SQLite migrate + doctor (proves the native SQLite library is extracted and loads).
# Usage: eng/smoke-cli-publish.sh [linux-x64|win-x64]   (default linux-x64; win-x64 runs from Git Bash on Windows)
set -euo pipefail
cd "$(dirname "$0")/.."
rid=${1:-linux-x64}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
dotnet publish src/Runwick.Cli -c Release -r "$rid" -o "$out" --nologo -v quiet
bin="$out/Runwick.Cli"
[[ $rid == win-* ]] && bin="$bin.exe"
test "$(find "$out" -maxdepth 1 -name '*.dll' | wc -l)" -eq 0 || { echo "not single-file: dlls next to the binary"; exit 1; }
"$bin" version | grep -q '^version: '
"$bin" --help | grep -q '^runwick <command>'
db="$out/smoke.db"
if command -v cygpath >/dev/null; then db=$(cygpath -m "$db"); fi # Git Bash: give the Windows binary a Windows path
export SMOKE_CS="Data Source=$db"
"$bin" --agent migrate --provider sqlite --connection-env SMOKE_CS | grep -q '"ok":true'
"$bin" --agent doctor --provider sqlite --connection-env SMOKE_CS | grep -q '"ok":true'
echo "$rid single-file publish smoke OK ($(du -h "$bin" | cut -f1))"
