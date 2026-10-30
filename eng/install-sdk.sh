#!/usr/bin/env bash
# Installs the pinned .NET SDK into $HOME/.dotnet (no root needed).
set -euo pipefail
VERSION="${RUNWICK_SDK_VERSION:-10.0.403}"
DIR="${DOTNET_INSTALL_DIR:-$HOME/.dotnet}"
script="$(mktemp)"
curl -fsSL https://dot.net/v1/dotnet-install.sh -o "$script"
if ! bash "$script" --version "$VERSION" --install-dir "$DIR" --no-path; then
  echo "SDK $VERSION unavailable; falling back to channel 10.0 GA" >&2
  bash "$script" --channel 10.0 --quality GA --install-dir "$DIR" --no-path
fi
rm -f "$script"
export PATH="$DIR:$PATH"
dotnet --version
