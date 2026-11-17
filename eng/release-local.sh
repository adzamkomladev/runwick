#!/usr/bin/env bash
# Local release, for when GitHub Actions is unavailable (same steps as .github/workflows/release.yml, minus the full
# test run: run `dotnet test Runwick.slnx -c Release` first). Usage: NUGET_API_KEY=... bash eng/release-local.sh v0.1.4
# HEAD must be the tagged commit and the tree clean, so the packages carry that commit's SourceLink.
# Set DRY_RUN=1 to build and inspect only (no GitHub release, no push to nuget.org).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.dotnet:$PATH" DOTNET_NOLOGO=1
tag="${1:?usage: eng/release-local.sh v<version>}"
version=$(sed -n 's|.*<Version>\(.*\)</Version>.*|\1|p' Directory.Build.props)
[ "$tag" = "v$version" ] || { echo "tag $tag does not match Version $version in Directory.Build.props"; exit 1; }
[ "$(git rev-parse HEAD)" = "$(git rev-parse "$tag^{commit}")" ] || { echo "HEAD is not $tag"; exit 1; }
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "working tree has uncommitted changes"; exit 1; }
[ "${DRY_RUN:-0}" = "1" ] || [ -n "${NUGET_API_KEY:-}" ] || { echo "set NUGET_API_KEY (nuget.org key scoped to the Runwick* packages)"; exit 1; }

dotnet restore Runwick.slnx --locked-mode
dotnet build Runwick.slnx -c Release --no-restore
bash eng/check-package-graph.sh
bash eng/check-licenses.sh
bash eng/check-release.sh
[ "${DRY_RUN:-0}" = "1" ] && { echo "dry run: packages in artifacts/packages, nothing published"; exit 0; }

flags=(--generate-notes --verify-tag)
case "$version" in *-*) flags+=(--prerelease);; esac
if gh release view "$tag" >/dev/null 2>&1; then
  gh release upload "$tag" artifacts/packages/* --clobber
else
  gh release create "$tag" artifacts/packages/* --title "Runwick $tag" "${flags[@]}"
fi
# .snupkg symbols go up with their .nupkg; --skip-duplicate makes a rerun safe.
dotnet nuget push "artifacts/packages/*.nupkg" --api-key "$NUGET_API_KEY" --source https://api.nuget.org/v3/index.json --skip-duplicate
