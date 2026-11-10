# Release checklist (v1)

Nothing here publishes anything. These are the steps between the current local state and "v1 published", which is the owner's step.

Local checks (all pass at the time of writing, except where noted):

1. `eng/check-package-graph.sh`: allowed project and package edges.
2. `eng/check-release.sh`: packs all 13 packages (and symbols) into `artifacts/packages` with 0 warnings and inspects each one: metadata, licence, README, dependency list, `net10.0` only, no unexpected files.
3. `eng/check-licenses.sh`: every NuGet and bundled npm package is on `eng/license-allowlist.txt`.
4. `dotnet test tests/Runwick.Core.Tests --filter Item=SP-030`: documented defaults equal the code.
5. `eng/smoke-package-install.sh [linux-x64|win-x64]`: the packed packages install into a fresh project and run jobs on SQLite (after step 2).
6. `eng/smoke-cli-publish.sh [linux-x64|win-x64]`: the CLI publishes as one self-contained binary and runs.

Steps 5 and 6 have passed on linux-x64 only; win-x64 is owed on a Windows machine (steps in `docs/reports/sp-030.md`).

Owner decisions and actions before publishing:

1. Licence: MIT, copyright holder Komla Adzam (set 2026-11-09).
2. Repository URL: https://github.com/adzamkomladev/runwick (set 2026-11-09). Build the release packages in a clone whose origin is that repository, so the SDK's built-in SourceLink links symbols to the commit.
3. Version: `0.1.1` (set 2026-11-10) in `Directory.Build.props`. Publishing is the `release` workflow: push a tag `v<version>` that equals that version (steps in `.github/workflows/release.yml`).
4. Check that the package name `Runwick` is free on NuGet (spec 00 §7 asks for this).
5. Confirm the SQLite public-domain point in [licenses.md](licenses.md).
6. Finish the qualification that G-6 depends on: the Windows and Mac checks, the SP-029 reference run and 24-hour soak, the Redis REDIS-01/07 work, and the G-3 and G-5 reviews. The current list is the G-6 table in `docs/reports/sp-030.md`.
7. Sign the packages if you use package signing; push with your own API key.
