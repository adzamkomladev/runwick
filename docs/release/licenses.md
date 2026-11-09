# Licences and third-party notices

Runwick itself is MIT-licensed (`LICENSE` at the repository root; every package declares the SPDX expression `MIT`). The holder line "Runwick contributors" is a placeholder for the owner to confirm before publishing.

## How it is checked

`eng/check-licenses.sh` reads the restore closure of every shipped project (`src/*/packages.lock.json`), finds each package's licence in its nuspec in the NuGet cache, and reads the licence of each production npm package in `src/Runwick.Dashboard/ClientApp/package-lock.json`. It fails on any licence not listed in `eng/license-allowlist.txt`, and also if a package has only a licence URL or a licence file instead of an SPDX expression. Run `dotnet restore` first. The check prints every package, so its output is the full list.

## Allow-list

| Licence | Why allowed |
|---|---|
| MIT | permissive, attribution only |
| Apache-2.0 | permissive, attribution and NOTICE handling |
| BSD-2-Clause, BSD-3-Clause | permissive, attribution only |
| PostgreSQL | permissive (Npgsql uses it) |
| npm: MIT | the dashboard's bundled web libraries |

Anything else (GPL, LGPL, MPL, AGPL, SSPL, "custom", missing) is not allowed automatically. Flag it for the owner; do not add it to the list to make the check pass.

## What was found (2026-11-08)

31 NuGet packages and 10 npm production packages; all are on the allow-list. Nothing needs a decision.

NuGet, grouped by licence:

- MIT: Cronos 0.13.0, Inertia.Net 1.7.0, Microsoft.Data.Sqlite and .Core 10.0.14, Microsoft.Extensions.* (Configuration, DependencyInjection, Diagnostics, FileProviders, Hosting, Logging, Options, Primitives; 10.0.0 to 10.0.14), RESPite 3.4.20, StackExchange.Redis 3.4.20, System.IO.Hashing 10.0.13.
- Apache-2.0: NodaTime 3.3.7, OpenTelemetry, OpenTelemetry.Api, OpenTelemetry.Api.ProviderBuilderExtensions 1.20.1, SQLitePCLRaw.bundle_e_sqlite3, .core, .lib.e_sqlite3, .provider.e_sqlite3 2.1.13.
- PostgreSQL: Npgsql 10.0.3.

npm (all MIT; built into the dashboard's `dist/` assets by `npm run build`): @inertiajs/core 3.9.1, @inertiajs/react 3.9.1, react 19.3.1, react-dom 19.3.1, scheduler 0.28.0, es-toolkit 1.53.2, laravel-precognition 2.1.1, @jridgewell/resolve-uri 3.1.2, @jridgewell/sourcemap-codec 1.6.0, @jridgewell/trace-mapping 0.3.31. Build-only tools (vite, @vitejs/plugin-react) are not shipped and are not checked.

## Bundled native SQLite

`Runwick.Sqlite` brings the SQLite engine (3.53.3 at the last build) through `SQLitePCLRaw.lib.e_sqlite3`. The package wrapper is Apache-2.0. SQLite itself is in the public domain (the authors dedicate it to the public domain; it has no licence conditions). The nuspec check sees only the wrapper's Apache-2.0, so the public-domain status of the engine rests on SQLite's own statement at sqlite.org/copyright.html; the owner accepted this on 2026-11-09. The same native library is the only native code Runwick ships.

## Third-party notices

`THIRD-PARTY-NOTICES.md` (repository root) holds the licence text of each bundled npm package. It is packed into `Runwick.Dashboard`, the only package that contains third-party code (React and Inertia in `dist/`); `eng/check-release.sh` fails if it is missing there. The NuGet dependencies are referenced, not copied, and carry their own notices. If the npm lock file changes, regenerate the file (it is the licence file of each production package from `npm ci --omit=dev`) and rerun `eng/check-licenses.sh`.
