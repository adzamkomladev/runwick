# Platform, language and package foundation

Status: planning decision/proposal, 2026-10-29. The user selected **.NET 10 and newer, with current/newer C#**. That platform floor is settled. Stable-release interpretation, build policy and package topology below are engineering proposals; no projects, SDK installation or runtime implementation were created.

## 1. Platform decision

All initial runtime/contract packages target `net10.0`. Support starts at .NET 10; do not add .NET 8/9, .NET Framework or .NET Standard targets. This applies equally to the lightweight contracts package, providers and optional web integration. Modern-only targeting is consistent with Microsoft's guidance for libraries intended for modern .NET applications. [Targeting guidance](https://learn.microsoft.com/en-us/dotnet/standard/library-guidance/cross-platform-targeting)

Interpret “latest C# and newer versions” as stable language releases, advancing as compatible stable platform releases arrive. As inspected on this date, .NET 10 ships C# 14; the .NET 11 download is a release candidate carrying C# 15. Start with C# 14 for `net10.0`; the preview/RC combination can have a compatibility test lane but is not the initial shipping dependency. [C# 14](https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-14), [.NET 11 downloads](https://dotnet.microsoft.com/en-us/download/dotnet/11.0)

The language/runtime relationship matters when adopting newer syntax. Microsoft's supported pairing is C# 14 on .NET 10+, C# 15 on .NET 11+; using a language newer than the target framework's associated version is unsupported. Therefore a newer compiler installation does not authorize C# 15 across a `net10.0` source build. [Language versioning](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/language-versioning)

Forward policy:

1. Keep the .NET 10 asset while it is a supported Runwick target; run consumer/runtime tests on each newer stable .NET version added to the advertised matrix.
2. Add a newer target only when it enables useful platform-specific APIs/implementation or is needed for dependency compatibility. Do not multiply targets solely to list runtime versions.
3. Use the latest stable supported language per target. Shared source must compile for every selected target; newer-only syntax belongs in target-specific source when needed.
4. Raising the minimum above .NET 10 is an explicit breaking release decision. It is not an automatic side effect of an SDK update. Future-target certification also depends on providers and optional dashboard integration.
5. “10+” is the support direction, not proof that every future runtime is already tested. Document exact tested versions and platform lifecycle limits for each release; no promise of indefinite servicing for an out-of-support runtime.

## 2. Reproducible build policy

At SP-002, select the latest stable SDK compatible with the chosen targets and dependencies, then commit its full version in global.json. Proposed release settings: `allowPrerelease: false`, `rollForward: disable`; CI installs that exact SDK. Servicing upgrades are regular explicit changes with CI validation, not a permanent freeze. Record the SDK/compiler versions in build artifacts. SDK choice and runtime targeting are separate controls. [global.json](https://learn.microsoft.com/en-us/dotnet/core/tools/global-json)

Set an explicit C# version for each target, initially `14.0`, rather than floating `LangVersion=latest`. Microsoft warns that latest varies with the installed compiler and can enable incompatible features. This implements the user's latest-language direction through intentional upgrades with reproducible builds. [Language configuration](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/configure-language-version)

Initial project defaults: nullable reference types enabled, deterministic builds, central package-version management, pinned/restored dependency graph and reviewed analyzer settings. Release CI fails build/analyzer issues selected as errors; it does not suppress serialization, trimming or platform warnings merely to turn a job green. Exact provider/adapter versions are selected and verified before their projects are created; no floating package versions.

No SDK patch number is frozen in this planning document because implementation has not begun. Recheck the stable release at project creation and record the actual version. This prevents a researched version from silently becoming an obsolete build prerequisite.

## 3. Concrete package proposal

| Package | Contents | Direct Runwick dependencies |
|---|---|---|
| Runwick.Contracts | IQueueJob, IJobHandler<T>, optional IJobContext, job metadata attributes and context value types | None |
| Runwick | Dispatcher/preparation/recovery APIs, registration/configuration, worker engine, middleware, scheduler/capacity modules, management application services and storage extension contracts | Runwick.Contracts |
| Runwick.Postgres | PostgreSQL commands/projections, migrations, profile checks and provider-specific transaction staging | Runwick |
| Runwick.Sqlite | SQLite commands/projections, migrations, bounded executor and transaction staging | Runwick |
| Runwick.Redis | Redis commands/projections, publication/recovery and persistence-profile checks | Runwick |
| Runwick.AspNetCore | Auth/policy adapters, management endpoints, antiforgery and HTTP result mapping | Runwick |
| Runwick.Dashboard | Inertia server adapter integration, page projections and prebuilt frontend assets | Runwick.AspNetCore |
| Runwick.Outbox | Optional relay, immutable source intent and delivery contracts | Runwick |
| Runwick.Outbox.Postgres | Source staging and relay persistence on Npgsql | Runwick.Outbox |
| Runwick.Outbox.Sqlite | Source staging and relay persistence on Microsoft.Data.Sqlite | Runwick.Outbox |
| Runwick.Testing | Public application test helpers for registration, dispatch capture and controlled handler invocation | Runwick |

Names are proposals; registry availability has not been checked. The decision register D-02 remains reviewable. This refines the earlier optional abstractions boundary into a concrete Runwick.Contracts recommendation: an application-owned shared job-contract project should not need worker-hosting, database or ASP.NET dependencies. Handler/context interfaces in that package remain plain contracts, without container registration or HttpContext types.

The main Runwick package transitively includes Contracts, so ordinary applications need no extra direct reference. A producer adds Runwick and one storage provider; a worker adds the same packages and its handlers. Installing the engine assembly does not start it: AddWorker/AddScheduler remain explicit. Avoid separate worker/scheduler/scaler packages while they share one engine/versioning contract.

Provider dependencies point toward the core; core never references a concrete provider. The driver's service-provider interface must be accessible across assemblies, with an explicit versioned extension contract rather than pretending all implementation interfaces can be C# internal. Initially ship core/provider releases together and negotiate semantic operation capabilities at startup. Exact NuGet compatibility ranges follow D-13; interfaces being public does not establish unlimited third-party driver compatibility.

Core emits through .NET diagnostic primitives without an exporter dependency. Hosts select telemetry integration/exporters. A separate Runwick telemetry package is unnecessary unless a concrete host convenience integration later justifies it. The [selected optional outbox packages](transactional-outbox.md) never enter every installation's dependency graph.

Runwick.Testing helpers must label simulated storage/dispatch accurately. The repository's independent reference model, container fixtures, fault injectors and durability certification suite are internal test projects, not production APIs or a certified in-memory driver.

## 4. Installation and role matrix

| Consumer | Required package set | Enabled roles |
|---|---|---|
| Shared application job definitions | Runwick.Contracts | None |
| Producer application | Runwick + selected provider | Producer |
| Embedded application | Runwick + selected provider + handler project | Producer and worker; scheduler only if configured |
| External worker | Runwick + selected provider + handler project | Worker; scheduler independently configurable |
| Management API host | Runwick.AspNetCore + selected provider | Management; no worker requirement |
| Embedded/standalone dashboard | Runwick.Dashboard + selected provider | Management/dashboard; other roles explicit |
| Application test project | Runwick.Testing and relevant application packages | Test helpers only |

The web packages may require ASP.NET Core; Contracts/core/providers must not bring in Microsoft.AspNetCore.App. Frontend assets are built during package production and embedded/versioned with the dashboard; consumers do not need Node/npm. PostgreSQL, SQLite and Redis client dependencies are isolated to their respective providers. No EF dependency is required.

An application can select different providers in independently configured processes, but processes sharing one logical queue application must agree on its store identity/profile. Merely installing multiple provider packages does not create replication, migration or cross-store transaction support.

## 5. Runtime, trimming and platform claims

Initial certification targets ordinary .NET hosting on explicitly tested operating systems and architectures. Define the exact OS/architecture/container matrix with provider selection; do not infer it from a successful net10.0 compilation. Test SQLite native assets and dashboard path/proxy behavior on the environments actually advertised.

The existing assembly-discovery, payload serialization and cached DI factory proposals require a trimming/Native AOT review. Provide explicit registration paths and serializer metadata hooks where feasible; do not label the whole library AOT-safe until published application tests pass for the selected providers and roles. This does not introduce an AOT requirement or change the user-selected platform floor.

New language features do not change durable names, wire hashes, job-state meanings or ownership authority. SDK/provider upgrades must preserve published wire vectors and supported command/schema behavior. Never encode a compiler-generated CLR name as durable job identity.

## 6. Validation and decision status

SP-002/027 acceptance additions:

- Build all shipping projects for their declared targets using the pinned SDK and exact restore graph.
- Compile a shared contract project referencing Contracts alone, and attribute/fluent producer/worker examples.
- Inspect packed dependency graphs: no ASP.NET/provider/EF dependencies in Contracts; no dashboard/provider implementations in core.
- Verify producer-only registration starts no worker/scheduler; standalone management starts neither implicitly.
- Install/run samples on each advertised stable runtime/OS/architecture combination; a future runtime remains unverified until added.
- Exercise the selected dashboard/provider dependencies under net10.0; an incompatible adapter cannot force a silent minimum-version bump.
- After SDK/language changes, rerun wire vectors and compatibility samples in addition to compilation; record any target-specific API differences.

D-01: platform floor and latest-Npgsql direction accepted by user; language/build policy specified here. The [provider dependency plan](provider-dependency-plan.md) selects the current stable Npgsql 10.0.3 baseline and proposes other provider versions and certification targets. SDK/restore/OS/AOT evidence remains open. D-02: package recommendation drafted, not approved. Identity/serialization, timing and [PostgreSQL integration](postgres-schema-and-operations.md)/[upgrades](postgres-upgrades-and-compatibility.md) develop D-03–D-06/D-13; [capacity defaults/budgets](capacity-defaults-and-budgets.md) develops D-07 and the [scheduler packet](scheduler-evaluator-and-defaults.md) develops D-08. Cronos/NodaTime remain internal core-module dependencies; Contracts exposes neither. The [final baseline](../architecture-baseline.md) completes all six remaining planning items; G-0 review remains open. No runtime implementation has started.
