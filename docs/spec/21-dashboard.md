# 21 — Dashboard (SP-024)

**Sources:** [v1-delivery-plan.md](../v1-delivery-plan.md) §6 (SP-024 row), [00-overview.md](00-overview.md) §4, §6 (Dashboard role, `Runwick.Dashboard` package row and pins), [16-management.md](16-management.md) (session, permissions, limits, MGMT cases), [17-retention.md](17-retention.md) §7 (retention reads), [11-autoscaler.md](11-autoscaler.md) §6 (scaling history), [docs/reports/sp-023.md](../reports/sp-023.md) (the spike: what Inertia.Net 1.7.0 gives and what Runwick adds), `src/Runwick.Dashboard.Spike` (the spike code), [dashboard-integration-closure.md](../architecture/dashboard-integration-closure.md) (transport contract, DASH-01–07), [management-and-dashboard.md](../architecture/management-and-dashboard.md) (design, MGMT-01–12), [management-retention-defaults.md](../architecture/management-retention-defaults.md) (browser poll and request limits).

## In short

SP-024 ships `Runwick.Dashboard`: a web UI for people who run Runwick. It shows an overview, jobs and their attempts, queues, workers and capacity, schedules, scaling history, settings, retention and audit. It lets people with the right permission run the existing controls (cancel, retry, pause, and so on).

- **No second backend.** Every page reads through `IManagementSession` (16), in the same process. The dashboard adds one read (scaling history, §6). Everything else already exists.
- **Same login, same permissions.** The dashboard uses the authentication schemes, grant mapping and actor mapping of `AddRunwickManagement` (16 §5). A page or button is shown only when the caller's grants allow it, and the server checks again on every request.
- **Built on the spike.** Each dashboard mount is an Inertia.Net 1.7.0 named mount. Runwick keeps the six pieces the spike had to add (sp-023, "Gaps Runwick still covers"). The React client is built ahead of time and embedded in the package. Users never need Node.
- **Embedded or standalone.** Embedded: mounted at a path inside an existing ASP.NET Core app. Standalone: an app that has only Runwick, management and the dashboard, with no worker or scheduler. The code is the same.
- **Polling only.** Pages refresh by polling, at most every 5 s, paused when the tab is hidden. There is no live push channel in v1.

Not in SP-024: server-side rendering, a live push channel (SSE or WebSocket), queue depth or other counts (16 has no count reads), editing schedule definitions or payloads, dispatching jobs from the UI, pool overrides, exports, a shipped standalone executable, and SQLite or Redis support for the dashboard reads (they follow when those stores implement `IManagementQueryStore`).

## Changes

- (2026-11-08) New document. Decisions taken where the inputs were silent are marked **[choice]** in the text. The list:
  1. Public API is `AddRunwickDashboard(id, ...)` plus `MapRunwickDashboard(path, id)` (§2).
  2. Permissions come from the management grant mapping, optionally overridden per mount. The spike's `ViewPolicy`/`OperatePolicy` are dropped (§3).
  3. Antiforgery stays per mount (spike gap 3), not the host-wide `IAntiforgery` the management API uses. A failed check is 403 (§3.3).
  4. Controls are posted to the mount, not to the JSON API. Success is a 303 back to the page; anything else is a 200 page with an `outcome` (§5).
  5. Dashboard requests and API requests share one management rate limiter per principal (§3.4).
  6. The client build is one entry script and one stylesheet, with no lazy chunks (§7).
  7. A source hash guards against a stale committed build (§7.3).
  8. Poll intervals per page; only the first page of a list is polled (§4).
  9. New read `ListScalingEvents` (§6).
  10. Route collisions under the mount are checked when the app has started, and stop the app (§2.2).
  11. Standalone is a host configuration, not a shipped program (§2.3).
  12. The spike project, its tests and its sample host are removed once their tests are ported (§9).
  13. The mount root shows the Jobs page.
- (2026-11-09) Client rebuilt on shadcn/ui components, with an Overview page. The list:
  1. New page Overview (`/overview`, §4): key-figure cards with small trend lines, recent failures, queues and the latest scaling events. It uses only reads that already exist. Each figure is a tally of one read's first page (at most 50 rows, shown as "50+" when the read has a next page), never a total. A prop the caller may not read is `null`, and the page polls only the props the caller may read. The `can` map gains `overview` (`Jobs.Read`). The mount root still shows the Jobs page.
  2. New client dependencies (§7.1): Tailwind CSS (compiled to the one static stylesheet), the shadcn/ui component sources written into `ClientApp/components/ui/`, `radix-ui`, `@tabler/icons-react`, `class-variance-authority`, `clsx`, `tailwind-merge`, `tw-animate-css`. `@tailwindcss/vite` is build-only.
  3. Two local shims replace `react-remove-scroll` and `aria-hidden` through npm `overrides` (§7.1). The real packages inject a `<style>` element at run time, which the CSP blocks, and depend on `tslib` (0BSD, outside the licence allow-list).
  4. Icons are Tabler (MIT), not lucide (ISC). Trend lines are small hand-drawn SVGs, not Recharts: Recharts depends on d3 packages under ISC. Both licences need an owner decision before they can be bundled.
  5. `npm:Apache-2.0` is on the licence allow-list (for `class-variance-authority`); `eng/check-licenses.sh` skips npm `link` entries, whose target is checked under its own path.

## 1. Package

| Item | Value |
|---|---|
| Project | `src/Runwick.Dashboard/Runwick.Dashboard.csproj`, packable, `net10.0` |
| Runwick dependency | `Runwick.AspNetCore` only (00 §6) |
| External dependency | Inertia.Net 1.7.0 (contentHash pinned in `packages.lock.json`, see sp-023 "Pinned versions") |
| Embedded resources | `dist/**`, logical name `runwick-dashboard/{path}` |
| Client source | `src/Runwick.Dashboard/ClientApp/` (not packed) |
| Namespaces | `Runwick.Dashboard` for types; extension methods in `Microsoft.Extensions.DependencyInjection` and `Microsoft.AspNetCore.Builder`, as `Runwick.AspNetCore` does |

Layout:

```
src/Runwick.Dashboard/
  Runwick.Dashboard.csproj
  DashboardServiceCollectionExtensions.cs   AddRunwickDashboard
  DashboardApplicationBuilderExtensions.cs  MapRunwickDashboard
  DashboardOptions.cs
  DashboardMount.cs        gate, routing, root view, antiforgery (from the spike)
  DashboardAssets.cs       embedded build, asset version (from the spike, unchanged)
  Pages/*.cs               one class per page: reads, props, actions
  ClientApp/               package.json, package-lock.json, vite.config.js, app.jsx, pages/, e2e/
  dist/                    committed build output: manifest.json, assets/, source.sha256
```

The DEP-01 package-graph declaration replaces `Runwick.Dashboard.Spike` with `Runwick.Dashboard` (edge to `Runwick.AspNetCore`, external `Inertia.Net`). The packed `.nupkg` contains no `ClientApp/` file and no `node_modules`.

## 2. Public API

```csharp
// Services. Requires AddRunwickManagement (16 §5).
services.AddRunwickManagement(m => m
    .AuthenticationSchemes("Cookies")
    .MapGrants(p => ManagementRoles.Operator));

services.AddRunwickDashboard("default", d =>       // mount id
{
    d.Title = "Runwick";                            // optional, default "Runwick"
    d.AuthenticationSchemes("Cookies");             // optional; default: the management schemes
    d.MapGrants(p => ManagementRoles.Viewer);       // optional; default: the management grant map
    d.Actor(p => new ManagementActor(...));         // optional; default: the management actor map
});

// Pipeline. Before the host's own app.UseInertia(), if it has one.
app.MapRunwickDashboard("/runwick", "default");
```

```csharp
public static IServiceCollection AddRunwickDashboard(this IServiceCollection services, string mountId,
    Action<DashboardOptions> configure);
public static IApplicationBuilder MapRunwickDashboard(this IApplicationBuilder app, PathString path,
    string mountId = "default");
```

**[choice]** Two steps, as in the spike: Inertia.Net needs a named mount's options at service registration, and the path is only known when the pipeline is built. The second step is called `Map…` to match `MapRunwickManagement` and `app.Map(path, …)`, which it uses.

### 2.1 What registration does

`AddRunwickDashboard` registers, for mount `{id}`:

- the keyed `DashboardMount` and its keyed `IInertiaRootView`, then `AddInertia("runwick-{id}", o => { o.Version = assets.Version; o.EncryptHistory = true; })`;
- `AddDataProtection()` and `AddAuthorization()` (idempotent).

It registers no startup filter, no global MVC filter, no shared props on the host's default Inertia mount, and no hosted service of its own. It does not change the host's default authentication scheme, `AntiforgeryOptions`, `HttpJsonOptions` or default Inertia options. (Inertia.Net and Data Protection add their own registrations; sp-023 DASH-07 lists them.)

### 2.2 Validation

| Check | When | Result |
|---|---|---|
| Mount id matches `^[a-z0-9-]{1,32}$` | `AddRunwickDashboard` | `ArgumentException` |
| Mount id registered twice | `AddRunwickDashboard` | `InvalidOperationException` |
| Path matches `^(/[a-z0-9][a-z0-9-]*)+$` (no trailing slash) | `MapRunwickDashboard` | `ArgumentException` |
| Mount id not registered | `MapRunwickDashboard` | `InvalidOperationException` |
| `AddRunwickManagement` not called | `MapRunwickDashboard` | `InvalidOperationException` |
| No schemes or no grant map (neither mount nor management sets them) | `MapRunwickDashboard` | `InvalidOperationException`. No anonymous mode |
| A host endpoint whose route lies under the mount path | when the app has started | **[choice]** logged as Critical with the route pattern, then `IHostApplicationLifetime.StopApplication()`. Until the check has run, the mount answers 503 `starting` |

The route check reads every `EndpointDataSource` once, on `ApplicationStarted`. It is the follow-up sp-023 asked for ("host routes under the mount path become unreachable").

**Order rule (sp-023 gap 1).** `MapRunwickDashboard` must run before the host's `app.UseInertia()`. Runwick cannot detect the wrong order from inside the mount. The XML doc of `MapRunwickDashboard` says so, and the getting-started text says so.

### 2.3 Standalone

**[choice]** Standalone is a host configuration, not a separate program:

```csharp
builder.Services.AddRunwick(r => r.Application("billing").UsePostgres(cs));   // no AddWorker, no AddScheduler
builder.Services.AddAuthentication().AddRunwickBasic(o => o.Verifier = verifier);
builder.Services.AddRunwickManagement(m => m.AuthenticationSchemes(RunwickBasicDefaults.Scheme).MapGrants(...));
builder.Services.AddRunwickDashboard("default", _ => { });
var app = builder.Build();
app.MapRunwickDashboard("/runwick");
```

This process writes no worker presence row and runs no Runwick hosted service. The shipped Basic setup is `AddRunwickBasic` from `Runwick.AspNetCore` (16 §5): explicit verifier, no default credentials, HTTPS only, 10 failures per minute per bucket. The spike's own Basic handler is a test helper and is not shipped.

## 3. Requests through a mount

Order inside the mount's branch (as in the spike):

1. **Gate** (Runwick): headers, assets, authentication, grants, request limits, rate limits.
2. **`UseInertia("runwick-{id}")`** (Inertia.Net): asset-version check, state cookies, redirects.
3. **Router** (Runwick): pages, controls, reveals.

### 3.1 Headers on every response

- `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`.
- Everything except assets: `Cache-Control: no-store, private`, `Vary: X-Inertia`.
- HTML: `Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'`. No inline script or style anywhere; the client keeps `progress: false` and `modulePreload.polyfill: false`.

### 3.2 Authentication and grants

- The gate authenticates with the mount's schemes only. No principal: challenge those schemes. Basic gives 401 with `WWW-Authenticate: Basic`. A cookie scheme gives its redirect to the host's login page.
- A principal with **no grant at all** gets 403 (forbid on the mount's schemes).
- The caller (`ManagementCaller`) is built from the principal with the mount's grant and actor maps. Nothing in a request (body, header, query) supplies actor or grant facts.
- Authentication runs before the adapter's asset-version check: a logged-out request with an old version gets 401 or the login redirect, never 409.
- **Leaving the mount (sp-023 gap 4).** For an Inertia request (`X-Inertia: true`), any redirect whose target is outside the mount path becomes `409` with `X-Inertia-Location: <absolute target>`. The client then loads the login or access-denied page in full.
- Each page and prop has a fixed permission (§4). The server checks it with `IManagementSession`, which throws `ManagementDeniedException` (403) or `ManagementNotFoundException` (404). Hiding a button is never the check.

### 3.3 Antiforgery

**[choice]** The dashboard keeps the spike's per-mount token (sp-023 gap 3), not the host-wide `IAntiforgery` that `MapRunwickManagement` uses. Reason: the host-wide service has one cookie and one option set per app, so two mounts (or a mount and the host) would share a token, and a host change to `AntiforgeryOptions` would change the dashboard.

| Part | Value |
|---|---|
| Cookie | `rwk.{id}.csrf`, `Path` = the mount's full path (PathBase + mount), `HttpOnly`, `SameSite=Strict`, `Secure` when the request is HTTPS |
| Request token | written by the root view into `<meta name="runwick-csrf">`; never in page props, so never in history state |
| Header | `X-Runwick-Csrf` |
| Binding | Data Protection purpose `Runwick.Dashboard.Csrf.v1` + mount id; bound to the actor's scheme, issuer and subject |
| Needed for | every POST (controls, bulk cancel, reveals, retention edit) |
| Failure | 403, no session call. `X-Inertia`, `Authorization` or any other header never replaces it |
| GET on a POST route | 405 with `Allow: POST` |

Replicas of one mount must share a Data Protection key ring, as for management cursors (16 §2.1).

### 3.4 Request and rate limits

Reused from `Runwick.AspNetCore` (16 §5, 06):

- POST bodies: `application/json` only (else 415), at most 64 KiB (else 413).
- **[choice]** The same `ManagementLimiter` instance as the JSON API, keyed by principal. A page load, partial reload or poll counts as one read; a control as one mutation; a reveal as one reveal. Asset requests are not counted. Limits are process-local (120 reads, 30 mutations, 10 reveals per minute; 16 active requests per instance). Over a limit: 429 with `Retry-After`, or 503 `busy`.
- Each session call keeps its own 1 s query budget; a timeout is 503 `query_budget_exceeded`.

### 3.5 Path base and proxies

Runwick reads `PathBase`, `IsHttps` and the principal only. Trusting `X-Forwarded-Prefix`, `-Proto` and `-Host` is the host's `UseForwardedHeaders` with known proxies. Asset URLs, the page `url`, the `base` prop, cookie paths and every redirect carry `PathBase` + mount path. Redirect targets are built from the mount path plus escaped IDs (`Uri.EscapeDataString`), never from request input.

### 3.6 Props

- Props are serialized with Runwick's own JSON options (web defaults, camelCase) and handed to Inertia.Net as `JsonElement`s, so the host's `HttpJsonOptions` cannot rename them (sp-023 gap 2).
- Every page has the shared props `base` (mount URL), `title`, `user` (actor display name) and `can` (a map of page and action names to booleans, derived from the grants). `can` is a display hint only.
- Props carry the safe DTOs of 16 §2 as they are. No payload, error text, lease token, uniqueness key, resource key, connection string or antiforgery token is ever a prop. `EncryptHistory` stays on as an extra layer, not as the protection.
- **Partial reloads (sp-023 gap 5).** Each page lists its props. A partial reload (`X-Inertia-Partial-Data`) naming a prop the page does not have, or one the caller may not read, is 403 before anything is read.

## 4. Pages

Routes are relative to the mount. All pages are `GET`. "Poll" is the interval while the tab is visible (§4.1).

| Page (component) | Route | Session reads (permission) | Actions (permission) | Poll |
|---|---|---|---|---|
| Overview (`Overview`) | `/overview` | first pages of `ListJobsAsync` (created in the last hour; failed, created in the last 24 h; pending), `ListQueuesAsync`, `ListWorkersAsync`, `ListScalingEventsAsync` (page: `Jobs.Read`; each prop its own read permission, `null` without it) | none | 10 s |
| Jobs (`Jobs`) | `/`, `/jobs?queue&state&jobName&from&to&cursor` | `ListJobsAsync` (`Jobs.Read`) | bulk cancel of selected rows, ≤ 100 (`Jobs.Cancel` per item) | 5 s |
| Job (`Job`) | `/jobs/{id}` | `GetJobAsync` (`Jobs.Read` on its queue) | cancel, retry, rerun (`Jobs.Cancel`/`Retry`/`Rerun`); reveal payload (`Jobs.ReadPayload`); reveal an attempt's error (`Jobs.ReadError`) | 5 s |
| Queues (`Queues`) | `/queues?cursor` | `ListQueuesAsync` (`Queues.Read`) | pause, resume (`Queues.Control` on that queue) | 10 s |
| Workers and capacity (`Workers`) | `/workers?expired&cursor` | `ListWorkersAsync`, `ListScopesAsync` (`Capacity.Read`) | set scope capacity (`Capacity.Edit`) | 10 s |
| Schedules (`Schedules`) | `/schedules?cursor` | `ListSchedulesAsync` (`Schedules.Read`) | pause, resume with policy (`Schedules.Control`); trigger (`Schedules.Trigger`); revalidate (`Schedules.Edit`) | 10 s |
| Scaling (`Scaling`) | `/scaling?instance&pool&from&to&cursor` | `ListScalingEventsAsync` (`Capacity.Read`, §6) | none | 15 s |
| Settings (`Settings`) | `/settings` | `GetSettingsAsync` (`Settings.Read`) | none | none |
| Retention (`Retention`) | `/retention` | `GetRetentionAsync` (`Settings.Read`) | edit policy (`Retention.Manage`) | 30 s |
| Audit (`Audit`) | `/audit?targetKind&targetId&from&to&cursor` | `ListAuditAsync` (`Audit.Read`) | none | 15 s |

Rules for every page:

- **[choice]** The mount root (`/`) shows the Jobs page. The navigation lists only the pages the caller's grants allow. A page the caller may not read is 403.
- Filters and limits are those of 16 §2: page size 50, keyset cursors, no totals, time range 7 days by default and 31 at most. A list page shows "Next" when the page has a `next` cursor; there is no page count.
- A cursor that is rejected (`invalid_cursor`, for example expired after 15 minutes) renders the first page with a notice. It is not an error screen.
- Each list shows its `observedAt` ("as of 12:04:31").
- A job whose detail expired shows `detailExpiredAt`; its reveal button answers "detail expired" (410 from the session).
- Queues show paused state and revision, no depth (16 has no count read). **[choice]** The page links to the telemetry dashboard (18 §9) for depth and age.
- Job, attempt and error data are shown as text nodes. Error detail is shown as error **code** only until revealed.

### 4.1 Polling

- A poll is an Inertia partial reload of the page's list props (`router.reload({ only: [...] })`): one request per page, even on the Workers page with two lists.
- **[choice]** Only the first page of a list is polled (no `cursor` in the URL). Later pages are history and do not move.
- At most one poll in flight per page. A new poll is not started while one runs.
- Paused while `document.hidden`; on becoming visible, one poll runs at once and the timer restarts.
- On failure (network error, 429, 5xx) the interval doubles up to 60 s; the first success resets it. A 409 asset-version answer is handled by the Inertia client (full reload).
- Polls stop while a control or reveal request is in flight on the page, so a refreshed revision cannot change under a submitted command.
- No interval is below 5 s (management-retention-defaults, "Browser poll").

## 5. Controls

A control is a `POST` to the mount, sent by the Inertia client (`router.post`) with the antiforgery header.

| Route (relative to the mount) | Session call | Body |
|---|---|---|
| `POST /jobs/{id}/cancel`, `/retry`, `/rerun` | `CancelJobAsync`, `RetryJobAsync`, `RerunJobAsync` | `expectedGeneration`, `expectedRevision`, `reason?` |
| `POST /jobs:cancel` | `CancelJobsAsync` | `items[]` (`jobId`, `commandId`, `expectedGeneration`, `expectedRevision`), `reason?` |
| `POST /queues/{name}/pause`, `/resume` | `PauseQueueAsync`, `ResumeQueueAsync` | `expectedRevision`, `reason?` |
| `POST /scopes/{kind}/{key}/capacity` | `SetScopeCapacityAsync` | `capacity`, `expectedPolicyRevision`, `reason?` |
| `POST /schedules/{key}/pause`, `/resume`, `/trigger`, `/revalidate` | schedule controls | `expectedRevision`, `policy` (resume), `reason?` |
| `POST /retention/policy` | `EditRetentionPolicyAsync` | `expectedRevision`, the seven values, `reason?` |

- **Command IDs.** The client makes a UUID (`crypto.randomUUID()`) when the person confirms an action and sends it in `X-Runwick-Command-Id` (bulk: one per item, in the body). If the request fails without an answer, the "Try again" button resends **the same** command ID and body. The server never makes a command ID. Missing or not a UUID: 400.
- The expected revision and generation come from the props the page was rendered with.

Answers:

| Session result | Answer |
|---|---|
| Applied, replayed, or cancel requested (16: 200/202) | `303 See Other` to the resource's page (job, or the list page). The browser never resends the POST. The client shows "done" (or "cancel requested") from the request's success callback, not from a prop |
| Store rejection (16: 409), not found (404), validation (400), `outcome_unknown` or `store_unavailable` (503) | `200` Inertia page of the same resource, with prop `outcome = { code, commandId, actualRevision? }`. Not a 409, so it cannot be mistaken for an asset-version 409 |
| Denied (403) | 403 |
| Bulk | `200` Jobs page with `outcome = { items: [{ jobId, status, code? }] }` in request order (16 §4) |

An `outcome_unknown` page says the result is not known yet and offers "Check again", which resends the same command ID. It never says the action failed or succeeded.

An asset-version 409 on a POST never replays the POST: the client reloads the page and the person acts again (sp-023 DASH-06).

### 5.1 Reveals

| Route | Session call |
|---|---|
| `POST /jobs/{id}/payload:reveal` | `RevealPayloadAsync` |
| `POST /attempts/{id}/error:reveal` | `RevealErrorAsync` |

- Sent with plain `fetch` (not an Inertia visit), with the antiforgery header and an optional `reason`. They are POST because the session writes an audit row first (16 §3).
- Answer: JSON, `Cache-Control: no-store`, the stored value as a JSON value. 410 `detail_expired` and 403/404 use the error body of 16 §5.
- The client keeps the value in component state only: not in props, history, flash or remembered state. It shows it as formatted text and clears it on navigation.

## 6. Management addition: scaling history

The Scaling page needs a read that 16 does not have.

```csharp
Task<ManagementPage<ScalingEventSummary>> ListScalingEventsAsync(ManagementCaller caller, ScalingEventQuery query,
    CancellationToken cancellationToken = default);

public sealed record ScalingEventQuery(Guid? InstanceId = null, string? Pool = null,
    DateTimeOffset? From = null, DateTimeOffset? To = null, int? Limit = null, string? Cursor = null);

public sealed record ScalingEventSummary(Guid EventId, Guid InstanceId, string Pool, int OldTarget, int NewTarget,
    int? Desired, int? Candidate, string Reason, DateTimeOffset? SnapshotAt, DateTimeOffset CreatedAt);
```

| Item | Value |
|---|---|
| Permission | `Capacity.Read` (application-wide, as `ListWorkers`) |
| Route | `GET {prefix}/v1/scaling-events?instance&pool&from&to&limit&cursor` in `MapRunwickManagement` |
| Order | `created_at DESC, event_id DESC`, keyset |
| Range | default last 7 days, at most 31 days (as `ListJobs`) |
| Page | 50 default, 200 max; cursor rules of 16 §2.1 with read name `ListScalingEvents` |
| Fields | as the record above; `membership_digest` is not shown |
| PostgreSQL | `IManagementQueryStore` read over `scaling_events`, served by the existing index `scaling_events_retention (application, created_at, event_id)` read backwards. No schema change, no new revision |

16 records this addition in its Changes section.

## 7. Client and assets

### 7.1 Client

- React 19.3.1, `@inertiajs/react` and `@inertiajs/core` 3.9.1, Vite 8.3.4, `@vitejs/plugin-react` 6.1.2 (exact versions in `package.json`; hashes in `package-lock.json`, as in the spike).
- SPA only: no SSR, no Vite dev-server discovery, no runtime TypeScript generation, no external fetch (no CDN, no web fonts).
- No `dangerouslySetInnerHTML`, `eval`, `new Function` or HTML parsing of job data in `ClientApp/` sources.
- **[choice]** UI: shadcn/ui components (sources in `ClientApp/components/ui/`, adapted to plain JSX) on `radix-ui` 1.8.1, Tailwind CSS 4.3.3 (with `@tailwindcss/vite`, build-only), `tw-animate-css` 1.4.0, `class-variance-authority` 0.7.1, `clsx` 2.1.1, `tailwind-merge` 3.7.0 and `@tabler/icons-react` 3.49.0. System fonts only.
- `react-remove-scroll` and `aria-hidden` are replaced by the local shims in `ClientApp/shims/` (npm `overrides`). The shim locks page scroll by setting `data-scroll-locked` on `<body>`, which `app.css` turns into `overflow: hidden`, so no `<style>` element is ever created.
- Light and dark themes, plus the system setting; the choice is kept in `localStorage` in that browser only. Every animation and transition is cut to nothing under `prefers-reduced-motion: reduce`.
- No new npm dependency beyond those above without a spec change.

### 7.2 Build and embedding

```
cd src/Runwick.Dashboard/ClientApp
npm ci            # or: npm ci --offline, from a warm npm cache
npm run build     # vite build → ../dist, then writes ../dist/source.sha256
```

- `vite.config.js` as in the spike: `base: './'` (the mount path is known only at run time), `publicDir: false`, manifest `manifest.json`, `modulePreload.polyfill: false`.
- **[choice]** One entry script and its stylesheet, no lazy chunks (dynamic imports inlined). This removes the rolling-deploy limit sp-023 noted: an old tab can never ask a new server for a chunk it does not have.
- `dist/` is **committed**. `dotnet build` and `dotnet pack` never run Node; they embed `dist/**` as resources. Consumers never need Node or npm (00 §6).
- `DashboardAssets` (unchanged from the spike) reads the embedded files, finds the entry and stylesheets from the manifest, and sets the asset version to the first 16 hex digits of SHA-256 over every file's path and bytes. Any changed byte gives a new version.
- Assets are served at `{mount}/assets/{file}` only (other build files such as `manifest.json` are never served), with `Cache-Control: public, max-age=31536000, immutable`, a content type from the extension (`.js` → `text/javascript`, `.css` → `text/css`), `nosniff`, and no authentication (they hold no data). An unknown file is 404.

### 7.3 Offline builds and the stale-build guard

- On the reference machine (2026-11-08) `node` v26.13.0 and `npm` 11.20.0 are installed (vite-plus toolchain) and the npm registry was reachable. The spike has no `node_modules`; its `dist/` is committed. Vite 8 needs Node 20.19 or later.
- Without npm or network, nothing in the .NET build or tests breaks: the committed `dist/` is the build. Only changing the client needs npm.
- **[choice]** `npm run build` writes `dist/source.sha256`: SHA-256 over every file under `ClientApp/` except `node_modules/` and `e2e/`, in ordinal path order, each as `path \0 bytes \0`. A .NET test (`Committed_build_matches_client_sources`) recomputes it and fails when a source changed without a rebuild. This needs no Node.
- A byte-identical rebuild on another machine or Node version is not promised. The committed `dist/` is the build of record.

## 8. Acceptance cases

### 8.1 DASH-01–07 against the real UI

These are the cases of dashboard-integration-closure, made concrete for SP-024. They rerun the 30 spike tests against `Runwick.Dashboard` over the real PostgreSQL store and real management session, and add the cases below.

| Case | Required proof |
|---|---|
| DASH-01 | Two mounts in one app (`/ops/a`, `/ops/b`) with different schemes and grant maps, and two apps in one process both at `/runwick`. Each mount has its own asset version, state cookie (`.Inertia.State.runwick-{id}`) and root view. A principal of A gets 401/403 from B; A's antiforgery cookie and token sent to B get 403 and no audit row; the props of one never appear in the other |
| DASH-02 | A host with its own Inertia page: its HTML, Inertia JSON and stale-version responses are byte-identical with and without the dashboard. Dashboard pages contain none of the host's shared props, root view or version. A host fallback route does not capture the mount. A host `HttpJsonOptions` naming policy does not change dashboard prop names. A host that calls `UseInertia()` before the mount still gets the reload loop (pinned test, documents the order rule) |
| DASH-03 | Basic (`AddRunwickBasic`), host cookie and an OIDC-style external challenge. Logged out: 401 / 302 to login / 409 `X-Inertia-Location` for Inertia requests, never data and never 409 for version first. Every control and reveal: missing, forged, other-user or other-mount token is 403 and no store change; GET is 405. A Viewer's partial reload of an operator-only prop, and a reveal without `Jobs.ReadPayload`, are 403 |
| DASH-04 | A job whose name, queue, reason and error code carry `</script><script>alert(1)</script>"'&<img src=x onerror=…>` and U+2028, seen on the Jobs, Job, Audit and Scaling pages, initial HTML and partial reloads: shown as text, no dialog, no `<img>` element, data round-trips unchanged. No CSP violation on any page. HTML, JSON, 401 and reveal responses are `no-store, private`; assets immutable. `history.state` holds no payload or token |
| DASH-05 | Behind `UsePathBase("/ops")` and a known proxy's `X-Forwarded-Prefix`: asset URLs, page `url`, `base`, cookie paths and every 303 carry the full prefix; a prefix from an unknown address is ignored; an ID like `\\evil.example` stays inside the mount when redirected |
| DASH-06 | Old client, new build: a visit gets 409 with an absolute `X-Inertia-Location`, the old asset is 404, the next click loads the new entry. A retry posted by an old client runs once and returns 303; the follow-up GET gets 409 and never resends the POST. The same command ID sent twice gives one transition and one audit row. Stale revision and `outcome_unknown` answer 200 with `outcome` and the original command ID |
| DASH-07 | Standalone (§2.3): every page and control works; no worker presence row; every hosted service comes from the web host, Inertia.Net or Data Protection. Embedded: no startup filter, host default Inertia options unchanged. Startup refusals of §2.2, including the route collision |

### 8.2 MGMT-01–12 across every response path

"Every path" means: initial HTML, Inertia JSON visit, partial reload, poll, control POST, bulk POST, reveal POST, and asset GET where it applies.

| Case | What the dashboard tests add to 16 §7 |
|---|---|
| MGMT-01 | No credentials, wrong scheme, bad Basic password: every page path returns no data (401 or challenge); assets still load; the host's default scheme and its own routes are unchanged |
| MGMT-02 | §3.3 on every POST route for cookie and Basic principals; `X-Inertia: true` alone grants nothing |
| MGMT-03 | A queue-scoped caller: another queue's job is 404 on the Job page, partial reload, poll, every control and reveal; lists and next pages show only allowed queues; a tampered cursor in the URL renders the first page with a notice; an actor field in a POST body is ignored |
| MGMT-04 | Retry from a page whose revision went stale: 200 with `outcome.code = stale_revision` and `actualRevision`; cancel racing Finish: one transition, one audit row |
| MGMT-05 | "Try again" after a dropped reply resends the same command ID: one transition, one audit row; after the grant is removed the resend is 403 and shows no receipt data |
| MGMT-06 | The DASH-04 hostile strings through initial HTML, partial reload, poll and reveal (shown as text); no payload, error text or token in props, history state, or logs at Information or above |
| MGMT-07 | Two mounts and the JSON API in one process with different grant maps, and two browsers in parallel: no shared props, `can` map or caller; an unrelated host MVC route under a sibling path is unchanged |
| MGMT-08 | As DASH-05, plus Basic over plain HTTP refused on every path, and HTTPS only from a known proxy's `X-Forwarded-Proto` |
| MGMT-09 | Audit insert made to fail: a control answers 200 with `outcome.code = store_unavailable` (or 503 for the reveal) and the job is unchanged; the reveal shows no data |
| MGMT-10 | Polling stays at or above each page's interval and at one request in flight; a hidden tab sends none; 429 doubles the interval up to 60 s; a 101-item bulk is 400; 200-row and 31-day caps on the Jobs, Audit and Scaling pages |
| MGMT-11 | After detail expiry the Job page shows `detailExpiredAt` and the reveal shows "detail expired" (410); after purge the Job page is 404; the Retention page shows policy and progress; an edit with a stale revision shows `revision_conflict` |
| MGMT-12 | The standalone dashboard (no worker, no scheduler) and the JSON API offer the same reads and controls to the same caller, with the same results and audit rows |

### 8.3 Other dashboard cases

| Case | Test |
|---|---|
| DSH-01 | `Committed_build_matches_client_sources` (§7.3) |
| DSH-02 | `ListScalingEvents`: order, keyset pages, 31-day cap, `instance` and `pool` filters, `Capacity.Read` required (403 without) |
| DSH-03 | Every page's nav and `can` map for Viewer, Operator and Administrator presets match the permissions of §4 |
| DSH-04 | The packed `Runwick.Dashboard.nupkg` contains `dist` resources and no `ClientApp/` or `node_modules` file; DEP-01 graph declares the edge |

DSH-01 … 04 are new IDs for checks the source cases do not name.

## 9. Tests and how they run

| Part | Where | Notes |
|---|---|---|
| Protocol and service tests | `tests/Runwick.Dashboard.Tests` | TestServer over the real PostgreSQL store (`RUNWICK_TEST_PG` or Testcontainers, 00 §7). Traits `Item=SP-024` and `Case=DASH-0x` / `MGMT-xx` / `DSH-0x`. Ports the 30 spike tests |
| Management addition | `tests/Runwick.AspNetCore.Tests` | DSH-02 over HTTP |
| Browser host | `samples/Runwick.Dashboard.Host` | Replaces `samples/Runwick.Dashboard.SpikeHost`: a host with its own Inertia page at `/ops/`, the dashboard at `/ops/runwick` before `UseInertia()`, PostgreSQL, seeded jobs including the hostile name |
| Browser runs | `src/Runwick.Dashboard/ClientApp/e2e/browser.sh` with `playwright-cli` | Chromium: every flow (each page, each control, reveal, polling and hidden-tab pause, upgrade, login redirects, CSP report check). Firefox and WebKit: smoke flow (load, navigate, one control, one reveal) |

- **[choice]** After the tests are ported and pass, SP-024 removes `src/Runwick.Dashboard.Spike`, `tests/Runwick.Dashboard.Spike.Tests` and `samples/Runwick.Dashboard.SpikeHost`, and their entries in the solution and the package graph.
- WebKit could not run on the spike machine (missing system libraries). DASH-07 is closed only with a WebKit smoke result from a machine that has the Playwright WebKit dependencies. Until then the SP-024 report says "WebKit not run".
- The browser run pins its `playwright-cli` version in `e2e/browser.sh` and takes the port from an environment variable (default 5791); the spike assumed both.

## 10. Must not claim

- that hiding a page or button is authorization; the server checks every request;
- live updates: the dashboard polls; a list can be up to one interval (or 60 s after failures) old, and every list says when it was read;
- queue depth, job totals or any count beyond one page; the Overview tallies at most one page of a read ("50+" beyond it);
- that a cancel stopped a handler (00 §9 rule 5); "cancel requested" is shown as requested;
- that an `outcome_unknown` action failed or succeeded;
- that a revealed payload can be taken back from the browser, a screenshot or memory;
- isolation finer than application plus queue grants (no tenant isolation);
- that the order rule (mount before `UseInertia()`) is enforced; it is documented only;
- that rate limits hold across replicas; they are per process;
- WebKit support before the WebKit smoke run passes;
- that a rebuild of the client is byte-identical to the committed `dist/`;
- dashboard support on SQLite or Redis before those stores implement the management reads.
