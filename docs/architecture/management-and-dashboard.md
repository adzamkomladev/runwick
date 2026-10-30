> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Management backend and dashboard internals

Current selection: [dashboard integration closure](dashboard-integration-closure.md) chooses Inertia.Net 1.5.3 and its pinned React pairing; [management/retention defaults](management-retention-defaults.md) fixes permission names, query limits and lifetimes. The older InertiaCore review below is historical comparison, not the selected adapter.

Status: engineering proposal, 2026-10-29. No endpoints, UI, authentication integration or security tests have been implemented. Extends the [public contracts](public-api-contract.md), [storage protocol](storage-command-protocol.md) and [driver matrix](driver-capabilities.md). The optional embedded/standalone dashboard and Inertia.NET direction are retained; the exact adapter and client versions remain an integration gate.

## 1. Boundaries and host ownership

Use three internal layers, without deciding final NuGet packaging here:

1. Management application services own authorized queries, revisioned commands, operation resolution and audit context. They call driver operations, never execute handlers.
2. ASP.NET Core adapters authenticate callers, validate antiforgery and request limits, construct trusted caller context, and translate service outcomes into HTTP responses.
3. Dashboard pages and bundled assets consume those services. Initial HTML, Inertia responses, polling and optional event connections share the same authorization boundary.

A standalone management host connects to the configured Runwick store without starting workers or schedulers. Embedded hosting mounts an explicit path prefix and selects named authentication schemes/policies without replacing the host's defaults. Registration must not install global authorization exceptions or expose an alternative unprotected JSON API.

The service boundary requires a trusted caller context for every management operation. In-process host integration supplies an explicitly configured system identity/policy when appropriate; a missing HTTP context never means administrator. This is protection against untrusted requests, not isolation from application code that already owns database credentials.

The dashboard can display jobs/attempts, queues, workers/capacity, schedules, scaling decisions, sanitized settings and retention status using this single backend. It needs no second storage product, JavaScript server or remote asset CDN at runtime. Ship versioned, prebuilt assets; server-side rendering is not required for v1.

## 2. Permissions and query scope

Permissions are capabilities mapped through host policies. Suggested role presets are convenience mappings, not hard-coded role-name checks:

| Capability family | Access | Suggested preset |
|---|---|---|
| Jobs/attempts read | Status, registered type, queue, timing, revision and safe outcome code | Viewer |
| Payload or detailed error read | Separate explicitly granted sensitive-detail access | Separate grant |
| Job cancel/retry | Generation-aware operation with reason | Operator |
| Queue read/control | Queue status and pause/resume | Viewer / Operator |
| Schedule read/trigger/control | Metadata, manual trigger, pause/resume | Viewer / Operator |
| Schedule edit | Definition changes with cutover policy | Administrator |
| Capacity read/edit | Targets/limits, revisioned ceilings/rates/overrides | Viewer / Administrator |
| Settings read / retention manage | Sanitized effective settings / revisioned cleanup policy | Viewer / Administrator |
| Audit read | Sanitized administrative history | Separate grant |

Every grant is scoped to an application and, where meaningful, queues/resources. Possessing a job ID or command ID grants no access. Configure application/store binding server-side; reject a route selecting an unauthorized binding. Actor subject, issuer and scheme come from the authenticated principal, never from request fields. Do not use payload properties, trace baggage or caller-provided headers as authorization facts.

Apply scope predicates before pagination, totals, facets and search. Single-resource requests load only the safe authorization metadata, check permission, then project permitted fields. A blanket endpoint authorization attribute cannot decide all resource-specific access; ASP.NET Core documents resource-based authorization for that case. [Resource authorization](https://learn.microsoft.com/en-us/aspnet/core/security/authorization/resource-based?view=aspnetcore-10.0)

The baseline isolation boundary is the configured application plus authorized queue set. A payload containing TenantId does not establish tenant isolation. Supporting finer tenant permissions requires immutable, indexed security-partition metadata and equivalent enforcement across every driver/query/command; do not advertise it until specified and tested. If a host's custom policy cannot be translated into a safe bounded query, reject that query configuration instead of filtering a page after reading unrestricted results.

Queues and security-partition bindings used to authorize a mutation must be immutable for that generation or checked in the atomic command. Authorization uses current host policy at request evaluation; Runwick cannot make remote identity-provider revocation atomic with a database commit. Recheck authorization for each new request and every replay/resolution, even if a receipt already exists. Cached results are never authorization tokens.

## 3. Authentication and browser requests

| Mode | Proposed integration contract |
|---|---|
| Basic Auth | Explicit credential verifier/secret configuration, no default credentials; HTTPS required; use vetted credential verification and bounded authentication throttling; map identities to the same policies |
| Existing host authentication | Explicit accepted schemes and required policies; preserve host login/challenge handling and default scheme configuration |
| Host OIDC | Host performs the OIDC flow and maintains the authenticated server session; Runwick consumes that principal and does not place access/refresh tokens in props |

Missing authentication/policy configuration prevents dashboard mapping/startup. No implicit anonymous mode based on environment name or perceived local address. This still permits a simple Basic Auth setup with an explicit permission preset.

Require antiforgery validation for browser mutations under both cookie and Basic Auth. Browsers automatically send Basic credentials too, so HTTPS and Basic Auth alone do not prevent CSRF. Use the ASP.NET Core antiforgery service with a request token header and its protected cookie; an Inertia header is not proof of intent. Never mutate through GET. [ASP.NET Core CSRF guidance](https://learn.microsoft.com/en-us/aspnet/core/security/anti-request-forgery)

Cookie settings, principal-bound antiforgery identity and shared Data Protection configuration must work across management replicas. The authentication cookie is Secure/HttpOnly; any JavaScript-readable antiforgery request token is distinct from it. CORS is off by default. A separately enabled machine API may use explicitly scoped bearer authentication; exclude cookie/Basic authentication from that route if using a non-browser CSRF exemption. A client setting Authorization or X-Inertia cannot select the exemption itself.

Honor forwarded scheme/host/path information only through explicitly trusted proxy configuration. Do not trust arbitrary forwarded identity headers. Test HTTPS enforcement, allowed hosts, PathBase, redirect destinations and antiforgery behind the actual proxy setup. [ASP.NET Core proxy guidance](https://learn.microsoft.com/en-us/aspnet/core/host-and-deploy/proxy-load-balancer)

## 4. Revisioned command execution

All mutations follow this sequence:

1. Authenticate, enforce request-size/content-type limits, validate browser antiforgery, bind to the authorized application.
2. Validate the command schema, bounded reason and command ID; load safe resource metadata and authorize the requested operation.
3. Submit expected generation/control revision, immutable target binding and trusted audit actor to the driver. Serialize against worker/scheduler commands using the existing storage lock order.
4. Commit the control transition, command receipt and successful-operation audit record atomically. Return the committed semantic result, including the resulting revision and any continuing work.
5. On uncertain acknowledgement, preserve command identity and resolve/replay the same command. Do not generate another command ID automatically.

Successful replay returns the prior result without another transition or successful-operation audit record. Authorization still runs first. Receipt retention/deadlines follow the storage protocol; an expired unknown operation is not safe to recreate blindly. Driver-owned audit is an addition to administrative command records, not a second best-effort write after mutation.

| HTTP outcome | Meaning |
|---|---|
| 200 | Applied/resolved result; retry means a new generation is queued, not already executing |
| 202 | Cancellation request committed for owned work; response explicitly distinguishes requested from terminal cancellation |
| 400 | Invalid or unsupported input before mutation |
| 401 / 403 | Missing authentication / denied capability for a visible boundary |
| 404 | Missing or outside caller-visible resource scope; avoid confirming hidden IDs |
| 409 | Stale revision/generation, invalid current state, or uniqueness conflict; no silent overwrite |
| 503 | Store unavailable or unresolved outcome; typed body distinguishes known-not-submitted from unknown/recovering and carries a safe operation-resolution reference when applicable |

Cancellation cannot claim a noncooperative handler stopped. Queue pause does not cancel reservations. Retry preserves job ID and history, requires the observed generation/revision and uses the established uniqueness rules. Capacity decreases drain naturally; rate changes report their conservative admission freeze. A retry button cannot edit payloads or reset ownership fences.

Bulk controls operate on a bounded explicit list of IDs and expected revisions, returning per-item results/operation IDs. They do not promise a single transaction over an evolving filter. Preview and execute may race; changed items conflict. A process interrupted during a bulk request resolves each submitted command before offering a further action.

## 5. Safe dashboard projections and transport

Return purpose-built DTOs, never serialized storage records. Lists contain no payload, stack trace, connection string, credential, lease token, uniqueness key or arbitrary resource key. Settings show a curated allowlist and persistence profile, not IConfiguration. Error codes are bounded classifications; detailed diagnostics require a separate permission.

Payload/error detail is an explicit reveal request, authorized and audited before disclosure. Use a separate no-store response fetched into temporary page state, outside shared props, prefetched responses and remembered history. Render it as text/structured JSON, never HTML. Clear it on navigation/logout; do not claim that browser memory or screenshots can be revoked. Retention expiry returns an explicit detail-expired result without losing the remaining job identity.

Inertia transports page data in initial HTML and subsequent responses. Both need server-side filtering; hiding a component is not authorization. Apply the same checks to partial/deferred requests, exports if later added, and every polling endpoint. Protected page/API responses use private no-store caching and correct transport headers. History clearing/encryption is additional protection only when the chosen adapter/client pair supports it. [Inertia protocol](https://inertiajs.com/docs/v3/core-concepts/the-protocol)

Use context-appropriate HTML/JSON encoding, a restrictive tested Content Security Policy and local hashed assets. Test payloads containing closing script tags, quotes and markup through initial rendering and JSON paths. Avoid unsafe raw HTML components and client-side compilation of job data. Redirects stay within validated destinations; failed commands do not echo arbitrary request objects or exception details.

Queries use bounded keyset pagination, supported indexed filters and bounded time ranges. Cursor integrity binds sort/filter/application scope and expiry; reauthorize even a valid cursor. Page responses include observedAt, revision and any approximation/staleness indicator. Counts must use authorized scope and an explicit cost budget; a browser refresh must not trigger an unbounded history scan.

Polling is the baseline: pause hidden pages, back off failures, cap frequency and in-flight requests, and cancel obsolete requests. A shared read cache includes authorized scope, policy version where available, filters and projection permissions; do not reuse privileged projections for lower access. Revalidate access before serving cached data.

Optional real-time transport sends bounded invalidation hints for authorized scopes, followed by ordinary authorized reads. Authenticate each connection, enforce bounded lifetime/reconnection and close revoked/expired sessions when detected. Do not broadcast raw job events globally and filter in JavaScript. Gaps/disconnects recover by polling; this channel is not a durable execution or acknowledgement mechanism.

## 6. Audit and retention

Successful administrative mutations persist an audit row/event in the same atomic unit as effects and receipt. Proposed fields: application, command ID, operation, target identity, actor issuer/subject/scheme, decision instant, safe reason, expected/resulting revisions, allowlisted before/after settings, outcome and optional trace ID. Unique application/command/operation identity prevents replay duplication. Index by application/time/ID and target/time, with bounded actor lookup where supported.

Do not store credential material, authorization headers, raw payloads, lease tokens, arbitrary HTTP bodies or full connection settings. Bound and sanitize reasons; users can paste secrets into free text, so document this limit rather than promise perfect redaction. Audit detail access is separately authorized.

If required successful-mutation audit cannot commit, the mutation cannot report success. PostgreSQL/SQLite use their transaction; Redis must include audit projection in the pending-intent/publication recovery gate. Sensitive-detail access similarly requires a durable access-intent audit before returning data; a later disconnect may mean the record says disclosure authorized even though delivery is unknown.

Denied/invalid requests go to bounded security logging, with safe target identifiers and rate limiting to resist log amplification. They need not write to an unavailable job store. External audit export can be retried asynchronously from persisted records; exporter failure does not undo a committed mutation. Ordinary execution telemetry is separate and never required for job completion.

Audit is append-only through Runwick APIs, not tamper-proof against a database administrator. Cleanup is policy-controlled and itself audited; retention cannot erase live receipt/recovery/fence/uniqueness obligations. Separate payload, attempt-detail, command receipt and audit retention. Retention policy edits are revisioned; cleanup reports progress and failures instead of implying immediate deletion. No one TTL governs everything.

## 7. Implementation evidence and adapter gate

Existing [implementation research](../implementation-research.md) records inspected Hangfire dashboard authorization/antiforgery paths and TickerQ authorization configuration/tests. Carry those lessons into explicit pipeline and bypass tests; do not infer a vulnerability merely from configurable behavior or an integration issue. Relevant pinned sources: [Hangfire middleware](https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.AspNetCore/Dashboard/AspNetCoreDashboardMiddleware.cs), [TickerQ authorization tests](https://github.com/Arcenox-co/TickerQ/blob/f366cf7fa32575be12e3638546d04ed6d0708c63/tests/TickerQ.Tests/DashboardAuthorizationPipelineTests.cs).

For the handoff's Inertia.NET direction, inspected candidate kapi2289/InertiaCore at commit `81aa1496c18e1ab88fbcdcffa4c23e9c49e4a1cb`. This is a candidate, not a claim that the handoff selected this package:

- AddInertia registers a singleton response factory and adds a global MVC action filter; UseInertia assigns a static factory. This creates an embedded-host/multiple-service-provider integration concern to test. It does not by itself prove data leakage. [Registration](https://github.com/kapi2289/InertiaCore/blob/81aa1496c18e1ab88fbcdcffa4c23e9c49e4a1cb/InertiaCore/Extensions/Configure.cs), [static facade](https://github.com/kapi2289/InertiaCore/blob/81aa1496c18e1ab88fbcdcffa4c23e9c49e4a1cb/InertiaCore/Inertia.cs).
- Shared props use the current HttpContext features. The non-SSR HTML path serializes JSON and HTML-encodes it into a data-page attribute. Reuse the principle of request-local props and context encoding, and verify the final client/protocol combination. Do not assume a current v3 client from protocol documentation matches this candidate automatically. [ResponseFactory](https://github.com/kapi2289/InertiaCore/blob/81aa1496c18e1ab88fbcdcffa4c23e9c49e4a1cb/InertiaCore/ResponseFactory.cs).

The integration spike must validate the selected adapter/client versions, verify supported .NET targets, inspect response/filter behavior, and test two simultaneous hosts, an existing host Inertia setup, distinct path roots, auth challenges, antiforgery, prop isolation, redirects, CSP and asset upgrades. Prefer an instance-bound adapter surface where feasible; any required upstream fix or narrow adapter must be explicit. None of these integration tests have run. This source review is not a security audit of InertiaCore.

## 8. Acceptance cases

| ID | Required proof |
|---|---|
| MGMT-01 | Missing/invalid auth never serves protected HTML, JSON, partial props, polling or hints; selected host schemes stay intact |
| MGMT-02 | Cookie and browser Basic mutations reject missing/wrong antiforgery; GET cannot mutate; forged Inertia headers grant no bypass |
| MGMT-03 | Cross-application/queue IDs, forged actor fields and unsigned/foreign cursors reveal no hidden records/counts and cause no mutation |
| MGMT-04 | Stale generation/revision conflicts; cancel versus Finish and retry versus uniqueness serialize with correct receipt/audit |
| MGMT-05 | Replay after permission removal does not disclose receipt data or mutate; ambiguous commit resolves once with one success audit |
| MGMT-06 | Initial/partial rendering and payload detail safely display hostile strings; secrets never enter default projections/history/logs |
| MGMT-07 | Concurrent hosts/users do not share props, settings or auth context; embedded paths do not alter unrelated MVC routes |
| MGMT-08 | Trusted-proxy/path-prefix deployments enforce HTTPS and safe redirects; spoofed forwarding headers do not grant access |
| MGMT-09 | Audit failure prevents successful admin mutation/detail reveal; Redis failure at each write cannot publish unaudited success |
| MGMT-10 | Large history, rapid polling, bulk actions and event reconnects obey query/request budgets; policy scopes survive caching |
| MGMT-11 | Retention removes only eligible details, preserves correctness records and reports expired detail distinctly |
| MGMT-12 | Embedded and standalone dashboard expose the same authorized operations with workers absent from the management process |

These extend the [driver contract suite](driver-contract-suite.md), not replace it. Numerical limits, permission names and adapter versions are selected in the closure documents above. Runtime tests and [observability evidence](observability-and-operations.md) remain implementation obligations.
