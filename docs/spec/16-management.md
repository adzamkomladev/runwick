# 16 — Management API (SP-020)

**Sources:** [management-and-dashboard.md](../architecture/management-and-dashboard.md) (design and MGMT-01–12), [management-retention-defaults.md](../architecture/management-retention-defaults.md) (permission names, limits, lifetimes), [08-controls-and-completion.md](08-controls-and-completion.md) (controls, audit, preview services), [13-scheduler.md](13-scheduler.md) §6 (schedule controls), [00-overview.md](00-overview.md) §4 and §6 (roles, packages), [v1-delivery-plan.md](../v1-delivery-plan.md) §6 (SP-020, SP-032).

## In short

SP-020 puts the controls built in M3 behind an authenticated, permission-checked API, and adds the read side: lists and details of jobs, queues, workers, capacity, schedules and audit. It has two layers:

1. **`IManagementSession`** in core (`Runwick.Management`). It is transport-free. Every call carries a trusted caller: who they are and what they are allowed to do. It checks permissions, builds bounded queries and sends the existing revisioned commands.
2. **`Runwick.AspNetCore`**, a new package. It maps that session to versioned HTTP endpoints (`/v1/…`), authenticates with schemes the host chooses, checks antiforgery on browser mutations, and applies request and rate limits.

A standalone management host runs the session and the endpoints with no worker and no scheduler. The dashboard (SP-024) and the CLI (SP-032) are later clients of this same API.

Not in SP-020: retention cleanup and the detail-expired projection (SP-021), the dashboard UI and its HTML paths (SP-023/024), telemetry instruments (SP-022), a real-time channel, and a shared projection cache (default off, 06).

## Changes

- (2026-11-08) New document.
- (2026-11-08, implementation of SP-020) What the build settled where this document was silent or could not be met as written:
  - **Resending a client command ID.** NotAfter is part of the command fingerprint (03 §8), so a resend with a fresh NotAfter would be `command_conflict`. Before each mutation the session reads the NotAfter of an existing receipt for that command ID and reuses it; a first send gets now + the lifetime. One extra read per mutation. Two concurrent *first* sends of one ID can still race: one gets `command_conflict`, and its resend replays. For a schedule trigger the proposed job ID is derived from the command ID (SHA-256), so a resent trigger has the same body.
  - **Cursor key without `CursorKey`.** Data Protection has no public key-derivation API, so `Runwick.AspNetCore` seals cursors with an `IDataProtector` (purpose `Runwick.Management.Cursor.v1`, authenticated encryption) instead of deriving an HMAC key. Replicas sharing the key ring still accept each other's cursors. The core session alone, with no key, uses a random per-process HMAC key. With `CursorKey` set, the cursor is `payload ‖ HMAC-SHA256` as §2.1 says.
  - **Application-wide permissions.** Only `Jobs.*` and `Queues.*` may be queue-scoped. `Capacity.*`, `Schedules.*`, `Settings.Read`, `Retention.Manage` and `Audit.Read` are application-wide (audit rows are not filtered per queue). Grants come from the host's per-request mapping, so the check is in `ManagementGrant`'s constructor: a bad grant throws where it is built (presets at type load), and the request fails with 500, never with wider access.
  - **Visibility.** A job (or attempt) is visible when the caller has `Jobs.Read` on its queue; otherwise every read, reveal and control on it is 404. A visible job without the specific permission is 403. A queue is visible for its controls with `Queues.Read` or `Queues.Control` on it. A list filtered to a queue outside the caller's scope returns an empty page.
  - **Pages.** `limit` above 200 is clamped to 200; below 1 is 400. Queues, workers, scopes and schedules are keyset-paged with the same cursor rules as jobs and audit.
  - **Reveal of an expired detail** writes no audit row (nothing is disclosed) and returns 410 with the job ID.
  - **Bulk item `status`:** `applied`, `accepted` (cancel requested on owned work), `rejected` (with `code`), `not_found`, `forbidden`, `unavailable`.
  - **Schedule control bodies** use one field, `expectedRevision`: the control revision for pause and resume, the definition revision for trigger and revalidate. Resume takes `policy`: `skipPaused` (default), `runOnce` or `catchUp`.
  - **Error codes** not named above: 401 `unauthenticated`, 400 `antiforgery_invalid`, 429 `rate_limited`, 413 `payload_too_large`, 415 `unsupported_media_type`, 403 `forbidden`, 404 `not_found`. A store rejection `command_expired` is 503 `store_unavailable`.
  - **Antiforgery header.** The host's `AntiforgeryOptions` are not changed: the token sent in `X-Runwick-Antiforgery` is moved to the host's configured antiforgery header just before validation, and any client value in that header is dropped first.
  - **Limits are options.** The numbers of §5 are the defaults of `ManagementOptions` (set with `AddRunwickManagement(m => m.Configure(...))`); `GetSettings` reports them. The Basic failure limit counts verifier failures only; a refused non-HTTPS request does not count.
  - **MGMT-05 dropped reply** uses the harness fault point `AfterCommitBeforeReply` with `DropReply` rather than the TCP fault proxy: the session's own resend resolves it, and a client resend replays.
  - **MGMT-12 schedule controls** act on a schedule registered by a separate scheduler-side client, since the management host runs no scheduler.

## 1. Caller, permissions and scope

```csharp
public static class ManagementPermissions
{
    public const string JobsRead = "Jobs.Read", JobsReadPayload = "Jobs.ReadPayload", JobsReadError = "Jobs.ReadError",
        JobsCancel = "Jobs.Cancel", JobsRetry = "Jobs.Retry", JobsRerun = "Jobs.Rerun",
        QueuesRead = "Queues.Read", QueuesControl = "Queues.Control",
        SchedulesRead = "Schedules.Read", SchedulesTrigger = "Schedules.Trigger", SchedulesControl = "Schedules.Control", SchedulesEdit = "Schedules.Edit",
        CapacityRead = "Capacity.Read", CapacityEdit = "Capacity.Edit",
        SettingsRead = "Settings.Read", RetentionManage = "Retention.Manage", AuditRead = "Audit.Read";
}

public sealed record ManagementGrant(string Permission, IReadOnlySet<string>? Queues); // null = every queue
public sealed record ManagementCaller(ManagementActor Actor, IReadOnlyList<ManagementGrant> Grants);
public static class ManagementRoles { Viewer, Operator, Administrator } // presets returning grants
```

Rules:

- The application is bound by configuration (the runtime's application). Nothing in a request selects it.
- The actor (subject, display, scheme) comes from the authenticated principal through the host's mapping. Request bodies, headers and payloads never supply actor or grant facts.
- Presets: **Viewer** = every `*.Read` except payload, error and audit. **Operator** = Viewer + `Jobs.Cancel/Retry/Rerun`, `Queues.Control`, `Schedules.Trigger/Control`. **Administrator** = Operator + `Schedules.Edit`, `Capacity.Edit`, `Retention.Manage`. `Jobs.ReadPayload`, `Jobs.ReadError` and `Audit.Read` are never in a preset; they are granted explicitly.
- A queue-scoped grant covers jobs in that queue and that queue's control. Scopes, schedules, workers and settings are application-wide: a grant for them needs `Queues = null`. A queue-scoped grant for those permissions is rejected at startup (configuration error), not silently widened.
- `RevalidateSchedule` needs `Schedules.Edit`.

**Outcomes**: every session call returns a result or throws one of three typed exceptions: `ManagementDeniedException` (permission missing for a visible resource → 403), `ManagementNotFoundException` (missing, or outside the caller's queues → 404, same body either way), `ManagementValidationException` (→ 400). Store unavailability and unknown outcome keep the existing exceptions (→ 503).

## 2. Reads

All lists use **keyset pagination only**: page size 50 by default, at most 200. Scope predicates (allowed queues) are in the SQL `WHERE` clause, before the `LIMIT`. Each page carries `ObservedAt` (store time) and `Next` (an opaque cursor, or null at the end). No list returns a total; there is no count endpoint in SP-020.

| Read | Permission | Filters | Order | Fields (no payload, error text, lease token, uniqueness key or resource key) |
|---|---|---|---|---|
| `ListJobs` | `Jobs.Read` | queue, state, job name, created-from/to (default last 7 days, at most 31 days) | `created_at DESC, job_id DESC` | id, job name, payload version, queue, priority, state, generation, revision, created/updated/eligible/terminal times, terminal reason, cancel requested, execution count |
| `GetJob` | `Jobs.Read` on its queue | — | — | summary + generations (number, created reason, created by, started count, max executions) + attempts (id, generation, reservation/execution numbers, worker instance, times, outcome, disposition, applied reason, error **code** only) |
| `ListQueues` | `Queues.Read` | — | queue name | name, paused, revision, ready count per state is **not** included (needs a count; SP-022 metrics) |
| `ListWorkers` | `Capacity.Read` | include expired (default no) | `heartbeat_at DESC` | instance id, host, process id, build, status, subscriptions, pool targets, started/heartbeat/expires |
| `ListScopes` | `Capacity.Read` | kind | kind, key | kind, key, capacity, policy revision, live permits |
| `ListSchedules` | `Schedules.Read` | — | key | key, paused, needs review, definition and control revisions, next due, last materialized |
| `ListAudit` | `Audit.Read` | target kind, target id, from/to | `audit_id DESC` | every audit column except `details` beyond its allowlisted keys |
| `GetSettings` | `Settings.Read` | — | — | allowlist: application, store kind and schema revision, persistence profile, roles enabled in this process, management limits. Never connection strings or `IConfiguration` |

A job in a queue the caller cannot read is **404**, never 403 (design §2: never confirm hidden IDs). Each query runs with `statement_timeout = 1 s`; a timeout returns 503 `query_budget_exceeded`, not a partial page.

### 2.1 Cursors

A cursor is `base64url(payload ‖ tag)`, at most 2 KiB. The payload holds: format version 1, read name, application, the normalized filters, the last row's sort key, the caller's allowed-queue set hash, and an expiry 15 minutes from issue. The tag is HMAC-SHA256 over the payload with the management cursor key.

- The key comes from `ManagementOptions.CursorKey` (32+ bytes). `Runwick.AspNetCore` derives it from ASP.NET Core Data Protection (purpose `Runwick.Management.Cursor.v1`) when none is set, so replicas sharing a Data Protection key ring accept each other's cursors.
- A cursor with a bad tag, another read's name, other filters, another queue-set hash, or a passed expiry is **400 `invalid_cursor`**. Permissions are checked again on every page; a valid cursor grants nothing.

## 3. Reveal

`RevealPayload(jobId)` needs `Jobs.ReadPayload`; `RevealError(attemptId)` needs `Jobs.ReadError`. Both also need the queue to be in scope.

One store transaction (`RecordDisclosure/1`): insert an audit row (`operation = RevealPayload | RevealError`, `target_kind = job | attempt`, new command ID, actor, reason), then read the payload bytes and headers (or the attempt's `error` object), then commit. Data is returned **only after the commit**. If the audit insert fails, nothing is returned (MGMT-09). A payload whose `payload_retained` is false or whose `detail_expired_at` is set returns **410 `detail_expired`** with the job identity. The response body is the stored JSON as a JSON value, never HTML, with `Cache-Control: no-store`.

## 4. Mutations

| Mutation | Permission | Body |
|---|---|---|
| Cancel / Retry / Rerun job | `Jobs.Cancel` / `Jobs.Retry` / `Jobs.Rerun` (job's queue) | `commandId`, `expectedGeneration`, `expectedRevision`, `reason?` |
| Bulk cancel | `Jobs.Cancel` per item | `items[]` (≤ 100: `jobId`, `commandId`, `expectedGeneration`, `expectedRevision`), `reason?` |
| Pause / resume queue | `Queues.Control` (that queue) | `commandId`, `expectedRevision`, `reason?` |
| Set scope capacity | `Capacity.Edit` | `commandId`, `capacity`, `expectedPolicyRevision`, `reason?` |
| Pause / resume / trigger / revalidate schedule | `Schedules.Control` / `Schedules.Control` / `Schedules.Trigger` / `Schedules.Edit` | `commandId`, expected revision, `policy` (resume), `reason?` |

Rules:

- **The client supplies `commandId`** (a UUID). The session passes it to the store unchanged. This is how an ambiguous reply is resolved: the client sends **the same request with the same command ID**, and the receipt returns the committed result (`replayed: true`) with no second transition and no second audit row. A same ID with a different body is 409 `command_conflict` (03 §8). For a schedule trigger, the command ID is also the trigger ID.
- Permissions are checked **before** the store is called, on every send including replays. A replay after the permission was removed is 403 and discloses nothing from the receipt (MGMT-05).
- Lifetimes: job, queue and capacity controls use the administrative 5 min NotAfter (06); schedule controls use 30 s (13 §9). Inside the session an unknown outcome is resent with the same command until NotAfter, as in 08 §10. If NotAfter passes, the result is 503 `outcome_unknown` with the command ID; the client may resend it, and the store's receipt is the answer.
- Reason: at most 512 UTF-8 bytes, otherwise 400.
- Bulk: items run at most 4 at a time; each has its own command, receipt, audit row and result, returned in request order. There is no transaction across items.
- The internal preview services (08 §10) stay for in-process trusted callers. `IManagementSession` calls the same store operations through a shared sender that accepts a command ID.

Result mapping:

| Session result | HTTP |
|---|---|
| Applied (or replayed) | 200, body with the new position and `replayed` |
| Cancel requested on owned work (`effect = cancel_requested`) | 202 |
| Store rejection `stale_revision`, `stale_generation`, `invalid_state`, `unique_conflict`, `command_conflict`, `cancel_already_requested`, `no_change` | 409, body `{code, actualRevision?}` |
| `scope_not_registered`, unknown queue or schedule | 404 |
| Bulk | 200 with per-item `{jobId, status, code?, …}`; the HTTP status never summarizes items |

## 5. HTTP surface (`Runwick.AspNetCore`)

```csharp
services.AddRunwickManagement(m =>
{
    m.AuthenticationSchemes("Cookies");                 // required: host schemes accepted for this API
    m.MapGrants(principal => ManagementRoles.Operator);  // required: principal → grants
    m.Actor(principal => new ManagementActor(...));      // optional: default = NameIdentifier/Name + scheme
});
services.AddAuthentication().AddRunwickBasic(o => o.Verifier = …); // optional Basic scheme
app.MapRunwickManagement("/runwick/api");               // endpoints under {prefix}/v1/…
```

- Missing schemes or grant mapping fails at `MapRunwickManagement` (startup), never as anonymous access. There is no anonymous or "local only" mode.
- The endpoint group requires an authenticated principal from the listed schemes only; it does not change the host's default scheme, challenge or other routes (MGMT-07).
- **Antiforgery:** every mutation validates the ASP.NET Core antiforgery request token (header `X-Runwick-Antiforgery`) unless the group was mapped with `.ForMachineClients()`. A machine-client group accepts only schemes that are not cookie-based and not Basic; mapping it with such a scheme fails at startup. `X-Inertia` or any other header never skips the check. `GET {prefix}/v1/antiforgery` returns a token for an authenticated caller. Mutations are never GET.
- Basic (`AddRunwickBasic`): needs an explicit verifier, no default credentials; rejects non-HTTPS requests; failures are limited to 10/min per bucket (source address + user name) and answer 401 with `WWW-Authenticate: Basic`.
- Requests: JSON only (`application/json`, else 415), body ≤ 64 KiB (413). Responses: JSON (camelCase, `runwick.json`-independent), `Cache-Control: no-store, private`, `X-Content-Type-Options: nosniff`.
- Limits (process-local, 06): per principal 120 reads, 30 mutations and 10 reveals per minute, burst a quarter of that; at most 10,000 tracked principals, then a shared restricted bucket; 16 active requests per instance with 32 waiting up to 1 s. Over a limit: 429 with `Retry-After`, or 503 `busy` for the instance limit.
- Errors use one body shape: `{ "code": "...", "message": "...", "commandId"?, "actualRevision"? }`. No exception text, stack or request echo.
- Forwarded headers are the host's choice (`UseForwardedHeaders` with known proxies). Runwick reads only `HttpContext.Request.IsHttps`, `PathBase` and the principal; redirects are never issued by the API.

Route table (all under `{prefix}/v1`):

| Method and path | Session call |
|---|---|
| `GET jobs?queue&state&jobName&from&to&limit&cursor` | ListJobs |
| `GET jobs/{id}` | GetJob |
| `POST jobs/{id}/payload:reveal`, `POST attempts/{id}/error:reveal` | RevealPayload, RevealError (POST: they write audit) |
| `POST jobs/{id}/cancel`, `…/retry`, `…/rerun`; `POST jobs:cancel` | controls; bulk cancel |
| `GET queues`; `POST queues/{name}/pause`, `…/resume` | ListQueues; queue controls |
| `GET workers?includeExpired` | ListWorkers |
| `GET scopes?kind`; `POST scopes/{kind}/{key}/capacity` | ListScopes; SetScopeCapacity |
| `GET schedules`; `POST schedules/{key}/pause`, `…/resume`, `…/trigger`, `…/revalidate` | ListSchedules; schedule controls |
| `GET audit?targetKind&targetId&from&to&limit&cursor` | ListAudit |
| `GET settings` | GetSettings |
| `GET antiforgery` | token |

## 6. Store additions (PostgreSQL, schema revision 15)

- Internal `IManagementQueryStore`: the reads of §2 and `RecordDisclosure/1` (§3). Reads take allowed queues as an array parameter (`queue = ANY($n)`, or no predicate when every queue is allowed).
- Index `jobs_created_list (application, created_at DESC, job_id DESC)` for unfiltered lists; `jobs_state_list (application, state, created_at DESC, job_id DESC)` for state filters. The existing `jobs_queue_list` serves the queue filter.
- `management_audit`: operation check adds `RevealPayload`, `RevealError`; target kind check adds `attempt`.
- Migration `0015_management.sql`; the manifest adds `RecordDisclosure/1` at revision 15.

## 7. Cases (MGMT, SP-020 subset)

Test project `tests/Runwick.AspNetCore.Tests` (TestServer over the real PostgreSQL store; trait `Item=SP-020`).

| Case | What it runs |
|---|---|
| MGMT-01 | No credentials, wrong scheme, bad Basic password: every route returns 401 and no data; the host's own default scheme and an unrelated route behave as before; Basic over HTTP is refused |
| MGMT-02 | Cookie and Basic mutations without or with a wrong antiforgery token: 400 and no store change; `X-Inertia: true` does not skip it; GET on a mutation path is 405; machine-client group with a cookie scheme fails at startup |
| MGMT-03 | A queue-scoped caller: another queue's job id is 404 on read, reveal and every control (no audit row, no change); lists and cursor pages contain only allowed queues; a forged actor field in the body is ignored; a tampered, expired, other-filter or other-scope cursor is 400 |
| MGMT-04 | Stale generation and stale revision are 409 with the actual revision; cancel racing Finish and retry racing a uniqueness holder each end with one transition, one receipt, one audit row |
| MGMT-05 | Same command ID twice: second is `replayed`, one audit row; same ID with another body is 409; replay after the grant is removed is 403 and the body has no receipt data; a dropped reply (fault proxy) resolved by resending gives one transition and one audit row |
| MGMT-07 | Two management hosts with different prefixes and grant maps in one process, and two callers in parallel, see only their own grants and results; a host MVC route under a sibling path is unaffected |
| MGMT-08 | With `UseForwardedHeaders` and a known proxy, `X-Forwarded-Proto: https` from the proxy enables Basic and from an unknown address does not; `PathBase` prefixes work; the API issues no redirect |
| MGMT-09 | Audit insert made to fail (fault point `AfterMutation` raising in the audit insert): the control returns 503/not applied and the job is unchanged; a reveal returns no data |
| MGMT-10 | 200-row page cap, 31-day range cap, 101-item bulk is 400, a burst over the read/mutation/reveal rate gives 429 with `Retry-After`, the instance limit gives 503 `busy`; over 10,000 principals uses the shared bucket and the map stays ≤ 10,000 |
| MGMT-12 (part) | A standalone host (`AddRunwick` + PostgreSQL + `MapRunwickManagement`, no `AddWorker`/`AddScheduler`) serves every read and control; no worker presence row is written |

MGMT-06, MGMT-11 and the dashboard half of MGMT-12 are SP-021/SP-024.
