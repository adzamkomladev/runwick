> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Commercial strategy: hosted store ("Shape C") and what v1 must leave open

Status: product decision recorded 2026-10-30. The user selected a **hosted job store and coordination service** (Hatchet-style) as the commercial product, with the self-hosted library remaining free and first-class. This document records the model, the billing units, the architectural consequences and the backlog additions. It does not change the first PostgreSQL milestone.

## 1. Model

- The library — contracts, engine, PostgreSQL/SQLite/Redis drivers, scheduler, management services, self-hosted dashboard and the CLI — is free and permissively licensed. No paid license.
- **Runwick Cloud** (name provisional) hosts the job store and coordination: customers' producers and workers use a `Runwick.Cloud` driver that talks to the hosted service over gRPC. The service owns durable state, scheduling, recovery, retention, the multi-tenant dashboard and billing. Customers host only their own producers and workers.
- Both audiences are served by one engine: self-hosted monolith users run the PostgreSQL driver directly; cloud users run the same engine with a different driver.

### Why this is cheap to build on the current design

```text
self-hosted:  worker engine → Runwick.Postgres driver → customer PostgreSQL
cloud:        worker engine → Runwick.Cloud driver ──gRPC──► Runwick server
                                                           = core + Runwick.Postgres driver
                                                             + gRPC adapter + tenancy + metering
```

Workers only ever reach the store through the named operations of the [storage command protocol](architecture/storage-command-protocol.md); every record is already `application`-scoped; actor identity, expected revisions and receipts already exist. `Runwick.Cloud` is a client implementation of the same internal store interfaces that forwards commands; the server is the existing core and PostgreSQL driver behind an RPC layer. Nothing in the engine forks.

## 2. Billing units

| Dimension | Definition | Source of truth |
|---|---|---|
| **Run** | The first committed Begin of a generation (`execution_number == 1`), classified by the generation's recorded `created_reason`: `initial` and scheduled occurrences **count**; `rerun_succeeded` (re-running a job that already succeeded, via `RerunSucceeded`) **counts**; `manual_retry` of a failed/cancelled job is **free for the first 5 such generations of that job** and counts from the 6th onward (decided 2026-10-30). Automatic retries within a generation (`execution_number > 1`) are free, bounded by plan caps on `MaxExecutions`. Rate deferrals, pre-start abandonments, released reservations and cancellations while waiting are not runs. | Committed Begin receipts / attempt rows plus `job_generations.created_reason`; never client reports. Reconcilable per tenant per period. |
| **Concurrency** | Purchased concurrent-run slots per workspace/environment, enforced as a tenant-wide distributed concurrency scope of kind `application` checked in Reserve. Local worker tickets are unlimited by plan; the store admits at most N running attempts. Independent of delivery mode: workers may **pull** (poll/long-poll) or receive **push** assignment over a stream; both are supported and the slot limit applies equally (decided 2026-10-30). | `concurrency_scopes` row with plan-controlled capacity and revision. |
| **Retention** | Days/months of job, attempt and event history, mapped onto the existing per-record-class retention policies. | `retention_policies` ceilings set by plan. |

Abuse guards: plan caps on `MaxExecutions` and unstarted-abandonment budgets so free retries cannot become free compute; per-tenant RPC and connection budgets.

Pricing shape (structure, not numbers): free tier with monthly runs, 1–2 concurrency slots and 7-day retention; metered runs beyond; concurrency-slot packs; retention tiers; enterprise for SSO/compliance/isolation.

## 3. Consequences for the architecture

| Concern | Response |
|---|---|
| Two round trips per job (Reserve, Begin) cost 30–80 ms each over the internet | Add `ReserveAndBegin/1` as the cloud driver's normal path (rate check in the same server transaction). Available to self-hosted too. |
| Polling cost at scale | Two delivery modes, selectable per worker pool: **pull** (bounded polling/long-poll, the self-hosted default) and **push** (long-lived stream on which the server offers "claim these N"). In both, readiness is a hint that Reserve/`ReserveAndBegin` revalidates, so ownership rules are identical. Batched Reserve (K jobs per call) as the protocol already permits. Push is the cloud default; pull remains available for restrictive networks and for self-hosted parity. |
| Renewal fan-out | One batched Renew RPC per worker per interval with independent per-lease outcomes; Renew without receipt (R1). |
| Transactional dispatch impossible across customer DB and hosted store | `StageAsync` is not offered by the Cloud driver. `Runwick.Outbox.Postgres/Sqlite` become the recommended cloud dispatch path. R6 transactional completion does not apply to cloud; document the inbox + idempotency-key pattern. |
| The service is the customer's critical path | HA PostgreSQL with synchronous replication, multi-zone stateless API tier, status page, SLA. The driver-matrix durability profile becomes the provider's operational promise. Workers already degrade honestly on disconnect: stop admission, finish running work, retry renewals, never fabricate outcomes. |
| Noisy neighbours | Per-tenant RPC rate limits, transaction/connection budgets and the tenant-wide concurrency ceiling. |
| Tenant isolation | Workspace/environment above `application`; API keys scoped per environment and role (producer/worker/admin); gate rows enforce store/tenant binding; schema- or database-per-tenant available for large accounts. Fuzz ISO-01 across tenants. |
| Reconnect semantics | Same rules as the Redis closure: resolve old immutable commands, never re-issue with a fresh ID, re-establish ownership before invoking. |

## 4. Free CLI

`runwick` ships in the OSS repository as a `dotnet tool` and a single-file binary, working against a self-hosted store (connection string) or the cloud (API key):

- `login`, `context use <env>`
- `jobs list|get|cancel|retry|rerun|tail`, `queues pause|resume`, `schedules list|trigger|pause|resume|edit`, `workers list`, `capacity get|set`, `dispatch <job-name> --payload @file.json`, `events tail`
- `migrate` and `doctor` (self-hosted)
- `--output json`, non-interactive, every mutation accepts `--expected-revision` and `--command-id`, so CI scripts can drive it idempotently. Dashboard and CLI consume the same versioned management API.
- **`--agent` mode** (decided 2026-10-30): a separate output contract optimized for AI agents rather than humans — not merely JSON formatting of the human output. In agent mode the CLI: emits one schema-versioned JSON envelope per command (`{schemaVersion, command, ok, result, error:{code, retryable, hint}, nextActions:[…], revisions:{…}}`); writes JSON Lines for streaming commands; never prompts, colors, paginates or truncates; always returns full identifiers and the revision tokens needed for the next mutation; uses stable machine error codes with a short remediation hint; and exposes `runwick --agent describe` to print the complete command/argument schema as a tool manifest so an agent can discover the CLI without documentation. Human output and agent output are both generated from the same command model, so they cannot drift in capability, only in presentation.

## 5. Backlog additions

These are added to the [implementation backlog](implementation-backlog.md) as section 6b. They follow the existing 30 items and do not alter the first PostgreSQL milestone.

| ID | Deliverable | Depends on |
|---|---|---|
| SP-031 | Protocol additions, versioned: `ReserveAndBegin/1`, batched Reserve, batched Renew with independent outcomes, `RerunSucceeded` (new generation of a succeeded job, confirmed 2026-10-30), `job_generations.created_reason` (`initial` / `manual_retry` / `rerun_succeeded` / `scheduled`), `application`-kind concurrency scope, typed `QuotaExceeded` (known-not-applied, stable code), push-assignment hint channel alongside pull | SP-008, SP-011, SP-013 |
| SP-032 | Management API as a public versioned surface (was dashboard-internal); `Runwick.Cli` over it with human and `--agent` output contracts and `--agent describe` manifest | SP-020 |
| SP-033 | Cloud wire contract: gRPC operation set mirroring the storage command protocol, pull and push delivery modes, API-key scopes, tenant/store binding in every command, reconnect rules, conformance against the driver contract suite | SP-031, SP-028 |
| SP-034 | `Runwick.Cloud` client driver passing the applicable shared contract suite against a reference server | SP-033 |

Server-side tenancy, metering, billing and the hosted dashboard live in a separate proprietary repository that consumes the OSS packages; they are outside this backlog.

## 6. Seams the OSS core keeps open

- Versioned event schema on `queue_events` / `management_audit` with a keyset "since cursor" query (feeds metering, webhooks, OTel logs).
- Management services remain transport-free behind one `IManagementSession` with actor identity.
- `store_id` + `application` as the fleet key; dashboard code never assumes one store per process.
- Driver SPI public and versioned (D-02/D-13) — this is what makes `Runwick.Cloud` a driver rather than a fork.
- Telemetry names frozen as a public contract; `Runwick.OpenTelemetry` convenience package, structured-log event table, pinned messaging-convention mapping, shipped Grafana dashboard and alert rules (SP-022).

## 7. Sequencing and cost

The self-hosted library must be excellent first: the Cloud driver is the same engine, and the free library is the funnel. Expect roughly 9–15 months of work after v1 for a credible hosted service, early infrastructure in the low thousands per month (replicated PostgreSQL, multi-zone API tier, metering, auth), plus on-call and compliance effort. Order: v1 → SP-031/032 (benefit self-hosted users directly) → SP-033/034 → hosted service private beta → billing.
