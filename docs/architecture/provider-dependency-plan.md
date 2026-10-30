# Provider dependencies and connection ownership

Status: planning, researched 2026-10-29. The user selected the latest Npgsql driver. **Npgsql 10.0.3 is the selected planning baseline**, the latest stable release listed today, with a native net10.0 asset. Recheck the latest stable release when implementation starts and pin that exact version. Other selections below are engineering recommendations; no packages have been installed or provider tests run.

This develops D-01/D-06/D-09 and the connection requirements of [execution timing](execution-defaults-and-timing.md), [PostgreSQL operations](postgres-schema-and-operations.md) and [upgrades](postgres-upgrades-and-compatibility.md).

## 1. Dependency selections

| Dependency | Owner | Baseline and disposition |
|---|---|---|
| Npgsql | Spindle.Postgres | **10.0.3 selected** under the user's latest-stable direction; net10.0 asset available |
| Microsoft.Data.Sqlite | Spindle.Sqlite | **10.0.14 recommended**, current stable; start with its default native SQLite bundle |
| StackExchange.Redis | Spindle.Redis | **3.4.20 selected planning pin**; runtime acceptance depends on the selected AOF-always profile prototype |
| System.Text.Json, diagnostics and time primitives | Contracts/core as needed | Use net10.0 framework APIs; no extra package solely to duplicate framework functionality |
| Microsoft.Extensions abstractions/hosting | Core as needed | Select and pin only the actual required packages at SP-002; provider package numbers do not determine their versions |
| Cronos / NodaTime | Internal scheduler module | D-08 now recommends 0.13.0 / 3.3.7 with an explicit TZDB 2026f artifact/hash; adapter conformance remains open |
| Inertia adapter/client | Dashboard | Inertia.Net 1.5.3 with Inertia React/core 3.9.1 and React 19.3.1; [selected integration](dashboard-integration-closure.md), tests pending |

Version evidence: [Npgsql 10.0.3](https://www.nuget.org/packages/Npgsql/10.0.3), [Microsoft.Data.Sqlite 10.0.14](https://www.nuget.org/packages/Microsoft.Data.Sqlite/10.0.14), [StackExchange.Redis 3.4.20](https://www.nuget.org/packages/StackExchange.Redis/3.4.20). A dependency's older target assets do not broaden Spindle's .NET 10+ support floor. Package compatibility is not certification of the storage protocol.

Use explicit SQL and provider APIs initially. EF, Dapper and provider-specific telemetry exporters are not required dependencies. Hosts may use them independently.

## 2. Npgsql connection and transaction ownership

Npgsql supplies thread-safe, long-lived data sources and pooled connections. Spindle should keep a small, explicit set of data sources per configured store rather than construct one per operation. Each atomic command opens one connection, starts one transaction and retains that connection through commit/rollback. All gate, receipt, job and permit statements belong to that transaction. [Npgsql basic usage](https://www.npgsql.org/doc/basic-usage.html)

Proposed connection roles:

| Role | Work | Ownership/budget |
|---|---|---|
| Control pool | Renew, Finish and their receipt resolution | Reserved bounded capacity; ordinary dispatch/dashboard work cannot borrow it |
| Work pool | Dispatch, Reserve, Begin, scheduling, recovery and bounded projections | Shared bounded capacity with admission limits by operation class |
| Notification session | Optional LISTEN/wake-up listener | Dedicated session outside transaction pools; reconnect then reconcile by polling |
| Migration session | Deployment runner and session advisory lock | Dedicated physical primary connection retained across migration phases; accounted separately |
| Caller transaction | Explicit transaction staging | Caller owns connection, commit, rollback and disposal |

Separate pools protect access to connections, not access to a stalled server or locks. Deadline checks and the independent lease watchdog remain mandatory. [D-07](capacity-defaults-and-budgets.md) sets pool sizes, queue bounds and per-group budgets; an enabled outbox relay adds two source connections per group. In multi-host configurations account for all underlying host pools; one configured pool maximum is not automatically the deployment maximum.

For driver-owned connections, propose explicit transactions and `Enlist=false`, so ambient application transactions cannot accidentally hold worker transitions open. Explicit staging uses the supplied transaction and never substitutes a new connection, commits it, or retries its business transaction. If the host supplies data sources, validate/document required settings and ownership; do not create secret-derived duplicate pools or dispose caller-owned sources implicitly.

Keep initial execution on ordinary explicit connections. Defer multiplexing and automatic statement preparation until their transaction/session behavior and workload benefit are demonstrated. Use dedicated direct sessions for migrations and notifications; transaction-pooling proxies need separate certification and cannot supply session affinity merely because ordinary SQL works through them.

## 3. Failure and timeout integration

The inspected [NpgsqlTransaction implementation at v10.0.3](https://github.com/npgsql/npgsql/blob/v10.0.3/src/Npgsql/NpgsqlTransaction.cs) routes CommitAsync to a connector COMMIT command; rollback is a separate path. This is source inspection, not an executed failure test. Spindle's inference: a missing COMMIT response must still follow the existing uncertain-outcome/receipt protocol. Choosing a current driver does not remove that requirement.

1. One operation deadline covers admission wait, pool acquisition, connection establishment, SQL, commit and response handling. Do not reset the full allowance at every stage.
2. Configure provider timeouts/cancellation within that budget. Npgsql has separate connection, command and cancellation-response timeouts; cancellation is not evidence of rollback. A late task cannot restore locally expired execution authority. [Connection parameters](https://www.npgsql.org/doc/connection-string-parameters.html)
3. Retry a whole driver-owned transaction only when rollback/non-application is established, or replay through the existing operation-ID protocol. Keep the same command identity and fingerprint. Serialization/deadlock failures are transaction retry candidates, not a reason to rerun arbitrary application code.
4. I/O loss, cancellation or timeout around commit is potentially ambiguous. Resolve the receipt within the command's resolution budget; do not generate a fresh operation ID to hide uncertainty.
5. Resolve receipts and ownership against the authoritative primary. Npgsql can select/connect to hosts, but does not synchronize their histories and does not implicitly replay a failed command on another server. HA durability remains a declared server/deployment profile. [Npgsql failover behavior](https://www.npgsql.org/doc/failover-and-load-balancing.html)

Keep parameter logging and server error-detail inclusion disabled by default. Use parameterized values, validated/quoted schema identifiers and schema-qualified objects. Network deployment examples should use authenticated, certificate-verified transport; do not inherit an opportunistic encryption setting as Spindle's production recommendation. Caller-provided authentication/rotation remains host-controlled.

## 4. PostgreSQL server and encoding targets

Recommend PostgreSQL **18 as the reference major**, with **16–18 as the initial certification targets**. Today's official minor versions are 16.15, 17.11 and 18.6. These are proposed test targets, not a claim of tested support. Keep baseline SQL compatible with 16 unless a reviewed feature deliberately raises the minimum. Pin exact server images and digests when the harness is created. [PostgreSQL version policy](https://www.postgresql.org/support/versioning/)

Npgsql's version and PostgreSQL's version are independent. Selecting Npgsql 10 does not mean targeting PostgreSQL 10. Certify a direct-primary profile first; add proxy/HA profiles with their own failover, session and acknowledged-history tests.

Use explicitly typed parameters/readers for IDs, counters, bytea payloads and capability arrays. Normalize queue metadata timestamps to the common millisecond contract and UTC before binding; do not alter original payload bytes. Npgsql maps timestamptz to UTC values; it does not preserve an original time-zone ID. Scheduler zones therefore remain separate fields. Avoid legacy timestamp switches and implicit object mappings in durable codecs. [Date/time mappings](https://www.npgsql.org/doc/types/datetime.html)

## 5. SQLite and Redis consequences

Microsoft.Data.Sqlite's default package brings a SQLitePCLRaw native bundle. At restore, record the resolved native packages/assets and at runtime record sqlite_version()/sqlite_source_id(); the managed package version alone is insufficient to identify the engine. Alternative native/system bundles require separate validation. [Native bundles](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/custom-versions)

Its async ADO.NET methods execute synchronously. Keep the existing bounded SQLite executor, bounded admission and independent lease watchdog; replacing synchronous calls with Async-suffixed calls does not provide asynchronous I/O. Verify per-connection settings, busy/lock behavior, checkpoints and multi-process same-host contention. [SQLite async limitations](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/async)

StackExchange.Redis 3.4.20 is the selected client baseline for SP-018. The [Redis closure](redis-design-closure.md) selects direct-primary AOF-always, noeviction and fail-fast bounded lanes. It does not require WAITAOF connection pinning. Pin the exact Redis 8.2 server build/image in the harness and test acknowledged restart/persistence behavior; the server family is a certification target, not a latest-release claim.

Required prototype evidence: pending intent precedes all projections, AOF-always replies and fresh replay-confirmation writes satisfy the durability boundary, connection loss cannot falsely certify a read receipt, and backlog/reconnect never grants expired ownership. Writes that fail or have ambiguous persistence remain unknown. A future everysec+WAITAOF profile would separately require proven physical-connection affinity; it is outside initial scope. Preserve the [BullMQ review](bullmq-redis-review.md) and failover limits.

## 6. Version maintenance and certification

Pin direct dependencies centrally when projects are created; no floating package versions. Record the actual transitive restore graph, licenses and native assets in build evidence. A dependency's published minimum version is not a resolved transitive pin. Keep repository builds reproducible without claiming that a library lock file dictates downstream consumers' dependency graphs.

Recheck stable releases before implementation and release; upgrades are explicit changes with provider contract, fault and wire-vector checks. Add newer compatible stable versions through that process. A new dependency release must not silently alter payload profiles, command semantics or the .NET floor.

Proposed initial host certification: .NET 10 on Linux x64 and Windows x64, with exact OS/container versions recorded by SP-002. Other operating systems/architectures and Native AOT remain unclaimed until tested; SQLite native loading requires its own checks. This matrix is an engineering recommendation, not a new user requirement.

## 7. Planned acceptance evidence

| Case | Required evidence | Work items |
|---|---|---|
| DEP-01 | Exact direct/transitive graph restores and compiles on net10.0; package boundaries stay intact | SP-002 |
| DEP-02 | Transaction staging uses caller connection/transaction; ordinary worker calls do not accidentally enlist in ambient transactions | SP-005–006 |
| DEP-03 | Saturate work connections; Renew/Finish retain bounded access; server/lock stalls still trigger local authority expiry | SP-005, SP-009, SP-029 |
| DEP-04 | Lose COMMIT response, cancel and disconnect; distinguish rollback from ambiguity and resolve/replay without duplicate effects | SP-003, SP-005–008 |
| DEP-05 | PostgreSQL 16–18 DDL, encodings, receipt/lock behavior and upgrade paths pass; session-affinity failures are detected | SP-005, SP-026, SP-028 |
| DEP-06 | SQLite native assets load on advertised platforms; bounded execution, cancellation limits and multi-process contention hold | SP-019, SP-028 |
| DEP-07 | Redis AOF-always acknowledgement/fresh replay confirmation, reconnect, backlog and persistence failures satisfy the selected profile | SP-018, SP-025, SP-028 |
| DEP-08 | Provider upgrades preserve wire/profile vectors and do not leak parameters, credentials or payloads through diagnostics | SP-022, SP-026–028 |

All cases are planned, not executed. The [D-07 defaults/budgets packet](capacity-defaults-and-budgets.md) specifies fairness, rate-policy and scaler values, I/O/connection budgets and CAP workloads. The [D-08 scheduler packet](scheduler-evaluator-and-defaults.md) now supplies parser/data versions and implementation/conformance requirements. All remaining planning is closed in the [final baseline](../architecture-baseline.md).
