# 15 — G-2 test plan: distributed controls, capacity and scheduling (SP-011 … SP-017)

**Sources:** [07-test-plan-g1.md](07-test-plan-g1.md) (harness, traits, report shape), [08-controls-and-completion.md](08-controls-and-completion.md), [12-schedule-evaluator.md](12-schedule-evaluator.md), [driver-contract-suite.md](../architecture/driver-contract-suite.md), [architecture-acceptance.md](../architecture-acceptance.md) "G-2", [v1-delivery-plan.md](../v1-delivery-plan.md) §5, [implementation-backlog.md](../implementation-backlog.md) §4, [performance-acceptance-plan.md](../architecture/performance-acceptance-plan.md), [capacity-control.md](../architecture/capacity-control.md), [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md), [scheduling-internals.md](../architecture/scheduling-internals.md), [scheduler-evaluator.md](../architecture/scheduler-evaluator.md), [transactional-outbox.md](../architecture/transactional-outbox.md), [management-and-dashboard.md](../architecture/management-and-dashboard.md), [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md), [provider-dependency-plan.md](../architecture/provider-dependency-plan.md), [docs/plans/next-milestone-draft.md](../plans/next-milestone-draft.md).

This document lists the cases gate **G-2** (milestone M3) requires, the case IDs inferred here, the harness additions, the G-2 measurements and the pass rule. The suites, traits, harness contract (07 §1–§2) and the case definitions already in 07 are reused unchanged. A case here that 07 defined (CMD, ISO, CFG, UNI, PGX, CAP, WRK, TIME) keeps 07's definition. G-2 adds only the sub-parts that 07 deferred, or the new operations it runs on.

## Changes (2026-11-01, G-2 owed items for SP-011, SP-015, SP-017)

- (2026-11-01) CFG-01 (SP-011) is `OperationVersionTests`, not `MigrationTests`. It covers all 21 operation versions M3 added, not only the five of revision 2: the controls and `EventsSince` (rev 2), `ReceiveOutbox` (3), `ActivateCapabilities` and `EditRatePolicy` (4), `ReadinessProbe/2` and `ListUnsupportedBacklog` (5), the eight schedule operations (6), and `SetPoolOverride`, `EstimateDemand` and `RecordScalingEvent` (7). Each version is in the manifest from its revision on and in no earlier one. At schema revision current ± 1, every receipted operation is `UnsupportedVersion`/`schema_incompatible`, every read throws, and `RecordScalingEvent` returns false; the committed snapshot is unchanged. `SetQueueState/1` (public path) is in the gate check, but its manifest version is the M2 one.
- (2026-11-01) The SER rows of §3 use the existing case SER-11 (golden vectors under both encoders, 02 §13): `SER-11` with `Item=SP-015` for the definition-fingerprint vectors (`tests/vectors/wire/schedule-definition.json`), and `SER-11` with `Item=SP-017` for the outbox hash-domain vectors (`tests/vectors/wire/outbox.json`, plus the stored-row check on both sources).
- (2026-11-01) DEP-01 rerun (SP-015) is `PackageGraphTests` (Core, Pure). It runs `eng/check-package-graph.sh`, checks the Cronos/NodaTime pins against `Directory.Packages.props` and every `packages.lock.json`, and checks that no public or protected member of `Runwick` or `Runwick.Contracts` exposes a Cronos or NodaTime type (12 §1).
- (2026-11-01) Pass rule 4 for the Contract/Fault cases uses the harness `CommittedHistory` check (`DriverFixture.HistoryViolations()`). It rebuilds each job's history from `job_attempts`, the closing receipts and the cancel events, replays it through the ReferenceModel, and compares the result with the committed job and generation. Renewals are not durable, so leases are synthetic and lease liveness is not rechecked. Rate deferrals are not in the model. Self-test: `CommittedHistoryTests` (case `HARNESS-history`, `Item=SP-011`).
- (2026-11-01) `RequiresDocker` stays as 07 §1.1 defines it: only on tests that need container control. A real-store test without a restart or clock step does not carry it.

## Changes (2026-10-31, SP-011)

- New document. §3 inventory for all of M3. §4 defines the SP-011 cases (TXC, CTL, EVT, MODE are inferred, I-1). §5–§10 name the cases of SP-012 … SP-017; each of those items completes its section with definitions when its own spec (09, 10, 11, 13, 14) is written.

## Changes (2026-11-01, SP-013 owed cases)

- 2026-11-01: The capacity-control §6 rate rows get inferred IDs RATE-05 … RATE-10 (I-1), one per row. The enlargement/cleanup race keeps no ID and stays deferred to SP-021 (it needs the cleanup predicate).
- 2026-11-01: §3 RATE row now reads RATE-01 … RATE-10. §6 lists the tests for RATE-05 … RATE-10, UPG-02, UPG-06 and PGX-01.
- 2026-11-01: The "old binary" of UPG-02 / UPG-06 is a peer process whose known-capability set lacks `rate_admission` (peer command `openOldBinary`). The gate (03 §10.3) and V-24 read only that set, so the peer behaves as a pre-SP-013 binary on every path the gate guards. No separately built old binary is used.
- 2026-11-01: PGX-01 at the current schema revision (7) uses the 04 §7.15 harness gate change (admin SQL) for `drain`, `maintenance` and an added required capability. The capability added is one this binary does not know (`teleport`), which is how "a required capability added" looks to this binary. The activation of `rate_admission` racing an old-binary Begin is UPG-02.

## Changes (2026-11-01, SP-014 and SP-016 owed cases)

- I-1 gains SCL-02 … SCL-05, one per capacity-control §6 scaler row (inferred).
- The CAP-11 automatic-mode sub-part is defined as: during a store outage every automatic target holds (unknown demand, no growth feedback from failing finishes, attempts bounded), and a late estimate is abandoned at its deadline so at most one estimate statement per worker is ever outstanding.
- The CAP-01 automatic-mode sub-part is defined as: hung handlers never hold more tickets than the clamped automatic target; an override above the maximum is clamped to it.
- §7 and §9 list the new tests; the owed lists shrink to what is still open (scaling-event retention, SP-021; P-06 scheduler half, measured separately).

## 1. Suites, projects and traits

As 07 §1. Additions:

| Project | Kind | Hosts |
|---|---|---|
| `tests/Runwick.Core.Tests` | existing | `Scheduling/` evaluator cases (SP-015, Pure) |
| `tests/Runwick.Postgres.Tests` | existing | Controls, events, R6, deployments (SP-011); readiness (SP-012); rates (SP-013); scaler store half (SP-014); scheduler storage (SP-016); `ReceiveOutbox/1` destination half (SP-017) |
| `tests/Runwick.Outbox.Tests` | new (SP-017) | Outbox source adapters (PostgreSQL, SQLite) and relay, Contract/Fault |
| `tests/Runwick.Workloads` | existing | P-04, P-05, P-06, P-09 and the G-1 measurement reruns (§11) |

Traits are unchanged: `Category`, `Item` (`SP-011` … `SP-017`), `Case`, `RequiresDocker`.

## 2. Harness additions

| Addition | Item | Purpose |
|---|---|---|
| `WorkerHost.RegisterWork(builder, queue)` and the `queue` parameter of `WorkerHost.Create` | SP-011 | Producer-only and control-only providers register the same `test.work` contract; worker hosts on disjoint queues (MODE-03) |
| `HandlerScript` behaviours read the attempt scope from `IJobContext.Items[typeof(IServiceProvider)]` | SP-011 | Handlers resolve attempt-scoped `IPostgresJobCompletion` (TXC) |
| `WorkerEngine.StagedCompletionsObserved` counter | SP-011 | Proves an attempt settled by receipt observation without a Finish (TXC-02, MODE-05) |
| Multi-replica peers (≥ 3 worker processes per store) | SP-012 | Presence, fairness and P-05 at 1/3/6 replicas |
| Scheduler peer (`AddScheduler()` in the peer process) | SP-016 | Scheduler role independent of workers |
| Outbox source fixture (a PostgreSQL source database and a SQLite source file, each separate from the destination store) | SP-017 | Source adapters and relay |
| Controlled store clock (07 I-9 faketime) | SP-013, SP-016 | Backward time for rates, DST and misfire cases |
| `G2CaseTable` in the report builder | G-2 report | One row per §3 case from trait-tagged results, as `G1CaseTable` |

Roles in MODE cases are separate `ServiceProvider`s, each with its own connection identity (`Application Name`) against the same store. Within one test process that is an independent role configuration, not a separate OS process. The SP-012 rerun of MODE-01 … MODE-05 uses peer processes for the worker roles.

## 3. G-2 case inventory

| Family | Source document | G-2 cases | Item |
|---|---|---|---|
| END | driver-contract-suite §3 | END-02, END-03 | SP-011 |
| MGMT | management-and-dashboard §6 | MGMT-04 (store half; the HTTP half is SP-020) | SP-011 |
| CTL | this document (inferred) | CTL-01 … CTL-05 | SP-011 |
| EVT | this document (inferred) | EVT-01, EVT-02 | SP-011 |
| TXC | this document (inferred) | TXC-01 … TXC-06 | SP-011 |
| MODE | this document (inferred) | MODE-01 … MODE-05 | SP-011; rerun with presence in SP-012 |
| UNI | 07 §7 | UNI-04 manual-retry sub-part | SP-011 |
| PGX | postgres-schema-and-operations §7 | PGX-04 manual-retry and scope-edit sub-parts (SP-011), materialization sub-part (SP-016); PGX-06 missing-payload subset (SP-011); PGX-07 subset (SP-012); PGX-01, PGX-03 (SP-013); PGX-02 schedule half (SP-016) | several |
| ADM | 07 §5 | ADM-04 rerun through the public pause path | SP-011 |
| CMD | 07 §4 | CMD-01 … CMD-05 on every new receipted operation (SP-011, SP-016, SP-017); CMD-06 rate half (SP-013) | several |
| ISO/CFG/OPS | 07 §9, driver-contract-suite | ISO-01 for controls (SP-011), schedules (SP-016) and delivery identities (SP-017); CFG-01 for the new operation versions (SP-011); OPS-03 for the administrative class (SP-011) | several |
| CAP | capacity-defaults-and-budgets §7 | CAP-01 fixed remainder, CAP-02 … CAP-04 (SP-012); CAP-05 (SP-013); CAP-06 … CAP-08, CAP-01/10/11 auto sub-parts, CAP-12 scale-down sub-part (SP-014) | several |
| RDY | this document (inferred) | RDY-01 … RDY-03 | SP-012 |
| OBS | driver-contract-suite | driver-contract-suite:OBS-01 | SP-012 |
| WRK | 07 §10 | WRK-15, WRK-19 (SP-012); WRK-02 (SP-014) | several |
| RATE | capacity-control §6 | RATE-01 … RATE-04; RATE-05 … RATE-10, the §6 rate rows (inferred, I-1) | SP-013 |
| TIME | 07 §11 | TIME-06 rate-deferral sub-part | SP-013 |
| UPG | postgres-upgrades | UPG-02 / UPG-06 subset (old binary cannot Begin after activation; rollback after activation rejected) | SP-013 |
| SCL | this document (inferred) | SCL-01 | SP-014 |
| SCHED | scheduler-evaluator §8 | SCHED-01 … SCHED-08, SCHED-14 (SP-015, Pure); SCHED-09 … SCHED-13, SCHED-15 (SP-016) | several |
| DEP | provider-dependency-plan §7 | DEP-01 rerun with Cronos/NodaTime | SP-015 |
| SER | identity-and-serialization §7 | Definition-fingerprint vectors (SP-015); outbox hash-domain vectors (SP-017) | several |
| SCH | scheduling-internals §10 | SCH-01 … SCH-05 and the §10 table | SP-016 |
| OUTBOX / OUT | transactional-outbox, driver-contract-suite | OUTBOX-01 … OUTBOX-07, OUT-01, OUT-02, TX-01 … TX-04 analogues on both sources | SP-017 |
| Measurements | performance-acceptance-plan, §11 | G1-M1/G1-M3 rerun, P-04, P-05 subset (SP-012); P-06 rate half (SP-013); P-06 scheduler half (SP-016); P-09 (SP-017) | several |

Not in G-2 (later gates): MGMT HTTP half, authorization, DASH (SP-020); RET-01, retention-backed payload guarantees and the grant/history cleanup tasks (SP-021); the SQLite twin of transactional completion and SQLite as a queue provider (SP-019); REDIS (SP-018/SP-025); the full UPG matrix (SP-026); HA/failover classes.

### I-1 Inferred case IDs

These acceptance bullets have no ID in the source documents. The IDs are defined here only, as 07 I-2 did for G-1:

| Inferred ID | Item | Covers |
|---|---|---|
| TXC-01 … TXC-06 | SP-011 | R6 transactional completion (08 §8) |
| CTL-01 … CTL-05 | SP-011 | Revisioned controls not covered by END/MGMT (08 §3) |
| EVT-01, EVT-02 | SP-011 | Event cursor (08 §7) |
| MODE-01 … MODE-05 | SP-011, SP-012 | The five deployments (08 §9) |
| RDY-01 … RDY-03 | SP-012 | Readiness/presence bullets |
| SCL-01 | SP-014 | The R4 scaler trace |
| RATE-05 (2026-11-01) | SP-013 | capacity-control §6 "Last rate grant across replicas": at least three worker processes, no more than N grants per rolling window, no partial multi-scope charge |
| RATE-06 (2026-11-01) | SP-013 | §6 "Rate rejection after reservation": exact permits released, attempt closed, budget and abandonment unchanged, future eligibility set |
| RATE-07 (2026-11-01) | SP-013 | §6 "Begin commit response lost": the replay neither charges nor starts twice (Applied and deferred Begins) |
| RATE-08 (2026-11-01) | SP-013 | §6 "Handler failure / crash after Begin", crash half through the fault harness: the grant stays counted, recovery charges nothing, the retry needs a new grant |
| RATE-09 (2026-11-01) | SP-013 | §6 "Multiple concurrency/rate scopes": opposite declared order under barriers, no lock-order cycle |
| RATE-10 (2026-11-01) | SP-013 | §6 "Backward database time" on the controlled store clock: logical rate time never reverses, no early refill |
| SCL-02 | SP-014 | capacity-control §6 "replica count and capability mix change": demand shares adapt (added 2026-11-01) |
| SCL-03 | SP-014 | §6 "oscillating backlog and long jobs": sample/cooldown/step rules hold and no work is cancelled (added 2026-11-01) |
| SCL-04 | SP-014 | §6 "storage outage and pending finishes": no scale-up feedback loop, attempts bounded (added 2026-11-01) |
| SCL-05 | SP-014 | §6 "zero minimum and missing metrics": discovery survives and bootstrap recovers (added 2026-11-01) |

## 4. SP-011 cases

Every SP-011 case runs on the direct-primary profile unless it says otherwise. Each mutation case also checks that the committed snapshot satisfies the ReferenceModel invariants.

#### END-02 — Cancel of a waiting job

Cancel/1 (T17) on a pending job closes it as `cancelled` (`cancelled_while_waiting`) with no attempt, releases its active key (a new job with the same key is then admitted), writes one audit row and emits `job_cancelled` with the actor. The race with Reserve is covered by the expected-revision check: a Reserve that committed first moves the revision, and the Cancel then gets `revision_conflict` (CTL-03).

#### END-03 — Cancel of an owned attempt is a request, not a stop

Cancel/1 on a running job records `cancel_requested_*` (T22). The lease and permits stay, and the next Renew reports `CancellationRequested`. A second Cancel gets `cancel_already_requested`. An unsuccessful Finish then closes the job as `cancelled` (`cancellation_requested`). Nothing asserts that the handler stopped. MODE-04 shows the engine cancelling the handler token on Renew.

#### MGMT-04 (store half) — Effect, receipt and audit commit together

`FaultSpec.Throw()` at `BeforeCommit` on a Cancel leaves no effect, no receipt and no audit row. The same command afterwards applies exactly once. The HTTP half (a failed audit cannot publish success) is SP-020.

#### UNI-04 (manual-retry sub-part)

ManualRetry/1 of a failed keyed job creates generation 2 (`created_reason = 'manual_retry'`, `started_count = 0`) and rebinds the active key to the same job. The new generation runs with a fresh budget; lifetime counters continue.

#### PGX-04 (manual-retry sub-part)

ManualRetry/1 of a cancelled job whose active key is now held by another job gets `UniquenessConflict` naming the holder. Nothing changes and no audit is written. The scope-edit sub-part is CTL-01. The materialization sub-part is SP-016.

#### PGX-06 (missing-payload subset)

ManualRetry/1 on a job whose payload was pruned (harness pruning until SP-021) is `HistoryUnavailable` / `payload_unavailable` with no change.

#### ADM-04 (rerun through the public path)

SetQueueState/1 on the public path pauses `default` (revision + 1, `updated_by` = the actor). Dispatch and the owned attempt continue (Begin, Finish), and Reserve gets `QueuePaused`. Pausing again gives `no_change`. A stale revision gives `VersionConflict`. Resume restores Reserve. Two audit rows are written.

#### CMD-01 … CMD-05, OPS-03, ISO-01, CFG-01 on the control operations

`ControlProtocolTests`, each over Cancel, ManualRetry, RerunSucceeded, SetQueueState and EditScopePolicy where marked:

| Case | Proves |
|---|---|
| CMD-01 (all five) | The same command sent twice concurrently and once more sequentially applies once. Only one reply is fresh, every reply has the same DecidedAt, and there is one receipt and one audit row |
| CMD-02 (all five) | The same ID with a changed actor, reason or NotAfter is `CommandConflict`, and the committed snapshot is unchanged |
| CMD-03 (all five, store level; Cancel through `IJobControl`) | An in-process reply drop after commit is `StoreOutcomeUnknownException`, and the same command replays the stored reply. The preview service resends the same command itself and returns `Replayed = true` with one receipt and one audit |
| CMD-04 (Cancel) | A Cancel waiting on a row lock past its NotAfter is `CommandExpired` / `ObservedNoEffect`, decided at or after NotAfter, with no receipt, audit or change |
| CMD-05 (ManualRetry) | After the receipt is pruned, the old command replays as `CommandExpired` and the snapshot is unchanged: no second generation |
| OPS-03 (all five) | NotAfter beyond 10 min is `InvalidRequest` with no receipt. Receipts are `durable` class under the operation's name |
| ISO-01 | Application B cannot address A's job (`NotFound`). A's cancel and pause leave B's same-named queue and key untouched, write no audit in B, and emit no event visible to B |
| CFG-01 | `MigrationTests`: a revision-2 manifest lists the five new operation versions, and the gate rejects any schema revision other than the binary's |

#### CTL-01 — A capacity decrease revokes nothing (PGX-04 scope-edit sub-part)

A resource scope of capacity 2 with two live permits is edited to 1. The edit succeeds (policy revision + 1) and reports 2 live permits. Both permits stay, and the holders keep renewing. A third job is `CapacityBlocked` until both release.

#### CTL-02 — Membership cannot be edited

The request has no membership field. EditScopePolicy/1 on a scope no job registered is `NotFound` / `scope_not_registered`, and an unknown scope kind is `InvalidRequest`. Requirements are unchanged.

#### CTL-03 — A stale position is a conflict with no change

A Cancel whose expected revision predates a Reserve gets `VersionConflict` / `revision_conflict` with detail `actual=<generation>/<revision>`. It writes no receipt and no audit, and leaves no cancel request. Stale SetQueueState is ADM-04.

#### CTL-04 — One receipt and one audit per success

A successful Cancel writes one receipt and one audit row. A replay returns the same value and DecidedAt with `Replayed = true` and writes nothing. A rejection (the job is already terminal) writes neither. RerunSucceeded applies only to succeeded jobs, and ManualRetry not to them. Actor bounds (08 §2) are rejected as `InvalidRequest` naming the field.

#### CTL-05 — Bulk controls return per-item results

`IJobControl.CancelManyAsync` over a valid target, a stale target and a missing job returns `[success, revision_conflict, job_not_found]` in target order. Each item is its own command and receipt. Only the success writes audit.

#### EVT-01 — Cursor rereads are stable

Ten dispatches drained with page sizes 3 and 4 give the same ordered cursor list, ordered by `(tx_id, event_id)`. Reading from any returned cursor continues exactly after it. Every event has `schema_version = 1`.

#### EVT-02 — A slow writer is never skipped

Writer A inserts an event and stays open. Writer B commits after A started. A page from the last cursor returns nothing past A's horizon and reports `Partial`, with `Next` unchanged. After A commits, both rows are returned in `(tx_id, event_id)` order. Pages never exceed the requested size.

#### TXC-01 — Caller rollback leaves the attempt owned

The handler stages a completion and rolls back. The attempt stays owned. The normal path settles it from the pipeline's result. If the process dies before then, the attempt stays owned until its lease expires, and Recover applies the usual precedence.

#### TXC-02 — Caller commit finalizes exactly once

The handler writes a business row, stages the completion and commits. The job is `succeeded` with one ledger row and one Finish receipt under the staged command ID. The engine sends no Finish (`FinishesSent = 0`, `StagedCompletionsObserved = 1`). A second `CompleteInTransactionAsync` in the same attempt throws `JobCompletionException` (`completion_already_staged`).

#### TXC-03 — Lost caller COMMIT reply

Profile `DirectPrimaryViaProxy`, with the business connection's COMMIT reply dropped. The commit happened. The engine resolves the staged command by receipt, the job is `succeeded` once, and the handler is not invoked again.

#### TXC-04 — A stale owner gets `ownership_lost`

The lease is expired by admin SQL and the job is recovered before the handler stages. `CompleteInTransactionAsync` throws `JobOwnershipLostException`, and the caller must roll back. No business row commits under the stale attempt.

#### TXC-05 — `staged_completion_not_committed`

The handler stages and then returns without committing (the transaction is rolled back, or disposed while open). The attempt finishes `PermanentFailure` with code `staged_completion_not_committed`. Parameterized over leaving the transaction open versus rolling it back.

#### TXC-06 — Exception after a committed completion

The handler commits a staged completion and then throws. The commit stands: the job is `succeeded`, and the exception is kept as a diagnostic only.

#### MODE-01 … MODE-05 — The five deployments (08 §9)

| Case | Roles | Proves |
|---|---|---|
| MODE-01 | One provider: producer, worker and control | Dispatch → succeeded; pause observed through the cursor; a held job cancelled; resume → processed |
| MODE-02 | Producer-only provider (no `WorkerEngine`), two worker providers | 20 jobs from the producer, each processed once by one of the workers; the producer's pause is observed through a worker's cursor |
| MODE-03 | Two worker-only providers on disjoint queues, one producer per queue | Each worker takes only its own lane; pausing one lane holds that lane only |
| MODE-04 | Control-only provider (no `WorkerEngine`) and a worker | The control role pauses, cancels a held job (`job_cancelled` observed with the actor), resumes; cancels a running job (T22, observed on Renew, handler token cancelled); a job dispatched by the control role is processed |
| MODE-05 | Producer, control, and a worker whose handler uses staged completion, concurrently | Every third queued job cancelled while paused; the producer keeps dispatching across the resume; every other job succeeds once with one ledger row and no engine Finish; no ledger row for cancelled jobs |

## 5. SP-012 cases

CAP-01 (fixed-mode remainder), CAP-02, CAP-03 (real store), CAP-04, driver-contract-suite:OBS-01, WRK-15, WRK-19, PGX-07 subset, MODE-01 … MODE-05 with presence and peer-process workers, and:

- **RDY-01** — A 10,000-job blocked backlog causes no per-job transaction during readiness probing.
- **RDY-02** — Stale presence expires by store time without touching leases.
- **RDY-03** — Jobs no live worker supports stay visible as such, with the "no recently advertised capable worker" diagnostic.

Built in `ReadinessTests` (09 §6 has the full table): RDY-01 with WRK-15 (500-job backlog behind a full queue scope: no Reserves, receipts or events over 10 s), CAP-04 (cross-queue independence, ≥ 50 ms probe spacing under 5,000 wakes), RDY-03 (both diagnostic reasons), RDY-02 with WRK-19 (expired and pruned presence, renewals continue), driver-contract-suite:OBS-01 (wakes suppressed: work, pause/resume and recovery found by polling), and the MODE presence repeat (subscription and capability rows per role).

Owed before acceptance: CAP-01 fixed remainder; CAP-02 and CAP-03 real-store halves (no `Case` trait exists for them in G-1 either); the RDY-01 type-scope half at 10,000 jobs; PGX-07 plans at 10⁵ / 10⁶ rows; MODE-01 … 05 with peer-process workers; G1-M1 / G1-M3 rerun, P-04, P-05.

## 6. SP-013 cases

RATE-01 … RATE-04, CMD-06 rate half, CAP-05, PGX-01, PGX-03, TIME-06 rate-deferral sub-part, UPG-02/UPG-06 subset, and the capacity-control §6 rate rows (last grant across replicas, rejection after reservation, lost Begin reply, crash after Begin, opposite-order scopes, enlargement/cleanup race, backward time via the controlled store clock).

Built in `RateAdmissionTests` (PostgreSQL only; the SQLite twin is SP-019):

| Case | Test | Proves |
|---|---|---|
| PGX-03 (rate half) | `Rate_scopes_are_refused_until_the_application_activates_rate_admission` | Dispatch with rate scopes is `rate_admission_not_active` with nothing written; activation, replay, `no_change`, unknown capability; one audit row |
| RATE-01 | `Competing_begins_get_at_most_n_grants_per_rolling_window` | 5 concurrent Begins on 2 per 3 s: 2 Applied, 3 RateLimited with EligibleAt ≥ oldest grant + W; no 3 s interval holds more than 2 grants |
| RATE-02 | `One_full_scope_defers_without_a_partial_charge_and_releases_permits` | All-or-none across job-type and resource scopes; permits released; counters 0/0/0; job pending; one `attempt_admission_deferred` event |
| RATE-03 | `A_failed_execution_keeps_its_grant_and_the_retry_needs_a_new_one` | No refund on RetryableFailure; the retry's Begin is deferred from `retrying` |
| RATE-04 | `Window_edits_freeze_admission_and_logical_time_never_goes_backward` | `no_change`, freeze to `not_before`, `revision_conflict`; a scope time ahead of the store clock is the charge instant |
| CMD-06 (rate half) | `A_replayed_deferral_returns_the_same_decision_without_a_second_close` | Replay returns the stored deferral; one close; no extra grant |
| CAP-05 | `Rate_policy_bounds_are_validated_at_registration_and_on_edit` | Registration bounds (limit, window, ms precision, scope count, two job-type rates) and edit bounds; unknown scope |
| TIME-06 (rate sub-part) | `The_worker_retires_a_deferred_begin_and_runs_the_job_once_admitted` | Real `WorkerHost`: `RateDeferrals ≥ 1`, each job runs once, grants ≥ W apart, unstarted guard 0 |

Added 2026-11-01 (`RateAdmissionReplicaTests`, `RateAdmissionClockTests`, `RateGateRaceTests`):

| Case | Test | Proves |
|---|---|---|
| RATE-05 | `Three_replica_processes_never_grant_more_than_n_per_rolling_window` | Three peer processes, nine jobs, limit 3 per 5 s with a second (resource) scope: the first all-at-once round grants exactly 3; every round ≤ 3; 9 grants in each scope, two per attempt; no 5 s interval holds more than 3 |
| RATE-06 | `A_rate_rejection_after_reservation_releases_exact_permits_and_sets_eligibility` | Only the deferred attempt's permit is released; `admission_deferred`; started/unstarted/execution 0/0/0; `eligible_at` = EligibleAt ≥ grant + W; Reserve then gets NoWork |
| RATE-07 | `A_lost_begin_reply_replays_without_a_second_grant_or_start` | `DropReply` after commit on an Applied and on a deferred Begin: the same command replays (Applied with the same execution number; the stored deferral); one start, one close, no extra grant |
| RATE-08 | `A_crash_after_begin_keeps_the_grant_and_the_retry_needs_a_new_one` | Peer `KillProcess` at `AfterCommitBeforeReply` of Begin: one grant; Recover keeps it; the retry is deferred inside the window and admitted after it with a second grant |
| RATE-09 | `Opposite_order_rate_scopes_lock_in_uuid_order_and_never_deadlock` | Twelve rounds, alternating processes: an admin lock on the higher-UUID scope plus a barrier at `AfterLocksBeforeFreshTime` interleave the two Begins; the opposite-order Begin waits on the first Begin (lower UUID), never on the admin lock; one Applied, one RateLimited on both scopes; no 40P01 |
| RATE-10 | `A_backward_store_clock_never_reverses_rate_time_or_refills_early` | libfaketime server (RequiresDocker): after a 30 s backward step the next Begin is deferred, `last_decision_at` does not go back, EligibleAt covers the remaining logical window |
| UPG-02 (subset) | `After_activation_an_old_binary_cannot_begin_and_keeps_only_settlement_paths` | An old-binary peer works before activation; its Begin racing activation waits on the gate and is refused `capability_unsupported` with nothing written; it cannot Begin a rated job (no grant), Reserve or Dispatch; Renew, Finish and ReleaseReservation stay open |
| UPG-06 (subset) | `Rollback_after_activation_is_rejected` | Lowering `rate_admission` is `InvalidRequest`; repeating it is `no_change`; an old binary cannot complete its handshake; the requirement, revision and audit are unchanged |
| PGX-01 | `Gate_changes_race_every_command_family_at_the_current_revision` (11 rows) | At schema revision 7, for Dispatch, DispatchMany, Stage, Reserve, rate-aware Begin, Renew, Finish, Recover, ReleaseReservation, PublishPresence and RegisterQueue: a command holding the gate commits under the old gate before `drain` / an added capability / `maintenance` commits; a command arriving during the change observes it before any mutation (creation paths refused with nothing written; settlement paths applied under `drain` and refused under `maintenance`) |

Owed before SP-013 acceptance: P-06 (rate decision latency, §11). Deferred: the enlargement/cleanup race (no ID; needs the SP-021 cleanup predicate), `not-applicable` with SP-021 as the reason.

## 7. SP-014 cases

CAP-06 … CAP-08, the CAP-01/10/11 auto sub-parts, CAP-12 scale-down sub-part, WRK-02, the capacity-control §6 scaler rows, and:

- **SCL-01** — The R4 trace 4 → … → 16 reaches 16 under sustained demand, and the symmetric trace reaches the configured minimum, with no running work cancelled.

Built in `ScalerTests` (Core, Pure) and `ScalingTests` (PostgreSQL):

| Case | Test | Proves |
|---|---|---|
| SCL-01 | `Sustained_demand_grows_by_bounded_steps_to_the_maximum`, `Sustained_idleness_shrinks_…`, `Changes_are_at_least_one_cooldown_apart_…` | 4 → 5 → 7 → 9 → 12 → 15 → 16 and 16 → 12 → 9 → 6 → 4; first change after 3 samples; ≥ 30 s apart |
| CAP-06 | `The_service_estimate_is_cold_then_an_ewma_…`, `Occupancy_is_a_time_weighted_mean_…` | Cold 1 s, EWMA, confidence at 20, live lower bound; window integration |
| CAP-07 | `ScalerTests` band, waiver, freeze, unknown, bootstrap, override, membership; `ScalingTests.Pool_overrides_are_revisioned_…`, `Unchanged_targets_are_never_recorded_…` | Every rule of 11 §3.3–§3.4 and §5; revision conflict, `no_change`, replay, bounds; history only on change; unknown ≠ zero |
| CAP-08 | `The_blocked_ratio_needs_twenty_cycles_…`; `The_estimate_shares_backlog_…` | Freeze threshold; real estimate (due count, capable instances, digest, overrides) |
| WRK-02 / CAP-12 (scale-down) | `The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing` | Target 0 stops refill; the running handler completes; nothing cancelled; history rows 1 → 0 → 1 |

Added 2026-11-01 (`ScalerCaseTests`, `ScalerOutageTests` in PostgreSQL; two more `ScalerTests`):

| Case | Test | Proves |
|---|---|---|
| CAP-08, SCL-02 | `ScalerCaseTests.Two_workers_on_one_queue_share_the_backlog_and_the_survivor_takes_it_all` | Two hosts, one queue, 240 due: each estimate reports due 240 and 2 capable instances, desired 7 each; after one stops, the survivor's desired is 12 |
| CAP-06 | `ScalerCaseTests.Each_lane_counts_at_most_2000_due_rows_and_a_full_lane_sets_truncated` | 1,999 + 3 due (future rows excluded) is exact; a lane of 2,000 sets `truncated` and is counted as 2,000; 2,001 still counts 2,000 |
| CAP-07, CAP-11 | `ScalerCaseTests.A_late_estimate_is_unknown_demand_and_never_overlaps_the_next` | Estimate blocked by a lock: each sample fails at its deadline, target holds at 1 with `demand_unknown`, at most one estimate statement waits; after release growth resumes |
| CAP-01, CAP-12, WRK-02, SCL-03 | `ScalerCaseTests.Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing` | Hung handlers never exceed the clamped target (override 50 → 4); reductions to 1 and 0 cancel nothing; three history rows |
| CAP-10 | `ScalerCaseTests.A_pressure_stop_freezes_growth_and_resets_its_evidence` | While admission is pressure-stopped every sample is `frozen:pressure`; growth after resume needs three fresh samples |
| CAP-11, SCL-04 | `ScalerOutageTests.A_store_outage_freezes_automatic_targets_with_pending_finishes` | Store killed with 12 pending finishes: one target throughout, entries ≤ max, attempts bounded, invariants hold |
| SCL-03 | `ScalerTests.An_oscillating_backlog_and_long_jobs_never_move_the_target` | Demand flipping every sample never builds evidence |
| SCL-05 | `ScalerTests.A_zero_minimum_keeps_discovery_through_missing_metrics_and_recovers_from_zero` | Min 0 keeps its bootstrap ticket through unknown samples; from zero, demand regrows after one cooldown |

Owed before SP-014 acceptance: scaling-event retention (06 §1.13). No retention job exists before SP-021; it is owed there.

## 8. SP-015 cases

SCHED-01 … SCHED-08 and SCHED-14 as Pure tests under the pinned TZDB artifact (12 §9), the definition-fingerprint vectors, and DEP-01 rerun. Defined in 12.

## 9. SP-016 cases

SCH-01 … SCH-05 (SCH-05 history pruning by harness pruning), SCHED-09 … SCHED-13, SCHED-15, PGX-04 materialization sub-part, PGX-02 schedule half, CMD-01 … CMD-05 and ISO-01 on the scheduler operations, and the scheduling-internals §10 table.

Built in `ScheduleStoreTests` (PostgreSQL); the case table with test names is 13 §11: SCH-01 … SCH-05, SCHED-09, SCHED-10, SCHED-12, SCHED-13, SCHED-15, PGX-02 (schedule half), CMD-01 … CMD-03, ISO-01.

Added 2026-11-01: `SchedulePlannerTests` (Core, Pure: SCHED-09, SCHED-10, SCHED-13, SCHED-15, SCH-04) and `ScheduleCaseTests` (PostgreSQL: the hosted path and `IJobContext.Schedule`, SCHED-11, the scheduler half of SCHED-14, PGX-04 materialization, CMD-04, CMD-05, and the remaining scheduling-internals §10 rows). The table is 13 §11.

Owed before SP-016 acceptance: P-06 scheduler half (§11, measured separately).

## 10. SP-017 cases

OUTBOX-01 … OUTBOX-07, OUT-01, OUT-02, TX-01 … TX-04 analogues for Stage on both sources, CMD-01 … CMD-05 on `ReceiveOutbox/1`, ISO-01 for delivery identities, and the hash-domain vectors. Definitions are in 14 §9 (project `tests/Runwick.Outbox.Tests`, traits `Item=SP-017`, `Case`); P-09 is 14 §9.4.

## 11. G-2 measurements

| Workload | Item | Recorded against |
|---|---|---|
| G1-M1, G1-M3 rerun | SP-012 | The G-1 values in `docs/reports/sp-010-g1.md` |
| P-04 fairness | SP-012 | performance-acceptance-plan P-04 |
| P-05 contention subset, 1/3/6 replicas | SP-012 | performance-acceptance-plan P-05 |
| P-06 rate half (100 authorizations/s; 1 and 8 scopes; rejected-Begin churn; grant rows and WAL) | SP-013 | performance-acceptance-plan P-06 |
| P-06 scheduler half (1,000 minute schedules; a 1,000-occurrence catch-up) | SP-016 | lag p99 ≤ 3 s; worker lease loss = 0 |
| P-09 outbox | SP-017 | ≥ 100 deliveries/s; p99 ≤ 2 s |

Environment fields, raw-sample retention and the thresholds rule are as in 07 §14.2–§14.5. `eng/run-g2.sh` mirrors `eng/run-g1.sh`. No measurement substitutes for an invariant test.

## 12. G-2 pass rule and report

### 12.1 Report

`docs/reports/g2.md`, assembled after SP-014, SP-016 and SP-017 are accepted, from the per-item reports `docs/reports/sp-011.md` … `sp-017.md`. It has the 07 §15.1 structure:

1. scope and exclusions, with spec revisions;
2. commits and tags `sp-011` … `sp-017`, artifact versions, environment and deviations from the reference host;
3. one row per §3 case and per deferred sub-part, generated by `G2CaseTable` from the xunit v3 result files and traits;
4. fault classes actually exercised, workloads and raw results;
5. limits, reviewer, decision and date.

### 12.2 Pass rule

G-2 passes when all of the following hold:

1. Every case in §3 has a report row. A missing row fails the gate.
2. Every row is `passed`, except sub-parts this document defers to a later SP, which are `not-applicable` with that SP as the reason.
3. There are no hidden skips: zero `failed`, `unimplemented` or `not-run` rows, and the run uses Docker.
4. Every Contract/Fault history is accepted by the ReferenceModel.
5. §11 measurements are recorded with no correctness violation during any workload.
6. The architecture-acceptance G-2 checklist is satisfied, with each box linked to its cases: five deployments (MODE); controls and audit races (END-02/03, MGMT-04, CTL); bounded readiness and diagnostics (CAP-02 … 04, RDY); rates across replicas with old-writer rejection (RATE, UPG subset); scaler traces (SCL-01, CAP-06 … 08); cron/DST and materialization/misfire/edit/retention (SCHED, SCH); outbox contract (OUTBOX, OUT).
7. Reviewer, decision and date are recorded.
