# 07 — G-1 test plan: PostgreSQL execution gate (SP-003, SP-010)

**Sources:** [driver-contract-suite.md](../architecture/driver-contract-suite.md) (primary), [v1-delivery-plan.md](../v1-delivery-plan.md), [implementation-backlog.md](../implementation-backlog.md), [architecture-acceptance.md](../architecture-acceptance.md), [performance-acceptance-plan.md](../architecture/performance-acceptance-plan.md), [execution-defaults-and-timing.md](../architecture/execution-defaults-and-timing.md), [worker-engine-internals.md](../architecture/worker-engine-internals.md), [identity-and-serialization.md](../architecture/identity-and-serialization.md), [dispatch-wire-contract.md](../architecture/dispatch-wire-contract.md), [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md), [capacity-control.md](../architecture/capacity-control.md), [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md), [postgres-storage-design.md](../architecture/postgres-storage-design.md), [storage-command-protocol.md](../architecture/storage-command-protocol.md), [provider-dependency-plan.md](../architecture/provider-dependency-plan.md), [public-api-contract.md](../architecture/public-api-contract.md), [management-retention-defaults.md](../architecture/management-retention-defaults.md), [driver-capabilities.md](../architecture/driver-capabilities.md), [review-2026-10-30.md](../review-2026-10-30.md).

This document defines the test harness built in SP-003, the exact case list that gate **G-1** (milestone M2, PostgreSQL execution) requires, the first performance measurements recorded by SP-010, and the G-1 pass rule. Every case listed in §4–§13 is mandatory for G-1 unless this document marks a named sub-part as deferred to a later SP item.

## Changes (2026-10-30)

Contradictions resolved and implementation choices made while consolidating the sources:

| # | Kind | Decision |
|---|---|---|
| C-1 | Contradiction | Fixed-window fingerprint is **two digests** (business + policy, R3). UNI-02 asserts `Reused` + `PolicyDrift = true` for a policy-only change, never a conflict. The single-digest description in `runwick-explained.md` is superseded. |
| C-2 | Contradiction | Aging threshold is **`>=` 60 s** (capacity-defaults "at least 60 s" wins over capacity-control "exceeds"). CAP-03 tests exact equality at 60 000 ms as aged. |
| C-3 | Contradiction | `ReleaseReservation` writes a receipt in the `ownership` retention class (NotAfter + 10 min) and uses `OwnershipCommandLifetime`; CMD/TIME cases include it. |
| C-4 | Contradiction | Recovery of an abandoned reservation returns the job to `pending` only if `started_count = 0`, otherwise `retrying` (RUN-01, TIME-06). |
| C-5 | Contradiction | Driver-suite `OBS-01` and telemetry `OBS-01` collide; neither is in G-1. Case trait values use `document:case` whenever an ID is ambiguous (§1.1). |
| C-6 | Contradiction | Server/driver targets: Npgsql 10.0.3; PostgreSQL 16–18 with 18 as reference (`postgres-upgrades` "none selected" superseded). |
| C-7 | Contradiction | Capability-set bound: at most **512 decoding tuples per probe/Reserve**; 513 is rejected before any write (CFG-01). |
| I-1 | Implementation choice | The worker validation matrix in worker-engine-internals §11 has no IDs. This document assigns **WRK-01 … WRK-19** in table order (§10). |
| I-2 | Implementation choice | SP-005/SP-006 "done when" criteria with no case ID receive inferred IDs **INIT-01** and **BATCH-01 … BATCH-03** (§13). The G-1 acceptance bullet "first measurements cover contention, query plans, idle cost and renewal margin" receives inferred measurement IDs **G1-M1 … G1-M3** (§14.3). |
| I-3 | Implementation choice | Receipt cleanup is SP-021 (not M2). CMD-05 and TIME-12 simulate cleanup with the test-only `PruneReceipts` harness call, which deletes rows from the `storage_commands` partition whose `retain_until` has passed. |
| I-4 | Implementation choice | Revisioned queue pause/resume is SP-011. ADM-04 drives pause through the internal `SetQueueState/1` operation via the harness (`SetQueuePaused`), which takes the production lock path (store gate → application gate → receipt → queue control `FOR UPDATE`). |
| I-5 | Implementation choice | Batched Renew is SP-031. In G-1, OWN-04 runs against independent concurrent Renew commands issued by one worker's lease coordinator. |
| I-6 | Implementation choice | Renew finding a missing or mismatched permit set rolls back and raises the driver invariant fault with stable code `permit_set_mismatch` (OWN-05). |
| I-7 | Implementation choice | Fault points are an internal hook (`IFaultInjector`, no-op by default) compiled into every build and reachable only through `InternalsVisibleTo` (`Runwick.Harness`, `Runwick.Harness.Peer`). No public API arms a fault. |
| I-8 | Implementation choice | Isolation: one migrated template database per container; `Start` creates database `rw_<ns>` from it, so store gate, application gate and receipts are isolated per test. |
| I-9 | Implementation choice | Store clock jumps (TIME-13) use a harness-built image `FROM ${RUNWICK_TEST_PG_IMAGE}` with libfaketime preloaded and a `FAKETIME_TIMESTAMP_FILE` the harness rewrites through container exec. |
| I-10 | Implementation choice | Test timing profile `TestTiming.Fast` = every worker default ÷ 10 (§2.7). It must pass the same startup validation as the defaults. |
| I-11 | Implementation choice | P-01 arrivals are an open-loop Poisson process with a recorded seed. P-02 ramp steps and saturation rule are defined in §14.3. |
| I-12 | Implementation choice | CMD-06, UNI-04, CAP-01, CAP-10, CAP-11, PGX-04, PGX-09, SER-06 and TIME-06 have sub-parts that depend on later items (rates, manual retry, retention cleanup, autoscaler, other drivers). Those sub-parts are reported `not-applicable` with the named later SP; every other sub-part is mandatory. |
| I-13 | Implementation choice | SER-12 bounds: allocation for preparing a payload of size *n* is linear (ratio at 1 MiB vs 512 KiB ≤ 2.2) and an over-limit input is rejected before more than limit + 64 KiB is buffered. |
| I-14 | Implementation choice (2026-10-30, SP-003/SP-005) | Harness shapes: `ArmFault(point, FaultSpec, FaultTarget, times)` returns a `FaultHandle` whose barrier is awaited/released through `WaitAtBarrier(handle)` / `ReleaseBarrier(handle)`; `OpenClient("local")` builds an in-process client over the real `AddRunwick(...).UsePostgres(...)` service graph with the harness fault injector; peers are opened with `PeerClient`. Each per-test database cloned from `rw_template` receives a fresh `store_id`. Test projects run under VSTest (`IsTestingPlatformApplication=false`) so trait filters work. |
| I-15 | Implementation choice (2026-10-30, SP-006) | UNI-04 (a) issues the identical-intent window dispatch (active `k` + window `w`) **before** the active-only dispatch of `k`: the business digest includes the active key (02), so after `k` is rebound to a new job the identical intent is a composition conflict (04 §7.2 case c), while before it the cleared active key is not reclaimed (case d → `Reused` J). CMD-05 checks `ExpiredUnknown` through `ResolveOperation` with the simulated 24 h offset applied to the bound (PruneReceipts simulates retention passing; the public `ResolveAsync(operationId)` bounds by the deadline cap and cannot see simulated time). Deadlock freedom (PGX-04, BATCH-03) is asserted with the runner's transient-conflict counter, since the runner silently retries 40P01. |
| C-8 | Contradiction (2026-10-31, SP-010, failed test) | CAP-01 follows 06 V-15, which is normative for pool configuration: `FixedConcurrency(n)` requires `n ≥ 1`, and `AutoConcurrency` (including `min > max` and `max > 16`) is rejected at startup in M2. CAP-01 therefore asserts that `FixedConcurrency(0)` **fails startup** instead of suspending admission. "Target 0 suspends admission" and the automatic-mode bound checks move to the deferred SP-014 sub-part. |
| C-9 | Contradiction (2026-10-31, SP-010, failed test) | CAP-10 asserts "with no latency the gates do not flap under ordinary jitter", while the engine stops admission once the oldest control wait reaches 1 s (05). With 16 renewals due together on 4 renewal lanes, the last one waits three Renew round trips. That is about 20 ms on the reference host, but on the G-1 host a Renew takes 0.5–1 s with no fault injected, so the store round trip alone crosses the threshold. CAP-10 therefore measures the p90 of 20 sequential Renews on a separate namespace of the same server. If three of them are under 500 ms, no stop is allowed in the no-latency phase. Otherwise every stop in that phase must be `controlWait`, resume after ≥ 3 s of healthy checks, be overtaken by at most the claim in flight, and lose no authority. The G-1 report states which branch ran. The strict branch must run on the reference host before performance qualification. |
| I-16 | Implementation choice (2026-10-31, SP-010, failed test) | Workload producers handle an outcome-unknown Dispatch as the contract prescribes (CMD-02): they resend the same prepared command until its outcome is known. A recovered dispatch counts as accepted exactly once. The correctness check is `accepted ≤ jobs ≤ accepted + unresolved`. P-08c first ran with a producer that counted unknown outcomes as failures and reported "6250 accepted, 6251 jobs" (one unknown-outcome Dispatch had committed). That was a checker defect, not a store violation; P-08c is rerun with the recovering producer. |
| I-17 | Implementation choice (2026-10-31, SP-010, failed test) | A workload run in which any worker process exits non-zero is a run failure: the crashed worker's samples and summary are missing, so the run proves nothing. In the first G-1 run, G1-M3 lost worker 1 to a race in `LeaseCoordinator.OldestWait`. The last waiter could leave between the emptiness check and `Max()`, which threw. The same read feeds the engine's 1 s pressure check, where the fault would close admission. It was fixed, a Pure regression test was added (CAP-10), and G1-M3 is rerun. |
| I-18 | Implementation choice (2026-10-31, SP-010, failed test) | Tests failed in G-1 runs 3 and 4 because of timing in the tests, not engine behaviour. (1) Deadlock freedom (PGX-04, BATCH-03, ADM-03, TX-03) is asserted with the runner's 40P01 counter alone: the transient-conflict counter of I-15 also counts 55P03, and a 2 s `lock_timeout` behind a slow commit on a loaded host is not a deadlock. (2) OWN-01 reserves with a 6 s lease, renews while the Recover is held, and releases the Recover only after the original expiry, rather than renewing 1.5 s before expiry. The lease stays under the 10 s `idle_in_transaction_session_timeout`, because the held Recover has an open transaction (run 4 used 10 s). The Renew had missed that margin, and the StaleLease now comes from the fresh-expiry recheck that the case names. (3) CAP-10 reads its events only after admission has resumed: the last stop, raised by the final slow Finishes, needs three healthy checks before it resumes. (4) DEP-04 (b) and (d) cancel or sever only once the commit is visible to another session, and hold the COMMIT reply for 10 s (`CommitReplyLatency`). Previously a fixed 300/500 ms pause ran against a 2 s per-message delay. On run 4 the cancel reached the server before it ran the COMMIT, so the COMMIT was cancelled and rolled back, which is correctly reported as not applied rather than unknown. (5) OWN-02 reserves A with a 3 s lease, not 1 s (the case says 2 s), and asserts that A's Begin applied: under load, Reserve then Begin took longer than 1 s and A's attempt was abandoned before start. (3, continued) CAP-10's no-latency phase still starts 16 handlers together, so renewals still fall due in waves, but their Finishes are 1 s apart. Sixteen simultaneous Finishes through two settlement lanes are the overload of the first phase, not "ordinary jitter", and with a 0.6–1 s Finish on this host they queue past the 5 s stop threshold. |

## 1. Test suites and projects

Four suites are kept separate (driver-contract-suite §1):

1. **Pure** — deterministic engine/model tests: fake clocks, registry/selector/controller state, codec/fingerprint vectors, timing arithmetic. They cannot prove database locking or persistence.
2. **Contract** — real-store semantic tests: independent connections and processes, synchronization barriers, operations through driver interfaces, normalized committed snapshots.
3. **Fault** — disconnects, process kill, server restart, failed writes, delayed commits. Each result names the injected fault class and deployment profile.
4. **Perf** — workload measurements (§14). They never substitute for invariant tests.

A process kill is not a power-loss test, and the two are never reported interchangeably. HA/failover classes are not part of G-1 (direct-primary profile only).

| Project | Kind | Hosts |
|---|---|---|
| `tests/Runwick.Harness` | Class library | `DriverFixture`, `EngineFixture` (fake time), fault injector, `WireProxy`, `PeerClient`, `ReferenceModel`, snapshot normalizer, history recorder, G-1 report builder |
| `tests/Runwick.Harness.Peer` | Console executable | Peer process (§2.4) |
| `tests/Runwick.Core.Tests` | xunit v3 test project | `Category=Pure` cases |
| `tests/Runwick.Postgres.Tests` | xunit v3 test project | `Category=Contract` and `Category=Fault` cases |
| `tests/Runwick.Workloads` | xunit v3 test project | `Category=Perf` workloads (§14); excluded from ordinary CI runs |

`Runwick.Testing` is a shipped package and hosts no G-1 case. Test framework: xunit v3 4.0.2, `Microsoft.NET.Test.Sdk` 18.11.0, `Microsoft.Extensions.TimeProvider.Testing` 10.11.0, `Testcontainers.PostgreSql` 4.16.0 (central versions in `Directory.Packages.props`).

### 1.1 Traits

Every test carries exactly these traits:

| Trait | Values |
|---|---|
| `Category` | `Contract` \| `Fault` \| `Pure` \| `Perf` |
| `Item` | The SP item the test proves: `SP-003` … `SP-010` |
| `Case` | Case ID, e.g. `CMD-03`, `TIME-07`, `WRK-09`, `INIT-01`. Ambiguous IDs use `document:case` (e.g. `driver-contract-suite:OBS-01`) |
| `RequiresDocker` | `true` only on tests that need container control (`RestartStore`, clock jumps). Omitted otherwise |

One case may have several tests (e.g. a Pure arithmetic test and a Fault real-store test); each test has one `Category` and one `Item`. A case passes only when all its tests pass.

## 2. Harness contract (SP-003)

### 2.1 DriverFixture

```csharp
internal sealed class DriverFixture : IAsyncDisposable
{
    Task StartAsync(StoreProfile profile, string isolatedNamespace);
    Task<IRunwickTestClient> OpenClient(string processIdentity);   // "local" or a peer identity
    Task<BusinessTransaction> OpenBusinessTransaction(
        IsolationLevel level = IsolationLevel.ReadCommitted,
        bool viaProxy = false);                                       // declared enlistment only
    Task<StoreSnapshot> ReadCommittedSnapshot();
    Task<FaultHandle> ArmFault(FaultPoint point, FaultAction action, FaultTarget target, int times = 1);
    Task WaitAtBarrier(FaultPoint point, FaultTarget target, TimeSpan? timeout = null); // default 30 s → test failure
    Task ReleaseBarrier(FaultPoint point, FaultTarget target);
    Task RestartStore(RestartMode mode, TimeSpan downtime = default);  // RestartMode: Graceful | Kill
    Task SetQueuePaused(string application, string queue, bool paused); // internal SetQueueState/1 (I-4)
    Task PruneReceipts(string retentionClass, DateTimeOffset retainUntilBefore); // test-only cleanup (I-3)
    Task<DateTimeOffset> StoreNow();                                  // SELECT clock_timestamp()
    Task StepStoreClock(TimeSpan offset);                             // RequiresDocker; libfaketime image only (I-9)
    WireProxy Proxy { get; }
    Task Stop();
}
```

- `StoreProfile` names the topology and persistence settings. G-1 uses `DirectPrimary` (fsync=on, synchronous_commit=on, full_page_writes=on, PostgreSQL 18) and `DirectPrimaryViaProxy` (same server, all client connections routed through `WireProxy`). The profile name, server version string and settings are written into every history.
- `isolatedNamespace` is a lowercase `[a-z0-9_]{1,40}` token; it names the per-test database `rw_<ns>` and is the default application name. Tests that need two applications (ISO-01, CMD-02) pass explicit application names on the clients.
- Clients use the real serializer profile, SQL, receipt codec and Npgsql connection behaviour. No client path is mocked in Contract/Fault tests.

### 2.2 Snapshots and history

`ReadCommittedSnapshot()` runs one `REPEATABLE READ READ ONLY` transaction and returns normalized rows of `store_metadata`, `application_controls`, `queue_controls`, `jobs`, `job_payloads` (payload replaced by length + SHA-256), `job_generations`, `job_attempts`, `concurrency_scopes`, `job_concurrency_requirements`, `concurrency_permits`, `dispatch_keys`, `storage_commands`, `queue_events`. Rows are sorted by their primary key using the canonical encodings (UUIDs by big-endian byte order, text bytewise); timestamps are UTC milliseconds. It never observes another transaction's uncommitted rows.

Every Contract/Fault test records a history: command ID, operation/version, fingerprint, request and reply (`Status`, `Effect`, `Replayed`, `DecidedAt`), local monotonic send/receive bounds, database decision instant, attempt/generation/ownership token, injected faults, and snapshots taken before and after. Payloads and secrets are redacted. The **ReferenceModel** (in `Runwick.Harness`) replays the history and rejects illegal transitions; it is an independent implementation and never calls the production outcome classifier, retry evaluator or SQL builder. A Contract/Fault test fails if the model rejects its history, even when its explicit assertions pass.

The history checker accepts legitimate concurrency reorderings. It does not demand a particular winner, FIFO across processes, or one physical execution across lease loss; it demands one **valid** ownership history.

### 2.3 Fault points and actions

Fault points are invoked by the PostgreSQL transaction runner for every operation (Dispatch/Stage, Reserve, Begin, Renew, Finish, Recover, ReleaseReservation, receipt resolution):

| `FaultPoint` | Location |
|---|---|
| `BeforeGateLock` | Connection open, transaction begun, before `store_metadata FOR SHARE` |
| `AfterLocksBeforeFreshTime` | All row locks of the operation held, before the fresh `clock_timestamp()` decision read |
| `BeforeFirstMutation` | Decision made, before the first `INSERT`/`UPDATE`/`DELETE` |
| `AfterMutation` | After each mutation statement; `FaultTarget.Step = n` selects the n-th (1-based), unset matches every step |
| `BeforeCommit` | Receipt finalized, immediately before `COMMIT` is sent |
| `AfterCommitBeforeReply` | `COMMIT` acknowledged by the server, before the reply is returned to the caller |
| `DuringReceiptLookup` | Inside receipt resolution (`ResolveCommand`, `ResolveAsync`, worker resolution of an unknown command), before the receipt `SELECT` |

| `FaultAction` | Effect | Valid at |
|---|---|---|
| `Throw(exceptionKind)` | Raises the given exception kind (`Transient`, `Io`, `Deadlock40P01`, `Serialization40001`); the transaction rolls back | Every point except `AfterCommitBeforeReply` |
| `Delay(duration)` | Sleeps, then continues | Every point |
| `Barrier` | Blocks until `ReleaseBarrier`; `WaitAtBarrier` returns once reached | Every point |
| `DropReply` | Discards the committed result, closes the physical connection (never returned to the pool) and raises the same `NpgsqlException(IOException)` a lost socket produces, classified *outcome unknown* | `AfterCommitBeforeReply` only |
| `KillProcess` | The peer blocks at the point, reports it, and the fixture terminates it with `Process.Kill(entireProcessTree: true)` (SIGKILL); no `finally`/dispose runs | Peer clients only; arming it for `"local"` fails the test |

`FaultTarget` = `CommandId(Guid)` \| `Operation(name)` \| `Any`, plus optional `Step` and `Occurrence` (n-th matching hit). A fault fires `times` times (default 1) and is then disarmed. Barriers are the only mechanism used to arrange races; no test uses sleep-based ordering.

### 2.4 Peer process

`Runwick.Harness.Peer` is a separate OS process started by `OpenClient(processIdentity)` for any identity other than `"local"`. It hosts the same client or worker code (dispatcher, store clients, `WorkerHostedService`) against the same database. Control is JSON Lines over stdin/stdout: the fixture sends commands (`open`, `dispatch`, `reserve`, `begin`, `renew`, `finish`, `recover`, `release`, `startWorker`, `stopWorker`, `armFault`, `releaseBarrier`) and the peer emits replies and events (`atBarrier`, `reply`, `handlerInvoked`, `log`). Its history is merged into the test history by command ID. Two-process cases (ADM-01, ADM-02, RUN-01, UNI-01, WRK-18) always use at least one peer; an in-process shared connection does not satisfy them.

### 2.5 Wire proxy

`WireProxy` is a TCP proxy on `127.0.0.1:<ephemeral>` between Npgsql and the server. Proxied connection strings use `SslMode=Disable` (test only) so the proxy can parse PostgreSQL frontend/backend messages. Connections are tagged by the `application_name` startup parameter `rw:<processIdentity>:<role>` where role ∈ `control | work | producer | business | listener`.

| Mode | Behaviour |
|---|---|
| `PassThrough` | Forward bytes unchanged |
| `Latency(delay, direction, roleFilter)` | Delay each message in `ClientToServer`, `ServerToClient` or `Both` directions for matching connections |
| `DropCommitReply(roleFilter, times)` | On a matching client `Query` (`'Q'`) whose text is `COMMIT` (case-insensitive, leading whitespace allowed), or an extended-protocol `Parse`/`Bind`/`Execute`/`Sync` sequence executing `COMMIT`: forward it to the server, keep reading server bytes until `ReadyForQuery` (`'Z'`), record the `CommandComplete` tag, then close the client socket with linger 0 **without relaying any of those bytes**, and close the server socket |
| `Sever(roleFilter)` | Immediately close matching client and server sockets |
| `CommitReplyLatency(delay, roleFilter)` | Hold the server's reply to each `COMMIT` on matching connections for `delay`; every other message passes on time |

**Change 2026-10-30 (SP-010, failed test).** `Latency` delays every message, and one ownership command is about ten round trips (BEGIN, timeouts, gates, receipt, body, receipt finalization, COMMIT). At `Latency(4 s, ServerToClient)` each Renew therefore takes about 40 s, more than `ControlRequestTimeout`. No Renew completes, so TIME-02 cannot observe `d`. TIME-02's real-store sub-part uses `CommitReplyLatency(4 s, control)`, which gives one reply 4 s late per command and matches the case's "reply at t = 4 s". The P-08 workloads keep per-message `Latency` as written. Their effective per-command delay, about 10–20 s at 1 s, is reported with the results.

`DropCommitReply` records whether the server tag was `COMMIT` or `ROLLBACK`. A test that expects a committed-then-lost reply fails with "fault not exercised" if the tag was `ROLLBACK`. `RestartStore` re-points the proxy upstream to the restarted container's port, so clients keep a stable endpoint.

### 2.6 Store provisioning

- Default: Testcontainers starts `RUNWICK_TEST_PG_IMAGE` (default `postgres:18`) with `-c fsync=on -c synchronous_commit=on -c full_page_writes=on -c max_connections=300`. The report records the resolved image digest. One container per test assembly (xunit v3 assembly fixture) holds the migrated template database `rw_template`; `Start` runs `CREATE DATABASE rw_<ns> TEMPLATE rw_template`.
- Tests that call `RestartStore` or `StepStoreClock` run in xunit collection `StoreRestart` (parallelization disabled) on a dedicated container, and carry `RequiresDocker=true`.
- `RestartStore(Graceful)` stops the container with the image's stop signal (SIGINT, PostgreSQL fast shutdown) and a 30 s timeout. `RestartStore(Kill)` sends SIGKILL to the container. Both keep the container filesystem, wait `downtime`, start it again, wait for `pg_isready` and re-point the proxy.
- **Escape hatch:** when `RUNWICK_TEST_PG` holds an admin connection string (must allow `CREATE DATABASE`), no container is started and the template is created on that server. Tests with `RequiresDocker=true` call `Assert.Skip("requires-docker")`; the report builder records them as `not-run`. A G-1 report must come from a run without the escape hatch (§15.2).

### 2.7 Test timing profile

`TestTiming.Fast` divides every worker/control default by 10:

| Setting | Default | Fast |
|---|---|---|
| LeaseDuration / LeaseSafetyMargin | 60 s / 10 s | 6 s / 1 s |
| RenewalInterval (jitter) | 15 s [12, 18] | 1.5 s [1.2, 1.8] |
| LeaseWatchdogInterval | ≤ 1 s | ≤ 100 ms |
| SchedulerAllowance | 2 s | 200 ms |
| ControlRequestTimeout | 5 s | 500 ms |
| Control retry backoff (nominal / cap) | 100 ms / 1 s | 10 ms / 100 ms |
| MinimumInvocationMargin | 15 s | 1.5 s |
| Claim / Begin / Completion resolution budgets | 15 s / 15 s / 30 s | 1.5 s / 1.5 s / 3 s |
| CancellationGrace / CleanupBudget | 10 s / 10 s | 1 s / 1 s |
| OwnershipCommandLifetime | 30 s | 3 s |
| RecoveryScanInterval | 5 s [4, 6] | 500 ms [400, 600] |
| ShutdownBudget (drain / cancellation / finalization) | 25 s (15 / 5 / 5) | 2.5 s (1.5 / 0.5 / 0.5) |

Validation of Fast: 1.8 + 2×0.5 + 0.1 + 0.2 = 3.1 s < 5 s; 1.5 s ≥ 2×0.5 + 0.1 + 0.2 = 1.3 s; StopMargin = 5 − 1.8 − 0.5 − 0.2 = 2.5 s > 0; ResumeMargin = 3.0 s. Generation retry policies in tests are set per job type and stated in each case. Timing assertions include bounded slack of `SchedulerAllowance` plus measured RTT and use store decision timestamps, never local wall-clock guesses.

**Change 2026-10-30 (SP-009, budget miss).** Real-store worker tests (`Runwick.Postgres.Tests` cases that start the worker engine, and the peer worker) run with `TestTiming.Scaled(divisor)`, where the divisor comes from `RUNWICK_TEST_TIMING_DIVISOR` (1, 2, 4, 5 or 10; default 4). Fast (÷10) budgets a 500 ms `ControlRequestTimeout` and a 1.5 s `BeginResolutionBudget`. On the shared development host (load average ≈ 15 on 8 cores), store calls stalled for several seconds at a time. The engine then correctly latched `BeginUnresolved` and `SafetyDeadline` and recovered, but cases asserting a healthy steady state became nondeterministic. Every 06 §3 relationship scales linearly, so ÷4 passes the same validation: lease 15 s, safety 2.5 s, renewal 3.75 ± 0.75 s, timeout 1.25 s, StopMargin 6.25 s. Pure engine tests use fake time and the defaults. Cases whose wording fixes a Fast number derive it from the active profile instead: TIME-10 (a) renewals ≥ 40 s / RenewalIntervalMax − 1, and WRK-18 kill points at the scaled phase boundaries. A CI host with headroom can set the divisor to 10.

### 2.8 Engine fixture (Pure)

`EngineFixture` runs the real worker engine (supervisor, tickets, registry, pool coordinator, lease coordinator, arbiter, completion coordinator, attempt runner) against `FakeTimeProvider` and a scripted in-memory store implementing the internal store interfaces. The script controls each reply (value, delay, drop, duplicate delivery, `Replayed` flag) and exposes task barriers on the runner, cancellation callbacks and scope disposal. Engine tests prove task ownership and sequencing, not SQL isolation; every engine-level case that depends on durable effects has a paired Contract/Fault test.

## 3. G-1 case inventory

| Family | Source document | G-1 cases |
|---|---|---|
| CMD | driver-contract-suite §3 | CMD-01 … CMD-05, CMD-06 (subset) |
| ADM | driver-contract-suite §3 | ADM-01 … ADM-05 |
| RUN | driver-contract-suite §3 | RUN-01, RUN-02 |
| OWN | driver-contract-suite §3 | OWN-01 … OWN-05 |
| END | driver-contract-suite §3 | END-01, END-04 |
| UNI | driver-contract-suite §3 | UNI-01 … UNI-04 |
| TX | driver-contract-suite §4 | TX-01 … TX-04 |
| ISO/ENC/CFG | driver-contract-suite §3 | ISO-01, ENC-01, CFG-01 |
| TIME | execution-defaults-and-timing §9 | TIME-01 … TIME-13 |
| WRK | worker-engine-internals §11 (IDs per I-1) | WRK-01, 03–14, 16–18 (WRK-02, 15, 19 deferred) |
| SER | identity-and-serialization §7 | SER-01 … SER-12 |
| PGX | postgres-schema-and-operations §7 | PGX-05, PGX-04 (subset), PGX-09 (subset) |
| DEP | provider-dependency-plan §7 | DEP-02, DEP-04 |
| CAP | capacity-defaults-and-budgets §7 | CAP-01, CAP-03, CAP-10 (subset), CAP-11 (subset) |
| Inferred | this document (I-2) | INIT-01, BATCH-01 … BATCH-03 |
| Measurements | performance-acceptance-plan, this document | P-01, P-02, P-08 (G-1 subset), G1-M1 … G1-M3 |

Not in G-1 (later gates): END-02, END-03 (SP-011); RATE-01–04, CMD-06 rate half (SP-013); SCH-01–05 (SP-016); OUT-01–02 (SP-017); RET-01 (SP-021); driver-contract-suite:OBS-01 (SP-012/SP-022); PGX-01–03, 06–08; DEP-01, 03, 05–08; CAP-02, 04–09, 12; UPG-01–10; MGMT; OUTBOX; REDIS; DASH; OPS; telemetry OBS.

Every delivery-plan case ID referenced for M2 (CMD, UNI, TX, ADM, RUN, OWN, END-01/04, TIME, SER, PGX-05) has a definition in the source documents. Definitions inferred here exist only for the unnumbered worker matrix (WRK), the SP-005/SP-006 done-when criteria (INIT, BATCH) and the G-1 measurement bullet (G1-M).

**SP-010 G-1 scenario traceability** (delivery plan §10):

| G-1 scenario | Cases |
|---|---|
| Rollback | TX-01, TX-03, PGX-05, BATCH-01 |
| Death before/after Begin | RUN-01, WRK-18 |
| Stale Finish | OWN-02, OWN-03 |
| Dropped replies | CMD-03, END-01, TX-04, DEP-04, WRK-03, WRK-07, TIME-09, TIME-12 |
| Ignored cancellation | TIME-07, WRK-09 |
| Shutdown phases | TIME-11, WRK-04, WRK-18 |
| First measurements | P-01, P-02, P-08, G1-M1 … G1-M3 |

## 4. Command identity — CMD (driver-contract-suite)

#### CMD-01 — Same mutation ID/body, concurrent and sequential replay
- **Proves:** SP-006 (Dispatch), SP-008 (ownership commands) · `Runwick.Postgres.Tests` · Contract
- **Setup:** `DirectPrimary`; one `PreparedDispatch` P (operation ID O, NotAfter = now + 5 min); one reserved job for the ownership variant.
- **Steps:** (a) Arm `Barrier` at `BeforeCommit` for O; submit `DispatchAsync(P)` on two connections concurrently (local + peer); `WaitAtBarrier`; release. (b) Replay `DispatchAsync(P)` sequentially three times. (c) Repeat (a)/(b) for one `Reserve`, `Begin`, `Finish`, `Recover` and `ReleaseReservation` command each, reusing the same `StoreCommand` (same ID/body/NotAfter).
- **Assert:** exactly one job, one `job_generations` row, one receipt row for O; both concurrent calls return identical `DispatchReceipt` (`JobId`, `OperationId`, `Disposition = Created`); at store level exactly one reply has `Replayed = false`, all others `Replayed = true` with identical `Status`, `Effect = Applied`, value and `DecidedAt`. Ownership commands: one attempt, one execution increment, one permit set, one outcome; replays return the stored reply.

#### CMD-02 — Same ID, changed operation/body/deadline/application scope
- **Proves:** SP-005 (receipt codec) · Postgres.Tests · Contract
- **Setup:** Command ID X applied as Dispatch in application `a`.
- **Steps:** Submit X in `a` with (i) changed payload, (ii) changed `NotAfter`, (iii) changed diagnostic metadata only, (iv) a different operation name (Reserve body). Submit X in application `b` with a valid Dispatch body.
- **Assert:** (i)–(iv) return `CommandConflict`, `Effect = ObservedNoEffect`, no new rows; the stored receipt and job in `a` are unchanged. In `b`, X applies independently (`Created`, a new job in `b`); `b`'s reply never contains `a`'s job ID or decision; receipts are keyed `(application, retention_class, command_id)`.

#### CMD-03 — Commit succeeds, response is dropped
- **Proves:** SP-006, SP-008 · Postgres.Tests · Fault
- **Setup:** `DirectPrimary` (in-process variant) and `DirectPrimaryViaProxy` (wire variant).
- **Steps:** For Dispatch, Reserve, Begin, Finish, Recover, ReleaseReservation: arm `DropReply` at `AfterCommitBeforeReply` (in-process) or `DropCommitReply` for the issuing role (wire). Submit; observe the outcome-unknown error; retry the **same** command (dispatcher: `DispatchAsync(prepared)` from the exception's recovery handle; worker: arbiter resolution).
- **Assert:** the error is classified outcome-unknown (never known-rolled-back); the retry returns the original effect with `Replayed = true`; snapshot shows one job / one attempt / one execution increment / one permit set / one outcome; no second grant; `ResolveAsync(O)` returns `Confirmed` with the original receipt; the proxy recorded tag `COMMIT`.

#### CMD-04 — Deadline expires while waiting for a lock
- **Proves:** SP-005 (transaction runner) · Postgres.Tests · Fault
- **Steps:** (a) Open a business transaction that stages a dispatch holding dispatch key K (uncommitted). Submit Dispatch D with key K and `NotAfter = StoreNow() + 2 s`; D blocks on K. Wait until `StoreNow() > NotAfter`; roll back the business transaction. (b) Ownership variant: hold the job's concurrency scope row `FOR UPDATE` from a business transaction; submit Finish F with `NotAfter = StoreNow() + 1 s`; release after expiry. (c) Apply Dispatch E before its deadline; after its NotAfter, replay E.
- **Assert:** (a) and (b) return `CommandExpired`, `Effect = ObservedNoEffect`; no job, key, receipt, outcome or permit change committed; the decision instant recorded in the history is after the lock wait. (c) returns the recorded receipt (`Replayed = true`, original `DecidedAt`) — an existing result is read before NotAfter is checked.

#### CMD-05 — Receipt pruned after retention; old command resubmitted
- **Proves:** SP-005, SP-006 · Postgres.Tests · Contract
- **Steps:** Apply Dispatch P with short NotAfter (now + 2 s) and Finish F (lifetime 3 s). After both NotAfter instants pass, `PruneReceipts("durable", far future)` removes their receipts (I-3). Replay P and F with identical bodies. Call `ResolveAsync(P.OperationId)`.
- **Assert:** both replays return `CommandExpired`; no second job, no new generation, no outcome change; `ResolveAsync` returns `ExpiredUnknown`; the dispatcher does not retry under a fresh operation ID.

#### CMD-06 (subset) — Pure observation versus committed deferral
- **Proves:** SP-007 · Postgres.Tests · Contract
- **Steps:** Issue Reserve command R against an empty queue (`NoWork`), a paused queue (`QueuePaused`) and a saturated scope (`CapacityBlocked`). Make work available / unpause / free capacity, then resubmit R with the same ID and body.
- **Assert:** the first replies have `Effect = ObservedNoEffect`, `Replayed = false` and leave **no** receipt row; the resubmission returns `Applied` with a reservation (pure outcomes may change on retry).
- **Deferred:** committed `RateLimited` Begin deferral that must replay (`Effect = Applied`, `Replayed = true`) — SP-013 (rate-aware Begin/1 is not in M2). Reported `not-applicable`.

## 5. Admission — ADM (driver-contract-suite)

#### ADM-01 — Two processes claim the same supported ready job
- **Proves:** SP-007 · Postgres.Tests · Contract
- **Setup:** One job J `(name, payloadVersion 1, runwick.json, 1)`; clients `local` and `peer-1` advertising that tuple. Separate jobs U1 `(name, 2, runwick.json, 1)` and U2 `(name, 1, runwick.json, 2)`.
- **Steps:** Arm `Barrier` at `AfterLocksBeforeFreshTime` for the first Reserve; issue the second Reserve while the first holds J; release. Repeat 100 iterations with alternating winners. Then Reserve repeatedly from workers advertising only `(name, 1, runwick.json, 1)`.
- **Assert:** exactly one `Applied` reservation of J per iteration; the other returns `NoWork` (SKIP LOCKED) and creates no attempt/permit; J has one live attempt and one ownership token. U1 and U2 are never reserved: state `pending`, zero attempts, zero counters.

#### ADM-02 — Two jobs contend for the last shared permit
- **Proves:** SP-007 · Postgres.Tests · Contract
- **Setup:** `ConfigureJob<T>().MaxConcurrent(1)` (type scope, capacity 1); jobs J1, J2; local + peer clients.
- **Steps:** Barrier the first Reserve at `AfterLocksBeforeFreshTime` while holding the scope row; issue the second; release; 100 iterations.
- **Assert:** exactly one grant; the loser returns `CapacityBlocked` (or `NoWork`), with **no** attempt row and **no** partial permit; `count(concurrency_permits where scope) = 1`.

#### ADM-03 — A+B versus B+A scope requirements
- **Proves:** SP-007 · Postgres.Tests · Fault
- **Setup:** Resource scopes via `PerResource(j => key, 1)`: job X requires resources `r-a` then `r-b`; job Y declares `r-b` then `r-a`.
- **Steps:** Reserve X and Y concurrently from two processes with barriers at `AfterMutation` step 1 of each (after the first permit insert), in both release orders; 200 iterations.
- **Assert:** no PostgreSQL `40P01` raised by Runwick-owned lock acquisition; scope rows are locked in UUID order regardless of declaration order; each reservation is all-or-none (a job has either all its permits or none); at most one of X/Y holds permits at a time.

#### ADM-04 — Queue pause races claim
- **Proves:** SP-007 · Postgres.Tests · Fault
- **Steps:** (a) Barrier Reserve R at `BeforeCommit` (holding queue control `FOR SHARE`); call `SetQueuePaused(true)` (blocks on `FOR UPDATE`); release R. Then issue Reserve R2. (b) Barrier the pause at `BeforeCommit`; issue Reserve R3 (blocks); release. (c) While paused, `DispatchAsync` a new job.
- **Assert:** (a) R commits its reservation, the pause commits after it, R2 returns `QueuePaused`; (b) R3 returns `QueuePaused` and creates no attempt; the existing reservation from (a) is not revoked; (c) dispatch returns `Created` (pause never blocks dispatch).

#### ADM-05 — Mixed-type/resource-blocked lane and cursor wrap
- **Proves:** SP-007 · Postgres.Tests · Contract
- **Setup:** One queue/lane; type A with `MaxConcurrent(1)` already occupied; 20 A jobs eligible earlier than one B job.
- **Steps:** Reserve repeatedly using the returned `StoreError.ResumeAfter` cursor; then insert a B job with `eligible_at` earlier than the cursor and continue until the cursor wraps.
- **Assert:** each Reserve inspects at most 8 blocked candidates (driver scan counter); the later B job is reserved within ⌈20/8⌉ + 1 = 4 calls; after the wrap the earlier B job is reserved; no A job is reserved while its scope is full; cursor order is `(eligible_at, job_id)` with UUID byte order.

## 6. Execution accounting and ownership — RUN, OWN, END (driver-contract-suite)

#### RUN-01 — Reserve crash before Begin versus crash after Begin
- **Proves:** SP-008 · Postgres.Tests · Fault
- **Setup:** `MaxExecutions = 3`, `MaxConsecutiveUnstartedAbandonments = 5`, lease 3 s; peer worker.
- **Steps:** (a) Peer reserves J; `KillProcess` at Begin `BeforeGateLock`. After expiry, Recover from a local client; replay that Recover; issue a concurrent Recover with a different command ID. (b) Peer reserves and begins J2; `KillProcess` at Begin `AfterCommitBeforeReply`. Recover as in (a).
- **Assert:** (a) `consecutive_unstarted_abandonments = 1`, execution count unchanged (0), attempt closed as abandoned with no execution number, job `pending` (started_count = 0), permits released. (b) execution count = 1 (not incremented by recovery), unstarted counter = 0, job `retrying` with `eligible_at` = recovery decision + persisted delay, permits released. In both: the replayed Recover returns `Replayed = true`; the concurrent second Recover returns `StaleLease`; each counter changed exactly once.

#### RUN-02 — Concurrent/replayed Begin
- **Proves:** SP-008 · Postgres.Tests · Contract
- **Steps:** (a) Send the same Begin command on two connections concurrently (barrier at `BeforeCommit`). (b) Send two Begin commands with different IDs for the same lease concurrently.
- **Assert:** (a) one `Applied` with `Replayed = false`, the other `Replayed = true` with an identical `BegunExecution` (`ExecutionNumber`, `TotalExecutionNumber`, `AuthorizedAt`); (b) one `Applied`, the other `InvalidState`. In both: execution count incremented once, one `started_at`, permit set unchanged (no second grant set), at most one `handlerInvoked` event in a worker variant.

#### OWN-01 — Renew A, recover stale discovery of A
- **Proves:** SP-008 · Postgres.Tests · Fault
- **Steps:** Build `RecoverRequest(J, attemptA, tokenA)` from a snapshot taken when A's lease is near expiry; arm `Barrier` on that Recover at `BeforeGateLock`; submit; A renews successfully (fresh lease); release the barrier.
- **Assert:** Recover returns `StaleLease` (it rechecks token and fresh expiry under its locks); job remains owned by A with the renewed `lease_expires_at`; permits unchanged; A's next Renew is `Applied`.

#### OWN-02 — Expired A, replacement B, late renew/finish/release A
- **Proves:** SP-008 · Postgres.Tests · Fault
- **Steps:** A reserves and begins J (lease 2 s) and then stops renewing; after expiry Recover runs and B reserves and begins J. A sends Renew, Finish(Succeeded) and ReleaseReservation with its old handle.
- **Assert:** all three return `StaleLease` with `Effect = ObservedNoEffect`; B's attempt, ownership token, `lease_expires_at` and exact permit rows are unchanged; no outcome is recorded on A's closed attempt beyond the recovery outcome.

#### OWN-03 — Lease expires while the operation waits on locks
- **Proves:** SP-008 · Postgres.Tests · Fault
- **Steps:** A owns J (lease 3 s). A business transaction locks J's concurrency scope row `FOR UPDATE`. A sends Renew (blocks on the scope lock), and separately Finish on a second job in the same scope. Hold until `StoreNow() > lease_expires_at + 100 ms`; roll back the blocker.
- **Assert:** both return `StaleLease`; `lease_expires_at` and permit expiries unchanged; the decision used `clock_timestamp()` read after the lock wait (history decision instant > blocker release), never transaction-start `now()`.

#### OWN-04 — Renew multiple leases with one stale member
- **Proves:** SP-008, SP-009 · Postgres.Tests · Fault
- **Setup:** Worker with three running attempts; attempt 2's lease is taken over (expired + recovered + reserved by another client).
- **Steps:** Let the lease coordinator renew all three concurrently (independent Renew commands, I-5).
- **Assert:** attempts 1 and 3 renew `Applied` with extended expiries; attempt 2 returns `StaleLease` and is latched lost locally (cancellation requested, no Finish); no healthy renewal is rolled back or delayed past its due time + `SchedulerAllowance`.

#### OWN-05 — Missing/mismatched permit data
- **Proves:** SP-008 · Postgres.Tests · Fault
- **Steps:** For a running attempt with two permits: (a) delete one permit row by direct SQL; Renew. (b) Restore, then set one permit's `expires_at` different from the job's; Renew.
- **Assert:** Renew raises the invariant fault `permit_set_mismatch` (I-6); the transaction rolls back: job `lease_expires_at` and all remaining permit expiries unchanged; no `Applied` reply; the worker variant latches the attempt lost and issues no Finish.

#### END-01 — Finish after observed success; response lost
- **Proves:** SP-008, SP-009 · Postgres.Tests · Fault
- **Steps:** Worker runs a handler that returns success; arm `DropReply` on Finish `AfterCommitBeforeReply` (and the `DropCommitReply` wire variant).
- **Assert:** the arbiter resolves with the same command ID and exact frozen body; result `Applied`, `Replayed = true`; job `succeeded` once; handler invocation count = 1; no second attempt; no Renew sent between the unknown Finish and its resolution.

#### END-04 — Timeout versus delayed acknowledgement
- **Proves:** SP-009 · Postgres.Tests · Fault
- **Steps:** (a) `ExecutionTimeout = 1 s`; handler ignores cancellation and returns success at 1.5 s. (b) `ExecutionTimeout = 2 s`; handler returns at 0.5 s; arm `Delay(3 s)` on Finish `BeforeGateLock`.
- **Assert:** (a) Finish outcome `TimedOut` (late handler result times out); (b) outcome `Succeeded` (pipeline duration 0.5 s; storage latency excluded).

## 7. Uniqueness — UNI (driver-contract-suite)

#### UNI-01 — Parallel same active key
- **Proves:** SP-006 · Postgres.Tests · Contract
- **Steps:** 8 concurrent `DispatchAsync` calls (4 local, 4 peer) with `ActiveKey = "k"`, different payloads and different operation IDs.
- **Assert:** exactly one `Created`; seven `Reused` with `DuplicateMatch = Active` and the same `JobId`; one `jobs` row; one active `dispatch_keys` row; the stored payload is the first writer's.

#### UNI-02 — Same window key/same fingerprint; then changed payload
- **Proves:** SP-006 · Postgres.Tests · Contract
- **Steps:** Dispatch with `WindowKey = "w"`, `Window = 1 h`. Then, with fresh operation IDs: (a) identical intent; (b) changed payload; (c) changed only `MaxExecutions` (policy-only change, e.g. a changed default); (d) changed only `traceparent`.
- **Assert:** (a) `Reused`, `DuplicateMatch = Window`, `PolicyDrift = false`; (b) `UniquenessConflict`, stored payload/binding unchanged; (c) `Reused`, `PolicyDrift = true`, a `policy_drift` `queue_events` row, original generation policy unchanged; (d) `Reused`, `PolicyDrift = false`, original trace retained. Binding stores both digests with their versions.

#### UNI-03 — Active + window bindings identify different jobs
- **Proves:** SP-006 · Postgres.Tests · Contract
- **Steps:** J1 holds active key `a`; J2 holds window key `w`. Dispatch with `ActiveKey = "a"`, `WindowKey = "w"`.
- **Assert:** `UniquenessConflict` (composition conflict) carrying a conflicting job ID; neither binding is rewritten; no job created.

#### UNI-04 — Terminal/retention versus key reuse
- **Proves:** SP-006, SP-008 · Postgres.Tests · Contract
- **Steps:** (a) J with active key `k` and window key `w` (window 10 s) finishes `Succeeded`. Dispatch active `k` only. Dispatch window `w` with identical intent. (b) After the window expiry (store time), dispatch `w` again. (c) J3 with active `k3` finishes `PermanentFailure`; dispatch `k3`.
- **Assert:** (a) the Finish transaction cleared the active binding: active `k` → `Created` new job; window `w` → `Reused` J (terminal) — window history survives. (b) `Created` new job. (c) `Created`.
- **Deferred:** manual-retry reacquisition races (SP-011) and window survival across retention cleanup (SP-021). Reported `not-applicable`.

## 8. Enlistment — TX (driver-contract-suite §4)

#### TX-01 — Business row + job + unique binding rolled back
- **Proves:** SP-006 · Postgres.Tests · Contract
- **Steps:** `OpenBusinessTransaction`; insert a business row; `StageAsync(job, tx)` with active and window keys; roll back.
- **Assert:** neither the business row nor the job, payload, generation, key bindings or receipt is visible; `ResolveAsync(opId)` returns `NotObserved` before NotAfter and `ExpiredUnknown` after; a later dispatch with the same keys returns `Created`.

#### TX-02 — Same writes committed by caller
- **Proves:** SP-006 · Postgres.Tests · Contract
- **Steps:** As TX-01 but the caller commits.
- **Assert:** after `StageAsync` returns, the transaction is still active and its connection open (caller can execute further SQL); the driver neither committed nor disposed it; after commit, business row, job, bindings and receipt are visible together; `StagedDispatch` carries job ID, operation ID and disposition only; `ResolveAsync` returns `Confirmed` (`Created`).

#### TX-03 — Caller snapshot/deadlock/serialization failure
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Steps:** (a) Two caller transactions at `SERIALIZABLE` with a read/write conflict on business rows, each staging a dispatch. (b) Caller tx1 locks business row B then stages key `k`; tx2 stages key `k` then locks B.
- **Assert:** the loser receives the PostgreSQL error unchanged (`40001` / `40P01`); the driver performed no internal retry and opened no other connection; nothing from the loser is visible; the caller retries the whole transaction with the same `PreparedDispatch` and exactly one job exists.

#### TX-04 — Lost caller COMMIT response
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Steps:** Business transaction via proxy (`viaProxy: true`); stage a dispatch; arm `DropCommitReply(role=business)`; commit. Variant: `Sever(business)` before COMMIT is sent.
- **Assert:** the caller's commit throws; the earlier `StagedDispatch` made no success claim; `ResolveAsync(opId)` returns `Confirmed` (proxy tag `COMMIT`) and business row + job are visible. Variant: nothing visible; `ResolveAsync` returns `NotObserved`, then `ExpiredUnknown` after NotAfter.

## 9. Isolation, encodings, configuration — ISO, ENC, CFG (driver-contract-suite)

#### ISO-01 — Same IDs/keys in two application namespaces
- **Proves:** SP-007 · Postgres.Tests · Contract
- **Setup:** Applications `a` and `b` in one database with identical queue, job names, scope names/resource keys, active/window keys and command IDs.
- **Steps:** Dispatch identical intents in both; fill `a`'s scope to capacity; Reserve from workers of `a` and `b`; replay `a`'s command IDs in `b`.
- **Assert:** each application gets its own job and bindings; `b`'s Reserve never returns `a`'s jobs and is not blocked by `a`'s permits; scope counts are per application; receipts and conflicts never cross namespaces.

#### ENC-01 — Boundary timestamps, GUID ordering, large fences
- **Proves:** SP-005 · Postgres.Tests · Contract (+ Pure vectors in Core.Tests)
- **Steps:** (a) `NotBefore` with sub-millisecond ticks; lease and command expiries landing on sub-millisecond instants; decisions at exactly the expiry instant. (b) 10 000 UUIDs (including v7 and byte patterns that differ between `Guid.CompareTo` and byte order) compared with the driver's big-endian byte comparer and with server `ORDER BY uuid`. (c) Fence/counter values at `long.MaxValue − 1`, then increment twice.
- **Assert:** (a) NotBefore rounds **up**, lease/command expiry rounds **down** to 1 ms; equality with an expiry means expired; values round-trip exactly. (b) Identical order from comparer and server. (c) Exact values; the overflowing increment raises an explicit overflow error and commits nothing.

#### CFG-01 — Unsupported operation/schema/profile or stale revision
- **Proves:** SP-005 · Postgres.Tests · Contract
- **Steps:** (a) Worker for an application with a configured rate policy. (b) Client whose compatibility manifest is older/newer than `store_metadata.schema_revision`/`protocol_epoch`. (c) `application_controls.required_capabilities` containing an unknown capability. (d) Stale `manifest_digest`/gate revision. (e) Reserve with 513 decoding tuples (C-7). (f) Dispatch with an unregistered serializer profile. (g) Application `admission_mode = maintenance`.
- **Assert:** (a) worker startup fails before any write (rate-aware Begin/1 is not in M2); (b)–(d) handshake rejects with `UnsupportedVersion`/`AdmissionClosed` before unsafe writes; (e) rejected before the transaction; (f) rejected at Prepare; (g) Dispatch/Reserve return `AdmissionClosed`. No case silently downgrades a guarantee; snapshots show zero writes.

## 10. Worker engine — WRK (worker-engine-internals §11, IDs per I-1)

All WRK cases prove SP-009. Engine-level parts run in `Runwick.Core.Tests` (Pure, `EngineFixture`); real-store parts in `Runwick.Postgres.Tests` (Contract/Fault) with `TestTiming.Fast`.

| ID | Scenario | Setup / steps | Required assertion | Host · Category |
|---|---|---|---|---|
| WRK-01 | One slow handler among fast ones | Fixed 4; one 10 s handler and 40 × 10 ms handlers | Each retired ticket refills independently (a new Reserve follows each retirement within one claim cycle); fast jobs complete while the slow one runs; no batch-wide wait | Core (Pure), Postgres (Contract) |
| WRK-02 | Target reduction with in-flight claims | — | **Deferred to SP-014** (no runtime target change in M2); `not-applicable` | — |
| WRK-03 | Lost Reserve/Begin reply, delayed duplicate response | Drop Reserve then Begin replies; script delivers the original reply late after resolution | Same command ID on every resolution attempt; same attempt; one permit set; at most one local invocation | Core (Pure), Postgres (Fault) |
| WRK-04 | Shutdown while Reserve commits | Barrier Reserve at `BeforeCommit`; call `StopAsync`; release | Returned claim tracked; no handler starts after admission closed; the reservation is returned by `ReleaseReservation` (`released`, no budget charged) | Postgres (Fault) |
| WRK-05 | Synchronous handler prefix / blocking constructor | Handler blocks 2 s before first await; constructor blocks 2 s | Not executed inline on pool or lease coordinator; renewals stay on schedule; timer-lag sample records the starvation | Core (Pure), Postgres (Contract) |
| WRK-06 | Renewal deadline during slow pipeline | `Delay` on Renew `BeforeGateLock` past the local safety deadline | Renewal independent of pipeline; loss latched at the deadline; cancellation requested; no Finish; a late success reply does not revive the attempt | Core (Pure), Postgres (Fault) |
| WRK-07 | Finish commits, reply lost, renewal becomes stale | `DropReply` on Finish | Arbiter blocks Renew until resolved; resolution returns Applied; no stale-renewal result is reported as a failed execution | Core (Pure), Postgres (Fault) |
| WRK-08 | Storage outage after handler success | 16 handlers succeed; `RestartStore(Kill, 20 s)` | Pending outcomes ≤ 16 entries; completion channel bounded; no invocation replay; admission backs off; all Finishes resolve after restart | Postgres (Fault, RequiresDocker) |
| WRK-09 | Timeout ignored by handler | Timeout 1 s; handler ignores token for 5 s | No premature scope disposal, permit release or slot replacement; ticket held; result `TimedOut`, never timely success | Core (Pure), Postgres (Fault) |
| WRK-10 | Slow/throwing cancellation callback | One callback blocks 30 s, one throws | Other leases renew on time; callback tasks and faults tracked and observed | Core (Pure) |
| WRK-11 | Scoped service isolation and async disposal | Scoped `IAsyncDisposable` service with instance IDs | One scope per attempt; `IJobContext` initialized; no instance shared across attempts; disposed once after the pipeline | Core (Pure), Postgres (Contract) |
| WRK-12 | Middleware calls next twice / returns early | Two middleware variants | No second handler call; misuse classified `PermanentFailure`; early return without explicit outcome is misuse; downstream task joined before disposal | Core (Pure) |
| WRK-13 | Upgrade v1→v2→v3, gap and malformed payload | Chains complete / with v1→v2 missing; malformed v2 payload | Capabilities include only complete paths; v1 job with a gap stays `pending`, never reserved; malformed data: Begin then `PermanentFailure`; original bytes unchanged | Core (Pure), Postgres (Contract) |
| WRK-14 | Mixed-type lane under type ceiling | As ADM-05 through the pool coordinator | Later unblocked type remains discoverable despite repeated blocked earlier type | Postgres (Contract) |
| WRK-15 | Queue ceiling with 10k waiting jobs | — | **Deferred to SP-012** (queue-wide saturation short-circuit); `not-applicable` | — |
| WRK-16 | Dropped/coalesced notifications and reconnect | Drop every local post-commit wake signal | Polling discovers work within idle backoff max (2 s + 20 %) + one claim; no duplicate invocation. LISTEN reconnect sub-part applies only when the optional listener ships (not M2) | Core (Pure), Postgres (Contract) |
| WRK-17 | Scope cleanup throws after timely success | Scoped service throws on dispose | Finish `Succeeded`; diagnostic preserved; no automatic rerun | Core (Pure), Postgres (Contract) |
| WRK-18 | Host dies at each shutdown phase | Peer worker with begun, unbegun and finishing attempts; `Process.Kill` during drain, cancellation and finalization phases | Leases recoverable afterwards (RUN-01 rules); no `Succeeded` recorded for a handler that did not finish; no false durable acknowledgement | Postgres (Fault) |
| WRK-19 | Presence expires while job lease healthy | — | **Deferred to SP-012** (presence records); `not-applicable` | — |

## 11. Timing — TIME (execution-defaults-and-timing §9)

TIME-05/06 prove SP-008; all other TIME cases prove SP-009 (delivery plan).

#### TIME-01 — Startup validation of timing relationships
- **Host:** Core.Tests · Pure · SP-009
- **Steps:** Validate the defaults; then each invalid set: `safetyMargin >= leaseDuration`; lease 30 s / safety 10 s (renewal check 31 s ≥ 20 s; StopMargin = 20 − 18 − 5 − 2 = −5 s); `minimumInvocationMargin = 12 s` (< 13 s); shutdown slices 15/6/5 (26 > 25); jitter range outside the interval or inverted; zero/negative/non-finite durations; overflow of `renewalIntervalMax + 2·timeout`.
- **Assert:** defaults pass with 18 + 10 + 1 + 2 = 31 s < 50 s, 15 s ≥ 13 s, 15 + 5 + 5 = 25 s ≤ 25 s, StopMargin 25 s, ResumeMargin 30 s; each invalid set fails startup naming the violated rule; validation never increases the lease or reduces safety; the validation result asserts no measured capacity.

#### TIME-02 — Delayed fresh reply, replay and late responses
- **Host:** Core.Tests · Pure · SP-009 (+ Postgres.Tests Fault with proxy `Latency(4 s, ServerToClient)` on default timing)
- **Steps:** Renew sent at s = 0, reply at t = 4 s with r = ExpiresAt − DecidedAt = 60 s. Deliver a `Replayed = true` reply. Deliver a reply after the local deadline.
- **Assert:** d = s + 60 − 10 = 50 s; usable margin at receive 46 s; next renewal due ≤ s + 18 s and ≤ d − (2·5 + 1 + 2); the replayed result does not move d; a reply after d does not revive authority (loss stays latched).

#### TIME-03 — Deadline expires during thread-pool queueing
- **Host:** Core.Tests · Pure · SP-009 (+ Postgres.Tests Fault for Begin accounting)
- **Steps:** Hold the runner start barrier; advance fake time past (a) the local authority deadline, (b) a configured `ExecutionTimeout`; release.
- **Assert:** (a) no decode, DI scope or handler invocation; the committed Begin stays counted (execution count 1); (b) finalized `TimedOut` without creating a scope, if still owner.

#### TIME-04 — Classification at the timeout boundary
- **Host:** Core.Tests · Pure · SP-009
- **Steps:** (a) Pipeline elapsed exactly = timeout. (b) Timer callback delayed 500 ms beyond the boundary but recorded completion 1 ms before it. (c) Timely success with slow Finish (3 s).
- **Assert:** (a) `TimedOut`; (b) `Succeeded`; (c) `Succeeded`. Classification uses recorded pipeline duration, not acknowledgement latency or timer delivery.

#### TIME-05 — Retry arithmetic and replay
- **Host:** Core.Tests · Pure (evaluator) + Postgres.Tests · Contract (persistence) · SP-008
- **Steps/Assert:** exponential-v1, equal jitter, initial 5 s, max 60 s: n = 1 → b = 5 000 ms, delay ∈ [2 500, 5 000]; n = 2 → [5 000, 10 000]; n = 3 with `MaxExecutions = 3` → terminal `failed`; n = 5 → b = min(60 000, 80 000) = 60 000 → [30 000, 60 000]; n = 64 saturates without overflow. Same entropy input → same delay. Explicit `RequestedRetryDelay`: negative → middleware contract fault; 0 → 1 ms; 1.5 ms → 2 ms; 120 s → 60 s; no jitter added; execution still charged. Real store: Finish(RetryableFailure) with `DropReply`; replay returns the identical persisted `eligible_at` (no redraw); `eligible_at` = Finish decision instant + delay.

#### TIME-06 — Unstarted recovery budget
- **Host:** Postgres.Tests · Contract · SP-008
- **Steps:** Reserve J and let the lease expire; Recover; repeat. Separately: abandon twice, then Begin, then abandon again. Separately: Reserve then `ReleaseReservation`.
- **Assert:** delays for k = 1…4 lie in [500, 1 000], [1 000, 2 000], [2 000, 4 000], [4 000, 8 000] ms (initial 1 s, cap 30 s); the fifth consecutive abandonment (k = 5) terminates the generation `failed` with `reservation_abandonment_exhausted`; Begin resets k to 0; `ReleaseReservation` changes neither k nor the execution count.
- **Deferred:** "rate deferral charges neither budget" — SP-013; `not-applicable`.

#### TIME-07 — Ignored cancellation
- **Host:** Postgres.Tests · Fault · SP-009 (+ Core Pure)
- **Steps:** Handler ignores its token; request cancellation via shutdown and via timeout; keep it running past `CancellationGrace`.
- **Assert:** renewals stop after grace; ticket and scope retained; worker never deletes permits (they remain until store-side expiry and are released by Recover); a renewal reply arriving after the latch does not revive local work; another process may later recover the job.

#### TIME-08 — Slow/throwing callbacks and hung disposal
- **Host:** Core.Tests · Pure + Postgres.Tests · Fault · SP-009
- **Steps:** Callback blocking 30 s; callback throwing; `DisposeAsync` hanging past `CleanupBudget` after a timely success.
- **Assert:** unrelated leases renew on time; callback faults observed; hung disposal stops renewal and latches abandonment without freeing ticket/scope; the worker never sends `Finish(RetryableFailure)` for that attempt; the diagnostic says unconfirmed completion.

#### TIME-09 — Unknown Reserve/Begin/Finish budgets
- **Host:** Postgres.Tests · Fault · SP-009
- **Steps:** `DropReply` on each command plus `Throw(Io)` at `DuringReceiptLookup` on every resolution attempt until the budget expires (Fast: claim 1.5 s, Begin 1.5 s, completion 3 s).
- **Assert:** every resolution attempt reuses the original command ID and body; unresolved Reserve suspends pool admission; unresolved Begin prevents invocation and stops renewal; unresolved Finish stops renewal and retains ambiguity without rerunning the pipeline; ticket counts never exceed the pool maximum.

#### TIME-10 — Long job without timeout; short timeout
- **Host:** Postgres.Tests · Contract · SP-009
- **Steps:** (a) No timeout; handler runs 40 s under Fast (≈ 6.7 lease durations). (b) `ExecutionTimeout = 2 s` with lease 6 s.
- **Assert:** (a) ≥ 20 Renews applied, `Succeeded`, execution count 1; (b) cancellation requested at 2 s and outcome `TimedOut` (the timeout does not inherit the lease duration).

#### TIME-11 — Host token and shutdown phases
- **Host:** Postgres.Tests · Fault · SP-009 (+ Core Pure)
- **Steps:** Under default and Fast budgets, deliver the host token (a) before `StopAsync`, (b) during drain, (c) during cancellation, (d) during finalization, with begun, unbegun and finishing attempts.
- **Assert:** phase cutoffs are measured from the first observed stop signal and never restarted by `StopAsync`; unbegun reservations are released in drain; Finish/Renew during shutdown use a token that is not the cancelled host token; at the host deadline control I/O stops and `StopAsync` returns within the bound + `SchedulerAllowance`; unresolved work is reported.

#### TIME-12 — Finish receipt after local loss; receipt expiry
- **Host:** Postgres.Tests · Fault · SP-009
- **Steps:** Finish(Succeeded) commits, reply dropped, local deadline passes before resolution. Second run: also `PruneReceipts` after NotAfter before resolution.
- **Assert:** first run resolves `Recorded` Succeeded and reports historical success, not failure; second run reports explicit `ExpiredUnknown` without reapplying the Finish (job state from recovery only).

#### TIME-13 — Runtime safety beyond static inequalities
- **Host:** Postgres.Tests · Fault · SP-009
- **Steps:** (a) Saturate the work data source (8 connections) with producer/background calls while 16 attempts renew. (b) Hold scope locks so Renews block past local deadlines. (c) `StepStoreClock(+30 s)` and `StepStoreClock(−30 s)` (RequiresDocker, I-9). (d) Block all thread-pool workers for 3 s.
- **Assert:** (a) Renew/Finish use the control data source and stay within their lanes (4/2 calls); (b) local authority expiry latches loss; (c) store decisions use stored instants (forward jump: renewals after the jump get `StaleLease` when expired; backward jump: no local revival); (d) watchdog latches loss when the deadline passes; no Finish is sent after a latch in any sub-case.

## 12. Serialization — SER (identity-and-serialization §7)

| ID | Setup / steps | Required assertion | Proves · Host · Category |
|---|---|---|---|
| SER-01 | Register jobs without explicit name; invalid grammar (length 0/129/201, leading space, illegal char); same-layer conflicting name/version/profile; rename the CLR type | Missing/invalid identity and same-layer conflicts fail at startup before any dispatch; CLR rename keeps the durable name and fingerprint | SP-004 · Core · Pure |
| SER-02 | Policy declared at library → application → attribute → fluent → dispatch; attribute middleware with equal `Order`; dispatch attempting to disable required uniqueness or change retry budget | Deterministic effective policy with source map; ties broken by ordinal alias order; forbidden dispatch overrides rejected | SP-004 · Core · Pure |
| SER-03 | Payloads with duplicate decoded names (`"a"` and `"a"`), malformed UTF-8/unpaired surrogates, unknown members, quoted numbers, NaN/Infinity, comments, trailing commas; each bound at limit and limit + 1: payload 1 MiB, depth 32, string 256 KiB, property name 256 B, 256 properties, 16 384 elements, numeric token 128 B, metadata 16 KiB / 32 entries, metadata key 128 chars, envelope 2 MiB | Every invalid form and every limit + 1 rejected with a stable code at Prepare and at import; limit values accepted | SP-004 · Core · Pure |
| SER-04 | Host mutates ASP.NET `JsonOptions` and a global serializer setting; attempt to mutate finalized profile options | Persisted bytes unchanged; finalized options throw on mutation (`MakeReadOnly`) | SP-004 · Core · Pure |
| SER-05 | Prepare a payload; mutate the source object; dispatch the prepared request | Bytes, keys and command body unchanged; stored `job_payloads` bytes equal the prepared bytes | SP-004 · Core · Pure (+ Postgres Contract) |
| SER-06 | Dispatch payloads containing `1`, `1.0`, `1e0`, `-0`, a large integer beyond Int64 in a `JsonElement`, escaped and unescaped strings; Reserve them | `ReservedJob.PayloadUtf8` byte-identical to the dispatched bytes; profile ID/version identical on `jobs`, `job_payloads` and `ReservedJob`. **Deferred:** cross-driver identity — SP-019/SP-025 | SP-007 · Postgres · Contract |
| SER-07 | Job with matching name/version but unsupported profile; supported profile with malformed data | Unsupported profile never reserved (stays `pending`, no attempt); malformed data: Begin, then Finish `PermanentFailure`, execution count 1 | SP-007 · Postgres · Contract |
| SER-08 | Historical v1 payload under profile v1; worker with v1→v2→v3 upgrades; then worker without the profile | Handler receives v3; original bytes, job ID and fingerprint unchanged; without the profile the job is not claimed and remains `pending` | SP-004 · Core Pure + Postgres Contract |
| SER-09 | Unknown polymorphic discriminator; member-level attribute enabling number coercion or case-insensitivity on a built-in profile type | Unknown discriminator fails decode without loading any type; registration rejects metadata weakening the profile | SP-004 · Core · Pure |
| SER-10 | Import envelopes: altered app/store/versions, expired deadline, body mismatch with operation ID, non-canonical base64, unknown envelope field, size 2 MiB + 1; import a valid envelope whose policy no longer matches current defaults | Each invalid envelope rejected before materialization; valid import preserves operation ID/replay identity; policy change reported (`PolicyDrift` or explicit compatibility failure), never silently re-defaulted; prior receipt still resolves | SP-004 · Core Pure (+ Postgres Contract for apply, SP-006) |
| SER-11 | Golden vectors in `tests/vectors/`: canonical bytes and SHA-256 digests for every item of dispatch-wire-contract §8 (object order/whitespace, array order, escapes, duplicate keys, malformed Unicode, composed/decomposed, numeric lexemes, missing/null/default policy, immediate vs explicit instants, equal instants with different offsets, ms rounding, diagnostic vs business metadata, changed route/budget/timeout/scope membership, unchanged prepared request after default change, older fingerprint version, active aliasing, conflicting bindings, rollback, imported envelope alterations); domains `runwick.dispatch-request.v1`, `runwick.command.v1` | Encoder output and both digests (business, policy) match the vectors byte-for-byte; a deliberate encoder change fails the suite | SP-004 · Core · Pure |
| SER-12 | Prepare and import at 512 KiB and 1 MiB payloads and 2 MiB envelopes; feed over-limit streams | Allocation ratio 1 MiB : 512 KiB ≤ 2.2; over-limit input rejected before limit + 64 KiB is buffered (I-13); 1 MiB payload round-trips through PostgreSQL exactly, no provider truncation/normalization; latency recorded | SP-004 · Core Pure (+ Postgres Contract) |

## 13. PostgreSQL, provider and capacity cases

#### PGX-05 — Constraints after injected rollback/commit loss
- **Proves:** SP-005 · Postgres.Tests · Fault
- **Steps:** For Dispatch, batch Dispatch, Reserve, Begin, Finish, Recover, ReleaseReservation: `Throw(Transient)` at each `AfterMutation` step and at `BeforeCommit`; `DropReply` at `AfterCommitBeforeReply`. Separately, in a raw transaction insert a `storage_commands` placeholder with no result/`decided_at` and commit.
- **Assert:** rolled-back cases leave a snapshot equal to the pre-state; committed cases satisfy every FK/unique/check and deferred constraint and the ReferenceModel; the placeholder commit fails on the deferred receipt-completeness trigger and commits nothing.

#### PGX-04 (subset) — Finish versus duplicate dispatch; opposite-order scope batches
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Steps:** (a) J running with active key `k`; Finish(J, Succeeded) and Dispatch(active `k`) with barriers forcing each order (Finish holding job then key; Dispatch holding key). (b) Two `DispatchManyAsync` batches whose items provision new resource scopes `r1, r2` and `r2, r1` with overlapping keys, run concurrently 100 times.
- **Assert:** (a) no deadlock; Dispatch never locks J; Finish first → Dispatch `Created`; Dispatch first → `Reused` J, then Finish clears the binding. (b) no deadlock; scope rows unique per natural key; both batches commit.
- **Deferred:** manual retry (SP-011) and materialization/scope edits (SP-016/SP-011); `not-applicable`.

#### PGX-09 (subset) — Durations, ordering and policy versions
- **Proves:** SP-005 · Postgres.Tests · Contract
- **Assert:** all duration columns are bigint milliseconds with checked arithmetic; text identities sort bytewise under the selected deterministic collation; UUID order equals the big-endian byte comparer; generation policy rows record evaluator name/version and jitter kind and round-trip the SER-11 vectors. **Deferred:** cross-driver comparison (SP-019/SP-025).

#### DEP-02 — Caller transaction staging; no ambient enlistment
- **Proves:** SP-006 (staging), SP-005 (`Enlist=false`) · Postgres.Tests · Contract
- **Steps:** (a) `StageAsync` on a caller transaction; compare `pg_backend_pid()` of the caller connection with the backend that wrote the staged rows; read Runwick data-source statistics. (b) Run Reserve and Finish inside a `TransactionScope` that is then disposed without completing.
- **Assert:** (a) staged rows are written on the caller's backend; no Runwick pool connection was opened; (b) Reserve and Finish committed independently of the ambient scope.

#### DEP-04 — Lost COMMIT, cancel, disconnect
- **Proves:** SP-005, SP-008 · Postgres.Tests · Fault
- **Steps:** Via proxy: (a) `DropCommitReply`; (b) cancel the caller token while COMMIT is in flight (proxy `Latency(2 s)`); (c) `Sever` before the COMMIT message is sent; (d) `Sever` after COMMIT is forwarded.
- **Assert:** (c) classified known-not-applied and nothing committed; (a), (b), (d) classified outcome-unknown, resolved by the same command ID via receipt, with exactly one effect; Npgsql cancellation is never treated as rollback evidence.

#### CAP-01 — Pool bounds and group limits
- **Proves:** SP-009 · Core.Tests Pure + Postgres.Tests Contract
- **Steps:** Configure pools whose maxima sum to 17 (group maximum 16); `FixedConcurrency(0)`; default pool (no mode); `FixedConcurrency(8)` × 2; auto declarations with `min > max` and `max > 16`. Run fixed 16 with 16 hung handlers and further backlog.
- **Assert:** invalid sums/bounds fail startup; default pool target and maximum 4; `FixedConcurrency(n)` sets target and hard maximum n; `FixedConcurrency(0)` and auto declarations fail startup (C-8); with hung handlers outstanding tickets never exceed 16 and no replacement attempt is admitted. **Deferred:** automatic-mode runtime behaviour, target 0 suspending admission, and target reductions — SP-014; `not-applicable`.

#### CAP-03 — Aging, threshold equality, rotation, strict starvation
- **Proves:** SP-009 · Core.Tests · Pure (selector with fake time)
- **Steps:** Weighted lanes 6:3:1 with high continuously claimable; low lane oldest eligible age 59 999 ms, 60 000 ms, 60 001 ms; failed aging attempts; strict and strict-with-aging modes.
- **Assert:** aging is attempted after 8 successful admissions (counter saturates at 8); age 60 000 ms **is** aged (C-2), 59 999 is not; an aging success resets the counter and lane scores; a failed aging attempt rotates the lower-lane pointer and leaves scores unchanged; `strict` starves the low lane (documented), `strict-with-aging` selects it periodically.

#### CAP-10 (subset) — Renewal waves and pressure gates
- **Proves:** SP-009 · Postgres.Tests · Fault
- **Steps:** Fixed 16, 16 long handlers started together so renewals fall due together; proxy `Latency(1 s)` on `control`; concurrent Finishes.
- **Assert:** renewal lane never exceeds 4 concurrent calls, settlement 2; wave queueing visible in recorded control wait; the stop gate (`StopMargin`, Fast 2.5 s / default 25 s) halts new Reserves; resume requires three healthy 1 s checks and margin ≥ `ResumeMargin`; a late Renew reply after the local deadline never revives authority; with no latency the gates do not flap under ordinary jitter. **Deferred:** autoscaler growth-freeze interaction (SP-014); `not-applicable`.

#### CAP-11 (subset) — Bounded overload and outage
- **Proves:** SP-009, SP-006 · Postgres.Tests · Fault (RequiresDocker for the outage)
- **Steps:** (a) Proxy `Latency(10 s)` so provider tasks outlive local timeouts. (b) 65 concurrent waiting dispatches in one group. (c) `RestartStore(Kill, 60 s)` under P-01 load.
- **Assert:** (a) I/O slots are held until the provider task ends and its connection is disposed; (b) the 65th returns `local_capacity_exceeded`, known-not-submitted, no receipt; (c) tickets, completion channel, pending outcomes and waiting producer operations stay within their bounds (16 / 16 / 16 / 64) throughout. **Deferred:** dashboard/management lanes (SP-020/SP-024); `not-applicable`.

#### INIT-01 (inferred) — Fresh and interrupted initialization
- **Proves:** SP-005 · Postgres.Tests · Fault
- **Definition (implementation choice, from SP-005 "fresh and interrupted initialization recover"):** run the migration runner on an empty database; separately kill its dedicated session after each migration phase/transaction; rerun; run two runners concurrently.
- **Assert:** a fresh run produces `schema_revision`, `protocol_epoch` and manifest exactly as specified; an interrupted run resumes from actual catalog state and never marks readiness falsely; the session advisory lock serializes concurrent runners; workers started before completion reject the store.

#### BATCH-01 (inferred) — Batch atomicity
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Definition (implementation choice):** `DispatchManyAsync` of 100 items with `Throw` at each `AfterMutation` step; one invalid item; 101 items; 2 MiB + 1 total.
- **Assert:** all-or-nothing: either all items created/reused or none; invalid input rejected known-not-applied with per-item diagnostics; size bounds enforced before any write.

#### BATCH-02 (inferred) — Per-ordinal receipt and replay
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Definition (implementation choice):** batch with Created, Reused (active and window) and `PolicyDrift` items; `DropReply`; replay.
- **Assert:** one receipt with one result per ordinal; replay returns identical per-ordinal results; `ResolveAsync` returns the same batch receipt.

#### BATCH-03 (inferred) — Conflicts and concurrent batches
- **Proves:** SP-006 · Postgres.Tests · Fault
- **Definition (implementation choice):** batch containing duplicate keys within itself and a window-fingerprint conflict, with and without `ContinueOnConflict`; two concurrent batches sharing keys in opposite orders.
- **Assert:** default: whole batch fails `UniquenessConflict`, nothing created; `ContinueOnConflict = true`: conflicting ordinals reported as failures with no rows, remainder committed atomically; duplicates within a batch resolve to one job; concurrent batches never deadlock.

## 14. First measurements (SP-010)

### 14.1 Status at G-1

Measurements are recorded and compared with the D-14 thresholds. A threshold miss at G-1 does not fail the gate; it requires a profile, trace and comparison in the report (performance-acceptance-plan "Failure disposition"). Any correctness violation observed during a workload (ReferenceModel rejection, false durable-success report, ownership/atomicity invariant break, resource count above a declared cap) **fails G-1**.

### 14.2 Environment fields (every run)

OS and kernel; CPU model and count; memory; container limits; disk type and fsync configuration; PostgreSQL version and image digest; server settings (`fsync`, `synchronous_commit`, `full_page_writes`, `shared_buffers`, `max_connections`, WAL settings); Npgsql version; .NET SDK/runtime (`global.json` 10.0.403); exact source commit; workload seed; load-generator configuration; topology (direct primary, proxy present or not); measured client↔server RTT; connection budgets (control 6, work 8, listener 0/1); worker timing profile; whether the host is the reference environment.

Reference environment: Linux x64, worker 4 vCPU / 8 GiB, separate database host 8 vCPU / 16 GiB on local SSD/NVMe, RTT ≤ 1 ms, PostgreSQL 18. Runs execute on a dedicated host, never a shared CI runner; any deviation from the reference environment is listed in the report.

### 14.3 Workloads

| Workload | Definition |
|---|---|
| **P-01 ordinary async work** | 1 KiB payload; fixed 16; handler `await Task.Delay(100 ms)`; 100 arrivals/s open-loop Poisson from a separate producer process (I-11); one queue, one worker process; warm-up 5 min, measured 15 min, 3 repetitions; drain all accepted work at the end |
| **P-02 store overhead** | No-op handler; fixed 16; offered rate starts at 50/s and increases by 50/s every 3 min step (1 min settle + 2 min measured). Saturation = first step where confirmed finishes < 98 % of offered rate, or backlog grows monotonically for 60 s. Sustainable throughput = highest step before saturation. Then a 15 min confirmation run at the sustainable rate, 3 repetitions (I-11) |
| **P-08a control latency 1 s** | P-01 workload; proxy `Latency(1 s, Both)` on worker `control` and `work` connections for 5 min, then removed for 5 min |
| **P-08b control latency 5 s** | As P-08a with 5 s (equal to the default `ControlRequestTimeout`) |
| **P-08c store outage** | P-01 workload; `RestartStore(Kill, downtime: 60 s)`; observe 10 min after restart |
| **G1-M1 idle cost** (inferred) | One worker, fixed 16, empty queue, 10 min; notifications as shipped in M2 |
| **G1-M2 renewal margin** (inferred) | Fixed 16; 16 handlers of 120 s continuously for 15 min (default timing) |
| **G1-M3 contention** (inferred) | Two worker processes fixed 8 each on one queue, type ceiling 4, no-op handler at the P-02 sustainable rate, 15 min |

P-08 lost replies, process death at named boundaries and ignored cancellation are covered as correctness cases (CMD-03, RUN-01, WRK-18, TIME-07); their workload versions are part of SP-029.

### 14.4 Metrics recorded (raw samples kept)

- Throughput: offered arrivals/s, dispatched/s, confirmed finishes/s, backlog over time, failures/throttles/`local_capacity_exceeded` counts.
- Latency p50/p95/p99/max with sample counts: dispatch (call → receipt), due-to-Begin (`eligible_at` → Begin `DecidedAt`), pipeline-end-to-confirmed-Finish, Reserve, Begin, Renew, Finish (each including local admission wait), control-I/O wait.
- Ownership: renewal margin min/p01, missed renewal margins, lease-safety losses, pressure-gate activations by reason, unresolved-command counts and oldest unsettled age.
- Store: WAL bytes per job (`pg_current_wal_lsn()` delta), rows per job/receipt, receipt writes/s, scope-lock wait, `EXPLAIN (ANALYZE, BUFFERS)` of the ready probe and Reserve candidate query at run end, connection count, idle probes/s and queries/s (G1-M1), SKIP-LOCKED misses and `CapacityBlocked` rate (G1-M3).
- Process: CPU, GC counts/pause, allocations per job, managed heap, RSS, thread-pool queue length and timer lag.
- P-08: per-resource maxima (tickets, completion channel, pending outcomes, producer waiters, connections, memory), admission pause/resume instants, time from store restoration to readiness (`Ready` probe plus first `Applied` Reserve).

### 14.5 Thresholds compared (D-14)

| Property | Threshold |
|---|---|
| P-01 throughput | ≥ 100 confirmed finishes/s steady state, no sustained backlog growth |
| P-01 latency | Dispatch p95 ≤ 50 ms / p99 ≤ 200 ms; due-to-Begin p99 ≤ 2 s; pipeline-end-to-confirmed-Finish p99 ≤ 500 ms |
| Healthy control I/O | p99 ≤ 100 ms including local admission; zero lease-safety losses in healthy runs (P-01, P-02, G1-M2) |
| P-08 outage | Resource counts within declared caps; admission pauses; readiness recovers ≤ 30 s after restoration |
| Correctness (all) | Zero violated state/identity/ownership/atomicity invariants; no false durable-success reports |

P-02 absolute maximum, write amplification and idle cost have no threshold; they are recorded measurements.

### 14.6 What the G-1 report must state

1. Every environment field (§14.2) and the deviation list from the reference environment.
2. Per workload: offered load, results, sample counts, each threshold with pass/miss, and for every miss the profile/trace reference and the measured cause.
3. **Two-round-trip ceiling.** Reserve and Begin are separate transactions issued sequentially before invocation, followed by Finish. Report measured mean and p99 of `t_reserve`, `t_begin`, `t_finish` and the per-job store-time floor `t_reserve + t_begin + t_finish`, and the slot-bound ceiling `16 / (t_begin + t_handler + t_finish + t_cleanup)` jobs/s.
4. **Serial-claim ceiling.** Per-pool claim concurrency is 1 and prefetch 0, so one pool admits at most `C_claim = 1 / t_claim` jobs/s, where `t_claim` is the measured mean Reserve duration including local admission. Report `C_claim`, and for P-01 the margin to 100/s (100/s needs `t_claim ≤ 10 ms`).
5. Which bound limited P-02's sustainable throughput: `C_claim`, the slot-bound ceiling, or a measured database bottleneck (lock wait, WAL, CPU) — with the evidence.
6. The consequence for **SP-031**: whether `ReserveAndBegin/1`, batched Reserve and batched Renew are justified by the measured ceilings, with the expected gain computed from the measured `t_reserve`/`t_begin`.
7. G1-M1 idle probes/s and queries/s; G1-M2 minimum renewal margin and zero-loss confirmation; G1-M3 contention cost; query plans.
8. Limits of the milestone: direct-primary profile only; process kill is not power loss; no HA, rates, scheduling, management, other drivers.

## 15. G-1 pass rule and report

### 15.1 Report location and format

The report is `docs/reports/sp-010-g1.md`. It follows the backlog §1 evidence rule (design revision, implementation commit, environment/profile, named cases, results, limits, supporting artifacts) in the acceptance-checklist §5 structure:

1. **Scope and exclusions; design/decision revisions** — spec revision of this document and the cited source documents; deferred sub-parts listed with their SP.
2. **Implementation commit, artifact versions, environment, persistence and topology profile** — commit SHA, package versions, §14.2 fields, store profile(s) used.
3. **Case results** — one row per case and per deferred sub-part:

   | Document | Case | Title | SP | Host project | Category | Result | Reason / evidence |
   |---|---|---|---|---|---|---|---|

   `Result` ∈ `passed` \| `failed` \| `unimplemented` \| `not-applicable` \| `not-run`. The table is generated by the report builder in `Runwick.Harness` from the xunit v3 result files and the `Case`/`Item`/`Category` traits; humans add only reasons and evidence links.
4. **Injected fault classes, workloads, thresholds and raw results** — fault classes actually exercised (in-process `DropReply`, wire `DropCommitReply`, `Sever`, `Latency`, `KillProcess`, `RestartStore(Graceful)`, `RestartStore(Kill)`, clock step, thread-pool stall) and §14.6 content with links to raw artifacts (`artifacts/perf/<run-id>/`, test result files, histories).
5. **Limits/dependencies, reviewer, decision and date.**

### 15.2 Pass rule

G-1 passes when all of the following hold:

1. Every case in §3's G-1 inventory has a row in the report; a missing row fails the gate.
2. Every row is `passed`, except sub-parts this document explicitly defers (WRK-02, WRK-15, WRK-19 and the "Deferred" parts of CMD-06, UNI-04, TIME-06, PGX-04, PGX-09, SER-06, CAP-01, CAP-10, CAP-11), which are `not-applicable` with the named later SP as reason.
3. No hidden skips: zero `failed`, `unimplemented` or `not-run` rows; no test with a G-1 `Case` trait is skipped in the run the report is generated from. That run uses Docker (no `RUNWICK_TEST_PG` escape hatch), so every `RequiresDocker=true` test executed.
4. Every Contract/Fault history was accepted by the ReferenceModel.
5. P-01, P-02, P-08 and G1-M1 … G1-M3 have recorded results with all §14.6 statements, and no correctness violation occurred during any workload.
6. Reviewer, decision and date are recorded.

Unsupported capabilities (rates, enlistment beyond PostgreSQL, HA) are declared limits of the milestone, not skipped tests.
