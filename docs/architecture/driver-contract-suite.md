> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Shared storage contract and fault-injection suite

Status: test design, 2026-10-29. No test project or driver exists yet; nothing below is a passing test claim. This suite verifies the [driver capability profiles](driver-capabilities.md) against the execution, capacity, and scheduling proposals.

## 1. Test the semantic boundary

Run identical behavioral scenarios against each real driver/profile. SQL shape, number of Redis keys, and lock mechanism are implementation details; job identity, command results, ownership, accounting, and externally visible atomicity are the contract.

Separate four suites:

1. **Pure engine/model tests:** deterministic clocks, registry/selector/controller state, recurrence and encoding. These cannot prove database locking or persistence.
2. **Real-store semantic tests:** independent connections/processes, synchronization barriers, operations through driver interfaces, and normalized inspection snapshots.
3. **Fault/durability profiles:** disconnects, process kill, server restart, failed writes, delayed commits, persistence barriers and, where advertised, failover. Each result names the injected fault and deployment profile.
4. **Workload measurements:** latency/throughput/memory/WAL/script time under stated loads. These cannot substitute for invariant tests.

A process kill is not a power-loss test: the OS may still retain and flush cached writes. An async replica promotion is not a same-primary crash restart. Do not report these fault classes interchangeably.

## 2. Harness contracts

Proposed test-only fixture surface:

```text
DriverFixture:
  Start(profile, isolatedNamespace)
  OpenClient(processIdentity)
  OpenBusinessTransaction()                  only for declared enlistment
  ReadCommittedSnapshot()                   normalized jobs/attempts/permits/etc.
  ArmFault(point, action, targetCommandId)
  WaitAtBarrier(point, targetCommandId)
  ReleaseBarrier(...)
  RestartStore(mode)
  Stop()
```

Clients use real serializers, SQL/scripts, receipt codecs, and connection behavior. Inspection uses a consistent committed view; it must not mistake a transaction's in-progress rows or Redis recovery projections for published state. An unavailable/recovering partition may report that state, but not a fabricated empty queue.

Fault points include before admission locks, after locks before fresh-time validation, before first mutation, after each mutation step, immediately before publication/COMMIT, after commit before reply, and during receipt lookup/cleanup. Redis's intent candidate adds after-intent, each projection, and before/after final publication. Production code must not expose arbitrary fault controls; use internal test builds/transport proxies.

Use deterministic barriers rather than sleep-based guesses to arrange races. Store time cannot generally be replaced by a .NET fake clock: combine controlled short leases with server-side barriers and separate clock-source tests. Clock adjustment tests run only in isolated infrastructure. Real timing assertions include bounded slack and observe store decision timestamps; do not weaken ordering invariants to make flaky tests pass.

Every history captures command ID/fingerprint, request and reply, local monotonic send/receive bounds, database decision instant, attempt/generation/token, and committed snapshots. Redact payload/secrets. A small independent reference model evaluates legal transitions; it must not call the same production classifier or SQL-builder being tested.

## 3. Mandatory semantic cases

| ID | Scenario / forced ordering | Required assertion |
|---|---|---|
| CMD-01 | Same mutation ID/body, concurrent and sequential replay | One logical effect and stable recorded result |
| CMD-02 | Same ID, changed operation/body/deadline/application scope | Conflict within its namespace; no cross-application result leakage |
| CMD-03 | Commit succeeds, response is dropped | Replay resolves original effect; no second job/attempt/grant |
| CMD-04 | Deadline expires while waiting for a lock | No new decision after expiry; an earlier applied receipt still resolves |
| CMD-05 | Receipt pruned after retention; old command resubmitted | Expired/unknown result, never application under a fresh identity |
| CMD-06 | Pure NoWork versus committed RateLimited Begin | Pure outcome may change on retry; committed deferral must replay |
| ADM-01 | Two processes claim the same supported ready job | One valid reservation; unsupported payload versions or serializer profile ID/versions never claimed |
| ADM-02 | Two jobs contend for last shared permit | One grant; loser creates neither an attempt nor partial permits |
| ADM-03 | A+B versus B+A scope requirements | Correct all-or-none admission, no application lock-order deadlock |
| ADM-04 | Queue pause races claim | Lock/publication order determines admission; dispatch remains allowed |
| ADM-05 | Mixed-type/resource-blocked lane and cursor wrap | Bounded scanning eventually discovers claimable later and earlier work |
| RUN-01 | Reserve crash before Begin versus crash after Begin | Separate abandonment guard and execution counters, each updated once |
| RUN-02 | Concurrent/replayed Begin | One execution authorization, one grant set, no duplicate attempt start |
| OWN-01 | Renew A, recover stale discovery of A | Recovery rechecks current token and expiry, leaves renewed ownership |
| OWN-02 | Expired A, replacement B, late renew/finish/release A | A cannot change B's state or release B's permits |
| OWN-03 | Lease expires while operation waits on locks | Fresh-time check rejects; transaction-start time cannot extend authority |
| OWN-04 | Renew multiple leases with one stale member | Healthy independent operations are not rolled back by stale member |
| OWN-05 | Missing/mismatched permit data | Invariant fault, no partial “successful” renewal |
| END-01 | Finish after observed success; response lost | One durable outcome; no invocation replay |
| END-02 | Cancel before Begin, during run, racing timely success | Matches specified state/cancellation precedence with audit preserved |
| END-03 | Manual retry races old Finish/new unique dispatch | New generation protected; uniqueness conflict changes nothing |
| END-04 | Timeout versus delayed acknowledgement | Late handler result times out; timely result does not time out merely due to storage latency |
| UNI-01 | Parallel same active key | Single active binding and documented reused job ID |
| UNI-02 | Same window key/same fingerprint; then changed payload | Stable reuse versus explicit mismatch conflict |
| UNI-03 | Active + window bindings identify different jobs | Explicit composition conflict; neither binding rewritten |
| UNI-04 | Terminal/retention/manual retry versus key reuse | Active binding cleared safely; window history survives required retention |
| RATE-01 | Competing Begins at Nth rolling-window boundary | No more than N grants in the defined interval per scope |
| RATE-02 | Multiple rate scopes, one full | No partial charge; closed deferred attempt; permits released; execution/abandonment budgets unchanged |
| RATE-03 | Failure/crash after authorization | Rate grant retained; retry needs another grant |
| RATE-04 | Window enlargement, cleanup, backward logical time | No reuse of missing history; edit freeze and nondecreasing time honored |
| SCH-01 | Two schedulers, same occurrence plan | One occurrence identity, job mapping, and cursor advance |
| SCH-02 | Failure between occurrence/job/cursor writes | No published partial operation; rollback or explicit recovery followed by consistent publication |
| SCH-03 | Pause/edit races materialization | Revisions enforced; no new occurrence merely from revision change |
| SCH-04 | Partial catch-up batch, restart, budget cap | Stable cutoff; cursor only covers processed range; remainder preserved |
| SCH-05 | History pruning and manual-trigger replay | High-water/receipt identity prevents unintended redispatch |
| ISO-01 | Same IDs/keys in two application namespaces | No claims, key bindings, scope counts or history cross namespaces |
| ENC-01 | Boundary timestamps, GUID ordering, large fences | Identical accepted precision/order; exact counters or explicit overflow |
| CFG-01 | Unsupported operation/schema/profile or stale revision | Rejected before unsafe writes; no silent guarantee downgrade |
| OBS-01 | Drop every notification | Polling still discovers due jobs, controls and recovery work |
| RET-01 | Retention races renewal/finish/schedule operations | No deletion of live authority or required receipts/fences/cursor history |

History checking permits legitimate concurrency reorderings. It does not demand a particular winner, FIFO across replicas, one physical execution across lease loss, or availability during an intentionally blocked recovery partition. Assert one **valid** ownership history under the profile's no-history-loss assumptions, not that arbitrary uncooperative application code stopped.

## 4. Enlistment cases

| ID | Scenario | PostgreSQL / SQLite | Redis |
|---|---|---|---|
| TX-01 | Business row + job + unique binding rolled back | Neither becomes visible; no committed receipt | Reject unsupported enlistment before writes |
| TX-02 | Same writes committed by caller | All visible together; driver neither commits nor disposes caller objects | Not advertised |
| TX-03 | Caller snapshot/deadlock/serialization failure | Whole-transaction retry remains caller-owned | Not advertised |
| TX-04 | Lost caller COMMIT response | Staged dispatch did not imply committed success; application resolves transaction ambiguity | Not advertised |
| OUT-01 | Relay dispatch commits, delivered mark lost | Stable message ID deduplicates within declared replay horizon | Same obligation for an outbox adapter |
| OUT-02 | Replay outside dedup retention / restored outbox | Explicit contract limit; no claim of unlimited exactly-once dispatch | Same |

Do not count TX-02 as passed for Redis by executing independent Redis and business-store writes and observing that both usually succeed. Outbox adapter tests become mandatory if that integration is packaged, not proof of native cross-store atomicity.

## 5. Driver-specific fault gates

### PostgreSQL

- Terminate a connection before/after COMMIT and confirm the correct rollback/unknown classification.
- Hold a scope lock past lease expiry; verify actual decision-time checks.
- Inject deadlock/serialization failures and preserve command identity on retry.
- Crash/restart the same primary under the selected WAL/commit profile; verify acknowledged commands remain present.
- For any HA claim, delay replication and promote according to the tested policy. Check jobs, receipts, rate grants and fences together; forbid a stale primary from continuing writes.
- Run compatibility/migration tests with permitted old/new engine versions; incompatible Begin implementations cannot skip rate accounting.

### SQLite

- Use separate OS processes on one local file; an in-process shared connection is insufficient.
- Force writer contention before BEGIN IMMEDIATE and busy/rollback handling through the actual provider.
- Attempt deferred-snapshot upgrade; verify caller-owned retry behavior.
- Crash a writer during mutations/commit; verify job, receipt and permits together after recovery.
- Hold dashboard readers and stress checkpoint/WAL growth while measuring renewal safety.
- Verify required connection pragmas after pool reuse, and classify unsupported network-file topology.
- Test synchronous provider execution against the bounded storage executor; no lease-coordinator inline blockage.

### Redis

- Inject a returned error/runtime failure after every state write. No Applied or no-effect response may conceal a partial transition.
- If an intent protocol is selected, kill/restart after every intent/projection/publication step. All authoritative readers must reject mixed projections; recovery must be idempotent and bounded.
- Corrupt expected key types, exhaust configured memory, deny a required command, and interrupt receipt/persistence-barrier responses. Recovery must not depend on silently evicting unrelated queue data.
- Verify same-slot placement for jobs with cross-queue resource limits and for scheduler batches. Reject a cross-slot plan before changing state.
- Compare server-time behavior with deliberately skewed client clocks; test numeric exactness near score/counter limits.
- Replay receipt resolution over a new connection; prove any chosen durability barrier applies to the correct primary/write stream. Barrier timeout never reports confirmed rollback.
- Restart an intact persistent primary and separately perform a deliberately lossy replica promotion. The latter must demonstrate/report the profile limitation, not be counted as passing no-history-loss conformance.
- Remove script cache/reload function bundles as supported; operation versions and key schema must remain compatible.

BullMQ tests for missing/stolen locks, renewal, stalled recovery, and rate limiting informed these cases. Runwick's additional command receipts, separate Begin, multi-scope rates, and partial-write requirements need its own tests. See the [pinned implementation review](bullmq-redis-review.md).

## 6. Performance acceptance reports

Use comparable job payloads, histories, queue/lane counts, active scopes, and failure rates, but allow different physical algorithms. Every report includes server/provider versions, hardware/storage, persistence and replication settings, connection budgets, raw samples, and workload seed.

Measure at least:

- Idle probes per second versus pools/lanes/replicas, with notifications disabled and enabled.
- Reservation/Begin/renew/Finish p50/p95/p99 and missed renewal margins, not throughput alone.
- Saturated queue/type/resource scopes at 1/3/6 workers or processes; rate-rejected Begin churn.
- Database writes/WAL or Redis bytes and script duration per attempt, per heartbeat, and per retained receipt/grant.
- Recovery backlog and scheduling catch-up impact on foreground dispatch and renewals.
- Cleanup/checkpoint/compaction interference at realistic retained-history sizes.
- Memory and drain time during a prolonged storage outage; pending completions remain bounded.

Set pass thresholds from an agreed workload before claiming a production capacity. An optimization is accepted only if it preserves semantic tests and improves the intended workload without unexplained tail-latency regressions. Do not compare Redis with persistence disabled against fully durable SQL and call the result a driver ranking.

## 7. Certification result and implementation order

Each driver/profile produces a report with: semantic version, configuration checks, applicable test IDs, pass/fail/unimplemented/not-applicable, fault-class coverage, unresolved limitations, and benchmark references. A mandatory unimplemented test blocks certification. Unsupported enlistment/topology is a declared capability, not a hidden skipped test. A known-loss failover experiment can pass its limitation-reporting test while still failing the stronger persistence profile.

Build the harness and reference model with the first PostgreSQL execution slice. Add rate/scheduler cases when those operation versions land. Port SQLite against the same logical snapshots. Prototype Redis publication/recovery and durability before investing in full dashboard/read-side parity. Keep tests versioned with contracts so a semantic change updates every driver expectation.

No runtime test, upstream test suite, container fault experiment, or benchmark was executed during this planning pass.

The later [identity/serialization packet](identity-and-serialization.md) adds SER-01–12, including exact byte preservation and profile-aware reservation. Run its provider-relevant cases alongside this suite; an identical name/version with an unsupported serializer profile is not a compatible job. These cases remain unexecuted.
