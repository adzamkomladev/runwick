# 10 — Rate admission and rate-aware Begin (SP-013)

**Sources:** [capacity-control.md](../architecture/capacity-control.md) §3–§4, §6; [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md) §3, §7 (CAP-05); [driver-contract-suite.md](../architecture/driver-contract-suite.md) (RATE-01 … RATE-04); [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md); [postgres-upgrades-and-compatibility.md](../architecture/postgres-upgrades-and-compatibility.md); 03 §5.2 (T20), §6.3, §10.3; 04 §5, §8; 05 §3.3, §9; 08 (actor, audit, events).

## Changes (2026-10-31, SP-013)

New document. It replaces 03 §6.3 ("not in M2").

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution |
|---|---|---|
| C-1 | capacity-control §4 calls the edit freeze "a proposal to review"; capacity-defaults §3 recommends it | Normative here (§6.3). D-07 closed at G-0 |
| C-2 | capacity-control asks for "an operation/schema version" for rate-aware Begin | Begin stays `Begin/1`. The guard is the application capability `rate_admission` (§6.1). A binary that does not know it fails the gate on every creation path (03 §10.3), so it can never own a rate-limited job |
| C-3 | Where seed policies come from for resource keys that appear at dispatch | Seeds travel with the dispatch item, outside the fingerprint (§3.1). Scope membership is in the fingerprint (`rateScopes`, 02 §7.1) |

Implementation choices *[choice]*:

1. Builder surface: `JobBuilder.RollingWindow(maxExecutions, window)` (job-type scope) and `RollingWindowPerResource(selector, maxExecutions, window)`. Queue rate scopes are supported by records and Begin but have no builder in M3.
2. Edits go through a separate operation `EditRatePolicy/1`, not `EditScopePolicy/1`, because the arithmetic and freeze differ.
3. A deferral's jitter is drawn once inside the transaction and stored in the receipt.

Changes (2026-11-01, owed cases):

- 2026-11-08 (SP-028): §6.1 names no caller. The operator path is `ActivateCapabilityAsync` on the three migration classes and `runwick store activate-capability --name rate_admission` (23 §3.6); it writes no audit row or event. The store operation stays for in-process callers.

- 2026-11-01: The capacity-control §6 rate rows are cases RATE-05 … RATE-10 (15 I-1). The enlargement/cleanup race has no ID and stays with SP-021.
- 2026-11-01: §4 lock order, made testable: the rate scopes are locked by one `SELECT … ORDER BY rate_scope_id FOR UPDATE`, so there is no fault point between two scope locks. RATE-09 interleaves the acquisitions with an admin row lock on the higher-UUID scope plus a barrier at `AfterLocksBeforeFreshTime`, and checks which backend the second Begin waits on.
- 2026-11-01: §6.1, "binaries without the capability": their handshake also fails after activation, because RegisterQueue and RegisterScope are creation paths (03 §10.3). An old binary already running keeps Renew, Finish, Recover and ReleaseReservation. This is the UPG-06 "rollback after activation is rejected" subset, together with the refusal to lower a capability.
- 2026-11-01: §4 "a crash after Begin keeps the grant": Recover charges and refunds nothing (RATE-08).

## 1. Contract

An exact rolling-window count of Begin authorizations. For a scope with limit N and window W, at logical decision time t, a Begin is admitted only if fewer than N grants of that scope have `authorized_at ∈ (t − W, t]`. Each execution costs one grant in every scope it requires, all or none. A grant is a Begin authorization, not when user code or a downstream call ran.

There is no implicit limit, and no other algorithm silently replaces this one.

## 2. Registration

| Bound | Value |
|---|---|
| Limit | 1 – 100,000 |
| Window | whole milliseconds, 1 s – 24 h |
| Rate scopes per job | 8 |
| Job-type rate per job | at most one |

Violations are registration errors (`invalid_rate_policy`, `too_many_rate_scopes`, `same_layer_conflict`). Resource keys follow the resource-key grammar (`invalid_key`).

## 3. Records (migration `0004_rates`)

### 3.1 `rate_scopes`

`(application, rate_scope_id)` PK, natural key `(application, scope_kind, scope_key)`, `limit_count`, `window_ms`, `policy_revision`, `last_decision_at` (logical time high-water mark), `not_before` (edit freeze end, logical time), `updated_by`.

Dispatch seeds an absent scope from the item's seed policy and never changes an existing one. Seeds are inserted in (kind, key) order with `ON CONFLICT DO NOTHING`, then the IDs are resolved.

### 3.2 `job_rate_requirements`

`(application, job_id, rate_scope_id)`, written at dispatch, immutable for the job's life (every generation, including manual retries, keeps the dispatch-time set). The FK to jobs cascades.

### 3.3 `rate_grants`

`(application, rate_scope_id, attempt_id)` PK, `job_id`, `authorized_at` (= logical t), `policy_revision`. Index `(application, rate_scope_id, authorized_at)`. There is no FK to jobs: grant history survives job retention. Grant cleanup (the predicate "outside the current window, under the scope lock") is SP-021.

## 4. Begin/1 with the rate step

Lock order: gates → receipt → job → active key (cancel/exhausted plans only) → concurrency scopes (UUID order) → rate scopes (UUID order) → dependent writes.

1. Lock the job and decide the plan. Cancellation and an exhausted budget close the attempt before any rate step, as in M2.
2. For the authorize plan of a job with rate requirements (immutable, read without a lock), lock its concurrency scopes, then its rate scopes.
3. `DecideAsync` (fresh database time after the last lock), then the ownership check.
4. `t = max(now, last_decision_at of every locked rate scope)`. Every locked scope's `last_decision_at` is raised to t, whichever way the decision goes, so logical time never goes backward.
5. Count grants in `(t − W, t]` per scope in a fresh statement. A scope blocks if the count is ≥ N or `t < not_before`.
6. **Admitted:** insert one grant per scope at t, then perform the M2 Begin writes and receipt.
7. **Deferred (T20):** charge nothing. The hint is the latest, over the blocking scopes, of the instant the (count − N + 1)th oldest retained grant leaves the window and `not_before`. Then:
   - `logical = max(hint, t + 250 ms)`;
   - `EligibleAt = now + (logical − t) + jitter[0, 100] ms`, rounded up to the millisecond. A backward clock therefore defers longer, never shorter;
   - close the attempt `admission_deferred` with the exact permits released, ownership cleared and the job back to `pending` (started_count = 0) or `retrying`, with `eligible_at = EligibleAt`;
   - no execution, abandonment or unstarted-guard charge; active uniqueness is kept;
   - event `attempt_admission_deferred` with the blocking scopes and t;
   - reply `RateLimited` / effect `Applied`, `Error = rate_limited` with EligibleAt (epoch ms) in Detail. The receipt holds `{jobId, generation, revision, state, deferredUntil, blockingScopes}`.

A replay returns the same deferral (same EligibleAt and DecidedAt) and never closes twice or charges. Renew, Finish and Recover never charge or refund. A handler failure or a crash after Begin keeps the grant, and the retry needs a new one.

## 5. Worker

`RateLimited` with a committed effect retires the entry: no scope, no middleware, no unstarted-guard increment, and the ticket returns to the pool at once. The engine counts it in `RateDeferrals` and emits `beginRateDeferred`. A `RateLimited` reply without a committed effect is an invariant fault. The worker does not hold a slot until EligibleAt, and Reserve never precharges. Queue/scope cooldowns on deferral are SP-012's classification work.

The 05 §3.3 / V-24 refusal is lifted for binaries that know `rate_admission` (all SP-013 builds).

## 6. Administration

Both operations are administrative (`durable` class, 5 min default NotAfter, 10 min cap), take a `ControlActor` (08 §2), and write audit and an event in the effect's transaction (08 §5, §7).

### 6.1 `ActivateCapabilities/1`

It merges `{capability: version}` into `application_controls.required_capabilities` and never lowers one. Only capabilities this binary knows, at most its own version, are accepted (`invalid_request` otherwise). Applying the same set again gives `no_change`. It bumps the application revision and records the event `capabilities_activated` and an audit row with target `application`.

Until `rate_admission` is active, Dispatch of an item with rate scopes is `UnsupportedVersion` / `rate_admission_not_active`, with nothing written. After activation, binaries without the capability fail the gate on every creation path (03 §10.3) and keep only settlement paths. Deactivation and rollback after activation are rejected (UPG-06 subset); the full matrix is SP-026.

### 6.2 `EditRatePolicy/1`

`{scopeKind, scopeKey, limit, windowMs, expectedPolicyRevision, actor}`, with bounds as §2.

| Result | When |
|---|---|
| `NotFound` / `scope_not_registered` | No such scope |
| `VersionConflict` / `revision_conflict` (`actual=`) | Stale revision |
| `InvalidState` / `no_change` | Same limit and window; the freeze is not restarted |
| Applied | Revision + 1, the event `rate_policy_edited`, an audit row with target `rate_scope` / `kind/key` |

### 6.3 Edit freeze

A semantic change sets `not_before = t + newWindow`, where t is the scope's nondecreasing logical time. Existing grants remain. This avoids admitting on history that a larger window would need but that cleanup may already have pruned.

## 7. Cases

| Case | Test | Proves |
|---|---|---|
| RATE-01 | `Competing_begins_get_at_most_n_grants_per_rolling_window` | 5 concurrent Begins on a 2/3 s scope → 2 admitted and 3 deferred, with EligibleAt ≥ the oldest grant + W. After the window, no 3-second interval ever holds more than 2 grants |
| RATE-02 | `One_full_scope_defers_without_a_partial_charge_and_releases_permits` | With two scopes, one full: no grant in either, the attempt is `admission_deferred`, permits are released, `started/unstarted/execution` = 0/0/0, the job is pending, one event |
| RATE-03 | `A_failed_execution_keeps_its_grant_and_the_retry_needs_a_new_one` | The grant survives RetryableFailure. The retry's Begin is deferred (`retrying`) |
| RATE-04 | `Window_edits_freeze_admission_and_logical_time_never_goes_backward` | Identical edit → `no_change`. Enlarged window → freeze, and Begin is deferred to ≥ `not_before`. Stale revision → conflict. A scope time ahead of the clock charges at that later instant |
| CMD-06 (rate half) | `A_replayed_deferral_returns_the_same_decision_without_a_second_close` | Same Begin command twice: the same deferral, one close, no extra grant |
| CAP-05 | `Rate_policy_bounds_are_validated_at_registration_and_on_edit` | Registration and edit bounds; unknown scope |
| PGX-03 (rate half) | `Rate_scopes_are_refused_until_the_application_activates_rate_admission` | Refused before activation, nothing written. Activation replay, `no_change`, unknown capability, audit |
| TIME-06 (rate sub-part) | `The_worker_retires_a_deferred_begin_and_runs_the_job_once_admitted` | The real worker: deferrals ≥ 1, every job runs once, consecutive grants ≥ W apart, unstarted guard untouched |
| RATE-05 | `RateAdmissionReplicaTests.Three_replica_processes_never_grant_more_than_n_per_rolling_window` | Three peer worker processes: exactly N grants in the first contended window, never more than N in any window, no partial two-scope charge |
| RATE-06 | `RateAdmissionReplicaTests.A_rate_rejection_after_reservation_releases_exact_permits_and_sets_eligibility` | Exact permit release, closed attempt, unchanged budgets, `eligible_at` = EligibleAt |
| RATE-07 | `RateAdmissionReplicaTests.A_lost_begin_reply_replays_without_a_second_grant_or_start` | Lost Begin reply (Applied and deferred): replay neither charges nor starts twice |
| RATE-08 | `RateAdmissionReplicaTests.A_crash_after_begin_keeps_the_grant_and_the_retry_needs_a_new_one` | Peer killed after Begin commits: grant kept through recovery, the retry needs a new grant |
| RATE-09 | `RateAdmissionReplicaTests.Opposite_order_rate_scopes_lock_in_uuid_order_and_never_deadlock` | Opposite declared order under barriers: UUID lock order, no deadlock, all-or-none grants |
| RATE-10 | `RateAdmissionClockTests.A_backward_store_clock_never_reverses_rate_time_or_refills_early` | Backward store clock (libfaketime): no early refill, logical time kept |
| UPG-02 (subset) | `RateAdmissionReplicaTests.After_activation_an_old_binary_cannot_begin_and_keeps_only_settlement_paths` | Old-binary peer: Begin refused after activation (also when racing it), settlement paths kept |
| UPG-06 (subset) | `RateAdmissionReplicaTests.Rollback_after_activation_is_rejected` | No lowering; an old binary cannot start after activation |
| PGX-01 | `RateGateRaceTests.Gate_changes_race_every_command_family_at_the_current_revision` | Gate changes race every command family, including the rate-aware Begin, at revision 7 |

Not yet covered: the P-06 rate measurement, still owed by SP-013 before acceptance. The "enlargement/cleanup race" row needs SP-021's cleanup predicate and is deferred to SP-021 (`not-applicable`).

## 8. Must not claim

- that a grant timestamp is when user code or a downstream call ran;
- token-bucket or approximate semantics;
- a safe throughput range from the validation bounds;
- that grant cleanup exists (SP-021);
- that a deferral hint is a reservation: another worker may take the capacity first.
