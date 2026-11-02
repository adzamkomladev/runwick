# PF-05 proposal: one server-side function per operation

> **Proposal, 2026-11-02. Not approved. Not normative.** `docs/spec/` stays the single normative source. No spec file is changed by this document; where PF-05 would change a spec paragraph, the paragraph is quoted and the exact proposed change is stated (§9). No code is written before the reviewer (adzamkomla) has answered §10. Every number below is copied from `docs/reports/pf-01.md` … `pf-04.md` (host: the shared KVM guest, **non-reference**, `runwick-pg` through `docker-proxy`). No measurement was run for this proposal.

Inputs: `docs/plans/perf-plan-draft.md` (PF-05 and the "Decision point after PF-04"), 03 §1, §8–§10, 04 §5–§10 and the dated PF-02/03/04 notes, 07 §2.2–§2.3 and the dated PF-02/03/04 notes, `src/Runwick.Postgres/Store/PgRunner.cs`, `PgTransaction.cs`, `PgStore.*.cs`.

## 0. Summary

- Each worker-path operation becomes one batch `[BEGIN, set_config, SELECT * FROM runwick.<op>_v1(…)]` followed by `COMMIT` (Applied) or `ROLLBACK` (anything else): **2 round trips per call**, against 4–9 today.
- Per job (Dispatch + Reserve + Begin + Finish): **21 → 8 round trips** (11 if Dispatch stays on statements in the first stage). Renew: **4 → 2**.
- The 03 protocol is unchanged: the same replies, codes, receipts, receipt codec v1, outcome classification (COMMIT stays a separate message, so 03 §9.2 is untouched), fencing and `synchronous_commit`.
- 04 §5 lock order and the 03 §1.3 fresh-time rule hold by construction: the function body runs the 04 §7 statements in their written order, one statement at a time, in a `VOLATILE` function under READ COMMITTED.
- The driver-side fault points that sit between body statements (`AfterLocksBeforeFreshTime`, `BeforeFirstMutation`, `AfterMutation`) become `runwick.rw_fault()` hook lines in the function bodies. A migration variant flag strips them from production stores. The other four points stay in the client.
- Functions are versioned by name (`reserve_v1`), never replaced in place, and covered by the migration checksum plus a body digest check.
- Recommendation: a measured **spike first** (Renew and Reserve), then the worker path **before** PF-06 batched Reserve, so `ReserveAndBegin/1` is written once, as a function. Dispatch comes last.

## 1. The problem

### 1.1 Where PF-01 … PF-04 left the count

PERF-01 counts commands per operation. A round trip (RT) is one command, plus COMMIT for driver-owned operations, plus `RELEASE` for staged ones (pf-01 §1).

| Operation | PF-01 (RT) | PF-02 (RT) | PF-04 commands | PF-04 RT |
|---|---|---|---|---|
| Dispatch | 15 | 11 | 4 | 5 |
| Reserve | 17 | 13 | 5 | 6 (7 for a job with concurrency scopes) |
| Begin, unrated | 16 | 12 | 4 | 5 |
| Begin, rated (1 or 8 scopes) | 21 | 17 | 6 | 7 |
| Renew | 9 | 7 | 3 | 4 |
| Finish | 16 | 12 | 4 | 5 (6 for a retryable outcome) |
| Recover | 16 | 12 | 8 | 9 |
| ReleaseReservation | 16 | 12 | 8 | 9 |
| `StageAsync` | 16 | 12 | 5 | 6 |
| `StageFinishAsync` | 17 | 13 | 5 | 6 |
| **Per job** | **64** | **48** | **17** | **21** |

The decision point in the plan reads: *"If per-job round trips are still above ~20, or the Reserve target is missed on the reference environment, do PF-05. Otherwise PF-05 waits until after G-6."*

- **21 > 20.** This criterion is deterministic and does not depend on the host, so it is met on its own.
- The Reserve target (p50 ≤ 25 ms) was missed **on this non-reference host**: 39.0 / 40.3 ms in the PF-04 runs, and 33.6 / 37.2 ms after PF-03 (pf-04 §3, pf-03 §3). That is not evidence for the reference-environment criterion, which is still open (PQ1).

### 1.2 What each remaining round trip is (post-PF-04 code)

| Operation | RT 1 | RT 2 | RT 3 | RT 4 | RT 5 | RT 6 | RT 7 |
|---|---|---|---|---|---|---|---|
| Reserve | BEGIN + timeouts + G1 + G2 | R1 + R3 + receipt clock | Q + candidate claim | G + payload + S + clock | (scoped jobs) live-permit count P | effects + R4 | COMMIT |
| Begin, unrated | gates | receipt | J + G (`EXISTS` rate req.) + clock | effects + R4 | COMMIT | | |
| Begin, rated | gates | receipt | J + G + clock | S + R (rate scopes) + clock | window counts | effects + charge + R4 | COMMIT |
| Renew | gates | J + S + clock | job + permit updates | COMMIT | | | |
| Finish (terminal outcome) | gates | receipt | J + G + K + S + clock | close A→P→J'→K'→E + R4 | COMMIT | | |
| Finish (retryable) | gates | receipt | J + G | K? + S + clock | close + R4 | COMMIT | |
| Dispatch (no keys) | gates | receipt | queue check + clock | effects + R4 | COMMIT | | |
| `StageFinishAsync` | read-only check + SAVEPOINT | G1 + G2 | receipt | locks + clock | close + R4 | RELEASE | |

- **The frame costs three round trips per receipted call:** the gate batch, the receipt batch and COMMIT. Across a job's four operations that is 12 of the 21.
- **The body costs the other nine.** Each body batch boundary exists for one of two reasons: a later statement depends on an earlier result (the plan, the lock set, the decision), or the 04 §6.1 rule "a decision that depends on a lock result is never batched ahead of that lock" forbids merging. PF-04 already merged everything that rule allows.
- **Statement batching is exhausted.** Further client-side merging would put decisions ahead of their locks.

### 1.3 What 2 round trips can and cannot buy (estimate, to be measured)

From the PF-03 runs on this host (load 4.4–7.8):

- **Marginal cost of one command:** (Reserve 33.6 − Renew 20.6) / 2 ≈ 6.5 ms, and Begin − Renew ≈ 4.8 ms. That is about 5–6.5 ms per command, including that command's server work and client handling.
- **Bare round trip:** `SELECT 1` (open + 1 RT) is 2.5–2.9 ms.
- **Reserve, 6 → 2 RT:** removing 4 commands takes about 19–26 ms off 33.6–37 ms. The server still runs the same statements inside the function, so a range of **about 10–20 ms** is plausible on this host. It is not established.
- **The residue is server execution and client CPU** (73–133 ms per job before PF, not re-measured since; pf-03 §3). PF-05 reduces client CPU (one command, one parameter set and one reader per call) but does not remove it. PF-08 is still needed.

This uncertainty is why §6 recommends a spike before the full item.

## 2. What is not reopened

These stay exactly as specified:

- **Receipts and replay:** 03 §8 and R1–R4.
- **Fencing:** 03 §6.6 ownership check, 04 §7.3 fences.
- **Lock order:** 04 §5.
- **Fresh time after locks:** 03 §1.3.
- **Durable COMMIT:** `synchronous_commit = on`.
- **Connections:** READ COMMITTED, one transaction per pooled connection, no multiplexing.
- **Renew without a receipt:** R1.
- **Replies:** 03 §9 failure classification and every stable code. The only exception is §3.8 item 5, which is put to the reviewer.
- **Pools and lanes:** 04 §9.1.
- **Statements:** the 04 §7 statement text. The functions contain the same statements.
- **Other drivers:** SQLite (SP-019) and Redis (SP-025).

## 3. Design

### 3.1 Frame

Driver-owned operation:

```text
RT 1  [BEGIN ISOLATION LEVEL READ COMMITTED]                         (Npgsql sends it with the first command)
      SELECT set_config('lock_timeout', …, true), set_config('statement_timeout', …, true),
             set_config('idle_in_transaction_session_timeout', '10000ms', true);
      SELECT * FROM runwick.reserve_v1(p_application => $1, …);        (one batch)
RT 2  COMMIT        when effect = 'Applied'
      ROLLBACK      otherwise (no effect, replay, rejection, invariant error)
```

Staged operation (`StageAsync`, `StageFinishAsync`), on the caller's connection and transaction:

```text
RT 1  SAVEPOINT runwick_stage; SELECT * FROM runwick.finish_v1(…);    (one batch; no set_config, as today)
RT 2  RELEASE SAVEPOINT runwick_stage                                 when effect = 'Applied'
      ROLLBACK TO SAVEPOINT runwick_stage; RELEASE SAVEPOINT …        otherwise
```

- **Read-only caller transaction.** The read-only check moves into the function: its first statement reads `current_setting('transaction_read_only')` and, when it is `on`, returns the driver-only status `TransactionInvalid` without writing anything. The client then rolls back to the savepoint and throws `transaction_invalid` (`StagingValidationException`), as today.
- **COMMIT stays a separate message.** The client chooses COMMIT or ROLLBACK from the function's result. That keeps "COMMIT written" observable to the client, so 03 §9.2 is unchanged: a failure before RT 2 is sent is *not applied*, and a failure after it is *outcome unknown*.
- **Single-trip variants are not proposed** (see §6 D).

### 3.2 Common parameters and results

Every operation function takes the **frame parameters**. Values the C# side already owns are passed in, so C# stays their single source.

```sql
p_application      text,          -- rw_identifier
p_command_id       uuid,
p_not_after        timestamptz,
p_fingerprint      bytea,         -- 02 §8.3 command digest; NULL for Renew
p_class            text,          -- Operations.RetentionClass(op); NULL for Renew
p_deadline_cap     interval,      -- Operations.DeadlineCap(op)
p_store_id         uuid,          -- expected store identity (PgOp.ExpectedStoreId ?? runner StoreId); NULL = any
p_revisions        bigint[],      -- schema revisions this binary accepts (§5.3)
p_epoch            integer,       -- binary protocol epoch
p_capabilities     jsonb,         -- RunwickRuntime.KnownCapabilities
p_is_creation      boolean,       -- PgOp.IsCreation
p_allow_drain      boolean,       -- PgOp.AllowedWhileDraining
p_faults           jsonb DEFAULT NULL   -- harness only (§4); the production variant never reads it
```

Every function returns one row with the **common result columns** and then its own columns:

```sql
status text, effect text,              -- StoreStatus name; 'Applied' | 'ObservedNoEffect'
code text, detail text,                -- StoreError.Code / Detail
conflicting_job_id uuid, blocked_scope_id uuid, resume_eligible_at timestamptz, resume_job_id uuid,
decided_at timestamptz, replayed boolean,
receipt_status text, receipt_result jsonb,   -- on replay only: the stored row, decoded by ReceiptCodec in C#
faults_fired text[]                          -- harness variant only; NULL in production
```

- **Call style.** Calls use named notation (`p_x => @x`), so adding a defaulted parameter in a later version cannot shift positions.
- **Reply mapping.** The C# side maps the row to `StoreReply<T>` with the same records as today.
- **Replay decoding stays in C#.** On replay the function returns the raw receipt; `ReceiptCodec.Read*` decodes it. Replay therefore has one implementation, whichever path wrote the receipt.

### 3.3 Shared internal functions

These are called only from operation functions. All are `VOLATILE` (except `rw_retry_delay_v1`), `SECURITY INVOKER`, and have no `SET` clauses (§3.8).

| Function | Body (spec) | Returns |
|---|---|---|
| `runwick.rw_gate_v1(p_application, p_store_id, p_revisions, p_epoch, p_capabilities, p_is_creation, p_allow_drain, p_seed boolean)` | G1 `FOR SHARE`, G2 `FOR SHARE` (04 §6.2), every 03 §10.1 check in the order of `PgRunner.CheckGatesAsync`. `p_seed` is the RegisterApplication seed, which is not in scope | `(status, code)`, both NULL = pass |
| `runwick.rw_receipt_v1(p_application, p_class, p_command_id, p_operation, p_fingerprint, p_not_after, p_deadline_cap)` | R1; R3 only when R1 inserted; otherwise R2 `FOR UPDATE`. Then the receipt clock and the `CommandExpired` / `deadline_too_far` checks (04 §6.3, 03 §8.2 steps 1–3) | `(outcome, r_status, r_result, r_decided_at)`; outcome ∈ `new`, `replay`, `conflict`, `incomplete`, `expired`, `too_far` |
| `runwick.rw_finalize_v1(p_application, p_class, p_command_id, p_status, p_result, p_decided_at)` | R4, then a row-count check | void; raises `RWI03` if the count ≠ 1 |
| `runwick.rw_ownership_v1(state, attempt, token, worker, generation, lease_expires_at, now, lease cols, allowed_states text[])` | 03 §6.6, branch for branch as `PgStore.Ownership` | `(status, code)` or NULLs |
| `runwick.rw_close_v1(…)` | 04 §7.10 A → P → J' → K' → E, with a row-count check after each statement | new revision |
| `runwick.rw_retry_delay_v1(n int, initial_ms bigint, max_ms bigint, entropy bigint)` | exponential-v1 with equal jitter (`RetryEvaluator.Delay`). The unsigned 64-bit entropy is reconstructed as `entropy::numeric + 2^63` | bigint ms (`IMMUTABLE`) |
| `runwick.rw_uuid_v7(ts timestamptz)` | A v7 UUID from `ms_floor(ts)` and `gen_random_uuid()` bits, with the layout of `Guid.CreateVersion7(DateTimeOffset)`. PostgreSQL 16–17 have no `uuidv7()`, and attempt IDs are built from the decision time today (§10 Q9) | uuid |
| `runwick.rw_fault(p_point text, p_step int, p_faults jsonb)` | Test hook (§4). It exists only in the fault-hooks variant | void |

### 3.4 Operation signatures

Each operation function takes the frame parameters (§3.2) and returns the common result columns. Only the operation-specific parts are shown.

```sql
-- Reserve/1 (04 §7.3)
CREATE FUNCTION runwick.reserve_v1(<frame>,
    p_queue text, p_priority text,
    p_after_eligible_at timestamptz, p_after_job_id uuid,
    p_names text[], p_versions integer[], p_profile_ids text[], p_profile_versions integer[],  -- 1..512, validated in C#
    p_lease interval, p_worker_id uuid, p_token uuid)                                         -- token drawn by the client (random, no time)
RETURNS TABLE (<common>,
    job_id uuid, generation integer, revision bigint, attempt_id uuid, lease_expires_at timestamptz,
    reservation_number bigint, job_name text, payload_version integer,
    serializer_profile_id text, serializer_profile_version integer, eligible_at timestamptz,
    max_executions integer, max_unstarted integer, execution_timeout_ms bigint,
    retry_policy jsonb, unstarted_retry_policy jsonb,
    fence_scope_ids uuid[], fence_values bigint[],
    payload bytea, headers jsonb, created_at timestamptz)              -- also returned on replay; payload NULL → HistoryUnavailable

-- Begin/1, unrated and rated (04 §7.5, 10 §4)
CREATE FUNCTION runwick.begin_v1(<frame>,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_min_defer interval, p_max_jitter_ms integer, p_entropy bigint)   -- 06 defaults and the deferral jitter draw
RETURNS TABLE (<common>,
    revision bigint, state text, lease_expires_at timestamptz,
    execution_number integer, total_execution_number bigint,          -- authorize
    attempt_outcome text, terminal_reason text,                       -- cancel / exhausted
    deferred_until timestamptz, blocking_scope_ids uuid[])            -- RateLimited (Applied)

-- Renew/1 (04 §7.6): no receipt, so p_fingerprint, p_class and p_deadline_cap are NULL
CREATE FUNCTION runwick.renew_v1(<frame>,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer, p_lease interval)
RETURNS TABLE (<common>, lease_expires_at timestamptz, cancel_requested boolean)

-- Finish/1 (04 §7.7); the same function serves StageFinishAsync
CREATE FUNCTION runwick.finish_v1(<frame>,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer,
    p_outcome text, p_elapsed_ms bigint, p_requested_delay_ms bigint, p_error jsonb, p_entropy bigint)
RETURNS TABLE (<common>,
    revision bigint, state text, attempt_outcome text, disposition text, terminal_reason text,
    eligible_at timestamptz, retry_delay_ms bigint)

-- Recover/1 (04 §7.8)
CREATE FUNCTION runwick.recover_v1(<frame>,
    p_job_id uuid, p_expected_attempt_id uuid, p_expected_token uuid, p_entropy bigint)
RETURNS TABLE (<same columns as finish_v1>)

-- ReleaseReservation/1 (04 §7.9)
CREATE FUNCTION runwick.release_reservation_v1(<frame>,
    p_job_id uuid, p_attempt_id uuid, p_worker_id uuid, p_token uuid, p_generation integer)
RETURNS TABLE (<common>, revision bigint, state text, disposition text, eligible_at timestamptz)

-- Dispatch/1 and DispatchMany/1 (04 §7.2), driver-owned and StageAsync; stage c (§6)
CREATE FUNCTION runwick.dispatch_v1(<frame>,
    p_many boolean, p_continue_on_conflict boolean,
    -- items, one element per ordinal
    p_job_ids uuid[], p_job_names text[], p_payload_versions integer[], p_profile_ids text[], p_profile_versions integer[],
    p_queues text[], p_priorities text[], p_not_befores timestamptz[],
    p_active_keys text[], p_window_keys text[], p_window_ms bigint[], p_business_fps bytea[], p_policy_fps bytea[],
    p_payloads bytea[], p_headers jsonb[],
    p_max_executions integer[], p_max_unstarted integer[], p_timeouts_ms bigint[], p_retry jsonb[], p_unstarted_retry jsonb[],
    -- scope and rate provisioning, flattened (PostgreSQL arrays cannot be ragged)
    p_scope_ordinals integer[], p_scope_kinds text[], p_scope_keys text[], p_scope_capacities integer[],
    p_rate_ordinals integer[], p_rate_kinds text[], p_rate_keys text[], p_rate_limits integer[], p_rate_windows_ms bigint[])
RETURNS TABLE (<common>,
    items jsonb)   -- [{ordinal, jobId?, disposition?, match?, policyDrift?, failureCode?, conflictingJobId?}], camelCase as 03 §8.5
```

`ReserveAndBegin/1` (PF-06) would be `runwick.reserve_and_begin_v1`, built from the same helpers. It needs its own 03 proposal and is not specified here.

### 3.5 Body shape (Reserve as the example)

The statements are the 04 §7.3 text. Comments carry the 04 references so review can compare the body line by line.

```sql
-- 0. staging read-only guard (§3.1)
-- 1. Gates: SELECT * INTO … FROM runwick.rw_gate_v1(…);  rejection → RETURN (client ROLLBACK)
-- 2. Receipt: rw_receipt_v1(…)
--      'replay'   → read the payload row; RETURN the stored receipt, replayed = true   (client ROLLBACK)
--      'conflict' | 'incomplete' | 'expired' | 'too_far' → RETURN rejection          (client ROLLBACK)
-- 3. Q  : SELECT paused FROM runwick.queue_controls … FOR SHARE;           no row / paused → RETURN
-- 4. C  : SELECT … FROM runwick.jobs … LIMIT 1 FOR NO KEY UPDATE OF j SKIP LOCKED;   none → RETURN NoWork
-- 5. G  : SELECT … FROM runwick.job_generations …                        (separate statement: fresh snapshot after C's lock)
--    pay: SELECT payload_utf8, headers, created_at …
-- 6. S  : SELECT … FROM runwick.concurrency_scopes … ORDER BY scope_id FOR UPDATE OF s;   -- last blocking lock
PERFORM runwick.rw_fault('AfterLocksBeforeFreshTime', 0, p_faults);   -- rw:fault
v_clock := pg_catalog.clock_timestamp();                               -- decision time, after every lock
IF v_clock >= p_not_after THEN RETURN <CommandExpired>; END IF;
v_now := runwick.ms_floor(v_clock);
-- 7. P  : live-permit count at v_now (scoped jobs);  blocked → RETURN CapacityBlocked + resume cursor
PERFORM runwick.rw_fault('BeforeFirstMutation', 0, p_faults);          -- rw:fault
-- 8. effects, each followed by its row-count check and an AfterMutation hook:
--    fences  UPDATE … RETURNING scope_id, fencing_counter  (count = number of fencing scopes)
--    attempt INSERT (attempt_id = rw_uuid_v7(v_clock))
--    job     UPDATE … RETURNING revision INTO v_rev        (exactly one row, else RAISE 'RWI01')
--    permits INSERT … ORDER BY scope_id                    (count = number of scopes)
--    event   INSERT
-- 9. R4: PERFORM runwick.rw_finalize_v1(…, jsonb result built per receipt codec v1);
RETURN <Applied row>;
```

### 3.6 How each guarantee is kept

**Lock order (04 §5).**
- The function executes the 04 §7 statements in their written order, and PL/pgSQL runs one statement at a time. Every rank is acquired in the same sequence as the 04 §5.2 matrix row for that operation:
  - Reserve: gates → receipt → Q → C → S.
  - Begin: gates → receipt → J → (K) → S → R.
  - Renew: gates → J → S.
  - Finish, Recover, Release: gates → receipt → J → (K) → S.
  - Dispatch: gates → receipt → keys → new jobs → scope provisioning.
- Sorted locking keeps its `ORDER BY … FOR …` text, and implicit FK locks keep their insert order (04 §5.3).
- **Three differences in lock footprint, all reductions:**
  1. Reserve no longer claims a candidate when Q says paused or unregistered. PF-04 rule 4 claimed it in the same batch and rolled it back.
  2. R3 runs only when R1 inserted. PF-02 ran it always; it is a plain read with no lock.
  3. Finish takes K and S once the disposition is known, without PF-04's "terminal anyway" split. The resulting lock set is the same.
- **Mixed paths.** During the dual-path window (§5.3), a function-path transaction and a statement-path transaction on the same rows take the same ranks in the same order, so no new cycle is possible. PGX-04 and ADM-03 re-run with one participant on each path.

**Fresh decision time after locks (03 §1.3, 04 §6.4).**
- `v_clock := pg_catalog.clock_timestamp()` is a separate PL/pgSQL assignment. It is evaluated only after the preceding lock statement has returned, that is, after every lock wait.
- The function is `VOLATILE`. Under READ COMMITTED, each statement in a volatile function takes a new snapshot, so G, read after J's lock, sees the generation the locked row names (PF-04 rule 2). A `STABLE` function would read every statement from the call's starting snapshot and break that rule; PGX-13 asserts `provolatile = 'v'`.
- When a plan locks more rows (Begin cancel/exhausted/rated; Finish K and S), the clock is read once, after the last of those locks. PF-04's "clock read in a batch that is followed by further locks is discarded" case cannot arise.
- Dispatch's step-10 guard reads `clock_timestamp()` again immediately before R4, as written in 04 §6.4.

**Receipt branches and replay (R1–R4, 03 §8.2).**
- `rw_receipt_v1` is 04 §6.3 in order:
  - R1 `INSERT … ON CONFLICT DO NOTHING RETURNING true`. Under READ COMMITTED, the speculative insert waits for an in-flight holder of the same key, as today.
  - R1 inserted: R3. A cross-class row gives `CommandConflict`.
  - R1 not inserted: R2 `FOR UPDATE`, a new statement and therefore a new snapshot that sees the holder's committed row. A different operation, version or fingerprint gives `CommandConflict`. A NULL result gives `InvariantFault receipt_incomplete`. Otherwise the outcome is **replay**.
  - Then the clock, `CommandExpired` and `deadline_too_far`.
- The existing result is read before the NotAfter check, so a resubmission after the deadline still replays.
- **Replay returns the stored `status`, `result` and `decided_at`, and the client rolls back.** Nothing new is written, as 03 §8.2 step 2 requires. Reserve replay also returns the payload row; when it is missing, C# builds `HistoryUnavailable payload_unavailable` as today.
- **R4 runs inside the function, last, after the effects.** Its result JSON is built in SQL to receipt codec v1. This gives codec v1 a second writer; PGX-15 (§7) proves byte-level equivalence through the C# reader.
- **The client rolls back every non-Applied result.** Rejections leave at most the R1 placeholder, which the rollback removes. A client defect that committed such a result would still be caught by the RWR01 deferred trigger, for every receipt-bearing operation.

**Fencing.**
- Reserve's fence update keeps `RETURNING scope_id, fencing_counter`. The function inserts the permits with those values and returns them in `fence_scope_ids` / `fence_values`; the receipt carries the same `fences` array.
- The PF-04 prediction (counter + 1, compared after the batch) is not needed. The returned value is used directly, and its count is checked.
- Renew, Finish, Recover and Release keep the exact handle comparison: `rw_ownership_v1`, and the `current_attempt_id`, `ownership_token`, `worker_instance_id` and `generation` predicates in every update.
- OWN-02's "a stale Finish cannot delete replacement permits" holds through the same `DELETE … AND attempt_id AND ownership_token` predicate.

**SQLSTATE mapping (04 §10).**
- Errors raised by statements inside a function reach the client with their own SQLSTATE and `constraint_name`. Every existing row of the 04 §10 table applies unchanged: 40P01, 40001 and 55P03 retry; 23505 is mapped by constraint name; 57014; RWR01 at COMMIT; 57P0x and I/O before or after COMMIT.
- **No `EXCEPTION` block** appears in a function. Such a block opens a subtransaction per entry (XID use, subtransaction cache overflow above 64), and it would also hide SQLSTATEs from the runner.
- **Business outcomes are result rows, not errors:** NoWork, QueuePaused, CapacityBlocked (with `BlockedScopeId` and `ResumeAfter`), StaleLease, InvalidState, conflicts (with `ConflictingJobId`), CommandExpired and the AdmissionClosed / UnsupportedVersion gate refusals.
- **New server-raised codes** (class `RW`, alongside the existing `RWR01`):

| SQLSTATE | Raised by | Runner mapping (same reply as today) |
|---|---|---|
| `RWI01` | A row-count check failed (04 §7.10 "exactly one row", Reserve/Begin job update, attempt update, grant count) | `InvariantFault invalid_transition`, nothing committed |
| `RWI02` | Renew or finalization found a missing or extra permit (OWN-05, P-27) | `InvariantFault permit_set_mismatch` |
| `RWI03` | R4 updated no row | `InvariantFault receipt_incomplete` (today an unclassified `InvalidOperationException`; §10 Q8) |
| `RWF01` | `rw_fault` `Throw(Transient)` / `Throw(Io)`, harness variant only; `MESSAGE` = kind | as `InjectedFaultException` of that kind (`store_unavailable`, not applied) |
| `42883` on the function call | the function does not exist (store below revision 8) | dual-path release: fall back to statements (§5.3); otherwise `schema_incompatible` (existing row) |

- **Messages carry codes only:** no keys, payload bytes or parameter values. This matches `Include Error Detail = false` and 04 §10's "server error detail and parameters are never logged".

**`synchronous_commit`.**
- COMMIT is still a client statement on a connection with the server default (`on`). A function cannot commit, and none sets `synchronous_commit`.
- Every runwick function has `proconfig IS NULL`: no `SET` clause of any kind, so no per-call GUC save and restore and no way to weaken durability. PGX-13 asserts this.
- Functions are `SECURITY INVOKER`, so the runtime role's 04 §9.3 DML grants still bound what they can do. They reference built-ins as `pg_catalog.*` and objects as `runwick.*` (P-1).

**Multi-statement row-count checks.**
- After every effect statement the function runs `GET DIAGNOSTICS v_n = ROW_COUNT` (or `RETURNING … INTO` with an explicit count) and raises `RWI01` or `RWI02` on a mismatch. The check runs immediately after the statement, as before PF-04, not after the batch.
- PF-04's caveat ("if a later statement of the same batch fails on a constraint because an earlier one affected no row, the reply is the constraint's `InvariantFault` instead") no longer arises.
- **Renew's job update matching 0 rows is not an error:** the function returns `StaleLease lease_expired` before running the permit update, so nothing has been written.
- **Retry delay draws stay "once per deciding transaction".** The client draws `p_entropy` (64 random bits) before the call, the function uses it at most once, and a replay returns the stored delay.

### 3.7 What stays in C#

- Request bound checks (03 I-6, P-22) before any SQL.
- Command fingerprints (02 §8.3).
- Retention class and deadline cap tables, passed in.
- Decoding of replies and receipts (`ReceiptCodec`).
- Transient retry with backoff (P-13), 0A000 handling (PF-03), the commit-sent flag and outcome classification.
- Lanes and pools.
- The operations not listed in §3.4: controls, rate policy, `ActivateCapabilities`, schedules, outbox, readiness and presence, `ResolveCommand`, `RegisterApplication`, `RegisterQueue`, `RegisterScope`. They keep the statement path. They are not on the per-job path; PublishPresence (5 RT, background) is a later candidate.

### 3.8 Rules for function bodies

1. `LANGUAGE plpgsql VOLATILE SECURITY INVOKER`, no `SET` clauses, no `EXCEPTION` blocks.
2. Explicit column lists into scalar variables. No `SELECT *`, `%ROWTYPE` or `RETURN QUERY` over table rows. A migration that changes a column then either is absorbed by PL/pgSQL replanning plus the assignment cast, or must ship a new function version in the same migration (§5.1).
3. PostgreSQL 16 syntax only (C-1 / P-CI on 16–18): no `uuidv7()`, no `MERGE … RETURNING`, no `JSON_TABLE`.
4. 04 statement text verbatim, with the 04 § reference in a comment above each statement.
5. Proposed reply change: `RWI03` for an R4 that matches no row. This is the only reply that differs from today, and it is put to the reviewer (Q8).

## 4. Fault model

### 4.1 Which points move

| `FaultPoint` (07 §2.3) | After PF-05, for function operations |
|---|---|
| `BeforeGateLock` | **Client, unchanged** (PF-02 note): fires before the batch carrying BEGIN; no server transaction yet. |
| `AfterLocksBeforeFreshTime` | **Server hook**, a line before `v_clock := clock_timestamp()`. This restores the original §2.3 meaning exactly: all locks held, clock not yet read. PF-04's "the server already read the clock" nuance disappears, so a `Delay` here again moves the decision time. |
| `BeforeFirstMutation` | **Server hook**, before the first effect statement after the decision. R1 and R4 are not mutation steps, as today. |
| `AfterMutation` (`Step = n`) | **Server hook** after each effect statement: per statement, the original §2.3 wording (PF-04 made it per batch). Whether to keep per-batch numbering instead is §10 Q6. |
| `BeforeCommit` | **Client, unchanged**: the function returned (R4 and all checks ran), COMMIT not yet sent. |
| `AfterCommitBeforeReply` | **Client, unchanged.** |
| `DuringReceiptLookup` | **Client, unchanged**: it fires only in `ResolveCommand`, which stays on statements. |

### 4.2 Hooks and their actions

- **Arming.** The harness injector (`HarnessFaultInjector`, I-7, I-14) stays the place where faults are armed. When an armed fault targets a server point and matches the call's command or operation, the injector serializes it into `p_faults`:

  ```json
  [{"point","step","action","kind","delayMs","barrierKey","times"}]
  ```

  The production injector (no-op) passes NULL.
- **Hook lines.** Each is one line ending in `-- rw:fault`:

  ```sql
  IF p_faults IS NOT NULL THEN PERFORM runwick.rw_fault('AfterMutation', 3, p_faults); END IF;   -- rw:fault
  ```

  `rw_fault` appends the point to the call's `faults_fired` list, which comes back in the result row or, for `Throw`, in the error message.
- **Actions:**

| Action | Server-side effect |
|---|---|
| `Throw(Deadlock40P01)` / `Throw(Serialization40001)` | `RAISE … USING ERRCODE = '40P01'` / `'40001'`: the runner's existing transient path |
| `Throw(Transient)` / `Throw(Io)` | `RAISE … ERRCODE 'RWF01'`, mapped as the injected kind. For `Io`, the harness may instead arm `Sever` on the wire proxy (07 §2.5) when a real socket loss is wanted |
| `Delay(d)` | `pg_sleep(d)` |
| `Barrier` | `pg_advisory_xact_lock_shared(barrierKey)` while the harness session holds `pg_advisory_lock(barrierKey)`. `WaitAtBarrier` polls `pg_locks` for an ungranted advisory lock on that key, as `RateSupport.WaitForLockWaiter` already does; `ReleaseBarrier` unlocks |
| `KillProcess` (peer) | The peer's backend waits at the barrier and the fixture kills the peer. The harness sets `client_connection_check_interval` on its connections, so the orphaned backend aborts during the wait instead of after release. A harness self-test covers it |
| `DropReply` | Client only (`AfterCommitBeforeReply`), unchanged |

- **Fire counts.** `FaultHandle.Fired` is updated from `faults_fired`. PGX-05's "fault not exercised" step probing therefore works unchanged: a step that does not exist is reported as not fired.

### 4.3 "Compiled out" through a migration variant

- **One file, two variants.** `0008_functions.sql` is shipped once. The migrator has an internal option `FaultHooks`, settable only through `InternalsVisibleTo` (`Runwick.Harness`). The public `RunwickPostgresMigrations.MigrateAsync` never sets it.
- **Production variant (default).** The migrator removes every line ending in `-- rw:fault`, and every block between `-- rw:fault-begin` and `-- rw:fault-end` (the `rw_fault` definition itself), before executing. Production bodies contain no hook, and `rw_fault` does not exist.
- **Fault-hooks variant.** The file runs as written. The harness builds `rw_template` (I-8) with this variant.
- **Checksum (04 §8.1).** SHA-256 over the file's LF-normalized bytes for the production variant, and over the file bytes `‖ 0x00 ‖ "rw:fault-hooks"` for the hooks variant.
  - A production binary that finds a hooks-variant ledger row reports `migration_checksum_mismatch`, an existing code.
  - `store_metadata` gains `fault_hooks boolean NOT NULL DEFAULT false`. `rw_gate_v1` refuses a hooks store unless the binary passes `p_capabilities` with the harness marker, so a production worker can never run against a test store. The code for that refusal is §10 Q7.
- **Validation (04 §8.3).** The expected function list and `md5(prosrc)` per function and per variant are derived at build time from the embedded file, the P-19 approach. Validate compares them with `pg_proc`; a difference gives `schema_validation_failed`. This also catches an operator's `CREATE OR REPLACE` on a released function. Whether `Describe/1` repeats the digest check at worker startup is §10 Q7.
- **Performance runs** (`floor`, `eng/run-perf.sh`, PF-09) must use a production-variant store. The PERF-01 round-trip test runs on either: hooks add no commands.

### 4.4 Fault cases by id (from the current test sources)

**Cases that use a point which moves into the server.** Each is re-run on the function path with its assertions unchanged unless noted.

| Case | Point / action, operation | Change |
|---|---|---|
| ADM-01, ADM-02 | `AfterLocksBeforeFreshTime` Barrier, Reserve (local and peer) | Server barrier. Assertions unchanged. |
| ADM-03 | `AfterMutation` Barrier, Reserve | With per-statement steps, step 1 is the first effect (the fence update when fenced, otherwise the attempt insert) instead of "permits + job + R4 written". Both scope rows are locked either way, so the case's deadlock-freedom assertion is unchanged. Under Q6's alternative it keeps PF-04's boundary. |
| RATE-09 | `AfterLocksBeforeFreshTime` Barrier, Begin (rated) | Server barrier after S and R. Unchanged. |
| PGX-05 | `AfterMutation` Throw at every step, plus `BeforeCommit` and `AfterCommitBeforeReply`, for Dispatch, DispatchMany, Reserve, Begin, Finish, Recover, ReleaseReservation | Server `RWF01` per step; the step count per operation rises again (one per effect statement). The probe loop is unchanged. Dispatch and DispatchMany move only in stage c. |
| BATCH-01 | `AfterMutation` Throw, DispatchMany | Moves in stage c. |
| HARNESS self-tests (barrier, throw) | `AfterLocksBeforeFreshTime` Barrier and `BeforeFirstMutation` Throw on Reserve | New server-side variants for each action, plus `KillProcess` at a server barrier and fired reporting. |
| OWN-03 (no fault point; a held row lock) | Lease expiry during a lock wait | Re-run. The fresh-expiry decision is now inside the function. |
| TX-01 … TX-04, TXC-01 … TXC-06, MODE-05 | Staged paths | Re-run on the 2-RT staged frame. TX-03's propagated 40001/40P01 is unchanged; the error now comes from inside the function. |

**Cases that keep client-side points.** No change to the mechanism; they re-run on the function path:

- `BeforeGateLock`: CAP-11, END-04, OWN-01, RUN-01, TIME-02, TIME-11, TIME-13, WRK-19.
- `BeforeCommit`: ADM-04, CMD-01, CMD-04, DEP-04, MGMT-04, PGX-01, PGX-04, RUN-02, UPG-02, WRK-04.
- `AfterCommitBeforeReply`: BATCH-02, CMD-03, END-01, RATE-07, RATE-08, RUN-01, SCHED-12, TIME-02, TIME-05, WRK-03.

**Cases on operations that stay statement-based.** Unchanged: OUTBOX-04 and the outbox CMD cases (`ReceiveOutbox`), SCH-02, SCH-03, SCHED-12, and the control and schedule CMD-03 variants.

**DEP-08 changes.** See §7.

## 5. Migrations and versioning

### 5.1 Function bodies under the checksum

- **The migration.** `0008_functions` (target revision 8) is additive only: `CREATE FUNCTION`, the `store_metadata.fault_hooks` column, and `GRANT EXECUTE … TO <runtime role>` in the grants step (04 §9.3). Its manifest lists the same operations and versions as revision 7: the protocol does not change.
- **Released functions are immutable.** A released migration body never changes under the same ID (04 §8.1), so a released function is never replaced.
  - A change to a function's behaviour, parameters or result is a new name (`reserve_v2`) in a new migration.
  - The old function stays until a later contract migration drops it, once no supported binary calls it.
  - `CREATE OR REPLACE` of a released function is forbidden and is detected by the prosrc digest (§4.3).
- **Table migrations.** A migration that changes a table a function reads either is shown harmless by the DEP-08 function variant, or ships new function versions in the same migration.

### 5.2 Interaction with PF-03 (prepared statements, `0A000`)

- **Few prepared statements.** Each function call is one auto-prepared statement per operation, so `Max Auto Prepare = 64` has room to spare.
- **The call's result type never changes.** The call's row description is the function's OUT columns, which never change under a name (§5.1). The call itself therefore cannot meet `0A000 cached plan must not change result type`; a dropped function is `42883` (§3.6).
- **Statements inside functions.** PL/pgSQL plans its statements through the plan cache, which revalidates after DDL without the fixed-result check that raises `0A000`. Rule §3.8.2 keeps the remaining shape errors out.
- **`plan_cache_mode = force_custom_plan`** (PF-03 item 1, a startup option) applies to plans inside PL/pgSQL too. The Reserve candidate query is still planned for each call's values, so the plan-stability argument of PF-03 carries over unchanged.
- **The PF-03 runner rule stays as is** (roll back, `UnprepareAll`, `Clear`, retry the same command). It still covers the statement path, the staged statements and the operations of §3.7. `MigrateAsync` keeps clearing pools.

### 5.3 Rolling upgrades with old binaries (UPG-02 / UPG-06)

**What happens today.** `PgRunner.CheckGatesAsync` requires `schema_revision == CurrentRevision`. A deployed revision-7 binary (B7) therefore refuses a revision-8 store with `schema_incompatible` on every call: it cannot run during or after `0008`. (UPG-02 / UPG-06 in G-2 cover only the capability-activation subset, 15 changelog 2026-11-01.)

**Proposed path.** The reviewer chooses between this and stop-and-migrate (§10 Q5).

1. **Release N (dual path).**
   - It accepts revisions {7, 8} (`p_revisions`, and the statement-path gate).
   - It keeps the statement path, and uses the function path when the last gate it observed on that data source reported revision ≥ 8.
   - A `42883` on the function call (the store is at 7) is not applied: the call fails before any statement in the function runs. The runner marks functions unavailable and retries the same command on the statement path.
   - A statement-path gate that reads revision 8 switches the next call to functions.
2. **Upgrade order.**
   1. Deploy N everywhere on revision 7 (statement path).
   2. Run `0008`. Its validate phase commits revision 8.
   3. Every N instance switches on its next call. During the switch, both paths run on the same rows: same lock order, same receipts, cross-path replay through the single C# decoder.
3. **Release N+1** removes the statement path for the moved operations and accepts {8}.
4. **Rollback.**
   - From N+1 to N: allowed (N accepts 8).
   - To B7 after `0008`: not allowed. That is the existing 04 §8.4 rule ("rollback to an older binary is allowed only if its manifest accepts the installed schema"), with its UPG-06 subset unchanged.
5. **UPG-02 / UPG-06 subsets.**
   - The old-binary peer passes a `p_capabilities` set without `rate_admission`, so `rw_gate_v1` refuses it on creation paths exactly as `CheckGatesAsync` does now. Both subsets run unchanged against the function path.
   - The owed PF-03 variant (an old binary with warm prepared statements) gets a function-path sibling: release N warm on revision 7, `0008` under load, no failed call, no history violation (UPG-02 PF-05 variant, §7).
6. **Optional operator switch.** `UseServerFunctions = false` would force the statement path in release N if functions misbehave in production (§10 Q5).

## 6. Alternatives and recommendation

| | Option | Per job (RT) | Renew | Cost and risk | Verdict |
|---|---|---|---|---|---|
| A | **Stay at PF-04** | 21 | 4 | None new. Leaves the decision point's RT criterion unmet; any later P-01 gain then rests on PF-06, PF-07 and PF-08. | Not recommended: the criterion was written for this case. |
| B | **Partial: Reserve and Begin only** | 5 + 2 + 2 + 5 = 14 | 4 | Pays the whole fixed cost (hooks, variant migration, dual path, helpers, differential tests) for about half the gain. Finish (settlement lane) and Renew (renewal lane, CAP-10 waves) are the lease-critical calls and stay at 5 and 4. | Not recommended as an end state. It is the right shape for the **spike**. |
| C | **PF-06 batched Reserve first** | per claimed job: (ReserveAndBegin ≈ 7–9) / N + Finish 5 + Dispatch 5 | 4 | Removes the serial-claim ceiling, which RT work alone cannot; P-01 at 100/s needs it anyway. Per-call latency is unchanged. `ReserveAndBegin/1` would be written as statements, then ported if PF-05 follows; it also needs an 03 proposal. | Do it **after** the PF-05 worker path. |
| D | **Single-trip variants** (autocommit `[set_config; fn]` as one implicit transaction, or `COMMIT` pipelined behind the call with RWR01 rejecting non-Applied commits) | 4 | 1 | Every transport failure after the call is sent becomes *outcome unknown*: an 03 §9.2 change, more resolution traffic, and `BeforeCommit` disappears. Pipelining also uses a safety trigger as control flow. | Not proposed. Revisit with measured PF-05 numbers. |
| E | **PF-05 as proposed, in stages** | 11 (stage b), then 8 (stage c) | 2 | L. The risks are listed in §7. | **Recommended**, staged as below. |

**Recommended order.** This changes step D/E of the plan's "Order" table, which is the reviewer's call (§10 Q3).

1. **PF-05a: spike (S, 2–3 days, branch, not merged).**
   - `renew_v1`, `reserve_v1`, `rw_gate_v1` and `rw_receipt_v1`, without the hooks variant.
   - Measured with `floor` interleaved against the PF-03 build on the same host and database, with load recorded.
   - Go/no-go on Reserve and Renew p50 (threshold: §10 Q2).
2. **PF-05b: worker path (M–L).**
   - Infrastructure: variant migration and hooks, dual path, helpers, PGX-13 … PGX-16.
   - Functions: Reserve, Begin (unrated and rated, including the deferral), Renew, Finish and `StageFinishAsync`, Recover, ReleaseReservation.
   - One operation per commit, as PF-04 did. Per job 11 RT, Renew 2.
3. **PF-06 batched Reserve / `ReserveAndBegin/1`**, written once as `reserve_and_begin_v1` from the PF-05b helpers. It reuses the rate step as SQL, so `ChargeRatesAsync` is not copied a second time.
4. **PF-05c: Dispatch, DispatchMany and `StageAsync`** (the in-memory key resolution of 04 §7.2 cases a–f, ported to PL/pgSQL). Per job 8 RT.
5. **Independent of this order.** PF-06 batched Renew, PF-07 and PF-08 (step C) stay where they are. If batched Renew lands after PF-05b, it is written as `renew_many_v1`.

## 7. Risks and test plan

### 7.1 Risks

| Risk | Mitigation |
|---|---|
| **Gain smaller than the RT arithmetic suggests**: server and client work remain (§1.3) | PF-05a spike with a go/no-go threshold before PF-05b |
| **Two implementations** of the decision logic during the dual-path release, plus SQL copies of the retry evaluator and the receipt codec v1 writer (C# keeps them for SQLite, Redis and the statement path) | PGX-14 differential test; PGX-15 codec equivalence; PGX-16 evaluator vectors |
| **Visibility**: per-statement `PhaseTimings` no longer exist | One `function` phase per call; `track_functions = pl` and `auto_explain` on perf runs only |
| **Subtransactions and GUC overhead** | §3.8: no `EXCEPTION`, no `SET` clauses (PGX-13) |
| **Drift**: functions are database objects an operator can edit | prosrc digests at validate (and optionally at `Describe`, Q7) |
| **Hooks variant reaching production** | Variant checksum, the `fault_hooks` gate refusal, PGX-13 on a production-variant store |
| **PostgreSQL 16 compatibility** | §3.8.3; certification on 16, 17 and 18 (04 §11) |
| **Mixed-path deadlocks** during the switch | Same statement order by construction; PGX-04 and ADM-03 with one participant on each path |
| **Reviewability**: the logic moves from C# to PL/pgSQL | Verbatim 04 statement text with § comments; one operation per commit |

### 7.2 New and changed cases

| Case | Kind | Asserts |
|---|---|---|
| PERF-01 | changed | Function operations are 1 command each. RT: Reserve 2, Begin 2 (rated 2), Renew 2, Finish 2, Recover 2, ReleaseReservation 2, `StageFinishAsync` 2 (with RELEASE). After stage c: Dispatch 2, `StageAsync` 2. **Per job: 4 commands / 8 RT; 11 RT after stage b.** Unchanged operations keep their PF-04 counts. |
| PGX-13 (new) | catalogue | Every `runwick` function is `VOLATILE` (except `rw_retry_delay_v1`), `SECURITY INVOKER`, `proconfig IS NULL`, and has a prosrc digest equal to the build's for the store's variant. A production-variant store has no `rw_fault` and no hook line. A production binary refuses a hooks store. |
| PGX-14 (new) | differential | For each moved operation and each reply branch, the same pre-state through the statement path and the function path (release N) gives the same reply and the same snapshot diff, with generated IDs, tokens and instants normalized. Branches: Applied, replay, conflict, cross-class, incomplete, expired, `deadline_too_far`, NoWork, QueuePaused, CapacityBlocked, the StaleLease codes, InvalidState, BudgetExhausted, RateLimited deferral, Begin cancel/exhausted, and the `RWI01` / `RWI02` faults. |
| PGX-15 (new) | codec | Every receipt the functions write decodes with `ReceiptCodec` and re-encodes equal (canonical JSON). Statement-written receipts replay through the function path, and function-written receipts through the statement path. |
| PGX-16 (new) | vectors | `rw_retry_delay_v1` equals `RetryEvaluator.Delay` on the 02 vectors and at the saturation, `n = 1` and `entropy = 0 / 2^64 − 1` boundaries. `rw_uuid_v7` has the layout and ordering of `Guid.CreateVersion7` (PGX-09 ordering). |
| HARNESS self-tests | extended | Each action at each server point; `faults_fired`; `KillProcess` at a server barrier leaves nothing committed. |
| DEP-08 | changed | Run on both paths. On the function path, "at least one `0A000` retried" becomes "no call fails and any `0A000` is retried"; the call statement is not expected to see one. Add a column change under a column a function reads. |
| UPG-02 (PF-05 variant) | new | Release N warm on revision 7, `0008` under load from three loops, the switch to functions: no failed call, at least one statement→function switch, ReferenceModel accepts. A revision-7-only peer is refused `schema_incompatible` after validate. |
| Re-run on the function path | — | Every case in §4.4; the CAP-10 calibration branch is recorded as before; PGX-04 and ADM-03 mixed-path. |

### 7.3 PERF-01 and the plan targets

PERF-01 is checked in every suite, as now. The latency targets are the plan's "After PF-05" column and are checked only on the reference environment (PQ1). On this host they are reported, not gated.

| Measure | Target |
|---|---|
| Round trips per job | ≤ 12 (8 proposed) |
| Round trips per Renew | ≤ 3 (2 proposed) |
| Reserve p50 | ≤ 15 ms |
| Worker CPU per job | ≤ 25 ms |
| `C_claim` | ≥ 40/s |

### 7.4 ReferenceModel and history checks

- **The history is unchanged:** command ID, operation, fingerprint, request, reply (`Status`, `Effect`, `Replayed`, `DecidedAt`), send and receive bounds, decision instant, ownership token, injected faults, and before/after snapshots (07 §2.2). The C# driver still builds the same reply records, from the function row.
- **Server-side injected faults** are recorded from `faults_fired`.
- **The model stays independent.** It never called the production SQL builder, and it does not call the functions either. Moving the SQL server-side does not couple the model to it.
- **Mixed-path workloads** (release N during `0008`) produce one history that the model must accept. A Contract/Fault test still fails when the model rejects its history.
- **G-1 and G-2 workloads** re-run under PF-09 on the function path. Their history checks apply unchanged.

## 8. Effort

| Stage | Size | Notes |
|---|---|---|
| PF-05a | S | |
| PF-05b | M–L | About 7 functions plus helpers; harness hooks; dual path; 4 new cases plus re-runs |
| PF-05c | M | Dispatch item resolution, the largest single body |

## 9. Spec paragraphs this would change (quoted; proposed text, not applied)

**04 §6.1**, after *"Statements whose inputs do not depend on prior results may be sent together in one `NpgsqlBatch`; a decision that depends on a lock result is never batched ahead of that lock."*

> Add: "An operation listed in §7.17 runs as one batch of `BEGIN`, the `set_config` statement above and one call of its function, followed by `COMMIT` when the function returns effect `Applied` and `ROLLBACK` otherwise. The function runs §6.2–§6.4 and the operation's §7 statements in their written order, one statement at a time; it is `VOLATILE`, so each statement under READ COMMITTED reads a snapshot taken after the locks before it. Staging sends `SAVEPOINT runwick_stage` and the call as one batch, then `RELEASE` or `ROLLBACK TO` + `RELEASE`."

**04 §6.3**, *"Driver logic: R1 inserted → R3 (a row → `CommandConflict`, roll back). R1 not inserted → R2 …"*

> Change "Driver logic:" to "Driver logic (inside `runwick.rw_receipt_v1` for §7.17 operations):". The rest is unchanged, except that R3 runs only when R1 inserted.

**04 "Changes (2026-11-01, PF-04)" rule 3**, *"Values a `RETURNING` clause used to supply are predicted from locked rows … and compared with the returned values; every row count is checked."*

> Add: "Not used by §7.17 operations: their functions read `RETURNING` directly and check each row count immediately after its statement (`RWI01`/`RWI02`)."

**04 §7**: add **§7.17 Server functions** listing §3.4's signatures, §3.8's rules and the frame/result parameters of §3.2.

**04 §8.1**, *"Checksum = SHA-256 of the file's UTF-8 bytes after normalizing line endings to LF … a released migration body is never changed under the same ID."*

> Add: "For a migration with fault-hook lines (`-- rw:fault`), the production variant strips them before execution and is checksummed as above; the harness variant is checksummed over the file bytes followed by `0x00` and `rw:fault-hooks`. A released function is never replaced; a changed function is a new versioned name."

**04 §8.3**: add the prosrc-digest query and "any difference → `schema_validation_failed`".

**04 §9.3**: add "functions `runwick.*_v1` → `EXECUTE`" for the runtime role.

**04 §10**: add the rows of §3.6's table (`RWI01`, `RWI02`, `RWI03`, `RWF01`; `42883` on a function call during the dual-path release).

**07 §2.3**, *"Fault points are invoked by the PostgreSQL transaction runner for every operation …"*

> Change to: "Fault points are invoked by the PostgreSQL transaction runner (`BeforeGateLock`, `BeforeCommit`, `AfterCommitBeforeReply`, `DuringReceiptLookup`) and, for §7.17 operations, by `runwick.rw_fault` hook lines inside the server functions (`AfterLocksBeforeFreshTime`, `BeforeFirstMutation`, `AfterMutation`), which exist only in stores migrated with the fault-hooks variant." Add the action table of §4.2.

**07 "Changes (2026-11-01, PF-04)"**, the `AfterLocksBeforeFreshTime` and `AfterMutation` rows.

> Add: "For §7.17 operations: before the clock read (not after), and after each mutation statement (not each batch)." The second part depends on Q6.

**07 I-7**, *"Fault points are an internal hook (`IFaultInjector`, no-op by default) compiled into every build and reachable only through `InternalsVisibleTo` …"*

> Add: "Server-side hooks are present only in a fault-hooks store; production migrations never create them."

**No 03 paragraph changes.** If Q8 is answered "keep today's behaviour", `RWI03` maps to the current handling instead.

## 10. Open questions for the reviewer (adzamkomla)

| # | Question |
|---|---|
| Q1 | Accept the 2-RT frame (COMMIT stays separate, 03 §9.2 untouched) and leave the single-trip variants (§6 D) for after PF-05 is measured? |
| Q2 | Spike first (PF-05a)? If so, what go/no-go threshold, and on which host? A suggestion to react to: Reserve p50 at least 30 % below the PF-03 build in interleaved runs on this host, confirmed on the reference environment once PQ1 is decided. |
| Q3 | Order: PF-05 worker path **before** PF-06 batched Reserve (`ReserveAndBegin/1` written once, as a function), changing step D/E of the plan? |
| Q4 | Scope: include Dispatch, DispatchMany and `StageAsync` (stage c), or leave the producer path on statements (11 RT per job)? Controls, schedules, outbox and presence stay on statements: agreed? |
| Q5 | Rolling upgrade: a dual-path release N (accepts revisions 7 and 8, switches per data source) or stop-and-migrate? Keep a `UseServerFunctions` switch for one release? |
| Q6 | `AfterMutation` inside functions: per statement (the original 07 §2.3 wording; changes ADM-03's step-1 boundary) or keep PF-04's per-batch numbering? |
| Q7 | Refusal of a hooks-variant store by a production binary: reuse `schema_incompatible` (detail `fault_hooks`) or a new code? Should `Describe/1` also verify function digests at startup, not only at migrate/validate? |
| Q8 | An R4 that matches no row: map it to `InvariantFault receipt_incomplete` (`RWI03`), or keep today's unclassified exception? This is the only reply that would change. |
| Q9 | Build attempt IDs in SQL (`rw_uuid_v7` from the decision time, PostgreSQL 16-compatible), or let the client build them before the call from its own clock? |
| Q10 | PQ1: is a reference environment available for the spike's go/no-go and for the "After PF-05" latency targets, or are they reported non-reference until SP-029? |

## 11. Decisions (2026-11-02)

**Answered by adzamkomla:**

| # | Decision |
|---|---|
| Q2 | No spike. Convert every operation that needs it, in one go. |
| Q3 | Yes: the PF-05 worker path comes before PF-06 batched Reserve, and `ReserveAndBegin/1` is written once, as a function. |
| Q4 | Full scope ("convert everything that needs to be converted"). Besides the worker path, this includes Dispatch, DispatchMany, `StageAsync`, and the frequent background operations: schedule materialize/claim, outbox receive/settle and presence. Rare admin operations (controls, rate policy) stay on statements. |
| Q5 | Stop and migrate. A binary keeps refusing any schema revision but its own; there is no dual path and no `UseServerFunctions` switch. |
| Q10 | No reference environment for now. Report numbers as non-reference; a reference measurement may come once all the work is done. |
| Plan Q5 | Back off the idle poll interval now that wake notifications exist (06 defaults and OBS-01's bound change with a dated entry). |

**Not answered; the implementer takes these defaults, and any of them can be reversed:**

| # | Default |
|---|---|
| Q1 | The 2-round-trip frame. COMMIT stays separate, and the single-trip variants are left for later. |
| Q6 | Keep PF-04's per-batch `AfterMutation` numbering, so ADM-03's boundary does not move. |
| Q7 | Reuse `schema_incompatible`, with detail `fault_hooks`. `Describe/1` checks the function digests at startup as well. |
| Q8 | Keep today's behaviour, so no reply changes. |
| Q9 | Build attempt IDs in SQL (`rw_uuid_v7` from the decision time). |
