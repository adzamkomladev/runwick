# 13 — Schedules, materialization, misfires and manual triggers (SP-016)

**Sources:** [scheduling-internals.md](../architecture/scheduling-internals.md) §2–§10 (primary); [scheduler-evaluator-and-defaults.md](../architecture/scheduler-evaluator-and-defaults.md) §3, §5; [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md) §4–§6; [storage-command-protocol.md](../architecture/storage-command-protocol.md); [management-retention-defaults.md](../architecture/management-retention-defaults.md) (command lifetimes, schedule retention); [public-api-contract.md](../architecture/public-api-contract.md) §9, §18; 12 (evaluator); 08 (actor, audit, events, receipts); 10 (rate scopes, capability activation pattern); 03 §3, §8.

## Changes (2026-11-09, P-06 threshold)

- P-06's lag rule is now "p99 ≤ schedules ÷ `JobsPerSecond` + 3 s" (13 s for 1,000 schedules at the default 100/s), by owner decision. All schedules fall due in the same second, and the scheduler's default allowance of 100 jobs per second makes the last one wait schedules ÷ 100 s by design. Defaults are unchanged. See `docs/reports/sp-029.md`.

## Changes (2026-11-02, idle caps)

- **Cap.** While the schedule listener is healthy (from a successful (re)subscription until the session is lost or refused, or the listener stops), the idle back-off doubles to **3 × `IdlePollInterval`** (15 s by default) instead of `IdlePollInterval` (5 s). Otherwise it is 5 s, as before. A lost session ends the current idle wait at once (one pass, then the back-off from `PollInterval`).
- **Known due instants.** Occurrences become due by time, and nothing notifies that. After every applied materialization the scheduler records the schedule's new `scan_after_at`, mapped to local time through the plan's cutoff, and no idle wait runs past the earliest recorded instant. A schedule this process has materialized is therefore read when it falls due, whatever the cap. A schedule it has not seen since start (or one another replica materializes) is found within the cap: ≤ 16.5 s instead of ≤ 5.5 s. Edits still notify.
- **Why 15 s.** The idle scheduler already costs 0.23 statements/s (pf-05-08 report §5); 15 s cuts the due reads by a further third. With the due-instant bound, the longer cap costs lateness only for schedules this process has not yet materialized.

## Changes (2026-11-02, PF-07)

Optional wake channel. RegisterSchedule (when it creates the row), UpdateSchedule and SetScheduleState notify `runwick_schedule` with `application/schedule_key`, inside their existing statements and only on commit (04 Changes 2026-11-02, PF-07). A scheduler whose store offers notifications listens on one dedicated session; a notification ends the current idle wait and resets the idle back-off to `PollInterval`. Polling is unchanged and stays the correctness path: a dropped notification only delays the next pass to the normal back-off.

## Changes (2026-10-31, SP-016)

New document. It replaces 01 §18 "AddScheduler/AddSchedule reserved" and 03 §8.4 "scheduler commands not in M2".

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution |
|---|---|---|
| C-1 | management-retention gives scheduler commands 30 s NotAfter even when an administrator issues them; 08 gives administrative operations 5 min / 10 min | Schedule operations are their own lifetime family (§9): 30 s default, 60 s cap, `durable` class. They are **not** administrative in the 08 sense, but the operator-issued ones still write `management_audit`. `RegisterSchedule` follows the other `Register*` operations (5 min / 10 min) because it is a startup seed |
| C-2 | scheduling-internals §9 lists nine operation names, never Delete, but §3 describes tombstones | Eight operations (§6). Pause and Resume are one operation, `SetScheduleState/1`, mirroring `SetQueueState/1`. Delete/tombstone is deferred to SP-020/SP-021; the `enabled` column and tombstone semantics stay in the schema |
| C-3 | scheduler-evaluator §5 allows a 4 MiB materialization plan; public-api-contract says DispatchMany's 2 MiB is "the same ceiling as a materialization plan" | 32 jobs and 2 MiB of payload per materialization transaction. One ceiling for both paths. 4 MiB stays an upper bound nobody uses |
| C-4 | A per-schedule lease is suggested by the task wording; scheduling-internals §4 says no lease is needed | No scheduler lease. Competing replicas may compute the same plan; the locked row's revisions and the receipt pick one winner |
| C-5 | `schedule/<id>` as a `resource` key could collide with a user's per-resource key | Dedicated scope kind `schedule`, key = schedule ID text, capacity 1 (§7.2). The concurrency scope kind check widens |
| C-6 | `management_audit` admits only administrative operations; MaterializeOccurrences is a system command | Operator schedule operations write audit with target `schedule` / key. MaterializeOccurrences and RegisterSchedule write events and `schedule_audit_ranges`, never `management_audit`. RecordScheduleEvaluationFailure writes audit with the system actor |
| C-7 | Trigger while `needs_review` is unspecified | Allowed, like pause. Rejected for a disabled schedule (`schedule_disabled`) |
| C-8 | 12 §5.1 leaves the fingerprint's `settings` member to this document | §3.3 |
| C-9 | Wire names differ between the public-api sample (`five-field`, `run-once`) and 12 | Public API takes enums. Persisted names: misfire `skip`/`run_once`/`catch_up`, resume `skip_paused`/`run_once`/`catch_up`, overlap `allow`/`serialize`, gap and fold as 12 |
| C-10 | Dispatch hard-codes `created_reason = 'initial'` | DispatchCore takes the reason; scheduled jobs are `scheduled` (already allowed by `job_generations_reason_ck`) |
| C-11 | 08 §3.1's lock list omits schedule rows | Schedule rows rank after the receipt and before queue control (§8) |

Implementation choices *[choice]*:

1. **The caller plans, the store checks.** The store never runs the evaluator. Every cursor-moving request carries what the caller computed from the stored definition (occurrences, next due instant, cutoff), together with the expected revisions it planned against. The store checks revisions, the high-water mark, bounds and the cutoff against fresh database time, then applies.
2. **The store builds the jobs** from the definition's frozen template (§3.2). The request carries only instants, coverage and proposed job IDs. A plan therefore cannot change the payload, profile tuple, queue or policy (PGX-02 schedule half by construction).
3. Window uniqueness on scheduled jobs is refused in M3 (`schedule_window_unsupported`). An active key requires `AllowOccurrenceReuse()`; without it registration fails with `schedule_occurrence_reuse_required`.
4. Resume stores a pending policy (§5.4) instead of evaluating in the store; the scheduler applies it at the next materialization. An edit needs no pending policy: the caller sends the new first due instant, and the store records the skipped interval directly (§6.4).
5. Capability `scheduler` (version 1) gates RegisterSchedule, MaterializeOccurrences and TriggerSchedule (`UnsupportedVersion` / `scheduler_not_active`) until `ActivateCapabilities/1` raises it (10 §6.1). Control operations on existing schedules are not gated.

## Changes (2026-11-08, SP-028)

- §1 item 5: who raises `scheduler`. The operator runs `runwick store activate-capability --name scheduler --application NAME` (PostgreSQL, SQLite) or `RunwickRedisMigrations.ActivateCapabilityAsync` once per application (23 §3.6). The scheduler's startup error for `scheduler_not_active` names this command.

## Changes (2026-11-01, SP-016 owed cases)

- `IJobContext.Schedule` (01 §9) is populated for jobs the store builds from a schedule; it is null for every other job. Inferred details:
  - The store writes the occurrence into the job's payload row as a third `headers` member, `schedule` (key, scheduled-at, covered-from, covered-until, epoch ms), beside the business and diagnostic maps. Reserve reads it with the payload. There is no migration and no extra join, and the IC-07 diagnostic-metadata rule is unchanged.
  - A scheduled occurrence reports its scheduled instant and covered range. A run-once covers from the first missed instant through the cutoff; otherwise the range is the instant itself.
  - A manual trigger reports the trigger command's decision time (store time after its locks) as the scheduled instant and both ends of the range.
  - A `ResolveCatchUp` run-once reports the open episode's first due instant through its cutoff.
  - 00's scheduler row no longer says the member is always null.
- §11 lists the tests added for the owed items: SCHED-11, the scheduler half of SCHED-14, PGX-04 materialization, CMD-04 and CMD-05 as separate tests, the hosted path, pure planner tests and the remaining scheduling-internals §10 rows.

## 1. Contract

A schedule is a durable, keyed definition that produces jobs at evaluated instants. Each occurrence produces at most one job or one recorded disposition. The cursor moves forward only and only over occurrences that were emitted, coalesced or explicitly skipped. The scheduler is a role: any number of replicas may run it, none holds a lease, and materialization is a store command whose effect is atomic with the jobs it creates.

## 2. Public API (preview in M3)

```csharp
public static class RunwickSchedulerExtensions
{
    public static RunwickBuilder AddScheduler(this RunwickBuilder builder, Action<SchedulerOptions>? configure = null);
    public static ScheduleBuilder<TJob> AddSchedule<TJob>(this RunwickBuilder builder, string key, TJob job) where TJob : class, IQueueJob;
}

public sealed class ScheduleBuilder<TJob>
{
    ScheduleBuilder<TJob> Cron(string expression, CronFormat format = CronFormat.FiveField, string? timeZone = null);
    ScheduleBuilder<TJob> Once(DateTimeOffset dueAt);
    ScheduleBuilder<TJob> OnGap(ScheduleGap gap);            // Skip (default) | Shift
    ScheduleBuilder<TJob> OnFold(ScheduleFold fold);         // Earlier (default) | Later | Both
    ScheduleBuilder<TJob> OnMisfire(MisfirePolicy policy, TimeSpan? grace = null);   // RunOnce (default) | Skip | CatchUp
    ScheduleBuilder<TJob> CatchUpLimits(int maxJobsPerEpisode = 1_000, TimeSpan? maxAge = null);
    ScheduleBuilder<TJob> Overlap(ScheduleOverlap overlap);  // Allow (default) | Serialize
    ScheduleBuilder<TJob> AllowOccurrenceReuse();
    ScheduleBuilder<TJob> WithOptions(DispatchOptions options);   // queue, priority, policy, uniqueness of the template
}

public interface IScheduleControl   // Runwick.Management, preview, actor as 08 §10
{
    Task<ScheduleControlResult> PauseAsync(string key, long expectedControlRevision, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<ScheduleControlResult> ResumeAsync(string key, long expectedControlRevision, ResumePolicy policy, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<ScheduleControlResult> TriggerAsync(string key, long expectedDefinitionRevision, Guid triggerId, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<ScheduleControlResult> RevalidateAsync(string key, long expectedDefinitionRevision, ManagementActor actor, string? reason = null, CancellationToken ct = default);
}
```

Registration validation (startup, `ConfigurationException`): key grammar as resource keys (`invalid_key`), exactly one of `Cron`/`Once` (`schedule_definition_missing`), the job registered, the evaluator's own validation (12 §2, §6), grace 0–24 h, catch-up limits (§10), window uniqueness refused, active key needs reuse. `AddSchedule` without `AddScheduler` in the same host is allowed (another process may run the role); the registration seed then happens on the first scheduler start that declares the schedule.

`UpdateSchedule`, `ResolveCatchUp` and the preview query are store-level in M3 (`IScheduleStore`); their public wrappers are SP-020.

## 3. Records (migration `0006_scheduler`)

### 3.1 Tables

| Table | Key | Content |
|---|---|---|
| `schedules` | `(application, schedule_id)`, unique `(application, schedule_key)` | `definition_revision`, `control_revision`, `cursor_revision`; `enabled`, `paused`, `needs_review`, `review_reason`; `next_due_at`, `scan_after_at`, `evaluated_through`; episode `episode_id`, `episode_cutoff`, `episode_emitted`, `episode_limit`; pending `resume_policy`, `resume_cutoff`; `created_at`, `updated_at`, `updated_by` |
| `schedule_definitions` | `(application, schedule_id, revision)`, immutable | `kind` (`cron`/`one_time`), `due_at`, `cron_text`, `cron_dialect`, `zone_id`, `evaluator_profile`, `tzdb_version`, `tzdb_sha256` (32 bytes iff cron), `gap_policy`, `fold_policy`, `misfire_policy`, `misfire_grace_ms`, `max_batch`, `max_episode_jobs`, `max_catchup_age_ms`, `overlap_policy`, `allow_reuse`, template (§3.2), `definition_fingerprint`, `cutoff_at` |
| `schedule_occurrences` | `(application, schedule_id, occurrence_kind, occurrence_key)` | `scheduled_at`, `definition_revision`, `job_id` (logical, no FK), `disposition`, `coalesced`, `covered_from`, `covered_through`, `materialized_at`, `command_id` |
| `schedule_audit_ranges` | `(application, audit_id)` | `schedule_id`, `definition_revision`, `command_id`, `from_exclusive`, `through_inclusive`, `reason` (`misfire_skip`, `run_once_covered`, `skipped_paused`, `skipped_by_edit`, `catch_up_skipped`), `created_at` |

Indexes: due `(application, scan_after_at, schedule_id) WHERE enabled AND NOT paused AND NOT needs_review AND next_due_at IS NOT NULL` (the due query repeats the predicate literally); history `(application, schedule_id, scheduled_at, occurrence_kind, occurrence_key)`.

Occurrence keys: `scheduled` → the UTC instant as epoch milliseconds in decimal; `manual` → the trigger ID (UUID text). Dispositions: `created`, `linked_existing` (an active key bound an existing job), `skipped` (never stored per occurrence; ranges carry skips).

### 3.2 Template

Frozen at registration or edit from the declared job through the dispatch preparer: job name, payload version, serializer profile tuple, payload bytes (≤ the dispatch payload bound), queue, priority, execution policy snapshot, concurrency scope requirements, rate requirements (10 §3.1) and an optional active key. Stored as columns, a `template` jsonb and a `payload_template` bytea. A `serialize` overlap adds `ScopeRequirement("schedule", scheduleId, 1)` when the store builds each job; the template itself does not contain it.

### 3.3 Fingerprint settings (12 §5.1)

```text
settings = { allowReuse, bounds: {maxBatch, maxCatchupAgeMs, maxEpisodeJobs},
             job: {name, payloadVersion, profileId, profileVersion, templateSha256},
             dispatch: {activeKey, priority, queue, policy (02 §7.2 policy value)},
             misfire: {graceMs, policy}, overlap }
```

Members sorted by the canonical encoder. `templateSha256` is lowercase hex SHA-256 of the payload bytes. Scope and rate seeds are not fingerprinted (they never change an existing scope).

## 4. Materialization

### 4.1 Planning (scheduler process, no locks)

1. Read due schedules (§6.9): `scan_after_at ≤ now`, the due-index predicate, ordered by `(scan_after_at, schedule_id)`, at most 32, with the current definition, and database time T.
2. Skip, with a local diagnostic only, any definition whose evaluator profile or TZDB artifact this process lacks (12 §6.3; SCHED-11). No global `needs_review`.
3. Compute the plan through the frozen cutoff T (§5), under the evaluator slot and the 250 ms budget.
4. Send one `MaterializeOccurrences/1` with a fresh command ID, NotAfter 30 s, and the plan. An unknown outcome keeps the same command and plan and resolves through `ResolveCommand` until the 30 s resolution budget ends. Local replanning of that schedule is blocked meanwhile; a new command is never minted for the same plan.

After a full page, carry its last `(scan_after_at, schedule_id)` into the next read as an exclusive position. A full page that applies at least one change continues promptly under the same local allowance. A full page with no applied changes backs off and resumes after its last row, so local incompatibilities or revision conflicts cannot hold the scan at the head of the index. The scan horizon is the store time of the first page. Each page also checks current due eligibility and uses its own store time as the planning cutoff. A short page ends the scan and the next poll starts at the head. The cursor is local and ephemeral. Rows that move behind it can be read again on the next scan. Store failures back off before retrying the current page position.

### 4.2 Store effect

Lock order: gates → receipt → schedule row → (Dispatch core:) queue control read → dispatch keys → new jobs → concurrency scopes → rate scopes → occurrences, cursor, audit ranges, events, receipt.

Checks, in this order, all rejections writing nothing:

| Check | Result |
|---|---|
| Capability `scheduler` inactive | `UnsupportedVersion` / `scheduler_not_active` |
| No such schedule | `NotFound` / `schedule_not_found` |
| `enabled = false` | `InvalidState` / `schedule_disabled` |
| Paused / needs review | `InvalidState` / `schedule_paused` / `schedule_needs_review` |
| Any of definition, control or cursor revision differs; episode ID differs | `VersionConflict` / `revision_conflict` (`actual=d/c/u`) |
| More than 32 jobs or 2 MiB of payload | `InvalidRequest` / `plan_too_large` |
| A scheduled instant ≤ `evaluated_through` (high-water mark) | `InvariantFault` / `occurrence_below_high_water` |
| Fresh time after the last lock < cutoff (backward clock) | `InvalidState` / `cutoff_in_future`; the scheduler replans |
| NotAfter passed | `CommandExpired` |
| Dispatch-core rejection (queue not registered, rate admission not active, uniqueness conflict) | That status; the scheduler records an evaluation failure only for deterministic codes (§6.7) |
| An occurrence row already exists | `InvariantFault` / `occurrence_exists` |

Effect: the jobs (created reason `scheduled`, eligible now), one occurrence row per planned occurrence with the resulting job and disposition, the new cursor (`evaluated_through`, `next_due_at`, `scan_after_at`, episode fields, optional `needs_review` with reason), the cleared pending resume when the plan consumed it, the audit ranges, `cursor_revision + 1`, the event `schedule_materialized` (counts, cutoff, episode), the jobs' `dispatched` events, and the receipt `{scheduleId, cursorRevision, occurrences: [{kind, key, jobId, disposition}]}`.

A replay returns the stored receipt. An occurrence whose job was later deleted by retention still returns its stored mapping; after receipt retention a replay is `CommandExpired` and never a replacement job.

## 5. Plans

Inputs: the schedule state, its definition, T. All instants are whole milliseconds. `next(x)` is the first occurrence strictly after x (12 §7); for `one_time` it is `due_at` if `due_at > x`, otherwise none.

### 5.1 Normal

`next_due_at > T` → nothing to do; `scan_after_at := next_due_at`. Otherwise, if `next_due_at > T − grace` (not misfired), emit every occurrence in `[next_due_at, T]` up to 32. If more remain, the batch is partial: `evaluated_through := last emitted`, `next_due_at := first unemitted`, `scan_after_at := T`. Otherwise `evaluated_through := T`, `next_due_at := next(T)`, `scan_after_at := next_due_at` (or none, which leaves the schedule out of the due index).

### 5.2 Misfire (`next_due_at ≤ T − grace`, equality counts; default grace 60 s)

| Policy | Jobs | Cursor |
|---|---|---|
| `skip` | 0; one audit range `(evaluated_through, T]` `misfire_skip` | `evaluated_through := T`, `next := next(T)` |
| `run_once` (default) | 1, keyed by `next_due_at`, `covered_from = next_due_at`, `covered_through = T`; audit range `run_once_covered` | as skip |
| `catch_up` | Opens an episode: `episode_id` new, `episode_cutoff := T`, emitted 0, limit `max_episode_jobs`. If the oldest missed occurrence is older than `T − max_catchup_age`, no jobs: `needs_review` (`catch_up_too_old`), cursor preserved | §5.3 |

Count and age limits apply only to `catch_up`.

### 5.3 Catch-up episodes

Each batch emits up to 32 occurrences chronologically from `next_due_at` through `episode_cutoff`. `evaluated_through := last emitted`, `episode_emitted += n`. If occurrences through the cutoff remain: when `episode_emitted = episode_limit`, set `needs_review` (`catch_up_limit`) and keep the cursor; otherwise `scan_after_at := T + 250 ms` (pacing) and yield. When none remain, the episode closes (`episode_* := null`), `evaluated_through := episode_cutoff`, `next_due_at := next(episode_cutoff)`. Exactly `episode_limit` jobs with nothing left completes the episode. The cutoff never moves during an episode.

### 5.4 Pending policies

Resume (§6.3) stores `resume_policy` and `resume_cutoff` instead of evaluating. At the next plan, if a pending policy exists and `next_due_at ≤ resume_cutoff`, the occurrences through `resume_cutoff` are handled as a misfire with that policy and cutoff (`skip_paused` → audit `skipped_paused`); later occurrences then follow §5.1–§5.2. The plan clears the pending policy.

Edits (§6.4) do not use this mechanism: UpdateSchedule moves `evaluated_through` to its cutoff, writes the `skipped_by_edit` audit range itself and sets `next_due_at` to the caller's first due instant. The planner still accepts a stored `skipped_by_edit` policy and treats it like `skip_paused`, so a future store-side edit path can reuse it.

### 5.5 One-time schedules

The occurrence is `due_at`. Late beyond grace, it follows its misfire policy (`catch_up` behaves as `run_once`). After its disposition `next_due_at := null`; the schedule is exhausted.

## 6. Operations

All take a command ID and NotAfter, write a receipt, and follow 03 §8 replay rules. Operator operations take a `ControlActor` (08 §2) and write `management_audit` (target `schedule`, ID = key) and an event in the effect's transaction; rejections write neither.

| § | Operation | Body | Guard | Effect |
|---|---|---|---|---|
| 6.1 | `RegisterSchedule/1` | key, definition, cutoff T (sampled from the store), first due instant after T | capability | Absent → insert schedule and definition revision 1, event `schedule_registered`, result `created`. Same fingerprint → `same`, nothing written. Different → `drift` with the stored fingerprint, nothing written (never an overwrite; startup never resets pause or enabled) |
| 6.2 | `MaterializeOccurrences/1` | §4 | §4.2 | §4.2 |
| 6.3 | `SetScheduleState/1` | key, paused, resume policy, expected control revision, actor | control revision | Pause: `paused := true`, cancels nothing. Resume: rejected while an episode is open (`episode_unresolved`); `paused := false`, pending policy with `resume_cutoff := now`. Same state → `no_change`. `control_revision + 1`, event `schedule_paused`/`schedule_resumed` |
| 6.4 | `UpdateSchedule/1` | key, definition, expected definition revision, cutoff T, first due after T, actor | definition revision; one-time due instant in the future | Same fingerprint → `no_change` (cursor untouched). Otherwise new definition revision, `evaluated_through := max(evaluated_through, T)` with audit range `skipped_by_edit` over the old unmaterialized interval, `next_due_at := first due`, `needs_review := false` unless an episode is open (rejected: `episode_unresolved`). Materialized jobs keep their payload. Event `schedule_updated` |
| 6.5 | `TriggerSchedule/1` | key, expected definition revision, trigger ID, proposed job ID, actor | capability; definition revision; enabled | One `manual` occurrence keyed by the trigger ID with the template job; allowed while paused or under review; never moves the cursor; honors overlap. An existing trigger ID returns its stored occurrence (`replayedTrigger`). Event `schedule_triggered` |
| 6.6 | `ResolveCatchUp/1` | key, episode ID, resolution (`skip`, `extend` + jobs, `run_once`), next due after the cutoff, proposed job ID, actor | episode ID | `skip`: audit range to the cutoff (`catch_up_skipped`), episode closed, next due set. `extend`: limit raised (≤ 10,000 total), review cleared. `run_once`: one job keyed by `next_due_at` covering to the cutoff, episode closed. Event `catch_up_resolved` |
| 6.7 | `RecordScheduleEvaluationFailure/1` | schedule ID, expected definition revision, code, detail, system actor | definition revision | `needs_review := true`, `review_reason := code`; cursor untouched. Only deterministic evaluator errors (12 §10: invalid definition, range overflow, horizon exhausted) and deterministic dispatch-core rejections (`queue_not_registered`, `uniqueness_conflict`, `invalid_request`). Local incompatibility, pressure and transient errors only back off locally. Event `schedule_needs_review`; audit with the system actor |
| 6.8 | `RevalidateSchedule/1` | key, expected definition revision, actor | definition revision; no open episode | `needs_review := false`, `scan_after_at := now`; emits no jobs. Event `schedule_revalidated` |
| 6.9 | Due read (no receipt) | limit ≤ 32, optional exclusive scan position and horizon | gate | Plain read of due schedules with their current definition and the database time |

## 7. Overlap and uniqueness

### 7.1 Overlap

`allow` adds nothing. `serialize` adds the exclusive scope `(schedule, schedule_id)`, capacity 1, to every automatic and manual job. It serializes execution, not materialization: jobs keep being created while one runs.

### 7.2 Uniqueness

A template active key makes every occurrence share one key, so it requires explicit reuse (§2). With reuse, an occurrence that finds the key bound records `linked_existing` and the bound job. A uniqueness rejection never advances the cursor.

## 8. Lock order

gates → receipt → [outbox delivery identities, 0003] → schedule rows (one per command) → queue controls (read) → dispatch keys (sorted) → new jobs (ascending ID) → concurrency scopes (UUID order) → rate scopes (UUID order) → dependent rows. Edits and controls lock only the schedule row. Worker completion never locks a schedule row. Materialization never charges rate grants; charging is at Begin (10 §4).

## 9. Lifetimes and retention

| Operation | Default NotAfter | Cap | Receipt class |
|---|---|---|---|
| `RegisterSchedule` | 5 min | 10 min | durable (NotAfter + 24 h) |
| All other schedule operations | 30 s | 60 s | durable (NotAfter + 24 h) |

Occurrence detail is retained 30 days after materialization (cleanup is SP-021). The high-water mark and the schedule row are kept indefinitely; there is no automatic schedule TTL.

## 10. Defaults and bounds

| Name | Value |
|---|---|
| Due read | ≤ 32 schedules per pass, ordered `(scan_after_at, schedule_id)` |
| Polling | Continue promptly after a full productive page. Base poll 1 s after a short productive page. Unproductive pages and failures back off to 5 s (15 s with a healthy schedule listener), × (1 ± 0.10). Idle waits respect the earliest recorded due instant. |
| Materializations in flight | 1 per scheduler/store group |
| Local allowance | 100 jobs/s, burst 32 per scheduler/store group |
| Request / resolution budget | 5 s / 30 s |
| Plan | ≤ 32 jobs, ≤ 2 MiB payload |
| Misfire grace | default 60 s; 0 – 24 h |
| Catch-up | ≤ 32 per transaction; episode limit default 1,000 (1 – 10,000); oldest age default 7 d (1 min – 31 d); pacing ≥ 250 ms |
| Preview | 10 occurrences (12 §9) |
| I/O lane | background; never the renewal lane |

## 11. Cases

| Case | Test | Proves |
|---|---|---|
| SCH-01 | `Competing_materializations_of_one_plan_create_one_job` | Two commands with the same plan concurrently: one occurrence, one job, `cursor_revision + 1`, the loser `VersionConflict` |
| SCH-02 | `A_fault_inside_materialization_leaves_nothing` | Faults at `AfterMutation` and `BeforeCommit`: no job, key, occurrence, receipt or cursor change |
| SCH-03 | `Pause_racing_materialization_has_one_serial_outcome` | Pause and materialize on one row: either the jobs exist and the pause follows, or the plan is rejected with `revision_conflict`; never both stale |
| SCH-04 | `Catch_up_runs_in_batches_under_a_fixed_cutoff` | 70 missed: batches of 32/32/6 with one cutoff; `evaluated_through` = last emitted; after a restart the remainder continues; the limit sets review with the cursor kept |
| SCH-05 | `Pruned_history_never_redispatches_below_the_high_water_mark` | After deleting occurrence rows, a plan at or below `evaluated_through` is `occurrence_below_high_water`; a replayed trigger returns its receipt |
| SCHED-09 | `Misfire_policies_cover_the_missed_range` | T = 12:10Z, missed from 12:00Z, grace 60 s: run-once emits 12:00Z covering to 12:10Z; catch-up emits 11; skip 0; next 12:11Z (pure planner plus store) |
| SCHED-10 | `An_episode_of_exactly_the_limit_completes_and_one_more_needs_review` | Limit 40: 40 missed completes; 41 sets `catch_up_limit` |
| SCHED-11 | `A_missing_profile_is_a_local_diagnostic_only` | A definition with an unknown profile is skipped locally; the row is not marked for review |
| SCHED-12 | `A_lost_materialization_reply_keeps_the_same_command` | DropReply: the same command resolves to the same mapping; no second job |
| SCHED-13 | `An_edit_cuts_over_forward_and_never_rewinds` | Old jobs keep their payload; a plan against the old revision is rejected; `evaluated_through` never decreases |
| SCHED-15 | `Manual_triggers_are_idempotent_and_ignore_the_cursor` | Same trigger ID → same job; paused schedule still triggers; serialize scope present; resume while an episode is open is rejected |
| PGX-02 (schedule half) | `Scheduled_jobs_carry_the_template_profile_tuple` | Job tuple equals the definition's |
| CMD-01 … 05 | `Schedule_commands_follow_the_receipt_protocol` (theory per operation) | Concurrent and sequential sends apply once; changed body → `CommandConflict`; DropReply resolves; lock past NotAfter → `CommandExpired`; pruned receipt → `CommandExpired` |
| ISO-01 | `The_same_schedule_key_in_two_applications_never_interacts` | Two applications, one key: separate rows, jobs and revisions |
| PGX-04 (materialization) | `ScheduleCaseTests.Materialization_and_scope_creation_in_opposite_orders_never_deadlock`, `…Materialization_versus_scope_creation_and_edits_in_both_orders` | 30 free races and two barrier orders with new scopes created in opposite orders, plus an edit waiting on the row: every command applies, no deadlock |
| P-06 (scheduler half) | not yet written | 1,000 minute schedules plus one 1,000-occurrence catch-up; lag p99 ≤ schedules ÷ `JobsPerSecond` + 3 s (13 s for 1,000 schedules at the default 100/s; owner decision 2026-11-09); worker lease loss 0 |

Added 2026-11-01:

| Case | Test | Proves |
|---|---|---|
| SCHED-09, SCHED-10, SCHED-13, SCHED-15, SCH-04 | `SchedulePlannerTests` (Core, Pure, 17 tests) | The planner alone: misfire policies and grace equality, batches of 32, one cutoff per episode, the limit and age reviews, pending resume policies, one-time schedules, revisions carried, never rewinding |
| SCHED-09, SCHED-13, SCHED-15 | `ScheduleCaseTests.The_hosted_scheduler_seeds_once_materializes_and_jobs_see_their_occurrence` | Hosted `SchedulerService`: the seed registers once; the loop materializes; `IJobContext.Schedule` equals the occurrence row (manual: store time; plain job: null); a restart keeps the pause; a drifted declaration never overwrites |
| SCHED-11 | `ScheduleCaseTests.A_missing_profile_is_a_local_diagnostic_only` | Unknown profile and unknown TZDB hash: `schedule_missing_profile` / `schedule_missing_artifact` local diagnostics, no review, no job, no event or audit, cursor kept; a valid schedule beside them runs; restored, it materializes |
| SCHED-14 (scheduler half) | `ScheduleCaseTests.An_elapsed_evaluation_budget_backs_off_locally_and_writes_nothing` | An elapsed budget is a local `schedule_budget_elapsed` diagnostic; no failure command, review, job or cursor move. The slot rule is SP-015's `EvaluatorBoundsTests` |
| CMD-04 | `ScheduleCaseTests.A_schedule_command_whose_deadline_passes_during_a_lock_wait_expires_without_effect` | Materialization waiting on a held schedule row past NotAfter: `CommandExpired` / `ObservedNoEffect`, no receipt, job or cursor move; an applied command replayed after NotAfter returns its receipt |
| CMD-05 | `ScheduleCaseTests.Pruned_schedule_receipts_replay_as_expired_and_never_reapply` | After harness pruning, Materialize, SetScheduleState and Trigger replays are `CommandExpired` with no new job, audit or revision |
| SCHED-13 (§10 clock backward) | `ScheduleCaseTests.A_plan_cut_off_after_store_time_is_rejected_and_the_cursor_waits` | A cutoff after store time is `cutoff_in_future`; nothing changes; a plan at store time applies |
| SCH-03 (§10 queue paused) | `ScheduleCaseTests.A_paused_queue_keeps_materializing_while_execution_waits` | Materialization continues on a paused queue; Reserve refuses; the same plan again creates nothing |
| SCHED-15 (§10 slow serialized) | `ScheduleCaseTests.Serialized_catch_up_jobs_never_run_concurrently` | Three caught-up serialized jobs on four slots run one at a time |
| SCH-01 (§10 uniqueness) | `ScheduleCaseTests.Cross_occurrence_uniqueness_needs_explicit_reuse_and_then_links` | Active key without reuse fails validation; with reuse later occurrences are `linked_existing` to one job |

Other §10 rows: two schedulers (SCH-01), disconnect (SCHED-12), fault rollback (SCH-02), pause before plan (SCH-03), edit race (SCHED-13 and PGX-04 order 1), downtime and budget (SCHED-09, SCHED-10, SCH-04), wrong evaluator (SCHED-11), pruned history (SCH-05), manual triggers (SCHED-15), staggered definitions (the seed drift test). Fold, gap and transitions are SP-015 (SCHED-01 … 08). Retry overtaking under `serialize` is visible only as ordinary retry timing; no test asserts it.

## 12. Must not claim

- strict chronological completion under `serialize` (jobs are created in order; execution order is the worker's);
- a hard per-schedule backlog cap beyond the episode limit;
- deduplication after history loss beyond the high-water mark;
- an application-wide scheduler throughput limit (the allowance is per scheduler/store group);
- that a paused schedule's pending jobs are paused (pause stops materialization only);
- exactly-once execution of an occurrence (it is at most one job; the job is at-least-once).
