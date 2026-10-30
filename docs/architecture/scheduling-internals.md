# Scheduling and occurrence materialization

Status: engineering proposal, 2026-10-29. Complements [capacity control](capacity-control.md), [worker internals](worker-engine-internals.md), and the [PostgreSQL design](postgres-storage-design.md). No runtime implementation, migration, or scheduler test has run.

The subsequent [driver matrix](driver-capabilities.md) proposes a common millisecond precision for persisted occurrence identity and tests publication/retention across each supported backend. Normalize instants before key construction; higher-precision storage does not create a different logical occurrence.

The [scheduler evaluator/defaults packet](scheduler-evaluator-and-defaults.md) now develops D-08: Cronos/NodaTime recommendations, exact restricted grammar and OR adapter, pinned zone-artifact identity, transition-aware UTC evaluation, defaults and SCHED cases. It takes precedence for those concrete choices and refines prepared cutovers so evaluation remains outside write locks.

## 1. What the scheduler guarantees

The scheduler converts a due logical occurrence into a durable job. It does not run handlers or own their retry policy. Multiple scheduler replicas may process different schedules concurrently. A database uniqueness constraint and an atomic cursor/job transaction prevent duplicate materialization; a leader election or process mutex alone does not.

One occurrence normally maps to one job ID, whose attempts and manual-retry generations remain under the existing engine. Delayed dispatch directly creates a job with a future eligible_at and needs no recurring schedule record. A managed one-time schedule uses the occurrence machinery once and then becomes exhausted.

Misfire means the scheduler has not materialized a due occurrence within its configured grace period. A job already queued but waiting for capacity is not a scheduler misfire. Queue pause stops reservations; schedule pause stops new materialization. Existing jobs continue according to queue and job controls.

## 2. Definition, control, and cursor records

All records are scoped by application, use bounded stable identifiers, and store instants as timestamptz. Proposed records:

```text
schedules:
  application, schedule_id uuid                       PK
  schedule_key text                                  UNIQUE(application, schedule_key)
  definition_revision bigint, control_revision bigint
  cursor_revision bigint
  enabled boolean, paused boolean, needs_review boolean
  next_due_at timestamptz?, scan_after_at timestamptz
  evaluated_through timestamptz?
  episode_id uuid?, episode_cutoff timestamptz?
  episode_next_at timestamptz?, episode_emitted integer
  created_at, updated_at timestamptz

schedule_definitions:
  application, schedule_id, revision                  PK; FK → schedules
  kind text                                          one_time | cron
  due_at timestamptz?                                 one_time
  cron_text text?, cron_dialect text?, zone_id text?
  evaluator_version text, zone_data_version text?
  zone_data_sha256 bytea?                             32 bytes for cron; null for one_time
  gap_policy text?, fold_policy text?
  misfire_policy text, misfire_grace_ms bigint
  max_batch integer, max_episode_jobs integer
  max_catchup_age_ms bigint
  overlap_policy text                                allow | serialize
  job_name text, payload_version integer
  payload_template_utf8 bytea, serializer_profile_id text, serializer_profile_version integer
  policy_snapshot_version integer, dispatch_options jsonb
  definition_fingerprint bytea

schedule_occurrences:
  application, schedule_id, occurrence_kind, occurrence_key   PK
  scheduled_at timestamptz?, definition_revision bigint
  job_id uuid?, disposition text
  covered_from timestamptz?, covered_through timestamptz?
  materialized_at timestamptz, command_id uuid

schedule_audit_ranges:
  application, audit_id uuid                          PK
  schedule_id, definition_revision, command_id
  from_exclusive timestamptz?, through_inclusive timestamptz
  reason text, created_at timestamptz
```

Definitions are immutable versions. Separate operational pause/control revisions survive code deployments. Cursor revision changes whenever materialization or a control operation changes future scheduling position. Represent timestamps/nullability and state relationships with constraints when authoring migrations; the above is a record plan, not executable DDL.

The [consolidated schema](postgres-schema-and-operations.md) aligns durations to milliseconds and templates to original bytes/profile identity. Every command enters through store/application compatibility gates. These supersede the earlier microsecond/template sketches; no runtime schema was deployed from them.

Suggested index: `(application, scan_after_at, schedule_id)` for enabled, unpaused schedules not requiring review, plus an occurrence history index `(application, schedule_id, scheduled_at)`. scan_after_at normally equals next_due_at, but bounded retry/backoff may postpone a failed or catch-up scan without changing the intended occurrence time. Exhausted schedules have no next_due_at and leave the due index. Always use literal predicates matching the chosen partial index.

The evaluated-through mark covers only work actually emitted, coalesced, or explicitly skipped in the committed batch. A partial normal/catch-up batch advances it to its last processed instant, never straight to the episode cutoff. Only a fully settled episode or explicit skip can advance through that cutoff. Keep next_due_at at the first unprocessed occurrence.

The definition includes a versioned serialized job template and declarative context bindings such as scheduled instant and schedule key. Payload construction must be deterministic and bounded. Do not run arbitrary user factories or make external calls while holding schedule locks. A future dynamic-payload feature needs its own failure/idempotency contract.

## 3. Occurrence identity and retention

For automatic occurrences use kind `scheduled` and a canonical occurrence key encoding the UTC scheduled instant at the persisted timestamp precision. Unique identity is `(application, schedule_id, kind, key)`. Definition revision is metadata, not part of this unique key: merely editing a definition must not create another job for the same scheduled instant.

The recurrence evaluator's output is a strictly increasing sequence of distinct UTC instants. If a gap-shift policy maps several nominal local matches to the same instant, coalesce them into one occurrence for that schedule and record the adjustment. This is a stated policy, not accidental ON CONFLICT behavior. Fold occurrences with distinct UTC instants are distinct even when their local labels match.

Manual triggers use kind `manual` with a caller command/idempotency ID. A repeated manual-trigger request returns its existing result; a deliberate new click with a new ID creates new work. Manual triggers do not advance the automatic cursor and normally honor the same overlap policy. An authorized trigger may run while the schedule is paused; that action is audited and does not resume it.

Preserve the schedule's evaluated-through high-water mark and stable identity after occurrence history is pruned. Normal materialization never inserts at or before that committed mark. Old command replays obey their receipts/deadlines. Editing a definition cannot rewind the cursor; historical replay is a separate future operation with explicit identities, not a timestamp update.

Occurrence job references are retained logical IDs and need not prevent job-history deletion. A replay after history retention returns “previously materialized, history expired,” not a replacement job. Schedule deletion creates a tombstone; recreating a schedule is an explicit new identity. Keep paused definitions and identity metadata required for retention/replay correctness.

## 4. Materialization transaction

Use optimistic computation outside locks, followed by a short validation/write transaction:

1. Read a due schedule snapshot, definition, cursor/control revisions, and a database time T. Compute a bounded plan through this frozen cutoff. Return expected revisions, before/after cursor, occurrence keys/templates, and any audit range. No payload or business side effects occur in this phase.
2. Create one mutation command with stable ID, deadline, and fingerprint over the immutable plan. A changed plan requires a new ID after the earlier command is definitively rejected/resolved.
3. Lock command receipt → schedule row. Recheck enabled/pause/review flags, every expected revision, evaluator/time-zone version, and current deadline. If stale, record/reject VersionConflict without advancing anything. A historical receipt may resolve an already-applied command even after pause.
4. Insert occurrence identities and dispatch their jobs, job payloads, generation/requirements, and audited key bindings using the same transaction. Sort any dispatch keys across the whole batch before acquisition; job/scope creation follows the existing dispatch lock rules. Limit the batch size and transaction duration.
5. Write the after-cursor, next_due_at/scan_after_at, episode progress, event/audit ranges, and receipt. Commit all changes together. Signal workers only after commit or via the optional commit-bound notification path.

The scheduler is an internal dispatch caller that joins the transaction; it must not recursively call a public dispatch method that starts and commits a second transaction. It also cannot acquire new command receipts after lower-level locks: the materialization command owns this entire batch.

If an occurrence identity already exists, verify its compatible disposition/job reference and do not enqueue again. In the normal path the cursor and unique row committed together; an unexpected mismatch is a diagnostic/invariant failure, not permission to silently advance past unknown work. A lost commit response retries the same materialization command and returns the same occurrence/job mapping.

After store/application gates, materialization lock order is command → schedule → all dispatch keys → brand-new job records → scope provisioning/FKs. Provision concurrency then rate scope families across the entire batch, not alternating per job. Worker completion never locks a schedule row; it changes job/permit/key state only. Scheduler telemetry derives progress by reads. This avoids that schedule/job lock inversion; implicit FK/unique locks still need race tests.

Competing replicas may compute the same plan; only one can validate and advance the locked cursor. This first design intentionally allows redundant bounded computation rather than a long transaction around recurrence evaluation. SKIP LOCKED may be used for an optional short claim optimization later, but no scheduler lease is needed to make materialization correct.

## 5. Misfires and bounded recovery episodes

If `next_due_at <= T - MisfireGrace`, open a recovery episode with immutable cutoff T. Otherwise materialize normally due occurrences through T in bounded batches. Freezing T prevents “catch up to now” from continually moving while a backlog is processed.

The episode policy applies to **all unmaterialized due occurrences through T**, including recent ones inside the grace period once the oldest occurrence triggered recovery:

| Policy | Effect |
|---|---|
| skip | Emit no job for the episode; audit its time range; advance to the first occurrence strictly after T |
| run-once | Emit one job keyed by the earliest unmaterialized due instant; record coverage through T; advance strictly past T |
| catch-up | Emit each due occurrence in chronological UTC order, in bounded batches, preserving cursor progress between commits |

The run-once job carries both its scheduled instant and covered range so applications can distinguish it from an ordinary firing. It is not one job per scheduler restart. The atomic cursor/episode/occurrence transaction makes retries of the same episode idempotent.

Proposed validation profile: maximum 32 emitted jobs per transaction, maximum 1,000 jobs per catch-up episode, and maximum catch-up age seven days. These are tuning proposals, not universal defaults. Reaching an age/episode limit pauses materialization with `needs_review` and preserves the unprocessed cursor. Do not silently drop older occurrences. An explicit audited operation can skip the remainder, extend the budget, or change the policy.

Between batches yield to other due schedules and set a bounded scan delay for a schedule that cannot progress. Maintain separate global/per-schedule materialization throughput budgets so a recovery storm does not starve worker renewals or ordinary dispatch. If recurrence computation exceeds its time/search budget, report needs_review; it cannot hold a database transaction indefinitely.

Skip/run-once require the evaluator to find the next occurrence after T without enumerating every missed second. Range audit counts may be unknown; record the covered interval rather than inventing an exact count. Catch-up advances only through emitted/coalesced/explicitly skipped occurrences. Preserve the episode cutoff until it is completed or explicitly changed by management.

This adapts a known scheduler distinction: Quartz's CronTrigger implementation moves to a next firing for DoNothing or sets a single immediate firing for FireOnceNow. Spindle preserves its own occurrence identity, bounded episodes, and materialization-versus-execution boundary. No Quartz default or performance result is imported. [Pinned implementation](https://raw.githubusercontent.com/quartznet/quartznet/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz/Impl/Triggers/CronTriggerImpl.cs)

## 6. Cron dialect and DST mapping

Do not let a parser upgrade silently redefine a persisted schedule. Store a dialect/evaluator version and validate a preview of upcoming occurrences before accepting a definition.

Proposed initial dialect: explicit five-field minute cron, or explicitly selected six-field seconds cron; numeric values, wildcards, lists, ranges, and steps. Month/day names may be normalized during validation. When day-of-month and day-of-week are both restricted, this proposal uses OR semantics. No implicit format guessing, locale-specific parsing, or undocumented advanced tokens. Library-specific extensions require an explicit dialect version. The supported grammar and package selection remain a conformance gate before implementation.

Separate nominal local cron matching from time-zone mapping. A candidate evaluator must expose enough control to apply these stored policies:

| Situation | Proposed options | Meaning |
|---|---|---|
| Local time is absent during a forward transition | skip; shift-to-next-valid | Skip it or map to the transition's first valid instant; collisions coalesce as defined above |
| Local time has two offsets during a backward transition | earlier; later; both | Select the first/second UTC instant or emit both |

Recommended starting policy is skip gaps and earlier folds; it remains a proposal. UTC schedules have neither ambiguity. Do not assume a transition is always one hour, or that local calendar days always exist in every zone.

The D-08 packet recommends Cronos 0.13.0 as a nominal calendar matcher. Its AND day-field semantics require the proposed union adapter, and its built-in time-zone path does not implement all Spindle gap/fold policies. Use the UTC-only matcher through the private nominal-calendar adapter, with separately controlled zone mapping. [Release-tag implementation](https://github.com/HangfireIO/Cronos/blob/v0.13.0/src/Cronos/CronExpression.cs)

The D-08 packet recommends NodaTime 3.3.7 and an explicit TZDB 2026f artifact with its verified byte hash. Persist canonical zone ID, data label/hash and evaluator profile; require matching supported capabilities before processing. A local unsupported version leaves that replica unable to process the schedule, without globally blocking capable replicas. A data/profile change is an explicit forward definition migration. Mapping/ordering and dependency integration remain unvalidated. [Mapping API](https://nodatime.org/3.3.x/api/NodaTime.TimeZones.ZoneLocalMapping.html)

The evaluator returns instants in UTC order, including interleaved fold matches. For example, when every minute repeats, it cannot emit both offsets of 02:00 and then both offsets of 02:01: that order may move backward in UTC. Use transition-aware merging or an equivalent proven algorithm with bounded lookahead. A cursor based only on local wall time loses occurrences. The stored materialization cursor is UTC; evaluator continuation must reconstruct all remaining matches after that instant under the pinned rules.

DST examples for the Europe/Berlin 2026 transitions are **test fixtures to validate against the selected data version**, not an assumption that every zone behaves alike:

- Daily 02:30 on the spring transition: skip under gap=skip; shift-to-next-valid maps it to 03:00 local.
- Daily 02:30 on the fall transition: earlier/later each emit one instant; both emits two distinct instants.
- Every minute through the fall transition: both emits the repeated interval in strictly increasing UTC order.
- A shifted gap match colliding with a normal 03:00 match emits one occurrence with adjustment metadata.

Add non-hour transitions, midnight transitions, leap years, month-end ranges, impossible expressions, and data-version changes to the conformance suite. Do not write an unbounded second-by-second scan as the recurrence algorithm.

Planning fixture check against this environment's installed zoneinfo confirmed that Berlin 2026-03-29 02:30 is nonexistent and 2026-10-25 02:30 maps to 00:30Z or 01:30Z. This checks the examples only; it does not select a production zone-data version or validate the proposed recurrence adapter.

## 7. Overlap, retries, and uniqueness

`overlap=allow` adds no schedule-specific execution scope. `overlap=serialize` adds the stable exclusive resource scope `schedule/<schedule_id>` to every automatic and manual job. The existing reservation/permit transaction enforces it across replicas and definition revisions. It limits valid overlapping executions; a stale handler ignoring cancellation still requires external side-effect idempotency/fencing.

Serialization allows pending occurrences to accumulate. Catch-up limits cap a recovery episode, not lifetime backlog while a slow job runs. Expose outstanding occurrence count and oldest age; a future skip-while-active/coalescing policy needs a separate atomic admission contract and must not be silently substituted for serialize. The first design does not claim a hard per-schedule backlog cap.

Retries stay on the original occurrence's job ID. They do not advance or rewind the schedule. A newer occurrence may execute while an older one waits for retry eligibility, even with serialize; serialize means exclusion, not strict occurrence-completion order. Strict serial chronological workflows would require a different contract.

Default scheduled dispatch must not accidentally merge distinct occurrences through a recurring payload's active/window uniqueness key. Validate this at schedule registration: reject cross-occurrence deduplication settings unless the schedule explicitly opts into reuse. With reuse enabled, store disposition `linked_existing` and the reused job ID on each occurrence; make the collapse visible. Ordinary dispatch uniqueness is unchanged. No duplicate key conflict may advance the schedule while leaving an unrecorded disposition.

## 8. Pause, resume, edit, and deployment behavior

Schedule pause updates control revision under the same schedule lock as materialization. A batch committed before pause remains queued; after pause wins the lock no new plan can validate. Pause does not cancel existing jobs or reset their retry budgets.

Resume explicitly chooses `skip-paused`, `run-once`, or `catch-up`. Suggested default is skip-paused with an audit range and next occurrence strictly after resume's database cutoff. Existing recovery episodes must be resolved or explicitly replaced; do not silently mix their cutoff with the resume cutoff. Queue pause is independent and does not stop schedule cursor advancement.

Code registration is seed-if-absent with a structural fingerprint. A different fingerprint produces a drift diagnostic and requires a revision-checked definition update; replicas with different binaries cannot repeatedly rewrite the schedule. Operational pause/enable state is never reset by startup registration.

A definition edit is forward-only: prepare with a database-sampled cutover T and compute the new first instant strictly after T and the observed high-water mark outside write locks. Under the schedule lock validate all expected revisions/cursor and fresh-time constraints, reject an unresolved recovery episode unless explicitly settled, and atomically audit unmaterialized old-definition occurrences through T as skipped by edit with the new definition/cursor. T is the frozen logical cutover, not the commit timestamp. Already materialized jobs keep their original payload/policy. If skipping pending old occurrences is unwanted, drain/settle them before the edit. A no-op fingerprint update does not advance the cursor.

This proposal avoids double firing from revision-keyed uniqueness but intentionally makes edit consequences visible. Changing zone data, cron expression, payload version, or overlap structure is a definition change. Enabling serialize while old allow-mode jobs are still active cannot retroactively give them permits: quiesce/drain affected jobs before activating the stronger guarantee.

## 9. Commands and integration surface

New internal semantic operations (names provisional):

| Operation | Guards | Atomic result |
|---|---|---|
| RegisterSchedule | Stable key, definition fingerprint | Seed absent definition; return same or drift |
| MaterializeOccurrences | Command ID/deadline plus expected definition/control/cursor revisions | Occurrences + jobs + cursor/episode + receipt |
| PauseSchedule / ResumeSchedule | Expected control/cursor revisions | Controls and any explicit coverage/cursor change + audit |
| UpdateSchedule | Expected definition/control/cursor revisions | Forward cutover + new definition + audit |
| TriggerSchedule | Command ID/deadline, expected definition revision | Manual occurrence + job + receipt, no automatic cursor change |
| ResolveCatchUp | Expected revisions and episode ID | Explicit skip/extend/change action + audit |
| RecordScheduleEvaluationFailure | Expected definition/control/cursor revisions, exact evaluator/data profile, deterministic reason | needs_review + audit/receipt, no cursor advance; local missing capabilities/elapsed budgets cannot globally block schedules |
| RevalidateSchedule | Expected revisions/profile and successful outside-lock evaluation; no unresolved catch-up limit | Clear non-episode needs_review + audit/receipt; preserve cursor/pause, emit no jobs |

Use the same command fingerprint/replay rules as execution storage. Final codec/schema details follow this semantic design. Workers need bounded schedule/occurrence metadata in their payload envelope/context; it is data, not permission to mutate the cursor. Management queries join occurrence/job history without putting scheduler writes into handler completion transactions.

## 10. Validation and implementation gates

| Scenario | Required result |
|---|---|
| Two schedulers compute the same plan | One committed cursor advance and one logical occurrence/job per key |
| Disconnect after materialization commit | Same command resolves original jobs; no duplicate dispatch |
| Transaction fails after job insertion | Jobs, keys, occurrences, and cursor all roll back |
| Pause wins lock before stale plan | No materialization; existing queued jobs unchanged |
| Definition edit races materialization | One serial outcome; no revision-based duplicate occurrence |
| Long downtime under each misfire policy | Correct covered range, bounded batch work, stable cutoff |
| Catch-up budget reached | needs_review with unprocessed cursor preserved |
| Clock moves backward | Cursor never rewinds; due work waits for authoritative time |
| Queue paused but scheduler running | Materialization continues; execution waits; no scheduler duplicate |
| Slow/retrying serialized job | No two valid schedule permits; accumulation and retry overtaking are visible |
| Fold, gap, collision, non-hour transition | Matches stored policy and increasing UTC order |
| Scheduler with wrong zone/evaluator version | Diagnostic and no processing, rather than different occurrence keys |
| Occurrence/job history pruned | High-water mark still prevents ordinary redispatch |
| Manual trigger retried versus repeated intentionally | Same ID deduplicates; new ID creates a new manual occurrence |
| Cross-occurrence uniqueness requested | Validation error unless explicit reuse; linked disposition when enabled |
| Staggered schedule definitions across replicas | Drift reported; operational controls do not flap |

Implement evaluator conformance and plan generation as pure tests first; then receipt/transaction/constraint tests against PostgreSQL; then multi-process crash and contention tests. Measure materialization lag separately from job start lag, batch lock duration, due-index query cost, recovery-storm throughput, and impact on worker renewal deadlines.

The [D-08 packet](scheduler-evaluator-and-defaults.md) now supplies concrete parser/time-zone versions, grammar/defaults, tuning budgets and cutover refinements. Review and executable SCHED/SCH evidence remain open. Existing-library tests and documentation are evidence for comparison, not a substitute for these Spindle tests. The [outbox closure](transactional-outbox.md) and [final baseline](../architecture-baseline.md) now complete planning.
