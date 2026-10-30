> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Scheduler evaluator, time-zone data and defaults

Status: concrete D-08 recommendation, researched 2026-10-29. Planning only; no scheduler, dependency installation or runtime conformance suite has been implemented. This packet refines [scheduling internals](scheduling-internals.md) and uses the [capacity/I/O budgets](capacity-defaults-and-budgets.md). Package selections and numerical defaults below are recommendations, not completed certification.

## 1. Dependency selection and inspected implementations

Recommend **Cronos 0.13.0** for calendar matching and **NodaTime 3.3.7** for explicit time-zone mapping. These are the stable versions listed during this research. Keep both internal to Runwick's existing scheduler module; expose no dependency-specific types in job contracts. Recheck/pin exact versions when SP-015 starts. [Cronos package](https://www.nuget.org/packages/Cronos/0.13.0), [NodaTime package](https://www.nuget.org/packages/NodaTime/3.3.7)

The inspected [Cronos v0.13.0 implementation](https://github.com/HangfireIO/Cronos/blob/v0.13.0/src/Cronos/CronExpression.cs) has a UTC-only next-occurrence path and a separate TimeZoneInfo path. Its day predicate combines day-of-month and day-of-week with AND. Its time-zone path shifts gap matches and handles repeated interval expressions differently from fixed-time expressions. Therefore directly passing a zone to Cronos would not implement Runwick's proposed OR/day and configurable gap/fold semantics.

The corresponding [Cronos tests](https://github.com/HangfireIO/Cronos/blob/v0.13.0/tests/Cronos.Tests/CronExpressionFacts.cs) include UTC-kind rejection, gap/fold boundaries and half-hour transitions. We inspected those cases; we did not execute them. These are useful adapter regression inputs, not evidence that upstream rejected Runwick's policies for security or performance reasons.

The inspected [NodaTime 3.3.7 implementation](https://github.com/nodatime/nodatime/blob/3.3.7/src/NodaTime/DateTimeZone.cs) provides MapLocal with zero/one/two mappings and enumerates constant-offset zone intervals in instant order. Those primitives support Runwick's explicit mapping choices. Use an adapter around them rather than converting back through machine-local TimeZoneInfo rules.

## 2. Versioned cron grammar

Recommend two explicit dialect IDs: `runwick.cron5.v1` and `runwick.cron6.v1`. Five-field is the default; six-field requires an explicit selection. No field-count guessing. Both use the ISO/Gregorian calendar, with second zero implied by the five-field form.

| Field | Values |
|---|---|
| Second, six-field only | 0–59 |
| Minute | 0–59 |
| Hour | 0–23 |
| Day of month | 1–31, subject to the actual calendar |
| Month | 1–12; case-insensitive English JAN–DEC aliases |
| Day of week | 0–6, Sunday=0; 7 is also accepted as Sunday; SUN–SAT aliases |

Allow `*`, values, comma lists, ascending inclusive ranges, `*/n` and `a-b/n`. Steps are positive integers no greater than that field's domain width. Reject bare `a/n`, reversed/wrapping ranges, empty list members, leap second 60, year fields, macros, `?`, `L`, `W`, `#`, `H`, randomization and embedded time-zone/environment directives. New Cronos features do not automatically become Runwick grammar. Bound raw input to 256 ASCII characters and 64 list terms across the expression before parser calls; allow ASCII spaces/tabs as separators, not newlines/control characters.

Normalize names, duplicates and equivalent field sets into deterministic numeric bitsets. For day-of-week, expand ranges in the input's 0–7 domain before mapping 7 to 0. A field selecting its entire domain is unrestricted, including `*/1` or a full-range list. Preserve raw text for display, but hash the normalized sets, explicit dialect and policy/profile metadata. Formatting-only changes do not create definition drift.

Day rules are explicit:

- If both day fields are unrestricted, every valid calendar day matches.
- If one is unrestricted, the other controls matching.
- If both are restricted, either may match (**OR**).

Implement OR as the union of at most two Cronos streams: one expression with day-of-week wildcarded and one with day-of-month wildcarded. Merge/deduplicate their nominal local candidates. Other fields constrain both streams. For example, `0 9 1 * MON` means 09:00 on the first of the month or on Monday, with one occurrence when both match. Do not pass this expression unchanged to Cronos and silently obtain AND.

Reject provably impossible calendar branches before evaluation, such as February 30 with no weekday alternative. An impossible day-of-month branch does not invalidate a second, valid OR branch. A search/time budget being exhausted is not proof that an expression is impossible.

## 3. Time-zone artifact and profile identity

Default to explicit **UTC** when no zone is supplied. Accept case-sensitive IANA IDs and their aliases from the selected data artifact; normalize aliases through that artifact's canonical map and retain the entered ID for display. Reject Windows IDs and machine-local shorthand in the core API. A future convenience conversion must require an explicit territory/mapping version and persist the resulting IANA ID.

The Noda Time latest-data pointer currently identifies **TZDB 2026f**. The downloaded version-specific artifact was 132,773 bytes and had SHA-256:

```text
b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3
```

Recommend that exact artifact as the planning baseline: [versioned NZD file](https://nodatime.org/tzdb/tzdb2026f.nzd), [latest-data pointer inspected](https://nodatime.org/tzdb/latest.txt). The byte hash was checked; its contents have not been loaded through NodaTime in this workspace. Preserve the artifact and license/provenance in build inputs at implementation, rechecking its hash before packaging.

NodaTime supports loading an NZD stream into a separate provider, independently of its package's embedded rules. Use an application-owned, immutable provider per approved artifact; do not replace a process-global default or download latest data during scheduling. [Noda Time data loading](https://nodatime.org/3.3.x/userguide/tzdb)

Persist and include in the definition fingerprint:

- Dialect ID and evaluator profile, initially proposed as `runwick.cron.v1/cronos-0.13.0/nodatime-3.3.7`.
- Canonical zone ID, TZDB release label, exact NZD SHA-256 and stored gap/fold policy.
- Existing normalized cron fields, job/template profile, misfire/overlap settings and bounds.

Add `zone_data_sha256 bytea` to the initial schedule-definition schema, constrained to 32 bytes for cron definitions. Keep `zone_data_version` as the release label. The hash also identifies mapping/build differences that a release label alone cannot distinguish. UTC cron definitions retain the same explicit profile tuple for uniform compatibility checks. One-time schedules use `runwick.instant.v1` and need no cron/zone artifact.

Scheduler replicas advertise their supported profile/artifact tuples and query only compatible definitions. A replica missing an artifact reports local incompatibility; it must not globally set needs_review and block another capable replica. A deterministic failure reproduced under the required profile can set needs_review via a revision-checked command.

Dependency or data upgrades never silently change existing schedules. Deploy the approved new profile/data, preview affected definitions, then perform their existing forward-only definition update at a database cutover. Preserve old artifacts while old definitions need them. A binary may advertise support for an old profile only through explicit compatibility certification; merely having a newer package is insufficient. Materialized jobs, occurrence keys and old cursor history stay unchanged.

## 4. DST mapping algorithm

Defaults remain **skip gaps** and **earlier folds**. Supported explicit alternatives are shift-to-next-valid for gaps, and later/both for folds. Earlier/later refer to UTC order, not names such as standard/daylight time.

Use UTC zone intervals to avoid sorting an unbounded collection of repeated local matches:

1. Starting strictly after the committed UTC cursor, enumerate constant-offset intervals in UTC order within a bounded search horizon. Clip each interval to the requested instant range.
2. Translate that interval to local Gregorian coordinates. Use Cronos's UTC-only matcher on those coordinates through a private nominal-calendar adapter. Its DateTimeKind.Utc is only an encoding required by that API; the returned ticks are still nominal local calendar coordinates, never a persisted instant.
3. Merge the at-most-two calendar streams. Convert each candidate with the interval's offset, require that the resulting instant belongs to the interval and is strictly after the cursor, and check MapLocal against the requested fold policy. Jump over a wholly excluded fold range rather than scanning every excluded second.
4. At a forward transition, inspect the skipped local interval. For shift-to-next-valid, a bounded next-match query establishes whether any cron match lies in that gap; emit the transition's first valid instant once if so. Do not enumerate every skipped second to produce the same result. Skip emits none.
5. Merge a shifted gap occurrence with any normal match at that same instant. Record bounded adjustment metadata, including the skipped range; an exact count of collapsed nominal matches may be unknown. Process the boundary once, with half-open zone intervals and explicit inclusion rules.

The output is strictly increasing, distinct UTC instants normalized to the existing millisecond identity contract. A six-field expression has second resolution, not subsecond scheduling. Historical offsets may contain seconds; a five-field local match need not land on UTC second zero. Conversion must check range/overflow instead of rounding to a different occurrence.

A UTC cursor inside a fold must be able to find later matches whose local label is earlier than the cursor's local label. Enumerating UTC intervals handles that case without an arbitrary one-hour lookback. This is a proposed adapter algorithm requiring conformance tests, not a claim that combining these packages already implements it.

## 5. Scheduling and recovery defaults

| Setting | Initial recommendation |
|---|---|
| First cron occurrence on registration | First match strictly after a database-sampled registration cutoff; no implicit historical backfill |
| Misfire grace | 60 s; equality counts as a misfire |
| Default misfire | run-once, keyed by the earliest unmaterialized occurrence and covering the frozen episode cutoff |
| Catch-up | Explicit opt-in; at most 1,000 jobs per episode and oldest unprocessed age at most 7 days |
| Overlap | allow; serialize explicitly adds the existing schedule resource scope |
| Resume | skip-paused by default, with an audited cutoff; run-once/catch-up are explicit alternatives |
| Definition preview | Next 10 occurrences, with UTC/local instant, offset and adjustment/fold labels |
| Jobs per materialization transaction | At most 32, also subject to a 4 MiB total encoded plan budget |
| Due-schedule probe | At most 32 definitions per pass, ordered by scan_after_at and stable ID |
| Due/idle polling | 1 s while due work exists; idle backoff to 5 s, jitter ±10%; notifications are hints |
| Catch-up pacing | At least 250 ms persisted scan delay after a partial catch-up batch; yield to other due schedules |
| Local materialization allowance | 100 new jobs/s with burst 32 per scheduler/store group; one materialization command in flight |
| Materialization request / ambiguity resolution | 5 s end-to-end request budget; 30 s resolution budget; command NotAfter 30 s |

These are tuning recommendations. Local allowances multiply with scheduler replicas; they are not an application-wide throughput limiter. Persisted scan delay and cursor revisions coordinate a schedule across replicas. Validate current scan eligibility when applying a plan. Scheduler I/O uses the background class already budgeted in D-07, never worker renewal/settlement connections. Under control pressure, stop starting new batches while resolving any already-sent command.

Registration/edit/resume plans sample their cutoff T from storage during preparation and evaluate outside write locks. The mutation freezes T in its fingerprint and validates expected revisions/high-water mark under the schedule lock; it does not run the evaluator there. This refines the earlier edit sketch's wording about choosing T under the lock. Cutoff is a recorded logical boundary, not the eventual commit timestamp. Reject/replan a not-yet-applied plan if fresh validation time is earlier than T after a backward clock change. Conflict/replanning still requires the prior command to be definitively resolved. Already materialized old-definition jobs survive the cutover.

Bound encoded plan bytes before opening the write transaction. A payload template may fit its per-job limit but produce too many jobs for one batch; shorten the batch without moving past unprocessed occurrences. If even one job cannot fit the configured plan limit, reject configuration or mark the schedule for review; do not silently skip it. Limits include template expansions, metadata, keys and planned result mappings, not only payload bytes.

An uncertain materialization blocks another local plan for that schedule until resolved. Keep its immutable command/plan while bounded resolution runs. On resolution-budget expiry, stop new local scheduler submissions for the group and expose the uncertainty; do not discard the handle and create a fresh command. Another replica may still progress through the normal cursor/receipt protocol. This extends worker-style uncertainty discipline to scheduler-owned work without introducing a scheduler lease.

Catch-up age/count limits apply to catch-up, not to skip/run-once range compaction. At exactly 1,000 emitted jobs, finish the episode if no due work remains; otherwise set needs_review and preserve the first unprocessed occurrence. Default run-once can cover an old outage without enumerating every missed occurrence. Queue pause still permits materialization and is not a misfire; a slow serialized job can still accumulate future occurrences.

A managed one-time schedule has one UTC due instant and becomes exhausted after its recorded disposition. A past due instant uses its explicit/default misfire policy; run-once creates one job, skip records a skipped occurrence. skip-paused resume may exhaust a missed one-time schedule without running it. A one-time definition edit must provide a future instant after the cutover/high-water mark; use a new manual-trigger identity for an immediate intentional run.

## 6. Bounded evaluation and error classification

Validate grammar and calendar feasibility before calling third-party evaluation. Initial planning limits: at most 8 calendar years plus 2 days searched per next-occurrence lookup, 4,096 adapter candidate/transition steps per plan, and one CPU evaluator task per scheduler/store group. Skip/run-once jump to the next occurrence after their cutoff; catch-up returns a bounded prefix. Long gaps, non-hour transitions and a skipped whole date are explicit cases, not assumptions of a 24-hour maximum lookback.

Give plan computation a 250 ms cooperative work budget and registration/preview 1 s. Check deadlines between library calls and before publication. A stopwatch/cancellation token cannot preempt a synchronous library call; keep the executor slot until that call actually returns. Do not start replacement tasks on each timeout. The restricted grammar, feasibility checks and source/benchmark review must establish acceptable per-call cost before release. Failure of that gate requires revising the evaluator or isolation strategy, not claiming a hard CPU deadline.

Distinguish invalid grammar/calendar, no match within search budget, date-range overflow, missing profile/artifact, and provider/transport failure. Deterministic search-step/horizon exhaustion is visible needs_review for that definition when established under its correct profile, not a normal exhausted recurring schedule. An elapsed computation budget is local pressure: defer with backoff/report lack of progress, without declaring the shared definition invalid. DateTime/NodaTime boundary conversions require checked range handling. A transient database failure does not invalidate a cron definition.

Add a proposed internal `RecordScheduleEvaluationFailure` command for deterministic review conditions. It takes expected definition/control/cursor revisions, the profile/artifact tuple and a bounded reason; under the normal gates/receipt/schedule lock it sets needs_review and writes audit/receipt without advancing the cursor. Unsupported local profiles and transient/time-budget failures cannot use this command.

For a non-episode review, an explicit `RevalidateSchedule` computes a valid plan outside locks, then checks those same revisions/profile and clears needs_review with audit/receipt. It preserves the definition, unprocessed cursor and pause flag, and makes an enabled/unpaused schedule eligible for a fresh scan. It emits no job and cannot clear an unresolved catch-up limit; that uses ResolveCatchUp. A changed definition uses the existing UpdateSchedule cutover instead. This gives review conditions a recovery path without automatic retry loops or silent skipped history.

## 7. Conformance fixtures and acceptance

These are expected vectors to execute under the pinned evaluator/NZD pair. Installed system zoneinfo can cross-check familiar examples but cannot certify the pinned artifact.

| Case | Expected behavior |
|---|---|
| SCHED-01 | Five/six-field formats, aliases, Sunday 7, steps and normalized sets; unsupported tokens rejected before Cronos; equivalent text has one fingerprint |
| SCHED-02 | `0 9 1 * MON` after 2026-06-30T23:59:59Z in UTC emits July 1 09:00Z, then July 6 09:00Z; a Monday that is also the first emits once |
| SCHED-03 | February 30 without a weekday alternative is rejected; February 29 survives the non-leap year 2100 and finds 2104 after 2096 |
| SCHED-04 | Berlin daily 02:30 on 2026-03-29: gap skip emits none that day; shift emits 01:00Z (03:00 local), not 03:30 local |
| SCHED-05 | Berlin daily 02:30 on 2026-10-25: earlier=00:30Z, later=01:30Z, both emits those two in order |
| SCHED-06 | Every minute over Berlin's repeated 02:00–02:59 hour: both yields 120 distinct ordered instants; resuming after 00:59Z next finds 01:00Z despite the local clock moving backward |
| SCHED-07 | Gap shift colliding with a normal boundary match yields one occurrence; many gap matches do not cause unbounded enumeration |
| SCHED-08 | Lord Howe 30-minute transitions, Pacific/Apia's skipped 2011-12-30, midnight transitions and second-valued offsets preserve mapping/order without one-hour assumptions |
| SCHED-09 | At T=12:10Z, earliest missed minute 12:00Z and grace 60 s: run-once emits one 12:00Z identity covering through 12:10Z; catch-up emits 11; skip emits none; next is 12:11Z |
| SCHED-10 | Count/age/byte limits preserve first unprocessed cursor; exactly 1,000 final jobs complete, while a 1,001st triggers review; partial batches retain the episode cutoff |
| SCHED-11 | Replicas with different/missing profile/data hash never materialize under substitute rules; capability mismatch alone does not globally pause a valid schedule |
| SCHED-12 | Lost reply, pause/edit race, pacing and command expiry preserve occurrence/job/cursor atomicity across replicas |
| SCHED-13 | Definition/data upgrades use forward cutover, preserve existing jobs/high-water marks, reject stale plans and never silently rewind after history pruning |
| SCHED-14 | Invalid, sparse and dense expressions honor step/byte/task bounds; slow evaluator/provider work cannot spawn unbounded replacement tasks or starve renewal capacity |
| SCHED-15 | One-time late execution/skip, resume choices, manual trigger replay, overlap and explicit cross-occurrence reuse retain their distinct dispositions |

SP-015 owns evaluator/grammar/preview vectors; SP-016 owns plans/transactions/misfires; SP-026 owns profile/data upgrades; SP-027–029 own multi-process/fault/performance evidence. Combine these with existing SCH/PGX/UPG cases. Source inspection and artifact hashing performed here are evidence inputs; none of these cases has passed against Runwick.

An independent Python calendar/system-zoneinfo check confirmed the Berlin gap/fold, 120 repeated-minute instants, July OR example and 2096→2104 leap-year example. That environment reports zone data 2026e; this is deliberately not presented as validation of the selected 2026f NZD artifact or the Cronos/NodaTime adapter.

The [outbox design](transactional-outbox.md) and [final baseline](../architecture-baseline.md) complete all remaining planning items.
