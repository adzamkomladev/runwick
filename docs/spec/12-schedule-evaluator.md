# 12 — Schedule evaluator

**Sources:** [scheduler-evaluator-and-defaults.md](../architecture/scheduler-evaluator-and-defaults.md) §1–§4, §6–§7 (primary), [scheduling-internals.md](../architecture/scheduling-internals.md) §6, [02-serialization-and-wire.md](02-serialization-and-wire.md) §7 (canonical encoder), [06-defaults.md](06-defaults.md) (evaluator bounds), [next-milestone-draft.md](../plans/next-milestone-draft.md) (SP-015).

This file is normative for SP-015: the recurrence grammar, normalization, the definition fingerprint, the Cronos and NodaTime adapters, TZDB artifact identity, the bounded planner and preview, and the error classes. Storage, materialization, misfires and commands are spec 13 (SP-016). Numbers are restated where behavior depends on them; 06 is authoritative for the number if the two ever disagree.

## Changes (2026-10-31)

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution in this spec |
|---|---|---|
| C-1 | scheduling-internals §6 sketches "cron plus zone" handed to the library; the evaluator packet shows Cronos ANDs day fields and shifts gaps its own way | Cronos sees only normalized UTC-path expressions; zones are NodaTime's; OR is a union of two streams — §4, §7 |
| C-2 | The packet names an "evaluator profile" tuple but no fingerprint layout | Canonical envelope with `recurrence` and `settings` members under domain `runwick.schedule-definition.v1` — §5 |
| C-3 | "Steps" were not defined | One step = one zone interval visited, one Cronos call per stream, or one `MapLocal` — §8.2 |
| C-4 | The packet's edit sketch chose the cutoff "under the lock"; the evaluator packet moved evaluation outside locks | The evaluator is lock-free and storage-free; its caller supplies the cursor — §1 |

Implementation choices made here (each marked *[choice]* where it appears):

1. Gap wire names are `skip` and `shift`; fold wire names are `earlier`, `later`, `both` (§7).
2. Numbers in the grammar are one or two ASCII digits; a leading zero is allowed (`09`), three digits are not (§2.3).
3. Seven is Sunday only in day-of-week; `*` in day-of-week is 0–6, and a step's domain width there is 7 (§2.2).
4. The supported instant range is 0001-01-02T00:00Z to 9999-12-30T00:00Z, so local coordinates always fit `DateTime` (§8.1).
5. Fingerprint members other than `recurrence` belong to spec 13 and sit under `settings`; SP-015 hashes `settings = {}` (§5.1).
6. One-time definitions use the same envelope with a two-member `recurrence` (§5.3).
7. Error wire names are `schedule_*` snake_case, in the style of `RunwickErrorCodes` (§10).
8. Additional Pure cases `EVAL-01`…`EVAL-03` cover zone acceptance, artifact identity and golden fingerprints (§12.2).
9. The group's evaluator slot runs work synchronously inside a one-permit gate; cancellation abandons only the wait (§8.3).

## 1. Scope

The evaluator turns a definition (expression, dialect, zone, gap policy, fold policy) and a committed UTC cursor into the next occurrences strictly after that cursor. It is pure: no storage, no locks, no ambient clock except the `TimeProvider` used for its cooperative budget, and no machine time-zone data. It never chooses the cursor; spec 13 samples the registration cutoff and owns the cursor.

Cronos and NodaTime are internal dependencies of `Runwick`. No type from either appears in `Runwick.Contracts` or any public API (00 §6).

## 2. Grammar

### 2.1 Dialects

| ID | Fields | Notes |
|---|---|---|
| `runwick.cron5.v1` | minute hour day-of-month month day-of-week | Default. Second is implicitly 0 |
| `runwick.cron6.v1` | second minute hour day-of-month month day-of-week | Explicit selection only |
| `runwick.instant.v1` | none | One-time schedules (§5.3) |

The dialect is always explicit in the stored definition. The field count must equal the dialect's; a five-field dialect given six fields is invalid, never reinterpreted.

Both dialects use the ISO/Gregorian calendar. A six-field expression has second resolution, not sub-second.

### 2.2 Fields

| Field | Values | Names | Domain width |
|---|---|---|---|
| Second (cron6) | 0–59 | — | 60 |
| Minute | 0–59 | — | 60 |
| Hour | 0–23 | — | 24 |
| Day of month | 1–31, subject to the calendar | — | 31 |
| Month | 1–12 | `JAN`–`DEC`, case-insensitive | 12 |
| Day of week | 0–7, Sunday = 0 and 7 | `SUN`–`SAT`, case-insensitive | 7 *[choice]* |

### 2.3 Terms

A field is a comma list of terms. A term is one of:

- `*` — the whole field (day of week: 0–6);
- `v` — a value or name;
- `a-b` — an ascending inclusive range, `a ≤ b`, endpoints values or names;
- `*/n` or `a-b/n` — every `n`th value from the start of the range, `1 ≤ n ≤` the field's domain width.

Numbers are one or two ASCII digits *[choice]*. Day-of-week ranges are expanded in the 0–7 domain, then 7 maps to 0 (`5-7` = {0, 5, 6}; `1-7/2` = {0, 1, 3, 5}).

Rejected, with `schedule_invalid_grammar`:

- bare `a/n`; reversed or wrapping ranges (`50-10`, `FRI-MON`); empty list members (`1,,2`, `,1`, `1,`);
- values outside the field (second or minute 60, hour 24, day 0 or 32, month 0 or 13, day of week 8); step 0 or above the width;
- year fields, macros (`@daily`, `@every`), `?`, `L`, `W`, `#`, `H`, `R` and any other randomization;
- embedded time-zone or environment directives (`TZ=`, `CRON_TZ=`);
- names outside month and day-of-week, and names other than the three-letter forms;
- signs, three or more digits, and any character other than `0-9 A-Z a-z * , - /`.

New Cronos features never become Runwick grammar. Cronos is not called until the expression has passed this grammar, and it only ever receives the normalized rendering of §4.1.

### 2.4 Input bounds

Before any parsing: at most **256** characters, all printable ASCII (`!`–`~`) except that ASCII space and tab separate fields. Newlines, carriage returns, other control characters and non-ASCII characters are rejected. Leading, trailing and repeated separators are allowed. At most **64** list terms in total across all fields (every field contributes at least one).

## 3. Normalization

### 3.1 Sets

Each field normalizes to a bitset of its values: names to numbers, duplicates removed, ranges and steps expanded, day-of-week 7 to 0. Five-field second is {0}. The raw text is kept for display and never hashed: formatting changes are not definition drift.

### 3.2 Restriction

A field whose set equals its whole domain is **unrestricted**, however written (`*`, `*/1`, `0-59`, `0-7` for day of week, a full list). Only the day fields' restriction changes meaning:

| Day of month | Day of week | A date matches when |
|---|---|---|
| unrestricted | unrestricted | always |
| restricted | unrestricted | its day of month is selected |
| unrestricted | restricted | its day of week is selected |
| restricted | restricted | **either** is selected (OR) |

`0 9 1 * MON` is 09:00 on the 1st of every month or on every Monday; a Monday the 1st matches once.

### 3.3 Calendar feasibility

The day-of-month branch is feasible if some selected month has some selected day, with February at 29 days. When the day-of-month branch is restricted and infeasible and day of week is unrestricted, the expression is rejected with `schedule_invalid_calendar` (`0 0 30 2 *`, `0 0 31 4,6,9,11 *`). When day of week is also restricted, the infeasible branch is dropped and the day-of-week branch alone matches (`0 0 30 2 MON` = Mondays in February).

Search exhaustion (§8) never proves infeasibility.

## 4. Cronos adapter

### 4.1 Rendering and streams

Each field renders as `*` when unrestricted, otherwise as its ascending comma list. Five-field expressions use `CronFormat.Standard`; six-field use `CronFormat.IncludeSeconds`. Streams:

- at most one day field restricted: one stream with both day fields as rendered (Cronos's AND with `*` is exactly "the restricted field controls");
- both restricted: a day-of-month stream (day of week `*`), present only if feasible, and a day-of-week stream (day of month `*`).

Example: `0 9 1 * MON` → `0 9 1 * *` and `0 9 * * 1`.

### 4.2 Nominal coordinates

Only `CronExpression.GetNextOccurrence(DateTime fromUtc, bool inclusive)` is used. Its argument and result are **nominal local wall-clock coordinates**, encoded as `DateTimeKind.Utc` because that path requires it. They are never instants and never persisted. The next nominal match is the minimum over the streams; equal results are one candidate.

Cronos's `TimeZoneInfo` path is never used. A `CronFormatException` on a rendered expression is `schedule_provider_failure`, as is any other exception from Cronos, except that an out-of-range failure when the search starts in year 9990 or later means "no candidate".

## 5. Definition identity

### 5.1 Envelope

The definition fingerprint is `SHA-256(ASCII(domain) ‖ 0x00 ‖ encode(envelope))` with the canonical encoder of 02 §7 and domain **`runwick.schedule-definition.v1`**. The envelope is:

```text
{
  "fingerprintVersion": 1,
  "recurrence": <§5.2 or §5.3>,
  "settings": <object owned by spec 13; {} in SP-015>
}
```

Spec 13 places the job template profile, misfire, overlap and bounds under `settings` *[choice]*. Members are sorted by the encoder, not by authorship order.

### 5.2 Cron recurrence

| Member | Value |
|---|---|
| `dialect` | `runwick.cron5.v1` or `runwick.cron6.v1` |
| `evaluatorProfile` | `runwick.cron.v1/cronos-0.13.0/nodatime-3.3.7` |
| `second`, `minute`, `hour`, `dayOfMonth`, `month`, `dayOfWeek` | Ascending integer arrays of the normalized sets (five-field `second` = `[0]`) |
| `gapPolicy` | `skip` or `shift` |
| `foldPolicy` | `earlier`, `later` or `both` |
| `zone` | `{ "canonicalId", "tzdbVersion", "tzdbSha256" }`: canonical IANA ID, release label, lowercase hex SHA-256 of the NZD bytes |

UTC definitions carry the same tuple (with the artifact's canonical UTC ID). The raw text and the zone ID as entered are display data, never hashed: `Asia/Calcutta` and `Asia/Kolkata` give one fingerprint.

### 5.3 One-time recurrence

`{ "dialect": "runwick.instant.v1", "dueAt": <UTC epoch milliseconds> }`. No profile, zone or artifact.

### 5.4 Vectors

| Definition | Fingerprint |
|---|---|
| `0 9 * * MON-FRI`, cron5, `Europe/Berlin`, skip, earlier, `settings = {}` | `76883d95cd2413ed1e4f9438a8d786fff8bdbc4a0f48e22391998ee0945a9fc1` |
| the same with shift, both | `63ee9ef060168a8c58d656396dcf5db4c35c8ea1f8b133bb2c30502ec4d7ec10` |
| instant, `dueAt` = 2026-07-01T09:00Z (1782896400000) | `c66d3c054d3d4a4713d4b45143931c8f1b80a774c51b6d9415d2d6e0181fffaf` |

The one-time envelope's canonical encoding (hex):

```text
0600000003030000001266696e6765727072696e7456657273696f6e040000000131030000000a726563757272656e636506000000020300000007
6469616c656374030000001272756e7769636b2e696e7374616e742e763103000000056475654174040000000d313738323839363430303030300300
00000873657474696e67730600000000
```

(one string; wrapped for width). The vectors were derived by an independent re-implementation of the 02 §7 encoder.

## 6. Time-zone data

### 6.1 Artifact

| Property | Value |
|---|---|
| Release | TZDB **2026f** (`https://nodatime.org/tzdb/tzdb2026f.nzd`; `latest.txt` pointed to it on 2026-10-31) |
| Size | 132,773 bytes |
| SHA-256 | `b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3` |
| Location | `src/Runwick/Scheduling/Evaluation/Tzdb/tzdb2026f.nzd`, embedded as `Runwick.Scheduling.Evaluation.Tzdb.tzdb2026f.nzd` |
| Provenance and licence | `Tzdb/NOTICE.md`: IANA tz data (public domain) compiled by Noda Time (Apache-2.0); stored as binary (`.gitattributes` `*.nzd binary`) |

On first use the evaluator hashes the embedded bytes, refuses any other hash, loads them with `TzdbDateTimeZoneSource.FromStream`, validates the source, and refuses a reported `TzdbVersion` other than the label. Each failure is `schedule_missing_artifact`. The provider is application-owned and immutable (`DateTimeZoneCache` over that source). `DateTimeZoneProviders.Tzdb`, machine zoneinfo and run-time downloads are never used.

### 6.2 Zone IDs

No zone means explicit `UTC`. Otherwise the ID must be an exact, case-sensitive key of the artifact's canonical-ID map (canonical IDs and aliases), at most 64 printable ASCII characters. Aliases normalize through that map; the entered ID is kept for display. Windows IDs (`W. Europe Standard Time`), machine-local shorthand (`Local`), case variants and unknown IDs are `schedule_invalid_zone`. A future Windows-ID convenience must name a territory/mapping version and persist the resulting IANA ID.

### 6.3 Profile compatibility

A stored definition names its profile, release label and hash. A process evaluates it only if all three equal its own; otherwise `schedule_missing_profile` or `schedule_missing_artifact`, which is local incompatibility (§10), never a defect of the definition. A newer package or artifact does not imply support for an older profile; upgrades are spec 13's forward cutover (SCHED-13).

## 7. DST mapping

### 7.1 Interval walk

For one lookup after cursor `c` with horizon end `h` (§8.1):

1. Enumerate the zone's constant-offset intervals intersecting `[c, h)` in UTC order; clip each to `[max(start, c), min(end, h))`.
2. Inside an interval with wall offset `o`, translate the clipped start to nominal local time and ask the adapter for the next match, inclusive if the clipped start is after `c`, exclusive if it equals `c`.
3. Convert the match with `o`. If the instant is at or past the clipped end, leave the interval. Otherwise apply §7.3 and either emit or continue.

Intervals are half-open; a transition instant belongs to the later interval. Because intervals are walked in UTC order, a cursor inside a fold finds later matches whose local label is earlier than the cursor's, with no fixed lookback.

### 7.2 Gaps (forward transitions)

A forward transition at instant `t` from offset `p` to `o > p` skips local `[t+p, t+o)`.

- **skip** (default): matches in the gap produce nothing; the interval walk never sees them, since their instants fall outside both intervals.
- **shift**: if the adapter's first match at or after `t+p` is before `t+o`, emit **one** occurrence at `t` (local `t+o`), labeled gap-shifted with the skipped range. If the adapter also matches `t+o` exactly, that normal match is the same occurrence (`coalesced`). Two adapter queries, however many local times were skipped; the exact number of collapsed nominal matches is not computed.

### 7.3 Folds (backward transitions)

A local time with two mappings (`MapLocal` count 2) is in a fold. Earlier and later mean UTC order, not standard/daylight names.

| Policy | Earlier mapping | Later mapping |
|---|---|---|
| **earlier** (default) | emitted | excluded: jump to the end of the repeated local range |
| **later** | excluded: leave the interval (its tail is all fold) | emitted |
| **both** | emitted | emitted |

An emitted fold occurrence is labeled fold-earlier or fold-later. An excluded side is jumped, never scanned candidate by candidate.

### 7.4 Output

Occurrences are strictly increasing, distinct UTC instants strictly after the cursor, at the 02 §6 millisecond identity. Offsets with seconds are exact (a five-field match need not land on UTC second zero). Range conversions are checked (§8.1); nothing is rounded to a different occurrence.

## 8. Bounds

### 8.1 Horizon and range

Each lookup searches at most **8 calendar years + 2 days** after its cursor (so February 29 after 2096 finds 2104). The supported instant range is 0001-01-02T00:00Z to 9999-12-30T00:00Z *[choice]*. A cursor outside it, or a horizon clipped by its end with no match, is `schedule_range_overflow`. No match within an unclipped horizon is `schedule_no_match_within_horizon`.

### 8.2 Steps and time

A plan (one or more consecutive lookups) shares one budget:

- **4,096 steps**. One step = one zone interval visited, one Cronos call per stream, or one `MapLocal` call. Exceeding it is `schedule_step_limit_exceeded`.
- **250 ms** cooperative time for materialization plans; **1 s** for registration, edit and preview. Checked at every step and once more before the plan is returned. Exceeding it is `schedule_budget_elapsed`.

The deadline is cooperative: a synchronous Cronos or NodaTime call is not preempted, and this spec does not claim a hard CPU deadline.

### 8.3 One evaluator task per group

Each scheduler/store group owns one evaluator slot. Work runs synchronously inside it; the slot is released only when the work returns. A caller whose budget elapsed, or who is cancelled, abandons only its wait; no replacement task is started while a library call is running *[choice]*.

## 9. Preview

Preview returns the next **10** occurrences after a cursor under the registration budget. Each row has the UTC epoch milliseconds, the UTC instant (ISO-8601 extended), the local wall-clock time and offset at it, a label (`exact`, `fold-earlier`, `fold-later`, `gap-shifted`) and, for a gap shift, the skipped local range `from/to`.

## 10. Errors

| Wire name | Code | Class | Caller action |
|---|---|---|---|
| `schedule_invalid_grammar` | InvalidGrammar | Invalid | Reject registration/edit |
| `schedule_invalid_calendar` | InvalidCalendar | Invalid | Reject registration/edit |
| `schedule_invalid_zone` | InvalidZone | Invalid | Reject registration/edit |
| `schedule_no_match_within_horizon` | NoMatchWithinHorizon | Deterministic | Under the definition's own profile: `needs_review` (spec 13) |
| `schedule_step_limit_exceeded` | StepLimitExceeded | Deterministic | As above |
| `schedule_range_overflow` | RangeOverflow | Deterministic | As above |
| `schedule_missing_profile` | MissingProfile | LocalIncompatibility | Report locally; do not touch the shared definition |
| `schedule_missing_artifact` | MissingArtifact | LocalIncompatibility | As above |
| `schedule_budget_elapsed` | BudgetElapsed | LocalPressure | Defer with backoff; never invalidate |
| `schedule_provider_failure` | ProviderFailure | Transient | Retry later; never invalidate |

### 10.1 Rules

- Grammar and calendar validation happen before any library call.
- Only Deterministic errors may justify `needs_review`, and only when established under the definition's own profile and artifact.
- An elapsed budget is local pressure, not evidence about the definition.
- A database or transport failure is not an evaluator error and never invalidates a definition.
- Error messages are bounded and echo at most one token of input.

## 11. Implementation map

| Concern | Type (`Runwick.Scheduling.Evaluation`, internal) |
|---|---|
| Grammar, normalization, feasibility | `CronGrammar`, `CronDialect`, `NormalizedCron` |
| Cronos streams | `CronosAdapter` |
| Identity and fingerprint | `ScheduleDefinition`, `CronSchedule`, `GapPolicy`, `FoldPolicy` |
| Artifact | `TzdbArtifact` |
| Zones | `ZoneMapper`, `ResolvedZone` |
| Planner and budget | `RecurrencePlanner`, `PlannerLimits`, `EvaluationBudget`, `Occurrence`, `OccurrenceKind` |
| Preview | `PreviewItem` |
| Task bound | `EvaluatorSlot` |
| Errors | `EvaluationErrorCode`, `EvaluationErrorClass`, `ScheduleEvaluationException` |

## 12. Normative vectors

Every vector runs as a Pure test (`Category=Pure`, `Item=SP-015`, `Case=<ID>`) under the pinned 2026f artifact. Expected instants are UTC. Installed system zoneinfo may cross-check familiar examples (it confirmed the Berlin, Lord Howe, Apia, São Paulo and Monrovia transitions below under its 2026e data) but cannot certify the pinned artifact.

### 12.1 SCHED cases

| Case | Definition | After | Expected |
|---|---|---|---|
| SCHED-01 | Grammar | — | §2–§3 hold: five/six-field formats, month and day names, Sunday 7, steps and normalized sets; every §2.3 rejection is `schedule_invalid_grammar` from the Runwick grammar; equivalent text (`0 9 * * MON-FRI`, `0 09 * * 1-5`, `0\t9 * * 5,4,3,2,1`) has one fingerprint |
| SCHED-02 | `0 9 1 * MON`, UTC | 2026-06-30T23:59:59Z | 2026-07-01T09:00Z, 2026-07-06T09:00Z (then 07-13, 07-20, 07-27, 08-01, 08-03) |
| SCHED-02 | same | 2026-05-31T23:59:59Z | 2026-06-01T09:00Z once (a Monday the 1st), then 2026-06-08T09:00Z |
| SCHED-03 | `0 0 30 2 *` | — | `schedule_invalid_calendar`; `0 0 30 2 MON` is valid (Mondays in February) |
| SCHED-03 | `0 0 29 2 *`, UTC | 2096-02-29T00:00Z | 2104-02-29T00:00Z (2100 is not a leap year) |
| SCHED-04 | `30 2 * * *`, Europe/Berlin, skip | 2026-03-28T01:30Z | 2026-03-30T00:30Z (none on 2026-03-29) |
| SCHED-04 | same, shift | 2026-03-28T01:30Z | 2026-03-29T01:00Z (03:00 +02, gap-shifted, skipped 02:00/03:00), then 2026-03-30T00:30Z |
| SCHED-05 | `30 2 * * *`, Europe/Berlin | 2026-10-24T12:00Z | earlier: 2026-10-25T00:30Z; later: 2026-10-25T01:30Z; both: 00:30Z then 01:30Z; next 2026-10-26T01:30Z |
| SCHED-06 | `* 2 * * *`, Europe/Berlin, both | 2026-10-24T23:59:59Z | 120 instants 2026-10-25T00:00Z … 01:59Z, one minute apart; first 60 fold-earlier, last 60 fold-later |
| SCHED-06 | same | 2026-10-25T00:59Z | 2026-10-25T01:00Z (both or later); with earlier: 2026-10-26T01:00Z, the later side jumped |
| SCHED-07 | `*/30 2-3 * * *`, Europe/Berlin, shift | 2026-03-28T23:00Z | 2026-03-29T01:00Z once (gap-shifted, coalesced with the 03:00 match), then 01:30Z |
| SCHED-07 | `* 2 * * *`, Europe/Berlin, shift | 2026-03-28T23:00Z | 2026-03-29T01:00Z, then 2026-03-30T00:00Z, in at most 16 steps for both lookups |
| SCHED-08 | `45 1 * * *`, Australia/Lord_Howe | 2026-04-04T00:00Z | 30-min fold: earlier 2026-04-04T14:45Z; later 15:15Z; both both |
| SCHED-08 | `15 2 * * *`, Australia/Lord_Howe | 2026-10-03T00:00Z | 30-min gap: skip 2026-10-04T15:15Z; shift 2026-10-03T15:30Z then 2026-10-04T15:15Z |
| SCHED-08 | `0 12 * * *`, Pacific/Apia | 2011-12-29T00:00Z | skipped day: skip 2011-12-29T22:00Z, 2011-12-30T22:00Z; shift adds 2011-12-30T10:00Z (local 12-31T00:00, skipped 12-30T00:00/12-31T00:00, +14) |
| SCHED-08 | `0 0 * * *`, America/Sao_Paulo | 2018-11-03T00:00Z | midnight gap: skip 2018-11-03T03:00Z, 2018-11-05T02:00Z; shift adds 2018-11-04T03:00Z |
| SCHED-08 | `30 23 * * *` both / `0 0 * * *`, America/Sao_Paulo | 2019-02-16T12:00Z | midnight fold: 2019-02-17T01:30Z, 02:30Z / 2019-02-17T03:00Z (half-open boundary) |
| SCHED-08 | `0 12 * * *` / `30 0 * * *`, Africa/Monrovia | 1971-06-01 / 1972-01-06T12:00Z | second-valued offset −00:44:30: 1971-06-01T12:44:30Z; midnight gap: skip 1972-01-08T00:30Z, shift 1972-01-07T00:44:30Z first |
| SCHED-08 | `*/15 * * * *` and cron6 `* * * * * *`, Europe/Berlin, all policies | across both 2026 transitions | strictly increasing, whole-second instants; cron6 shift: 00:59:59Z, 01:00:00Z, 01:00:01Z |
| SCHED-14 | Bounds | — | 256 characters and 64 terms accepted, one more rejected; `* * * * *` plans 1,000 occurrences and fails a 4,096-occurrence plan with `schedule_step_limit_exceeded`; February 29 after 2096 in ≤ 4 steps; ten dense cron6 lookups across the fold in ≤ 48 steps; a 7-year horizon gives `schedule_no_match_within_horizon`; an elapsed clock gives `schedule_budget_elapsed` (LocalPressure); 9996-03-01 and `long.MaxValue` give `schedule_range_overflow`; the evaluator slot never runs two works at once and keeps its permit until the work returns |

### 12.2 Supplementary cases *[choice]*

| Case | Expected |
|---|---|
| EVAL-01 | No zone = explicit UTC (same canonical ID and fingerprint as `Etc/UTC`); `Asia/Calcutta`→`Asia/Kolkata`, `US/Pacific`→`America/Los_Angeles`, `Europe/Kiev`→`Europe/Kyiv` with one fingerprint per pair; case variants, Windows IDs, `Local`, empty, padded and unknown IDs are `schedule_invalid_zone` |
| EVAL-02 | The pinned artifact reports 2026f and the §6.1 hash; altered bytes or a different expected label are `schedule_missing_artifact`; a stored definition naming another profile, label or hash is local incompatibility |
| EVAL-03 | The §5.4 golden vectors; every hashed member (zone, either policy, dialect, a field, `settings`) changes the fingerprint; raw text and entered zone ID do not |

### 12.3 Must not claim

- A hard CPU deadline on a synchronous library call.
- System-zoneinfo results as certification of the pinned artifact.
- Any storage, materialization or misfire behavior (spec 13).
