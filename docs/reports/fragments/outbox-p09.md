# Outbox D: the relay at four slots (P-09)

Written 2026-11-20. Every number here was measured on the shared host (8 cores) while other sessions ran their own work; the load average during the measure windows was 7 to 29. PostgreSQL 18 ran in a local container with fsync, synchronous commit and full-page writes on, with source and destination as separate databases on the same server. This is not the reference host.

## Short answer

The relay was mostly waiting, not doing database work. At four slots a freed slot sat idle until the claim loop came round, the claim loop made one source claim per freed slot, and each claim read and sorted the whole backlog, so the slower it got the more backlog there was. A slot that did get a message then held it through the destination commit and the source mark.

The fix has four parts: the relay now claims ahead (claimed messages wait beside busy slots, so a freed slot starts its next message at once), the source claim walks the indexes in order instead of sorting the backlog, a slot carries one destination call that can hold a group of up to four messages, and slot release on reply and batched marks are on by default. No schema change and no migration.

**P-09 at four slots is met on this host at load average 7 to 13**: 97.6 to 99.8 confirmed deliveries/s against the offered 100/s (every staged message confirmed in the window), ready-to-confirm p99 1.13 to 1.41 s, no backlog. On the same host main confirmed 26 to 54/s and its backlog grew by 50 to 75 messages a second. Under heavier load (average 16 to 29) the new relay still kept up with the offered rate, but p99 was 2.1 to 2.3 s, just over the 2 s line. The paired tables below give both.

## Where the time goes

Traced P-09 runs on main (4 slots, 100 offers/s, 2-minute measure window; the relay's own timers plus `pg_stat_statements` with nested tracking). Load average 18 for the first run, 23 to 47 for the second.

| Step, per delivery | Main, load 18 | Main, load 23 to 47 |
|---|---:|---:|
| Confirmed deliveries/s | 51.5 | 32.7 |
| Source claim (round trip) | 24.7 ms, 1.43 messages per claim | 34.8 ms, 1.37 messages per claim |
| of which statement time in PostgreSQL | 13.4 ms, 2,047 buffers read per claim | 17.7 ms, 1,817 buffers per claim |
| Claim loop busy | 89% of the window | 83% of the window |
| Build the dispatch command (import, checks, fingerprint) | under 1 ms | 6.1 ms mean, 0.26 ms median |
| Destination `ReceiveOutbox` round trip | 31.3 ms (function 16.6, commit 14.4) | 50.8 ms (function 29.7, commit 18.0) |
| Source delivered mark | 10.9 ms (1.9 ms waiting for the connection) | 18.9 ms (4.9 ms waiting) |
| Time a message holds its slot | about 43 ms | 75.8 ms |
| Share of slot time in use | about 55% | 62% |

Reading it against the budget of 40 ms per delivery per slot:

- **Slots were idle 38% to 45% of the time.** A slot freed when its mark finished, then waited for the single claim loop, which was busy 83% to 89% of the time making claims of one or two messages. That is waiting, not work.
- **Each claim read the whole backlog.** The claim's `WHERE (state = 'pending' AND …) OR (state = 'leased' AND …) ORDER BY next_attempt_at` could not use either partial index in order, so PostgreSQL read every due row and sorted it. With a backlog of a few thousand rows that was about 2,000 buffers and 13 to 18 ms per claim, growing with the backlog: a feedback loop that turned a small shortfall into minutes of delay.
- **A busy slot took 43 to 76 ms per message**, already over the 40 ms budget on its own, because it held the slot through the destination commit (14 to 18 ms of fsync wait) and the source mark (11 to 19 ms).
- The database work itself was small: the destination function took 12 to 17 ms of PostgreSQL time per message and the mark 0.4 to 0.5 ms. Poll waits played no part on main, because there was always a backlog.

## What changed and why it is safe

1. **Claim ahead.** `MaxInFlight` now counts slots, that is destination calls in progress at once. The relay keeps up to twice what the slots carry leased (8 messages with a destination that cannot group, 32 with groups of four), counting messages that are waiting for their mark. Claimed messages wait in order, leased and renewed, and a slot that frees starts the next one at once. The claim asks for the room left under that cap, so claims grew from 1.4 to about 3.7 to 4.3 messages each. A busy relay whose claim came back short re-claims when a lease ends instead of waiting out the 1 s idle poll; an idle relay polls exactly as before.
2. **Index-ordered claims.** The PostgreSQL claim reads `pending` rows in `next_attempt_at` order and expired leases in `lease_expires_at` order, each from its own partial index, with the clock read once as an index bound, and leases the oldest `max` of the two. It still locks with `FOR UPDATE SKIP LOCKED`. Claim statement time fell from 13 to 18 ms to 0.7 to 1.3 ms, and buffers per claim from about 2,000 to 33 to 72. SQLite uses the same two ordered branches. The existing indexes serve both, so there is no schema change and no migration.
3. **A slot carries one destination call, and groups of four are the default.** With a PostgreSQL destination a freed slot takes up to `DeliveryGroupSize` waiting plain messages (now 4 by default) as one destination transaction. Groups now form from the waiting messages, so they averaged 3.7 to 4 messages instead of the 2.3 to 2.9 that groups from a single claim managed. SQLite and Redis destinations ignore the setting, as before.
4. **`ReleaseSlotAfterReply` and `BatchDeliveredMarks` are on by default.** With claim ahead, slots turn over faster than one mark statement per message can follow on the single control connection. Marks averaged about 3 per statement (4.6 in the batched statements, single statements when only one mark was waiting), and releasing the slot on the destination's reply keeps the mark off the slot. The paired runs show each step helps (see the tables).
5. A renewal that races its own message's mark (the mark won) is no longer counted as a lease loss. Earlier runs counted 20 to 260 of these; the traced runs of the final code count 0.

Why the safety rules hold:

- **Token-guarded confirmation.** Unchanged. Each message is marked under its own lease token; a batched mark guards every row with its own token.
- **Transport identity and receipts.** Unchanged. Each message keeps the transport command assigned at claim, and a group sends each command with its own ID, receipt and mapping. A waiting message checks its lease lifetime before its first send, like any other.
- **Leases.** Waiting messages are in the relay's in-flight set, so they are renewed and shutdown waits for them. After the host deadline nothing new starts and their leases expire and are reclaimed.
- **Lost replies, reclaims, mixed outcomes.** Handled by the same code as before: a lost destination reply resends the same command, a lost mark is repaired by the delivery mapping, a refused group falls back to single sends with the same commands.
- **No duplicate effects.** Every paired run reconciled every staged message to exactly one delivery mapping and one destination job, with no second mapping and no mark mismatch.

## Tests

New tests in `tests/Runwick.Outbox.Tests/ClaimAheadTests.cs` (7 runs, both source adapters where it applies):

- claimed messages wait for a slot: never more than 2 sends at once at `MaxInFlight = 2`, never more than 4 leases, and at least one message did wait; 30 messages, 30 jobs, 30 mappings;
- a relay that dies with two sends hung and two claimed messages waiting: nothing is marked, a new relay takes the pending ones at once and the four leased ones after their leases expire, each delivered once with one mapping;
- mixed outcomes in one claim: one malformed message blocks, two destination replies are lost after commit (resent and replayed), one mark fails before the write and one after it; 7 jobs, 7 mappings and exactly 7 receipts (resends and reclaims reused each message's transport command);
- with groups on, a slot carries one destination group: at most 2 destination calls at once at `MaxInFlight = 2`, groups of 3 or 4 formed from waiting messages, 24 jobs and 24 mappings.

OUTBOX-07 now checks in-flight and claim batch against the relay's lease cap instead of 4.

Results:

Not yet run on the final code in this session (pending: Outbox, Core, Contract, full PostgreSQL suite). Earlier on this branch: the relay subset passed 41/41 and the grouped-slot test 1/1; the full Outbox project ran 63 tests with 2 failures, both Redis-store `SourceCleanupTests` that need a Redis server and none was running.

## Benchmarks

P-09 workload (`tests/Runwick.Workloads`, `run P-09`): 8 business writers staging Poisson arrivals at 100/s with 1 KiB intents, 1 minute of warm-up, 3 minutes of measure, no injected mark loss in the timing runs, 3 minutes of drain, a fresh database pair per run. Every run held `flock /tmp/runwick-bench.lock`. Arms ran one after another in each round, order rotated between rounds. Load is the 1-minute load average at the start of the measure window and the mean of samples every 10 s during it. Note that the workload's relay wraps the source in a fault-injection adapter that has no wake listener, so every arm polls the source instead of being woken by notifications; a production PostgreSQL source would be woken.

### First series, 4 slots: which part helps

`main` is the current main; `claim ahead` is claim ahead, index-ordered claims, slot release and batched marks, sending each message alone; `claim ahead, old options` is the same with slot release and batched marks off; `+ groups of 4` adds groups of four. (This series ran before the last fix, which re-claims on lease end instead of waiting out the idle poll.)

| Round (load) | Arm | Confirmed/s | p50 | p95 | p99 | Backlog |
|---|---|---:|---:|---:|---:|---|
| 1 (23 to 29) | main | 25.8 | 229 s | 271 s | not reached | grew, 10,210 unconfirmed at end |
| | claim ahead, old options | 58.2 | 94 s | 107 s | 110 s | grew, drained after the writers stopped |
| | claim ahead | 72.9 | 22 s | 62 s | 64 s | grew, drained |
| | + groups of 4 | 99.6 | 0.87 s | 1.81 s | 2.29 s | none |
| 2 (19 to 26) | main | 42.3 | 150 s | 247 s | not reached | grew, 7,093 unconfirmed at end |
| | claim ahead, old options | 50.8 | 144 s | 161 s | 161 s | grew, drained |
| | claim ahead | 84.2 | 33 s | 39 s | 40 s | grew, drained |
| | + groups of 4 | 95.8 (staged 95.9) | 0.83 s | 1.68 s | 2.22 s | none |
| 3 (11 to 22) | main | 53.6 | 101 s | 160 s | 161 s | grew, drained |
| | claim ahead, old options | 72.7 | 50 s | 85 s | 85 s | grew, drained |
| | claim ahead | 102.0 | 14 s | 25 s | 26 s | grew early, drained |
| | + groups of 4 | 96.6 (staged 96.3) | 0.78 s | 1.62 s | 2.12 s | none |

### Second series, 4 slots: main against the final code

Arm order rotated. Round 13 and the 16-slot pairs were still running when this was written.

| Round (load) | Arm | Confirmed/s | p50 | p95 | p99 | Backlog |
|---|---|---:|---:|---:|---:|---|
| 11 | final (load 9.3, mean 10.0) | 99.8 (staged 99.7) | 0.32 s | 1.00 s | 1.13 s | none |
| | main (load 19.5, mean 18.5) | 49.8 | 141 s | 178 s | 178 s | grew, drained |
| 12 | final (load 10.2, mean 12.0) | 99.7 (staged 99.8) | 0.17 s | 0.94 s | 1.17 s | none |
| | main (load 7.0, mean 10.0) | 78.4 | 14.8 s | 49 s | 50 s | grew, drained |

Every run: 0 violations, no second mappings, destination jobs equal to mappings; final code 0 lease losses.

## What's left

- **The 2 s tail under heavy load.** With the host at load 16 to 29 the p99 sat at 2.1 to 2.3 s although throughput kept up. The traced runs put the remaining time in destination commits (fsync wait of 20 to 50 ms per group on this host) and in the mark connection; a quiet or reference host should be checked before calling P-09 met in general.
- **A reference-host run** with separate source and destination servers, as 14 §9.4 describes, and the full 15-minute measure with three repetitions.
- **SQLite was not measured.** The P-09 workload has only a PostgreSQL source. The SQLite source gets the same relay changes and the same ordered claim, but the SQLite destination does not group, so a slot there is one message; whether that meets 100/s is untested.
- **The workload's relay has no wake listener** (its fault-injection wrapper does not forward it), so these numbers are for a polling relay. Forwarding it would make the workload match a production PostgreSQL source.
- Redis destinations still send each message alone.

Raw results: the run directories under the session scratchpad (`runs/s4-r*`, `runs/s16-r*`, `runs/*-trace*`), each with `out/result.json`, the relay summary, load samples and a `pg_stat_statements` snapshot for its measure window.
