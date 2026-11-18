## Outbox relay at four slots: batched marks, claim batches and slot release (D2)

Written 2026-11-17. Informational: every number was measured on the shared host with other sessions' test suites running (load average 15 to 70 on 8 cores), so absolute rates are a fraction of what a quiet host gives. The writer could not even stage the offered 100 messages/s in most runs. Read the pairs as direction, not as a pass or fail of P-09.

### Where the time goes at four slots

A message's life at four slots is: claim, destination commit, source mark. Little's law then gives the ceiling: throughput is at most the slots divided by that life. A traced run on the unchanged code (host load about 44, so the times are inflated) showed:

| Step | Calls | Mean | Notes |
|---|---:|---:|---|
| Source claim | 662 | 88.5 ms | 1 to 2 messages per claim: a claim fires each time a slot frees |
| Destination `ReceiveOutbox` | 757 | 281 ms | of which commit 196 ms |
| Source delivered mark | 756 | 83 ms | 26 ms of it waiting for the control connection, 57 ms the statement and commit |
| Lease renewal | 18 | 89 ms | waited 123 ms behind marks for the same connection |

Two things stand out. Every mark, renewal and transport record shares one source connection, so marks queue behind each other. And because a claim fires as soon as one slot frees, a claim carries about one message, so the claim connection is busy with a call per delivery. In the earlier D2 sixteen-slot pairs the same effect was hidden by the extra slots.

### What changed (all opt-in, defaults unchanged)

- **`OutboxRelayOptions.BatchDeliveredMarks`.** On the PostgreSQL source, delivered marks that queue behind each other are written in one `UPDATE … FROM unnest(…)` and one commit (up to 64). Each row is still guarded by its own lease token, so a stale token leaves that row alone and reports false for that message only. A lone mark runs the original single-row statement. A caller that gives up before its mark is taken writes nothing; a caller whose deadline ends while it holds the connection leaves its own mark unknown, as for a single mark, and the others wait for the next turn.
- **`ClaimBatchMin` and `ClaimBatchWait`.** With messages in flight, the claim waits until that many slots are free or the wait (5 ms by default) ends, so one claim carries a batch.
- **`ReleaseSlotAfterReply`.** A message stops counting against `MaxInFlight` once the destination has answered, so the next claim and send overlap its mark. Its lease is still held and renewed until the mark is written, shutdown still waits, and at most twice `MaxInFlight` messages hold leases.

Transport identity, receipts, the token-guarded mark, mixed outcomes, lost marks and reclaims are unchanged.

### A bug the measurement found

The first version of the claim wait recomputed the remaining time after the loop test. When the wait expired between the two, the call received a negative timeout, threw, and ended the claim loop for good. One run delivered for 71 seconds and then nothing. The wait is now computed once and the loop breaks when no time remains; a test with a 1 ms wait delivers 40 messages. The affected run is excluded below and was repeated.

### Paired runs

P-09 at four slots, 100 offers/s, 1 minute of warm-up, 3 minutes of measurement, no mark loss, two minutes of drain (the backlog never drained in any arm, so ready-to-confirm latencies are minutes long and mean nothing here). Arms rotated each round. **base** is the current code; **full** is batched marks, claim batches of 2, slot release and destination groups of 4; **marks** is batched marks plus slot release only. Confirmed deliveries per second in the measurement window:

| Round | base | full | full ÷ base | marks | marks ÷ base |
|---|---:|---:|---:|---:|---:|
| 1 | 6.74 | 8.25 | 1.22 | 8.78 | 1.30 |
| 2 | 8.23 | 10.74 | 1.30 | 11.26 | 1.37 |
| 3 | 9.46 | 15.55 | 1.64 | 7.03 | 0.74 |
| 4 | 13.16 | 18.38 | 1.40 | | |
| 5 | 15.82 | 20.06 | 1.27 | | |
| 6 | 13.58 | 17.19 | 1.27 | | |
| 7 | 15.06 | 17.36 | 1.15 | | |
| 8 | 21.77 | 13.39 | 0.62 | | |

Across the eight valid pairs the median ratio for **full** is 1.27 (range 0.62 to 1.64); it won seven of eight. The marks-only arm is mixed (1.30, 1.37, 0.74) on three pairs. The relay made about 40% fewer source claims with **full** (median 2,850 against 4,720 over all runs), and 2,300 to 3,500 marks went out in 840 to 1,260 statements (2.6 to 2.8 per statement). No run left a second mapping for any message and none recorded a correctness violation.

The one pair where **full** lost (round 8) had the writer stage 64 messages/s against 103/s for its base, so the two arms did not see the same load.

### Reading

- Batching helps modestly. About a quarter more deliveries per second at the median is what the pairs support. It does not get anywhere near 100 deliveries/s at four slots on this host, where even the base arm only reached 7 to 22.
- By the slots-over-life arithmetic, four slots cannot reach 100/s unless a message's whole life is under 40 ms. A claim and a destination commit alone are about 25 plus 35 ms on a quiet host. Meeting P-09 at four slots needs the next claim to overlap the current send, which `ReleaseSlotAfterReply` starts to allow, or more slots. The earlier slot sweep reached 100/s at eight slots.
- P-09 at four slots is **not met** and was not expected to be on this host.

### Tests

New `BatchedMarkTests` (4): queued marks share statements and keep each lease token (11 of 12 applied, one stale token left leased, then settled by its owner); a lone mark uses the single-row statement and a cancelled caller writes nothing; a 1 ms claim wait never ends the claim loop; a relay with claim batches, destination groups, slot release and batched marks delivers 24 messages exactly once and every slot is counted back. See the final message of the task for full-project counts.

### Not done

- A quiet-host repeat. The decision to make any of these the default needs it.
- Batching the SQLite source's marks, and batching renewals.
- Counting messages per claim directly (claims and marks per statement are inferred from counters).
- Measuring 8 and 16 slots with the new options.

Raw files: `artifacts/perf/round-two/outbox-d2/` in the worktree (gitignored); the pair scripts are `pairs.sh`, `pairs2.sh` and `pairs3.sh` beside them.
