## Redis grouped dispatch and the BullMQ comparison (work area F)

Written 2026-11-15. Informational: every number below was measured on the shared host, with the full SP-029 matrix and other agents' test suites running at the same time (load average 20 to 60 on 8 cores). Benchmarks ran one at a time under a shared lock, alternating arms inside each round, so each pair saw the same neighbours. They still carry host noise. They do not replace a quiet-host run.

### What was built

An opt-in way to send several plain dispatches to Redis in one call (`RedisOptions.GroupDispatches`, default off, with `MaxGroupItems`, `MaxGroupBytes` and `MaxGroupsInFlight`). It is the F3 proof wired into the client.

- A caller that finds a free send slot goes out alone at once, as before. A caller that arrives while every slot is busy waits in a small bounded queue and goes out with the others waiting, when the next slot frees. Nothing waits on a timer. If the queue is full, or one dispatch is larger than a group may carry, that dispatch goes alone.
- Each member keeps its own command ID, fingerprint, deadline, receipt and event. Resending one later, alone or in another group, returns its original job. Delayed dispatches can be grouped. Dispatches with uniqueness keys, concurrency scopes, rate scopes or schedule headers never are.
- The Lua script decides each member alone. When it cannot group a set (repeated command ID, an unsupported shape, too many keys or writes, too large, or slow to decide) it says `group_fallback` before writing anything, and the client sends each member alone with its original ID.
- A lost reply gives every member an "outcome unknown" for its own command, to resolve or resend as before. A caller cancelled while waiting leaves nothing behind. A caller cancelled after the group was sent gets the real outcome. Shutdown fails never-sent members as not applied.
- No layout revision, no manifest change. Groups write the ordinary job, receipt and event rows.

Tests: 10 new (`RedisGroupedDispatchTests`) cover concurrent grouping with one event per command, lone callers never grouping, delayed members, queued cancel, cancel after send, a lost group reply, every write prefix of a group (the injected-failure sweep, 3 members, until a run completes) with recovery and resend, a declined group, a mixed group (replay, fresh, expired) and a kill and restart of Redis with 160 grouped dispatches all confirmed afterwards. The whole Redis project passes 86 of 86 (1.49 hours, including the kill-and-restart cases). The Contract project passes 480 of 480.

### Does it help? Mostly no, on this host

Five alternating pairs at 16 producer calls, 10,000 one-kilobyte dispatches each (median):

| Arm | Jobs/s | Enqueue p95 / p99 (ms) |
|---|---|---|
| Runwick, grouping off | 432 | 75 / 142 |
| Runwick, grouping on (2 slots, groups of about 6) | 360 | 97 / 191 |
| BullMQ 6.7.2 | 897 | 40 / 86 |

Grouping on was 0.81× of grouping off (paired ratios 0.79 to 1.03). Runwick without grouping was 0.49× of BullMQ.

Three alternating pairs at 64 producer calls:

| Arm | Jobs/s | Enqueue p99 (ms) |
|---|---|---|
| Runwick, grouping off | 684 | 277 |
| Grouping on, 2 slots (groups of 8) | 559 | 409 |
| Grouping on, 4 slots | 532 | 452 |
| BullMQ | 2,491 | 81 |

Three more pairs with more sends in flight (64 callers): off 705 jobs/s, 8 slots 761 (paired median 1.19×, pairs 1.19, 0.78, 1.24), 16 slots of 4 items 718 (1.14×). So grouping can break even or gain a little when enough sends are in flight, but the effect sits inside this host's noise.

### Why: the script costs too much per job, not the number of calls

- Redis reports 1,213 µs per single dispatch script call against 157 µs for a BullMQ add (same run, same host). Redis spent 12.7 s of CPU on 10,500 Runwick dispatches and 2.0 s on 10,500 BullMQ adds.
- With phase marks inside the real script (single dispatch, microseconds): reading the application and checking the gates 146, deciding 184, preparing events 90, ordering the writes 170, packing the intent 69, writing the guard 41, writing the data 218; about 1,000 in total. A group of 6 costs about 650 to 700 per job: the fixed part is shared, the rest is per job.
- Without the .NET client, over plain sockets, eight connections get about 280 to 350 jobs/s with single dispatches and about 900 to 920 jobs/s with groups of 8 (script time 1.3 to 1.9 ms per single call, 2.5 to 2.6 ms per group of 8). With `appendfsync always`, one fsync covers everything in an event-loop pass, so the number of jobs in flight decides throughput. That is why grouping can triple the raw ceiling.
- Through the .NET client the gain disappears. Grouping lowers the jobs in flight when few sends are allowed (2 slots of 8 is 16 jobs in flight against 64 callers sending alone), and it adds a queue hop, one reply parse per group and a thread-pool switch. With enough slots it roughly breaks even.

The 90% of BullMQ target needs script work per job to fall several times, not only fewer calls. That means changing what the dispatch writes (per-job events, receipts, indexes and the stored intent that is then written again), which is a protocol change, not a client change.

### Steady, delayed and drain workloads

Three alternating pairs each, 16 callers and workers, 1 KiB payloads.

- **Steady 500 offers/s.** Both systems are overloaded. Runwick accepted a median 256 jobs/s (7,331 refused locally of 15,000), grouping on 201, BullMQ 441 (1,775 refused). Runwick's due-to-handler p99 was 31.7 s (backlog: workers finish about 160 jobs/s) against BullMQ's offer-to-finish p99 of 0.5 s. This is a worker and finish-path limit, not a dispatch limit.
- **Delayed 100 offers/s, 5 s ahead.** Throughput is at parity: 100.0 against 96.6 jobs/s (BullMQ refused 102 locally, Runwick none). Enqueue p99: Runwick 233 ms, grouping on 265, BullMQ 290. Due-to-handler p95/p99: Runwick 494/689 ms, grouping on 888/1,348, BullMQ 691/864. Paired Runwick/BullMQ p99 ratios were 0.74, 1.71 and 1.43 (median 1.43), better than the earlier five-pair run (3.43×) but still outside the 10% band. Grouping did not help this workload.
- **Drain of 20,000 pre-seeded jobs, 16 workers.** Runwick 159 jobs/s, BullMQ 664 jobs/s (0.23×, pairs 0.25, 0.22, 0.23). Grouping does not apply: this is the claim and finish path.

### Verdict against the targets

Not met. At matched `appendfsync always`, saturated enqueue is 0.28× to 0.49× of BullMQ, drain 0.23×, steady 500 is 0.59× accepted, and delayed pickup p99 is 1.4× BullMQ's. Throughput at 100 delayed offers/s meets the 90% band. Grouped dispatch is correct and tested, but it is not a reason to turn on by default, and it should not be described as a way to reach the BullMQ target.

### Work hints, pipeline depth and intent encoding

- **Pipeline depth.** The right number of sends in flight depends on the fsync time and the caller count, not on a fixed small value. With 4 slots or fewer the grouped path under-fills Redis; with 8 to 16 it breaks even. Left at 2 by default because it is opt-in and conservative.
- **Intent encoding.** The script stores each command's after-images as a packed intent, then writes the same data a second time as it projects. Packing costs about 70 µs per job and the guard write 40 µs. Cutting both needs the recovery model to change; not attempted.
- **Ordering writes.** The bytewise comparator used to order hash fields costs about 80 to 110 µs per job. Replacing it with Lua's built-in string order saves about 12% of a single dispatch (992 to 876 µs) and 21% of a grouped one (697 to 551 µs per job), but built-in order follows the server's locale, so it is only safe if the script proves a bytewise locale or keeps its own comparator. Not applied.
- **Work hints.** The due-to-handler gap on delayed jobs comes from the worker's idle polling and its snapshot of ready work (up to two seconds old), which is shared engine behaviour across stores, not Redis script time. Not changed here.

### Evidence

Runner, raw results, the profile script and the raw-socket test are under `artifacts/perf/round-two/redis-f-grouped/` in the worktree for this branch (git ignores artifacts). Commands: `pairs.sh <workload> <rounds> [arms]`, `profile.py`, `rawthroughput.py`.
