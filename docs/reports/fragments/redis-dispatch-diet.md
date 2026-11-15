## Redis dispatch script diet (work area F follow-up)

Written 2026-11-15. Informational: measured on the shared host while other agents' test suites ran (load average well above the core count). Benchmarks ran one at a time under the shared bench lock, with arms alternating inside each round. Host noise is large, so read the end-to-end numbers as direction, not as exact gains.

### What was asked

The Redis F report found the dispatch script costs about 1.2 ms per job inside Redis, against 0.16 ms for a BullMQ add. This follow-up looks for script work that the guarantees (receipt, event, uniqueness, generation, fences) do not need, without changing what is stored, what is replied, or what a receipt says.

### What changed (3598d4c)

All changes keep the stored data and the replies identical. No layout change, so no migration.

- **Fields are written in order, not sorted.** The script wrote each hash's fields in bytewise order, which meant sorting them at the end of every dispatch. A new job hash has 28 fields and a receipt has 11. They are now written from fixed lists that are already in bytewise order (`U.JOB_FIELDS`, `RECEIPT_FIELDS`, through a new `U.hset_sorted`). Any later write to the same hash in the same command drops the shortcut and falls back to sorting, so a hash that was changed twice is still ordered correctly.
- **The event row is built directly.** Its eight keys are fixed, so the JSON text is assembled in bytewise key order instead of building a table and sorting its keys. The output text is the same as before.
- **The result is encoded once.** The receipt, the stored intent's reply and the final reply each encoded the same result table. It is now encoded once per command.
- **A cheaper bytewise comparator.** `U.blt` settles the first byte before entering its loop, and sorts of one item skip the sort. The ordering is the same (it is still an explicit byte comparison; no locale is involved).
- **Not adopted: Lua's built-in string order.** Measured at 11 µs against 104 µs for the 28 job fields, but it follows the server's locale, and the Redis spec (22, the Text row of the encoding table) says scripts never compare strings with Lua `<`. Nothing here proves the server runs a bytewise locale in every deployment, so ordering stayed explicit. A bucket-by-first-byte variant (66 µs) was also tried; the fixed lists beat it by removing the sort rather than speeding it up.

### Where the time goes in a single dispatch (microseconds, before the change)

Reading the application and gates 140, deciding 175, events 85, ordering writes 175, packing the intent 65, writing the guard 42, writing the data 195, about 940 to 1,000 in total. After the change: events 52, ordering 78, packing 46, the rest unchanged.

A finer profile (new `DEEP=1` mode of `profile.py`) shows what is left and cannot be cut without protocol changes:

- About 100 µs is spent before the script's own code runs: building the chunk's roughly 75 top-level functions for every call (prelude about 60, jobs about 40). Bisecting the prelude by section showed no single part dominating; it is the number of closures. Fixing it means shipping fewer, shared functions or caching them, which touches the claim and finish scripts another agent is editing, so it was not attempted.
- Reading the meta hash and checking gates, about 50 to 75 µs.
- Writing the data, about 200 µs for 11 Redis commands per dispatch (job, payload, 3 created indexes, jobtypes, ready index, receipt, receipt expiry, events, meta).
- Packing and storing the intent, then writing the same data again, about 110 µs. Cutting this needs the recovery model to change, not attempted.

### Script time, paired (`profile.py`, old Lua against new Lua, same Redis, back to back)

| | before | after | change |
|---|---|---|---|
| single dispatch, µs per job | 1,136 to 1,214 | 978 to 1,044 | about 13 to 15 percent less |
| group of 6, µs per job | 801 to 818 | 662 to 677 | about 17 percent less |

### End to end (bench harness, 10,000 enqueues, fresh AOF-always Redis 8.2 per run, grouping off)

Arms: `base` (tree before the change), `off` (this change), `bullmq`. Five rounds at 16 callers, three at 64.

| callers | base jobs/s (median) | this change | BullMQ | change / base | change / BullMQ |
|---|---|---|---|---|---|
| 16 | 375 | 401 | 559 | 1.07 by medians, per-round ratios 0.8 to 1.2 | 0.67 |
| 64 | 531 | 671 | 1,851 | 1.26 by medians, per-round ratios 1.06 to 1.30 | 0.36 |

Redis CPU seconds per run (user plus system): 16 callers, base 13.3 to 15.4, this change 10.5 to 13.3, BullMQ 3.0 to 4.0. At 64 callers, base 11.2 to 12.3, this change 8.9 to 9.9, BullMQ 1.9 to 3.3. So Redis does about 20 percent less work for the same ten thousand jobs. At 16 callers the throughput gain is within the noise (one round went the other way); the drop in Redis CPU is the steadier signal. BullMQ's own rounds varied by almost a factor of two (541 to 994 at 16 callers; 783 to 1,981 at 64), which shows how loaded the host was.

The gap to BullMQ is still large: the diet takes script time per dispatch from about 1.2 ms to about 1.0 ms, and the 90 percent target needs about 0.2 ms.

### Tests

- New `RedisDispatchLayoutTests`: the two pre-ordered lists are bytewise sorted and equal to the lists in the Lua source (so a field added in one place fails), a real dispatch writes every field of a job and receipt, the event row's keys are in bytewise order, and a resend replays the same job.
- Full Redis project (RUNWICK_TEST_REDIS, redis:8.2): 87 passed, 1 failed. The failure is `RedisSoakTests.Eight_connections_run_1200_mixed_commands...`: `ScheduleEvaluationException: The 250 ms evaluation budget elapsed` inside the C# cron planner, on a heavily loaded host. It is client-side schedule evaluation, not script code; re-run alone with the other Redis tests finished, the soak class passed (3 of 3).
- Contract project (with RUNWICK_TEST_REDIS set, so the Redis cases ran): 488 passed, 3 failed. All three failures are the Postgres case (`TelemetryPeerContractTests.A_peer_killed_after_Finish...`, `ManagementContractTests.A_read_the_caller_cancels...`, `BufferedFinishContractTests.A_graceful_stop_sends_the_buffered_finishes`), none the Redis case. This change touches only Redis Lua, so they are not caused by it, but I did not run the unchanged tree to prove they fail there too.

### Not done

- Chunk setup cost (about 100 µs per call) and the intent write-twice cost are the two remaining large items; both need work shared with the claim and finish scripts or a recovery-model change.
- No quiet-host run. No AOF bytes comparison was made: the stored commands are the same, only their order of construction changed, but the byte count was not measured.
- Grouped dispatch (`--redis-group`) end to end was not re-run; only its script time was (table above).

### Tools

`eng/bench-compare/redis-group/profile.py` gained `DEEP=1` (finer phase marks) and `LUA_DIR=<path>` (profile another checkout's scripts back to back). `pairs.sh` gained an arm `base` that runs a workload binary given by `BASE_DLL`.
