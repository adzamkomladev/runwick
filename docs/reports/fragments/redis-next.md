## Redis: test fixture race and where the lean scripts' time goes (perf round two, Redis F)

### Short answer

- **Test fixture race: fixed.** The Redis test project now migrates the shared test server once, before any test class starts. On a brand-new empty server, a class that writes `rw:*` keys without migrating first (for example `StoreDiagnosticsTests`) could run before the first fixture had migrated, and every later migration then refused with "rw:* keys but no rw:store" (the 52 setup failures seen earlier).
- **Lean script time: profiled, three small trims made, no measurable gain.** The trims are real (three fewer Redis commands per claim call, two fewer per finish call, and no JSON parsing in the event pack), but on this host the whole-script cost per job did not move outside the noise. No single piece of the lean path is large enough to matter on its own: the time is spread across reading jobs, building the after-image and texts, and the writes, and on a loaded host most of each call's cost is fixed per call.
- **Shorter job-hash field names (layout revision 3): not done.** The stored-data design measured them at about 3% of the log bytes and no CPU change. That is not worth a layout change.
- Nothing stored, replied or replayed changed. No migration.

### The fixture race

`RedisServer.EnsureMigratedAsync()` (tests/Runwick.Harness) already migrated the server once per process, but only when a test asked for it through `DriverFixture` or by calling it. Test classes run in parallel, and some write `rw:*` keys directly: `StoreDiagnosticsTests` writes an application's meta hash by hand and never migrates. On an empty server that class could go first. After that, the migration finds `rw:*` keys but no `rw:store` and refuses, so every fixture after it fails.

The fix is an xUnit assembly fixture, `RedisServerMigrated` (tests/Runwick.Redis.Tests/RedisServerMigrated.cs), which xUnit creates before any test class in the project runs. It calls the same once-per-process migration. Nothing else changed.

Checked on brand-new empty servers: `StoreDiagnosticsTests` alone (a class that never migrates) passed 3 of 3 and left `rw:store` behind, so the migration ran first. Five classes together (below) passed 15 of 15 on another empty server.

### Where the lean path's time goes

Measured with `profile_worker.py --lean-marks` (new): the scripts as the binary composes them, with CPU-clock marks between the lean path's steps. One call of 8 jobs, median of 60 calls, on main-merge's scripts. Host load averages were 60 to 115 during these runs, so absolute numbers are high; read them as shares.

Claim of 8 jobs (about 1,380 µs in all):

| Step | µs per call | What it is |
|---|---:|---|
| Script start and helpers | 50 | the frame's local helpers, the server time |
| Meta and gates | 80 | `HGETALL` meta, gates, receipt lookup |
| Queue row and contracts | 30 | |
| Candidates | 229 | `ZRANGE` of the ready set, `HGETALL` of each job |
| Decision | 134 | checks, attempt ids, counters |
| After-image and texts | 371 | job fields, index changes, the reply item and receipt texts |
| Receipt | 31 | |
| Event pack | 108 | **66 after this change** |
| Writes | 268 | the Redis commands themselves |
| Wake check, payloads | 67 | |

Finish of 8 jobs (about 1,220 µs in all): script start 46, meta and gates 57, reading and deciding the items 359 (of which about 200 is `HGETALL` of the job hashes and about 85 decoding the bodies), after-image and texts 326, event pack 53, writes 338, the rest about 45.

Two things the profile ruled out:

- **Lua garbage collection is not the per-call cost.** With the collector stopped for the duration of each script, the per-job figures were the same (claim 477 and 172 µs per job at 1 and 8 jobs per call, finish 444 and 175).
- **The per-call cost is mostly fixed work done cold.** Fitting the profile's numbers at 1 and 8 jobs per call gives about 310 to 420 µs per call plus about 120 µs per job. A tight loop of the same Redis commands inside one script costs a small fraction of what the first ones cost in a real call (for example `HGETALL` of a meta hash: about 7 µs warm, against about 70 µs for the whole meta-and-gates step in a call). On this loaded host, each call starts with cold caches. Batching (more jobs per call) is what spreads that cost, which buffered finish and claims in flight already do.

### What changed and why it is safe

All in `src/Runwick.Redis/Lua/lean/` (the lean claim and finish paths). The generic path is unchanged.

- **No-op removals are no longer sent.** The lean claim no longer sends `ZREM idx:created:s:reserved` and `ZREM idx:terminal`; the lean finish no longer sends `ZREM ready:<queue>:<priority>` for a job that ends, or `ZREM idx:terminal` for a job that retries. Every state change goes through the same index update (`U.set_state`), so a pending or retrying job is never in those two indexes, and a running job is never in a ready set or the terminal index. The removals could not change anything, and Redis does not write a removal that removes nothing to the AOF, so the log is the same too.
- **The claim's event pair is written as its pack line.** Before, the lean claim built two JSON detail texts per job, and the commit step matched them with patterns to fold them into the pack's `~rs` line. Now the claim gives the folded line directly. The line is built from the same values with the same formatting.
- **One `HSET` on meta at commit.** The event pack's `nextevent` and the revision now go out in one `HSET` instead of two. If a write fails partway, the stored intent is the same as before, and recovery writes both fields.

Proof that nothing stored or replied changed (`diffcheck.py`, which also gained a fix: it now normalizes the time inside any UUIDv7 attempt id, not only times that start with the digits 01a1, which the clock has since passed):

- These scripts against main-merge's scripts: 74 keys compared, 0 differences (keys, replies, replays and the events feed at page sizes 1 to 6).
- These scripts' lean path against their own generic path: 74 keys compared, 0 differences.
- Main-merge against itself, as a control: 0 differences.

REDIS-11 holds: no new text comparison was added.

### Before and after

`profile_worker.py --plain` (whole script, Lua CPU clock), four rounds alternating which build went first, 60 calls per size, load averages 61 to 72. In-script µs per job, median of rounds:

| | Claim, 1 per call | Finish, 1 per call | Claim, 8 per call | Finish, 8 per call |
|---|---:|---:|---:|---:|
| main-merge | 543 | 430 | 176 | 158 |
| this change | 533 | 464 | 177 | 162 |

The differences are inside the round-to-round spread. The event pack step, timed on its own, went from 108 to 66 µs per claim of 8 jobs, about 5 µs per job, which is about 1.5% of a claimed-and-finished job.

### Tests

Built with `dotnet build Runwick.slnx -c Release`. Against a brand-new Redis 8.2 (`appendfsync always`), run together: `StoreDiagnosticsTests`, `RedisLeanScriptTests` (LS-01 to LS-03, including every write prefix of a lean claim and finish), `RedisLayout2Tests` (event packs read back at every page size), `RedisDispatchLayoutTests` and `RedisWorkerPathTests` (wake hints): 15 of 15 passed. The full Redis, Core and Contract suites were not run here.

### What is not done

- **The 90% of BullMQ target is still not met,** and this change does not move it. What is left in the lean path is spread thin; the biggest single steps are building the after-image and its texts and the Redis writes themselves.
- **The per-call fixed cost** (about 310 to 420 µs per call on this host) is the largest piece at small batch sizes. Cutting it inside Lua is not possible: the work is already small, and the cost is that work running cold. A real change would be Redis Functions (`FUNCTION LOAD`), which define the helpers once per server instead of once per call. That is a larger design change (every script reads `KEYS`/`ARGV` as globals and keeps per-call state at file level, and functions must be loaded on every primary) and was not attempted.
- **Field names** stay long (see the short answer).
- Drain benchmarks were not run; see the commands in the hand-off.
