# Redis worker path: what to store per job (design for a decision)

Written 2026-11-20. Base: `main` at 21571c5. Status: **design only, nothing here is merged or switched on.** The user decides which options go ahead.

Prototype code is on the local branch `redis-stored-data-proto` (commit 973e55c, not pushed): the options sit behind a flag string in `src/Runwick.Redis/Lua/prelude.lua` (`__PX__`, all off by default). They change the writers only. The management readers of the new event and attempt formats are **not** written, so that branch must never be merged.

## Short answer

- **Throughput follows Redis main-thread cost per job, on disk and on memory alike.** In the paired drains, Runwick's share of BullMQ's throughput (0.41 on both logs, main) is almost exactly the inverse ratio of Redis CPU per job (BullMQ 0.62 against 1.43 ms on disk, 0.52 against 1.23 on memory). Under `appendfsync always` Redis also runs the fsync on its main thread, so on disk that thread is busy with CPU plus fsync waits.
- **Every stored-data option together** cuts the worker path's log bytes by 39%: 3,976 to 2,425 bytes per job, measured. This is the prototype "all": finish receipts per call, events packed per call with one stored row per claimed job, the attempt record packed into one field, and no state index for reserved and running jobs.
  - In-script CPU per job, at 8 jobs per call: claim 13% lower (331 to 287 µs), finish 10% lower (269 to 242 µs).
  - Paired drains, Redis CPU per drained job (insert included): 9 to 11% lower.
  - Paired drains, AOF per drained job: 26% lower (6,829 to 5,074 bytes).
  - Throughput: median 1.00× main on disk (rounds from 0.78 to 1.31, so noise) and 1.08× on memory (every round up, 1.05 to 1.14).
  - Against BullMQ: 0.36× on disk and 0.44× on memory (main: 0.41× on both).
- **90% of BullMQ is out of reach with any stored-data change, and still out of reach if guarantees are given up.** BullMQ uses about 0.35 ms of script time per job, Runwick about 1.0 ms (insert included). At 8 jobs per call, Runwick's worker scripts spend about 600 µs of CPU per job. Only about 200 µs of that builds and writes stored data. The other 400 µs is decision work: reading the job hash, ownership and state checks, decoding arguments, building replies, plus the fixed per-call setup. Even with **no receipts, no attempt history and no events at all**, Runwick would stay at about 1.5 to 2 times BullMQ's script time. Closing that needs a leaner decision path, not a leaner layout.
- **More jobs per call and per fsync helps more than any layout change.** The same main build with 32 slots instead of 16 drained 1.37× faster (median of 3 paired rounds on disk: 430 to 570 jobs/s). With 32 slots each claim and each event-loop turn carries more jobs, so the fixed per-call cost and the fsync are shared by more of them. The engine keeps **one claim in flight per pool** (`PoolCoordinator.LaneVisitAsync` awaits each claim). Letting a pool keep a second claim in flight, or claim ahead of freed slots, changes no stored data and no guarantee. It is the biggest lever found here, but it was not built or measured at 16 slots.
- **Recommended:**
  1. Claim pipelining in the engine (client only).
  2. Finish receipts per call (option a). It needs no layout revision because only the sending process reads them.
  3. One layout revision 2 that bundles the packed attempt record (c), events packed per call with one stored row per claimed job (b), and no null fields (e, which comes free with b).
  4. Redis Functions (`FCALL`) instead of `EVALSHA` is a separate decision. It removes about 165 µs of fixed Redis time per call. That is small at drain batch sizes but about 15% of a single-job claim plus finish.

  Not recommended: shorter field names (d), skipping the receipt lookup (f), and dropping the state index for active jobs.

## How this was measured

| Tool | What it gives |
|---|---|
| `eng/bench-compare/redis-group/writes.py --fields` | Real scripts against an AOF-always Redis 8.2 whose log is on the host. Bytes appended to the AOF per job, by key kind and by hash field, for Dispatch (1 KiB payload), a claim of 8 (ReserveAndBegin), RenewMany of 8, FinishMany of 8, a single Finish, and FinishAndClaim. This round added Dispatch, RenewMany and the per-field breakdown. |
| `profile_worker.py` + new `profsum.py` | In-script CPU per job (`os.clock`), by phase, at 1 and 8 jobs per call, for each Lua variant. Two rounds at k=1 and five at k=8, in rotated order, median shown. The host was shared (load about 3 to 7 on 8 cores). Single options differ from main by less than this host's round-to-round noise (about ±25 µs per job at k=8). Only the combined variant is clearly outside it. |
| `slowdrain.sh` | One drain under SLOWLOG at threshold 0: calls and Redis wall time per operation. This is where the batch sizes below come from. |
| `pairs-combined.sh` | Paired drains of 20,000 jobs, 1 KiB, 16 slots, buffered finish. Arms: `oldbuf` (main), `buf` (combined prototype), BullMQ 6.7.2. Fresh Redis 8.2 per run, arm order rotated, each round under `flock /tmp/runwick-bench.lock`, load recorded. |
| Throwaway micro-tests (not kept) | The fixed per-call cost: an empty `EVALSHA`, the prelude chunk alone, an `FCALL` of a function from a library loaded once, 5 × 3,000 calls each, interleaved. The figures are in the text. |

`slowdrain.sh` and `pairs-combined.sh` now take `NAME_PREFIX` for their container names.

What a drain actually sends (main, buffered finish, one SLOWLOG drain): **2,395 claims** for 20,000 jobs (8.4 jobs per claim), **1,061 FinishMany** (18.8 items each), 201 DispatchMany for the insert. So the per-call fixed costs are spread over 8 to 19 jobs. The per-job figures below use 8 jobs per call.

## 1. What is stored per job today (inventory)

AOF bytes per job, `writes.py`, main at 21571c5. "Cmds/job" is Redis write commands per job inside the script's `MULTI`/`EXEC` block.

### 1.1 Dispatch (single, 1 KiB payload): 3,587 bytes per job

In the drain the insert is a `DispatchMany` of 100. The per-call pieces (meta, `MULTI`/`EXEC`) then mostly disappear, and the drain's log works out to about 2,850 bytes per inserted job.

| Piece | Key and command | Bytes | Read by |
|---|---|---:|---|
| Payload | `SET pay:<id>` | 1,107 | the claim reply (handler input), export, detail expiry |
| Job hash, 26 fields | `HSET job:<id>` | 791 | every later decision; management (GetJob, ListJobs, stats) |
| Dispatch receipt, 11 fields (not compact) | `HSET rcpt:<cmd>` | 497 | replay of a lost reply; `ResolveOperation` from `JobDispatcher.ResolveAsync` (any process); retention |
| `dispatched` event row + index | `HSET events:data`, `ZADD events` | 325 + 60 | `EventsSince` (public events feed); retention |
| Meta (`rev`, `nextevent`) | `HSET meta` ×2 | 125 | every reader (`rev`); history check |
| Created indexes (all, by queue, by state) | `ZADD idx:created*` | 109 + 119 + 119 | ListJobs, stats, cleanup |
| Ready index | `ZADD ready:<q>:<p>` | 118 | claim |
| Receipt expiry | `ZADD rcpt:exp:durable` | 114 | retention |
| Job type count | `HSET jobtypes` | 74 | ListJobTypes |
| `MULTI`/`EXEC`/`SELECT` | | 29 | Redis |

### 1.2 Claim (ReserveAndBegin, 8 jobs per call): 2,067 bytes per job, 6.2 Redis commands per job

| Piece | Key and command | Bytes/job | Read by |
|---|---|---:|---|
| Attempt fields `a:<attempt>:gen, res, worker, token, reserved, exec, auth` (7 fields, each name carries the 32-hex attempt ID) | `HSET job:<id>` | 466 | GetJob attempt list and the dashboard timeline (gen, res, worker, reserved, exec, auth); job stats (worker, gen, reserved, auth). **`token`: nobody.** |
| Job-level fields `attempt, worker, token, lease, updated, state, rcount, xcount, rev, g:<n>:started` | same `HSET` | 396 | ownership checks of renew, finish, recover; management |
| Event rows `attempt_reserved` + `attempt_started` (2 rows of about 285 bytes) | `HSET events:data` | 576 | `EventsSince` only (the public `/v1/events` feed). The dashboard timeline is built from GetJob, not from events. No webhooks exist, and telemetry is in-process. |
| Event index | `ZADD events` | 42 | `EventsSince`, retention |
| Claim receipt, **one per call** (compact codec 1 since 2026-11-19) | `HSET rcpt:<cmd>` | 244 | replay of a lost claim reply (same process); retention |
| State index (pending out, running in) | `ZREM`/`ZADD idx:created:s:*` | 113 | ListJobs by state, stats |
| Attempt to job | `HSET attempts` | 84 | management `queueof`, RevealError |
| Lease index | `ZADD leases` | 65 | expired-lease scan (recovery), renew |
| Ready index removal | `ZREM ready:*` | 46 | claim |
| Meta `rev`, `nextevent`; receipt expiry; framing | | 16 + 14 + 4 | readers; retention; Redis |

### 1.3 Renew (RenewMany of 8): 182 bytes per job

`HSET job lease` (106) and `ZADD leases` (65), plus meta `rev`. This is already minimal, and it is not part of the drain.

### 1.4 Finish (FinishMany of 8): 1,911 bytes per job (single Finish: 2,370)

| Piece | Key and command | Bytes/job | Read by |
|---|---|---:|---|
| Attempt fields `fin, outcome, disp, reason, elapsed, closedby` | `HSET job:<id>` | 398 | GetJob and stats (fin, outcome, disp, reason). **`elapsed` and `closedby`: nobody** (also `delay`, `next` when retrying). |
| Job-level fields `terminalat, updated, reason, state, rev` + `HDEL attempt, worker, token, lease` | `HSET`, `HDEL job:<id>` | 337 | everything |
| Finish receipt, **one per finished job** (9 fields, compact result) | `HSET rcpt:<cmd>` | 495 | replay of a lost finish reply. Only the sending process resends a finish. `ResolveOperation` is only called for dispatches (and on PostgreSQL for staged completion). |
| Receipt expiry, one per job | `ZADD rcpt:exp:durable` | 66 | retention |
| `attempt_finished` event row + index | `HSET events:data`, `ZADD events` | 348 + 24 | `EventsSince`; retention |
| State index (running out, terminal in) | | 114 | ListJobs, stats, cleanup |
| Terminal index | `ZADD idx:terminal` | 66 | stats |
| Lease index removal | `ZREM leases` | 45 | |
| Meta, framing | | 20 | |

**Worker path per drained job (claim + FinishMany): 3,976 bytes.** For comparison, BullMQ writes about 2,815 bytes per job *including* the insert and the 1 KiB payload.

### 1.5 Where the CPU goes

Medians of the profiler, µs per call of 8 jobs, main:

| Phase | Claim | FinishMany | What it is |
|---|---:|---:|---|
| Fixed: chunk setup, `HGETALL meta` + gates, receipt lookup | 170 to 200 | 145 to 170 | the same for 1 or 16 jobs |
| Decision | 1,470 | 1,120 | reading candidates and job hashes, ownership and state checks, `cjson.decode` of finish bodies, building item replies |
| Receipt row | 210 | (in decision) | building the compact receipt |
| Events, building the write list, size bound | 190 | 290 | |
| Writing (`redis.call` of each write, which also feeds the AOF) | 450 | 480 | |
| Reply payload reads | 105 | 10 | |
| **Per job** | **about 330 to 350** | **about 265** | |

About a third of the worker path's CPU writes stored data (building and applying writes, events, receipts). Two thirds is the decision and fixed setup. That ratio is the main reason no stored-data option can reach BullMQ's 0.2 to 0.3 ms per job.

The fixed setup is mostly running the 1,700-line prelude chunk, which defines about 250 functions on every call. An `EVALSHA` that runs only the prelude and jobs chunk and reads `meta` costs about 206 µs of Redis wall time per call. An empty script costs 25 µs, and an `FCALL` of a function from a library loaded once costs 41 µs (with the same `HGETALL meta`). In-script CPU for the chunk is about 55 µs. The rest of the wall time is allocation and the garbage collection it causes.

## What limits the drain

Redis main-thread CPU per job times jobs per second, from the paired drains (the CPU includes the insert, so these overstate the drain phase a little):

| Log | Arm | CPU ms/job | Jobs/s | Main thread busy with CPU |
|---|---|---:|---:|---:|
| disk | main | 1.43 | 401 | about 57% |
| disk | BullMQ | 0.62 | 1,014 | about 63% |
| memory | main | 1.23 | 674 | about 83% |
| memory | BullMQ | 0.52 | 1,626 | about 85% |

Under `appendfsync always`, Redis writes and fsyncs the log on its main thread before it answers, once per event-loop turn. On disk, the rest of the main thread's time largely goes to those fsync waits. On memory, fsync is nearly free and CPU is the whole story.

Both queues therefore run their Redis main thread at about the same load, and throughput is set by how much of it each job costs. That cost is the script CPU plus a share of each fsync. Runwick's share of BullMQ's throughput (0.41 on both logs) equals the ratio of their CPU per job (0.43 and 0.42).

So there are two ways to go faster:

- **Less Redis work per job.** This is where stored-data cuts act. They reduce the bytes written, the commands and part of the Lua. They bought 9 to 11% of CPU per drained job and 8% throughput on memory.
- **More jobs per fsync and per fixed per-call setup.** A pool keeps one claim in flight and waits for its reply, which comes after the fsync, before sending the next. The 32-slot run (1.37× on disk) shows how much more jobs per round trip are worth. BullMQ's 16 workers each keep a call in flight, so one fsync covers many of them.

## 2. The options

The base for every table is main at 21571c5. Bytes are the worker path per drained job (claim of 8 plus FinishMany of 8), measured with `writes.py`. CPU is in-script µs per job at k=8, median of 5 rounds (`profsum.py`). Single rounds of the same variant differ by about ±25 µs per job on this host, so only differences larger than that mean anything.

### Summary table

| Option | Claim B/job | Finish B/job | Worker path B/job | Saved | Claim CPU µs/job | Finish CPU µs/job | Needs |
|---|---:|---:|---:|---:|---:|---:|---|
| main | 2,066 | 1,910 | 3,976 | – | 331 | 269 | – |
| (a) finish receipts per call (`fr`) | 2,066 | 1,653 | 3,719 | 257 (6%) | 355 | 268 | codec marker; no revision; a client change |
| (b1) events packed per call (`ev`) | 1,675 | 1,754 | 3,429 | 547 (14%) | 309 | 289 | layout revision 2 (old readers cannot read packs) |
| (b2) plus one stored row per claimed job (`ev`+`ev1`) | 1,628 | 1,754 | 3,382 | 594 (15%) | 336 | 270 | revision 2; feed unchanged if the reader expands the row (below) |
| (b3) only one row per claimed job, unpacked (`ev1`) | 1,784 | 1,910 | 3,694 | 282 (7%) | – | – | changes the public feed unless expanded on read |
| (c) attempt record packed into one field at close (`att`) | 1,661 | 1,728 | 3,389 | 587 (15%) | 314 | 273 | revision 2 |
| (d) shorter job-hash field names (computed, not prototyped) | about 2,025 | about 1,874 | about 3,900 | about 77 worker + 130 at dispatch | about 0 | about 0 | revision 2 plus dual names or a rewrite |
| (e) no null `actorId`/`attemptId` in event rows (`nul`) | 2,036 | 1,896 | 3,932 | 44 (1%), 32 more at dispatch | 333 | 300 | revision 2 (or free inside b) |
| (f) cut fixed per-call setup | 0 | 0 | 0 | 0 | see 2.6 | see 2.6 | depends (2.6) |
| (g) no state index for reserved/running (`six`) | 2,000 | 1,864 | 3,864 | 112 (3%) | 318 | 280 | revision 2; ListJobs by those states reads `leases` |
| **all of a, b2, c, g** | **1,157** | **1,268** | **2,425** | **1,551 (39%)** | **287** | **242** | revision 2 + client change |

The byte savings add up exactly (257 + 594 + 587 + 112 = 1,550). The CPU savings of single options are within noise. Only the combination is clearly measurable.

### 2.1 (a) One receipt per call instead of one per finished job

**Today.** A claim already writes one receipt per call (one `ReserveAndBegin` command for the whole batch, compact since 2026-11-19, 244 bytes per job at k=8). A `FinishMany` is a batch of independent `Finish/1` commands, each with its own command ID, fingerprint and deadline. Each gets its own 9-field receipt hash `rcpt:<item command>` and its own `rcpt:exp:durable` entry: 561 bytes per job.

**Prototype (`fr`).** All item receipts of one call go into one key `rcpt:<call>:f`. Its field is the item's command ID, and its value is `1|<fingerprint>|<NotAfter>|<decided>|<compact result>`. One `rcpt:exp:durable` entry per call, at the latest retain time of its items. Cleanup needs no change: it already deletes `rcpt:<member>`.

**Exact replay of one lost item.** The worker keeps each unresolved finish's command ID in memory. With this option it also keeps the call ID it was sent in. A resend carries that call ID as a hint. The script looks up `HGET rcpt:<call>:f <item>`, falls back to `rcpt:<item>` (receipts written before the change, or a single `Finish`), and from there acts as today: a different fingerprint is a conflict, a match replays the stored text through the same builders (byte-identical) and writes the confirmation marker. The marker cannot be the 9-field row's `confirmed` field any more, so it is written as `HSET rcpt:<call>:f c:<item> <now>` (a fresh write before the reply, as 22 §4.5 needs). The prototype writes the new receipts. The hinted lookup, the replay and the marker are described here but not built.

**Resolve.** `ResolveCommand/1` is never used for finishes on Redis. It is only called for dispatches (`JobDispatcher.ResolveAsync`), for management control commands, and for staged completion, which is PostgreSQL only. A finish whose sender died is not resolved by anyone. Its lease expires and Recover moves the job on. That is why the hint can live in memory. If a future caller must resolve a finish by its own ID from another process, this option needs a lookup key per item, and most of the saving is gone (it would keep about 190 of the 257 bytes' cost).

- **Saves:** 257 bytes per job (finish 1,910 to 1,653). CPU within noise (fewer fields and keys, one `ZADD` per call).
- **Guarantees touched:** exact replay holds only for a resend that carries the hint, which every resend by the sending process does. Recovery, crash consistency (still one script, one `MULTI`/`EXEC`) and retention are unchanged. The receipt now lives as long as the call's latest item, which is at most a few seconds longer.
- **Migration:** none for data. A new receipt form, recognized by the key suffix `:f` and the value prefix `1|` (a codec marker in the spirit of the compact receipt's `{"~":1}`). New binaries read both. Old binaries never read these receipts: a finish is resent only by the process that sent it, and that process runs the same binary.
- **Coexistence:** old and new binaries can share a store during a rolling restart, as with compact receipts (22 Changes 2026-11-19).
- **Risk:** low to medium. The risk is a resend path that loses the hint (a restart between send and resend). That path already cannot happen, because a restarted process has given up its leases. A test must pin that.
- **Tests:** a FinishMany whose reply is dropped, then resent alone and inside another batch with the hint: replays byte-identical, and the marker is written. Resent without the hint: replays from a legacy `rcpt:<item>` only. A conflicting fingerprint. Retention deletes `rcpt:<call>:f` once its latest item is past retain. RC-03-style old receipts still replay.
- **Sub-option, not recommended:** store 32 instead of 64 hex characters of the fingerprint (saves 32 bytes per receipt). A different body under the same command ID would then be detected with 2^-128 instead of 2^-256 certainty. That is fine in practice, but it changes a stated invariant for 1% of the bytes.

### 2.2 (b) Fewer event rows: packed per call, one stored row per step

**Today.** A claimed and finished job writes three rows of about 285 to 340 bytes: `attempt_reserved`, `attempt_started` and `attempt_finished`. Each row repeats the command ID, the time, the job ID, the attempt ID and two `null`s. The only reader is `EventsSince/1`, which serves the public `GET /v1/events` feed (`ManagementSession.ListEventsAsync`, application-wide `Jobs.Read`). The dashboard's job timeline is built from GetJob (the attempt fields), not from events. There are no webhooks in the code base. Telemetry is emitted in-process and never reads stored events. Retention deletes events by age (`job_events`, default 90 days).

**Prototype (`ev`).** One `events:data` field per script call, keyed by the call's first event ID, holding `{"~":1,"t":<time>,"c":<command>,"e":[[kind, job, attempt, actor, details],...]}`. Job and attempt are written as `0` when they repeat the previous entry's. Event IDs are still assigned one per event (`nextevent` advances by the number of events), and the `events` sorted set gets one member per call (score = first ID).

**Prototype (`ev1`).** In a claim, the `attempt_reserved` entry is folded into the `attempt_started` entry: its details gain `reservationNumber`. The reader must then expand one stored entry into the two public rows with consecutive IDs and byte-identical text. That is possible because everything in the reserved row (kind, job, attempt, command, time, generation, reservation number) is in the started entry. So the public feed does not change. Without expansion on read, Redis would show a different feed from PostgreSQL, which writes both rows. That is not acceptable.

**How the history is rebuilt on read.** `EventsSince(after, limit)` finds the pack holding `after + 1` with `ZRANGE events BYSCORE <after> -inf REV LIMIT 0 1` (one extra lookup per page). It expands that pack and the following ones with the same row writer the scripts use today, skips IDs ≤ `after`, and stops at `limit` rows, possibly in the middle of a pack. The cursor stays the event ID. EVT-01 (stable prefix) and EVT-02 (bounded page) hold: a pack is written by one script call, so it is complete or absent. Retention deletes whole packs by age, and every event in a pack has the same time. `cleanup.lua`'s `head_delete` must read the pack's `t` instead of a row's `occurredAt`. The reader and the cleanup change are described here; the prototype has neither.

- **Saves:** b1 547 bytes per job, b2 594 (claim 2,066 to 1,628; finish 1,910 to 1,754). On a 100-job DispatchMany it saves the per-row overhead of 100 rows too (single Dispatch 3,584 to 3,522). A further codec for the three hot kinds' details (fixed position lists, as the receipts have) would save an estimated 70 more bytes per job. That is not prototyped.
- **Guarantees touched:** none if the reader expands exactly. Event IDs, order and content stay the same. Crash consistency is unchanged.
- **Migration:** **layout revision 2.** A binary from before the change would hand a pack to the C# event parser as if it were a row. Readers must therefore exist everywhere before any writer packs. In v1 that is a revision bump (spec 23 §1: one store revision per binary; drain, stop old binaries, migrate, start new). No backfill: the new reader reads both forms (a row starts with `{"actorId"`, a pack with `{"~":1`), and old rows age out by retention.
- **Coexistence:** none across the revision. The revision gate refuses old binaries.
- **Risk:** medium. The read path is new code on a public feed. Paging inside a pack and retention at pack granularity need care.
- **Tests:** a byte-for-byte equality test of the feed (the same command sequence on main and on the new layout, comparing every row of `EventsSince` with IDs). Paging with every limit from 1 to the pack size + 1. A cursor in the middle of a pack. Retention cutting between packs. A mixed store with revision-1 rows before revision-2 packs. EVT-01/02 under three writers (REDIS-12). The PostgreSQL/SQLite contract cases for event kinds, unchanged.

### 2.3 (c) The attempt record folded into one field

The attempt is already part of the job hash. What makes it expensive is that it is stored as up to 13 separate fields, each named `a:<32-hex attempt ID>:<name>`. The names alone are about 520 bytes per attempt. It also duplicates job-level fields while the attempt is open (`worker`, `token`, `gen`, `rcount`).

**Prototype (`att`).**

- At claim, write no `a:` fields. The open attempt is described by the job's own fields (`attempt`, `worker`, `token`, `lease`, `rcount`, `gen`, `g:<n>:started`) plus two new short ones, `ares` (reserved at) and `aauth` (authorized at).
- At close (finish, recover, release, cancel, deferral), write one field `a:<attempt>` holding a fixed-order list: `[gen, reservation, execution, worker, reserved, authorized, finished, outcome, disposition, reason, elapsed, delay, next]`. `a:<attempt>:error` and `a:<attempt>:usage` stay separate because they can be large.
- Drop `token` and `closedby`, which nothing reads.
- The "attempt must not be closed twice" checks look at `a:<attempt>` and `aauth` instead of `a:<attempt>:fin` and `a:<attempt>:exec`.

- **Saves:** 587 bytes per job (claim 2,066 to 1,661, finish 1,910 to 1,728). CPU is within noise per step; fewer fields are built and written.
- **Readers to change:** `mread job` (GetJob attempt list: the open attempt from job fields, closed ones from the packed field), `mread` job stats (`jscan`: worker, gen, authorized, reserved, finished, error), `disclosure` (error, unchanged field). The dashboard timeline reads GetJob and needs no change.
- **Guarantees touched:** none for replay (receipts are separate) or recovery (the ownership checks read job-level fields, as today). Management views must show the same rows. The PostgreSQL columns `ownership_token` and `closed_by_command` would no longer have a Redis copy. 22 §3.1 says fields keep the PostgreSQL column names, so this is a written layout change.
- **Migration:** layout revision 2. Readers handle both forms for every job: per-field attempts written under revision 1 and packed ones. No backfill is needed, because drain-before-migrate means no attempt is open at the switch, and closed attempts are immutable. An optional background rewrite (like the `StatsIndex` walker with its cursor in `meta.bf`) could convert old jobs, so that the old-form reader can be deleted once no such job is left. Not recommended at first.
- **Risk:** medium. Every close path writes the record. A missed path would show an attempt with no record. REDIS-01 write-prefix sweeps cover crash states automatically.
- **Tests:** GetJob and job stats give identical results on both layouts for every close path: succeeded, failed with retry, permanent failure, cancelled before and after start, budget exhausted, rate deferral, recovered while reserved and while running, released. A job with attempts in both forms. A double close is refused. RevealError for both forms.

### 2.4 (d) Shorter job-hash field names, or a packed job record

Computed from the measured field list, not prototyped. Renaming every job-level field to two characters saves about 130 bytes per dispatched job, 41 per claim and 36 per finish: about 207 bytes per job in all, 3% of the drain's log. The CPU effect is nil (Lua string handling is not sensitive to these lengths). The attempt-field names, which are the long ones, are already handled by (c).

A **packed job record** (the whole job in one string) would rewrite the record on every step: about 450 to 600 bytes per write. That is no better than today's field writes once (c) is in, and every reader and every decision would have to decode it.

- **Migration:** revision 2 plus either dual names in every reader (every script reads job fields) or a full rewrite of every stored job at migration (a backfill with checkpoints, UPG-05).
- **Risk:** high churn (every script and reader) for 3%.
- **Recommendation:** do not do it.

### 2.5 (e) Dropping always-null event fields

`actorId` is null on every worker event, and `attemptId` on every dispatch event. Leaving out a null key saves 44 bytes per job on the worker path and 32 per dispatch (prototype `nul`). The C# reader already treats a missing key as null. This is still a change to stored rows read by other binaries, so it needs the revision, or it can ride along with (b): a pack writes `null` positionally and costs nearly nothing.

**Recommendation:** only as part of (b).

### 2.6 (f) The fixed per-call cost

Measured per call: chunk setup about 90 µs CPU (55 µs for the prelude and jobs chunk itself, the rest is the profiler's marks), `HGETALL meta` plus gates 40 to 60 µs, receipt lookup 5 to 30 µs. In wall time an `EVALSHA` that only runs the prelude and jobs chunk and reads `meta` costs about 206 µs, against 25 µs for an empty script.

At drain batch sizes (8.4 jobs per claim, 18.8 per finish) this is about 25 to 35 µs per job. At one job per call (low-rate traffic, delayed jobs, single Finish) it is about 330 µs of wall time per job for claim plus finish.

| Sub-option | Saves | Touches | Verdict |
|---|---|---|---|
| **f1 Skip the receipt lookup on a first send** (the client says "first send") | 5 to 30 µs per call (1 to 4 µs per job at drain batch sizes) | Exact-once. A mislabelled or duplicated first send of a claim would claim twice. The first reply is lost, so its jobs then sit reserved until their leases expire and use up one execution each when Recover closes them. | **Reject.** The gain is tiny and the risk is to the core guarantee. |
| **f2 Gate digest instead of reading all of meta** (the store writes one `gatekey` text whenever gates or capabilities change; the script compares it with the client's and reads only `store, guard, gatekey, rev, nextevent, bf`) | estimated 20 to 30 µs per call (not prototyped) | Gates must stay exact. The digest is written by the same scripts that change the gates. | Possible, small. A layout addition (new meta field) without a revision if old binaries keep reading all of meta. Low priority. |
| **f3 Caching meta between calls** | – | Redis Lua cannot keep state between `EVALSHA` calls. | Not possible as such. f4 is the real form. |
| **f4 Redis Functions (`FCALL`)**: load the library once, so the 250 function definitions are not rebuilt on every call | about 165 µs wall per call (206 to 41 µs in the micro-test). In the drain about 20 µs per job. At one job per call about 330 µs of about 2.2 ms per claimed and finished job (15%). | No stored job data. 22 §4.1 chose scripts over Functions [choice] because a function library is persistent server state another binary could replace. Mitigation: the library and function names carry the content hash (`rw_<sha12>_…`), so a name can only ever hold identical code, as an SHA does today. Libraries persist in the AOF and replicate. Under Cluster each primary needs its own load. Old libraries need deleting (by the migrator, never while a binary might use them). | Worth a separate decision. It is the only fixed-cost cut that matters, and it changes no guarantee. It needs a code restructure: every per-call value (`META`, `NOW`, the overlay tables, the quoted-text cache) becomes a variable reset at the start of each call. That is a few days of work plus the full REDIS suite. |

### 2.7 Other things found

- **(g) No state index for reserved and running jobs (`six`).** These jobs are exactly the members of `leases`. ListJobs with state reserved or running (and stats) could read `leases` and filter. Saves 112 bytes and about 2 Redis commands per job. But `leases` is ordered by expiry, not by creation, so a page in `created_at DESC` order would have to read every lease (bounded by 5,000 examined entries). For a store with more running jobs than that, the 503 `query_budget_exceeded` answer arrives sooner. **Not recommended:** the saving is 3%, and a management behaviour changes.
- **The `attempts` hash (attempt to job, 84 bytes per job).** It is needed by `queueof` and RevealError, which receive only an attempt ID. It cannot be derived. Keep it.
- **Unread fields** `elapsed`, `delay` and `next` (attempt). Nothing on Redis reads them, but they are PostgreSQL attempt columns and cheap inside the packed record of (c). Keep them there.
- **Claim pipelining (client only, no stored data).** See "What limits the drain". Two claims in flight per pool (or claims sized to all free slots plus the next expected ones) put more jobs into each event-loop turn, and so into each fsync and each fixed per-call setup. The 32-slot run gives an idea of the size (1.37× on disk). Nothing stored and no guarantee changes: each claim is still its own command with its own receipt. This is the largest single lever found, and it belongs in the engine (05 §9.6).

## 3. Benchmarks of the combined prototype

Drain of 20,000 jobs, 1 KiB, 16 slots, buffered finish, fresh Redis 8.2 per run (`pairs-combined.sh`, arm order rotated, every round under the bench lock).

- `oldbuf` is main at 21571c5.
- `buf` is the prototype build with `fr,ev,ev1,att,six` switched on. The build drained every job correctly. Its management reads of events and attempts would be wrong, because those readers are not written.
- BullMQ is 6.7.2 at concurrency 16.
- Script ms/job is Redis `commandstats` script time divided by 20,000. CPU ms/job is Redis main-thread CPU divided by 20,000. Both include the insert. The host was shared (load at start shown).

**Disk log (fsync on the host disk), 4 rounds:**

| Arm | Jobs/s per round | Median | Script ms/job | CPU ms/job | AOF B/job | Load |
|---|---|---:|---:|---:|---:|---|
| main | 308, 393, 409, 439 | 401 | 1.07 | 1.43 | 6,829 | 4.9, 5.2, 4.4, 13.2 |
| prototype | 403, 411, 320, 415 | 407 | 0.94 | 1.30 | 5,074 | 5.3, 8.3, 4.0, 7.6 |
| BullMQ | 594, 1,136, 892, 1,177 | 1,014 | 0.36 | 0.62 | 2,815 | 5.9, 6.0, 4.7, 7.9 |

Paired ratios per round:

| Pair | Per round | Median |
|---|---|---:|
| prototype / main | 1.31, 1.05, 0.78, 0.95 | 1.00 |
| prototype / BullMQ | 0.68, 0.36, 0.36, 0.35 | 0.36 |
| main / BullMQ | 0.52, 0.35, 0.46, 0.37 | 0.41 |

On disk the change is lost in round-to-round noise.

**AOF on memory (FSYNC=tmpfs), 3 rounds:**

| Arm | Jobs/s per round | Median | Script ms/job | CPU ms/job | AOF B/job | Load |
|---|---|---:|---:|---:|---:|---|
| main | 674, 592, 698 | 674 | 1.03 | 1.23 | 6,819 | 7.4, 6.1, 6.4 |
| prototype | 711, 677, 751 | 711 | 0.92 | 1.09 | 5,060 | 6.6, 6.7, 7.2 |
| BullMQ | 1,626, 1,892, 1,576 | 1,626 | 0.35 | 0.52 | 2,815 | 6.5, 6.2, 7.5 |

Paired ratios per round:

| Pair | Per round | Median |
|---|---|---:|
| prototype / main | 1.05, 1.14, 1.08 | 1.08 |
| prototype / BullMQ | 0.44, 0.36, 0.48 | 0.44 |
| main / BullMQ | 0.41, 0.31, 0.44 | 0.41 |

On memory the prototype is faster in every round, by about what its lower CPU per job predicts.

**More jobs per call (main only, disk, 3 rounds, load 4.1 to 5.2):**

| Slots | Jobs/s per round | Median | Script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| 16 | 501, 430, 417 | 430 | 1.02 | 6,829 |
| 32 | 520, 587, 570 | 570 | 0.97 | 6,737 |

Paired ratio per round: 1.04, 1.37, 1.37 (median 1.37). This is not a fair comparison with BullMQ at 16, because more slots means more jobs in flight. It only shows how much batching per round trip is worth.

**One SLOWLOG drain each (main and prototype, buffered finish, disk):** 2,395 and 2,397 claims, 1,061 and 1,055 FinishMany calls. So the batch shape is the same in both builds, and the prototype changes only the work per call. AOF per drained job: 6,830 against 5,079 bytes.

Results are in `eng/bench-compare/redis-group/results/drain-sdd*` (git-ignored). Delayed pickup (p95/p99) was not rerun: none of these options changes the probe, the wake hint or the pool's idle wait, which is what sets that tail.

## 4. Recommendation

**Order:**

1. **Claim pipelining in the worker engine** (client only, no layout or guarantee change). More jobs per call and per fsync was worth 1.37× in the 32-slot run, far more than any stored-data change. It needs its own design note in 05 and a paired run at 16 slots, because the 32-slot run also doubled the jobs in flight.
2. **(a) Finish receipts per call.** No revision, ships in a rolling upgrade, saves 257 bytes per job. It needs the hint to be carried by the finish resend path.
3. **Layout revision 2 bundle: (c) packed attempt record + (b2) events packed per call with one stored row per claimed job (expanded on read) + (e) for free.** Together with (a) this is the prototype's 1,439 of 1,551 bytes saved (without g), about 36% of the worker path's log, and about 10% of its script CPU. One revision, one drain-and-migrate upgrade, no backfill.
4. **(f4) Redis Functions:** separate decision. Most useful for low-rate and single-job traffic.
5. **Not recommended:** (d) shorter names, (f1) skipping the receipt lookup, (g) dropping the active-state index, the shorter fingerprint.

**Can 90% of BullMQ be reached?**

- **Memory log (CPU-bound): no, not with stored-data changes, and not even by giving up guarantees.** Every option together saves about a tenth of the worker scripts' CPU (13% on the claim, 10% on the finish), and 9 to 11% of Redis CPU per drained job. Removing the receipts (exact replay after a lost reply), the attempt history (management views) and the event rows (the public feed) entirely would still leave the decision work. That is about 0.4 ms per job at k=8 against BullMQ's 0.2 to 0.3 ms for everything. Reaching 90% would need a different execution model for the decision: no per-call chunk setup (f4), no overlay and after-image list for direct projections, no JSON decode of each finish body. Those are code changes, not layout changes, and each would need its own design.
- **Disk log: no, not with anything measured here.** Throughput follows Redis cost per job there too: CPU plus a share of each fsync. The stored-data changes do not move it beyond noise (Benchmarks). More jobs per call and per fsync (the 32-slot run: 1.37×) is the larger lever. Even both together, from main's 0.41×, plausibly reach 0.55 to 0.65× of BullMQ, not 0.9×. That figure is an estimate, not a measurement.
- **Pickup p95/p99 within 10%.** The delayed-pickup tail is set by the pool's idle wait and the probe hints, not by stored data (see the earlier delayed-pickup reports). The options here do not change it. (f4) would shorten a single claim by about 0.15 ms, which is invisible against a 300 to 600 ms p99.

## 5. Migration plan (for the recommended bundle)

1. **Release R1 (no revision):**
   - (a) finish receipts per call, with readers for both receipt forms;
   - the engine's claim pipelining.
   - Rolling upgrade allowed: old binaries never read the new receipts.
2. **Release R2 (layout revision 2):**
   - Writers: packed attempt record, packed events with the folded claim row, no `token`/`closedby`.
   - Readers: both revision-1 and revision-2 forms of attempts and events.
   - `RunwickRedisMigrations` moves `rw:store` and each application's `meta` to layout 2. It refuses while an intent is pending, as 22 §9.2 step 5 already says.
   - The binary's readable and writable layouts are `[2]`, so a revision-1 binary is refused with `schema_incompatible`, and so is a revision-2 binary on a revision-1 store.
   - Upgrade as spec 23 §4: back up, drain, stop old processes, migrate, reopen, start.
   - Rollback is the backup (spec 23 §5).
   - **No backfill.** Revision-1 event rows age out by retention, and revision-1 attempt fields stay readable for as long as their jobs are kept.
3. **Later, optional:** a background walker rewrites revision-1 attempt fields so the old-form reader can be removed.

What would have to be written or changed:

- 22 §3.1 and §3.2 (attempt and event encodings, the receipt form);
- 22 §7.1 (EventsSince over packs);
- 22 §9 and 23 §5 (layout floor 2);
- new REDIS cases for each option (listed under each option);
- the dashboard store snapshot test;
- `RedisDispatchLayoutTests` (it pins the dispatch receipt fields and the event row keys `actorId` to `occurredAt`; under (b) and (e) the row becomes a pack).

## 6. Decisions for the user

1. **Accept that 90% of BullMQ on a memory log is not reachable by changing stored data?** If not, the next step is a design for a leaner decision path (f4 plus a projection without the overlay), not a layout.
2. **Claim pipelining first?** It is client-only and the best single lever found (on both logs, and most on disk), but it is outside "stored data". Approve a separate design and measurement?
3. **(a) finish receipts per call:** accept that a lost finish reply can be replayed exactly only by the process that sent it (true today in practice, but then also by design)?
4. **Layout revision 2:** accept a drain-and-migrate upgrade (no rolling upgrade) for about 1,180 bytes (30%) per job from (b2) + (c) + (e), and a modest CPU gain? Or skip the revision and keep only (a)?
5. **(c):** accept dropping the Redis copies of `ownership_token` and `closed_by_command`, which nothing reads?
6. **(f4) Redis Functions:** reopen the 22 §4.1 choice?
7. **Backfill:** keep dual readers for revision-1 attempt fields indefinitely (recommended), or add the optional rewrite walker?
