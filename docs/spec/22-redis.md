# 22 — Redis store (SP-025)

**Sources:** [docs/reports/sp-018.md](../reports/sp-018.md) (the Redis spike: results, "What SP-025 must do", "Spec changes needed"), [redis-design-closure.md](../architecture/redis-design-closure.md) (selected representation, profile, limits, client lanes), [bullmq-redis-review.md](../architecture/bullmq-redis-review.md), [driver-capabilities.md](../architecture/driver-capabilities.md) §1, §2, §5, §6, §7 (Redis profile, encodings, outbox boundary), [driver-contract-suite.md](../architecture/driver-contract-suite.md) §2–§5 (fault points, cases, Redis fault gates), [00-overview.md](00-overview.md) §6 (`Runwick.Redis`, StackExchange.Redis 3.4.20), [03-storage-protocol.md](03-storage-protocol.md) (what every store must do), [04-postgres.md](04-postgres.md) (the reference implementation), [20-sqlite.md](20-sqlite.md) (the other non-reference store; this file follows its shape), [08](08-controls-and-completion.md), [10](10-rate-admission.md), [13](13-scheduler.md), [14](14-outbox.md), [16](16-management.md), [17](17-retention.md), [19](19-cli.md) for the operations added since M2, [v1-delivery-plan.md](../v1-delivery-plan.md) (SP-025 row: execution, schedules, audit, retention, outbox destination, bounded projections; applicable contract suite on the AOF-always profile; certification pending SP-028).

## In short

SP-025 adds `Runwick.Redis`: Runwick's job store on **one Redis primary** that writes every change to its append-only file and syncs it to disk before it replies. Workers and producers can run on many hosts. The store keeps exactly the behaviour of spec 03: the same commands, results, receipts, states and guarantees as PostgreSQL. Only the mechanics differ:

- **One profile only.** The server must run `redis-aof-always-single-primary-v1`: AOF on, `appendfsync always`, `no-appendfsync-on-rewrite no`, `maxmemory-policy noeviction`, `aof-load-truncated no`, standalone, primary role. Runwick reads these settings at startup and again while it runs. Anything else (a replica, Cluster, Sentinel, periodic fsync, RDB only, eviction, settings it cannot read) is refused, not accepted with weaker guarantees.
- **One application, one hash slot.** Every key of an application starts with `rw:{<application>}:`, so all of them live in one hash slot and one Lua script can change them together. Different applications can live side by side in one Redis database.
- **Every write is one script that decides, records, applies, publishes.** A Lua script reads the current state, checks the gates, receipt, deadline and ownership, and takes its decision with the **Redis server's clock**. Its first write stores the whole decision (the "intent": every value it will write) in the application's metadata hash. Then it writes those values, and its last write bumps the application revision and clears the intent in one `HSET`. This is the design SP-018 proved.
- **Half-written states are never shown.** Redis does not undo a script's earlier writes when it fails halfway. So every read and every write checks for a pending intent first. A read that finds one answers **Recovering**, never empty or stale data. A write that finds one first finishes it (rewriting the stored values is harmless) and then runs.
- **Counters are exact.** Lua numbers are floating point. Counters, fences and revisions are kept as decimal strings with exact int64 arithmetic, and a value that would pass the int64 maximum is refused.
- **No caller transactions.** Redis cannot join an application's business transaction. There is no staging or transactional-completion API on Redis; a business database reaches a Redis store through the outbox (14).

Not in SP-025 [choice]: Redis Cluster, Sentinel or any replica-promotion profile; an `appendfsync everysec` + `WAITAOF` profile; an operator-attested profile for managed services that hide `CONFIG`; Redis as an outbox **source**; cross-process wake-up notifications (Pub/Sub; workers poll, as on SQLite); certification (SP-028, G-4) and the performance qualification (SP-029).

## Changes

- (2026-11-11, bounded batch projection) `DispatchMany/1` and `FinishMany/1` can project the whole intent in the original script if it fits the inline batch limits in §4.3. Interrupted projection still resumes from the stored intent in 100-after-image chunks. Scripts are loaded by their content SHA and continue to read and write layout revision 1.
- (2026-11-10, exact stats) Two new projections, kept by the scripts as after-images: `idx:terminal` (sorted set, score terminal time ms, kept by every state change and removed on purge) and `jobtypes` (hash, field `<queue>\0<name>`, value the number of stored jobs, kept on dispatch and purge). An application registered before this binary has neither, so the first stats read runs the `StatsIndex` write op until it reports done: it walks `idx:created` 500 jobs per call, adds each job to both, and keeps its cursor in `meta.bf` (`done` at the end; a new application starts `done`). Dispatch and purge count a job in `jobtypes` only when the cursor has passed it, so a job is never counted twice. `StatsIndex` has no receipt and no manifest entry and the layout revision stays 1: the data is derived, and a binary that does not know it ignores it. New `mread` subs replace `qstats`: `jscan` (one page of at most 5,000 entries of a created, terminal or state index with each job's row, and a cursor), `jobnames`, `sstats`, `astats` (audit, paged by audit id). `GetJobStats` reads page after page within a 5 s budget and fails with `query_budget_exceeded` past it; each page is atomic, the whole read is not (16 §2.4).
- (2026-11-10, worker detail) PublishPresence/1 keeps the worker's detail (16 §2.3) as one more field of the worker row, `detail` (JSON text, dropped when over 16 KiB), and `mread workers` returns it. Two new `mread` subs: `qstats` (the queue row, the lowest score of `ready:<queue>:<priority>` for the three priorities as the oldest waiting eligible time, then the newest at most 5,001 jobs of `idx:created:q:<queue>` in the range) and `schedule` (the row, the current definition as stored, and the occurrences sorted as the SQL stores sort them, each with its job's state; more than 10,000 occurrence fields is the read budget). No layout revision, for the reasons of the attempt-usage entry below.
- (2026-11-09, attempt usage) Finish/1 and FinishMany/1 store the attempt's usage (16 §2.2) as one more field of the job hash, `a:<attempt>:usage` (JSON text), and `mread` returns it. No layout revision: the field is new data in an existing hash, the scripts ship with the binary, a binary that does not know the field never reads it, and cleanup deletes the whole hash as before.
- (2026-11-08) New document.
- (2026-11-08) SP-018's "What SP-025 must do", carried in: (1) a bound on how much one command may hold the application, with a measured renewal-wait budget (§4.3, §10, REDIS-07); (2) exact int64 counting in Lua with decimal strings, never `HINCRBY` or `INCR` in after-images (§3.2, REDIS-08); (3) decisions inside the script with the server clock, and the decision time stored in the intent so recovery after NotAfter publishes the original decision (§4.1, REDIS-09); (4) the qualified server's modules recorded next to the image digest, and a `CONFIG` that cannot be read treated as "profile unavailable" (§2.3, §6, REDIS-10).
- (2026-11-08) SP-018's three "Spec changes needed", applied:
  - **03 §2.1 / §9: a store-level Recovering outcome.** Added to 03 as a fourth way a driver call ends, `StoreRecoveringException` with code `store_recovering`, plus a `Recovering` receipt resolution (03 Changes 2026-11-08, SP-025). This file uses it in §4.2 and §4.4. 01 Changes 2026-11-08 (SP-025) records the public mapping.
  - **03 §9.3: new stable codes** `intent_too_many_keys`, `intent_too_large`, `intent_too_many_mutations` (InvalidRequest, refused before any write) and `counter_limit` (InvariantFault, no effect). Added to 03 in the same Changes entry, together with `history_discontinuity` (§2.4) and `store_profile_mismatch` (used by 20 §2.2 and §2.3 here, missing from the 03 table until now).
  - **Redis design, "declared keys":** a script cannot list its keys in advance when it picks the job from an index (Reserve, Recover, ReserveAndBegin, cleanup). The rule is now "at most 256 **distinct keys an intent writes**, all under the application's hash tag" (§3.1, §4.3). The design document is history and is not edited; this file is the normative rule.
- (2026-11-09) REDIS-07 finding: no operation can reach 1,024 after-images. An intent writes at most two after-images per hash or sorted-set key and one per string key, so the 256-key limit caps an intent near 512. The after-image limit (§10) therefore never binds before the key limit. REDIS-07 runs the largest real intents instead: a 120-job DispatchMany with 249 after-images and 248 keys, and a maximal public DispatchMany with 213 after-images. The 250 ms Renew p99 bound is unchanged and is judged on the reference host only (`RUNWICK_REFERENCE_HOST=1`).
- (2026-11-09) Concurrent writers are not recovery (§4.2, REDIS-16). A 100-item DispatchMany writes about 210 after-images, so it is always projected over several calls and its guard is visible between them. With 8 callers side by side, a call usually found another caller's intent pending, helped finish it, and then found the next caller's. The driver counted each of those as a failed try and gave up after 8 (writes) or 4 (reads and resolution) with `store_recovering`, on a healthy server: 23–29% of the queue shoot-out's 8-caller bulk calls. Now a Recovering answer whose recovery published the pending intent is progress: the call is resent for as long as its budget lasts (its NotAfter; 5 s for reads and resolution). `store_recovering` is reported only when recovery cannot finish the intent or the budget ends. Scripts, layout revision and limits are unchanged.
- (2026-11-08) Decisions taken where the sources were silent, each marked [choice] in the text: Recovering is an exception, not a `StoreStatus` (§4.4); scripts loaded by SHA, not Redis Functions (§4.1); the replay path writes the receipt-confirmation marker in the same call (§4.5); hex hash tag and key layout with two keys per job (§3); limits fixed, not options (§10); renewal-wait budget 250 ms p99 (REDIS-07); management pages examine at most 5,000 index entries (§7.2); `MODULE LIST` must be readable, like `CONFIG` (§2.3); replicas attached to the primary are allowed but reported (§2.3); no key TTLs anywhere (§3.2); 04's call-class ceilings reused instead of the design's older 4/8 lane budgets (§5); one admin connection for profile checks (§5); Redis ≥ 8.2 (§1); outbox profile string `redis.aof-always.v1` (§7).
- (2026-11-08) Public API additions recorded in 01 Changes: `UseRedis`, `RedisOptions`, `RunwickRedisMigrations`, `AcknowledgementProfiles.RedisAofAlways`, and `store_recovering` on `DispatchOutcomeUnknownException`.

## 1. Targets and boundaries

| Item | Rule |
|---|---|
| Client | StackExchange.Redis **3.4.20** (00 §6, §7). Lua scripts sent with `EVALSHA`; no other Redis client library, no ORM. |
| Server | Redis Open Source **≥ 8.2.0** [choice: the version SP-018 qualified; scripts need 7.0's shebang flags]. Describe records `redis_version`, `run_id`, `os`, `arch_bits` and the module list (§2.3). Older → `UnsupportedVersion engine_version_unsupported` at startup. Certification runs on a pinned image; SP-018 used `redis:8.2-alpine@sha256:fe9daae35cd4fd6ec22416aa4fe0cf6b3e36741e06ff45b9986253fe2a81c041` (Redis 8.2.11). A newer version is accepted and reported as `engine_certified: false` until a conformance report covers it (00 §9 rule 10). |
| Topology | One standalone primary, reached directly. Replica endpoints, Cluster mode and Sentinel are refused (§2.3). |
| Atomic partition | One application namespace = one hash slot (driver-capabilities §2). One application is never split across slots. |
| Deployment modes | All five modes of 00 §5, on any number of hosts that reach the one primary. |
| Notifications | None [choice]. A process wakes its own pools, scheduler and relay after its own writes (the existing in-process hint). Polling is enough for correctness (driver-contract-suite:OBS-01). |
| Acknowledgement profile | `AcknowledgementProfiles.RedisAofAlways = "redis.aof-always-single-primary-v1"`: a reply follows the script's writes being appended and fsynced under `appendfsync always` on the checked primary. A replayed reply is confirmed by a fresh write first (§4.5). |
| Caller transactions | None (§8). |
| Operations | Every operation of the PostgreSQL manifest at revision 17, batched operations included (§9.3). |

## 2. Registration, options and startup checks

### 2.1 API

```csharp
public static RunwickBuilder UseRedis(this RunwickBuilder builder, string configuration, Action<RedisOptions>? configure = null);

public sealed class RedisOptions
{
    public TimeSpan ProfileRecheckInterval { get; set; } = TimeSpan.FromSeconds(30);   // 5 s – 5 min
}
```

`configuration` is a StackExchange.Redis configuration string. Runwick takes from it only the endpoint, user, password, TLS settings, `defaultDatabase` and `connectTimeout` (capped at 5 s), builds its own `ConfigurationOptions` with the rest fixed (§5.2), and never logs it [choice: the client has settings that would break the protocol, and copying only the safe ones is simpler than refusing each unsafe one]. Refused at startup with `ConfigurationError` code `topology_unsupported`: more than one endpoint, a `serviceName` (Sentinel), or a Unix socket path that is not local. Refused with `invalid_policy`: a `$`-command rename map.

### 2.2 Application names in keys

The hash tag is the application's UTF-8 bytes as lowercase hex: application `billing` → `rw:{62696c6c696e67}:` [choice: the 02 §1 grammar already excludes braces, but hex leaves nothing for a future grammar change to break, and every tag is distinct for distinct names]. Scripts receive the metadata key as `KEYS[1]` and build every other key from its prefix; they refuse a prefix that does not match `^rw:\{[0-9a-f]{2,256}\}:meta$` with `InvariantFault invalid_transition` before any write.

### 2.3 Profile and topology check

At startup, on every reconnect, and every `ProfileRecheckInterval`, on a separate admin connection (§5.1):

1. `CONFIG GET` each of `appendonly` (`yes`), `appendfsync` (`always`), `no-appendfsync-on-rewrite` (`no`), `maxmemory-policy` (`noeviction`), `aof-load-truncated` (`no`).
2. `INFO everything`: `role:master`, `redis_mode:standalone`, `cluster_enabled:0`, `aof_enabled:1`, `aof_last_write_status:ok`, `redis_version` ≥ 8.2.0. Record `run_id`, `connected_slaves`, `maxmemory`.
3. `MODULE LIST`: record each module's name and version. Modules are allowed; they are reported, not judged [choice].
4. Any mismatch → the profile is **unavailable**: `UnsupportedVersion store_profile_mismatch`, naming the setting and the value seen (never the endpoint's secrets). A `CONFIG` or `MODULE LIST` call refused by an ACL (`NOPERM`) or missing is the value `unverifiable` and also makes the profile unavailable [choice for `MODULE LIST`]. At startup the worker does not start; at runtime admission stops (new Reserve, ReserveAndBegin, Dispatch and schedule materialization are not sent) and health reports `Unhealthy store_profile_mismatch` until a recheck passes. Calls already sent are resolved as usual.
5. `connected_slaves > 0` is allowed and reported as `replicas: <n>` in Describe [choice]: replication from the primary does not weaken it, but a promoted replica is a different history (§2.4) and not part of this profile.

SP-018 found that removing `CONFIG` with `rename-command` crashes the official 8.2 image (its modules assert at startup). The documentation says to restrict `CONFIG SET` with ACLs and leave `CONFIG GET` readable for Runwick's user.

### 2.4 History continuity

Each process keeps, per application, the store ID and the highest application revision (`rev`, §3.2) it has seen, and the server's `run_id`. After a reconnect, or when `run_id` changes:

- `rw:store` missing (FLUSHALL, empty server) or a different store ID → `UnsupportedVersion store_identity_mismatch`.
- Same store ID, but `rev` lower than the remembered one → `UnsupportedVersion history_discontinuity`.

Either stops admission and **invalidates local authority**: every lease this process holds on that store is treated as lost (the watchdog cancels the handlers, 05), and nothing resumes until an operator restarts the process against an intact history. A higher or equal revision with the same store ID resumes normally; a `run_id` change alone is an ordinary restart. A successful `PING` is never evidence of continuity. This check cannot detect every lost write (a history that lost writes and then advanced past the remembered revision passes it); it is the detector the design asks for, not a proof (§13).

## 3. Data layout

### 3.1 Keys

Every application key is `rw:{<hex>}:<suffix>`. The records are those of PostgreSQL revision 17 (04 §3 and later migrations); fields keep the PostgreSQL column names. Per-entity keys exist only for jobs [choice: two keys per job, so that a 100-item DispatchMany with every key kind stays within 256 distinct keys, §4.3]; everything else is an aggregate hash or index with entity IDs as fields or members.

| Records (04 / later) | Key suffix | Type | Content |
|---|---|---|---|
| store gate copy, application gate, publication | `meta` | hash | store ID, layout revision, epoch, migration mode, manifest digest; admission mode, required epoch and capabilities, gate revision; `rev`, `guard`, `intent`, `intentsha`; next IDs for events and audit |
| state counters | `counters` | hash | decimal strings |
| `queue_controls`, `pool_controls` | `queues`, `pools` | hash | field = name, value = canonical JSON row |
| `jobs`, `job_generations`, `job_attempts`, `job_concurrency_requirements`, `job_rate_requirements` | `job:<id>` | hash | job columns as fields; `gen:<n>` and `att:<attemptId>` fields hold canonical JSON rows; requirements immutable |
| `job_payloads` | `pay:<id>` | string | original payload bytes (binary-safe); profile and headers in `job:<id>` |
| ready candidates | `ready:<queue>:<priority>` | sorted set | score `eligible_at` ms, member job ID |
| lease expiries | `leases` | sorted set | score lease expiry ms, member job ID |
| `concurrency_scopes`, `concurrency_permits` | `scopes`, `permits` | hash | scope rows (fence and counts as decimal strings); permit field `<scopeId>:<jobId>` |
| `rate_scopes`, `rate_grants` | `ratescopes`, `grants:<scopeId>` | hash, sorted set | grants scored by grant time |
| `dispatch_keys` | `keys:active`, `keys:window`, `keys:window:exp` | hash, hash, sorted set | bindings; window expiry index |
| `storage_commands` | `rcpt:<commandId>`, `rcpt:exp:<class>` | hash, sorted set | receipt (incl. `confirmed_at`, §4.5); retention index by `retain_until` |
| `queue_events` | `events`, `events:data` | sorted set, hash | score = event ID; row by event ID |
| `management_audit` | `audit`, `audit:data` | sorted set, hash | score = audit ID |
| `worker_instances`, `worker_capabilities`, `worker_subscriptions`, `worker_pool_snapshots` | `workers`, `workers:exp` | hash, sorted set | presence rows; expiry index |
| `scaling_events` | `scaling`, `scaling:data` | sorted set, hash | |
| `schedules`, `schedule_definitions`, `schedule_audit_ranges` | `sched`, `sched:due` | hash, sorted set | |
| `schedule_occurrences` | `occ:<scheduleId>` | hash | field = occurrence key |
| `outbox_deliveries` | `outbox`, `outbox:exp` | hash, sorted set | field = delivery identity |
| `retention_policies`, `cleanup_progress` | `retention` | hash | |
| management projections (§7.2) | `idx:created`, `idx:created:q:<queue>`, `idx:created:s:<state>` | sorted set | score `created_at` ms |

Outside every application slot there is one key, `rw:store` (hash: store ID, `driver = redis`, layout revision, epoch, migration mode, manifest, digest), written only by the migration runner (§9) and read by Describe. Application scripts never touch it; RegisterApplication copies its gate fields into `meta` and the runner keeps the copies in step (§9.2).

### 3.2 Encodings

| Concern | Rule |
|---|---|
| UUIDs | 32 lowercase hex characters of the RFC 4122 big-endian bytes. Byte order = string order = `UuidOrder` (03 §2.4); equal scores in a sorted set are ordered by member bytes, which gives the `(eligible_at, job_id)` order of 04 for free. |
| Text | Stored as UTF-8 bytes. Scripts never compare strings with Lua `<` (Redis sets `LC_COLLATE` from the environment, so Lua's comparison is not guaranteed bytewise) [choice]; they compare with an explicit byte loop or let a sorted set order them. |
| Instants | Integer UTC epoch milliseconds, decimal text. Floor/ceil as 03 §2.4; exact as Lua numbers and sorted-set scores (below 2^53). |
| Durations | Integer milliseconds, as 04 §2. |
| Counters, revisions, fences | Decimal strings changed only by the exact helpers of the script prelude (add, compare), which refuse to pass the int64 maximum with `counter_limit`. Never `HINCRBY`, `INCR`, `INCRBY`, `HINCRBYFLOAT` or `ZINCRBY`: reapplying an after-image must not count twice. Event and audit IDs are also used as sorted-set scores, so they are refused past 2^53 − 1 with `counter_limit` [choice: unreachable in practice, but checked]. |
| JSON | Built by C# with the 02 canonical encoder and passed to scripts as bytes; scripts store and return them unchanged and never re-encode (cjson does not keep key order). JSON a script builds itself (receipt results, intent) is stored once and returned as stored. |
| Payloads | Original bytes in `pay:<id>`, never through JSON (02 §6). |
| Expiry | No protocol key ever has a TTL [choice, from the design's "no independent TTL"]. Leases, windows, receipts and retention end by explicit transitions; REDIS-13 checks every `rw:*` key has TTL −1. |

## 4. The write protocol

### 4.1 One command, one script

Each operation version is one Lua script with a `#!lua flags=…` shebang that declares it writes (so Redis checks memory and the `MISCONF` state before it runs any of it). Scripts are embedded in the binary, loaded with `SCRIPT LOAD` on connect and called with `EVALSHA`; `NOSCRIPT` loads and resends once (the script had not run) [choice: not Redis Functions, which are persistent server state another binary could replace; a script is identified by its SHA]. A script for a receipt-bearing operation:

1. Reads `meta`. Store ID differs → `store_identity_mismatch`. Gates as 03 §10.1 from the `meta` fields (store gate copy and application gate). A pending `guard` → returns `RECOVERING` (§4.2).
2. Reads the decision time `now` = Redis `TIME`, floored to 1 ms. Nothing the client sent is used as the time of a decision (03 §1 rule 3); NotAfter is compared with `now`. This replaces "fresh time after locks": a script runs alone on the server, so the time it reads is after every earlier command.
3. Receipt (03 §8.2): a receipt with the same operation and fingerprint replays (§4.5); a different one is `CommandConflict`; `now ≥ NotAfter` → `CommandExpired`.
4. Runs the operation's decision (§7) on what it read. Every refusal (`StaleLease`, `NoWork`, `CapacityBlocked`, `counter_limit`, the size limits of §4.3, …) returns a structured no-effect reply **before any write**. A script never raises a Lua error on purpose before its intent is stored.
5. **Intent.** One `HSET meta guard <commandId> intent <bytes> intentsha <sha1>` stores the complete decision: operation, command ID, fingerprint, decision time, every ID it chose (jobs, attempts, tokens, jitter), the reply, the expected next `rev`, and the ordered list of after-images (absolute values: whole hash fields, sorted-set members with scores, deletions).
6. **Projection.** Writes the after-images in order. The first call uses the projection budget in §4.3. A pending intent continues through recovery in chunks of 100 after-images (§4.2).
7. **Publication.** One `HSET meta rev <n+1> guard ""` publishes the revision and clears the guard together. The receipt row is one of the after-images, so the command is Applied exactly when this write happens.

Renew, RenewMany and PublishPresence have no receipt (03 §8.1); they skip step 3 and otherwise run the same steps. A no-effect outcome writes nothing at all. An intent accepted before its NotAfter is published even if publication happens after it: the deadline never cancels an accepted intent, and the published decision is the stored one (decision time, IDs, reply), not a new one.

### 4.2 Pending intents and recovery

A pending intent (non-empty `guard`) blocks the whole application:

- **Reads** (§4.6) return `RECOVERING` with the pending command ID. They never return empty work, a stale value, or a partial projection.
- **Writes** first finish the pending intent: they call the recovery script, which checks `sha1(intent) = intentsha`, that the intent names the guard's command and that its `rev` is `meta.rev + 1`, rewrites the next 100 after-images, and publishes after the last. The writer repeats this within its own remaining budget, then runs its own command. Each pending intent it finishes is progress, not a failed try: under concurrent writers it may find a new one every time, and it keeps going until its budget (NotAfter; 5 s for reads and resolution) ends. Reads and `ResolveCommand/1` follow the same rule. Rewriting an after-image twice is harmless, because they are absolute values: no counter moves twice and nothing is drawn again.
- A recovery call that fails (wrong key type, out of memory, the checks above) leaves the intent in place. The calling write ends with `store_recovering` (§4.4); the background class (§5.1) retries recovery every second while any call in the process has seen Recovering, with the 06 F-7 backoff after failures.
- **Nobody deletes an intent.** Not the driver, not cleanup, not a timeout. If a key has the wrong type or memory is exhausted, the application stays Recovering until an operator repairs the key or raises `maxmemory`; then recovery publishes and work continues (SP-018 REDIS-04). The runbook says this in those words.
- An intent that publishes a Reserve or Begin after the lease it grants has expired publishes it anyway. The lease is then historical: the worker's fresh ownership-checked Renew is refused (`lease_expired`, 03 §2.2), no handler starts, and Recover moves the job on. Expired ownership never authorizes a new handler invocation.

### 4.3 Limits

Checked in step 4, before the intent exists; exceeding one is `InvalidRequest` with no effect:

| Limit | Value | Code |
|---|---|---|
| Distinct keys the intent writes (all under the application's tag) | 256 | `intent_too_many_keys` |
| Serialized intent | 4 MiB | `intent_too_large` |
| After-images (one write command on one key each; a multi-field `HSET` or multi-member `ZADD` counts once) | 1,024 | `intent_too_many_mutations` |

The first call normally projects up to 100 after-images. `DispatchMany/1` and `FinishMany/1` can project all after-images inline when the intent has at most 256 after-images, 230 distinct written keys other than `meta`, 512 KiB of serialized intent and 8,192 after-image arguments excluding each operation and key. Intents above any of these inline batch limits use the normal 100-after-image projection budget. The acceptance limits above apply independently.

A script reads keys it discovers from an index (Reserve, Recover, ReserveAndBegin, cleanup), so it cannot declare them in `KEYS` beforehand; the limit counts the keys it **writes** [SP-018 spec change]. The layout of §3.1 keeps every request the public API allows inside these limits: a 100-item DispatchMany (02 §4.1) writes at most 2 keys per job plus fixed aggregates (under 230 keys), groups its aggregate writes into one command per key (under 800 after-images), and its payloads fit in the 2 MiB export bound (02 IC-20). Operations that work in batches of their own choosing (schedule materialization and catch-up, 13; cleanup, 17; recovery scans) shorten the batch until it fits.

**Renewal-wait bound** (SP-018 must-do 1). The guard serializes the application, so a Renew waits while a large intent is projected. SP-018 measured ~100 ms for a 503-mutation intent on a development machine. REDIS-07 measures Renew p99 on the reference host while maximum-size intents run continuously; the bound is **p99 ≤ 250 ms** [choice: one hundredth of the 25 s stop margin, 06]. If it is not met, the after-image limit is lowered (a constant change, recorded here) until it is; the acknowledgement contract does not change.

### 4.4 Recovering, as a result

A script reply `RECOVERING` becomes `StoreRecoveringException(code "store_recovering", PendingCommandId)` (03 Changes 2026-11-08, SP-025). It is not a `StoreStatus` [choice]: a status is a decision, and Recovering is not one. The caller acts as 03 says: a receipt-bearing command is resolved later by resending the same command (it may be the pending intent itself, in which case it is accepted and will publish); a Renew counts as not renewed; a read has no answer. The engine pauses admission for the application while it lasts and health reports `Degraded store_recovering`. Public: `ResolveAsync` returns `DispatchResolutionStatus.Recovering` (01); a convenience dispatch throws `DispatchOutcomeUnknownException` with code `store_recovering`.

### 4.5 Replay and the confirmation marker

A replayed receipt (same command ID, same fingerprint) is returned with `Replayed = true` and the stored result and decision time. Before replying, the same script call writes the receipt's `confirmed_at` field (always written, never `HSETNX`) [choice: one round trip, where the prototype used a separate call]. Under `appendfsync always` the reply to that write comes only after it, and everything before it in the AOF, is on disk, so the replay crosses a fresh write-and-reply boundary on this connection (design closure, "receipt-confirmation marker"). `ResolveCommand/1` does the same when it finds a receipt, so on Redis it runs on the write path. The marker is not a semantic change: it does not bump `rev` and is not an intent.

### 4.6 Read-only calls

ReadinessProbe, ListExpiredLeases, ListUnsupportedBacklog, EventsSince, EstimateDemand, Describe and the management reads run as read-only scripts (`flags=no-writes`): check the guard, the store and application gates, read, and return the values with the `rev` they belong to. One call is one consistent revision. They never write.

### 4.7 Error mapping

| Failure | Where | Classification | Code |
|---|---|---|---|
| Script reply with a status | — | decided | as the reply |
| `RECOVERING` reply | — | Recovering | `store_recovering` |
| `NOSCRIPT` | `EVALSHA` | not applied (did not run) | load the script, resend once |
| `OOM`, `MISCONF`, `READONLY`, `LOADING`, `BUSY`, `NOPERM` error replies | before the script runs (write-flagged scripts are checked first) | not applied | `store_unavailable`; `READONLY` and `NOPERM` also trigger a profile recheck (§2.3) |
| Connection not available (`BacklogPolicy.FailFast`: nothing queued) | before send | not applied | `store_unavailable` |
| Any other error reply from a running script (Lua error, `WRONGTYPE`, out of memory during projection) | after the script started | **outcome unknown**: an intent may be stored | `commit_outcome_unknown`; resolve by resending the same command, which replays, publishes the intent, or answers Recovering |
| Timeout or connection loss after the request was written | — | outcome unknown | `commit_outcome_unknown` (Renew: send a new Renew) |
| Server exits before replying (an AOF write or fsync error under `always`, SP-018 REDIS-02) | — | outcome unknown | `commit_outcome_unknown`; after restart the profile check and §2.4 run before any resend |

The driver records whether the request bytes reached the socket, as 04 §10 does for `COMMIT`. Error text and arguments are never logged; payload bytes never appear in a log or exception.

## 5. Connections and call classes

### 5.1 Connections

| Connection | Settings | Work |
|---|---|---|
| Control multiplexer | protocol (§5.2) | Renew, RenewMany, Finish, FinishMany, ReleaseReservation and their resolution |
| Work multiplexer | protocol | everything else, including recovery and cleanup |
| Admin connection | `AllowAdmin = true`, for §2.3 and management diagnostics (18 §7) | `CONFIG GET`, `INFO`, `MODULE LIST` |
| Migration connection | own, opened and closed by the runner | §9 |

The call classes and ceilings of 04 §9.1 apply as **logical in-flight limits**: renewal 4 and settlement 4 on the control multiplexer; admission 4, producer/admin `MaxConcurrentCalls` and background 2 on the work multiplexer [choice: 04's tuned ceilings, not the design's earlier 4/8 lane budget]. So `local_capacity_exceeded` and slot waits behave as on PostgreSQL. At most one unresolved owner command per attempt. A multiplexer is one socket; qualification (SP-029) records the actual socket count. A separate connection cannot keep a Renew from waiting behind a long script on the same server; §4.3 bounds that wait.

### 5.2 Client settings (protocol multiplexers)

| Setting | Value |
|---|---|
| `BacklogPolicy` | `FailFast`: a request is never queued while disconnected and never sent late |
| `AbortOnConnectFail` | `false` (the process keeps reconnecting; commands fail fast meanwhile) |
| `ConnectTimeout` | ≤ 5 s |
| `AsyncTimeout` | `ControlRequestTimeout` (06, 5 s); every call is also bounded by its operation budget |
| `AllowAdmin` | `false` |
| Command flags | never `FireAndForget`; never `DemandReplica`/`PreferReplica` |
| `ClientName` | `runwick:<application>:control` / `:work` / `:admin` |

Reconnect never creates a new command identity: an unresolved command is resolved by resending it (NotAfter is enforced by the server clock in the script, so a late arrival is `CommandExpired`, SP-018 REDIS-03) and ownership is re-established with a fresh Renew before user code runs.

## 6. Describe and the store gate

`Describe/1` returns the 03 §3.1 descriptor from `rw:store` and the application's `meta`, and adds to the store descriptor: `driver: redis`, `redis_version`, `run_id`, `engine_certified`, `modules` (name and version each), `profile: redis-aof-always-single-primary-v1`, `profile_status` (`verified` | `unavailable` with the failing setting), `replicas`, `maxmemory`, `topology: standalone-primary`, and the limits of §4.3. Qualification evidence (SP-028) records the image digest next to the module list (SP-018 must-do 4). The binary's readable and writable Redis layout revisions are `[1]`; the driver key selects this revision line, so a PostgreSQL or SQLite revision number never applies here.

## 7. Operations

Each operation makes the decisions 04 §7 (and 08, 09, 10, 11, 13, 14, 16, 17, 19 for later operations) give for PostgreSQL, in the same order, with the same records, values, events, receipts and replies, inside one script (§4.1). The table lists only what differs.

| Operation | Reference | Redis difference |
|---|---|---|
| RegisterApplication/Queue/Scope | 04 §7.1 | RegisterApplication creates `meta` from the store gate passed by the client and checked against it; seeds never overwrite |
| Dispatch/1, DispatchMany/1 | 04 §7.2, 14 (dispatch core) | No locks. Key bindings, scopes and jobs are decided in the PostgreSQL order, so events and reply order are identical. Writes to one aggregate key are grouped into one after-image |
| ManagementDispatch/1 | 19 §6.2 | Dispatch core + audit row in the same intent |
| Reserve/1 | 04 §7.3 | Candidates from `ready:<queue>:<priority>` with `ZRANGE … BYSCORE -inf <now> LIMIT`, in `(eligible_at, job_id)` order after the cursor; supported contracts passed as arguments and looked up in a Lua table; the 512-tuple and scan bounds of 04 |
| ReadinessProbe/1 and /2, ListUnsupportedBacklog | 04 §7.4, 09 | Read path (§4.6) |
| Begin/1 (rate-aware) | 04 §7.5, 10 §4 | Window count with `ZCOUNT grants:<scope> (<now − window> <now>`; grants added as after-images; T20 deferral writes its receipt as there |
| Renew/1 | 04 §7.6 | Write path, no receipt (R1) |
| RenewMany/1 | 03 Changes 2026-11-02 (PF-06) | One script, one intent. Each member is decided alone; a refused member, or one with `permit_set_mismatch`, contributes no after-images and does not affect the others (OWN-04). No intent is stored when no member renewed |
| ReserveAndBegin/1 | 03 Changes 2026-11-02 (PF-06 batched claim) | One script, one intent, one receipt with `items[]`; each claimed job sees the permits and grants of the earlier ones. Any `InvariantFault` returns no effect for the whole command (decided before the intent) |
| FinishMany/1 | 03 Changes 2026-11-02 (PF-06 batched finish) | One script, one intent, one Finish receipt per applied item. No intent when every item was refused or replayed |
| Finish/1, Recover/1, ReleaseReservation/1, shared close | 04 §7.7–§7.10 | — |
| ListExpiredLeases/1 | 04 §7.11 | `ZRANGE leases BYSCORE -inf <now> LIMIT 0 32` on the read path |
| PublishPresence/1 | 04 §7.12 | Write path, no receipt |
| ResolveCommand/1 | 04 §7.13 | Write path when a receipt exists (marker, §4.5); a pending intent for that command → `Recovering` |
| Cancel/ManualRetry/RerunSucceeded, SetQueueState, EditScopePolicy | 08 | Audit row in the same intent |
| ActivateCapabilities, EditRatePolicy | 10 | — |
| Schedule operations | 13 §6 | Materialization batch shortened to fit §4.3 |
| SetPoolOverride, EstimateDemand, RecordScalingEvent | 11 | EstimateDemand on the read path with `ZCOUNT` over `ready:*` |
| ReceiveOutbox/1 | 14 §5 | Delivery identity in `outbox`; the delivered mark's `confirmed_profile` is `redis.aof-always.v1` [choice] (14 Implementation choice 4) |
| RecordDisclosure/1 and the management reads | 16 §2–§3, §6 | §7.2; disclosure on the write path |
| EditRetentionPolicy/1, CleanupBatch/1 | 17 | §7.3 |
| EventsSince/1 | 08 §7.2 | §7.1 |

### 7.1 Events

Event IDs come from `meta`'s next-event counter, assigned inside the intent. Intents of one application are published one at a time, and a read cannot see an unpublished one, so event IDs appear in publication order and an event with a smaller ID can never appear later. `EventsSince/1` reads `ZRANGE events BYSCORE (<after> +inf LIMIT 0 <limit>` and the rows from `events:data`. `EventCursor.TxId` is always 0 and `Partial` always false. EVT-01 and EVT-02 hold as stated in 08.

### 7.2 Bounded management projections

Every mutation keeps the `idx:created*` sorted sets (§3.1) in step as after-images. `ListJobs` (16 §2) pages in `created_at DESC, job_id DESC` order from the most selective index for its filters (queue, then state, else all), checks the remaining filters (job name, the other of queue/state, the caller's allowed queues, the time range) on each entry, and stops when the page is full. One page is one read script and examines at most **5,000** index entries [choice]; a page that would need more returns 503 `query_budget_exceeded`, as a PostgreSQL statement timeout does (16 §2), never a partial page. The other lists (queues, workers, scopes, schedules, audit) are keyset pages over their aggregate hash fields or score indexes with the same 5,000-entry bound. Cursors are 16's signed cursors; their keys are the same values as on PostgreSQL.

### 7.3 Retention

17's classes, horizons, guards and loop apply unchanged. Each `CleanupBatch/1` is one intent that selects from the retention indexes (`rcpt:exp:*`, `keys:window:exp`, `grants:*`, `workers:exp`, `outbox:exp`, event and audit scores, terminal jobs from `idx:created:s:<state>`) and deletes with explicit after-images (`DEL job:<id> pay:<id>`, `HDEL`, `ZREM`). There is no "skip locked": the guard serializes cleanup with live work, so a batch is shortened to fit §4.3 (at most 100 jobs per purge batch: 200 keys) and the 17 pacing keeps it from starving renewals. Detail expiry deletes `pay:<id>` and the error fields and marks the job "detail expired". A receipt is never deleted while an intent names its command.

## 8. Caller transactions

None. Redis cannot take part in a business database's transaction, and a `MULTI`/`EXEC` beside it would not be atomic with it (driver-capabilities §7). `UseRedis` registers no staging dispatcher and no transactional completion service; the descriptor declares enlistment `none`. An application whose business data is in PostgreSQL or SQLite stages through `Runwick.Outbox.Postgres` or `Runwick.Outbox.Sqlite` (14) with this store as the destination. TX-02 to TX-04 are not advertised; TX-01 holds in its Redis form (nothing to enlist, so nothing is written).

## 9. Migrations and layout revision

### 9.1 API

```csharp
public static class RunwickRedisMigrations
{
    public static Task<RedisMigrationResult> MigrateAsync(string configuration, CancellationToken cancellationToken = default);
}
public sealed record RedisMigrationResult(Guid StoreId, long LayoutRevision, IReadOnlyList<string> AppliedPhases);
public sealed class RedisMigrationException : Exception { public string Code { get; } }
```

Codes: `store_unavailable`, `engine_version_unsupported`, `store_profile_mismatch`, `topology_unsupported`, `schema_not_empty`, `migration_lock_timeout`, `schema_validation_failed`, `migration_phase_failed`. The CLI's `migrate` and `doctor` (19) gain `--store redis` in SP-025.

### 9.2 Runner

1. Connect (§5.2 settings, plus admin) and run the §2.3 check. A store is never installed on a server outside the profile.
2. Take the migration lock: `SET rw:migration-lock <token> NX PX 60000`, retried for 60 s, then `migration_lock_timeout`; released with a compare-and-delete script.
3. `rw:store` exists: compare its layout revision and manifest digest with the binary's. Equal → no-op returning the store ID. Other `rw:*` keys exist without `rw:store` (`SCAN MATCH rw:* COUNT 1000` to the end, at install only) → `schema_not_empty`.
4. Write `rw:store` in one `HSET`: new store ID (`Guid.NewGuid()`), layout revision 1, epoch 1, `migration_mode = normal`, manifest and digest. `AppliedPhases = [expand, validate, activate]`.
5. Later revisions change `rw:store` and then each application's `meta` copy (found by `SCAN MATCH rw:{*}:meta`), one single-`HSET` script per application, which refuses while that application has a pending intent. A semantic change that needs drain follows 04 §8.4's drain → activate steps.

### 9.3 Manifest

Same canonical encoding and digest domain as 04 §8.5 (`runwick.manifest.v1`), with `layoutRevision: 1` in place of `schemaRevision`. `operations` = the PostgreSQL revision-17 operation set and versions (batched operations, `CleanupBatch/1`, `EditRetentionPolicy/1` and `ManagementDispatch/1` included). The script SHAs are not in the manifest; the operation versions are.

## 10. Defaults

| Name | Value | Bound |
|---|---|---|
| Distinct keys written per intent | 256 | fixed |
| After-images per intent | 1,024 | fixed; lowered if REDIS-07 fails (§4.3) |
| Intent size | 4 MiB | fixed |
| Default inline and recovery after-images per call | 100 | §4.3 describes bounded full projection for dispatch and finish batches |
| Management page examination | 5,000 index entries | fixed |
| Read batch (lists, recovery scans) | 100 rows | fixed |
| `ProfileRecheckInterval` | 30 s | 5 s – 5 min |
| Background recovery retry while Recovering | 1 s, then 06 F-7 backoff | fixed |
| `ConnectTimeout` / `AsyncTimeout` | 5 s / `ControlRequestTimeout` | ≤ 5 s / 06 |
| Migration lock | 60 s | fixed |
| Renewal-wait budget (test bound) | p99 ≤ 250 ms | REDIS-07 |

These numbers are listed here rather than in 06, as for SQLite (06 Changes 2026-11-08). The call-class ceilings of 06 §1.7 apply unchanged.

## 11. Schema revision

No PostgreSQL or SQLite schema revision. Redis layout revision 1, its own counter.

## 12. Tests

Harness changes:

- `DriverFixture` gains `StoreKind.Redis`. It starts the pinned image (§1) through Testcontainers with the profile's settings, or uses `RUNWICK_TEST_REDIS` (a configuration string to an existing server). Each test uses its own application names, so tests share a server. `ReadCommittedSnapshot()` reads every key of the application with read scripts at one `rev` and fails the test (does not return a snapshot) while an intent is pending.
- The Contract.Tests theories run over `StoreKind.Redis` too. Redis-only cases go to `tests/Runwick.Redis.Tests`, which replaces `tests/Runwick.Redis.Prototype.Tests`; the prototype's server control (`RedisNode`: `kill -9`, pause, restart, tmpfs) moves with it.
- `Runwick.Harness.Peer` accepts `--store redis --configuration <string>`.
- Fault points (driver-contract-suite §2): the runner's `IFaultInjector` adds `AfterIntent`, `AfterProjection(n)`, `BeforePublication` and `AfterPublicationBeforeReply`. Scripts take a fail-after-N-writes argument that the production client always sends as 0 and that no public API sets.
- The disk-full and crash cases need Docker and, for tmpfs, root; without them they are reported as not run, which blocks certification (no hidden skips).

Applicable driver-suite cases on the AOF-always profile (all must pass): CMD-01–06, ADM-01–05, RUN-01–02, OWN-01–05 (OWN-04 through `RenewMany/1`), END-01–04, UNI-01–04, RATE-01–04, SCH-01–05, ISO-01, ENC-01, CFG-01, driver-contract-suite:OBS-01, RET-01, OUT-01–02, TX-01 (Redis form, §8), the SER cases of identity-and-serialization that touch a store, and EVT-01–02. The ReserveAndBegin, RenewMany and FinishMany cases of 15 run on Redis too, plus MGMT-13 against a Redis-backed management host. Not advertised: TX-02–04. Declared unsupported (CFG-01 shows each refusal): Cluster, Sentinel, a replica endpoint, `appendfsync everysec`, RDB-only and no-AOF servers, eviction policies, caller enlistment.

Redis cases (trait `Item=SP-025`). REDIS-01–06 are SP-018's cases rerun on the real provider and its full command set; REDIS-07 onward are new:

| Case | What it runs |
|---|---|
| REDIS-01 | Every write prefix of every operation (rate grants, permits, fences, uniqueness windows, cancellation and receipt cleanup included, which SP-018 did not cover), including large intents between batches; `kill -9` of all interrupted states and restart from the AOF; reads hide every partial state; resending each command yields the byte-identical state and reply of a clean run; acknowledged commands survive a kill during AOF rewrites; a torn last AOF write refuses startup until `redis-check-aof --fix` |
| REDIS-02 | Lost reply → same reply on resend, changed body → `CommandConflict`; acknowledged result then `kill -9` → present after restart and the confirmation marker survives a second crash; disk full under `always` → server exits before replying, unknown outcome, clean restart, resend applies once |
| REDIS-03 | Command past NotAfter → `CommandExpired`, resolution "not applied"; paused server (`docker pause`) runs the command after NotAfter → refused by server time, resend refused too; server down → fails fast, nothing runs late; Begin published after its lease expired → fresh Renew refused, no handler, Recover moves the job |
| REDIS-04 | Limits (257 keys, 1,025 after-images, 4 MiB + 1) refused before any write with their codes; wrong key type after the intent → Recovering until repaired, then published; memory full → new commands refused before running, an accepted intent stays Recovering until `maxmemory` is raised, never deleted |
| REDIS-05 | Profile check: the selected profile passes; a replica, `everysec`, image defaults, `allkeys-lru`, Cluster mode, a Sentinel `serviceName` and an ACL that denies `CONFIG` are each refused with their reason; a runtime `CONFIG SET appendfsync everysec` stops admission within one recheck interval |
| REDIS-06 | 8 worker connections, 1,200 mixed commands (schedule fires racing for one slot, outbox receives, dispatches, reserve/begin/finish, management controls with audit) with injected failures; every concurrent read is a whole revision or Recovering; counts of fires, audit rows, deliveries and jobs agree at the end |
| REDIS-07 | Renewal wait (SP-018 must-do 1): on the reference host, the largest reachable intents run back to back while 64-lease `RenewMany` calls run. Renew p99 ≤ 250 ms. A maximal DispatchMany (100 items, active and window keys, several scopes) is accepted. Ordinary 100-job adds and 32-job finishes publish inline, and larger intents still complete through recovery |
| REDIS-08 | Exact counters (must-do 2): a fence and a counter at int64 max − 1 → +1 applies, the next → `counter_limit`, nothing written; an event ID at 2^53 − 1 → `counter_limit`; reapplying a published intent leaves every counter unchanged; the script bundle contains none of `HINCRBY`, `INCR`, `INCRBY`, `HINCRBYFLOAT`, `ZINCRBY` (source check) |
| REDIS-09 | Server clock (must-do 3): producer and worker clocks skewed ±1 h; every decision time, lease and eligibility follows Redis `TIME`; an intent recovered after its NotAfter publishes the original `DecidedAt`, IDs and reply |
| REDIS-10 | Evidence (must-do 4): Describe reports version, `run_id`, modules and profile status; an ACL that denies `MODULE LIST` → profile unavailable; the conformance report records the image digest with the module list |
| REDIS-11 | Encodings: UUID members order as `UuidOrder`, equal-score ties follow job ID bytes, ms floor/ceil at equality, non-ASCII byte order without Lua `<` (source check), hex hash tags for two applications whose names differ only in case land in distinct namespaces (ISO-01), a forged prefix is refused before any write |
| REDIS-12 | Events and projections: 3 writer processes and a tailing `EventsSince` reader while cleanup deletes old events: no event returned twice or skipped; `ListJobs` pages match a reference model for each filter combination; a filter that would examine more than 5,000 entries → 503 `query_budget_exceeded` |
| REDIS-13 | Retention: RET-01 (a)–(g) on Redis; every cleanup intent stays within §4.3; after a full pass every `rw:*` key has TTL −1 |
| REDIS-14 | Migration: empty server, re-run no-op, foreign `rw:x` key → `schema_not_empty`, concurrent migrate from two processes → one installs, the other no-op or `migration_lock_timeout`, a server outside the profile is refused before any write |
| REDIS-15 | History: `FLUSHALL` → `store_identity_mismatch`; an older AOF restored (revision goes back) → `history_discontinuity`, local leases cancelled, nothing resumes; a deliberately lossy replica promotion is detected where the revision went back and is **reported as a limitation**, not counted as no-history-loss conformance (driver-contract-suite §5, §7) |
| REDIS-16 | Concurrent writers on a healthy server: 8 callers each sending 100-item DispatchMany calls side by side (80 calls), each resolving its own operation right after; no call ends `store_recovering` (outcome unknown or not applied), every job is stored exactly once, the snapshot has no invariant violations |

## 13. Must not claim

- No HA: one primary. Failover, Sentinel promotion and Cluster are not supported, and a promoted replica may have lost acknowledged jobs, receipts, grants, permits and fences.
- No durability beyond the primary's disk and its fsync behaviour; a restored AOF or RDB is a history discontinuity (00 §9 rule 12). §2.4 detects a revision that went back; it cannot prove that no write was lost.
- A process kill or `docker pause` is not a power-loss test (driver-contract-suite §1); REDIS-01–03 claim crash consistency under the profile, nothing more.
- An application is one hash slot: its throughput is bounded by one server thread, and one large command delays every other command of that application (§4.3).
- No atomicity with any business database; the outbox is at-least-once with destination deduplication (14 §8).
- Redis results say nothing about throughput parity with PostgreSQL, and a comparison against Redis without `appendfsync always` is not a driver ranking (driver-contract-suite §6).
