# Redis design closure — planning closure 2 of 6

Status: implementation baseline, 2026-10-29. Closes D-09's representation/profile choices, not the SP-018 proof gate. Retains the [BullMQ implementation review](bullmq-redis-review.md) and common driver semantics.

## Selected representation

Use the application-scoped recoverable mutation intent described in the driver matrix as the **selected design**, rather than relying on a multi-write script to roll back on error. All keys for one application share its encoded hash tag. Preserve separate Begin accounting, exact rate grants, receipts, administrative audit and the new outbox delivery identities.

Before accepting an intent, validate bounded input, types, expected revisions, current authority and exact encodings. An accepted intent stores complete idempotent after-images and its original decision/IDs before projections. Every authoritative read/write checks the guard. Pending state returns Recovering; it cannot appear as empty work, committed success or confirmed rollback. Final publication updates the application revision/result and clears the guard in one metadata operation. Recovery reapplies assignments without incrementing counters or redrawing jitter. Acceptance before a deadline can require recovery after it; expired historical ownership never permits a new handler invocation.

Initial limits: 4 MiB serialized intent, 1,024 projection mutations, 256 declared keys; read/projection batches at most 100 rows. Commands exceeding limits fail before accepting the intent. Scheduler/cleanup operations shorten their batches; a single oversized atomic job fails explicitly. Deadline/work limits are preflight and between-command controls; a client timeout cannot preempt a running script. Measure script blocking time and recovery cost before qualification.

Only protocol writers may mutate this namespace. No eviction or independent TTL may erase correctness guards. Recovery checks key-set/fingerprint equality, generation/epoch and publication revision. Memory pressure can block recovery and the whole application; operators add capacity/fix the failed dependency before recovery, never delete the intent to make the queue look healthy.

## Initial persistent profile

Select `redis-aof-always-single-primary-v1`: direct access to one primary, AOF enabled, `appendfsync always`, `no-appendfsync-on-rewrite no`, `maxmemory-policy noeviction`, and `aof-load-truncated no`. Certification starts with a pinned Redis 8.2 server image; the exact patch/digest is a build-harness input recorded at SP-018, not an unverified current-version claim. Existing StackExchange.Redis 3.4.20 remains the .NET client baseline.

Redis documents that the always policy fsyncs an appended batch before replying. That is the basis for this selection; filesystem/server error behavior and intent-prefix recovery still require executable tests. It is not a guarantee against disk loss or arbitrary promotion. [Redis AOF policy](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

This profile deliberately does **not** depend on WAITAOF connection pinning. On replay after reconnect, write a bounded receipt-confirmation marker under the protocol before acknowledging persistence, so the confirmation crosses a fresh write/reply boundary rather than relying on an arbitrary read. Marker identity/result is idempotent and scoped to the receipt; retain it with that receipt. AOF write/fsync errors or unverifiable configuration make the profile unavailable.

Periodic-AOF/RDB-only configurations are not silently substituted. A future `everysec + WAITAOF` profile would need mutation and barrier on the same proven physical connection; a new connection cannot certify earlier connection writes. It is excluded from the initial shipped persistent profile. [WAITAOF connection semantics](https://redis.io/docs/latest/commands/waitaof/)

## Client and topology boundaries

Use three long-lived client lanes per worker/store group: control, work and optional notifications. Budget control at 4 concurrent protocol requests and work at 8; at most one unresolved owner command per attempt. These are logical in-flight limits, not a promise that one multiplexer is one socket. Record actual sockets/background connections in qualification. Notifications remain lossy hints; correctness reads use the primary and publication guard.

Disable offline mutation backlog and fire-and-forget writes. Reconnect creates no fresh operation identity: resolve old immutable requests, enforce NotAfter server-side and reestablish fresh ownership before invoking. Configure finite timeouts within D-05 budgets; keep an I/O slot until its actual provider task ends. The client exposes configurable disconnect backlog behavior, but configuration alone does not prove the complete Spindle protocol. [Client configuration](https://seredis.dev/Configuration.html)

Do not configure automatic Sentinel promotion, multi-primary routing or Cluster MOVED retries in the initial persistent profile. Same-slot key layout is retained for a future certified cluster topology. A role/endpoint/history change stops admission and invalidates local authority; resume requires the declared intact-history procedure, not merely a successful PING. Hostname continuity alone does not prove durable history continuity.

Redis remains a required planned v1 driver with this narrower, explicit topology. Unsupported promotion/cluster profiles are rejected at startup, not advertised as equivalent durability. A separate connection cannot isolate renewal from a long script on the same server; script latency is a release gate.

## Fixed prototype exit criteria and fallback

SP-018 must inject failure after every intent/projection/publication write; test AOF crash/rewrite/restart, disk-full/write failures, wrong key types, exhausted memory, truncated logs, reconnect/replay, near-limit counters and concurrent scheduler/outbox/audit writes. Every externally visible result must belong to one fully published revision or return Recovering. Verify that acknowledged results survive the declared same-primary restart profile and that no stale lease can authorize execution.

If projection recovery is incomplete, repair the intent protocol and rerun the same cases. If always-fsync latency exceeds the accepted workload budget, optimize batching/indexes and bounded work **without changing the acknowledgement contract**. If the client replays expired operations, correct its lane/backlog strategy within the selected profile. If any correctness obligation remains unprovable, keep Redis unavailable and block full-v1 acceptance; continue PostgreSQL implementation. Do not quietly drop Redis or substitute a weaker profile. A scope/guarantee change is a concrete review exception, not another automatic planning topic.

REDIS-01: all write prefixes/restarts; REDIS-02: persistence error/reply loss; REDIS-03: expired commands and reconnect; REDIS-04: bounded scripts/memory and control pressure; REDIS-05: incompatible topology/configuration rejection; REDIS-06: outbox/audit/schedule parity. Map to SP-018, SP-025 and SP-028–029. These are unexecuted acceptance obligations.
