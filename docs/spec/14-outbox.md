# 14 — Transactional outbox: source adapters, relay and `ReceiveOutbox/1` (SP-017)

**Sources:** [transactional-outbox.md](../architecture/transactional-outbox.md) (all sections, OUTBOX-01 … OUTBOX-07); [storage-command-protocol.md](../architecture/storage-command-protocol.md) "Outbox destination extension"; [driver-contract-suite.md](../architecture/driver-contract-suite.md) §4 (TX), OUT-01, OUT-02, CMD-01 … CMD-05, ISO-01; [performance-acceptance-plan.md](../architecture/performance-acceptance-plan.md) (P-09); 02 §7–§9; 03 §2.1, §4, §10; 04 §5, §6, §8; 06 §1.4, §1.7; 07 §4 (CMD), §8 (TX), §9 (ISO-01); 15 §10–§11.

## Changes (2026-11-02, idle caps)

- **Idle poll.** While the relay's wake listener is healthy (from a successful (re)subscription until the session is lost or refused, or the listener stops), a claim loop that claimed nothing with a free slot waits **5 × `IdlePoll`** (5 s by default). Otherwise it waits `IdlePoll` (1 s), as before; a full relay or one paused by renewal uncertainty always waits `IdlePoll`, and a freed slot ends any wait. A lost session ends the current wait at once.
- **Interface.** `IOutboxWakeSource.ListenAsync(onWake, onSubscribed, onLost, ct)`: a (re)subscription is now reported apart from a notification (both still end the idle wait), and a lost or refused session calls `onLost` before the reconnect back-off.
- **Cost.** Rows that become claimable by time (a `RetryLaterAsync` back-off, an expired lease of another owner) are not notified; an idle relay with a healthy listener finds them within 5 s instead of 1 s. Retry back-off starts at 1 s and the relay lease is 60 s, so this adds at most 4 s to an already delayed delivery.
- **Why 5 s.** Five times fewer idle claim transactions. The SQLite source has no notifications and keeps 1 s.

## Changes (2026-11-02, PF-07)

Optional wake channel on the PostgreSQL source. Stage's INSERT notifies `runwick_outbox` (empty payload) when it inserts a row; PostgreSQL delivers it only when the caller's transaction commits, so a rolled-back stage never wakes the relay. A relay whose source offers notifications listens on one dedicated unpooled session (outside the source's two-connection pool, application name `<name>:listen`), reconnecting after 1 s doubling to 30 s. A notification, and every (re)subscription, ends the claim loop's idle wait (`IdlePoll`) early. The SQLite source has no notifications. Claims, leases and polling are unchanged; with every notification lost the relay delivers on its normal idle poll.

## Changes (2026-11-01, SP-017 owed items)

- (2026-11-01) §10 item 2 is closed: the §7 golden vectors are frozen in `tests/vectors/wire/outbox.json`, cases OB-0 … OB-8. `tests/vectors/tools/canonical.mjs` generates them from this section, independently of the C# code. OB-0 is the GV-B0 intent with a 7-day horizon from 2026-10-30T10:00Z. OB-1 … OB-6 each change one hashed member; OB-7 and OB-8 change only the transport command, so they keep the OB-0 delivery digest. They are checked by `OutboxVectorTests` (Core) and, on both sources, by `HashDomainTests`. That test has the Node encoder recompute the stored `delivery_fingerprint`, the destination mapping and the `ReceiveOutbox` receipt fingerprint from a delivered row.
- (2026-11-01) §2.1 step 5, made precise. Stage rolls back to its savepoint on **every** exception, cancellation included, and never uses the caller's token for that rollback. Before this fix, a token cancelled after the INSERT skipped the rollback. On SQLite the staged row then stayed in the caller's transaction and committed with it. On PostgreSQL the caller's transaction was left aborted. If the rollback itself fails because the provider already ended the transaction (a broken connection, or a SQLite automatic rollback), that failure is swallowed, so the caller sees the original error unchanged (TX-03). Test: `SavepointTests`.
- (2026-11-01) SQLite source review (Microsoft.Data.Sqlite 10.0.14). `SqliteTransaction` implements `Save`/`Release`/`Rollback(name)` synchronously (`SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT`) and does not override the async forms. `SaveAsync`/`ReleaseAsync`/`RollbackAsync(name)` are therefore the `DbTransaction` defaults: they check the token first and then run synchronously. An await between them does not leave the connection, so it is safe across `ConfigureAwait(false)` continuations. `SqliteConnection` is not safe for concurrent use: two concurrent Stage calls on one caller transaction are outside the contract, as for any ADO.NET connection. Commands are bound to the caller's transaction, and readers are disposed before `RELEASE`. The relay's `BeginTransactionAsync` (isolation `Unspecified`) issues `BEGIN IMMEDIATE`, as §4.1 step 2 requires. A deferred caller transaction can get `SQLITE_BUSY` on its first write; that error propagates unchanged, and the whole-unit retry is the caller's (TX-03).

## Changes (2026-10-31, SP-017)

New document. It replaces the "not in M2: outbox delivery identities" placeholders in 03 and 04 §5.1.

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution |
|---|---|---|
| C-1 | transactional-outbox proposes `StageAsync(job, tx, destination, options, ct)`; the plan asks for one destination per relay group | The destination is bound once per host (`OutboxDestination`, §2.3). `StageAsync` takes no destination argument. A second destination is a second host/relay group (not in M3) |
| C-2 | "Persist each new request's exact bytes/ID in the source row before sending" versus "the envelope is immutable" | The source row stores the immutable intent once, plus the current transport command ID and NotAfter (§3.1). The request bytes are a pure function of (intent, source identity, AcceptUntil, command ID, NotAfter), so storing the two transport fields reproduces the exact request. No second copy of the payload is written |
| C-3 | A generic `PreparedDispatch` carries its own operation ID and NotAfter, which expire long before AcceptUntil | The outbox uses the prepared envelope only as the frozen intent codec. Its operation ID and NotAfter are ignored on the outbox path; the transport command ID and NotAfter are the outbox's own (§6.2). The receiver revalidates the intent digests (02 §9.3) |
| C-4 | "Receipt lock precedes delivery-identity lock, which precedes dispatch keys", but there is no committed delivery row before the first delivery | The delivery identity is locked with a transaction-scoped advisory lock on a hash of `(application, source_store_id, message_id)` at rank 3a (§5.3). No placeholder row is committed; a hash collision only serializes two unrelated identities |
| C-5 | After AcceptUntil every transport command is past its own NotAfter (NotAfter ≤ AcceptUntil), so no command can return a retained mapping | Resolution after the horizon uses a read-only lookup, `FindDeliveryAsync` (§5.5), with no receipt and no effect |
| C-6 | "Policy drift causes visible rejection" versus the existing rule that a prepared envelope applies its frozen policy | Frozen policy values are applied as frozen, never rewritten. Drift that makes the intent undeliverable (the job contract is no longer registered, the serializer profile or envelope format is unknown, the destination binding changed) blocks the message with a reason (§4.4). Ordinary dispatch drift (`PolicyDrift` on a Reused result, 02 §7.4) is recorded in the mapping and the delivered row |

Implementation choices *[choice]*:

1. Source UUID: generated by the installer (`gen_random_uuid()` on PostgreSQL, `Guid.NewGuid()` on SQLite) and never regenerated (§3).
2. Source time: the source database clock, millisecond floor (`clock_timestamp()`; SQLite `julianday('now')`).
3. Staging never contacts the destination. The destination store ID comes from `OutboxDestination.StoreId` or, when unset, from the host runtime after its first handshake; otherwise Stage fails with `outbox_destination_unknown` before any write.
4. The delivered mark records the profile `postgres.commit.v1` (the PostgreSQL destination confirms at COMMIT). Other profiles arrive with their destinations (Redis: SP-025).
5. The relay is an `IHostedService` registered by the adapter's DI extension. `OutboxRelayOptions.Enabled = false` gives a stage-only process.

## 1. Contract and scope

The outbox is an optional delivery integration. A job is staged on the caller's own database transaction in a source database that is **not** the queue store. After the caller commits, a relay delivers it to a Runwick destination with `ReceiveOutbox/1`. Delivery is at-least-once at the transport and deduplicated at the destination by the delivery identity within a finite horizon (§6). It is not exactly-once dispatch, not exactly-once handler execution and not a cross-database transaction (§8).

Use native staging (`IPostgresJobDispatcher.StageAsync`) when business data and the queue share one PostgreSQL connection and transaction. Use the outbox when they do not.

| Package | Depends on | Contents |
|---|---|---|
| `Runwick.Outbox` | Runwick | Public contracts (§2), stager, relay, `OutboxServices` |
| `Runwick.Outbox.Postgres` | Runwick.Outbox, Npgsql | `IPostgresOutbox`, `PostgresOutboxSchema`, PostgreSQL source adapter, `AddRunwickPostgresOutbox` |
| `Runwick.Outbox.Sqlite` | Runwick.Outbox, Microsoft.Data.Sqlite | `ISqliteOutbox`, `SqliteOutboxSchema`, same-host SQLite source adapter, `AddRunwickSqliteOutbox` |

The destination half (`ReceiveOutbox/1`, `outbox_deliveries`) is in `Runwick.Postgres` (§5). SQLite is a source only, never a queue provider (SP-019).

## 2. Public API

### 2.1 Stage

```csharp
public interface IPostgresOutbox
{
    Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, NpgsqlTransaction transaction,
        OutboxStageOptions? options = null, CancellationToken cancellationToken = default) where TJob : class, IQueueJob;
}

public interface ISqliteOutbox
{
    Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, SqliteTransaction transaction,
        OutboxStageOptions? options = null, CancellationToken cancellationToken = default) where TJob : class, IQueueJob;
}

public sealed record StagedOutboxMessage(Guid SourceStoreId, Guid MessageId, Guid DestinationStoreId,
    string DestinationApplication, JobId ProposedJobId, DateTimeOffset StagedAt, DateTimeOffset AcceptUntil, bool Existing);

public sealed record OutboxStageOptions
{
    public Guid? MessageId { get; init; }              // stable caller ID; UUIDv7 when absent
    public DispatchOptions? Dispatch { get; init; }    // frozen exactly as Prepare freezes it
    public TimeSpan? AcceptanceHorizon { get; init; }  // default 7 d, (0, 30 d], whole milliseconds
}
```

Stage, in order:

1. Freeze the job exactly as `Prepare` does (02 §6) against the configured destination binding. Validation errors (`DispatchValidationException`, `outbox_horizon_invalid`, `outbox_destination_unknown`) are raised before the first statement.
2. `SAVEPOINT runwick_outbox_stage` on the caller's transaction. A completed or connectionless transaction is `transaction_invalid`.
3. Read the source metadata (UUID, schema version) and the source time. A missing schema is `outbox_not_installed`; another version is `outbox_schema_incompatible`.
4. `INSERT … ON CONFLICT (message_id) DO NOTHING`. If the ID exists (committed, or committed by a concurrent writer the insert waited on), compare destination store, application, business digest and policy digest. Equal: return the stored row with `Existing = true` (its original `StagedAt`, `AcceptUntil` and proposed job ID). Different: `OutboxMessageConflictException` (`outbox_message_conflict`).
5. `RELEASE SAVEPOINT`. On any rejection the adapter rolls back to the savepoint first, so the caller's transaction is as it was.

Stage never contacts the destination, never commits, rolls back, disposes or retries the caller's transaction, and returns no queue receipt. A provider error other than those mapped above (for example `40001`, `40P01`, `SQLITE_BUSY`) propagates unchanged; the whole-transaction retry is the caller's (TX-03).

### 2.2 Errors

| Code | Type | When |
|---|---|---|
| `outbox_message_conflict` | `OutboxMessageConflictException` | Same message ID, different destination or intent |
| `outbox_destination_unknown` | `OutboxStagingException` | No `OutboxDestination.StoreId` and the runtime has not described the destination |
| `outbox_horizon_invalid` | `OutboxStagingException` | Horizon ≤ 0, > 30 d or not whole milliseconds |
| `outbox_not_installed` | `OutboxStagingException` | Source schema or metadata row missing (PostgreSQL `42P01`/`3F000`; SQLite "no such table") |
| `outbox_schema_incompatible` | `OutboxStagingException` | Source schema version ≠ the package's version |
| `transaction_invalid` | `OutboxStagingException` | Transaction completed, no connection, or read-only (`25006`) |

All are `ErrorDisposition.KnownNotApplied`.

### 2.3 Registration

```csharp
services.AddRunwick(r => r.Application("billing").UsePostgres(queueConnectionString));
services.AddRunwickPostgresOutbox(sourceConnectionString,
    destination: new OutboxDestination { StoreId = knownStoreId },  // optional
    relay: o => o.MaxInFlight = 4);                                 // optional
// or: services.AddRunwickSqliteOutbox("Data Source=app.db", ...);

await PostgresOutboxSchema.InstallAsync(adminSourceConnectionString);   // once, DDL-capable role
await SqliteOutboxSchema.InstallAsync("Data Source=app.db");
```

One source adapter per host (a second registration throws). The relay needs a destination store implementing `ReceiveOutbox/1` (capability `outbox_destination`); without one it fails at start. `OutboxRelayOptions` is validated at registration and again at start.

## 3. Source schema (versioned separately, version 1)

The source schema is not part of the Runwick queue schema, has its own version, and is installed by `…OutboxSchema.InstallAsync` (idempotent). It never runs Runwick migrations.

### 3.1 Records

| Record | Fields |
|---|---|
| `outbox_metadata` | singleton; `source_store_id`, `schema_version` (1), `created_at` |
| `outbox_messages` | `message_id` PK; destination binding `destination_store_id`, `destination_application`; immutable intent `intent` (the prepared envelope bytes, 02 §9.1), `intent_codec` (1), `business_digest`, `policy_digest`, `delivery_fingerprint`, `proposed_job_id`, `staged_at`, `accept_until`; relay state `state`, `next_attempt_at`, `lease_owner`, `lease_token`, `lease_expires_at`, `attempts`, `revision`; current transport `transport_command_id`, `transport_not_after`; result `delivered_job_id`, `delivered_disposition`, `confirmed_profile`, `delivered_at`; diagnostics `last_error` (≤ 256 B), `blocked_reason` (≤ 256 B) |

PostgreSQL: schema `runwick_outbox`, `uuid`/`timestamptz`/`bytea`, `COLLATE "C"` text. SQLite: tables `runwick_outbox_metadata`, `runwick_outbox_messages`, `STRICT`, UUIDs as lowercase `D` text, times as UTC epoch milliseconds.

Checks (both): state ∈ {pending, leased, delivered, blocked}; `leased ⇔ lease_token and lease_expires_at set`; `delivered ⇔ delivered_job_id set`; `staged_at < accept_until ≤ staged_at + 30 d`; digests 32 bytes; intent 1 B – 2 MiB. Partial indexes: due pending (`next_attempt_at WHERE state = 'pending'`) and lease expiry (`lease_expires_at WHERE state = 'leased'`).

Retention: pending and blocked rows have no automatic TTL. Delivered rows may lose their intent after 7 days from delivery; the identity and result row is kept until the later of that and `AcceptUntil + 24 h`. Message IDs are never recycled. Cleanup is not shipped in M3 (operators delete delivered rows past both bounds).

### 3.2 States

```text
           claim (due, or lease expired)
 pending ─────────────────────────────► leased ──mark delivered (token)──► delivered
    ▲                                     │
    └──── retry later (token, due time) ──┤
                                          └──block (token, reason)───────► blocked
```

Every transition after staging is one short source transaction guarded by `lease_token` (and, for renew and transport, by `lease_expires_at > now`). A `false` result means the token is stale and nothing was written. An expired lease is reclaimable by any owner; claiming increments `attempts` and issues a new token.

## 4. Relay

### 4.1 Sequence

1. **Initialize** (retried with backoff): destination handshake (`EnsureReadyAsync`), then read the source UUID and check the schema version.
2. **Claim** at most the free slots in one short source transaction (PostgreSQL: `FOR UPDATE SKIP LOCKED`; SQLite: `BEGIN IMMEDIATE`), commit.
3. Per message, outside any source transaction:
   1. Import the intent (02 §9.3). Malformed, unknown format/profile, digest mismatch → **block** with the codec's code.
   2. Check the destination binding (store, application) → `destination_binding_mismatch`; the registered contract → `contract_not_registered`; the delivery fingerprint recomputed from the row → `delivery_fingerprint_mismatch`. Each **blocks**.
   3. If relay time ≥ AcceptUntil: resolve after the horizon (§6.3).
   4. Reuse the persisted transport command while relay time < its NotAfter. Otherwise create a new command ID with `NotAfter = floor_ms(min(now + TransportLifetime, AcceptUntil))` and **persist it on the row (token-guarded, live lease) before sending**. A failed persist means the lease is lost: stop.
   5. Send. On outcome-unknown or a provider timeout, resend the **same** command until its NotAfter. Not applied (connection never opened) → retry later.
   6. Handle the reply (§4.4).
4. **Mark delivered** only from an `Applied` reply (or a matching retained mapping, §6.3), in its own source transaction guarded by the token. A mark that fails or whose reply is lost leaves the lease to expire; the next owner's request returns the same mapping.

The renewal loop renews every in-flight lease each `RenewalInterval`. A `false` renewal marks the lease lost: no new transport command is persisted or sent for it; a send already in flight remains resolvable and its mark is token-guarded. A renewal error sets *renewal uncertainty*: no new claims until a renewal round succeeds.

### 4.2 Budgets (06 §1.12)

| Setting | Default | Bound |
|---|---|---|
| `MaxInFlight` (messages; also the claim batch) | 4 | 1 – 64 |
| `LeaseDuration` | 60 s | ≥ 5 s and ≥ 3 × `RenewalInterval` |
| `RenewalInterval` | 15 s | > 0 |
| `ProviderCallTimeout` (each source or destination call) | 5 s | > 0 and < `RenewalInterval` |
| `IdlePoll` | 1 s (the idle wait is 5 × `IdlePoll` while the wake listener is healthy) | > 0 |
| `RetryInitial` → `RetryMax`, exponential, equal jitter | 1 s → 60 s | `RetryMax ≥ RetryInitial` |
| `TransportLifetime` | 30 s | (0, 30 s], and never past AcceptUntil |
| Source connections per relay group | 2 | fixed: one claim lane, one control lane (renew, transport, marks) |

No source transaction is open across destination I/O. A hung provider task keeps its slot until it returns. Destination traffic uses the producer lane (04 §9).

### 4.3 Shutdown

`StopAsync` cancels claims first, then waits for in-flight deliveries until the host's deadline, then aborts outstanding calls. It never marks delivered on the strength of a send attempt; abandoned leases expire and are reclaimed.

### 4.4 Reply handling

| Reply | Action |
|---|---|
| `Applied` | Mark delivered with job ID, disposition and profile |
| `CommandExpired` `delivery_horizon_passed` | Resolve after the horizon (§6.3) |
| `CommandExpired` (other), `AdmissionClosed` | Retry later (backoff), same delivery identity |
| Not applied / unresolved after NotAfter | Retry later |
| Anything else (`InvalidRequest`, `CommandConflict` `delivery_conflict`, `UniquenessConflict`, `UnsupportedVersion`, `InvariantFault` …) | Block with the error code |

Blocked messages are never rewritten or retried automatically. The operator either fixes the cause and resets the row to pending, or accepts the loss.

## 5. `ReceiveOutbox/1`

### 5.1 Request and reply

```text
StoreCommand<ReceiveOutboxRequest>(Application, CommandId, NotAfter, Body)
ReceiveOutboxRequest(Source: (SourceStoreId, MessageId), Item: DispatchItem, AcceptUntil, DeliveryFingerprint, StoreId)
OutboxDelivery(JobId, Disposition, Match, PolicyDrift, FirstDelivery, DeliveredAt, AcceptUntil, ConfirmedProfile)
```

Shape checks before any transaction (`InvalidRequest`, no receipt): 32-byte delivery fingerprint; non-empty source and message IDs; `NotAfter ≤ AcceptUntil`; the delivery fingerprint recomputed from the request equals the one sent. Store binding: `StoreId` must equal the destination's (`UnsupportedVersion`/`store_mismatch` as for Dispatch).

Retention class `durable`; producer lane; deadline cap **30 s** (`deadline_too_far`); a creation path for the gate (03 §10.3). Receipt fingerprint: §7.2.

### 5.2 `outbox_deliveries` (migration `0003_outbox`, schema revision 3)

| Column | Notes |
|---|---|
| `application`, `source_store_id`, `message_id` | PK |
| `delivery_fingerprint` (32 B), `fingerprint_version` (1) | §7.1 |
| `accept_until`, `retain_until` | `retain_until ≥ accept_until + 24 h`; `delivered_at < accept_until` |
| `job_id` | logical reference, no foreign key |
| `disposition` (Created, Reused), `duplicate_match`, `policy_drift` | from the dispatch result |
| `confirmed_profile`, `first_command_id`, `delivered_at` | |

Index `outbox_deliveries_cleanup (retain_until)`. Cleanup (not shipped in M3) deletes rows with `retain_until < now` in `retain_until` order, under the same advisory lock as delivery (§5.3), and never locks jobs.

### 5.3 Transaction

1. Gates, then the receipt (ranks 1–3; replay returns the stored reply).
2. **Rank 3a:** `pg_advisory_xact_lock(hashtextextended('runwick.outbox/' ‖ application ‖ '/' ‖ source ‖ '/' ‖ message, 0))`, then read the mapping `FOR UPDATE`.
3. Decision time (fresh clock after the locks). `now ≥ NotAfter` → `CommandExpired` `command_expired`. `NotAfter − now > 30 s` → `InvalidRequest` `deadline_too_far`.
4. Mapping exists: fingerprint equal → `Applied` with the stored delivery and `FirstDelivery = false` (receipt written); different → `CommandConflict` `delivery_conflict` with `ConflictingJobId`.
5. `now ≥ AcceptUntil` → `CommandExpired` `delivery_horizon_passed`.
6. Ordinary dispatch (03 §3.2, ranks 4–8, uniqueness and scopes unchanged). A rejection rolls back with the dispatch status.
7. Re-check `now ≥ AcceptUntil` at the dispatch path's own decision instant.
8. Insert the mapping, commit with the receipt. `Applied`, `FirstDelivery = true`.

### 5.4 Capability and manifest

The operation is added to the compatibility manifest at revision 3 (`"ReceiveOutbox": [1]`). A destination advertises capability `outbox_destination` = 1.

### 5.5 `FindDeliveryAsync` (read-only)

`FindDeliveryAsync(application, source, deliveryFingerprint)` reads the mapping without a receipt or lock and returns `(Delivery?, Conflict)`. It is used only after the horizon (§6.3). It runs under the resolution gate rules (allowed while draining).

## 6. Horizon and AcceptUntil

### 6.1 Cutoff

`AcceptUntil = floor_ms(source time at Stage) + horizon`, default 7 d, maximum 30 d, persisted in the source row and sent unchanged in every request. Clock skew can shorten the useful window; it cannot move the cutoff. Restaging the same message ID returns the stored cutoff (never a longer one).

### 6.2 Transport commands

Transport `NotAfter ≤ 30 s` after relay time and `≤ AcceptUntil`. The command ID and NotAfter are persisted on the source row before the first send. After an unknown outcome the same command is resent until its NotAfter. Only after it expires may a **new** command be issued, and only under the same delivery identity and cutoff: the delivery mapping makes this safe even if the old command is still finishing. This rule is specific to `ReceiveOutbox/1`; ordinary `DispatchAsync` never replaces an expired unknown operation with a new ID.

### 6.3 After the horizon

No new mapping is applied at a decision instant ≥ AcceptUntil, and every command sent after the cutoff is already past its own NotAfter. The relay resolves with `FindDeliveryAsync`: a matching mapping → mark delivered; a conflicting one → block `delivery_conflict`; none → block `horizon_passed_undelivered` for operator reconciliation. Extending the horizon or rerunning the work needs a new message ID, which accepts possible duplicate business effects.

### 6.4 Retention

The destination keeps a mapping until at least `AcceptUntil + 24 h`. Cleanup and delivery serialize on the same identity lock. Because all transport commands carry the cutoff, an old request can never newly apply after its mapping is pruned.

## 7. Hash domains (02 §7.3, §8)

### 7.1 `runwick.outbox.delivery/1`

`Digest("runwick.outbox.delivery/1", canonical object)` with members, in canonical order: `acceptUntil` (epoch ms), `application`, `businessDigest` (hex), `destinationStoreId`, `diagnosticMetadata` (string map), `fingerprintVersion` (1), `messageId`, `policyDigest` (hex), `proposedJobId`, `sourceStoreId`. UUIDs as 02 §8.2. It covers the destination binding, the source identity, the absolute cutoff and all frozen intent, and excludes the transport command ID and NotAfter.

### 7.2 `runwick.outbox.command/1`

`Digest("runwick.outbox.command/1", runwick.command.v1(storeId, application, "ReceiveOutbox", 1, commandId, floor_ms(NotAfter), { deliveryDigest: hex(delivery) }))` — the ordinary command envelope (02 §8.3) with the delivery fingerprint as body. It is the receipt fingerprint.

Golden vectors for both domains are added to `tests/vectors` at integration (15 §10).

## 8. Must not claim

- Exactly-once dispatch or exactly-once handler execution. A delivered mark means the destination accepted the intent; a `Reused` disposition means no new job was created.
- Deduplication after AcceptUntil, after the destination's retained mappings are pruned, or after a destination restore without its mappings (OUTBOX-06, OUT-02).
- Source ordering (FIFO) or completion ordering, even for one source.
- That a source restore is safe outside the retained destination horizon.
- SQLite as a queue provider (SP-019) or any SQLite multi-host use: the SQLite source is same-host only.
- Redis as a destination before SP-025.
- That `confirmed_profile` independently proves a fresh persistence boundary.

## 9. Cases

Project `tests/Runwick.Outbox.Tests`; traits `Category` (Contract | Fault), `Item = SP-017`, `Case`. Source and destination are separate databases on the harness server (07 §2.1); the SQLite source is a temp file. Stage and relay cases run on both sources (xunit theory over `SourceKind`). Test relay budgets: lease 6 s, renewal 2 s, provider timeout 1.5 s, idle poll 50 ms, retry 100 ms – 1 s.

### 9.1 Staging

#### OUTBOX-01 — Rollback removes everything; Stage never contacts the destination
- **Steps:** Destination behind the proxy with `DropEverything`. A host with a configured store ID and no handshake stages one message and rolls back, and stages another and commits. Then a host with no store ID and no handshake stages.
- **Assert:** the rolled-back message and business row are absent; the committed ones are present and `pending`; destination snapshot unchanged (no job, receipt or mapping); the unconfigured host fails `outbox_destination_unknown` with no write.

#### TX-01 analogue — Business row and message rolled back
- **Steps:** Business row + Stage (active and window keys) with message ID M; roll back. Stage M again and commit.
- **Assert:** after rollback nothing is visible and nothing is delivered; the second Stage is `Existing = false` and is delivered once.

#### TX-02 analogue — Caller commits
- **Assert:** after `StageAsync` the transaction is open and usable; nothing is visible elsewhere before commit (PostgreSQL); after commit row and message are visible together; `AcceptUntil = StagedAt + 7 d`; no job or receipt exists; restaging the same intent returns `Existing = true` with the stored cutoff and job ID; changed intent throws `outbox_message_conflict` and the caller's transaction still commits its other writes.

#### TX-03 analogue — Caller failure surfaces unchanged
- **Steps:** PostgreSQL: two `SERIALIZABLE` callers stage the same message ID with a read/write conflict. SQLite: a business constraint failure after Stage, caller rolls back and retries.
- **Assert:** the loser gets the provider's error (`40001` / SQLite `19`) unchanged; no internal retry; one message, one job, one mapping after the whole-unit retry.

#### TX-04 analogue — Lost caller COMMIT reply (PostgreSQL only)
- **Steps:** Business connection via the proxy. (a) Drop the COMMIT reply. (b) Sever before COMMIT.
- **Assert:** (a) the caller's commit throws; the message is delivered once (proxy tag `COMMIT`). (b) nothing visible, nothing delivered. SQLite: not applicable (same-host, no wire).

### 9.2 Relay

#### OUTBOX-02 — Lost delivered mark maps to the original job
- **Steps:** The first mark is lost before it reaches the source. (a) Re-claim while the persisted transport command is valid. (b) `TransportLifetime = 2 s`, so the re-claim issues a new command.
- **Assert:** both delivered with the proposed job ID; one job and one mapping each; `attempts = 2`; ReceiveOutbox receipts: (a) one, (b) two.

#### OUTBOX-03 — Two owners, stale token, expired transport request
- **Steps:** Owner A claims (1 s lease), persists a transport command and stalls; relay B reclaims and delivers. A then renews, persists, retries, blocks and marks; A sends an expired command and a fresh one.
- **Assert:** every A source write returns false and changes nothing; the expired command is `CommandExpired`/no effect; the fresh one returns B's job with `FirstDelivery = false`; one mapping, one job.

#### OUTBOX-04 — Nothing newly delivered at or after the cutoff
- **Steps:** Horizon 3 s. (a) A command with `NotAfter = AcceptUntil` is held at `AfterLocksBeforeFreshTime` until store time ≥ AcceptUntil. (b) A message delivered in time; after the cutoff its mapping and receipts are removed (cleanup race); the original command and a fresh one are resent. Then the relay runs.
- **Assert:** (a) `CommandExpired`, no mapping. (b) neither applies. The relay blocks both with `horizon_passed_undelivered`; one job in total.

#### OUTBOX-05 — Unavailable destination, drift and malformed intent are visible
- **Steps:** (a) Destination dropped after the relay's handshake, message staged, restored after 4 s. (b) A relay host that no longer registers the contract. (c) Intent bytes corrupted in the source row.
- **Assert:** (a) not delivered during the outage, delivered once after. (b) blocked `contract_not_registered`. (c) blocked with the codec code. No mapping and no job for (b)/(c).

#### OUTBOX-06 — Restores and the history-loss limit
- **Steps:** Stage, back up the destination (file copy), deliver. (a) Reset the source row to pending (source restore within the horizon). (b) Restore the destination backup and reset the source row.
- **Assert:** (a) redelivery resolves to the same job; one mapping. (b) the message is delivered as **new** again (fresh mapping after the restore instant): the duplicate-effect limit of §8.

#### OUTBOX-07 — Bounds during a long outage and shutdown (PostgreSQL source)
- **Steps:** Relay handshakes, destination dropped, 40 messages staged; sample for 12 s; stop with a 2 s deadline; restore and drain with a new relay.
- **Assert:** ≤ 2 relay source sessions, never `idle in transaction`; in-flight ≤ 4 and claim batch ≤ 4; no pending row due later than `RetryMax` + margin; nothing delivered; stop returns within deadline + provider timeout and no claims follow; after the outage exactly 40 jobs and 40 mappings.

#### OUT-01 — Lost marks under concurrent relays
- **Steps:** 60 messages, three relays, each losing 20 % of marks before the write.
- **Assert:** 60 jobs, 60 mappings; every message delivered with its proposed job ID; every recorded mark names the mapping's job.

#### OUT-02 — Replay outside the retained history is an explicit limit
- **Steps:** Horizon 2 s, relay starts after the cutoff; restage the same ID with a 7-day horizon; stage a new message ID.
- **Assert:** blocked `horizon_passed_undelivered`, no job; the restage returns `Existing = true` with the original cutoff; the new ID is delivered (one job).

### 9.3 `ReceiveOutbox/1`

#### CMD-01 — Same command concurrent and sequential
- **Steps:** Command C held at `BeforeCommit`; the same C on a second connection and a different command for the same delivery identity are submitted; release; replay C three times.
- **Assert:** one job, one generation, one mapping, one receipt for C; all C replies identical (`DecidedAt`, job) with exactly one `Replayed = false`; the sibling returns the same job with `FirstDelivery = false`.

#### CMD-02 — Same ID, changed body, deadline, identity or operation
- **Assert:** changed cutoff, NotAfter or message under C's ID → `CommandConflict`, no effect; Dispatch under C's ID → `CommandConflict`; the snapshot is unchanged. In application b, C's ID applies independently with b's own job.

#### CMD-03 — Commit applied, reply dropped
- **Assert:** outcome-unknown; the retry of the same command is `Replayed = true` with the same job. Relay variant: the dropped reply is resent under the same command ID; one job, one mapping, one receipt for that command.

#### CMD-04 — Deadline expires while waiting for the delivery identity lock
- **Steps:** A held at `BeforeCommit`; B (same identity, NotAfter = now + 1 s) waits on the identity lock past its NotAfter; release; after A's NotAfter, replay A.
- **Assert:** A applied; B `CommandExpired`, no effect, decided after its NotAfter, no receipt; A's replay returns its stored reply.

#### CMD-05 — Receipt pruned, old command resubmitted
- **Assert:** the resubmitted command is `CommandExpired` with no effect; a new command under the same identity and cutoff returns the mapping (`FirstDelivery = false`), one job.

#### ISO-01 — Delivery identities per application
- **Steps:** The same (source, message) and command ID delivered in applications a and b; a conflicting delivery in b.
- **Assert:** each application has its own mapping and job; b's lookup never sees a's mapping; b's conflict names b's job and leaves a's mapping unchanged.

### 9.4 Measurement P-09 (`tests/Runwick.Workloads`)

PostgreSQL source and destination on separate databases of the reference host, one relay group with default budgets except `MaxInFlight = 16`; 20 000 messages staged by 8 business writers at a steady rate. Record stage-commit to delivered-mark latency and sustained deliveries per second after a 30 s warm-up. Pass: **≥ 100 deliveries/s and p99 ≤ 2 s** (15 §11). Report source sessions and destination producer-lane occupancy alongside.

## 10. Open items

1. Delivered-row and mapping cleanup jobs (§3.1, §5.2) are specified but not shipped in M3.
2. Golden vectors for §7 are generated at integration and frozen in `tests/vectors`.
3. A `MaxInFlight` above 4 for P-09 is a measurement setting, not a default change.
