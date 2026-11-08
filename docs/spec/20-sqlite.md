# 20 — SQLite store (SP-019)

**Sources:** [driver-capabilities.md](../architecture/driver-capabilities.md) §2, §4, §6 (SQLite profile, encodings), [driver-contract-suite.md](../architecture/driver-contract-suite.md) §3–§5 (cases, SQLite fault gates), [storage-command-protocol.md](../architecture/storage-command-protocol.md), [provider-dependency-plan.md](../architecture/provider-dependency-plan.md) §5, §7 (Microsoft.Data.Sqlite, DEP-06), [03-storage-protocol.md](03-storage-protocol.md) (what every store must do), [04-postgres.md](04-postgres.md) (the reference implementation this one mirrors), [08](08-controls-and-completion.md), [10](10-rate-admission.md), [13](13-scheduler.md), [14](14-outbox.md), [16](16-management.md) for the operations added since M2, [v1-delivery-plan.md](../v1-delivery-plan.md) §6 and [implementation-backlog.md](../implementation-backlog.md) §5 (SP-019 rows).

## In short

SP-019 adds `Runwick.Sqlite`: Runwick's job store in a single SQLite database file, for applications whose workers all run **on one machine** (one or several processes). It keeps exactly the behaviour of spec 03: the same commands, results, receipts, states and guarantees as PostgreSQL. Only the mechanics differ:

- **One writer at a time.** SQLite lets only one transaction write to a file at once, across all processes. Every Runwick write starts with `BEGIN IMMEDIATE`, which takes that write lock up front. There are no row locks, no `SKIP LOCKED` and no lock-order rules: the file lock is the only lock.
- **Time is read after the lock.** Once a write holds the lock, the store reads the machine's clock and makes its decisions with that time, just as PostgreSQL reads `clock_timestamp()` after its locks.
- **SQLite calls block a thread.** The .NET SQLite library runs "async" calls synchronously. So each store runs its database work on a few dedicated threads (one writer, two readers), and the worker's lease and scheduling loops never wait inside a SQLite call.
- **Durable by default and only.** The file uses WAL mode with `synchronous=FULL`: a reply means the commit is on disk. A weaker setting is refused at startup, not silently accepted.
- **Same-host only.** A database on a network share is refused at startup. In-memory databases are refused too.
- **Your transaction, your file.** An application can stage a job (or complete one) inside its own SQLite transaction on the same file, so the business row and the job commit together.

Workers send their routine work in batches, as on PostgreSQL: one command claims and starts up to 32 jobs, one renews up to 64 leases, one finishes up to 32 jobs. On SQLite each batch is a single write transaction, so it waits for the file lock once and syncs to disk once instead of once per job.

The SQLite schema is created by one migration and matches PostgreSQL schema revision 15 table by table. It has its own revision counter starting at 1.

Not in SP-019 [choice]: cross-process wake-up notifications (polling plus wake-ups within one process), a `synchronous=NORMAL` profile, retention cleanup (SP-021 adds it for every store), and certification (G-4).

## Changes

- (2026-11-08) New document.
- (2026-11-08) Decisions taken where the sources were silent, each marked [choice] in the text: table prefix `runwick_` (§3); UUIDs as 16-byte big-endian blobs (§3); `STRICT` tables for overflow detection (§3); one writer thread per process (§5); `AUTOINCREMENT` events so a cursor never skips (§7.1); only the FULL profile (§2); caller staging requires the caller connection to be FULL and to have foreign keys on (§8); interrupt-based query budget (§5.3).
- (2026-11-08, user decision, adzamkomla) **Batched operations from the start.** `ReserveAndBegin/1`, `RenewMany/1` and `FinishMany/1` are in the SQLite manifest and implemented on the writer thread, one `BEGIN IMMEDIATE` per batch (§5.1, §7.2); `DispatchMany/1` uses multi-row statements, the SQLite form of PostgreSQL's set-based dispatch (§7.2). Cases SQL-12 and OWN-04 with `RenewMany`. Replaces the earlier deferral. `synchronous=FULL` stays the only profile.
- (2026-11-08) `ManagementDispatch/1` (19 §6.2) is in the SQLite manifest and schema from the start (§9.3).
- (2026-11-08) Public API additions recorded in 01 Changes: `UseSqlite`, `SqliteOptions`, `ISqliteJobDispatcher`, `ISqliteJobCompletion`, `RunwickSqliteMigrations`, `AcknowledgementProfiles.SqliteWalFull`.
- (2026-11-08, SP-019 build) Bundled engine recorded: Microsoft.Data.Sqlite 10.0.14 → SQLitePCLRaw.bundle_e_sqlite3 2.1.13 → SQLite **3.53.3** (source id `2026-06-26 20:14:12 d4c0e51e…d782c62`), above the 3.38 minimum (§1, SQL-08).
- (2026-11-08, SP-019 build) What the first build does differently, and what it leaves out:
  - Microsoft.Data.Sqlite retries a busy statement until its command timeout (set to 1 s), so the real lock wait is max(`WriterBusyTimeout`, about 1 s) (§4).
  - `ReserveAndBegin/1` and `FinishMany/1` run each item in turn inside the one `BEGIN IMMEDIATE` transaction, not as multi-row statements. `DispatchMany/1` is multi-row and chunked (§7.2).
  - SQLite has no wake notification source. Workers poll.
  - The harness picks SQLite by connection string (`Data Source=…`), not by a `--store` flag. SQL-02 holds the lock from a second connection in the same process, not from a Peer process.
  - The existing PostgreSQL suite classes were not moved. A new shared project, `Runwick.Contract.Tests`, runs its cases on both stores.
  - Not built yet (named gaps): `ManagementDispatch/1` (left out of the manifest until built), retention cleanup, controls and `EventsSince`, rates, schedules, scaling, outbox receive, management reads, and the §8 caller-transaction API (`ISqliteJobDispatcher`, `ISqliteJobCompletion`). Cases SQL-01, 03, 04, 05, 07 and 11 are not run yet.
- (2026-11-08, SP-019 gaps) **Retention on SQLite.** Retention cleanup (17) runs on SQLite too, so the SQLite schema revision 1 gains the retention policy and cleanup progress tables, the cleanup indexes and the audit kinds `EditRetentionPolicy` and `RetentionSummary` (§11). Revision 1 was never released, so it is changed in place rather than adding `0002`. The manifest is now the PostgreSQL revision-17 set: revision 15's operations plus `CleanupBatch/1`, `EditRetentionPolicy/1` and `ManagementDispatch/1` (§9.3). RET-01 becomes applicable (§12).

## 1. Targets and boundaries

| Item | Rule |
|---|---|
| Client | Microsoft.Data.Sqlite **10.0.14** with its default `SQLitePCLRaw.bundle_e_sqlite3` native bundle (00 §7). Explicit parameterized SQL; no EF/Dapper. |
| Engine | `sqlite_version() ≥ 3.38.0` (needs `STRICT` 3.37, `RETURNING` 3.35, built-in JSON 3.38). Describe records `sqlite_version()` and `sqlite_source_id()`; an older engine is `UnsupportedVersion engine_version_unsupported` at startup. The resolved native package and asset are recorded in build evidence (DEP-06). |
| Topology | One local file; every process that uses it runs on the same host. §2.3 rejects network and in-memory databases. |
| Deployment modes | All five modes of 00 §5, provided every process is on one host. |
| Notifications | None across processes. After its own commit a process wakes its own local pools, scheduler and relay (the existing in-process hint). Polling is sufficient for correctness (OBS-01). |
| Acknowledgement profile | `AcknowledgementProfiles.SqliteWalFull = "sqlite.wal-synchronous-full"`: a reply follows a commit with WAL and `synchronous=FULL` on the committing connection. |
| Operations | Every operation of the PostgreSQL manifest at revision 15, batched operations included, plus `ManagementDispatch/1` (19 §6.2) (§9.3). |

## 2. Registration, options and startup checks

### 2.1 API

```csharp
public static RunwickBuilder UseSqlite(this RunwickBuilder builder, string connectionString, Action<SqliteOptions>? configure = null);

public sealed class SqliteOptions
{
    public TimeSpan WriterBusyTimeout { get; set; } = TimeSpan.FromSeconds(2);   // 100 ms – 30 s
    public int ReaderThreads { get; set; } = 2;                                 // 1 – 8
}
```

The connection string must name a file (`Data Source=<path>`). Runwick opens its own connections from it, with `Pooling=False`, `Mode=ReadWriteCreate` (the migration) or `ReadWrite` (runtime), and never logs it. Keywords that change semantics are rejected at startup with `ConfigurationError` code `invalid_policy`: `Mode=Memory`, `Mode=ReadOnly`, `Cache=Shared`, a `Data Source` of `:memory:` or empty, and `Foreign Keys=False`.

### 2.2 Per-connection settings

Every driver connection runs, right after `Open`:

```sql
PRAGMA foreign_keys = ON;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = <WriterBusyTimeout ms>;   -- readers: 1000
PRAGMA trusted_schema = OFF;
PRAGMA journal_size_limit = 67108864;            -- WAL file trimmed to 64 MiB after a checkpoint
```

and reads back `PRAGMA journal_mode` (must be `wal`), `PRAGMA synchronous` (must be `2`) and `PRAGMA foreign_keys` (must be `1`). A mismatch closes the connection and fails the store with `store_profile_mismatch` (03 §9.3 class UnsupportedVersion; the worker does not start). `journal_mode=WAL` is persistent in the file and set by the migration (§9), never by the runtime. `wal_autocheckpoint` stays at SQLite's default (1000 pages) [choice]; SQL-05 measures its effect on renewals.

### 2.3 Topology check

At startup, before the first Describe:

1. Resolve the full path of the database file (`Path.GetFullPath`, then symbolic links resolved).
2. **Windows:** a UNC path (`\\server\…`) or `DriveInfo(path).DriveType == Network` → reject.
3. **Linux:** find the longest mount point containing the path in `/proc/self/mountinfo`; a filesystem type in {`nfs`, `nfs4`, `cifs`, `smb3`, `smbfs`, `9p`, `fuse.sshfs`, `afs`, `ceph`, `glusterfs`, `lustre`, `gpfs`, `fuse.s3fs`} → reject.
4. Reject: `ConfigurationError` code `topology_unsupported`, naming the filesystem type, not the path's secrets.
5. Any other OS, or a filesystem type the table above does not list and that is not a known local type (`ext4`, `xfs`, `btrfs`, `zfs`, `tmpfs`, `overlay`, `ntfs`, `refs`), is accepted and reported in Describe as `topology: unverified` (00 §9 rule 10: never reported as verified).

## 3. Schema conventions

| Concern | Rule |
|---|---|
| Names | Tables are the PostgreSQL tables of revision 15 with the prefix `runwick_` (`runwick_jobs`, `runwick_storage_commands`, …) [choice: no schemas in SQLite, and the file is shared with the application's tables]. Index and constraint names keep the PostgreSQL names with the same prefix. The outbox source tables (`runwick_outbox_metadata`, `runwick_outbox_messages`, 14 §3.3) do not collide. |
| Tables | Every table is `STRICT` [choice]. Integer overflow then fails as `SQLITE_CONSTRAINT_DATATYPE` instead of silently becoming a REAL (SQLite turns an overflowing `+ 1` into a float). |
| Application scope | As 04 §2: `application` leads every key except the store gate and ledger; every statement binds `$application`. |
| UUIDs | `BLOB` of 16 bytes, RFC 4122 big-endian (`Guid.TryWriteBytes(span, bigEndian: true, …)`), with `CHECK (length(x) = 16)` [choice]. `memcmp` order = `UuidOrder` (03 §2.4). |
| Text | `TEXT` with the default `BINARY` collation: `memcmp` over UTF-8 = 03 §2.4 text order. Byte-length checks use `length(CAST(x AS BLOB))`. |
| Instants | `INTEGER` UTC epoch milliseconds. Floor/ceil happen in C# before binding (03 §2.4); SQL never computes time from text dates. |
| Durations | `INTEGER` milliseconds (`*_ms`), as 04 §2. |
| Counters, revisions, fences | `INTEGER` (64-bit) with the same non-negative `CHECK`s as PostgreSQL. Overflow → `InvariantFault counter_overflow`. |
| JSON | `TEXT` with `CHECK (json_valid(x))` and the PostgreSQL byte bounds; written in the 02 canonical encoding where PostgreSQL's `jsonb` content is compared (receipts, policy snapshots), so the bytes are stable. |
| Payloads | `BLOB`, original bytes, never through JSON. |
| Partitioning | `storage_commands` is one table with its `retention_class` column (PostgreSQL partitions by it) and an index `(application, retention_class, not_after)`. |
| Deferred constraints | SQLite foreign keys support `DEFERRABLE INITIALLY DEFERRED`; `jobs_current_generation_fk` and `jobs_current_attempt_fk` keep it. |
| Receipt placeholder | PostgreSQL's deferred trigger (04 §4.10) has no SQLite twin; the runner checks before COMMIT that the receipt row it inserted is decided, else rolls back with `InvariantFault receipt_incomplete`. |
| Server functions | None. Each operation is C# issuing plain statements inside the runner's transaction, step for step as 04 §7 and the later specs describe (§7). |

The migration file `Sql/0001_initial.sql` is the full translation of PostgreSQL migrations 0001–0015 under these rules (tables, checks, indexes including partial ones, no functions). The jobs table, for illustration:

```sql
CREATE TABLE runwick_jobs (
    application        TEXT    NOT NULL CHECK (length(CAST(application AS BLOB)) BETWEEN 1 AND 128),
    job_id             BLOB    NOT NULL CHECK (length(job_id) = 16),
    job_name           TEXT    NOT NULL,
    payload_version    INTEGER NOT NULL CHECK (payload_version >= 1),
    -- … every column of runwick.jobs at revision 15, same names, same checks …
    revision           INTEGER NOT NULL CHECK (revision >= 1),
    eligible_at        INTEGER,
    CONSTRAINT jobs_pk PRIMARY KEY (application, job_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX runwick_jobs_ready ON runwick_jobs (application, queue, priority, eligible_at, job_id)
    WHERE state IN ('pending', 'retrying');
```

`WITHOUT ROWID` is used for tables whose primary key is the natural UUID key (jobs, attempts, generations, payloads, scopes, receipts, schedules); `runwick_queue_events` keeps its rowid (§7.1).

## 4. Transaction runner

### 4.1 Driver-owned mutation

For every receipt-bearing operation and every other write (Renew, PublishPresence, …):

1. Take a slot in the operation's call class (§5.1) and hand the work to the writer thread.
2. `BEGIN IMMEDIATE` on the writer connection. SQLite waits up to `busy_timeout` (= `min(WriterBusyTimeout, remaining operation budget)`, re-set per transaction) for other processes. Busy here is **not applied**: retried with the same command ID and NotAfter as a transient conflict (04 P-13 budget); exhausted → `transient_conflict_exhausted`; at or past NotAfter → `CommandExpired` (04 P-25).
3. Fault point `AfterLocksBeforeFreshTime`. Read the decision time `now` = host UTC clock (`TimeProvider.System`; the harness may inject an offset clock), floored to 1 ms. Nothing before step 2 is used for a decision (03 §1 rule 3).
4. Gates (03 §10.1): read `runwick_store_metadata` and `runwick_application_controls` in this transaction. No shared-lock statement is needed: the write lock already excludes every other writer, gate changes included.
5. Receipt (03 §8.2): look up `(application, command_id)`; a decided receipt with the same fingerprint replays, a different fingerprint is `CommandConflict`; otherwise insert the placeholder. `now ≥ NotAfter` → `CommandExpired`, rollback.
6. The operation's statements (§7). Fault points `BeforeFirstMutation`, `AfterMutation` as in PostgreSQL.
7. Decide the receipt row; fault point `BeforeCommit`; check the placeholder is decided (§3); `COMMIT` if the reply's effect is `Applied` and it is not a replay, else `ROLLBACK`.
8. Fault point `AfterCommitBeforeReply`; release the class slot in `finally`.

No handler, middleware, payload decoding, recurrence computation or remote call runs while the transaction is open (03 §1 rule 7).

### 4.2 Read-only calls

Probe, ListExpiredLeases, ListUnsupportedBacklog, EventsSince, EstimateDemand, ResolveCommand, Describe and the management reads run on a reader thread as `BEGIN` (deferred) … `COMMIT`: one WAL snapshot, so gates and data are consistent without locks. They never write. A read never upgrades to a write.

### 4.3 Error mapping

| SQLite result | Where | Classification | Code |
|---|---|---|---|
| `SQLITE_BUSY` (5) | `BEGIN IMMEDIATE` | not applied, transient | retry (§4.1 step 2) |
| `SQLITE_BUSY` | `COMMIT` | transaction still open | retry `COMMIT` while budget remains, then `ROLLBACK` → not applied, transient |
| `SQLITE_CONSTRAINT_PRIMARYKEY` on jobs/attempts/scopes keys | insert | not applied | `InvariantFault id_collision` |
| `SQLITE_CONSTRAINT_DATATYPE` | update | not applied | `InvariantFault counter_overflow` |
| other `SQLITE_CONSTRAINT_*` | any | not applied | `InvariantFault constraint_violation` |
| `SQLITE_ERROR` "no such table/column" | any | not applied | `UnsupportedVersion schema_incompatible` |
| `SQLITE_INTERRUPT` (9) | read (budget, §5.3) | nothing written | 503 `query_budget_exceeded` (management) / `statement_timeout` |
| `SQLITE_IOERR*` (10), `SQLITE_FULL` (13) | before `COMMIT` | not applied | `store_unavailable` |
| `SQLITE_IOERR*`, `SQLITE_FULL` | during `COMMIT` | **outcome unknown** [choice: a failed sync cannot prove the frames are absent] | `commit_outcome_unknown`; resolved by resending the same command |
| `SQLITE_CORRUPT` (11), `SQLITE_NOTADB` (26) | any | not applied | `store_unavailable`, health `Unhealthy` with reason `store_corrupt` |
| `SQLITE_READONLY` (8), `SQLITE_CANTOPEN` (14), `SQLITE_PERM` (3) | any | not applied | `store_unavailable` |
| `SQLITE_LOCKED` (6) | any | not applied | driver defect (shared cache is rejected); `InvariantFault` |

After any error the runner issues `ROLLBACK` if `connection` still reports an open transaction, and replaces the writer connection if `ROLLBACK` itself fails. Error text and parameters are never logged.

## 5. Executor and connections

### 5.1 Threads and call classes

Microsoft.Data.Sqlite runs its `…Async` methods synchronously. Each store instance therefore owns:

| Thread | Connection | Work |
|---|---|---|
| 1 writer thread [choice] | 1 long-lived writer connection | Every write, one at a time. A second writer thread in one process would only wait on the same file lock. |
| `ReaderThreads` (default 2) | one connection each | §4.2 reads |
| none (caller's thread) | caller's connection | Staging and transactional completion (§8) |
| migration (§9) | 1 separate connection, opened and closed by the runner | Migrations only |

Batches are single writes: `RenewMany/1` (renewal class), `FinishMany/1` (settlement class) and `ReserveAndBegin/1` (admission class) each run as **one** `BEGIN IMMEDIATE … COMMIT` on the writer thread, with one decision time read after the lock for every member, and one WAL sync at commit. The engine's batching (05, PF-06: which leases go into one `RenewMany`, which completions into one `FinishMany`, when a pool claims with `ReserveAndBegin`) is unchanged; the writer thread does not merge separate calls into one transaction.

The call classes and ceilings of 04 §9.1 apply unchanged as admission limits (renewal 4, settlement 4, admission 4, producer/admin `MaxConcurrentCalls`, background 2), so `local_capacity_exceeded` and slot waits behave as on PostgreSQL. The writer thread takes queued writes highest class first: renewal → settlement → admission → producer/admin → background; first come first served within a class. A write is never interrupted once its `BEGIN IMMEDIATE` succeeded.

Awaiting callers get a `TaskCompletionSource` completed with `RunContinuationsAsynchronously`, so no caller continuation runs on a store thread. The lease watchdog (05) keeps its own timer; a writer stuck behind another process's lock delays a Renew reply, never the watchdog's local expiry.

### 5.2 Cancellation

A call cancelled while queued is removed from the queue: not applied. Once on the writer thread, cancellation is honoured only between statements and before `COMMIT` (rollback, not applied); a `COMMIT` in progress completes and its result is delivered even if the caller left. The class slot is released only when the thread is done with the call (as 04 §9.1).

### 5.3 Query budget

Management reads keep their 1 s budget (16 §2) and the other reads their operation budget. A timer calls `sqlite3_interrupt` on the reader connection's handle (`SqliteConnection.Handle`, SQLitePCLRaw) when the budget ends [choice: Microsoft.Data.Sqlite has no statement timeout]. The read then fails with `SQLITE_INTERRUPT` (§4.3) and the reader's snapshot is released, so long reads cannot hold back checkpoints.

## 6. Describe and the store gate

`runwick_store_metadata` has the PostgreSQL columns (store ID, schema revision, epoch, migration mode, manifest, manifest version, digest, revision) plus `driver TEXT NOT NULL CHECK (driver = 'sqlite')`. `Describe/1` returns them with the application gate as 03 §3.1 and adds to the store descriptor: `sqlite_version`, `sqlite_source_id`, `journal_mode`, `synchronous`, `topology` (`local` | `unverified`, §2.3). The binary's readable and writable SQLite schema revisions are `[1]`; the manifest is §9.3. A PostgreSQL-only binary path never meets this file: the driver key in the descriptor selects the revision set.

## 7. Operations

Each operation runs the steps that 04 §7 (and 08, 09, 10, 11, 13, 14, 16 for later operations) give for PostgreSQL, in the same order, with the same rows, values, events, receipts and replies. The table lists only what differs.

| Operation | Reference | SQLite difference |
|---|---|---|
| RegisterApplication/Queue/Scope | 04 §7.1 | `INSERT … ON CONFLICT DO NOTHING RETURNING`; then read existing rows |
| Dispatch/1, DispatchMany/1 | 04 §7.2, 04 Changes 2026-11-07 (set dispatch), 14 (dispatch core) | No locks to sort. Key bindings, scopes and jobs are written in the PostgreSQL order anyway, so the event and reply order are identical. `DispatchMany` is set-based (§7.2) |
| ManagementDispatch/1 | 19 §6.2 | Dispatch core + audit row in the same write transaction |
| Reserve/1 | 04 §7.3 | No `FOR UPDATE SKIP LOCKED`: the write lock already makes the candidate scan exclusive. Capability tuples are bound as one JSON array and joined with `json_each($capabilities)` [choice: no temp tables]. Candidate order `(eligible_at, job_id)` with the 512-tuple and scan bounds of 04 |
| ReadinessProbe/1 and /2, ListUnsupportedBacklog | 04 §7.4, 09 | Read path (§4.2) |
| Begin/1 (rate-aware) | 04 §7.5, 10 §4 | Rate scopes and grants as PostgreSQL; T20 deferral writes its receipt as there |
| Renew/1 | 04 §7.6 | Write path; no receipt (R1) |
| RenewMany/1 | 03 Changes 2026-11-02 (PF-06) | §7.2 |
| ReserveAndBegin/1 | 03 Changes 2026-11-02 (PF-06 batched claim), 04 Changes 2026-11-07 (set-based claim) | §7.2 |
| FinishMany/1 | 03 Changes 2026-11-02 (PF-06 batched finish), 04 Changes 2026-11-07 (set-based finish) | §7.2 |
| Finish/1, Recover/1, ReleaseReservation/1, shared close | 04 §7.7–§7.10 | — |
| ListExpiredLeases/1, PublishPresence/1, ResolveCommand/1 | 04 §7.11–§7.13 | — |
| Cancel/ManualRetry/RerunSucceeded, SetQueueState, EditScopePolicy | 08 | Audit row in the same transaction |
| ActivateCapabilities, EditRatePolicy | 10 | — |
| Schedule operations | 13 §6 | — |
| SetPoolOverride, EstimateDemand, RecordScalingEvent | 11 | EstimateDemand on the read path |
| ReceiveOutbox/1 | 14 §5 | — |
| RecordDisclosure/1 and the management reads | 16 §2–§3, §6 | Reads on the read path with the §5.3 budget; disclosure on the write path |
| EventsSince/1 | 08 §7.2 | §7.1 |

### 7.1 Events

`runwick_queue_events` has `event_id INTEGER PRIMARY KEY AUTOINCREMENT` [choice: plain rowids may be reused after the newest rows are deleted, which would let a cursor skip new events]. Because writers are serialized and an event ID is assigned inside the writer's transaction, event IDs are in commit order and a row with a smaller ID can never appear later. `EventsSince/1` is therefore:

```sql
SELECT … FROM runwick_queue_events WHERE application = $application AND event_id > $after
ORDER BY event_id LIMIT $limit;
```

`EventCursor.TxId` is always 0 on SQLite and `Partial` is always false. EVT-01 and EVT-02 hold as stated in 08.

### 7.2 Batched and set-based operations

The replies, receipts, rows, events and their order are those of 03 for each operation; only the SQLite mechanics are given here.

- **RenewMany/1** (1–64 leases): one transaction. Each member runs inside `SAVEPOINT rw_m` … `RELEASE`; a member refused (`StaleLease`, `InvalidState`, `NotFound`) writes nothing, and a member's `InvariantFault permit_set_mismatch` is `ROLLBACK TO rw_m` for that member only. No member's outcome rolls back another (OWN-04). No receipt (R1). The transaction commits when at least one member renewed, else rolls back.
- **ReserveAndBegin/1** (1–32 jobs): one transaction; the candidate scan of Reserve/1 continues until `MaxJobs` jobs are claimed or candidates run out; each claimed job gets the Reserve and then the Begin steps (rate step and T20 deferral included) before the next job is scanned, so a later job sees the permits and grants of the earlier ones. One receipt with `items[]`. Any `InvariantFault` rolls the whole command back (03).
- **FinishMany/1** (1–32 items): one transaction; each item checks its own receipt (replay or new) and ownership, then runs Finish/1's steps. Item receipts are ordinary Finish receipts. The transaction commits when at least one item applied now, rolls back when every item was refused or replayed; any `InvariantFault` rolls the whole call back (03 Changes 2026-11-02).
- **Set-based statements.** Where PostgreSQL's revisions 12–14 moved per-item statements into set statements, SQLite does the same with multi-row `INSERT … VALUES (…), (…)` and `UPDATE … WHERE (application, job_id) IN (SELECT … FROM json_each($ids))`, chunked so one statement binds at most 32,766 parameters (SQLite's default limit since 3.32). This applies to `DispatchMany/1` (jobs, payloads, generations, requirements, events), `FinishMany/1` (attempt closes, permit deletes, events) and `ReserveAndBegin/1` (attempts, permits, events). It is a speed measure only: a set statement must produce exactly the rows of the per-item path (SQL-12 compares them).

## 8. Caller transactions

```csharp
public interface ISqliteJobDispatcher   // same members as IPostgresJobDispatcher (01 §15), with SqliteTransaction
{
    Task<StagedDispatch> StageAsync<TJob>(TJob job, SqliteTransaction transaction, DispatchOptions? options = null, CancellationToken cancellationToken = default) where TJob : class, IQueueJob;
    Task<StagedDispatch> StageAsync(PreparedDispatch prepared, SqliteTransaction transaction, CancellationToken cancellationToken = default);
    Task<StagedBatch> StageManyAsync(IReadOnlyList<BatchDispatchItem> items, SqliteTransaction transaction, BatchDispatchOptions? options = null, CancellationToken cancellationToken = default);
    Task<StagedBatch> StageManyAsync(PreparedBatch prepared, SqliteTransaction transaction, CancellationToken cancellationToken = default);
}

public interface ISqliteJobCompletion   // R6, as IPostgresJobCompletion (01 §16, 08 §8)
{
    Task<StagedCompletion> CompleteInTransactionAsync(SqliteTransaction transaction, CancellationToken cancellationToken = default);
}
```

Rules (01 §15–§16 and 08 §8 apply; only the SQLite parts are listed):

1. **Validation** before any write, failures `TransactionValidationException`: the transaction is open; its connection's main database file is the configured file (`PRAGMA database_list`, compared after full-path resolution); `runwick_store_metadata.store_id` equals the store's; the connection has `foreign_keys = 1` and `synchronous = 2` [choice: a weaker caller connection would make a staged job less durable than the profile says]; code `transaction_profile_mismatch` for the pragmas.
2. Runs on the caller's thread and connection, inside `SAVEPOINT runwick_stage` … `RELEASE` (or `ROLLBACK TO` + `RELEASE` on any exception, with the rollback never using the caller's token and a failing rollback swallowed, exactly as the SQLite outbox source, 14 Changes 2026-11-01).
3. Same gates, receipt and steps as the driver-owned operation; the decision time is read when staging starts writing, after the caller's own writes may already hold the lock.
4. A deferred caller transaction can get `SQLITE_BUSY` (or `SQLITE_BUSY_SNAPSHOT`) on its first write. That error propagates unchanged; the whole-transaction retry is the caller's (TX-03). The documentation recommends `BeginTransaction(deferred: false)` (`BEGIN IMMEDIATE`) for business transactions that stage.
5. Runwick never commits, rolls back, disposes or retries the caller's transaction.
6. **Contention, stated plainly:** while a caller's write transaction is open, every Runwick write in every process on that file waits, renewals included. A long business transaction can therefore make leases expire. SQL-07 measures it; the documentation says to keep staging transactions short.

## 9. Migrations

### 9.1 API

```csharp
public static class RunwickSqliteMigrations
{
    public static Task<SqliteMigrationResult> MigrateAsync(string connectionString, CancellationToken cancellationToken = default);
}
public sealed record SqliteMigrationResult(Guid StoreId, long SchemaRevision, IReadOnlyList<string> AppliedPhases);
public sealed class SqliteMigrationException : Exception { public string Code { get; } }
```

Codes: `store_unavailable`, `engine_version_unsupported`, `topology_unsupported`, `schema_not_empty`, `migration_lock_timeout`, `migration_checksum_mismatch`, `schema_validation_failed`, `migration_phase_failed`.

### 9.2 Runner

1. Open a dedicated connection (`Mode=ReadWriteCreate`, `Pooling=False`), apply §2.2 and §2.3 checks except the journal-mode read-back, check the engine version.
2. `PRAGMA journal_mode = WAL` (outside a transaction; must answer `wal`).
3. `BEGIN IMMEDIATE` with `busy_timeout = 60 s`; busy after that → `migration_lock_timeout`. The write lock is the migration lock: no Runwick write in any process can interleave.
4. If `runwick_schema_migrations` exists, read the ledger and compare checksums (SHA-256 of the file's UTF-8 bytes, LF line endings, as 04 §8.1). If any other `runwick_%` table exists without a ledger → `schema_not_empty`. The outbox source tables are ignored by this check.
5. Run `0001_initial.sql`, insert `runwick_store_metadata` (new store ID `Guid.NewGuid()`, `schema_revision = 1`, `migration_mode = 'normal'`, manifest, digest), validate (§9.4) and write the ledger rows (`expand`, `validate`, `activate` as `applied`), all in this one transaction, then `COMMIT`. SQLite DDL is transactional, so a failure leaves nothing.
6. A re-run on an applied file is a no-op returning the existing store ID.

Later SQLite migrations (`0002_…`) follow the same single-transaction shape; a semantic change that needs drain follows 04 §8.4's drain → activate steps.

### 9.3 Manifest

Same canonical encoding and digest domain as 04 §8.5 (`runwick.manifest.v1`). `schemaRevision: 1`; `operations` = the PostgreSQL revision-15 operation set and versions (batched operations included) plus `ManagementDispatch: [1]`. The `runwick_management_audit` operation check includes `Dispatch` (19 §6.2).

### 9.4 Validation

Compare `sqlite_schema` (type, name, tbl_name, and the normalized `sql` text) with the object list compiled into the binary; `PRAGMA foreign_key_check` returns no rows; `PRAGMA integrity_check` returns `ok` (on the new, empty schema it is instant). Any difference → `schema_validation_failed`, rollback.

## 10. Defaults

| Name | Value | Bound |
|---|---|---|
| `WriterBusyTimeout` | 2 s | 100 ms – 30 s; per transaction `min(value, remaining budget)` |
| Reader `busy_timeout` | 1 s | fixed |
| `ReaderThreads` | 2 | 1 – 8 |
| Writer threads | 1 | fixed |
| `journal_size_limit` | 64 MiB | fixed |
| Migration lock wait | 60 s | fixed |
| Management read budget | 1 s | 16 §2 |

These numbers are listed here rather than in 06 (06 Changes 2026-11-08 points here).

## 11. Schema revision

No PostgreSQL schema revision. SQLite schema revision 1 (`0001_initial.sql`), its own counter.

## 12. Tests

Harness changes:

- `DriverFixture` gains `StoreKind { Postgres, Sqlite }`. For SQLite it creates a temp directory per test, runs `MigrateAsync`, and its `ReadCommittedSnapshot()` reads every `runwick_` table in one read transaction into the same normalized snapshot as PostgreSQL.
- The driver-suite test classes that use only protocol interfaces, the fixture and the snapshot move from `tests/Runwick.Postgres.Tests` to a new `tests/Runwick.Contract.Tests` and run as theories over `StoreKind`. PostgreSQL-only cases (PGX, plans, proxy faults) stay where they are. SQLite-only cases go to `tests/Runwick.Sqlite.Tests`.
- `Runwick.Harness.Peer` accepts `--store sqlite --path <file>`, so contention and crash cases use separate OS processes (an in-process shared connection is not enough, driver-contract-suite §5).
- The SQLite runner calls the existing `IFaultInjector` at the points of §4.1; "lost reply" is `AfterCommitBeforeReply` + `DropReply` (there is no wire to cut).

Applicable driver-suite cases (all must pass, real files, real processes where the case involves more than one client): CMD-01–06, ADM-01–05, RUN-01–02, OWN-01–05 (OWN-04 through `RenewMany/1`), END-01–04, UNI-01–04, RATE-01–04, SCH-01–05, ISO-01, ENC-01, CFG-01, OBS-01, TX-01–03, OUT-01–02, the SER cases of identity-and-serialization that touch a store, and EVT-01–02. Not applicable: TX-04 (no wire between caller and file), RET-01 (SP-021). Declared unsupported (CFG-01 shows the refusal): multi-host. The ReserveAndBegin, RenewMany and FinishMany cases of 15 (G-2 plan) run on SQLite too, plus MGMT-13 against a SQLite-backed management host.

SQLite cases (trait `Item=SP-019`):

| Case | What it runs |
|---|---|
| SQL-01 | Three worker processes and one producer process on one file, 2,000 jobs with a capacity-1 scope and a rate scope: one valid history (reference model), no double grant, every job terminal |
| SQL-02 | A peer process holds `BEGIN IMMEDIATE` past `WriterBusyTimeout`: the operation is not applied, retried with the same command ID, applied once after release; held past NotAfter → `CommandExpired` |
| SQL-03 | Caller staging on a deferred transaction after another process wrote: `SQLITE_BUSY`/`BUSY_SNAPSHOT` reaches the caller unchanged, nothing staged; whole-unit retry stages once. Caller connection with `synchronous=NORMAL` → `transaction_profile_mismatch` before any write. Rollback (TX-01) and commit (TX-02) of business row + job + key |
| SQL-04 | `kill -9` of a writer process at every fault point of §4.1 (Peer `KillProcess`), then restart: job, receipt and permits agree; acknowledged commands present; resending each in-flight command resolves it once |
| SQL-05 | Two readers holding long snapshots while 3 processes renew and finish: WAL growth recorded, readers interrupted at the 1 s budget, checkpoint completes after they end, no renewal misses its margin (report numbers; budget from 06 renewal margin) |
| SQL-06 | Pragmas read back on every driver connection, including after the writer connection is replaced; a file switched to `journal_mode=DELETE` makes startup fail with `store_profile_mismatch`; `Mode=Memory`, `Cache=Shared`, a UNC path (Windows) and an `nfs` mount (Linux, mountinfo fixture) are rejected with their codes |
| SQL-07 | Executor: while the writer is held 1.5 s by another process, the lease watchdog and pool loops keep their timing (no thread-pool starvation, no coordinator blocked inside SQLite); renewal is served before queued dispatches; an open caller staging transaction delays renewals and the watchdog expires the local lease on time |
| SQL-08 | Native assets load on linux-x64 and win-x64; Describe reports `sqlite_version`/`sqlite_source_id`; an engine below 3.38 (fake version provider) is refused (DEP-06) |
| SQL-09 | Encoding vectors: UUID blob order equals `UuidOrder`, text order equals UTF-8 byte order, ms floor/ceil at equality, a fence at `long.MaxValue` incremented → `InvariantFault counter_overflow`, nothing written (ENC-01) |
| SQL-10 | Migration: empty file, re-run no-op, changed checksum → `migration_checksum_mismatch`, foreign `runwick_x` table → `schema_not_empty`, concurrent migrate from two processes → one applies, the other no-op or `migration_lock_timeout`; the outbox source tables in the same file are untouched |
| SQL-11 | Events: 3 writer processes, a reader tailing `EventsSince` from saved cursors while the newest events are deleted by the harness: no event returned twice or skipped |
| SQL-12 | Batches: `RenewMany` of 64 leases with one stale member and one permit-mismatch member renews the other 62 and commits once; `FinishMany` of 32 and `ReserveAndBegin` of 32 each commit once (commit count from the fault injector); for the same seeded workload, batched runs and single-operation runs (batches disabled in the engine) leave identical normalized snapshots; set-based `DispatchMany` of 1,000 items with 40 parameters each crosses the parameter chunk boundary and matches the per-item rows; a `RenewMany` queued behind a `DispatchMany` of 1,000 is served before the next queued dispatch |

## 13. Must not claim

- No multi-host support and no HA: one file on one host.
- No durability beyond the host's disk and its fsync behaviour; a restored file is a history discontinuity (00 §9 rule 12).
- A process kill is not a power-loss test (driver-contract-suite §1); SQL-04 claims crash consistency only.
- Same-host correctness says nothing about throughput parity with PostgreSQL.
