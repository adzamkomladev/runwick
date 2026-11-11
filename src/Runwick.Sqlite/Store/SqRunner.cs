using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Internal;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Runs one driver-owned write (20 §4.1): class slot, writer thread, <c>BEGIN IMMEDIATE</c>, the decision time read after
/// the lock, gates, receipt, body, receipt finalization and COMMIT, with the 20 §4.3 error mapping and same-command
/// retry of a busy lock.
/// </summary>
internal sealed class SqRunner(SqExecutor executor, SqliteOptions options, IFaultInjector faults, TimeProvider time)
{
    const int MaxTransientRetries = 3;
    static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(5);

    long _commits;
    long _busy;

    public IFaultInjector Faults => faults;

    public TimeProvider Time => time;

    public SqExecutor Executor => executor;

    /// <summary>Committed write transactions (SQL-12 counts one commit per batch).</summary>
    public long Commits => Interlocked.Read(ref _commits);

    /// <summary>Busy lock waits that ran out (each retried or surfaced).</summary>
    public long BusyConflicts => Interlocked.Read(ref _busy);

    public Guid? StoreId { get; set; }

    // ------------------------------------------------------------------ entry points

    /// <summary>An operation ported from a PostgreSQL server function: the body returns the reply row.</summary>
    public Task<StoreReply<T>> CallAsync<T>(SqOp op, Func<SqTx, SqRow> body, Func<SqRow, StoreReply<T>> map, CancellationToken ct,
        Func<SqTx, SqRow, SqRow>? onReplay = null) where T : class =>
        WithRetriesAsync(op, () => executor.WriteAsync(op.Lane, conn => RunOnce(conn, op, tx => Protocol(tx, body, onReplay), ct), ct, op.Operation),
            map, ct);

    public Task<StoreReply<DispatchResult>> DispatchAsync(SqOp op, Func<SqTx, SqRow> body, Func<SqRow, StoreReply<DispatchResult>> map,
        long bytes, CancellationToken ct) =>
        WithRetriesAsync(op, () => executor.DispatchAsync(this, op, body, bytes, ct), map, ct);

    /// <summary>A C#-bodied operation (as PgRunner.ExecuteAsync).</summary>
    public Task<StoreReply<T>> ExecuteAsync<T>(SqOp op, Func<SqTx, SqOutcome<T>> body, Func<SqTx, SqReceiptRow, StoreReply<T>>? replay,
        CancellationToken ct) where T : class =>
        WithRetriesAsync(op, () => executor.WriteAsync(op.Lane, conn => RunOnceOutcome(conn, op, body, replay, ct), ct, op.Operation), r => r, ct);

    /// <summary>A read (20 §4.2): deferred BEGIN … COMMIT on a reader thread, one snapshot, interrupted at the budget (§5.3).</summary>
    public async Task<T> ReadAsync<T>(Func<SqliteConnection, T> read, TimeSpan budget, CancellationToken ct)
    {
        try
        {
            return await executor.ReadAsync(conn =>
            {
                using var timer = new Timer(_ => SQLitePCL.raw.sqlite3_interrupt(conn.Handle), null, budget, Timeout.InfiniteTimeSpan);
                SqFile.Exec(conn, "BEGIN");
                try
                {
                    return read(conn);
                }
                finally
                {
                    timer.Change(Timeout.Infinite, Timeout.Infinite);
                    if (SqExecutor.InTransaction(conn))
                    {
                        SqFile.Exec(conn, "COMMIT");
                    }
                }
            }, ct).ConfigureAwait(false);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 9)
        {
            throw new StoreNotAppliedException("statement_timeout", ex);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 1 && ex.Message.Contains("no such", StringComparison.Ordinal))
        {
            throw new StoreIncompatibleException("The Runwick SQLite schema is not installed or differs; run the migrator.", ex);
        }
        catch (SqliteException ex)
        {
            throw new StoreNotAppliedException("store_unavailable", ex);
        }
    }

    public Task<T> ReadAsync<T>(Func<SqliteConnection, T> read, CancellationToken ct) => ReadAsync(read, DefaultBudget, ct);

    // ------------------------------------------------------------------ retry and slot

    Task<StoreReply<T>> WithRetriesAsync<T, TRaw>(SqOp op, Func<Task<TRaw>> submit, Func<TRaw, StoreReply<T>> map,
        CancellationToken ct) where T : class =>
        Telemetry.Telemetry.MeasureStorage(op.Operation, (int)op.Lane, () => WithRetriesCoreAsync(op, submit, map, ct)); // 18 §3.1

    async Task<StoreReply<T>> WithRetriesCoreAsync<T, TRaw>(SqOp op, Func<Task<TRaw>> submit, Func<TRaw, StoreReply<T>> map,
        CancellationToken ct) where T : class
    {
        var gate = executor.Gate(op.Lane);
        var laneStarted = PhaseTimings.Start();
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            PhaseTimings.Lap(laneStarted, op.Operation, "driverLane");
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    var raw = await submit().ConfigureAwait(false);
                    return map(raw);
                }
                catch (SqBusyException) when (attempt < MaxTransientRetries && time.GetUtcNow() < op.NotAfter && !ct.IsCancellationRequested)
                {
                    var nominal = Math.Min(1000, 100 << attempt);
                    var delay = (nominal + 1) / 2 + RandomNumberGenerator.GetInt32(nominal / 2 + 1);
                    await Task.Delay(TimeSpan.FromMilliseconds(delay), time, ct).ConfigureAwait(false);
                }
                catch (SqBusyException) when (time.GetUtcNow() >= op.NotAfter)
                {
                    return StoreReply<T>.NoEffect(StoreStatus.CommandExpired, new StoreError("command_expired"), SqTime.Floor(time.GetUtcNow()));
                }
                catch (SqBusyException ex)
                {
                    throw new StoreNotAppliedException("transient_conflict_exhausted", ex.InnerException);
                }
                catch (OperationCanceledException ex) when (ct.IsCancellationRequested)
                {
                    throw new StoreNotAppliedException("store_unavailable", ex);
                }
            }
        }
        finally
        {
            gate.Release();
        }
    }

    // ------------------------------------------------------------------ one transaction

    /// <summary>The state of one run, so the error mapping knows whether COMMIT was sent.</summary>
    sealed class RunState
    {
        public bool CommitSent;
    }

    internal SqRow RunDispatchOnce(SqliteConnection conn, SqOp op, Func<SqTx, SqRow> body, CancellationToken ct) =>
        RunOnce(conn, op, tx => Protocol(tx, body, null), ct);

    internal void RunDispatchGroup(SqliteConnection conn, List<SqExecutor.DispatchWork> group)
    {
        var state = new RunState();
        var first = group[0].Op;
        var results = new (SqRow? Row, SqTx? Ready, Exception? Error)[group.Count];
        try
        {
            // A busy BEGIN or COMMIT follows the ordinary per-command retry path with the original IDs.
            var lockDeadline = group.Min(item => item.Op.NotAfter);
            var rejected = Guard(conn, first, state, () =>
            {
                BeginWrite(conn, first.Operation, lockDeadline);
                var statementsStarted = PhaseTimings.Start();
                var committedCount = 0;
                var commitDeadline = DateTimeOffset.MaxValue;
                for (var i = 0; i < group.Count; i++)
                {
                    results[i] = RunDispatchSavepoint(conn, group[i]);
                    if (results[i].Ready is { } ready)
                    {
                        committedCount++;
                        commitDeadline = ready.Op.NotAfter < commitDeadline ? ready.Op.NotAfter : commitDeadline;
                    }
                }

                if (committedCount == 0)
                {
                    PhaseTimings.Lap(statementsStarted, first.Operation, "statements");
                    Rollback(conn);
                }
                else
                {
                    CommitWrite(conn, first.Operation, commitDeadline, state, statementsStarted);
                    PhaseTimings.DispatchCommit(committedCount);
                }

                return null!;
            });

            for (var i = 0; i < group.Count; i++)
            {
                var item = group[i];
                var (row, ready, error) = results[i];
                item.Registration.Dispose();
                if (rejected is not null)
                {
                    item.Complete(rejected);
                }
                else if (error is not null)
                {
                    item.Fail(error);
                }
                else
                {
                    try
                    {
                        ready?.Fault(FaultPoint.AfterCommitBeforeReply);
                        item.Complete(row!);
                    }
                    catch (Exception ex)
                    {
                        item.Fail(new StoreOutcomeUnknownException("commit_outcome_unknown", item.Op.CommandId, item.Op.Operation, ex));
                    }
                }
            }
        }
        catch (Exception ex) when (state.CommitSent)
        {
            // Any result that depended on the shared transaction must be resolved using its own command identity.
            foreach (var item in group)
            {
                Exception error = new StoreOutcomeUnknownException("commit_outcome_unknown", item.Op.CommandId, item.Op.Operation, ex);
                item.Fail(ex is SqBrokenConnectionException ? new SqBrokenConnectionException(error) : error);
            }
        }
        catch (SqBrokenConnectionException ex)
        {
            throw new SqBrokenConnectionException(new StoreNotAppliedException("store_unavailable", ex.InnerException ?? ex));
        }
    }

    (SqRow? Row, SqTx? Ready, Exception? Error) RunDispatchSavepoint(SqliteConnection conn, SqExecutor.DispatchWork item)
    {
        var tx = new SqTx(conn, null, item.Op, faults, item.Ct);
        SqFile.Exec(conn, "SAVEPOINT runwick_dispatch");
        try
        {
            SqTx? ready = null;
            var row = Guard(conn, item.Op, state: null, () =>
            {
                item.Ct.ThrowIfCancellationRequested();
                ReadDecisionTime(tx);
                var result = Protocol(tx, item.Body, null);
                if (!result.Commits)
                {
                    RollbackDispatchSavepoint(conn);
                    return result;
                }

                Finalize(tx, result.Status, result.Result ?? [], result.Decided);
                CheckBeforeCommit(tx);
                item.Ct.ThrowIfCancellationRequested();
                SqFile.Exec(conn, "RELEASE runwick_dispatch");
                ready = tx;
                return result;
            }, () => RollbackDispatchSavepoint(conn));
            return (row, ready, null);
        }
        catch (Exception ex) when (ex is not SqBrokenConnectionException)
        {
            return (null, null, ex);
        }
    }

    static void RollbackDispatchSavepoint(SqliteConnection conn)
    {
        try
        {
            SqFile.Exec(conn, "ROLLBACK TO runwick_dispatch; RELEASE runwick_dispatch");
        }
        catch (Exception ex) when (ex is SqliteException or InvalidOperationException)
        {
            throw new SqBrokenConnectionException(ex);
        }
    }

    SqRow RunOnce(SqliteConnection conn, SqOp op, Func<SqTx, SqRow> protocol, CancellationToken ct)
    {
        var state = new RunState();
        return Guard(conn, op, state, () =>
        {
            var tx = Begin(conn, op, ct);
            var statementsStarted = PhaseTimings.Start();
            var row = protocol(tx);
            if (!row.Commits)
            {
                PhaseTimings.Lap(statementsStarted, op.Operation, "statements");
                Rollback(conn);
                return row;
            }

            if (op.HasReceipt)
            {
                Finalize(tx, row.Status, row.Result ?? [], row.Decided);
            }

            Commit(tx, state, statementsStarted);
            return row;
        });
    }

    StoreReply<T> RunOnceOutcome<T>(SqliteConnection conn, SqOp op, Func<SqTx, SqOutcome<T>> body, Func<SqTx, SqReceiptRow, StoreReply<T>>? replay,
        CancellationToken ct) where T : class
    {
        var state = new RunState();
        return Guard(conn, op, state, () =>
        {
            var tx = Begin(conn, op, ct);
            var statementsStarted = PhaseTimings.Start();
            var outcome = OutcomeProtocol(tx, body, replay);
            if (outcome is SqOutcome<T>.Rollback rollback)
            {
                PhaseTimings.Lap(statementsStarted, op.Operation, "statements");
                Rollback(conn);
                return rollback.Reply;
            }

            var commit = (SqOutcome<T>.Commit)outcome;
            if (op.HasReceipt)
            {
                Finalize(tx, commit.Reply.Status.ToString(), commit.Result ?? [], commit.Reply.DecidedAt);
            }

            Commit(tx, state, statementsStarted);
            return commit.Reply;
        }, (status, code) => StoreReply<T>.NoEffect(status, new StoreError(code), SqTime.Floor(time.GetUtcNow())));
    }

    SqRow Guard(SqliteConnection conn, SqOp op, RunState? state, Func<SqRow> run, Action? rollback = null) =>
        Guard<SqRow>(conn, op, state, run, (status, code) => SqRow.NoEffect(status.ToString(), code, SqTime.Floor(time.GetUtcNow())), rollback);

    /// <summary>The 20 §4.3 mapping around one transaction; a failed ROLLBACK replaces the writer connection.</summary>
    T Guard<T>(SqliteConnection conn, SqOp op, RunState? state, Func<T> run, Func<StoreStatus, string, T> noEffect, Action? rollback = null)
    {
        void Revert()
        {
            if (rollback is null)
            {
                SafeRollback(conn);
            }
            else
            {
                rollback();
            }
        }

        try
        {
            return run();
        }
        catch (Exception ex) when (state?.CommitSent == true)
        {
            Revert();
            if (ex is StoreOutcomeUnknownException)
            {
                throw;
            }

            throw new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation, ex);
        }
        catch (SqBusyException)
        {
            Revert();
            Interlocked.Increment(ref _busy);
            throw;
        }
        catch (SqInvariantException ex)
        {
            Revert();
            return noEffect(StoreStatus.InvariantFault, ex.Code);
        }
        catch (SqliteException ex)
        {
            Revert();
            var (status, code) = Classify(ex);
            return noEffect(status, code);
        }
        catch (Exception ex) when (ex is InjectedFaultException or IOException or OperationCanceledException)
        {
            Revert();
            throw new StoreNotAppliedException("store_unavailable", ex);
        }
        catch
        {
            Revert();
            throw;
        }
    }

    /// <summary>
    /// SQLite result → (status, code) for a transaction that rolled back (20 §4.3). Unavailable conditions throw
    /// <see cref="StoreNotAppliedException"/>.
    /// </summary>
    internal static (StoreStatus, string) Classify(SqliteException ex) => (ex.SqliteErrorCode, ex.SqliteExtendedErrorCode) switch
    {
        (19, 1555 or 2067) when UniqueColumns(ex) is "runwick_jobs.application, runwick_jobs.job_id"
            or "runwick_job_attempts.application, runwick_job_attempts.attempt_id"
            or "runwick_concurrency_scopes.application, runwick_concurrency_scopes.scope_id" => (StoreStatus.InvariantFault, "id_collision"),
        (19, 2067) when UniqueColumns(ex) is "runwick_job_attempts.application, runwick_job_attempts.job_id" =>
            (StoreStatus.InvariantFault, "invalid_transition"),
        (19, 3091) => (StoreStatus.InvariantFault, "counter_overflow"),
        (19, _) => (StoreStatus.InvariantFault, "constraint_violation"),
        (1, _) when ex.Message.Contains("no such", StringComparison.Ordinal) => (StoreStatus.UnsupportedVersion, "schema_incompatible"),
        (1, _) when ex.Message.Contains("integer overflow", StringComparison.Ordinal) => (StoreStatus.InvariantFault, "counter_overflow"),
        (6, _) => (StoreStatus.InvariantFault, "shared_cache_lock"),
        (9, _) => throw new StoreNotAppliedException("statement_timeout", ex),
        _ => throw new StoreNotAppliedException("store_unavailable", ex),
    };

    /// <summary>The column list of a UNIQUE/PRIMARY KEY failure message ("UNIQUE constraint failed: t.a, t.b").</summary>
    static string? UniqueColumns(SqliteException ex)
    {
        var at = ex.Message.IndexOf("constraint failed: ", StringComparison.Ordinal);
        return at < 0 ? null : ex.Message[(at + "constraint failed: ".Length)..].Trim().TrimEnd('\'', '.');
    }

    SqTx Begin(SqliteConnection conn, SqOp op, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        BeginWrite(conn, op.Operation, op.NotAfter);
        var tx = new SqTx(conn, null, op, faults, ct);
        ReadDecisionTime(tx);
        return tx;
    }

    void BeginWrite(SqliteConnection conn, string operation, DateTimeOffset notAfter)
    {
        var remaining = notAfter == DateTimeOffset.MaxValue ? DefaultBudget : notAfter - time.GetUtcNow();
        var wait = Math.Clamp((long)Math.Min(options.WriterBusyTimeout.TotalMilliseconds, remaining.TotalMilliseconds), 0, 30_000);
        SqFile.SetBusyTimeout(conn, wait);
        var beginStarted = PhaseTimings.Start();
        try
        {
            SqFile.Exec(conn, "BEGIN IMMEDIATE");
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 5)
        {
            throw new SqBusyException(ex);
        }
        finally
        {
            PhaseTimings.Lap(beginStarted, operation, "beginImmediate");
        }
    }

    void ReadDecisionTime(SqTx tx)
    {
        tx.Fault(FaultPoint.AfterLocksBeforeFreshTime);
        tx.Clock = time.GetUtcNow();
        tx.Now = SqTime.Floor(tx.Clock);
    }

    void Commit(SqTx tx, RunState state, long statementsStarted)
    {
        CheckBeforeCommit(tx);
        CommitWrite(tx.Connection, tx.Op.Operation, tx.Op.NotAfter, state, statementsStarted);
        if (tx.Op.Operation == Operations.Dispatch)
        {
            PhaseTimings.DispatchCommit(1);
        }

        tx.Fault(FaultPoint.AfterCommitBeforeReply);
    }

    static void CheckBeforeCommit(SqTx tx)
    {
        tx.Fault(FaultPoint.BeforeCommit);
        if (tx.Op.HasReceipt)
        {
            // The PostgreSQL deferred trigger has no SQLite twin: the receipt this transaction inserted must be decided (20 §3).
            using var check = tx.Sql("""
                SELECT count(*) FROM runwick_storage_commands
                WHERE application = @application AND command_id = @command_id AND result IS NULL
                """).P("command_id", tx.Op.CommandId);
            if (tx.Count(check) != 0)
            {
                throw new SqInvariantException("receipt_incomplete");
            }
        }
    }

    void CommitWrite(SqliteConnection conn, string operation, DateTimeOffset notAfter, RunState state, long statementsStarted)
    {
        var commitStarted = PhaseTimings.Lap(statementsStarted, operation, "statements");
        state.CommitSent = true;
        for (var tries = 0; ; tries++)
        {
            try
            {
                SqFile.Exec(conn, "COMMIT");
                break;
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5 && tries < 5 && time.GetUtcNow() < notAfter)
            {
                // COMMIT busy leaves the transaction open: retry while the budget remains (20 §4.3).
                Thread.Sleep(20);
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5)
            {
                state.CommitSent = false;
                throw new SqBusyException(ex);
            }
        }

        Interlocked.Increment(ref _commits);
        PhaseTimings.Lap(commitStarted, operation, "commit");
    }

    static void Rollback(SqliteConnection conn)
    {
        if (SqExecutor.InTransaction(conn))
        {
            SqFile.Exec(conn, "ROLLBACK");
        }
    }

    static void SafeRollback(SqliteConnection conn)
    {
        try
        {
            Rollback(conn);
        }
        catch (Exception ex) when (ex is SqliteException or InvalidOperationException)
        {
            throw new SqBrokenConnectionException(ex);
        }
    }

    // ------------------------------------------------------------------ frame: gates and receipt

    /// <summary>The frame of rw_frame_v1, then the body. A replayed receipt goes to <paramref name="onReplay"/>.</summary>
    internal SqRow Protocol(SqTx tx, Func<SqTx, SqRow> body, Func<SqTx, SqRow, SqRow>? onReplay)
    {
        if (Frame(tx) is { } frame)
        {
            return frame.Replayed && onReplay is not null ? onReplay(tx, frame) : frame;
        }

        return body(tx);
    }

    /// <summary>Gates, then the receipt protocol; null = proceed.</summary>
    internal SqRow? Frame(SqTx tx)
    {
        if (Gates(tx) is { } gate)
        {
            return gate;
        }

        // An operation without a receipt (Renew, RenewMany) still refuses a command whose deadline has passed, as the PostgreSQL frame does.
        if (!tx.Op.HasReceipt)
        {
            return tx.Clock >= tx.Op.NotAfter ? SqRow.NoEffect("CommandExpired", "command_expired", tx.Now) : null;
        }

        return Receipt(tx);
    }

    SqOutcome<T> OutcomeProtocol<T>(SqTx tx, Func<SqTx, SqOutcome<T>> body, Func<SqTx, SqReceiptRow, StoreReply<T>>? replay) where T : class
    {
        if (Gates(tx) is { } gate)
        {
            return new SqOutcome<T>.Rollback(gate.Reject<T>());
        }

        if (tx.Op.HasReceipt)
        {
            if (Receipt(tx) is { } r)
            {
                if (!r.Replayed)
                {
                    return new SqOutcome<T>.Rollback(r.Reject<T>());
                }

                if (replay is null)
                {
                    return new SqOutcome<T>.Rollback(tx.Reject<T>(StoreStatus.InvariantFault, "receipt_incomplete", detail: "no replay codec"));
                }

                var row = new SqReceiptRow(tx.Op.Operation, 1, tx.Op.Fingerprint!, tx.Op.NotAfter, r.Status, r.Result, r.DecidedAt);
                return new SqOutcome<T>.Rollback(replay(tx, row));
            }
        }

        return body(tx);
    }

    /// <summary>
    /// Set by the test harness only: the binary says so to the gate with the <c>rw:harness</c> capability marker (as on
    /// PostgreSQL). SQLite stores never have fault hooks, so it changes nothing else.
    /// </summary>
    internal static bool HarnessMarker { get; set; }

    /// <summary>G1/G2 with the 03 §10.1 checks (20 §4.1 step 4); the write lock already excludes gate changes.</summary>
    SqRow? Gates(SqTx tx)
    {
        var op = tx.Op;
        var s = tx.One(tx.Sql("SELECT store_id, schema_revision, protocol_epoch, migration_mode FROM runwick_store_metadata WHERE singleton_id = 1"),
            r => (StoreId: r.Uuid(0), Revision: r.GetInt64(1), Epoch: r.GetInt32(2), Mode: r.GetString(3)));
        if (s == default)
        {
            return SqRow.NoEffect("UnsupportedVersion", "schema_incompatible", null);
        }

        if ((op.ExpectedStoreId ?? StoreId) is { } id && id != s.StoreId)
        {
            return SqRow.NoEffect("UnsupportedVersion", "store_identity_mismatch", null);
        }

        if (s.Mode != "normal" || s.Revision == 0)
        {
            return SqRow.NoEffect("AdmissionClosed", "store_maintenance", null);
        }

        if (s.Revision != Migrations.SqliteMigrator.CurrentRevision || s.Epoch != 1)
        {
            return SqRow.NoEffect("UnsupportedVersion", "schema_incompatible", null);
        }

        if (op.Operation == Operations.RegisterApplication)
        {
            tx.ApplicationSeeded = tx.Exec(tx.Sql("""
                INSERT INTO runwick_application_controls (application, created_at, updated_at) VALUES (@application, @now, @now)
                ON CONFLICT DO NOTHING
                """).P("now", tx.Now), mutation: false) == 1;
        }

        var app = tx.One(tx.Sql("""
            SELECT required_protocol_epoch, required_capabilities, admission_mode FROM runwick_application_controls WHERE application = @application
            """), r => (Epoch: r.GetInt32(0), Caps: r.GetString(1), Mode: r.GetString(2)));
        if (app == default)
        {
            return SqRow.NoEffect("AdmissionClosed", "application_not_registered", null);
        }

        if (op.IsCreation && (app.Epoch > 1 || UnknownCapabilities(app.Caps)))
        {
            return SqRow.NoEffect("UnsupportedVersion", "capability_unsupported", null);
        }

        return app.Mode switch
        {
            "open" => null,
            "drain" when op.AllowedWhileDraining => null,
            "drain" => SqRow.NoEffect("AdmissionClosed", "application_draining", null),
            _ when op.Operation is "ResolveCommand" or Operations.RegisterApplication => null,
            _ => SqRow.NoEffect("AdmissionClosed", "application_maintenance", null),
        };
    }

    static bool UnknownCapabilities(string json)
    {
        using var doc = JsonDocument.Parse(json);
        foreach (var p in doc.RootElement.EnumerateObject())
        {
            if (p.Name == "rw:harness" && HarnessMarker)
            {
                continue;
            }

            if (!Hosting.RunwickRuntime.KnownCapabilities.TryGetValue(p.Name, out var v) || !p.Value.TryGetInt32(out var need) || need > v)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>R1–R3, expiry and the deadline cap (rw_receipt_v1). Null = new command, placeholder inserted.</summary>
    internal SqRow? Receipt(SqTx tx)
    {
        var op = tx.Op;
        var cls = Operations.RetentionClass(op.Operation);
        var row = tx.One(tx.Sql("""
            SELECT retention_class, operation, operation_version, fingerprint, status, result, decided_at
            FROM runwick_storage_commands WHERE application = @application AND command_id = @command_id
            ORDER BY retention_class = @class DESC LIMIT 1
            """).P("command_id", op.CommandId).P("class", cls),
            r => new { Class = r.GetString(0), Op = r.GetString(1), Version = r.GetInt32(2), Fp = r.Blob(3), Status = r.StrN(4),
                Result = r.JsonN(5), Decided = r.TimeN(6) });
        if (row is not null)
        {
            if (row.Class != cls || row.Op != op.Operation || row.Version != 1 || !row.Fp.AsSpan().SequenceEqual(op.Fingerprint))
            {
                return SqRow.NoEffect("CommandConflict", "command_conflict", null);
            }

            if (row.Result is null || row.Status is null || row.Decided is null)
            {
                return SqRow.NoEffect("InvariantFault", "receipt_incomplete", null);
            }

            var replay = SqRow.Applied(row.Status, null, row.Decided.Value, row.Result);
            replay.Replayed = true;
            return replay;
        }

        if (tx.Clock >= op.NotAfter)
        {
            return SqRow.NoEffect("CommandExpired", "command_expired", tx.Now);
        }

        if (op.NotAfter - tx.Clock > Operations.DeadlineCap(op.Operation))
        {
            return SqRow.NoEffect("InvalidRequest", "deadline_too_far", tx.Now);
        }

        var notAfter = SqTime.Floor(op.NotAfter);
        tx.Exec(tx.Sql("""
            INSERT INTO runwick_storage_commands (application, retention_class, command_id, operation, operation_version, protocol_epoch,
                fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
            VALUES (@application, @class, @command_id, @operation, 1, 1, @fingerprint, 1, @not_after, 1, @retain)
            """).P("class", cls).P("command_id", op.CommandId).P("operation", op.Operation).P("fingerprint", op.Fingerprint)
            .P("not_after", notAfter).P("retain", notAfter + Operations.RetentionFloor(op.Operation)), mutation: false);
        return null;
    }

    /// <summary>R4 with its row-count check.</summary>
    internal static void Finalize(SqTx tx, string status, JsonObject result, DateTimeOffset decidedAt)
    {
        var n = tx.Exec(tx.Sql("""
            UPDATE runwick_storage_commands SET status = @status, result = @result, decided_at = @decided
            WHERE application = @application AND retention_class = @class AND command_id = @command_id AND result IS NULL
            """).P("status", status).P("result", result.ToJsonString()).P("decided", decidedAt)
            .P("class", Operations.RetentionClass(tx.Op.Operation)).P("command_id", tx.Op.CommandId), mutation: false);
        if (n != 1)
        {
            throw new InvalidOperationException("Receipt finalization updated no row.");
        }
    }

    internal static SqReceiptRow? ReadReceipt(SqTx tx, string retentionClass) =>
        tx.One(tx.Sql("""
            SELECT operation, operation_version, fingerprint, not_after, status, result, decided_at, receipt_codec_version
            FROM runwick_storage_commands WHERE application = @application AND retention_class = @class AND command_id = @command_id
            """).P("class", retentionClass).P("command_id", tx.Op.CommandId),
            r => new SqReceiptRow(r.GetString(0), r.GetInt32(1), r.Blob(2), r.Time(3), r.StrN(4), r.JsonN(5), r.TimeN(6), r.GetInt32(7)));

    // ------------------------------------------------------------------ caller transactions (20 §8)

    /// <summary>
    /// Staging on the caller's transaction (20 §8): <c>SAVEPOINT runwick_stage</c> … <c>RELEASE</c>, or ROLLBACK TO + RELEASE.
    /// Never begins, commits, rolls back or retries the caller's transaction; SQLite errors propagate unchanged.
    /// </summary>
    public StoreReply<T> Stage<T>(SqliteTransaction transaction, SqOp op, Func<SqTx, SqRow> body, Func<SqRow, StoreReply<T>> map,
        Func<SqTx, SqRow, SqRow>? onReplay, CancellationToken ct) where T : class
    {
        var conn = transaction.Connection ?? throw new SqStagingValidationException("transaction_invalid", "The transaction is completed.");
        ValidateCaller(conn, transaction);
        var tx = new SqTx(conn, transaction, op, faults, ct);
        tx.Raw("SAVEPOINT runwick_stage");
        try
        {
            tx.Fault(FaultPoint.AfterLocksBeforeFreshTime);
            tx.Clock = time.GetUtcNow();
            tx.Now = SqTime.Floor(tx.Clock);
            var row = Protocol(tx, body, onReplay);
            if (row.Commits && op.HasReceipt)
            {
                Finalize(tx, row.Status, row.Result ?? [], row.Decided);
            }

            EndStage(tx, release: row.Commits);
            return map(row);
        }
        catch (SqInvariantException ex)
        {
            EndStage(tx, release: false);
            return StoreReply<T>.NoEffect(StoreStatus.InvariantFault, new StoreError(ex.Code), time.GetUtcNow());
        }
        catch
        {
            EndStage(tx, release: false);
            throw;
        }
    }

    /// <summary>
    /// 20 §8 rule 1, before any write: the caller's main file is the configured file and its connection has
    /// <c>foreign_keys = 1</c> and <c>synchronous = 2</c>. The store identity is the frame's G1 gate inside the savepoint.
    /// </summary>
    void ValidateCaller(SqliteConnection conn, SqliteTransaction transaction)
    {
        string? main = null;
        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = transaction;
            cmd.CommandText = "SELECT file FROM pragma_database_list WHERE name = 'main'";
            main = cmd.ExecuteScalar() as string;
        }

        var configured = Path.GetFullPath(SqFile.FilePath(executor.ConnectionString));
        if (string.IsNullOrEmpty(main) || !string.Equals(Path.GetFullPath(main), configured, StringComparison.Ordinal))
        {
            throw new SqStagingValidationException(RunwickErrorCodes.TransactionStoreMismatch, "The transaction's database file is not the configured Runwick store.");
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = transaction;
            cmd.CommandText = "SELECT (SELECT foreign_keys FROM pragma_foreign_keys) = 1 AND (SELECT synchronous FROM pragma_synchronous) = 2";
            if (Convert.ToInt64(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) != 1)
            {
                throw new SqStagingValidationException(RunwickErrorCodes.TransactionProfileMismatch,
                    "The transaction's connection needs PRAGMA foreign_keys = 1 and PRAGMA synchronous = FULL (2).");
            }
        }
    }

    static void EndStage(SqTx tx, bool release)
    {
        try
        {
            tx.Raw(release ? "RELEASE runwick_stage" : "ROLLBACK TO runwick_stage; RELEASE runwick_stage");
        }
        catch (SqliteException) when (!release)
        {
            // A failing rollback is swallowed, as the SQLite outbox source does (14 Changes 2026-11-01).
        }
    }
}

/// <summary>The write lock was not granted within the busy timeout: not applied, transient (20 §4.1 step 2).</summary>
internal sealed class SqBusyException(Exception inner) : Exception("database busy", inner);
