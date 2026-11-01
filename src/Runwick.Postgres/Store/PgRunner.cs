using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Internal;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Runs one driver-owned operation: connection, READ COMMITTED transaction, gates, receipt protocol,
/// body, finalization and commit, with SQLSTATE classification and same-command transient retry (04 §6, §10).
/// </summary>
internal sealed class PgRunner(PgDataSources sources, IFaultInjector faults, TimeProvider time)
{
    const int MaxTransientRetries = 3;
    static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(5);

    long _transientConflicts;
    long _deadlocks;

    public IFaultInjector Faults => faults;

    /// <summary>40P01/40001/55P03 rollbacks observed by this runner (each retried or surfaced); deadlock-freedom evidence for tests.</summary>
    public long TransientConflicts => Interlocked.Read(ref _transientConflicts);

    /// <summary>40P01 rollbacks only; a 55P03 lock timeout or 40001 is not a deadlock (07 I-18).</summary>
    public long Deadlocks => Interlocked.Read(ref _deadlocks);

    long _stalePlans;

    /// <summary>0A000 stale prepared plans retried by this runner (04 §9.2, PF-03).</summary>
    public long StalePlans => Interlocked.Read(ref _stalePlans);

    /// <summary>Known store identity (from Describe); gates compare against it.</summary>
    public Guid? StoreId { get; set; }

    public async Task<StoreReply<T>> ExecuteAsync<T>(
        PgOp op,
        Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay,
        CancellationToken ct) where T : class
    {
        var gate = sources.Gate(op.Lane);
        var lap = PhaseTimings.Start();
        await gate.WaitAsync(ct).ConfigureAwait(false);
        PhaseTimings.Lap(lap, op.Operation, "driverLane");
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    return await RunOnceAsync(op, body, replay, ct).ConfigureAwait(false);
                }
                catch (TransientConflictException) when (attempt < MaxTransientRetries && time.GetUtcNow() < op.NotAfter && !ct.IsCancellationRequested)
                {
                    var nominal = Math.Min(1000, 100 << attempt);
                    var delay = (nominal + 1) / 2 + RandomNumberGenerator.GetInt32(nominal / 2 + 1);
                    await Task.Delay(TimeSpan.FromMilliseconds(delay), time, ct).ConfigureAwait(false);
                }
                catch (TransientConflictException) when (time.GetUtcNow() >= op.NotAfter)
                {
                    // A lock wait outlived NotAfter: rolled back, and the receipt protocol rejects any later
                    // application of this command, so the outcome is known (03 §1.3, CMD-04).
                    return StoreReply<T>.NoEffect(StoreStatus.CommandExpired, new StoreError("command_expired"), PgTime.Floor(time.GetUtcNow()));
                }
                catch (StoreNotAppliedException ex) when (ex.Code == "statement_timeout" && time.GetUtcNow() >= op.NotAfter)
                {
                    return StoreReply<T>.NoEffect(StoreStatus.CommandExpired, new StoreError("command_expired"), PgTime.Floor(time.GetUtcNow()));
                }
                catch (TransientConflictException ex)
                {
                    throw new StoreNotAppliedException("transient_conflict_exhausted", ex.InnerException);
                }
            }
        }
        finally
        {
            gate.Release();
        }
    }

    async Task<StoreReply<T>> RunOnceAsync<T>(
        PgOp op,
        Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay,
        CancellationToken ct) where T : class
    {
        NpgsqlConnection connection;
        var lap = PhaseTimings.Start();
        try
        {
            connection = await sources.For(op.Lane).OpenConnectionAsync(ct).ConfigureAwait(false);
            lap = PhaseTimings.Lap(lap, op.Operation, "open");
        }
        catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or IOException)
        {
            throw new StoreNotAppliedException("store_unavailable", ex);
        }

        var commitSent = false;
        await using (connection.ConfigureAwait(false))
        {
            NpgsqlTransaction? transaction = null;
            try
            {
                transaction = await connection.BeginTransactionAsync(PgParams.ReadCommitted, ct).ConfigureAwait(false);
                var tx = new PgTx(connection, transaction, op, faults, ct);
                await tx.Fault(FaultPoint.BeforeGateLock).ConfigureAwait(false);

                var outcome = await ProtocolAsync(tx, body, replay, Timeouts(op)).ConfigureAwait(false);
                lap = PhaseTimings.Lap(lap, op.Operation, "statements");
                if (outcome is PgOutcome<T>.Rollback rollback)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return rollback.Reply;
                }

                var commit = (PgOutcome<T>.Commit)outcome;
                await tx.Fault(FaultPoint.BeforeCommit).ConfigureAwait(false);
                commitSent = true;
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                PhaseTimings.Lap(lap, op.Operation, "commit");
                await tx.Fault(FaultPoint.AfterCommitBeforeReply).ConfigureAwait(false);
                return commit.Reply;
            }
            catch (PostgresException ex) when (ex.SqlState is "40P01" or "40001" or "55P03")
            {
                Interlocked.Increment(ref _transientConflicts);
                if (ex.SqlState == "40P01")
                {
                    Interlocked.Increment(ref _deadlocks);
                }

                throw new TransientConflictException(ex);
            }
            catch (InjectedFaultException ex) when (!commitSent && ex.Kind is "Deadlock40P01" or "Serialization40001")
            {
                throw new TransientConflictException(ex);
            }
            catch (PostgresException ex) when (!commitSent && IsStalePlan(ex))
            {
                // 0A000 "cached plan must not change result type": a migration changed a table under a statement
                // this connection prepared. Nothing was applied; drop the connection's prepared statements and
                // retry the same command (04 §9.2, PF-03). The pool is cleared too: every pooled connection prepared
                // the same statements, and the retry budget must not be spent one connection at a time.
                Interlocked.Increment(ref _stalePlans);
                await ForgetPreparedAsync(connection, transaction).ConfigureAwait(false);
                sources.For(op.Lane).Clear();
                throw new TransientConflictException(ex);
            }
            catch (PostgresException ex) when (ex.SqlState == "40003")
            {
                throw new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation, ex);
            }
            catch (PostgresException ex)
            {
                // A server error reply (including at COMMIT) means the transaction rolled back.
                return Classify<T>(op, ex);
            }
            catch (InvariantViolationException ex) when (!commitSent)
            {
                return StoreReply<T>.NoEffect(StoreStatus.InvariantFault, new StoreError(ex.Code), time.GetUtcNow());
            }
            catch (Exception ex) when (commitSent && ex is not StoreOutcomeUnknownException)
            {
                throw new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation, ex);
            }
            catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException or OperationCanceledException or InjectedFaultException)
            {
                throw new StoreNotAppliedException(
                    ex is PostgresException { SqlState: "57014" } ? "statement_timeout" : "store_unavailable", ex);
            }
            finally
            {
                if (transaction is not null)
                {
                    await transaction.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Gates, receipt acquisition, body and receipt finalization (no BEGIN/COMMIT): shared by driver-owned runs and staging.</summary>
    async Task<PgOutcome<T>> ProtocolAsync<T>(
        PgTx tx,
        Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay,
        (long LockMs, long StatementMs)? timeouts = null) where T : class
    {
        if (await CheckGatesAsync<T>(tx, timeouts).ConfigureAwait(false) is { } gateReply)
        {
            return new PgOutcome<T>.Rollback(gateReply);
        }

        if (tx.Op.HasReceipt && await AcquireReceiptAsync<T>(tx, replay).ConfigureAwait(false) is { } receipt)
        {
            return new PgOutcome<T>.Rollback(receipt);
        }

        var outcome = await body(tx).ConfigureAwait(false);
        if (outcome is PgOutcome<T>.Commit { Effects: { } effects } batched)
        {
            // Effects and R4 in one round trip (PF-04): every input is known after the decision. The row counts are
            // checked before COMMIT; a wrong count rolls everything back, R4 included, as before.
            var r4 = tx.Op.HasReceipt ? effects.Exec(FinalizeCommand(tx, batched), mutation: false) : null;
            await tx.RunAsync(effects).ConfigureAwait(false);
            if (batched.Verify?.Invoke() is { } verdict)
            {
                return verdict;
            }

            if (r4 is not null && r4.Affected != 1)
            {
                throw new InvalidOperationException("Receipt finalization updated no row.");
            }
        }
        else if (outcome is PgOutcome<T>.Commit commit && tx.Op.HasReceipt)
        {
            await FinalizeReceiptAsync(tx, commit).ConfigureAwait(false);
        }

        return outcome;
    }

    /// <summary>
    /// Caller-owned staging (04 §6.1): the same statements on the caller's transaction, inside a savepoint so a
    /// rejection leaves the caller's transaction as it was. Never begins, commits, rolls back or retries the
    /// caller's transaction; PostgreSQL errors (40001/40P01/...) propagate unchanged.
    /// </summary>
    public async Task<StoreReply<T>> StageAsync<T>(
        NpgsqlTransaction transaction,
        PgOp op,
        Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay,
        CancellationToken ct) where T : class
    {
        const string Savepoint = "runwick_stage";
        var connection = transaction.Connection ?? throw new StagingValidationException("transaction_invalid", "The transaction is completed or has no connection.");
        var tx = new PgTx(connection, transaction, op, faults, ct);
        string? readOnly;
        try
        {
            // The read-only check and the savepoint in one round trip (04 §6.1, PF-02): the savepoint does not depend
            // on the check's result, and a read-only caller transaction gets its savepoint released before the
            // rejection, so it is left as it was.
            using var ro = tx.Sql($"SELECT current_setting('transaction_read_only');\nSAVEPOINT {Savepoint}");
            readOnly = (string?)(await tx.QueryBatchAsync(ro).ConfigureAwait(false))[0][0][0];
        }
        catch (InvalidOperationException ex)
        {
            // Npgsql: completed transaction or closed connection.
            throw new StagingValidationException("transaction_invalid", ex.Message);
        }

        if (readOnly == "on")
        {
            await transaction.ReleaseAsync(Savepoint, ct).ConfigureAwait(false);
            throw new StagingValidationException("transaction_invalid", "The transaction is read-only.");
        }

        PgOutcome<T> outcome;
        try
        {
            outcome = await ProtocolAsync(tx, body, replay).ConfigureAwait(false);
        }
        catch (InvariantViolationException ex)
        {
            await transaction.RollbackAsync(Savepoint, ct).ConfigureAwait(false);
            return StoreReply<T>.NoEffect(StoreStatus.InvariantFault, new StoreError(ex.Code), time.GetUtcNow());
        }

        if (outcome is PgOutcome<T>.Rollback rollback)
        {
            await transaction.RollbackAsync(Savepoint, ct).ConfigureAwait(false);
            return rollback.Reply;
        }

        await transaction.ReleaseAsync(Savepoint, ct).ConfigureAwait(false);
        return ((PgOutcome<T>.Commit)outcome).Reply;
    }

    internal static bool IsStalePlan(PostgresException ex) =>
        ex.SqlState == "0A000" && ex.MessageText.Contains("cached plan", StringComparison.Ordinal);

    static async Task ForgetPreparedAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction)
    {
        try
        {
            if (transaction is not null)
            {
                await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }

            connection.UnprepareAll();
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException or InvalidOperationException)
        {
            // The connection is broken or closing; the pool discards it and its statements with it.
        }
    }

    static StoreReply<T> Classify<T>(PgOp op, PostgresException ex) where T : class
    {
        var (status, code) = ex.SqlState switch
        {
            "RWR01" => (StoreStatus.InvariantFault, "receipt_incomplete"),
            "23505" when ex.ConstraintName is "jobs_pk" or "job_attempts_pk" or "concurrency_scopes_pk" => (StoreStatus.InvariantFault, "id_collision"),
            "23505" when ex.ConstraintName == "job_attempts_one_open" => (StoreStatus.InvariantFault, "invalid_transition"),
            var s when s.StartsWith("23", StringComparison.Ordinal) => (StoreStatus.InvariantFault, "constraint_violation"),
            "42P01" or "42703" or "42883" or "42704" or "3F000" => (StoreStatus.UnsupportedVersion, "schema_incompatible"),
            "57014" => throw new StoreNotAppliedException("statement_timeout", ex),
            _ => throw new StoreNotAppliedException("store_unavailable", ex),
        };
        return StoreReply<T>.NoEffect(status, new StoreError(code, Detail: ex.ConstraintName), DateTimeOffset.UtcNow);
    }

    /// <summary>Session timeouts of 04 §6.1, sent in the gate batch (driver-owned runs only).</summary>
    (long LockMs, long StatementMs) Timeouts(PgOp op)
    {
        var remaining = op.NotAfter == DateTimeOffset.MaxValue ? DefaultBudget : op.NotAfter - time.GetUtcNow();
        var statementMs = (long)Math.Clamp(remaining.TotalMilliseconds, 100, 600_000);
        return (Math.Min(2000, statementMs), statementMs);
    }

    const string TimeoutsSql = """
        SELECT set_config('lock_timeout', @lock || 'ms', true),
               set_config('statement_timeout', @stmt || 'ms', true),
               set_config('idle_in_transaction_session_timeout', '10000ms', true);

        """;

    const string G1Sql = """
        SELECT store_id, schema_revision, protocol_epoch, migration_mode
        FROM runwick.store_metadata WHERE singleton_id = 1 FOR SHARE
        """;

    const string G2Sql = """
        SELECT revision, required_protocol_epoch, required_capabilities::text, admission_mode
        FROM runwick.application_controls WHERE application = @application FOR SHARE
        """;

    /// <summary>
    /// G1/G2 with the checks of 03 §10.1; returns a rejection or null. The timeouts, G1 and G2 go in one round trip
    /// (04 §6.1: their inputs do not depend on prior results; G2 runs after G1's lock and no decision precedes it).
    /// RegisterApplication seeds its G2 row after G1's checks, so it sends G2 separately.
    /// </summary>
    async Task<StoreReply<T>?> CheckGatesAsync<T>(PgTx tx, (long LockMs, long StatementMs)? timeouts) where T : class
    {
        var op = tx.Op;
        var seeds = op.Operation == Operations.RegisterApplication;
        List<object?[]>? apps = null;
        using (var g1 = tx.Sql((timeouts is null ? "" : TimeoutsSql) + G1Sql + (seeds ? "" : ";\n" + G2Sql)))
        {
            if (timeouts is { } t)
            {
                g1.P("lock", t.LockMs.ToString(System.Globalization.CultureInfo.InvariantCulture))
                  .P("stmt", t.StatementMs.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }

            var sets = await tx.QueryBatchAsync(g1).ConfigureAwait(false);
            var rows = sets[timeouts is null ? 0 : 1];
            if (!seeds)
            {
                apps = sets[^1];
            }

            if (rows.Count == 0)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "schema_incompatible");
            }

            var s = (StoreId: (Guid)rows[0][0]!, Revision: (long)rows[0][1]!, Epoch: (int)rows[0][2]!, Mode: (string)rows[0][3]!);
            var expected = op.ExpectedStoreId ?? StoreId;
            if (expected is { } id && id != s.StoreId)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "store_identity_mismatch");
            }

            if (s.Mode != "normal" || s.Revision == 0)
            {
                return tx.Reject<T>(StoreStatus.AdmissionClosed, "store_maintenance");
            }

            if (s.Revision != Migrations.PgMigrator.CurrentRevision || s.Epoch != 1)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "schema_incompatible");
            }
        }

        if (seeds)
        {
            using var seed = tx.Sql("""
                INSERT INTO runwick.application_controls (application, created_at, updated_at)
                VALUES (@application, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp()))
                ON CONFLICT DO NOTHING
                """);
            await seed.ExecuteNonQueryAsync(tx.Ct).ConfigureAwait(false);
            using var g2 = tx.Sql(G2Sql);
            apps = (await tx.QueryBatchAsync(g2).ConfigureAwait(false))[0];
        }

        if (apps!.Count == 0)
        {
            return tx.Reject<T>(StoreStatus.AdmissionClosed, "application_not_registered");
        }

        var app = (Epoch: (int)apps[0][1]!, Caps: (string)apps[0][2]!, Mode: (string)apps[0][3]!);
        if (op.IsCreation && (app.Epoch > 1 || UnknownCapabilities(app.Caps)))
        {
            return tx.Reject<T>(StoreStatus.UnsupportedVersion, "capability_unsupported");
        }

        return app.Mode switch
        {
            "open" => null,
            "drain" when op.AllowedWhileDraining => null,
            "drain" => tx.Reject<T>(StoreStatus.AdmissionClosed, "application_draining"),
            _ when op.Operation is "ResolveCommand" or Operations.RegisterApplication => null,
            _ => tx.Reject<T>(StoreStatus.AdmissionClosed, "application_maintenance"),
        };
    }

    static bool UnknownCapabilities(string json)
    {
        using var doc = JsonDocument.Parse(json);
        foreach (var p in doc.RootElement.EnumerateObject())
        {
            if (!Hosting.RunwickRuntime.KnownCapabilities.TryGetValue(p.Name, out var v) || !p.Value.TryGetInt32(out var need) || need > v)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>R1–R3 and the expiry/cap checks (04 §6.3). Returns a terminal reply (replay, conflict, expiry) or null to proceed.</summary>
    static async Task<StoreReply<T>?> AcquireReceiptAsync<T>(PgTx tx, Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay) where T : class
    {
        var op = tx.Op;
        var retentionClass = Operations.RetentionClass(op.Operation);
        bool inserted;
        bool crossClass;
        DateTimeOffset now;

        // R1, R3 and the clock read in one round trip (04 §6.1): R3 and the clock run after R1's insert or wait, and
        // their results are used only when R1 inserted; otherwise R2 follows as before.
        using (var r1 = tx.Sql("""
            INSERT INTO runwick.storage_commands (
                application, retention_class, command_id, operation, operation_version, protocol_epoch,
                fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
            VALUES (@application, @class, @command_id, @operation, 1, 1, @fingerprint, 1, @not_after, 1,
                    @not_after + CASE @class WHEN 'ownership' THEN interval '10 minutes' ELSE interval '24 hours' END)
            ON CONFLICT DO NOTHING
            RETURNING true;

            SELECT 1 FROM runwick.storage_commands
            WHERE application = @application AND command_id = @command_id AND retention_class <> @class
            LIMIT 1;

            SELECT clock_timestamp()
            """).P("class", retentionClass).P("command_id", op.CommandId).P("operation", op.Operation)
                .P("fingerprint", op.Fingerprint, NpgsqlDbType.Bytea).P("not_after", op.NotAfter))
        {
            var sets = await tx.QueryBatchAsync(r1).ConfigureAwait(false);
            inserted = sets[0].Count == 1;
            crossClass = sets[1].Count > 0;
            now = new DateTimeOffset(DateTime.SpecifyKind((DateTime)sets[2][0][0]!, DateTimeKind.Utc));
        }

        if (inserted)
        {
            if (crossClass)
            {
                return tx.Reject<T>(StoreStatus.CommandConflict, "command_conflict");
            }
        }
        else
        {
            var row = await ReadReceiptAsync(tx, retentionClass).ConfigureAwait(false);
            if (row is null)
            {
                // Same ID under the other class only: a different operation.
                return tx.Reject<T>(StoreStatus.CommandConflict, "command_conflict");
            }

            if (row.Operation != op.Operation || row.OperationVersion != 1 || !row.Fingerprint.AsSpan().SequenceEqual(op.Fingerprint))
            {
                return tx.Reject<T>(StoreStatus.CommandConflict, "command_conflict");
            }

            if (row.Result is null || row.Status is null || row.DecidedAt is null)
            {
                return tx.Reject<T>(StoreStatus.InvariantFault, "receipt_incomplete");
            }

            if (replay is null)
            {
                return tx.Reject<T>(StoreStatus.InvariantFault, "receipt_incomplete", detail: "no replay codec");
            }

            return await replay(tx, row).ConfigureAwait(false);
        }

        if (now >= op.NotAfter)
        {
            return StoreReply<T>.NoEffect(StoreStatus.CommandExpired, new StoreError("command_expired"), PgTime.Floor(now));
        }

        if (op.NotAfter - now > Operations.DeadlineCap(op.Operation))
        {
            return StoreReply<T>.NoEffect(StoreStatus.InvalidRequest, new StoreError("deadline_too_far"), PgTime.Floor(now));
        }

        return null;
    }

    internal static async Task<ReceiptRow?> ReadReceiptAsync(PgTx tx, string retentionClass)
    {
        using var r2 = tx.Sql("""
            SELECT operation, operation_version, fingerprint, not_after, status, result::text, decided_at
            FROM runwick.storage_commands
            WHERE application = @application AND retention_class = @class AND command_id = @command_id
            FOR UPDATE
            """).P("class", retentionClass).P("command_id", tx.Op.CommandId);
        var rows = await tx.QueryAsync(r2, r => new ReceiptRow(
            r.GetString(0), r.GetInt32(1), (byte[])r[2], PgTime.Read(r, 3),
            r.IsDBNull(4) ? null : r.GetString(4),
            r.IsDBNull(5) ? null : JsonNode.Parse(r.GetString(5))!.AsObject(),
            PgTime.ReadNullable(r, 6))).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    static NpgsqlCommand FinalizeCommand<T>(PgTx tx, PgOutcome<T>.Commit commit) where T : class =>
        tx.Sql("""
            UPDATE runwick.storage_commands
            SET status = @status, result = @result, decided_at = @decided_at
            WHERE application = @application AND retention_class = @class AND command_id = @command_id AND result IS NULL
            """).P("status", commit.Reply.Status.ToString()).Json("result", commit.Result ?? [])
                .P("decided_at", commit.Reply.DecidedAt).P("class", Operations.RetentionClass(tx.Op.Operation)).P("command_id", tx.Op.CommandId);

    static async Task FinalizeReceiptAsync<T>(PgTx tx, PgOutcome<T>.Commit commit) where T : class
    {
        using var r4 = FinalizeCommand(tx, commit);
        if (await tx.ExecAsync(r4, mutation: false).ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException("Receipt finalization updated no row.");
        }
    }

    sealed class TransientConflictException(Exception inner) : Exception("transient conflict", inner);
}

/// <summary>The caller's transaction cannot host staging (01 §15); mapped to TransactionValidationException.</summary>
internal sealed class StagingValidationException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}
