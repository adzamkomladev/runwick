using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
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

    public IFaultInjector Faults => faults;

    /// <summary>Known store identity (from Describe); gates compare against it.</summary>
    public Guid? StoreId { get; set; }

    public async Task<StoreReply<T>> ExecuteAsync<T>(
        PgOp op,
        Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay,
        CancellationToken ct) where T : class
    {
        var gate = sources.Gate(op.Lane);
        await gate.WaitAsync(ct).ConfigureAwait(false);
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
        try
        {
            connection = await sources.For(op.Lane).OpenConnectionAsync(ct).ConfigureAwait(false);
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
                await SetTimeoutsAsync(tx, op).ConfigureAwait(false);
                await tx.Fault(FaultPoint.BeforeGateLock).ConfigureAwait(false);

                if (await CheckGatesAsync<T>(tx).ConfigureAwait(false) is { } gateReply)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return gateReply;
                }

                if (op.HasReceipt)
                {
                    var receipt = await AcquireReceiptAsync<T>(tx, replay).ConfigureAwait(false);
                    if (receipt is not null)
                    {
                        await transaction.RollbackAsync(ct).ConfigureAwait(false);
                        return receipt;
                    }
                }

                var outcome = await body(tx).ConfigureAwait(false);
                if (outcome is PgOutcome<T>.Rollback rollback)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return rollback.Reply;
                }

                var commit = (PgOutcome<T>.Commit)outcome;
                if (op.HasReceipt)
                {
                    await FinalizeReceiptAsync(tx, commit).ConfigureAwait(false);
                }

                await tx.Fault(FaultPoint.BeforeCommit).ConfigureAwait(false);
                commitSent = true;
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                await tx.Fault(FaultPoint.AfterCommitBeforeReply).ConfigureAwait(false);
                return commit.Reply;
            }
            catch (PostgresException ex) when (ex.SqlState is "40P01" or "40001" or "55P03")
            {
                throw new TransientConflictException(ex);
            }
            catch (InjectedFaultException ex) when (!commitSent && ex.Kind is "Deadlock40P01" or "Serialization40001")
            {
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

    async Task SetTimeoutsAsync(PgTx tx, PgOp op)
    {
        var remaining = op.NotAfter == DateTimeOffset.MaxValue ? DefaultBudget : op.NotAfter - time.GetUtcNow();
        var statementMs = (long)Math.Clamp(remaining.TotalMilliseconds, 100, 600_000);
        var lockMs = Math.Min(2000, statementMs);
        using var cmd = tx.Sql("""
            SELECT set_config('lock_timeout', @lock || 'ms', true),
                   set_config('statement_timeout', @stmt || 'ms', true),
                   set_config('idle_in_transaction_session_timeout', '10000ms', true)
            """).P("lock", lockMs.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .P("stmt", statementMs.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await cmd.ExecuteNonQueryAsync(tx.Ct).ConfigureAwait(false);
    }

    /// <summary>G1/G2 with the checks of 03 §10.1; returns a rejection or null.</summary>
    async Task<StoreReply<T>?> CheckGatesAsync<T>(PgTx tx) where T : class
    {
        var op = tx.Op;
        using (var g1 = tx.Sql("""
            SELECT store_id, schema_revision, protocol_epoch, migration_mode
            FROM runwick.store_metadata WHERE singleton_id = 1 FOR SHARE
            """))
        {
            var rows = await tx.QueryAsync(g1, r => (StoreId: r.GetGuid(0), Revision: r.GetInt64(1), Epoch: r.GetInt32(2), Mode: r.GetString(3))).ConfigureAwait(false);
            if (rows.Count == 0)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "schema_incompatible");
            }

            var s = rows[0];
            var expected = op.ExpectedStoreId ?? StoreId;
            if (expected is { } id && id != s.StoreId)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "store_identity_mismatch");
            }

            if (s.Mode != "normal" || s.Revision == 0)
            {
                return tx.Reject<T>(StoreStatus.AdmissionClosed, "store_maintenance");
            }

            if (s.Revision != 1 || s.Epoch != 1)
            {
                return tx.Reject<T>(StoreStatus.UnsupportedVersion, "schema_incompatible");
            }
        }

        if (op.Operation == Operations.RegisterApplication)
        {
            using var seed = tx.Sql("""
                INSERT INTO runwick.application_controls (application, created_at, updated_at)
                VALUES (@application, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp()))
                ON CONFLICT DO NOTHING
                """);
            await seed.ExecuteNonQueryAsync(tx.Ct).ConfigureAwait(false);
        }

        using var g2 = tx.Sql("""
            SELECT revision, required_protocol_epoch, required_capabilities::text, admission_mode
            FROM runwick.application_controls WHERE application = @application FOR SHARE
            """);
        var apps = await tx.QueryAsync(g2, r => (Revision: r.GetInt64(0), Epoch: r.GetInt32(1), Caps: r.GetString(2), Mode: r.GetString(3))).ConfigureAwait(false);
        if (apps.Count == 0)
        {
            return tx.Reject<T>(StoreStatus.AdmissionClosed, "application_not_registered");
        }

        var app = apps[0];
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
        using (var r1 = tx.Sql("""
            INSERT INTO runwick.storage_commands (
                application, retention_class, command_id, operation, operation_version, protocol_epoch,
                fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
            VALUES (@application, @class, @command_id, @operation, 1, 1, @fingerprint, 1, @not_after, 1,
                    @not_after + CASE @class WHEN 'ownership' THEN interval '10 minutes' ELSE interval '24 hours' END)
            ON CONFLICT DO NOTHING
            RETURNING true
            """).P("class", retentionClass).P("command_id", op.CommandId).P("operation", op.Operation)
                .P("fingerprint", op.Fingerprint, NpgsqlDbType.Bytea).P("not_after", op.NotAfter))
        {
            inserted = (await tx.QueryAsync(r1, _ => true).ConfigureAwait(false)).Count == 1;
        }

        if (inserted)
        {
            using var r3 = tx.Sql("""
                SELECT 1 FROM runwick.storage_commands
                WHERE application = @application AND command_id = @command_id AND retention_class <> @class
                LIMIT 1
                """).P("command_id", op.CommandId).P("class", retentionClass);
            if ((await tx.QueryAsync(r3, _ => 1).ConfigureAwait(false)).Count > 0)
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

        var now = await tx.ClockAsync().ConfigureAwait(false);
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

    static async Task FinalizeReceiptAsync<T>(PgTx tx, PgOutcome<T>.Commit commit) where T : class
    {
        using var r4 = tx.Sql("""
            UPDATE runwick.storage_commands
            SET status = @status, result = @result, decided_at = @decided_at
            WHERE application = @application AND retention_class = @class AND command_id = @command_id AND result IS NULL
            """).P("status", commit.Reply.Status.ToString()).Json("result", commit.Result ?? [])
                .P("decided_at", commit.Reply.DecidedAt).P("class", Operations.RetentionClass(tx.Op.Operation)).P("command_id", tx.Op.CommandId);
        if (await tx.ExecAsync(r4, mutation: false).ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException("Receipt finalization updated no row.");
        }
    }

    sealed class TransientConflictException(Exception inner) : Exception("transient conflict", inner);
}
