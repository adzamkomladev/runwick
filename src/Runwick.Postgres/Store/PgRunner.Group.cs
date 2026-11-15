using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// What one group transaction left behind. <see cref="Rows"/> holds one reply per command, in order, when the function
/// answered. <see cref="CommitUnknown"/> means COMMIT was sent and the outcome is not known. Neither means nothing
/// committed and every command should be retried alone with its own identity.
/// </summary>
internal sealed record PgGroupCall(PgRow[]? Rows, bool CommitUnknown, Exception? Failure)
{
    public static readonly PgGroupCall Fallback = new(null, false, null);
}

internal sealed partial class PgRunner
{
    /// <summary>
    /// One transaction for 1 to 8 plain Dispatch/1 commands (0023_dispatch_group): BEGIN, the session timeouts and
    /// <c>runwick.dispatch_group_v1</c> in one round trip, then COMMIT when any command applied now and ROLLBACK
    /// otherwise. It never throws for a store failure: before COMMIT is sent the commands retry alone (their receipts make
    /// that safe), after it the outcome of each is unknown. Takes one producer lane slot, as a single call does.
    /// </summary>
    public Task<PgGroupCall> CallGroupAsync(IReadOnlyList<PgOp> ops, PgCall call, CancellationToken ct) =>
        Telemetry.Telemetry.MeasureStorage(Operations.Dispatch, (int)PgLane.Producer, () => CallGroupCoreAsync(ops, call, ct),
            r => r.Rows is not null ? "applied" : r.CommitUnknown ? "outcome_unknown" : "not_applied");

    async Task<PgGroupCall> CallGroupCoreAsync(IReadOnlyList<PgOp> ops, PgCall call, CancellationToken ct)
    {
        var gate = sources.Gate(PgLane.Producer);
        try
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return PgGroupCall.Fallback;
        }

        try
        {
            NpgsqlConnection connection;
            try
            {
                connection = await sources.For(PgLane.Producer).OpenConnectionAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or IOException)
            {
                return PgGroupCall.Fallback;
            }

            var commitSent = false;
            await using (connection.ConfigureAwait(false))
            {
                NpgsqlTransaction? transaction = null;
                try
                {
                    transaction = await connection.BeginTransactionAsync(PgParams.ReadCommitted, ct).ConfigureAwait(false);
                    var tx = new PgTx(connection, transaction, ops[0], faults, ct);
                    await tx.Fault(FaultPoint.BeforeGateLock).ConfigureAwait(false);
                    var rows = await InvokeGroupAsync(connection, transaction, ops, call, ct).ConfigureAwait(false);
                    if (!rows.Any(r => r.Commits))
                    {
                        await transaction.RollbackAsync(ct).ConfigureAwait(false);
                        return new PgGroupCall(rows, false, null);
                    }

                    await tx.Fault(FaultPoint.BeforeCommit).ConfigureAwait(false);
                    commitSent = true;
                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    await tx.Fault(FaultPoint.AfterCommitBeforeReply).ConfigureAwait(false);
                    return new PgGroupCall(rows, false, null);
                }
                catch (PostgresException ex) when (ex.SqlState == "40003")
                {
                    return new PgGroupCall(null, true, ex);
                }
                catch (PostgresException ex) when (!commitSent && IsStalePlan(ex))
                {
                    Interlocked.Increment(ref _stalePlans);
                    await ForgetPreparedAsync(connection, transaction).ConfigureAwait(false);
                    sources.For(PgLane.Producer).Clear();
                    return PgGroupCall.Fallback;
                }
                catch (PostgresException ex)
                {
                    // A server error reply, at COMMIT too, means the transaction rolled back. The late RWB01, an id
                    // collision, a lock timeout or a deadlock all end here, and each command retries alone.
                    if (ex.SqlState is "40P01" or "40001" or "55P03")
                    {
                        Interlocked.Increment(ref _transientConflicts);
                        if (ex.SqlState == "40P01")
                        {
                            Interlocked.Increment(ref _deadlocks);
                        }
                    }

                    return PgGroupCall.Fallback;
                }
                catch (Exception ex) when (commitSent)
                {
                    return new PgGroupCall(null, true, ex);
                }
                catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException or OperationCanceledException
                    or InjectedFaultException or InvalidOperationException)
                {
                    return new PgGroupCall(null, false, ex);
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
        finally
        {
            gate.Release();
        }
    }

    async Task<PgRow[]> InvokeGroupAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, IReadOnlyList<PgOp> ops, PgCall call,
        CancellationToken ct)
    {
        // The statement budget covers the latest deadline: an early deadline is judged per command inside the function and
        // must not reject the others.
        var remaining = ops.Max(o => o.NotAfter == DateTimeOffset.MaxValue ? DefaultBudget : o.NotAfter - time.GetUtcNow());
        var statementMs = (long)Math.Clamp(remaining.TotalMilliseconds, 100, 600_000);
        using var batch = new NpgsqlBatch(connection, transaction);
        var set = new NpgsqlBatchCommand(TimeoutsSql);
        set.Parameters.AddWithValue("lock", NpgsqlDbType.Text, Math.Min(2000, statementMs).ToString(System.Globalization.CultureInfo.InvariantCulture));
        set.Parameters.AddWithValue("stmt", NpgsqlDbType.Text, statementMs.ToString(System.Globalization.CultureInfo.InvariantCulture));
        batch.BatchCommands.Add(set);

        var cmd = new NpgsqlBatchCommand();
        var p = cmd.Parameters;
        p.AddWithValue("p_application", NpgsqlDbType.Text, ops[0].Application);
        p.AddWithValue("p_store_id", NpgsqlDbType.Uuid, (object?)(ops[0].ExpectedStoreId ?? StoreId) ?? DBNull.Value);
        p.AddWithValue("p_revisions", NpgsqlDbType.Array | NpgsqlDbType.Bigint, AcceptedRevisions);
        p.AddWithValue("p_epoch", NpgsqlDbType.Integer, 1);
        p.AddWithValue("p_capabilities", NpgsqlDbType.Jsonb, CapabilitiesJson());
        call.Bind(p);
        cmd.CommandText = "SELECT * FROM runwick." + call.Function + "("
            + string.Join(", ", p.Select(x => x.ParameterName + " => @" + x.ParameterName)) + ")";
        batch.BatchCommands.Add(cmd);

        var rows = new List<PgRow>(ops.Count);
        var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            await reader.NextResultAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(PgRow.Read(reader));
            }
        }

        return rows.Count == ops.Count
            ? [.. rows]
            : throw new InvalidOperationException($"runwick.{call.Function} returned {rows.Count} rows for {ops.Count} commands.");
    }
}
