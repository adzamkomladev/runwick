using System.Data;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>An operation's open transaction plus the fault hooks and decision time (04 §6).</summary>
internal sealed class PgTx(NpgsqlConnection connection, NpgsqlTransaction transaction, PgOp op, IFaultInjector faults, CancellationToken ct)
{
    int _mutations;

    public NpgsqlConnection Connection => connection;

    public PgOp Op => op;

    public string Application => op.Application;

    public CancellationToken Ct => ct;

    /// <summary>Decision time (ms floor of the fresh clock read after the last blocking lock).</summary>
    public DateTimeOffset Now { get; private set; }

    public NpgsqlCommand Sql(string sql)
    {
        var cmd = new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("application", NpgsqlDbType.Text, op.Application);
        return cmd;
    }

    public ValueTask Fault(FaultPoint point, int step = 0) =>
        faults.OnAsync(new FaultContext(point, op.Operation, op.CommandId, op.Application, step), ct);

    public async Task<DateTimeOffset> ClockAsync()
    {
        using var cmd = new NpgsqlCommand("SELECT clock_timestamp()", connection, transaction);
        var value = (DateTime)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;
        return new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc));
    }

    /// <summary>Fresh decision time after all blocking locks (03 §1.3). Returns false when the command expired.</summary>
    public async Task<bool> DecideAsync()
    {
        await Fault(FaultPoint.AfterLocksBeforeFreshTime).ConfigureAwait(false);
        var now = await ClockAsync().ConfigureAwait(false);
        Now = PgTime.Floor(now);
        return now < op.NotAfter;
    }

    public async Task<int> ExecAsync(NpgsqlCommand cmd, bool mutation = true)
    {
        await BeforeMutation(mutation).ConfigureAwait(false);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        await AfterMutation(mutation).ConfigureAwait(false);
        return rows;
    }

    public async Task<List<T>> QueryAsync<T>(NpgsqlCommand cmd, Func<NpgsqlDataReader, T> map, bool mutation = false)
    {
        await BeforeMutation(mutation).ConfigureAwait(false);
        var list = new List<T>();
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                list.Add(map(reader));
            }
        }

        await AfterMutation(mutation).ConfigureAwait(false);
        return list;
    }

    /// <summary>
    /// Runs a multi-statement command (statements joined with <c>;</c>) in one round trip and returns every result
    /// set's rows. The statements run in order on the server, so each runs after the locks of the ones before it (04 §6.1).
    /// </summary>
    public async Task<List<List<object?[]>>> QueryBatchAsync(NpgsqlCommand cmd)
    {
        var sets = new List<List<object?[]>>();
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            do
            {
                var rows = new List<object?[]>();
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var row = new object?[reader.FieldCount];
                    reader.GetValues(row!);
                    rows.Add(row);
                }

                sets.Add(rows);
            }
            while (await reader.NextResultAsync(ct).ConfigureAwait(false));
        }

        return sets;
    }

    async ValueTask BeforeMutation(bool mutation)
    {
        if (mutation && _mutations == 0)
        {
            await Fault(FaultPoint.BeforeFirstMutation).ConfigureAwait(false);
        }
    }

    async ValueTask AfterMutation(bool mutation)
    {
        if (mutation)
        {
            await Fault(FaultPoint.AfterMutation, ++_mutations).ConfigureAwait(false);
        }
    }

    public StoreReply<T> Reject<T>(StoreStatus status, string code, Guid? conflictingJobId = null, Guid? blockedScope = null,
        CandidateCursor? resumeAfter = null, string? detail = null) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code, null, conflictingJobId, blockedScope, resumeAfter, detail), Now);
}

/// <summary>Static description of one store call.</summary>
internal sealed record PgOp(
    string Application,
    Guid CommandId,
    string Operation,
    DateTimeOffset NotAfter,
    byte[]? Fingerprint,
    PgLane Lane,
    Guid? ExpectedStoreId)
{
    public bool HasReceipt => Fingerprint is not null;

    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox
        || ScheduleOperations.IsCreation(Operation);

    public bool AllowedWhileDraining => Operation is Operations.Renew or Operations.Finish or Operations.Recover
        or Operations.ReleaseReservation or "PublishPresence" or "ReadinessProbe" or "ListExpiredLeases" or "ResolveCommand"
        or Operations.RegisterApplication or Operations.EventsSince || Operations.IsAdministrative(Operation)
        || Operation is ScheduleOperations.SetScheduleState or ScheduleOperations.UpdateSchedule or ScheduleOperations.ResolveCatchUp
            or ScheduleOperations.RecordScheduleEvaluationFailure or ScheduleOperations.RevalidateSchedule;
}

/// <summary>Body result: commit (with the receipt result for receipt-bearing operations) or roll back.</summary>
internal abstract record PgOutcome<T> where T : class
{
    public sealed record Commit(StoreReply<T> Reply, JsonObject? Result) : PgOutcome<T>;

    public sealed record Rollback(StoreReply<T> Reply) : PgOutcome<T>;
}

/// <summary>A committed receipt row read by R2.</summary>
internal sealed record ReceiptRow(string Operation, int OperationVersion, byte[] Fingerprint, DateTimeOffset NotAfter,
    string? Status, JsonObject? Result, DateTimeOffset? DecidedAt);

internal static class PgTime
{
    public static DateTimeOffset Floor(DateTimeOffset value) =>
        new(value.UtcTicks - value.UtcTicks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);

    public static DateTimeOffset Ceil(DateTimeOffset value) =>
        value.UtcTicks % TimeSpan.TicksPerMillisecond == 0 ? value.ToUniversalTime() : Floor(value) + TimeSpan.FromMilliseconds(1);

    public static DateTimeOffset Read(NpgsqlDataReader r, int i) =>
        new(DateTime.SpecifyKind(r.GetDateTime(i), DateTimeKind.Utc));

    public static DateTimeOffset? ReadNullable(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : Read(r, i);

    public static long Ms(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    public static DateTimeOffset FromMs(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms);
}

internal static class PgParams
{
    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, object? value, NpgsqlDbType type)
    {
        cmd.Parameters.Add(new NpgsqlParameter(name, type) { Value = value ?? DBNull.Value });
        return cmd;
    }

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, Guid value) => cmd.P(name, value, NpgsqlDbType.Uuid);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, string? value) => cmd.P(name, value, NpgsqlDbType.Text);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, int value) => cmd.P(name, value, NpgsqlDbType.Integer);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, long value) => cmd.P(name, value, NpgsqlDbType.Bigint);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, bool value) => cmd.P(name, value, NpgsqlDbType.Boolean);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, DateTimeOffset value) =>
        cmd.P(name, value.UtcDateTime, NpgsqlDbType.TimestampTz);

    public static NpgsqlCommand P(this NpgsqlCommand cmd, string name, DateTimeOffset? value) =>
        cmd.P(name, value?.UtcDateTime, NpgsqlDbType.TimestampTz);

    public static NpgsqlCommand Json(this NpgsqlCommand cmd, string name, JsonNode? value) =>
        cmd.P(name, value?.ToJsonString(), NpgsqlDbType.Jsonb);

    public static NpgsqlCommand Arr<T>(this NpgsqlCommand cmd, string name, T[] values, NpgsqlDbType elementType) =>
        cmd.P(name, values, NpgsqlDbType.Array | elementType);

    public static IsolationLevel ReadCommitted => IsolationLevel.ReadCommitted;
}
