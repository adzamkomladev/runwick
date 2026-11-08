using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>Call classes of 04 §9.1, kept on SQLite as admission limits and writer priority (20 §5.1); lower runs first.</summary>
internal enum SqLane
{
    Renewal,
    Settlement,
    Admission,
    Producer,
    Background,
}

/// <summary>Instants are UTC epoch milliseconds (20 §3); floor/ceil happen here, before binding.</summary>
internal static class SqTime
{
    public static DateTimeOffset Floor(DateTimeOffset value) =>
        new(value.UtcTicks - value.UtcTicks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);

    public static DateTimeOffset Ceil(DateTimeOffset value) =>
        value.UtcTicks % TimeSpan.TicksPerMillisecond == 0 ? value.ToUniversalTime() : Floor(value) + TimeSpan.FromMilliseconds(1);

    public static long Ms(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    public static DateTimeOffset FromMs(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms);
}

/// <summary>UUIDs as 16-byte RFC 4122 big-endian BLOBs (20 §3): memcmp order is UuidOrder.</summary>
internal static class SqUuid
{
    public static byte[] Bytes(Guid g) => g.ToByteArray(bigEndian: true);

    public static Guid Read(byte[] b) => new(b, bigEndian: true);
}

internal static class SqParams
{
    /// <summary>Binds a value in its SQLite form: Guid as big-endian BLOB, instants as ms, bool as 0/1, JSON as text.</summary>
    public static SqliteCommand P(this SqliteCommand cmd, string name, object? value)
    {
        cmd.Parameters.AddWithValue("@" + name, Convert(value));
        return cmd;
    }

    public static object Convert(object? value) => value switch
    {
        null => DBNull.Value,
        Guid g => SqUuid.Bytes(g),
        DateTimeOffset t => SqTime.Ms(t),
        bool b => b ? 1L : 0L,
        JsonNode n => n.ToJsonString(),
        TimeSpan s => (long)s.TotalMilliseconds,
        int i => (long)i,
        _ => value,
    };

    /// <summary>A text or number list bound as one JSON array for <c>json_each(@name)</c> (UUID lists use <see cref="In"/>).</summary>
    public static SqliteCommand Arr<T>(this SqliteCommand cmd, string name, IEnumerable<T> values)
    {
        var a = new JsonArray();
        foreach (var v in values)
        {
            a.Add(v switch
            {
                DateTimeOffset t => JsonValue.Create(SqTime.Ms(t)),
                _ => JsonValue.Create(v),
            });
        }

        cmd.Parameters.AddWithValue("@" + name, a.ToJsonString());
        return cmd;
    }

    /// <summary>Binds each value as its own parameter and returns the <c>(@n0, @n1, …)</c> list for an IN clause.</summary>
    public static string In<T>(this SqliteCommand cmd, string name, IReadOnlyList<T> values)
    {
        if (values.Count == 0)
        {
            return "(NULL)";
        }

        var names = new string[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            names[i] = "@" + name + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            cmd.Parameters.AddWithValue(names[i], Convert(values[i]));
        }

        return "(" + string.Join(", ", names) + ")";
    }

    public static Guid Uuid(this SqliteDataReader r, int i) => SqUuid.Read((byte[])r.GetValue(i));

    public static Guid? UuidN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.Uuid(i);

    public static DateTimeOffset Time(this SqliteDataReader r, int i) => SqTime.FromMs(r.GetInt64(i));

    public static DateTimeOffset? TimeN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.Time(i);

    public static bool Bool(this SqliteDataReader r, int i) => r.GetInt64(i) != 0;

    public static string? StrN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

    public static int? IntN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.GetInt32(i);

    public static long? LongN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : r.GetInt64(i);

    public static byte[] Blob(this SqliteDataReader r, int i) => (byte[])r.GetValue(i);

    public static JsonObject? JsonN(this SqliteDataReader r, int i) => r.IsDBNull(i) ? null : JsonNode.Parse(r.GetString(i))!.AsObject();
}

/// <summary>Static description of one store call (as PgOp).</summary>
internal sealed record SqOp(
    string Application,
    Guid CommandId,
    string Operation,
    DateTimeOffset NotAfter,
    byte[]? Fingerprint,
    SqLane Lane,
    Guid? ExpectedStoreId = null)
{
    public bool HasReceipt => Fingerprint is not null;

    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve or Operations.ReserveAndBegin
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox or Operations.ManagementDispatch
        || ScheduleOperations.IsCreation(Operation);

    public bool AllowedWhileDraining => Operation is Operations.Renew or Operations.RenewMany or Operations.Finish or Operations.FinishMany or Operations.Recover
        or Operations.ReleaseReservation or "PublishPresence" or "ReadinessProbe" or "ListExpiredLeases" or "ResolveCommand"
        or Operations.RegisterApplication or Operations.EventsSince or SqliteStore.ManagementRead or SqliteStore.RecordDisclosure
        || Operations.IsAdministrative(Operation)
        || Operation is ScheduleOperations.SetScheduleState or ScheduleOperations.UpdateSchedule or ScheduleOperations.ResolveCatchUp
            or ScheduleOperations.RecordScheduleEvaluationFailure or ScheduleOperations.RevalidateSchedule;
}

/// <summary>Body result of a C#-bodied operation: commit with the receipt result, or roll back (as PgOutcome).</summary>
internal abstract record SqOutcome<T> where T : class
{
    public sealed record Commit(StoreReply<T> Reply, JsonObject? Result) : SqOutcome<T>;

    public sealed record Rollback(StoreReply<T> Reply) : SqOutcome<T>;
}

/// <summary>A committed receipt row (R2).</summary>
internal sealed record SqReceiptRow(string Operation, int OperationVersion, byte[] Fingerprint, DateTimeOffset NotAfter,
    string? Status, JsonObject? Result, DateTimeOffset? DecidedAt);

/// <summary>
/// The reply of an operation body ported from a PostgreSQL server function (the <c>rw_reply_v1</c> row): the same fields,
/// so the PostgreSQL reply mappings carry over unchanged.
/// </summary>
internal sealed class SqRow
{
    public required string Status { get; init; }

    public required string Effect { get; init; }

    public string? Code { get; init; }

    public string? Detail { get; init; }

    public Guid? ConflictingJobId { get; init; }

    public Guid? BlockedScopeId { get; init; }

    public DateTimeOffset? ResumeEligibleAt { get; init; }

    public Guid? ResumeJobId { get; init; }

    public DateTimeOffset? DecidedAt { get; init; }

    public bool Replayed { get; set; }

    public JsonObject? Result { get; set; }

    public byte[]? Payload { get; set; }

    public string? Headers { get; set; }

    public DateTimeOffset? CreatedAt { get; set; }

    /// <summary>Payload rows after the reply (ReserveAndBegin/1).</summary>
    public List<SqRow> More { get; } = [];

    /// <summary>An Applied reply whose transaction has nothing to keep (RenewMany/FinishMany where no member applied now).</summary>
    public bool NoCommit { get; set; }

    public bool Commits => Effect == nameof(StoreEffect.Applied) && !Replayed && !NoCommit;

    public bool HasValue => Effect == nameof(StoreEffect.Applied);

    public DateTimeOffset Decided => DecidedAt ?? default;

    public StoreStatus StoreStatus => Enum.Parse<StoreStatus>(Status);

    public static SqRow NoEffect(string status, string code, DateTimeOffset? decidedAt, string? detail = null, Guid? conflictingJobId = null,
        Guid? blockedScopeId = null, DateTimeOffset? resumeEligibleAt = null, Guid? resumeJobId = null) => new()
    {
        Status = status, Effect = nameof(StoreEffect.ObservedNoEffect), Code = code, Detail = detail, ConflictingJobId = conflictingJobId,
        BlockedScopeId = blockedScopeId, ResumeEligibleAt = resumeEligibleAt, ResumeJobId = resumeJobId, DecidedAt = decidedAt,
    };

    public static SqRow Applied(string status, string? code, DateTimeOffset decidedAt, JsonObject? result, string? detail = null) => new()
    {
        Status = status, Effect = nameof(StoreEffect.Applied), Code = code, Detail = detail, DecidedAt = decidedAt,
        // Through JSON text, so values read back exactly as from the stored receipt (a Guid node reads as a string).
        Result = result is null ? null : JsonNode.Parse(result.ToJsonString())!.AsObject(),
    };

    public StoreReply<T> Reject<T>() where T : class =>
        StoreReply<T>.NoEffect(StoreStatus, new StoreError(Code ?? "invariant_fault", null, ConflictingJobId, BlockedScopeId,
            ResumeJobId is { } j ? new CandidateCursor(ResumeEligibleAt!.Value, j) : null, Detail), Decided);

    public StoreReply<T> Applied<T>(T? value) where T : class =>
        new(StoreStatus, StoreEffect.Applied, Replayed, value, Code is null ? null : new StoreError(Code, Detail: Detail), Decided);
}

/// <summary>A row-count check failed inside an operation: rolled back, InvariantFault with the code (RWI01/RWI02 on PostgreSQL).</summary>
internal sealed class SqInvariantException(string code) : Exception(code)
{
    public string Code { get; } = code;
}

/// <summary>The caller's transaction cannot host staging (01 §15); mapped to TransactionValidationException.</summary>
internal sealed class SqStagingValidationException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}
