using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// One server-function call (04 §7.17, PF-05): the function name and its operation-specific arguments, sent in named
/// notation (<c>p_x => @p_x</c>) so a later defaulted parameter cannot shift positions. The frame arguments are added by
/// <see cref="PgRunner"/>.
/// </summary>
internal sealed class PgCall(string function)
{
    readonly List<NpgsqlParameter> _parameters = [];

    public string Function => function;

    public IReadOnlyList<NpgsqlParameter> Parameters => _parameters;

    public PgCall P(string name, object? value, NpgsqlDbType type)
    {
        _parameters.Add(new NpgsqlParameter(name, type) { Value = value ?? DBNull.Value });
        return this;
    }

    public PgCall P(string name, Guid value) => P(name, value, NpgsqlDbType.Uuid);

    public PgCall P(string name, Guid? value) => P(name, value, NpgsqlDbType.Uuid);

    public PgCall P(string name, string? value) => P(name, value, NpgsqlDbType.Text);

    public PgCall P(string name, int value) => P(name, value, NpgsqlDbType.Integer);

    public PgCall P(string name, int? value) => P(name, value, NpgsqlDbType.Integer);

    public PgCall P(string name, long value) => P(name, value, NpgsqlDbType.Bigint);

    public PgCall P(string name, long? value) => P(name, value, NpgsqlDbType.Bigint);

    public PgCall P(string name, bool value) => P(name, value, NpgsqlDbType.Boolean);

    public PgCall P(string name, TimeSpan value) => P(name, value, NpgsqlDbType.Interval);

    public PgCall P(string name, DateTimeOffset value) => P(name, value.UtcDateTime, NpgsqlDbType.TimestampTz);

    public PgCall P(string name, DateTimeOffset? value) => P(name, value?.UtcDateTime, NpgsqlDbType.TimestampTz);

    public PgCall Bytes(string name, byte[]? value) => P(name, value, NpgsqlDbType.Bytea);

    public PgCall Json(string name, JsonNode? value) => P(name, value?.ToJsonString(), NpgsqlDbType.Jsonb);

    public PgCall Arr<T>(string name, T[] values, NpgsqlDbType elementType) => P(name, values, NpgsqlDbType.Array | elementType);

    /// <summary>A 64-bit random draw, sent as its two's complement (the function reads it as unsigned).</summary>
    public PgCall Entropy(string name) =>
        P(name, BitConverter.ToInt64(System.Security.Cryptography.RandomNumberGenerator.GetBytes(8)), NpgsqlDbType.Bigint);
}

/// <summary>The reply row of a server function (<c>runwick.rw_reply_v1</c>).</summary>
internal sealed class PgRow
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

    public bool Replayed { get; init; }

    public JsonObject? Result { get; init; }

    public byte[]? Payload { get; init; }

    public string? Headers { get; init; }

    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>Rows after the reply row: the payload rows of a set-returning function (ReserveAndBegin/1).</summary>
    public List<PgRow> More { get; } = [];

    /// <summary>The call applied an effect now (not a replay): the client commits.</summary>
    public bool Commits => Effect == nameof(StoreEffect.Applied) && !Replayed;

    /// <summary>The reply carries a value (applied now or replayed).</summary>
    public bool HasValue => Effect == nameof(StoreEffect.Applied);

    public DateTimeOffset Decided => DecidedAt ?? default;

    public StoreStatus StoreStatus => Enum.Parse<StoreStatus>(Status);

    internal static PgRow Read(NpgsqlDataReader r) => new()
    {
        Status = r.GetString(0),
        Effect = r.GetString(1),
        Code = r.IsDBNull(2) ? null : r.GetString(2),
        Detail = r.IsDBNull(3) ? null : r.GetString(3),
        ConflictingJobId = r.IsDBNull(4) ? null : r.GetGuid(4),
        BlockedScopeId = r.IsDBNull(5) ? null : r.GetGuid(5),
        ResumeEligibleAt = PgTime.ReadNullable(r, 6),
        ResumeJobId = r.IsDBNull(7) ? null : r.GetGuid(7),
        DecidedAt = PgTime.ReadNullable(r, 8),
        Replayed = !r.IsDBNull(9) && r.GetBoolean(9),
        Result = r.IsDBNull(10) ? null : JsonNode.Parse(r.GetString(10))!.AsObject(),
        Payload = r.IsDBNull(11) ? null : (byte[])r[11],
        Headers = r.IsDBNull(12) ? null : r.GetString(12),
        CreatedAt = PgTime.ReadNullable(r, 13),
    };

    /// <summary>The no-effect reply with this row's status, code and details.</summary>
    public StoreReply<T> Reject<T>() where T : class =>
        StoreReply<T>.NoEffect(StoreStatus, new StoreError(Code ?? "invariant_fault", null, ConflictingJobId, BlockedScopeId,
            ResumeJobId is { } j ? new CandidateCursor(ResumeEligibleAt!.Value, j) : null, Detail), Decided);

    /// <summary>An applied reply with this row's status, replay flag and decision time.</summary>
    public StoreReply<T> Applied<T>(T? value) where T : class =>
        new(StoreStatus, StoreEffect.Applied, Replayed, value, Code is null ? null : new StoreError(Code, Detail: Detail), Decided);
}
