using System.Globalization;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>An immutable payload row returned with a reservation (the job's payload, headers and creation time).</summary>
internal sealed record RxPayload(byte[] Payload, string Headers, DateTimeOffset CreatedAt);

/// <summary>A decided script reply (22 §4.1): the fields of the PostgreSQL reply row, plus payload rows.</summary>
internal sealed class RxReply
{
    public required string Status { get; init; }

    public required bool HasValue { get; init; }

    public bool Replayed { get; init; }

    public string? Code { get; init; }

    public string? Detail { get; init; }

    public DateTimeOffset Decided { get; init; }

    public JsonObject? Result { get; init; }

    public Guid? Conflict { get; init; }

    public Guid? Blocked { get; init; }

    public DateTimeOffset? ResumeAt { get; init; }

    public Guid? ResumeJob { get; init; }

    public Dictionary<Guid, RxPayload> Payloads { get; } = [];

    public StoreStatus StoreStatus => Enum.Parse<StoreStatus>(Status);

    static string? S(RedisResult r) => r.IsNull ? null : (string?)r is { Length: > 0 } s ? s : null;

    static Guid? G(RedisResult r) => S(r) is { } s ? Guid.Parse(s) : null;

    public static DateTimeOffset T(string ms) => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(ms, CultureInfo.InvariantCulture));

    /// <summary>Parses <c>R status effect replayed code detail decided result conflict blocked resumeAt resumeJob [n (id payload headers created)*]</c>.</summary>
    public static RxReply Parse(RedisResult[] a, int at = 1)
    {
        var reply = new RxReply
        {
            Status = (string)a[at]!,
            HasValue = (string)a[at + 1]! == "A",
            Replayed = (string)a[at + 2]! == "1",
            Code = S(a[at + 3]),
            Detail = S(a[at + 4]),
            Decided = S(a[at + 5]) is { } d ? T(d) : default,
            Result = S(a[at + 6]) is { } json ? JsonNode.Parse(json)!.AsObject() : null,
            Conflict = G(a[at + 7]),
            Blocked = G(a[at + 8]),
            ResumeAt = S(a[at + 9]) is { } r ? T(r) : null,
            ResumeJob = G(a[at + 10]),
        };
        var i = at + 11;
        if (a.Length > i)
        {
            var n = (int)a[i++];
            for (var k = 0; k < n; k++, i += 4)
            {
                reply.Payloads[Guid.Parse((string)a[i]!)] = new RxPayload((byte[])a[i + 1]!, (string)a[i + 2]!, T((string)a[i + 3]!));
            }
        }

        return reply;
    }

    public StoreReply<T> Reject<T>() where T : class =>
        StoreReply<T>.NoEffect(StoreStatus, new StoreError(Code ?? "invariant_fault", null, Conflict, Blocked,
            ResumeJob is { } j ? new CandidateCursor(ResumeAt!.Value, j) : null,
            Detail is { Length: > 0 } d && d[0] == (char)91 ? RedisStore.DashIds(d).ToJsonString() : Detail), Decided);

    public StoreReply<T> Applied<T>(T? value) where T : class =>
        new(StoreStatus, StoreEffect.Applied, Replayed, value, Code is null ? null : new StoreError(Code, Detail: Detail), Decided);

    public override string ToString() => $"{Status}/{(HasValue ? "Applied" : "NoEffect")} {Code}";
}

/// <summary>
/// Runs one write (22 §4.1–§4.4): the script call, then pending-intent recovery and the same-command resend while the
/// operation budget lasts. A pending intent that cannot be finished surfaces as <c>store_recovering</c>.
/// </summary>
internal sealed class RxRunner(RxClient client, TimeProvider time)
{
    const int RecoveryBatch = 100;
    static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(5);
    int _recovering;

    public RxClient Client => client;

    public Guid? StoreId { get; set; }

    /// <summary>Set by the profile check (22 §2.3); admission calls are not sent while it is non-null.</summary>
    public string? ProfileFailure { get; set; }

    /// <summary>Set by the history check (22 §2.4); every call is refused until the process restarts.</summary>
    public string? HistoryFailure { get; set; }

    /// <summary>Script calls that answered Recovering (tests).</summary>
    public long RecoveringReplies;

    public async Task<RxReply> WriteAsync(RxOp op, RxScript script, RedisValue[] args, CancellationToken ct)
    {
        if (HistoryFailure is { } h)
        {
            throw new StoreIncompatibleException(h);
        }

        if (ProfileFailure is not null && op.Lane is RxLane.Admission or RxLane.Producer && op.Gate == 'c')
        {
            throw new StoreNotAppliedException("store_profile_mismatch");
        }

        var storeId = StoreId ?? throw new InvalidOperationException("Describe has not run yet.");
        var meta = RxClient.Meta(op.Application);
        var deadline = op.NotAfter == DateTimeOffset.MaxValue ? time.GetUtcNow() + DefaultBudget : op.NotAfter;
        for (var attempt = 0; ; attempt++)
        {
            var a = await client.EvalAsync(op.Lane, script, meta, client.Frame(op, storeId, args), op, ct).ConfigureAwait(false);
            switch ((string)a[0]!)
            {
                case "R":
                    return RxReply.Parse(a);
                case "E":
                    // A Lua error before the intent: nothing was written.
                    return new RxReply { Status = nameof(StoreStatus.InvariantFault), HasValue = false, Code = "script_error", Detail = (string?)a[1] };
                case "RECOVERING":
                    Interlocked.Increment(ref RecoveringReplies);
                    if (attempt >= 8 || time.GetUtcNow() >= deadline || !await RecoverAsync(op.Application, "", op.Lane, ct).ConfigureAwait(false))
                    {
                        StartBackgroundRecovery(op.Application);
                        throw new StoreOutcomeUnknownException("store_recovering", op.CommandId, op.Operation);
                    }

                    continue;
                case "P":
                    // Our own intent is larger than one call: finish it in batches, then resend to read the receipt.
                    var published = await RecoverOwnAsync(op, ct).ConfigureAwait(false);
                    if (published is not null && !op.HasReceipt)
                    {
                        return published;
                    }

                    continue;
                default:
                    throw new StoreOutcomeUnknownException("commit_outcome_unknown", op.CommandId, op.Operation);
            }
        }
    }

    async Task<RxReply?> RecoverOwnAsync(RxOp op, CancellationToken ct)
    {
        var meta = RxClient.Meta(op.Application);
        for (var i = 0; i < 64; i++)
        {
            var a = await RecoveryCallAsync(meta, RxClient.Hex(op.CommandId), op.Lane, ct).ConfigureAwait(false);
            switch ((string)a[0]!)
            {
                case "MORE":
                    continue;
                case "PUBLISHED":
                    return RxReply.Parse(a, 3);
                case "STUCK":
                    StartBackgroundRecovery(op.Application);
                    throw new StoreOutcomeUnknownException("store_recovering", op.CommandId, op.Operation);
                default:
                    return null;
            }
        }

        throw new StoreOutcomeUnknownException("store_recovering", op.CommandId, op.Operation);
    }

    async Task<RedisResult[]> RecoveryCallAsync(RedisKey meta, string expected, RxLane lane, CancellationToken ct) =>
        await client.EvalAsync(lane, RxScript.Write("recovery"), meta, [client.FailAfter, expected, RecoveryBatch], null, ct).ConfigureAwait(false);

    /// <summary>Finishes whatever intent is pending (22 §4.2); false when it cannot be finished now.</summary>
    public async Task<bool> RecoverAsync(string application, string expected, RxLane lane, CancellationToken ct)
    {
        var meta = RxClient.Meta(application);
        for (var i = 0; i < 64; i++)
        {
            RedisResult[] a;
            try
            {
                a = await RecoveryCallAsync(meta, expected, lane, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException)
            {
                return false;
            }

            switch ((string)a[0]!)
            {
                case "MORE":
                    continue;
                case "STUCK":
                    return false;
                default:
                    return true;
            }
        }

        return false;
    }

    /// <summary>The background class retries recovery every second (06 F-7 backoff after failures) while anything saw Recovering.</summary>
    void StartBackgroundRecovery(string application)
    {
        if (Interlocked.Exchange(ref _recovering, 1) == 1)
        {
            return;
        }

        _ = Task.Run(async () =>
        {
            var delay = TimeSpan.FromSeconds(1);
            try
            {
                while (!await RecoverAsync(application, "", RxLane.Background, CancellationToken.None).ConfigureAwait(false))
                {
                    await Task.Delay(delay, time).ConfigureAwait(false);
                    delay = TimeSpan.FromMilliseconds(Math.Min(30_000, delay.TotalMilliseconds * 2));
                }
            }
            finally
            {
                Interlocked.Exchange(ref _recovering, 0);
            }
        });
    }

    /// <summary>A read script (22 §4.6): Recovering runs recovery and retries; otherwise the raw reply.</summary>
    public async Task<RedisResult[]> ReadAsync(string application, RxScript script, RxLane lane, RedisValue[] args, CancellationToken ct)
    {
        if (HistoryFailure is { } h)
        {
            throw new StoreIncompatibleException(h);
        }

        var meta = RxClient.Meta(application);
        for (var attempt = 0; ; attempt++)
        {
            var a = await client.EvalAsync(lane, script, meta, RxClient.ReadFrame(StoreId, args), null, ct).ConfigureAwait(false);
            var kind = (string)a[0]!;
            if (kind == "RECOVERING")
            {
                Interlocked.Increment(ref RecoveringReplies);
                if (attempt < 4 && await RecoverAsync(application, "", lane, ct).ConfigureAwait(false))
                {
                    continue;
                }

                StartBackgroundRecovery(application);
                throw new StoreNotAppliedException("store_recovering");
            }

            if (kind == "NONE" && (string)a[1]! is "store_identity_mismatch" or "schema_incompatible")
            {
                throw new StoreIncompatibleException((string)a[1]!);
            }

            return a;
        }
    }
}
