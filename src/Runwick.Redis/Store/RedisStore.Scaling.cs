using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Redis.Store;

/// <summary>
/// Scaler support (11 §4–§6) as PgStore.Scaling: EstimateDemand on the read path (one revision, 22 §4.6), SetPoolOverride/1 with
/// its receipt and RecordScalingEvent without one.
/// </summary>
internal sealed partial class RedisStore : IScalingStore
{
    /// <summary>Due rows counted per lane before the count is reported as a lower bound (06 §1.13).</summary>
    const int ScalingDemandCap = 2_000;

    /// <summary>Longest override lifetime (06 §1.13).</summary>
    static readonly TimeSpan ScalingMaxOverride = TimeSpan.FromDays(7);

    public async Task<DemandEstimate> EstimateDemandAsync(string application, Guid instanceId, IReadOnlyList<string> queues, IReadOnlyList<JobContract> contracts,
        CancellationToken ct)
    {
        var tuples = contracts.Distinct().ToArray();
        if (queues.Count is < 1 or > 128 || tuples.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(queues), "Estimate takes 1–128 queues and 1–512 contracts.");
        }

        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("estimate_demand"), RxLane.Background,
            [Hex(instanceId), Json(new JsonArray([.. queues.Select(q => (JsonNode?)q)])), Json(Contracts(tuples)), ScalingDemandCap], ct).ConfigureAwait(false);
        switch ((string)a[0]!)
        {
            case "OK":
                break;
            case "NONE":
                return new DemandEstimate(time.GetUtcNow(), [], false, 0, 0, new Dictionary<string, int>(StringComparer.Ordinal));
            default:
                throw new StoreUnavailableException("The store is not accepting work (maintenance); demand is unknown.");
        }

        Seen(application, (string)a[1]!);
        var rows = JsonNode.Parse((string)a[4]!)!.AsArray().Select(n => new DemandRow(n!["queue"]!.GetValue<string>(),
            new JobContract(n["name"]!.GetValue<string>(), n["pver"]!.GetValue<int>(), n["profile"]!.GetValue<string>(), n["profver"]!.GetValue<int>()),
            n["due"]!.GetValue<long>(), n["capable"]!.GetValue<int>())).ToList();
        var live = JsonNode.Parse((string)a[5]!)!.AsArray().Select(n => Guid.Parse(n!.GetValue<string>())).ToList();
        var overrides = JsonNode.Parse((string)a[6]!)!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>(), StringComparer.Ordinal);
        return new DemandEstimate(At(a[2]), rows, (string)a[3]! == "1", live.Count, ScalingDigest(live), overrides);
    }

    /// <summary>A digest of the live membership: 0 when there is none; compared only with earlier digests of this store.</summary>
    static long ScalingDigest(List<Guid> live)
    {
        if (live.Count == 0)
        {
            return 0;
        }

        var hash = System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(string.Join(',', live.Order().Select(g => g.ToString("D")))));
        return BitConverter.ToInt64(hash, 0) is var d && d != 0 ? d : 1;
    }

    public async Task<bool> RecordScalingEventAsync(string application, ScalingEventRecord record, CancellationToken ct)
    {
        if (record.OldTarget == record.NewTarget || record.OldTarget < 0 || record.NewTarget < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(record), "A scaling event records an actual target change.");
        }

        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var id = Guid.CreateVersion7();
        var row = new JsonObject
        {
            ["eventId"] = Hex(id), ["instanceId"] = Hex(record.InstanceId), ["pool"] = record.Pool, ["oldTarget"] = record.OldTarget,
            ["newTarget"] = record.NewTarget, ["desired"] = record.Desired, ["candidate"] = record.Candidate, ["reason"] = record.Reason,
            ["snapshotAt"] = record.SnapshotAt?.ToUnixTimeMilliseconds(),
            // A string: the digest is a full int64, beyond what a Lua number holds exactly.
            ["membershipDigest"] = record.MembershipDigest?.ToString(CultureInfo.InvariantCulture),
        };
        var r = await Write(Op(application, Guid.CreateVersion7(), "RecordScalingEvent", time.GetUtcNow() + TimeSpan.FromSeconds(10), null, RxLane.Background, 'd'),
            "scaling_event", ct, Hex(id), Json(row)).ConfigureAwait(false);
        return r.HasValue;
    }

    public async Task<StoreReply<PoolOverride>> SetPoolOverrideAsync(StoreCommand<PoolOverrideRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var now = time.GetUtcNow();
        var invalid = RateValidateActor(r.Actor)
            ?? (string.IsNullOrEmpty(r.Pool) || Encoding.UTF8.GetByteCount(r.Pool) > 64 ? "pool" : null)
            ?? (r.Target is < 0 or > 10_000 ? "target" : null)
            ?? (r.Target is null && r.ExpiresAt is not null ? "expiresAt" : null)
            ?? (r.ExpiresAt is { } e && (e <= now || e > now + ScalingMaxOverride) ? "expiresAt" : null)
            ?? (r.ExpectedRevision < 0 ? "expectedRevision" : null);
        if (invalid is not null)
        {
            return StoreReply<PoolOverride>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), now);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.SetPoolOverride, command.CommandId, command.NotAfter,
            CommandEnvelopes.PoolOverride(r));
        var reply = await Write(Op(command.Application, command.CommandId, Operations.SetPoolOverride, command.NotAfter, fp, RxLane.Producer, 'd'),
            "set_pool_override", ct, Hex(r.InstanceId), r.InstanceId.ToString("D"), r.Pool,
            r.Target?.ToString(CultureInfo.InvariantCulture) ?? "", r.ExpiresAt is { } x ? Ms(x) : "", r.ExpectedRevision,
            Json(RateActorJson(r.Actor))).ConfigureAwait(false);
        return reply.HasValue ? reply.Applied(ReceiptCodec.ReadOverride(reply.Result!)) : RateReject<PoolOverride>(reply);
    }
}
