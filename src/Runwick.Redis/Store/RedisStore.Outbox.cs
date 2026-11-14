using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Redis.Store;

/// <summary>
/// ReceiveOutbox/1 and FindDelivery (14 §5, §6.3): receive_outbox_v3's steps in one script (<c>receive_outbox.lua</c>). Scripts
/// run one at a time, so every transport command for one delivery identity is serialized without a lock.
/// </summary>
internal sealed partial class RedisStore : IOutboxDestinationStore
{
    /// <summary>The delivered mark's confirmed profile on Redis (22 §7, 14 Implementation choice 4).</summary>
    string OutboxConfirmedProfile => options.Persistence == RedisPersistence.AofEverySecond ? "redis.aof-everysec.v1" : "redis.aof-always.v1";

    static string OutboxField(OutboxSourceIdentity s) => Hex(s.SourceStoreId) + ":" + Hex(s.MessageId);

    public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if ((OutboxValidate(command) ?? (InvalidDispatch([r.Item]) is not null ? "item" : null)) is { } invalid)
        {
            return StoreReply<OutboxDelivery>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = OutboxFingerprints.Command(storeId, command.Application, command.CommandId, command.NotAfter, r.DeliveryFingerprint);
        var acceptUntil = r.AcceptUntil.ToUnixTimeMilliseconds();
        var reply = await Write(Op(command.Application, command.CommandId, OutboxOperations.ReceiveOutbox, command.NotAfter, fp, RxLane.Producer,
                GateOf(OutboxOperations.ReceiveOutbox), r.StoreId),
            "receive_outbox", ct, OutboxField(r.Source), Convert.ToHexStringLower(r.DeliveryFingerprint), acceptUntil,
            acceptUntil + (long)OutboxOperations.MappingRetention.TotalMilliseconds, OutboxConfirmedProfile, Json(ItemJson(r.Item)),
            r.Item.Intent.PayloadUtf8).ConfigureAwait(false);
        return reply.HasValue ? reply.Applied(OutboxRead(reply.Result!)) : reply.Reject<OutboxDelivery>();
    }

    public async Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(deliveryFingerprint);
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("find_delivery"), RxLane.Background, [OutboxField(source)], ct).ConfigureAwait(false);
        switch ((string)a[0]!)
        {
            case "OK":
                break;
            case "NONE":
                return new OutboxLookup(null, false);
            default:
                throw new StoreNotAppliedException("store_maintenance");
        }

        Seen(application, (string)a[1]!);
        if ((string?)a[2] is not { Length: > 0 } json)
        {
            return new OutboxLookup(null, false);
        }

        var row = JsonNode.Parse(json)!.AsObject();
        var match = row["fp"]!.GetValue<string>() == Convert.ToHexStringLower(deliveryFingerprint);
        return new OutboxLookup(match ? OutboxRead(row, firstDelivery: false) : null, Conflict: !match);
    }

    /// <summary>14 §5.1 shape checks that need no store state; returns the offending field or null.</summary>
    static string? OutboxValidate(StoreCommand<ReceiveOutboxRequest> command)
    {
        var r = command.Body;
        if (r.DeliveryFingerprint is not { Length: 32 })
        {
            return "deliveryFingerprint";
        }

        if (r.Source.SourceStoreId == Guid.Empty || r.Source.MessageId == Guid.Empty)
        {
            return "source";
        }

        if (command.NotAfter > r.AcceptUntil)
        {
            return "notAfter";
        }

        var expected = OutboxFingerprints.Delivery(r.StoreId, command.Application, r.Source, r.AcceptUntil.ToUnixTimeMilliseconds(), r.Item.Intent, r.Item.Digests);
        return expected.AsSpan().SequenceEqual(r.DeliveryFingerprint) ? null : "deliveryFingerprint";
    }

    /// <summary>A delivery from a receipt result, or from a stored mapping row (which keeps the profile as <c>profile</c>).</summary>
    static OutboxDelivery OutboxRead(JsonObject o, bool? firstDelivery = null) => new(
        Guid.Parse(o["jobId"]!.GetValue<string>()),
        Enum.Parse<DispatchDisposition>(o["disposition"]!.GetValue<string>()),
        Enum.Parse<DuplicateMatch>(o["match"]!.GetValue<string>()),
        o["policyDrift"]!.GetValue<bool>(),
        firstDelivery ?? o["firstDelivery"]!.GetValue<bool>(),
        DateTimeOffset.FromUnixTimeMilliseconds(o["deliveredAt"]!.GetValue<long>()),
        DateTimeOffset.FromUnixTimeMilliseconds(o["acceptUntil"]!.GetValue<long>()),
        (o["confirmedProfile"] ?? o["profile"])!.GetValue<string>());
}
