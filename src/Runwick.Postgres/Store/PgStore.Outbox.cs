using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// ReceiveOutbox/1 (14 §5): delivery identity → ordinary dispatch → mapping, committed with the receipt. Lock order:
/// gates → receipt → delivery identity (transaction advisory lock) → dispatch keys → new jobs/scopes (04 §5, 14 §5.3).
/// </summary>
internal sealed partial class PgStore : IOutboxDestinationStore
{
    public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateOutbox(command) is { } invalid)
        {
            return StoreReply<OutboxDelivery>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), PgTime.Floor(time.GetUtcNow()));
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = OutboxFingerprints.Command(storeId, command.Application, command.CommandId, command.NotAfter, r.DeliveryFingerprint);
        var acceptUntil = PgTime.Floor(r.AcceptUntil);
        // The delivery lock and read, the fresh clock, the mapping answer or the horizon check, the dispatch core, the
        // mapping insert and R4 run in runwick.receive_outbox_v3 (14 §5).
        var call = new PgCall("receive_outbox_v3").P("p_source_store_id", r.Source.SourceStoreId).P("p_message_id", r.Source.MessageId)
            .Bytes("p_delivery_fingerprint", r.DeliveryFingerprint).P("p_accept_until", acceptUntil)
            .P("p_profile", OutboxOperations.PostgresProfile);
        foreach (var (name, value, type) in DispatchArgs([r.Item]))
        {
            call.P(name, value, type);
        }

        return await runner.CallAsync(
            Op(command.Application, command.CommandId, OutboxOperations.ReceiveOutbox, command.NotAfter, fp, PgLane.Producer, r.StoreId),
            call,
            row => row.HasValue ? row.Applied(ReadDeliveryJson(row.Result!)) : row.Reject<OutboxDelivery>(),
            ct).ConfigureAwait(false);
    }

    public async Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(deliveryFingerprint);

        // Read-only, no receipt. Runs under the "ResolveCommand" gate rules: it is a resolution, allowed while draining.
        var op = new PgOp(application, Guid.CreateVersion7(), "ResolveCommand", DateTimeOffset.MaxValue, null, PgLane.Background, null);
        var reply = await runner.ExecuteAsync<OutboxLookup>(op,
            async tx =>
            {
                var found = await ReadDeliveryAsync(tx, source, forUpdate: false).ConfigureAwait(false);
                var match = found is { } f && f.Fingerprint.AsSpan().SequenceEqual(deliveryFingerprint);
                var lookup = new OutboxLookup(match ? found!.Delivery : null, Conflict: found is not null && !match);
                return new PgOutcome<OutboxLookup>.Rollback(StoreReply<OutboxLookup>.Applied(lookup, PgTime.Floor(time.GetUtcNow())));
            },
            replay: null, ct).ConfigureAwait(false);
        return reply.Value ?? throw new StoreNotAppliedException(reply.Error?.Code ?? "store_unavailable");
    }

    sealed record DeliveryRow(OutboxDelivery Delivery, byte[] Fingerprint);

    /// <summary>14 §5.1 shape checks that need no store state; returns the offending field or null.</summary>
    static string? ValidateOutbox(StoreCommand<ReceiveOutboxRequest> command)
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

        var expected = OutboxFingerprints.Delivery(r.StoreId, command.Application, r.Source, PgTime.Ms(PgTime.Floor(r.AcceptUntil)), r.Item.Intent, r.Item.Digests);
        return expected.AsSpan().SequenceEqual(r.DeliveryFingerprint) ? null : "deliveryFingerprint";
    }

    static async Task<DeliveryRow?> ReadDeliveryAsync(PgTx tx, OutboxSourceIdentity source, bool forUpdate = true)
    {
        using var q = tx.Sql($"""
            SELECT job_id, disposition, duplicate_match, policy_drift, delivered_at, accept_until, confirmed_profile, delivery_fingerprint
            FROM runwick.outbox_deliveries
            WHERE application = @application AND source_store_id = @source AND message_id = @message
            {(forUpdate ? "FOR UPDATE" : "")}
            """).P("source", source.SourceStoreId).P("message", source.MessageId);
        var rows = await tx.QueryAsync(q, x => new DeliveryRow(
            new OutboxDelivery(x.GetGuid(0), Enum.Parse<DispatchDisposition>(x.GetString(1)), Enum.Parse<DuplicateMatch>(x.GetString(2)),
                x.GetBoolean(3), FirstDelivery: false, PgTime.Read(x, 4), PgTime.Read(x, 5), x.GetString(6)),
            (byte[])x[7])).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    static OutboxDelivery ReadDeliveryJson(JsonNode o) => new(
        Guid.Parse(o["jobId"]!.GetValue<string>()),
        Enum.Parse<DispatchDisposition>(o["disposition"]!.GetValue<string>()),
        Enum.Parse<DuplicateMatch>(o["match"]!.GetValue<string>()),
        o["policyDrift"]!.GetValue<bool>(),
        o["firstDelivery"]!.GetValue<bool>(),
        PgTime.FromMs(o["deliveredAt"]!.GetValue<long>()),
        PgTime.FromMs(o["acceptUntil"]!.GetValue<long>()),
        o["confirmedProfile"]!.GetValue<string>());
}
