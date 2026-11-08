using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// ReceiveOutbox/1 (14 §5, 20 §7): receive_outbox_v3's steps on the write lock. The write lock serializes every transport
/// command for one delivery identity, so no advisory lock is needed; the mapping is committed with the receipt.
/// </summary>
internal sealed partial class SqliteStore : IOutboxDestinationStore
{
    public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateOutbox(command) is { } invalid)
        {
            return StoreReply<OutboxDelivery>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), Now());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = OutboxFingerprints.Command(storeId, command.Application, command.CommandId, command.NotAfter, r.DeliveryFingerprint);
        var acceptUntil = SqTime.Floor(r.AcceptUntil);
        return await runner.CallAsync(
            Op(command.Application, command.CommandId, OutboxOperations.ReceiveOutbox, command.NotAfter, fp, SqLane.Producer, r.StoreId),
            tx => ReceiveBody(tx, r, acceptUntil, AcknowledgementProfile),
            row => row.HasValue ? row.Applied(ReadDeliveryJson(row.Result!)) : row.Reject<OutboxDelivery>(),
            ct).ConfigureAwait(false);
    }

    /// <summary>receive_outbox_v3 after the frame (the frame already checked expiry and the deadline cap at the decision time).</summary>
    static SqRow ReceiveBody(SqTx tx, ReceiveOutboxRequest r, DateTimeOffset acceptUntil, string profile)
    {
        var found = ReadDelivery(tx, r.Source);
        var now = tx.Now;

        // A retained mapping answers every later transport command for the identity, even after AcceptUntil.
        if (found is { } f)
        {
            if (!f.Fingerprint.AsSpan().SequenceEqual(r.DeliveryFingerprint))
            {
                return SqRow.NoEffect("CommandConflict", "delivery_conflict", now, null, f.Delivery.JobId);
            }

            return SqRow.Applied("Applied", null, now, DeliveryJson(f.Delivery));
        }

        if (now >= acceptUntil)
        {
            return SqRow.NoEffect("CommandExpired", "delivery_horizon_passed", now);
        }

        var core = DispatchCore(tx, [r.Item], continueOnConflict: false, "initial");
        if (core.Reject is { } reject)
        {
            return reject;
        }

        var item = core.Items![0]!.AsObject();
        var delivery = new OutboxDelivery(item["jobId"]!.GetValue<Guid>(), Enum.Parse<DispatchDisposition>(item["disposition"]!.GetValue<string>()),
            Enum.Parse<DuplicateMatch>(item["match"]!.GetValue<string>()), item["policyDrift"]!.GetValue<bool>(), FirstDelivery: true, now, acceptUntil, profile);
        var n = tx.Exec(tx.Sql("""
            INSERT INTO runwick_outbox_deliveries (
                application, source_store_id, message_id, delivery_fingerprint, fingerprint_version, accept_until, retain_until,
                job_id, disposition, duplicate_match, policy_drift, confirmed_profile, first_command_id, delivered_at)
            VALUES (@application, @source, @message, @fp, 1, @accept, @retain, @job, @disposition, @match, @drift, @profile, @command, @now)
            """).P("source", r.Source.SourceStoreId).P("message", r.Source.MessageId).P("fp", r.DeliveryFingerprint).P("accept", acceptUntil)
            .P("retain", acceptUntil + OutboxOperations.MappingRetention).P("job", delivery.JobId).P("disposition", delivery.Disposition.ToString())
            .P("match", delivery.Match.ToString()).P("drift", delivery.PolicyDrift).P("profile", profile).P("command", tx.Op.CommandId).P("now", now));
        if (n != 1)
        {
            throw new SqInvariantException("invalid_transition");
        }

        return SqRow.Applied("Applied", null, now, DeliveryJson(delivery));
    }

    public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(deliveryFingerprint);

        // Read-only, no receipt: one snapshot on the read path (20 §4.2).
        return runner.ReadAsync(conn =>
        {
            var tx = new SqTx(conn, null, new SqOp(application, Guid.CreateVersion7(), "ResolveCommand", DateTimeOffset.MaxValue, null, SqLane.Background),
                runner.Faults, ct);
            var found = ReadDelivery(tx, source);
            var match = found is { } f && f.Fingerprint.AsSpan().SequenceEqual(deliveryFingerprint);
            return new OutboxLookup(match ? found!.Delivery : null, Conflict: found is not null && !match);
        }, ct);
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

        var expected = OutboxFingerprints.Delivery(r.StoreId, command.Application, r.Source, SqTime.Ms(SqTime.Floor(r.AcceptUntil)), r.Item.Intent, r.Item.Digests);
        return expected.AsSpan().SequenceEqual(r.DeliveryFingerprint) ? null : "deliveryFingerprint";
    }

    static DeliveryRow? ReadDelivery(SqTx tx, OutboxSourceIdentity source) =>
        tx.One(tx.Sql("""
            SELECT job_id, disposition, duplicate_match, policy_drift, delivered_at, accept_until, confirmed_profile, delivery_fingerprint
            FROM runwick_outbox_deliveries
            WHERE application = @application AND source_store_id = @source AND message_id = @message
            """).P("source", source.SourceStoreId).P("message", source.MessageId),
            x => new DeliveryRow(
                new OutboxDelivery(x.Uuid(0), Enum.Parse<DispatchDisposition>(x.GetString(1)), Enum.Parse<DuplicateMatch>(x.GetString(2)),
                    x.Bool(3), FirstDelivery: false, x.Time(4), x.Time(5), x.GetString(6)),
                x.Blob(7)));

    static JsonObject DeliveryJson(OutboxDelivery d) => new()
    {
        ["jobId"] = d.JobId,
        ["disposition"] = d.Disposition.ToString(),
        ["match"] = d.Match.ToString(),
        ["policyDrift"] = d.PolicyDrift,
        ["firstDelivery"] = d.FirstDelivery,
        ["deliveredAt"] = SqTime.Ms(d.DeliveredAt),
        ["acceptUntil"] = SqTime.Ms(d.AcceptUntil),
        ["confirmedProfile"] = d.ConfirmedProfile,
    };

    static OutboxDelivery ReadDeliveryJson(JsonNode o) => new(
        Guid.Parse(o["jobId"]!.GetValue<string>()),
        Enum.Parse<DispatchDisposition>(o["disposition"]!.GetValue<string>()),
        Enum.Parse<DuplicateMatch>(o["match"]!.GetValue<string>()),
        o["policyDrift"]!.GetValue<bool>(),
        o["firstDelivery"]!.GetValue<bool>(),
        SqTime.FromMs(o["deliveredAt"]!.GetValue<long>()),
        SqTime.FromMs(o["acceptUntil"]!.GetValue<long>()),
        o["confirmedProfile"]!.GetValue<string>());
}
