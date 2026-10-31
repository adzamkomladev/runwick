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
        return await runner.ExecuteAsync(
            Op(command.Application, command.CommandId, OutboxOperations.ReceiveOutbox, command.NotAfter, fp, PgLane.Producer, r.StoreId),
            async tx =>
            {
                await LockDeliveryAsync(tx, r.Source).ConfigureAwait(false);
                var existing = await ReadDeliveryAsync(tx, r.Source).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<OutboxDelivery>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (tx.Op.NotAfter - tx.Now > OutboxOperations.TransportCap)
                {
                    return Rollback<OutboxDelivery>(tx, StoreStatus.InvalidRequest, "deadline_too_far");
                }

                // A retained mapping answers every later transport command for the identity, even after AcceptUntil.
                if (existing is { } found)
                {
                    if (!found.Fingerprint.AsSpan().SequenceEqual(r.DeliveryFingerprint))
                    {
                        return Rollback<OutboxDelivery>(tx, StoreStatus.CommandConflict, "delivery_conflict", found.Delivery.JobId);
                    }

                    var mapped = found.Delivery with { FirstDelivery = false };
                    return new PgOutcome<OutboxDelivery>.Commit(StoreReply<OutboxDelivery>.Applied(mapped, tx.Now), DeliveryJson(mapped));
                }

                if (tx.Now >= acceptUntil)
                {
                    return Rollback<OutboxDelivery>(tx, StoreStatus.CommandExpired, "delivery_horizon_passed");
                }

                var outcome = await DispatchCoreAsync(tx, [r.Item], continueOnConflict: false).ConfigureAwait(false);
                if (outcome.Rejection is { } rejection)
                {
                    return new PgOutcome<OutboxDelivery>.Rollback(StoreReply<OutboxDelivery>.NoEffect(rejection.Status, rejection.Error, tx.Now));
                }

                // The dispatch path re-read the clock after its own locks: the cutoff is checked at that decision instant.
                if (tx.Now >= acceptUntil)
                {
                    return Rollback<OutboxDelivery>(tx, StoreStatus.CommandExpired, "delivery_horizon_passed");
                }

                var d = outcome.Items[0].Result!;
                var value = new OutboxDelivery(d.JobId, d.Disposition, d.Match, d.PolicyDrift, FirstDelivery: true, tx.Now, acceptUntil,
                    OutboxOperations.PostgresProfile);
                using (var insert = tx.Sql("""
                    INSERT INTO runwick.outbox_deliveries (
                        application, source_store_id, message_id, delivery_fingerprint, fingerprint_version, accept_until, retain_until,
                        job_id, disposition, duplicate_match, policy_drift, confirmed_profile, first_command_id, delivered_at)
                    VALUES (@application, @source, @message, @fingerprint, 1, @accept_until, @accept_until + interval '24 hours',
                            @job, @disposition, @match, @drift, @profile, @command_id, @now)
                    """).P("source", r.Source.SourceStoreId).P("message", r.Source.MessageId)
                        .P("fingerprint", r.DeliveryFingerprint, NpgsqlDbType.Bytea).P("accept_until", acceptUntil)
                        .P("job", d.JobId).P("disposition", d.Disposition.ToString()).P("match", d.Match.ToString())
                        .P("drift", d.PolicyDrift).P("profile", value.ConfirmedProfile).P("command_id", command.CommandId).P("now", tx.Now))
                {
                    Expect(await tx.ExecAsync(insert).ConfigureAwait(false), 1);
                }

                return new PgOutcome<OutboxDelivery>.Commit(StoreReply<OutboxDelivery>.Applied(value, tx.Now), DeliveryJson(value));
            },
            (tx, row) => Task.FromResult(StoreReply<OutboxDelivery>.Applied(ReadDeliveryJson(row.Result!), row.DecidedAt!.Value, replayed: true)),
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

    static async Task LockDeliveryAsync(PgTx tx, OutboxSourceIdentity source)
    {
        // Serializes every transport command for one delivery identity, including the first one, without committing a
        // placeholder row. A hash collision only serializes two unrelated identities.
        using var l = tx.Sql("SELECT pg_advisory_xact_lock(hashtextextended('runwick.outbox/' || @application || '/' || @source || '/' || @message, 0))")
            .P("source", source.SourceStoreId.ToString("D")).P("message", source.MessageId.ToString("D"));
        await tx.QueryAsync(l, _ => 0).ConfigureAwait(false);
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

    static JsonObject DeliveryJson(OutboxDelivery d) => new()
    {
        ["jobId"] = d.JobId.ToString("D"),
        ["disposition"] = d.Disposition.ToString(),
        ["match"] = d.Match.ToString(),
        ["policyDrift"] = d.PolicyDrift,
        ["firstDelivery"] = d.FirstDelivery,
        ["deliveredAt"] = PgTime.Ms(d.DeliveredAt),
        ["acceptUntil"] = PgTime.Ms(d.AcceptUntil),
        ["confirmedProfile"] = d.ConfirmedProfile,
    };

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
