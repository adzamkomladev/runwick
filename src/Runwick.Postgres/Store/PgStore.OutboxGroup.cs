using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IOutboxGroupDestinationStore
{
    /// <summary>
    /// Grouped ReceiveOutbox/1 (runwick.receive_outbox_group_v1): every command keeps its own ID, fingerprint, deadline,
    /// receipt and mapping. The reply is the group's; a group-level refusal or failure has no effect and the caller sends
    /// each command alone under its own identity, which the per-command receipts make safe after a lost reply.
    /// </summary>
    public async Task<StoreReply<OutboxGroupDelivery>> ReceiveOutboxGroupAsync(
        IReadOnlyList<StoreCommand<ReceiveOutboxRequest>> commands, CancellationToken ct)
    {
        if (commands.Count is < 2 or > IOutboxGroupDestinationStore.MaxGroupItems
            || commands.Any(c => !string.Equals(c.Application, commands[0].Application, StringComparison.Ordinal)
                || !IOutboxGroupDestinationStore.CanGroup(c.Body) || ValidateOutbox(c) is not null)
            || commands.Select(c => c.CommandId).Distinct().Count() != commands.Count)
        {
            return StoreReply<OutboxGroupDelivery>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_outbox_group"), PgTime.Floor(time.GetUtcNow()));
        }

        var application = commands[0].Application;
        var storeId = await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var items = new JsonArray();
        foreach (var command in commands)
        {
            var r = command.Body;
            var item = new JsonObject
            {
                ["p_command_id"] = JsonValue.Create(command.CommandId),
                ["p_not_after"] = JsonValue.Create(PgTime.Floor(command.NotAfter).UtcDateTime),
                ["p_fingerprint"] = EncodeArgument(
                    OutboxFingerprints.Command(storeId, application, command.CommandId, command.NotAfter, r.DeliveryFingerprint), NpgsqlDbType.Bytea),
                ["p_source_store_id"] = JsonValue.Create(r.Source.SourceStoreId),
                ["p_message_id"] = JsonValue.Create(r.Source.MessageId),
                ["p_delivery_fingerprint"] = EncodeArgument(r.DeliveryFingerprint, NpgsqlDbType.Bytea),
                ["p_accept_until"] = JsonValue.Create(PgTime.Floor(r.AcceptUntil).UtcDateTime),
                ["p_profile"] = JsonValue.Create(OutboxOperations.PostgresProfile),
            };
            foreach (var (name, value, type) in DispatchArgs([r.Item]))
            {
                if (name is not ("p_scopes" or "p_rates"))
                {
                    item.Add(name, EncodeArgument(value, type));
                }
            }

            items.Add(item);
        }

        // The call envelope bounds transport for the group. Each command keeps its own deadline in SQL.
        var op = Op(application, Guid.CreateVersion7(), OutboxOperations.ReceiveOutbox, commands.Max(c => c.NotAfter), null, PgLane.Producer,
            commands[0].Body.StoreId);
        var reply = await runner.CallAsync(op, new PgCall("receive_outbox_group_v1").Json("p_items", items), row =>
            row.HasValue
                ? row.Applied(new OutboxGroupDelivery([.. commands.Select(c =>
                {
                    var item = GroupItem(row.Result![c.CommandId.ToString("D")]!.AsObject());
                    return item.HasValue ? item.Applied(ReadDeliveryJson(item.Result!)) : item.Reject<OutboxDelivery>();
                })]))
                : row.Reject<OutboxGroupDelivery>(), ct).ConfigureAwait(false);
        if (reply.Value is { } group)
        {
            for (var i = 0; i < commands.Count; i++)
            {
                WakeDelivered(commands[i], group.Outcomes[i]);
            }
        }

        return reply;
    }
}
