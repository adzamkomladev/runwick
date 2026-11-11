using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    public async Task<StoreReply<ScheduleGroupMaterialization>> MaterializeGroupAsync(
        IReadOnlyList<StoreCommand<MaterializeRequest>> commands, CancellationToken ct)
    {
        if (commands.Count is < 2 or > ScheduleBounds.MaxGroupPlans
            || commands.Any(c => !string.Equals(c.Application, commands[0].Application, StringComparison.Ordinal))
            || commands.Select(c => c.CommandId).Distinct().Count() != commands.Count
            || commands.Select(c => c.Body.ScheduleId).Distinct().Count() != commands.Count)
        {
            throw new ArgumentException("Expected two to four distinct schedule commands from one application.", nameof(commands));
        }

        var plans = new JsonArray();
        PgOp? firstOperation = null;
        long payloadBytes = 0;
        foreach (var command in commands)
        {
            var (prepared, _) = await PrepareMaterializationAsync(command, ct).ConfigureAwait(false);
            if (prepared?.GroupPayloadBytes is not { } bytes || (payloadBytes += bytes) > ScheduleBounds.MaxPlanPayloadBytes)
            {
                return await MaterializeSeparatelyAsync(commands, ct).ConfigureAwait(false);
            }

            var op = prepared.Operation;
            firstOperation ??= op;
            var plan = new JsonObject
            {
                ["p_command_id"] = JsonValue.Create(op.CommandId),
                ["p_not_after"] = JsonValue.Create(op.NotAfter),
                ["p_fingerprint"] = EncodeArgument(op.Fingerprint, NpgsqlDbType.Bytea),
            };
            foreach (var parameter in prepared.Call.Parameters)
            {
                if (parameter.ParameterName is not ("p_scopes" or "p_rates"))
                {
                    plan.Add(parameter.ParameterName, EncodeArgument(parameter.Value, parameter.NpgsqlDbType));
                }
            }

            plans.Add(plan);
        }

        // The call envelope bounds transport for the group. Each plan retains its own deadline in SQL.
        var operation = firstOperation! with { NotAfter = commands.Max(c => c.NotAfter) };
        var reply = await runner.CallAsync(operation, new PgCall("materialize_schedule_group_v1").Json("p_plans", plans), row =>
            row.HasValue
                ? row.Applied(new ScheduleGroupMaterialization(
                    [.. commands.Select(c => MapMaterialization(GroupItem(row.Result![c.CommandId.ToString("D")]!.AsObject())))],
                    Grouped: true))
                : row.Reject<ScheduleGroupMaterialization>(), ct).ConfigureAwait(false);
        // With no group-level effect, replay/send each command using its original identity.
        return reply.Status is StoreStatus.InvariantFault or StoreStatus.CommandExpired || reply.Error?.Code == "invalid_schedule_group"
            ? await MaterializeSeparatelyAsync(commands, ct).ConfigureAwait(false)
            : reply;
    }

    async Task<StoreReply<ScheduleGroupMaterialization>> MaterializeSeparatelyAsync(
        IReadOnlyList<StoreCommand<MaterializeRequest>> commands, CancellationToken ct)
    {
        var replies = new List<StoreReply<ScheduleMaterialization>>(commands.Count);
        foreach (var command in commands)
        {
            replies.Add(await MaterializeOccurrencesAsync(command, ct).ConfigureAwait(false));
        }

        return StoreReply<ScheduleGroupMaterialization>.Applied(new ScheduleGroupMaterialization(replies, Grouped: false), time.GetUtcNow());
    }

    static JsonNode? EncodeArgument(object? value, NpgsqlDbType type)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        if ((type & NpgsqlDbType.Array) != 0)
        {
            var element = type & ~NpgsqlDbType.Array;
            return new JsonArray([.. ((Array)value).Cast<object?>().Select(item => EncodeArgument(item, element))]);
        }

        return value switch
        {
            byte[] bytes => JsonValue.Create("\\x" + Convert.ToHexStringLower(bytes)),
            string text when type == NpgsqlDbType.Jsonb => JsonNode.Parse(text),
            string text => JsonValue.Create(text),
            bool bit => JsonValue.Create(bit),
            int number => JsonValue.Create(number),
            long number => JsonValue.Create(number),
            Guid id => JsonValue.Create(id),
            DateTime at => JsonValue.Create(at),
            _ => throw new InvalidOperationException($"Unsupported group argument type {type}."),
        };
    }

    static PgRow GroupItem(JsonObject item) => new()
    {
        Status = item["status"]!.GetValue<string>(),
        Effect = item["effect"]!.GetValue<string>(),
        Code = item["code"]?.GetValue<string>(),
        Detail = item["detail"]?.GetValue<string>(),
        ConflictingJobId = item["conflicting_job_id"]?.GetValue<Guid>(),
        BlockedScopeId = item["blocked_scope_id"]?.GetValue<Guid>(),
        ResumeEligibleAt = item["resume_eligible_at"]?.GetValue<DateTimeOffset>(),
        ResumeJobId = item["resume_job_id"]?.GetValue<Guid>(),
        DecidedAt = item["decided_at"]?.GetValue<DateTimeOffset>(),
        Replayed = item["replayed"]?.GetValue<bool>() ?? false,
        Result = item["result"]?.AsObject(),
    };
}
