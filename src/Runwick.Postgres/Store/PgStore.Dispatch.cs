using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    // Direct calls use dispatch_v4 and can publish a wake after commit. Staged calls use dispatch_v3 and its
    // transactional wake, as do the outbox, management and schedule paths through rw_dispatch_core_v3 (04 §7.2).

    public async Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        var reply = await runner.CallAsync(DispatchOp(command), DispatchCall([command.Body.Item], many: false, continueOnConflict: false),
            MapDispatch, ct).ConfigureAwait(false);
        var item = command.Body.Item;
        if (reply.Status == StoreStatus.Applied && !reply.Replayed && IsDueCreated(item, reply.Value, PgTime.Ms(reply.DecidedAt)))
        {
            sources.WakeDispatch(command.Application, item.Intent.Queue);
        }

        return reply;
    }

    public async Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct)
    {
        var items = command.Body.Items;
        var reply = await runner.CallAsync(DispatchManyOp(command), DispatchCall(items, many: true, command.Body.ContinueOnConflict),
            MapDispatchMany, ct).ConfigureAwait(false);
        if (reply.Status == StoreStatus.Applied && !reply.Replayed && reply.Value is { } value)
        {
            var decidedMs = PgTime.Ms(reply.DecidedAt);
            HashSet<string>? queues = null;
            foreach (var outcome in value.Items)
            {
                var item = items[outcome.Ordinal];
                if (IsDueCreated(item, outcome.Result, decidedMs) && (queues ??= new(StringComparer.Ordinal)).Add(item.Intent.Queue))
                {
                    sources.WakeDispatch(command.Application, item.Intent.Queue);
                }
            }
        }

        return reply;
    }

    static bool IsDueCreated(DispatchItem item, DispatchResult? result, long decidedMs) =>
        result?.Disposition == DispatchDisposition.Created && (item.Intent.NotBeforeMs is not { } notBefore || notBefore <= decidedMs);

    /// <summary>Dispatch/1 on a caller-owned transaction (01 §15, 04 §6.1).</summary>
    public Task<StoreReply<DispatchResult>> StageDispatchAsync(NpgsqlTransaction transaction, StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        runner.StageCallAsync(transaction, DispatchOp(command), DispatchCall([command.Body.Item], many: false, continueOnConflict: false, staged: true),
            MapDispatch, ct);

    /// <summary>DispatchMany/1 on a caller-owned transaction.</summary>
    public Task<StoreReply<DispatchBatchResult>> StageDispatchManyAsync(NpgsqlTransaction transaction, StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        runner.StageCallAsync(transaction, DispatchManyOp(command), DispatchCall(command.Body.Items, many: true, command.Body.ContinueOnConflict, staged: true),
            MapDispatchMany, ct);

    PgOp DispatchOp(StoreCommand<DispatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.Dispatch, command.NotAfter, command.Body.CommandFingerprint, PgLane.Producer, command.Body.StoreId);

    PgOp DispatchManyOp(StoreCommand<DispatchBatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.DispatchMany, command.NotAfter, command.Body.CommandFingerprint, PgLane.Producer, command.Body.StoreId);

    static PgCall DispatchCall(IReadOnlyList<DispatchItem> items, bool many, bool continueOnConflict, bool staged = false)
    {
        var call = new PgCall(staged ? "dispatch_v3" : "dispatch_v4").P("p_many", many).P("p_continue", continueOnConflict);
        foreach (var (name, value, type) in DispatchArgs(items))
        {
            call.P(name, value, type);
        }

        return call;
    }

    static StoreReply<DispatchResult> MapDispatch(PgRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadDispatch(row.Result!)) : row.Reject<DispatchResult>();

    static StoreReply<DispatchBatchResult> MapDispatchMany(PgRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadDispatchMany(row.Result!)) : row.Reject<DispatchBatchResult>();

    /// <summary>A rejection, or the per-item results in ordinal order.</summary>
    sealed record DispatchOutcome(IReadOnlyList<DispatchBatchItemResult> Items, (StoreStatus Status, StoreError Error)? Rejection);

    /// <summary>
    /// The validation bounds of 03 I-6. They depend on the request alone, so the client computes them; the function
    /// reports them after the gates and the receipt, before any write, as the statement path did.
    /// </summary>
    static string? InvalidDispatch(IReadOnlyList<DispatchItem> items)
    {
        foreach (var item in items)
        {
            var p = item.Intent.Policy;
            if (p.MaxExecutions is < 1 or > 100 || p.MaxConsecutiveUnstartedAbandonments is < 1 or > 100
                || p.ExecutionTimeoutMs is < 1 or > 2_592_000_000 || item.Scopes.Count > 8 || item.Intent.RateScopes.Count > 8
                || item.Rates.Count != item.Intent.RateScopes.Count
                || item.Rates.Any(r => r.Limit is < 1 or > 100_000 || r.WindowMs is < 1_000 or > 86_400_000)
                || !ValidRetry(p.Retry) || !ValidRetry(p.UnstartedRetry))
            {
                return "policy or scope bounds";
            }
        }

        return null;
    }

    /// <summary>The items of rw_dispatch_core_v3 and rw_dispatch_core_v4 as parallel arrays in ordinal order.</summary>
    static IEnumerable<(string Name, object? Value, NpgsqlDbType Type)> DispatchArgs(IReadOnlyList<DispatchItem> items)
    {
        const NpgsqlDbType A = NpgsqlDbType.Array;
        var intents = items.Select(i => i.Intent).ToList();
        yield return ("p_invalid", InvalidDispatch(items), NpgsqlDbType.Text);
        yield return ("p_job_ids", intents.Select(i => i.ProposedJobId).ToArray(), A | NpgsqlDbType.Uuid);
        yield return ("p_job_names", intents.Select(i => i.JobName).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_payload_versions", intents.Select(i => i.PayloadVersion).ToArray(), A | NpgsqlDbType.Integer);
        yield return ("p_profile_ids", intents.Select(i => i.SerializerProfileId).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_profile_versions", intents.Select(i => i.SerializerProfileVersion).ToArray(), A | NpgsqlDbType.Integer);
        yield return ("p_queues", intents.Select(i => i.Queue).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_priorities", intents.Select(i => i.Priority).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_not_before_ms", intents.Select(i => i.NotBeforeMs).ToArray(), A | NpgsqlDbType.Bigint);
        yield return ("p_active_keys", intents.Select(i => i.ActiveKey).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_window_keys", intents.Select(i => i.WindowKey).ToArray(), A | NpgsqlDbType.Text);
        yield return ("p_window_ms", intents.Select(i => i.WindowMs).ToArray(), A | NpgsqlDbType.Bigint);
        yield return ("p_business", items.Select(i => i.Digests.Business).ToArray(), A | NpgsqlDbType.Bytea);
        yield return ("p_policy", items.Select(i => i.Digests.Policy).ToArray(), A | NpgsqlDbType.Bytea);
        yield return ("p_payloads", intents.Select(i => i.PayloadUtf8).ToArray(), A | NpgsqlDbType.Bytea);
        yield return ("p_headers", items.Select(Headers).ToArray(), A | NpgsqlDbType.Jsonb);
        yield return ("p_max_executions", intents.Select(i => i.Policy.MaxExecutions).ToArray(), A | NpgsqlDbType.Integer);
        yield return ("p_max_unstarted", intents.Select(i => i.Policy.MaxConsecutiveUnstartedAbandonments).ToArray(), A | NpgsqlDbType.Integer);
        yield return ("p_timeouts", intents.Select(i => i.Policy.ExecutionTimeoutMs).ToArray(), A | NpgsqlDbType.Bigint);
        yield return ("p_retry", intents.Select(i => PolicyJson(i.Policy.Retry).ToJsonString()).ToArray(), A | NpgsqlDbType.Jsonb);
        yield return ("p_unstarted", intents.Select(i => PolicyJson(i.Policy.UnstartedRetry).ToJsonString()).ToArray(), A | NpgsqlDbType.Jsonb);
        yield return ("p_scopes", items.Select(i => new JsonArray([.. i.Scopes.Select(s => (JsonNode)new JsonObject
            {
                ["kind"] = s.Kind, ["key"] = s.Key, ["capacity"] = s.Capacity,
            })]).ToJsonString()).ToArray(), A | NpgsqlDbType.Jsonb);
        yield return ("p_rates", items.Select(i => new JsonArray([.. i.Rates.Select(r => (JsonNode)new JsonObject
            {
                ["kind"] = r.Kind, ["key"] = r.Key, ["limit"] = r.Limit, ["windowMs"] = r.WindowMs,
            })]).ToJsonString()).ToArray(), A | NpgsqlDbType.Jsonb);
    }

    /// <summary>
    /// Steps 2-10 of 04 §7.2 inside a statement-path transaction (the schedule operations that stay on statements): one
    /// statement that runs runwick.rw_dispatch_core_v3 with a transactional wake. Its writes count as one mutation step,
    /// and the decision time it read becomes this transaction's.
    /// </summary>
    async Task<DispatchOutcome> DispatchCoreAsync(PgTx tx, IReadOnlyList<DispatchItem> items, bool continueOnConflict,
        string createdReason = "initial")
    {
        var args = DispatchArgs(items).ToList();
        using var cmd = tx.Sql($"""
            SELECT c.p_now, (c.o_reject).status, (c.o_reject).code, (c.o_reject).detail, (c.o_reject).conflicting_job_id, c.o_items::text
            FROM runwick.rw_dispatch_core_v3(p_application => @application, p_command_id => @command_id, p_not_after => @not_after,
                p_continue => @continue, p_created_reason => @created_reason,
                {string.Join(", ", args.Select(a => $"{a.Name} => @{a.Name}"))},
                p_faults => NULL, p_steps => 0, p_now => NULL) AS c
            """).P("command_id", tx.Op.CommandId).P("not_after", tx.Op.NotAfter).P("continue", continueOnConflict).P("created_reason", createdReason);
        foreach (var (name, value, type) in args)
        {
            cmd.Parameters.Add(new NpgsqlParameter(name, type) { Value = value ?? DBNull.Value });
        }

        // The fresh-time point fires before the statement: the caller's own locks are held; the key locks are taken
        // inside the function, behind it (07 Changes 2026-11-02, PF-05).
        await tx.Fault(FaultPoint.AfterLocksBeforeFreshTime).ConfigureAwait(false);
        var row = (await tx.QueryAsync(cmd, r => (
            Now: PgTime.ReadNullable(r, 0), Status: r.IsDBNull(1) ? null : r.GetString(1), Code: r.IsDBNull(2) ? null : r.GetString(2),
            Detail: r.IsDBNull(3) ? null : r.GetString(3), Conflicting: r.IsDBNull(4) ? (Guid?)null : r.GetGuid(4),
            Items: r.IsDBNull(5) ? null : r.GetString(5)), mutation: true).ConfigureAwait(false))[0];
        if (row.Now is { } now)
        {
            tx.DecidedAt(now);
        }

        return row.Status is { } status
            ? new DispatchOutcome([], (Enum.Parse<StoreStatus>(status), new StoreError(row.Code!, ConflictingJobId: row.Conflicting, Detail: row.Detail)))
            : new DispatchOutcome(ReceiptCodec.ReadDispatchMany(new JsonObject { ["items"] = JsonNode.Parse(row.Items!) }).Items, null);
    }

    static bool ValidRetry(RetryPolicy r) =>
        r.InitialDelayMs is >= 1 and <= 86_400_000 && r.MaximumDelayMs >= r.InitialDelayMs && r.MaximumDelayMs <= 86_400_000;

    /// <summary>
    /// The payload row's headers: the two metadata maps and, for a store-built scheduled job (13 §3.2), its occurrence.
    /// A manual trigger's occurrence is the decision time of the creating command, so it is sent as nulls and filled in
    /// by runwick.rw_dispatch_core_v3.
    /// </summary>
    static string Headers(DispatchItem item)
    {
        var intent = item.Intent;
        static JsonObject Map(IReadOnlyDictionary<string, string> m)
        {
            var o = new JsonObject();
            foreach (var key in m.Keys.Order(Internal.Utf8Order.Instance))
            {
                o[key] = m[key];
            }

            return o;
        }

        var headers = new JsonObject { ["business"] = Map(intent.BusinessMetadata), ["diagnostic"] = Map(intent.DiagnosticMetadata) };
        if (item.Schedule is { } s)
        {
            var manual = s.ScheduledAt == default;
            headers["schedule"] = new JsonObject
            {
                ["key"] = s.ScheduleName,
                ["scheduledAt"] = manual ? null : PgTime.Ms(s.ScheduledAt),
                ["coveredFrom"] = manual ? null : PgTime.Ms(s.CoveredFrom),
                ["coveredUntil"] = manual ? null : PgTime.Ms(s.CoveredUntil),
            };
        }

        return headers.ToJsonString();
    }

    // ------------------------------------------------------------------ ResolveCommand/1

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) =>
        ResolveCoreAsync(command.Application, command.CommandId, command.Operation, command.Fingerprint, command.NotAfter, ct);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        ResolveCoreAsync(application, commandId, null, null, notAfterBound, ct);

    /// <summary>Joins the receipt row's lock queue, classifies (03 §8.6) and always rolls back.</summary>
    async Task<ReceiptResolution> ResolveCoreAsync(string application, Guid commandId, string? operation, byte[]? fingerprint, DateTimeOffset notAfter, CancellationToken ct)
    {
        ReceiptResolution? resolution = null;
        var op = new PgOp(application, commandId, "ResolveCommand", DateTimeOffset.MaxValue, null, PgLane.Producer, null);
        var reply = await runner.ExecuteAsync<ResolutionBox>(op, async tx =>
        {
            await tx.Fault(FaultPoint.DuringReceiptLookup).ConfigureAwait(false);
            ReceiptRow? found = null;
            foreach (var retentionClass in operation is null ? BothClasses : [Operations.RetentionClass(operation)])
            {
                found = await PgRunner.ReadReceiptAsync(tx, retentionClass).ConfigureAwait(false);
                if (found is not null)
                {
                    break;
                }
            }

            var now = await tx.ClockAsync().ConfigureAwait(false);
            if (found is { CodecVersion: not 1 } or { OperationVersion: not 1 })
            {
                // 23 §8: a receipt this binary cannot decode is never reported as not found, conflict or success.
                throw new StoreIncompatibleException($"Receipt codec {found.CodecVersion}/operation version {found.OperationVersion} is not supported by this binary.");
            }

            if (found is not null)
            {
                resolution = (operation is not null && (found.Operation != operation || !found.Fingerprint.AsSpan().SequenceEqual(fingerprint)))
                    ? new ReceiptResolution.Conflict()
                    : found.Result is null
                        ? new ReceiptResolution.NotObserved()
                        : new ReceiptResolution.Recorded(found.Operation, found.OperationVersion, found.Status!, found.DecidedAt!.Value,
                            System.Text.Json.JsonDocument.Parse(found.Result.ToJsonString()).RootElement.Clone());
            }
            else
            {
                var floor = operation is null ? TimeSpan.FromHours(24) : Operations.RetentionFloor(operation);
                resolution = now < notAfter ? new ReceiptResolution.NotObserved()
                    : now < notAfter + floor ? new ReceiptResolution.NotApplied()
                    : new ReceiptResolution.ExpiredUnknown();
            }

            return new PgOutcome<ResolutionBox>.Rollback(StoreReply<ResolutionBox>.NoEffect(StoreStatus.NoWork, new StoreError("resolved"), now));
        }, null, ct).ConfigureAwait(false);

        return resolution ?? throw new StoreIncompatibleException($"Receipt resolution failed: {reply}");
    }

    static readonly string[] BothClasses = [Operations.DurableClass, Operations.OwnershipClass];

    sealed record ResolutionBox;
}
