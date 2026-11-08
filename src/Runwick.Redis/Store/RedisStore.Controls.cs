using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Redis.Store;

/// <summary>
/// Revisioned controls (08 §3, 22 §7): Cancel/1, ManualRetry/1, RerunSucceeded/1, SetQueueState/1, EditScopePolicy/1,
/// ManagementDispatch/1 and EventsSince/1. Each control is one script: effect, event, audit row and receipt in one intent.
/// </summary>
internal sealed partial class RedisStore : IJobControlStore
{
    const int MaxScopeCapacity = 10_000;

    /// <summary>08 §2 bounds (as PgStore.ValidateActor); returns the offending field or null.</summary>
    internal static string? ControlValidateActor(ControlActor actor)
    {
        static bool Bytes(string? s, int min, int max) => s is null ? min == 0 : Encoding.UTF8.GetByteCount(s) is var n && n >= min && n <= max;

        if (actor is null || !Bytes(actor.Subject, 1, 256) || actor.Subject.Any(char.IsControl))
        {
            return "actor.subject";
        }

        if (!Bytes(actor.Display, 0, 256))
        {
            return "actor.display";
        }

        if (string.IsNullOrEmpty(actor.Scheme) || actor.Scheme.Length > 64 || !char.IsAsciiLetterOrDigit(actor.Scheme[0])
            || actor.Scheme.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-')))
        {
            return "actor.scheme";
        }

        return Bytes(actor.Reason, 0, 512) ? null : "reason";
    }

    static string ControlActorJson(ControlActor a) =>
        Json(new JsonObject { ["subject"] = a.Subject, ["display"] = a.Display, ["scheme"] = a.Scheme, ["reason"] = a.Reason });

    StoreReply<T> ControlInvalid<T>(string field) where T : class =>
        StoreReply<T>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: field), time.GetUtcNow());

    /// <summary>A refusal; a revision conflict carries the actual revision (the last number of <c>actual=…</c>).</summary>
    static StoreReply<T> ControlReject<T>(RxReply r) where T : class
    {
        var reply = r.Reject<T>();
        if (r.Code == "revision_conflict" && r.Detail is { } d && long.TryParse(d[(d.LastIndexOfAny(['=', '/']) + 1)..], CultureInfo.InvariantCulture, out var actual))
        {
            reply = reply with { Error = reply.Error! with { ActualRevision = actual } };
        }

        return reply;
    }

    async Task<StoreReply<T>> ControlAsync<T>(string application, Guid commandId, string operation, DateTimeOffset notAfter,
        Func<Guid, byte[]> fingerprint, string script, Func<JsonObject, T> read, CancellationToken ct, params StackExchange.Redis.RedisValue[] args)
        where T : class
    {
        var storeId = await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var r = await Write(Op(application, commandId, operation, notAfter, fingerprint(storeId), RxLane.Producer, 'd'), script, ct, args).ConfigureAwait(false);
        return r.HasValue ? r.Applied(read(r.Result!)) : ControlReject<T>(r);
    }

    // ------------------------------------------------------------------ Cancel/1, ManualRetry/1, RerunSucceeded/1

    public Task<StoreReply<ControlledJob>> CancelAsync(StoreCommand<CancelRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        return ControlValidateActor(r.Actor) is { } invalid
            ? Task.FromResult(ControlInvalid<ControlledJob>(invalid))
            : ControlAsync(command.Application, command.CommandId, Operations.Cancel, command.NotAfter,
                id => CommandEnvelopes.Fingerprint(id, command.Application, Operations.Cancel, command.CommandId, command.NotAfter, CommandEnvelopes.Cancel(r)),
                "cancel", ReceiptCodec.ReadControlled, ct, Hex(r.JobId), r.ExpectedGeneration, r.ExpectedRevision, ControlActorJson(r.Actor));
    }

    public Task<StoreReply<ControlledJob>> ManualRetryAsync(StoreCommand<RetryRequest> command, CancellationToken ct) =>
        RegenerateAsync(command, Operations.ManualRetry, ct);

    public Task<StoreReply<ControlledJob>> RerunSucceededAsync(StoreCommand<RetryRequest> command, CancellationToken ct) =>
        RegenerateAsync(command, Operations.RerunSucceeded, ct);

    Task<StoreReply<ControlledJob>> RegenerateAsync(StoreCommand<RetryRequest> command, string operation, CancellationToken ct)
    {
        var r = command.Body;
        return ControlValidateActor(r.Actor) is { } invalid
            ? Task.FromResult(ControlInvalid<ControlledJob>(invalid))
            : ControlAsync(command.Application, command.CommandId, operation, command.NotAfter,
                id => CommandEnvelopes.Fingerprint(id, command.Application, operation, command.CommandId, command.NotAfter, CommandEnvelopes.Retry(r)),
                "retry", ReceiptCodec.ReadControlled, ct, Hex(r.JobId), r.ExpectedGeneration, r.ExpectedRevision, ControlActorJson(r.Actor),
                operation == Operations.RerunSucceeded ? "1" : "0");
    }

    // ------------------------------------------------------------------ SetQueueState/1, EditScopePolicy/1

    public Task<StoreReply<QueueState>> SetQueueStateAsync(StoreCommand<QueueStateRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        return ControlValidateActor(r.Actor) is { } invalid
            ? Task.FromResult(ControlInvalid<QueueState>(invalid))
            : ControlAsync(command.Application, command.CommandId, Operations.SetQueueState, command.NotAfter,
                id => CommandEnvelopes.Fingerprint(id, command.Application, Operations.SetQueueState, command.CommandId, command.NotAfter, CommandEnvelopes.QueueState(r)),
                "queue_state", ReceiptCodec.ReadQueue, ct, r.Queue, r.Paused ? "1" : "0", r.ExpectedRevision, ControlActorJson(r.Actor));
    }

    public Task<StoreReply<ScopePolicy>> EditScopePolicyAsync(StoreCommand<ScopePolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ControlValidateActor(r.Actor)
            ?? (r.Capacity is < 0 or > MaxScopeCapacity ? "capacity" : null)
            ?? (r.ScopeKind is not (ScopeRef.QueueKind or ScopeRef.JobTypeKind or ScopeRef.ResourceKind) ? "scopeKind" : null)
            ?? (string.IsNullOrEmpty(r.ScopeKey) || Encoding.UTF8.GetByteCount(r.ScopeKey) > 512 ? "scopeKey" : null);
        return invalid is not null
            ? Task.FromResult(ControlInvalid<ScopePolicy>(invalid))
            : ControlAsync(command.Application, command.CommandId, Operations.EditScopePolicy, command.NotAfter,
                id => CommandEnvelopes.Fingerprint(id, command.Application, Operations.EditScopePolicy, command.CommandId, command.NotAfter, CommandEnvelopes.ScopePolicy(r)),
                "scope_policy", ReceiptCodec.ReadScope, ct, r.ScopeKind, r.ScopeKey, r.Capacity, r.ExpectedPolicyRevision, ControlActorJson(r.Actor));
    }

    // ------------------------------------------------------------------ ManagementDispatch/1 (19 §6.2)

    public async Task<StoreReply<ManagementDispatched>> ManagementDispatchAsync(StoreCommand<ManagementDispatchStoreRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ControlValidateActor(r.Actor) is { } invalid)
        {
            return ControlInvalid<ManagementDispatched>(invalid);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ManagementDispatch, command.CommandId, command.NotAfter,
            CommandEnvelopes.ManagementDispatch(r));
        var reply = await DispatchCallAsync(command.Application, command.CommandId, Operations.ManagementDispatch, command.NotAfter, fp, storeId, [r.Item],
            many: false, continueOnConflict: false, "initial", JsonNode.Parse(ControlActorJson(r.Actor))!.AsObject(), ct).ConfigureAwait(false);
        if (!reply.HasValue)
        {
            return reply.Reject<ManagementDispatched>();
        }

        var o = reply.Result!;
        return reply.Applied(new ManagementDispatched(ReceiptCodec.ReadDispatch(o["dispatch"]!), o["generation"]!.GetValue<int>(),
            o["revision"]!.GetValue<long>(), o["queue"]!.GetValue<string>()));
    }

    // ------------------------------------------------------------------ EventsSince/1 (08 §7.2, 22 §7.1)

    /// <summary>
    /// Event IDs are assigned inside the intent and a read never sees an unpublished one, so IDs appear in publication order:
    /// <c>event_id &gt; after</c> never skips a row. TxId is always 0 and Partial always false.
    /// </summary>
    public async Task<EventPage> EventsSinceAsync(string application, EventCursor? after, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 1000);
        var a = await ManagementReplyAsync(application, "events", [after?.EventId ?? 0L, limit], ct).ConfigureAwait(false);
        var events = new List<JobEvent>();
        for (var i = 3; i < a.Length; i++)
        {
            var o = JsonNode.Parse((string)a[i]!)!.AsObject();
            events.Add(new JobEvent(o["eventId"]!.GetValue<long>(), 0, 1, o["kind"]!.GetValue<string>(), OptGuid(o["jobId"]), OptGuid(o["attemptId"]),
                OptGuid(o["commandId"]), DateTimeOffset.FromUnixTimeMilliseconds(o["occurredAt"]!.GetValue<long>()), o["actorId"]?.GetValue<string>(),
                o["details"]!.AsObject()));
        }

        return new EventPage(events, events.Count > 0 ? events[^1].Cursor : after, false);
    }

    static Guid? OptGuid(JsonNode? n) => n is null ? null : Guid.Parse(n.GetValue<string>());
}
