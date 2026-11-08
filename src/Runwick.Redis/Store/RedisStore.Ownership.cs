using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Serialization;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

internal sealed partial class RedisStore : ILeaseBatchStore, IReserveAndBeginStore, IFinishManyStore
{
    // Dispatch, ownership and ResolveCommand (04 §7.2–§7.9, §7.13): argument building here, decisions in the Lua scripts.

    static readonly TimeSpan MinLease = TimeSpan.FromSeconds(1);
    static readonly TimeSpan MaxLease = TimeSpan.FromHours(1);
    const int MaxDeferJitterMs = 100;

    static string LeaseMs(TimeSpan d) => ((long)Math.Floor(d.TotalMilliseconds)).ToString(CultureInfo.InvariantCulture);

    static string Snake(AttemptOutcome outcome) => outcome switch
    {
        AttemptOutcome.Succeeded => "succeeded",
        AttemptOutcome.RetryableFailure => "retryable_failure",
        AttemptOutcome.PermanentFailure => "permanent_failure",
        AttemptOutcome.UserCancelled => "user_cancelled",
        AttemptOutcome.Interrupted => "interrupted",
        AttemptOutcome.TimedOut => "timed_out",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome)),
    };

    static long CeilMs(TimeSpan value) => (value.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;

    // ------------------------------------------------------------------ Dispatch/1, DispatchMany/1

    public async Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        var r = await DispatchCallAsync(command.Application, command.CommandId, Operations.Dispatch, command.NotAfter, command.Body.CommandFingerprint,
            command.Body.StoreId, [command.Body.Item], many: false, continueOnConflict: false, "initial", null, ct).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadDispatch(r.Result!)) : r.Reject<DispatchResult>();
    }

    public async Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct)
    {
        var r = await DispatchCallAsync(command.Application, command.CommandId, Operations.DispatchMany, command.NotAfter, command.Body.CommandFingerprint,
            command.Body.StoreId, command.Body.Items, many: true, command.Body.ContinueOnConflict, "initial", null, ct).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadDispatchMany(r.Result!)) : r.Reject<DispatchBatchResult>();
    }

    internal async Task<RxReply> DispatchCallAsync(string application, Guid commandId, string operation, DateTimeOffset notAfter, byte[] fingerprint,
        Guid? storeId, IReadOnlyList<DispatchItem> items, bool many, bool continueOnConflict, string createdReason, JsonObject? actor, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        if (InvalidDispatch(items) is { } invalid)
        {
            return new RxReply { Status = nameof(StoreStatus.InvalidRequest), HasValue = false, Code = "invalid_request", Detail = invalid, Decided = time.GetUtcNow() };
        }

        var args = new List<RedisValue>
        {
            continueOnConflict ? "1" : "0", many ? "1" : "0", createdReason, actor is null ? "" : Json(actor), items.Count,
        };
        foreach (var item in items)
        {
            args.Add(Json(ItemJson(item)));
            args.Add(item.Intent.PayloadUtf8);
        }

        return await Write(Op(application, commandId, operation, notAfter, fingerprint, RxLane.Producer, GateOf(operation), storeId), "dispatch", ct,
            [.. args]).ConfigureAwait(false);
    }

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

    static bool ValidRetry(RetryPolicy r) =>
        r.InitialDelayMs is >= 1 and <= 86_400_000 && r.MaximumDelayMs >= r.InitialDelayMs && r.MaximumDelayMs <= 86_400_000;

    static JsonObject PolicyJson(RetryPolicy r) => new()
    {
        ["backoff"] = r.Backoff, ["evaluatorVersion"] = r.EvaluatorVersion, ["initialDelay"] = r.InitialDelayMs, ["jitterKind"] = r.JitterKind,
        ["maximumDelay"] = r.MaximumDelayMs,
    };

    static JsonObject ItemJson(DispatchItem item)
    {
        var i = item.Intent;
        return new JsonObject
        {
            ["jobId"] = Hex(i.ProposedJobId), ["name"] = i.JobName, ["pver"] = i.PayloadVersion, ["profile"] = i.SerializerProfileId,
            ["profver"] = i.SerializerProfileVersion, ["queue"] = i.Queue, ["priority"] = i.Priority, ["notBefore"] = i.NotBeforeMs,
            ["akey"] = i.ActiveKey, ["wkey"] = i.WindowKey, ["wms"] = i.WindowMs,
            ["biz"] = Convert.ToHexStringLower(item.Digests.Business), ["pol"] = Convert.ToHexStringLower(item.Digests.Policy),
            ["headers"] = Headers(item),
            ["policy"] = new JsonObject
            {
                ["maxExec"] = i.Policy.MaxExecutions, ["maxUn"] = i.Policy.MaxConsecutiveUnstartedAbandonments, ["timeout"] = i.Policy.ExecutionTimeoutMs,
                ["retry"] = PolicyJson(i.Policy.Retry).ToJsonString(), ["uretry"] = PolicyJson(i.Policy.UnstartedRetry).ToJsonString(),
            },
            ["scopes"] = new JsonArray([.. item.Scopes.Select(s => (JsonNode?)new JsonObject
            {
                ["kind"] = s.Kind, ["key"] = s.Key, ["capacity"] = s.Capacity, ["id"] = Hex(Guid.CreateVersion7()),
            })]),
            ["rates"] = new JsonArray([.. item.Rates.Select(r => (JsonNode?)new JsonObject
            {
                ["kind"] = r.Kind, ["key"] = r.Key, ["limit"] = r.Limit, ["window"] = r.WindowMs, ["id"] = Hex(Guid.CreateVersion7()),
            })]),
        };
    }

    /// <summary>The payload row's headers: both metadata maps and, for a store-built scheduled job, its occurrence (0 = decision time).</summary>
    static string Headers(DispatchItem item)
    {
        static JsonObject Map(IReadOnlyDictionary<string, string> m)
        {
            var o = new JsonObject();
            foreach (var key in m.Keys.Order(StringComparer.Ordinal))
            {
                o[key] = m[key];
            }

            return o;
        }

        var headers = new JsonObject { ["business"] = Map(item.Intent.BusinessMetadata), ["diagnostic"] = Map(item.Intent.DiagnosticMetadata) };
        if (item.Schedule is { } s)
        {
            // A manual trigger (default times) takes the decision time; the script cannot rewrite headers, so the client's clock stands in.
            var manual = s.ScheduledAt == default;
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            headers["schedule"] = new JsonObject
            {
                ["key"] = s.ScheduleName,
                ["scheduledAt"] = manual ? now : s.ScheduledAt.ToUnixTimeMilliseconds(),
                ["coveredFrom"] = manual ? now : s.CoveredFrom.ToUnixTimeMilliseconds(),
                ["coveredUntil"] = manual ? now : s.CoveredUntil.ToUnixTimeMilliseconds(),
            };
        }

        return Json(headers);
    }

    // ------------------------------------------------------------------ Reserve/1

    public async Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512)
        {
            return Invalid<ReservedJob>("invalid_request", time);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.Reserve, command.CommandId, command.NotAfter, CommandEnvelopes.Reserve(request));
        var r = await Write(Op(command.Application, command.CommandId, Operations.Reserve, command.NotAfter, fp, RxLane.Admission, 'c'), "reserve", ct,
            Hex(request.WorkerInstanceId), request.Queue, request.Priority, LeaseMs(request.LeaseDuration), Hex(NewToken()),
            request.After is { } a ? Ms(a.EligibleAt) : "", request.After is { } b ? Hex(b.JobId) : "", Json(Contracts(contracts)), Entropy()).ConfigureAwait(false);
        if (!r.HasValue)
        {
            return r.Reject<ReservedJob>();
        }

        var id = Guid.Parse(r.Result!["jobId"]!.GetValue<string>());
        return r.Applied(ReadReserved(r.Result!, r.Payloads[id]));
    }

    // ------------------------------------------------------------------ Begin/1

    public async Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.Begin, command.CommandId, command.NotAfter, CommandEnvelopes.Begin(command.Body));
        var r = await Write(Op(command.Application, command.CommandId, Operations.Begin, command.NotAfter, fp, RxLane.Admission, 'c'), "begin", ct,
            Json(LeaseJson(command.Body.Lease)), Random.Shared.Next(0, MaxDeferJitterMs + 1)).ConfigureAwait(false);
        if (!r.HasValue)
        {
            return new BeginReply(r.Reject<BegunExecution>(), null);
        }

        var o = r.Result!;
        if (o.ContainsKey("deferredUntil"))
        {
            var reply = r.Applied<BegunExecution>(null);
            return new BeginReply(reply, null) { Deferred = reply.Status == StoreStatus.RateLimited ? ReceiptCodec.ReadBeginDeferred(o) : null };
        }

        if (o.ContainsKey("attemptOutcome"))
        {
            return new BeginReply(r.Applied<BegunExecution>(null), ReceiptCodec.ReadBeginClosed(o).Closed);
        }

        return new BeginReply(r.Applied(ReceiptCodec.ReadBegin(o)), null);
    }

    // ------------------------------------------------------------------ Renew/1, RenewMany/1

    public async Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease)
        {
            return Invalid<RenewedLease>("invalid_request", time);
        }

        await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var r = await Write(Op(command.Application, command.CommandId, Operations.Renew, command.NotAfter, null, RxLane.Renewal, 'd'), "renew", ct,
            LeaseMs(command.Body.LeaseDuration), "0", Json(new JsonArray(LeaseJson(lease)))).ConfigureAwait(false);
        return r.HasValue
            ? r.Applied(new RenewedLease(new LeaseGrant(lease, DateTimeOffset.FromUnixTimeMilliseconds(r.Result!["leaseExpiresAt"]!.GetValue<long>())),
                r.Result!["cancelRequested"]!.GetValue<bool>()))
            : r.Reject<RenewedLease>();
    }

    public async Task<StoreReply<RenewedLeases>> RenewManyAsync(StoreCommand<RenewManyRequest> command, CancellationToken ct)
    {
        var leases = command.Body.Leases;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease
            || leases.Count is 0 or > ILeaseBatchStore.MaxLeases
            || leases.Select(l => l.JobId).Distinct().Count() != leases.Count)
        {
            return Invalid<RenewedLeases>("invalid_request", time);
        }

        await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var r = await Write(Op(command.Application, command.CommandId, Operations.RenewMany, command.NotAfter, null, RxLane.Renewal, 'd'), "renew", ct,
            LeaseMs(command.Body.LeaseDuration), "1", Json(new JsonArray([.. leases.Select(l => (JsonNode?)LeaseJson(l))]))).ConfigureAwait(false);
        if (!r.HasValue)
        {
            return r.Reject<RenewedLeases>();
        }

        var items = r.Result!["items"]!.AsArray();
        var outcomes = new LeaseRenewal[leases.Count];
        for (var i = 0; i < leases.Count; i++)
        {
            var o = items[i]!;
            var status = Enum.Parse<StoreStatus>(o["status"]!.GetValue<string>());
            outcomes[i] = status == StoreStatus.Applied
                ? new LeaseRenewal(status, null, new RenewedLease(
                    new LeaseGrant(leases[i], DateTimeOffset.FromUnixTimeMilliseconds(o["leaseExpiresAt"]!.GetValue<long>())), o["cancelRequested"]!.GetValue<bool>()))
                : new LeaseRenewal(status, o["code"]!.GetValue<string>(), null);
        }

        return r.Applied(new RenewedLeases(outcomes));
    }

    // ------------------------------------------------------------------ ReserveAndBegin/1

    public async Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(StoreCommand<ReserveAndBeginRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512
            || request.MaxJobs is < 1 or > IReserveAndBeginStore.MaxJobs)
        {
            return Invalid<ClaimedJobs>("invalid_request", time);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ReserveAndBegin, command.CommandId, command.NotAfter,
            CommandEnvelopes.ReserveAndBegin(request));
        var n = request.MaxJobs;
        var tokens = new JsonArray([.. Enumerable.Range(0, n).Select(_ => (JsonNode?)Hex(NewToken()))]);
        var entropy = new JsonArray([.. Enumerable.Range(0, n).Select(_ => (JsonNode?)Entropy())]);
        var jitters = new JsonArray([.. Enumerable.Range(0, n).Select(_ => (JsonNode?)Random.Shared.Next(0, MaxDeferJitterMs + 1))]);
        var r = await Write(Op(command.Application, command.CommandId, Operations.ReserveAndBegin, command.NotAfter, fp, RxLane.Admission, 'c'),
            "reserve_and_begin", ct,
            Hex(request.WorkerInstanceId), request.Queue, request.Priority, LeaseMs(request.LeaseDuration), Json(tokens),
            request.After is { } a ? Ms(a.EligibleAt) : "", request.After is { } b ? Hex(b.JobId) : "", Json(Contracts(contracts)), n, Json(entropy),
            Json(jitters)).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReadClaimed(r)) : r.Reject<ClaimedJobs>();
    }

    static ClaimedJobs ReadClaimed(RxReply r)
    {
        var outcomes = new List<ClaimOutcome>();
        foreach (var node in r.Result!["items"]!.AsArray())
        {
            var item = node!.AsObject();
            var jobId = Guid.Parse(item["jobId"]!.GetValue<string>());
            var status = Enum.Parse<StoreStatus>(item["status"]!.GetValue<string>());
            var code = item["code"]?.GetValue<string>();
            switch (status)
            {
                case StoreStatus.CapacityBlocked:
                    var cursor = item["cursor"]!.AsObject();
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? "capacity_blocked",
                        BlockedScopeId: Guid.Parse(item["blockedScopeId"]!.GetValue<string>()),
                        ResumeAfter: new CandidateCursor(DateTimeOffset.FromUnixTimeMilliseconds(cursor["eligibleAt"]!.GetValue<long>()),
                            Guid.Parse(cursor["jobId"]!.GetValue<string>()))),
                        null, null, null, null));
                    break;
                case StoreStatus.Applied:
                    outcomes.Add(new ClaimOutcome(jobId, status, null, ReadReserved(item["reserve"]!.AsObject(), r.Payloads[jobId]),
                        ReceiptCodec.ReadBegin(item["begin"]!.AsObject()), null, null));
                    break;
                case StoreStatus.RateLimited:
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? "rate_limited",
                        Detail: item["begin"]!["deferredUntil"]!.GetValue<long>().ToString(CultureInfo.InvariantCulture)),
                        null, null, null, ReceiptCodec.ReadBeginDeferred(item["begin"]!.AsObject())));
                    break;
                case StoreStatus.InvalidState or StoreStatus.BudgetExhausted:
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? item["begin"]!["errorCode"]!.GetValue<string>()),
                        null, null, ReceiptCodec.ReadBeginClosed(item["begin"]!.AsObject()).Closed, null));
                    break;
                default:
                    throw new InvalidOperationException($"ReserveAndBegin item status {status} is not an outcome.");
            }
        }

        return new ClaimedJobs(outcomes);
    }

    // ------------------------------------------------------------------ Finish/1, FinishMany/1

    static JsonObject FinishBody(FinishRequest request) => new()
    {
        ["lease"] = LeaseJson(request.Lease), ["outcome"] = Snake(request.Outcome),
        ["elapsed"] = (long)Math.Max(0, request.ExecutionElapsed.TotalMilliseconds),
        ["requested"] = request.RequestedRetryDelay is { } d ? CeilMs(d) : null,
        ["error"] = request.Error is { } e
            ? new JsonObject { ["code"] = e.Code, ["exceptionType"] = e.ExceptionType, ["message"] = e.Message, ["stackTrace"] = e.StackTrace }.ToJsonString()
            : null,
    };

    byte[] FinishFingerprint(Guid storeId, StoreCommand<FinishRequest> command) =>
        CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.Finish, command.CommandId, command.NotAfter, CommandEnvelopes.Finish(command.Body));

    public async Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct)
    {
        if (command.Body.RequestedRetryDelay is { } requested && requested < TimeSpan.Zero)
        {
            return Invalid<FinishedAttempt>("requested_delay_invalid", time);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var r = await Write(Op(command.Application, command.CommandId, Operations.Finish, command.NotAfter, FinishFingerprint(storeId, command), RxLane.Settlement, 'd'),
            "finish", ct, Json(FinishBody(command.Body)), Draw()).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadFinished(r.Result!)) : r.Reject<FinishedAttempt>();
    }

    public async Task<StoreReply<FinishedMany>> FinishManyAsync(StoreCommand<FinishManyRequest> command, CancellationToken ct)
    {
        var items = command.Body.Items;
        if (items.Count is 0 or > IFinishManyStore.MaxItems
            || items.Any(i => i.Application != command.Application || i.Body.RequestedRetryDelay < TimeSpan.Zero)
            || items.Select(i => i.CommandId).Distinct().Count() != items.Count
            || items.Select(i => i.Body.Lease.JobId).Distinct().Count() != items.Count)
        {
            return Invalid<FinishedMany>("invalid_request", time);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var args = new List<RedisValue> { items.Count };
        foreach (var item in items)
        {
            args.Add(Hex(item.CommandId));
            args.Add(Convert.ToHexStringLower(FinishFingerprint(storeId, item)));
            args.Add(Ms(item.NotAfter));
            args.Add(Json(FinishBody(item.Body)));
            args.Add(Draw());
        }

        var r = await Write(Op(command.Application, command.CommandId, Operations.FinishMany, command.NotAfter, null, RxLane.Settlement, 'd'), "finish_many", ct,
            [.. args]).ConfigureAwait(false);
        if (!r.HasValue)
        {
            return r.Reject<FinishedMany>();
        }

        var outcomes = new List<StoreReply<FinishedAttempt>>();
        foreach (var node in r.Result!["items"]!.AsArray())
        {
            var item = node!.AsObject();
            var status = Enum.Parse<StoreStatus>(item["status"]!.GetValue<string>());
            var applied = item["effect"]!.GetValue<string>() == nameof(StoreEffect.Applied);
            var decided = item["decidedAt"] is { } ms ? DateTimeOffset.FromUnixTimeMilliseconds(ms.GetValue<long>()) : default;
            var code = item["code"]?.GetValue<string>();
            outcomes.Add(applied
                ? new StoreReply<FinishedAttempt>(status, StoreEffect.Applied, item["replayed"]!.GetValue<bool>(),
                    ReceiptCodec.ReadFinished(item["result"]!.AsObject()), code is null ? null : new StoreError(code), decided)
                : StoreReply<FinishedAttempt>.NoEffect(status, new StoreError(code ?? "invariant_fault"), decided));
        }

        return r.Applied(new FinishedMany(outcomes));
    }

    // ------------------------------------------------------------------ Recover/1, ReleaseReservation/1

    public async Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.Recover, command.CommandId, command.NotAfter, CommandEnvelopes.Recover(request));
        var r = await Write(Op(command.Application, command.CommandId, Operations.Recover, command.NotAfter, fp, RxLane.Background, 'd'), "recover", ct,
            Hex(request.JobId), Hex(request.ExpectedAttemptId), Hex(request.ExpectedOwnershipToken), Draw()).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadFinished(r.Result!)) : r.Reject<FinishedAttempt>();
    }

    public async Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ReleaseReservation, command.CommandId, command.NotAfter,
            CommandEnvelopes.Release(command.Body));
        var r = await Write(Op(command.Application, command.CommandId, Operations.ReleaseReservation, command.NotAfter, fp, RxLane.Settlement, 'd'), "release", ct,
            Json(LeaseJson(command.Body.Lease))).ConfigureAwait(false);
        return r.HasValue ? r.Applied(ReceiptCodec.ReadReleased(r.Result!)) : r.Reject<ReleasedReservation>();
    }

    // ------------------------------------------------------------------ ResolveCommand/1 (04 §7.13, 22 §4.5)

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) =>
        ResolveCoreAsync(command.Application, command.CommandId, command.Operation, command.Fingerprint, command.NotAfter, ct);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        ResolveCoreAsync(application, commandId, null, null, notAfterBound, ct);

    async Task<ReceiptResolution> ResolveCoreAsync(string application, Guid commandId, string? operation, byte[]? fingerprint, DateTimeOffset notAfter,
        CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var floor = operation is null ? TimeSpan.FromHours(24) : Operations.RetentionFloor(operation);
        var frame = RxClient.ReadFrame(storeId, []);
        frame[2] = operation ?? "";
        frame[3] = Hex(commandId);
        frame[4] = fingerprint is null ? "" : Convert.ToHexStringLower(fingerprint);
        frame[5] = Ms(notAfter);
        frame[7] = (long)floor.TotalMilliseconds;
        for (var attempt = 0; ; attempt++)
        {
            var a = await runner.Client.EvalAsync(RxLane.Producer, RxScript.Write("resolve"), RxClient.Meta(application), frame, null, ct).ConfigureAwait(false);
            switch ((string)a[0]!)
            {
                case "RECOVERING":
                    if (attempt < 4 && await runner.RecoverAsync(application, "", RxLane.Producer, ct).ConfigureAwait(false))
                    {
                        continue;
                    }

                    throw new StoreNotAppliedException("store_recovering");
                case "NONE":
                    if ((string)a[1]! == "store_identity_mismatch")
                    {
                        throw new StoreIncompatibleException("store_identity_mismatch");
                    }

                    return time.GetUtcNow() < notAfter + floor ? new ReceiptResolution.NotApplied() : new ReceiptResolution.ExpiredUnknown();
                case "Conflict":
                    return new ReceiptResolution.Conflict();
                case "NotObserved":
                    return new ReceiptResolution.NotObserved();
                case "NotApplied":
                    return new ReceiptResolution.NotApplied();
                case "ExpiredUnknown":
                    return new ReceiptResolution.ExpiredUnknown();
                case "Recorded":
                    return new ReceiptResolution.Recorded((string)a[1]!, int.Parse((string)a[2]!, CultureInfo.InvariantCulture), (string)a[3]!,
                        RxReply.T((string)a[4]!), JsonDocument.Parse((string)a[5]!).RootElement.Clone());
                default:
                    throw new StoreOutcomeUnknownException("commit_outcome_unknown", commandId, "ResolveCommand");
            }
        }
    }
}
