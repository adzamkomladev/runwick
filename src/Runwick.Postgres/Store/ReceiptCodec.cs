using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

/// <summary>Receipt result payloads, codec v1 (03 §8.5): camelCase, lowercase UUIDs, epoch-ms instants, ms durations.</summary>
internal static class ReceiptCodec
{
    static string U(Guid g) => g.ToString("D");

    static Guid G(JsonNode? n) => Guid.Parse(n!.GetValue<string>());

    static long Ms(DateTimeOffset t) => PgTime.Ms(t);

    static DateTimeOffset T(JsonNode? n) => PgTime.FromMs(n!.GetValue<long>());

    static DateTimeOffset? Tn(JsonNode? n) => n is null ? null : PgTime.FromMs(n.GetValue<long>());

    // Register*
    public static JsonObject Application(ApplicationSeedResult v) => new()
    {
        ["created"] = v.Created,
        ["revision"] = v.Gate.Revision,
        ["admissionMode"] = v.Gate.AdmissionMode,
    };

    public static ApplicationSeedResult ReadApplication(JsonObject o) =>
        new(o["created"]!.GetValue<bool>(), new ApplicationGate(o["revision"]!.GetValue<long>(), 1, new Dictionary<string, int>(), o["admissionMode"]!.GetValue<string>()));

    public static JsonObject Queues(QueueSeedResult v) => new()
    {
        ["queues"] = new JsonArray([.. v.Queues.Select(q => (JsonNode)new JsonObject
        {
            ["queue"] = q.Queue, ["created"] = q.Created, ["paused"] = q.Paused, ["revision"] = q.Revision,
        })]),
    };

    public static QueueSeedResult ReadQueues(JsonObject o) =>
        new([.. o["queues"]!.AsArray().Select(q => new QueueSeed(q!["queue"]!.GetValue<string>(), q["created"]!.GetValue<bool>(),
            q["paused"]!.GetValue<bool>(), q["revision"]!.GetValue<long>()))]);

    public static JsonObject Scopes(ScopeSeedResult v) => new()
    {
        ["scopes"] = new JsonArray([.. v.Scopes.Select(s => (JsonNode)new JsonObject
        {
            ["scopeId"] = U(s.ScopeId), ["kind"] = s.Kind, ["key"] = s.Key, ["created"] = s.Created,
            ["storedCapacity"] = s.StoredCapacity, ["configuredCapacity"] = s.ConfiguredCapacity,
            ["policyRevision"] = s.PolicyRevision, ["fencingEnabled"] = s.FencingEnabled,
        })]),
    };

    public static ScopeSeedResult ReadScopes(JsonObject o) =>
        new([.. o["scopes"]!.AsArray().Select(s => new ScopeSeed(G(s!["scopeId"]), s["kind"]!.GetValue<string>(), s["key"]!.GetValue<string>(),
            s["created"]!.GetValue<bool>(), s["storedCapacity"]!.GetValue<int>(), s["configuredCapacity"]!.GetValue<int>(),
            s["policyRevision"]!.GetValue<long>(), s["fencingEnabled"]!.GetValue<bool>()))]);

    // Dispatch
    public static JsonObject Dispatch(DispatchResult r) => new()
    {
        ["jobId"] = U(r.JobId),
        ["disposition"] = r.Disposition.ToString(),
        ["match"] = r.Match.ToString(),
        ["policyDrift"] = r.PolicyDrift,
    };

    public static DispatchResult ReadDispatch(JsonNode o) =>
        new(G(o["jobId"]), Enum.Parse<DispatchDisposition>(o["disposition"]!.GetValue<string>()),
            Enum.Parse<DuplicateMatch>(o["match"]!.GetValue<string>()), o["policyDrift"]!.GetValue<bool>());

    public static JsonObject DispatchMany(DispatchBatchResult r) => new()
    {
        ["items"] = new JsonArray([.. r.Items.Select(i =>
        {
            var o = new JsonObject { ["ordinal"] = i.Ordinal };
            if (i.Result is { } ok)
            {
                foreach (var (k, v) in Dispatch(ok))
                {
                    o[k] = v?.DeepClone();
                }
            }
            else if (i.Failure is { } f)
            {
                o["failureCode"] = f.Code;
                o["conflictingJobId"] = f.ConflictingJobId is { } c ? U(c) : null;
            }

            return (JsonNode)o;
        })]),
    };

    public static DispatchBatchResult ReadDispatchMany(JsonObject o) =>
        new([.. o["items"]!.AsArray().Select(i => i!["failureCode"] is { } code
            ? new DispatchBatchItemResult(i["ordinal"]!.GetValue<int>(), null,
                new StoreError(code.GetValue<string>(), ConflictingJobId: i["conflictingJobId"] is { } c ? G(c) : null))
            : new DispatchBatchItemResult(i["ordinal"]!.GetValue<int>(), ReadDispatch(i), null))]);

    // Reserve
    public static JsonObject Reserve(ReservedJob j) => new()
    {
        ["jobId"] = U(j.Position.JobId),
        ["generation"] = j.Position.Generation,
        ["revision"] = j.Position.Revision,
        ["state"] = j.Position.State,
        ["attemptId"] = U(j.Lease.Handle.AttemptId),
        ["workerInstanceId"] = U(j.Lease.Handle.WorkerInstanceId),
        ["ownershipToken"] = U(j.Lease.Handle.OwnershipToken),
        ["leaseExpiresAt"] = Ms(j.Lease.ExpiresAt),
        ["reservationNumber"] = j.ReservationNumber,
        ["contract"] = new JsonObject
        {
            ["name"] = j.Contract.Name, ["payloadVersion"] = j.Contract.PayloadVersion,
            ["serializerProfileId"] = j.Contract.SerializerProfileId, ["serializerProfileVersion"] = j.Contract.SerializerProfileVersion,
        },
        ["queue"] = j.Queue,
        ["priority"] = j.Priority,
        ["cursor"] = new JsonObject { ["eligibleAt"] = Ms(j.Cursor.EligibleAt), ["jobId"] = U(j.Cursor.JobId) },
        ["policy"] = Policy(j.Policy),
        ["fences"] = new JsonArray([.. j.Fences.Select(f => (JsonNode)new JsonObject { ["scopeId"] = U(f.ScopeId), ["value"] = f.Value })]),
    };

    /// <summary>Rebuilds a reservation from its receipt plus the immutable payload row.</summary>
    public static ReservedJob ReadReserve(JsonObject o, byte[] payload, IReadOnlyDictionary<string, string> business, IReadOnlyDictionary<string, string> diagnostic,
        DateTimeOffset createdAt)
    {
        var jobId = G(o["jobId"]);
        var generation = o["generation"]!.GetValue<int>();
        var c = o["contract"]!;
        return new ReservedJob(
            new JobPosition(jobId, generation, o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            new LeaseGrant(new LeaseHandle(jobId, G(o["attemptId"]), G(o["workerInstanceId"]), G(o["ownershipToken"]), generation), T(o["leaseExpiresAt"])),
            new JobContract(c["name"]!.GetValue<string>(), c["payloadVersion"]!.GetValue<int>(), c["serializerProfileId"]!.GetValue<string>(), c["serializerProfileVersion"]!.GetValue<int>()),
            new CandidateCursor(T(o["cursor"]!["eligibleAt"]), G(o["cursor"]!["jobId"])),
            o["reservationNumber"]!.GetValue<long>(),
            payload, business, diagnostic,
            o["queue"]!.GetValue<string>(), o["priority"]!.GetValue<string>(),
            ReadPolicy(o["policy"]!.AsObject()),
            [.. o["fences"]!.AsArray().Select(f => new StoreFence(G(f!["scopeId"]), f["value"]!.GetValue<long>()))],
            createdAt);
    }

    public static JsonObject Policy(ExecutionPolicy p) => new()
    {
        ["executionTimeout"] = p.ExecutionTimeoutMs,
        ["fingerprintVersion"] = 1,
        ["maxConsecutiveUnstartedAbandonments"] = p.MaxConsecutiveUnstartedAbandonments,
        ["maxExecutions"] = p.MaxExecutions,
        ["retry"] = Retry(p.Retry),
        ["unstartedRetry"] = Retry(p.UnstartedRetry),
    };

    public static JsonObject Retry(RetryPolicy r) => new()
    {
        ["backoff"] = r.Backoff, ["evaluatorVersion"] = r.EvaluatorVersion, ["initialDelay"] = r.InitialDelayMs,
        ["jitterKind"] = r.JitterKind, ["maximumDelay"] = r.MaximumDelayMs,
    };

    public static ExecutionPolicy ReadPolicy(JsonObject o) =>
        new(o["maxExecutions"]!.GetValue<int>(), o["executionTimeout"]?.GetValue<long>(), ReadRetry(o["retry"]!.AsObject()),
            o["maxConsecutiveUnstartedAbandonments"]!.GetValue<int>(), ReadRetry(o["unstartedRetry"]!.AsObject()));

    public static RetryPolicy ReadRetry(JsonObject o) =>
        new(o["backoff"]!.GetValue<string>(), o["evaluatorVersion"]!.GetValue<int>(), o["jitterKind"]!.GetValue<string>(),
            o["initialDelay"]!.GetValue<long>(), o["maximumDelay"]!.GetValue<long>());

    // Begin
    public static JsonObject Begin(BegunExecution b) => new()
    {
        ["jobId"] = U(b.Position.JobId),
        ["generation"] = b.Position.Generation,
        ["revision"] = b.Position.Revision,
        ["state"] = b.Position.State,
        ["attemptId"] = U(b.Lease.Handle.AttemptId),
        ["workerInstanceId"] = U(b.Lease.Handle.WorkerInstanceId),
        ["ownershipToken"] = U(b.Lease.Handle.OwnershipToken),
        ["leaseExpiresAt"] = Ms(b.Lease.ExpiresAt),
        ["executionNumber"] = b.ExecutionNumber,
        ["totalExecutionNumber"] = b.TotalExecutionNumber,
        ["authorizedAt"] = Ms(b.AuthorizedAt),
    };

    public static BegunExecution ReadBegin(JsonObject o)
    {
        var jobId = G(o["jobId"]);
        var generation = o["generation"]!.GetValue<int>();
        return new BegunExecution(
            new JobPosition(jobId, generation, o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            new LeaseGrant(new LeaseHandle(jobId, G(o["attemptId"]), G(o["workerInstanceId"]), G(o["ownershipToken"]), generation), T(o["leaseExpiresAt"])),
            o["executionNumber"]!.GetValue<int>(), o["totalExecutionNumber"]!.GetValue<long>(), T(o["authorizedAt"]));
    }

    public static JsonObject BeginClosed(BeginClosed c, string errorCode) => new()
    {
        ["jobId"] = U(c.Position.JobId),
        ["generation"] = c.Position.Generation,
        ["revision"] = c.Position.Revision,
        ["state"] = c.Position.State,
        ["attemptOutcome"] = c.AttemptOutcome,
        ["terminalReason"] = c.TerminalReason,
        ["errorCode"] = errorCode,
    };

    public static (BeginClosed Closed, string ErrorCode) ReadBeginClosed(JsonObject o) =>
        (new BeginClosed(new JobPosition(G(o["jobId"]), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            o["attemptOutcome"]!.GetValue<string>(), o["terminalReason"]!.GetValue<string>()), o["errorCode"]!.GetValue<string>());

    // Begin rate deferral (T20, 10 §4)
    public static JsonObject BeginDeferred(RateDeferral d) => new()
    {
        ["jobId"] = U(d.Position.JobId),
        ["generation"] = d.Position.Generation,
        ["revision"] = d.Position.Revision,
        ["state"] = d.Position.State,
        ["deferredUntil"] = Ms(d.EligibleAt),
        ["blockingScopes"] = new JsonArray([.. d.BlockingScopes.Select(s => (JsonNode)U(s))]),
    };

    public static RateDeferral ReadBeginDeferred(JsonObject o) =>
        new(new JobPosition(G(o["jobId"]), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            T(o["deferredUntil"]), [.. o["blockingScopes"]!.AsArray().Select(G)]);

    // Rate administration (10 §6)
    public static JsonObject Rate(RatePolicy p) => new()
    {
        ["rateScopeId"] = U(p.RateScopeId), ["scopeKind"] = p.ScopeKind, ["scopeKey"] = p.ScopeKey, ["limit"] = p.Limit,
        ["windowMs"] = p.WindowMs, ["policyRevision"] = p.PolicyRevision, ["notBefore"] = p.NotBefore is { } nb ? Ms(nb) : null,
        ["retainedGrants"] = p.RetainedGrants,
    };

    public static RatePolicy ReadRate(JsonObject o) =>
        new(G(o["rateScopeId"]), o["scopeKind"]!.GetValue<string>(), o["scopeKey"]!.GetValue<string>(), o["limit"]!.GetValue<int>(),
            o["windowMs"]!.GetValue<long>(), o["policyRevision"]!.GetValue<long>(), Tn(o["notBefore"]), o["retainedGrants"]!.GetValue<int>());

    // Pool overrides (11 §5)
    public static JsonObject Override(PoolOverride p) => new()
    {
        ["instanceId"] = U(p.InstanceId), ["pool"] = p.Pool, ["target"] = p.Target, ["revision"] = p.Revision,
        ["expiresAt"] = p.ExpiresAt is { } e ? Ms(e) : null,
    };

    public static PoolOverride ReadOverride(JsonObject o) =>
        new(G(o["instanceId"]), o["pool"]!.GetValue<string>(), o["target"]?.GetValue<int>(), o["revision"]!.GetValue<long>(), Tn(o["expiresAt"]));

    public static JsonObject Activated(ActivatedCapabilities a)
    {
        var required = new JsonObject();
        foreach (var (name, version) in a.RequiredCapabilities.OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            required[name] = version;
        }

        return new JsonObject { ["revision"] = a.Revision, ["requiredCapabilities"] = required };
    }

    public static ActivatedCapabilities ReadActivated(JsonObject o) =>
        new(o["revision"]!.GetValue<long>(),
            o["requiredCapabilities"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>(), StringComparer.Ordinal));

    // Finish / Recover
    public static JsonObject Finished(FinishedAttempt f, Guid attemptId, long? retryDelayMs) => new()
    {
        ["jobId"] = U(f.Position.JobId),
        ["generation"] = f.Position.Generation,
        ["revision"] = f.Position.Revision,
        ["state"] = f.Position.State,
        ["attemptId"] = U(attemptId),
        ["attemptOutcome"] = f.AttemptOutcome,
        ["disposition"] = f.Disposition,
        ["terminalReason"] = f.TerminalReason,
        ["eligibleAt"] = f.EligibleAt is { } e ? Ms(e) : null,
        ["retryDelay"] = retryDelayMs,
    };

    public static FinishedAttempt ReadFinished(JsonObject o) =>
        new(new JobPosition(G(o["jobId"]), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            o["attemptOutcome"]!.GetValue<string>(), o["disposition"]!.GetValue<string>(), o["terminalReason"]?.GetValue<string>(), Tn(o["eligibleAt"]));

    // ReleaseReservation
    public static JsonObject Released(ReleasedReservation r, Guid attemptId) => new()
    {
        ["jobId"] = U(r.Position.JobId),
        ["generation"] = r.Position.Generation,
        ["revision"] = r.Position.Revision,
        ["state"] = r.Position.State,
        ["attemptId"] = U(attemptId),
        ["disposition"] = r.Disposition,
        ["eligibleAt"] = r.EligibleAt is { } e ? Ms(e) : null,
    };

    public static ReleasedReservation ReadReleased(JsonObject o) =>
        new(new JobPosition(G(o["jobId"]), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            o["disposition"]!.GetValue<string>(), Tn(o["eligibleAt"]));

    // 08 §6 administrative receipts
    public static JsonObject Controlled(ControlledJob c) => new()
    {
        ["jobId"] = U(c.Position.JobId), ["generation"] = c.Position.Generation, ["revision"] = c.Position.Revision,
        ["state"] = c.Position.State, ["effect"] = c.Effect, ["terminalReason"] = c.TerminalReason,
        ["cancelRequestedAt"] = c.CancelRequestedAt is { } at ? Ms(at) : null,
    };

    public static ControlledJob ReadControlled(JsonObject o) =>
        new(new JobPosition(G(o["jobId"]), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["state"]!.GetValue<string>()),
            o["effect"]!.GetValue<string>(), o["terminalReason"]?.GetValue<string>(), Tn(o["cancelRequestedAt"]));

    public static JsonObject Queue(QueueState q) => new() { ["queue"] = q.Queue, ["paused"] = q.Paused, ["revision"] = q.Revision };

    public static QueueState ReadQueue(JsonObject o) =>
        new(o["queue"]!.GetValue<string>(), o["paused"]!.GetValue<bool>(), o["revision"]!.GetValue<long>());

    public static JsonObject Scope(ScopePolicy p) => new()
    {
        ["scopeId"] = U(p.ScopeId), ["kind"] = p.ScopeKind, ["key"] = p.ScopeKey, ["capacity"] = p.Capacity,
        ["policyRevision"] = p.PolicyRevision, ["livePermits"] = p.LivePermits,
    };

    public static ScopePolicy ReadScope(JsonObject o) =>
        new(G(o["scopeId"]), o["kind"]!.GetValue<string>(), o["key"]!.GetValue<string>(), o["capacity"]!.GetValue<int>(),
            o["policyRevision"]!.GetValue<long>(), o["livePermits"]!.GetValue<int>());
}
