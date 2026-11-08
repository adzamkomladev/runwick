using Runwick.Serialization;

namespace Runwick.Protocol;

/// <summary>Command fingerprints for the non-dispatch receipt-bearing operations (02 §8.3).</summary>
internal static class CommandEnvelopes
{
    public static byte[] Fingerprint(Guid storeId, string application, string operation, Guid commandId, DateTimeOffset notAfter, CanonicalValue body) =>
        Fingerprints.Digest(Fingerprints.CommandDomain,
            Fingerprints.Command(storeId, application, operation, 1, commandId, Fingerprints.FloorEpochMs(notAfter), body));

    public static CanonicalValue Lease(LeaseHandle h) => CanonicalValue.Object()
        .Add("attemptId", Fingerprints.Uuid(h.AttemptId))
        .Add("generation", h.Generation)
        .Add("jobId", Fingerprints.Uuid(h.JobId))
        .Add("ownershipToken", Fingerprints.Uuid(h.OwnershipToken))
        .Add("workerInstanceId", Fingerprints.Uuid(h.WorkerInstanceId))
        .Build();

    public static CanonicalValue Contract(JobContract c) => CanonicalValue.Object()
        .Add("name", c.Name)
        .Add("payloadVersion", c.PayloadVersion)
        .Add("serializerProfileId", c.SerializerProfileId)
        .Add("serializerProfileVersion", c.SerializerProfileVersion)
        .Build();

    public static CanonicalValue Reserve(ReserveRequest r) => CanonicalValue.Object()
        .Add("after", r.After is { } a
            ? CanonicalValue.Object().Add("eligibleAt", a.EligibleAt.ToUnixTimeMilliseconds()).Add("jobId", Fingerprints.Uuid(a.JobId)).Build()
            : CanonicalValue.Null)
        .Add("leaseDuration", Fingerprints.CeilMs(r.LeaseDuration))
        .Add("priority", r.Priority)
        .Add("queue", r.Queue)
        .Add("supportedContracts", CanonicalValue.Array(SortContracts(r.SupportedContracts).Select(Contract)))
        .Add("workerInstanceId", Fingerprints.Uuid(r.WorkerInstanceId))
        .Build();

    /// <summary>ReserveAndBegin/1: Reserve/1's members plus <c>maxJobs</c>. Tokens and jitter are drawn per call, outside it.</summary>
    public static CanonicalValue ReserveAndBegin(ReserveAndBeginRequest r) => CanonicalValue.Object()
        .Add("after", r.After is { } a
            ? CanonicalValue.Object().Add("eligibleAt", a.EligibleAt.ToUnixTimeMilliseconds()).Add("jobId", Fingerprints.Uuid(a.JobId)).Build()
            : CanonicalValue.Null)
        .Add("leaseDuration", Fingerprints.CeilMs(r.LeaseDuration))
        .Add("maxJobs", r.MaxJobs)
        .Add("priority", r.Priority)
        .Add("queue", r.Queue)
        .Add("supportedContracts", CanonicalValue.Array(SortContracts(r.SupportedContracts).Select(Contract)))
        .Add("workerInstanceId", Fingerprints.Uuid(r.WorkerInstanceId))
        .Build();

    public static CanonicalValue Begin(BeginRequest r) => CanonicalValue.Object().Add("lease", Lease(r.Lease)).Build();

    public static CanonicalValue Release(ReleaseRequest r) => CanonicalValue.Object().Add("lease", Lease(r.Lease)).Build();

    public static CanonicalValue Recover(RecoverRequest r) => CanonicalValue.Object()
        .Add("expectedAttemptId", Fingerprints.Uuid(r.ExpectedAttemptId))
        .Add("expectedOwnershipToken", Fingerprints.Uuid(r.ExpectedOwnershipToken))
        .Add("jobId", Fingerprints.Uuid(r.JobId))
        .Build();

    public static CanonicalValue Finish(FinishRequest r) => CanonicalValue.Object()
        .Add("error", r.Error is { } e
            ? CanonicalValue.Object().Add("code", e.Code).Add("exceptionType", e.ExceptionType).Add("message", e.Message).Add("stackTrace", e.StackTrace).Build()
            : CanonicalValue.Null)
        .Add("executionElapsed", Fingerprints.CeilMs(r.ExecutionElapsed))
        .Add("lease", Lease(r.Lease))
        .Add("outcome", r.Outcome.ToString())
        .Add("requestedRetryDelay", r.RequestedRetryDelay is { } d ? CanonicalValue.Int(Fingerprints.CeilMs(d)) : CanonicalValue.Null)
        .Build();

    public static CanonicalValue RegisterApplication() => CanonicalValue.Object().Build();

    public static CanonicalValue RegisterQueues(RegisterQueuesRequest r) => CanonicalValue.Object()
        .Add("queues", CanonicalValue.Array(r.Queues.Order(Internal.Utf8Order.Instance).Select(CanonicalValue.Str)))
        .Build();

    public static CanonicalValue RegisterScopes(RegisterScopesRequest r) => CanonicalValue.Object()
        .Add("scopes", CanonicalValue.Array(r.Scopes
            .OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance)
            .Select(s => CanonicalValue.Object().Add("capacity", s.Capacity).Add("key", s.Key).Add("kind", s.Kind).Build())))
        .Build();

    public static CanonicalValue SetQueueState(string queue, bool paused, long? expectedRevision) => CanonicalValue.Object()
        .Add("expectedRevision", expectedRevision is { } r ? CanonicalValue.Int(r) : CanonicalValue.Null)
        .Add("paused", paused)
        .Add("queue", queue)
        .Build();

    // ---- 08 §6 administrative bodies

    public static CanonicalValue Actor(ControlActor a) => CanonicalValue.Object()
        .Add("display", a.Display)
        .Add("reason", a.Reason)
        .Add("scheme", a.Scheme)
        .Add("subject", a.Subject)
        .Build();

    /// <summary>ManagementDispatch/1: the item's Dispatch/1 command digest (02 §8.3, over the same command ID and NotAfter) and the actor.</summary>
    public static CanonicalValue ManagementDispatch(ManagementDispatchStoreRequest r) => CanonicalValue.Object()
        .Add("actor", Actor(r.Actor))
        .Add("dispatch", Convert.ToHexStringLower(r.Item.Digests.Command))
        .Build();

    public static CanonicalValue Cancel(CancelRequest r) => JobControl(r.Actor, r.ExpectedGeneration, r.ExpectedRevision, r.JobId);

    public static CanonicalValue Retry(RetryRequest r) => JobControl(r.Actor, r.ExpectedGeneration, r.ExpectedRevision, r.JobId);

    static CanonicalValue JobControl(ControlActor actor, int generation, long revision, Guid jobId) => CanonicalValue.Object()
        .Add("actor", Actor(actor))
        .Add("expectedGeneration", generation)
        .Add("expectedRevision", revision)
        .Add("jobId", Fingerprints.Uuid(jobId))
        .Build();

    public static CanonicalValue QueueState(QueueStateRequest r) => CanonicalValue.Object()
        .Add("actor", Actor(r.Actor))
        .Add("expectedRevision", r.ExpectedRevision)
        .Add("paused", r.Paused)
        .Add("queue", r.Queue)
        .Build();

    public static CanonicalValue RatePolicy(RatePolicyRequest r) => CanonicalValue.Object()
        .Add("actor", Actor(r.Actor))
        .Add("expectedPolicyRevision", r.ExpectedPolicyRevision)
        .Add("limit", r.Limit)
        .Add("scopeKey", r.ScopeKey)
        .Add("scopeKind", r.ScopeKind)
        .Add("windowMs", r.WindowMs)
        .Build();

    public static CanonicalValue PoolOverride(PoolOverrideRequest r) => CanonicalValue.Object()
        .Add("actor", Actor(r.Actor))
        .Add("expectedRevision", r.ExpectedRevision)
        .Add("expiresAt", r.ExpiresAt is { } e ? CanonicalValue.Int(e.ToUnixTimeMilliseconds()) : CanonicalValue.Null)
        .Add("instanceId", Fingerprints.Uuid(r.InstanceId))
        .Add("pool", r.Pool)
        .Add("target", r.Target is { } t ? CanonicalValue.Int(t) : CanonicalValue.Null)
        .Build();

    public static CanonicalValue ActivateCapabilities(ActivateCapabilitiesRequest r)
    {
        var capabilities = CanonicalValue.Object();
        foreach (var (name, version) in r.Capabilities)
        {
            capabilities.Add(name, version);
        }

        return CanonicalValue.Object().Add("actor", Actor(r.Actor)).Add("capabilities", capabilities.Build()).Build();
    }

    public static CanonicalValue ScopePolicy(ScopePolicyRequest r) => CanonicalValue.Object()
        .Add("actor", Actor(r.Actor))
        .Add("capacity", r.Capacity)
        .Add("expectedPolicyRevision", r.ExpectedPolicyRevision)
        .Add("key", r.ScopeKey)
        .Add("kind", r.ScopeKind)
        .Build();

    public static CanonicalValue RetentionPolicy(RetentionPolicyRequest r)
    {
        var h = r.Policy.Hours();
        static CanonicalValue V(int? hours) => hours is { } v ? CanonicalValue.Int(v) : CanonicalValue.Null;
        return CanonicalValue.Object()
            .Add("actor", Actor(r.Actor))
            .Add("auditHours", V(h[3]))
            .Add("completedDetailHours", V(h[0]))
            .Add("expectedRevision", r.ExpectedRevision)
            .Add("failedDetailHours", V(h[1]))
            .Add("jobHistoryHours", V(h[2]))
            .Add("scalingEventsHours", V(h[4]))
            .Add("scheduleOccurrencesHours", V(h[5]))
            .Add("workerPresenceHours", V(h[6]))
            .Build();
    }

    /// <summary>Deduplicated, sorted by (name bytes, version, profile id bytes, profile version) — 02 IC-18.</summary>
    public static IReadOnlyList<JobContract> SortContracts(IEnumerable<JobContract> contracts) =>
        [.. contracts.Distinct()
            .OrderBy(c => c.Name, Internal.Utf8Order.Instance)
            .ThenBy(c => c.PayloadVersion)
            .ThenBy(c => c.SerializerProfileId, Internal.Utf8Order.Instance)
            .ThenBy(c => c.SerializerProfileVersion)];
}
