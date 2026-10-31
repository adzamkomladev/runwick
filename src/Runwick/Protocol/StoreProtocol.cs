using System.Text.Json;
using Runwick.Serialization;

namespace Runwick.Protocol;

// Driver-neutral storage contract (spec 03 §2–§3). Internal; not NuGet API.
// Policy snapshots reuse Serialization.ExecutionPolicy/RetryPolicy (whole milliseconds), which carry
// exactly the fields of spec 03 §2.3 ExecutionPolicySnapshot/RetryPolicySnapshot.

internal sealed record StoreCommand<T>(string Application, Guid CommandId, DateTimeOffset NotAfter, T Body);

internal enum StoreStatus
{
    Applied, NoWork, QueuePaused, CapacityBlocked, RateLimited,
    StaleLease, InvalidState, BudgetExhausted,
    VersionConflict, UniquenessConflict, NotFound,
    CommandExpired, CommandConflict, UnsupportedVersion,
    AdmissionClosed, HistoryUnavailable,
    InvalidRequest, InvariantFault,
}

internal enum StoreEffect
{
    ObservedNoEffect,
    Applied,
}

internal sealed record StoreError(
    string Code,
    long? ActualRevision = null,
    Guid? ConflictingJobId = null,
    Guid? BlockedScopeId = null,
    CandidateCursor? ResumeAfter = null,
    string? Detail = null);

internal sealed record StoreReply<T>(
    StoreStatus Status,
    StoreEffect Effect,
    bool Replayed,
    T? Value,
    StoreError? Error,
    DateTimeOffset DecidedAt) where T : class
{
    public static StoreReply<T> Applied(T value, DateTimeOffset decidedAt, bool replayed = false) =>
        new(StoreStatus.Applied, StoreEffect.Applied, replayed, value, null, decidedAt);

    public static StoreReply<T> NoEffect(StoreStatus status, StoreError error, DateTimeOffset decidedAt) =>
        new(status, StoreEffect.ObservedNoEffect, false, null, error, decidedAt);

    public override string ToString() => $"{Status}/{Effect}{(Replayed ? " (replayed)" : "")} {Error?.Code}";
}

internal sealed record JobContract(string Name, int PayloadVersion, string SerializerProfileId, int SerializerProfileVersion);

internal sealed record CandidateCursor(DateTimeOffset EligibleAt, Guid JobId);

internal sealed record LeaseHandle(Guid JobId, Guid AttemptId, Guid WorkerInstanceId, Guid OwnershipToken, int Generation);

internal sealed record LeaseGrant(LeaseHandle Handle, DateTimeOffset ExpiresAt);

internal sealed record StoreFence(Guid ScopeId, long Value);

internal sealed record JobPosition(Guid JobId, int Generation, long Revision, string State);

/// <summary>Scope membership plus the capacity used only to seed an absent scope.</summary>
internal sealed record ScopeRequirement(string Kind, string Key, int Capacity);

internal static class JobStates
{
    public const string Pending = "pending";
    public const string Reserved = "reserved";
    public const string Running = "running";
    public const string Retrying = "retrying";
    public const string Succeeded = "succeeded";
    public const string Failed = "failed";
    public const string Cancelled = "cancelled";

    public static bool IsTerminal(string state) => state is Succeeded or Failed or Cancelled;
}

// ---- 3.1 handshake

internal sealed record StoreDescriptor(
    Guid StoreId,
    long SchemaRevision,
    int ProtocolEpoch,
    string MigrationMode,
    int CompatibilityManifestVersion,
    ReadOnlyMemory<byte> ManifestDigest,
    ApplicationGate? Application,
    DateTimeOffset ObservedAt);

internal sealed record ApplicationGate(
    long Revision,
    int RequiredProtocolEpoch,
    IReadOnlyDictionary<string, int> RequiredCapabilities,
    string AdmissionMode);

internal sealed record RegisterApplicationRequest;

internal sealed record ApplicationSeedResult(bool Created, ApplicationGate Gate);

internal sealed record RegisterQueuesRequest(IReadOnlyList<string> Queues);

internal sealed record QueueSeedResult(IReadOnlyList<QueueSeed> Queues);

internal sealed record QueueSeed(string Queue, bool Created, bool Paused, long Revision);

internal sealed record RegisterScopesRequest(IReadOnlyList<ScopeRequirement> Scopes);

internal sealed record ScopeSeedResult(IReadOnlyList<ScopeSeed> Scopes);

internal sealed record ScopeSeed(
    Guid ScopeId, string Kind, string Key, bool Created, int StoredCapacity, int ConfiguredCapacity,
    long PolicyRevision, bool FencingEnabled);

internal interface IStoreHandshake
{
    Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct);

    Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct);

    Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct);

    Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct);
}

// ---- 3.2 dispatch

/// <summary>One frozen dispatch item: the Prepare intent, its digests and seed capacities.</summary>
internal sealed record DispatchItem(DispatchIntent Intent, IReadOnlyList<ScopeRequirement> Scopes, DispatchDigests Digests)
{
    public JobContract Contract => new(Intent.JobName, Intent.PayloadVersion, Intent.SerializerProfileId, Intent.SerializerProfileVersion);

    /// <summary>Seed policies for <see cref="DispatchIntent.RateScopes"/>, in the same order (10 §3.1). Not fingerprinted.</summary>
    public IReadOnlyList<RateRequirement> Rates { get; init; } = [];
}

/// <summary>A rate scope a dispatch requires, with the seed policy used only if the scope does not exist yet.</summary>
internal sealed record RateRequirement(string Kind, string Key, int Limit, long WindowMs);

/// <summary>Dispatch/1 body; <see cref="CommandFingerprint"/> is the 02 §8.3 digest computed at Prepare.</summary>
internal sealed record DispatchRequest(DispatchItem Item, Guid StoreId, byte[] CommandFingerprint);

internal sealed record DispatchBatchRequest(IReadOnlyList<DispatchItem> Items, bool ContinueOnConflict, Guid StoreId, byte[] CommandFingerprint);

internal sealed record DispatchResult(Guid JobId, DispatchDisposition Disposition, DuplicateMatch Match, bool PolicyDrift);

internal sealed record DispatchBatchItemResult(int Ordinal, DispatchResult? Result, StoreError? Failure);

internal sealed record DispatchBatchResult(IReadOnlyList<DispatchBatchItemResult> Items);

internal sealed record CommandIdentity(
    string Application, Guid CommandId, string Operation, int OperationVersion, byte[] Fingerprint, DateTimeOffset NotAfter);

internal interface IJobDispatchStore
{
    Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct);

    Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct);

    Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct);

    /// <summary>Operation-ID-only lookup used by <c>ResolveAsync</c> when no prepared request is at hand.</summary>
    /// <remarks><c>notAfterBound</c> is the latest NotAfter the operation can have had (UUIDv7 time + deadline cap).</remarks>
    Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct);
}

// ---- 3.3 reservation

internal sealed record ReserveRequest(
    Guid WorkerInstanceId,
    string Queue,
    string Priority,
    IReadOnlyList<JobContract> SupportedContracts,
    CandidateCursor? After,
    TimeSpan LeaseDuration);

internal sealed record ReservedJob(
    JobPosition Position,
    LeaseGrant Lease,
    JobContract Contract,
    CandidateCursor Cursor,
    long ReservationNumber,
    byte[] PayloadUtf8,
    IReadOnlyDictionary<string, string> BusinessMetadata,
    IReadOnlyDictionary<string, string> DiagnosticMetadata,
    string Queue,
    string Priority,
    ExecutionPolicy Policy,
    IReadOnlyList<StoreFence> Fences,
    DateTimeOffset CreatedAt);

internal sealed record BeginRequest(LeaseHandle Lease);

internal sealed record BegunExecution(
    JobPosition Position,
    LeaseGrant Lease,
    int ExecutionNumber,
    long TotalExecutionNumber,
    DateTimeOffset AuthorizedAt);

/// <summary>Value of an Applied Begin that closed the attempt (cancelled before start / budget exhausted).</summary>
internal sealed record BeginClosed(JobPosition Position, string AttemptOutcome, string TerminalReason);

internal sealed record RenewRequest(LeaseHandle Lease, TimeSpan LeaseDuration);

internal sealed record RenewedLease(LeaseGrant Lease, bool CancellationRequested);

internal interface IJobReservationStore
{
    Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct);

    /// <summary>Applied with <see cref="BegunExecution"/>, or Applied InvalidState/BudgetExhausted with <see cref="BeginClosed"/> in <c>Closed</c>.</summary>
    Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct);

    Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct);
}

/// <summary>Begin reply: the protocol reply plus the closing value when Begin closed the attempt (spec 03 §2.1 table).</summary>
internal sealed record BeginReply(StoreReply<BegunExecution> Reply, BeginClosed? Closed)
{
    /// <summary>Set with <see cref="StoreStatus.RateLimited"/>: the committed T20 deferral (10 §4).</summary>
    public RateDeferral? Deferred { get; init; }
}

/// <summary>
/// A committed rate deferral (T20): the attempt closed <c>admission_deferred</c>, the job back in its waiting state
/// until <see cref="EligibleAt"/>. <see cref="BlockingScopes"/> are the scopes that had no capacity, ascending.
/// </summary>
internal sealed record RateDeferral(JobPosition Position, DateTimeOffset EligibleAt, IReadOnlyList<Guid> BlockingScopes);

// ---- 3.4 state

internal enum AttemptOutcome
{
    Succeeded, RetryableFailure, PermanentFailure,
    UserCancelled, Interrupted, TimedOut,
}

internal sealed record AttemptError(string Code, string ExceptionType, string Message, string? StackTrace);

internal sealed record FinishRequest(
    LeaseHandle Lease,
    AttemptOutcome Outcome,
    TimeSpan ExecutionElapsed,
    TimeSpan? RequestedRetryDelay,
    AttemptError? Error);

internal sealed record FinishedAttempt(
    JobPosition Position,
    string AttemptOutcome,
    string Disposition,
    string? TerminalReason,
    DateTimeOffset? EligibleAt);

internal sealed record RecoverRequest(Guid JobId, Guid ExpectedAttemptId, Guid ExpectedOwnershipToken);

internal sealed record ReleaseRequest(LeaseHandle Lease);

internal sealed record ReleasedReservation(JobPosition Position, string Disposition, DateTimeOffset? EligibleAt);

internal interface IJobStateStore
{
    Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct);

    Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct);

    Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct);
}

// ---- 08 §3 revisioned controls (M3, SP-011); a separate interface so drivers without controls need no stubs

/// <summary>The host-supplied trusted actor plus the request's reason (08 §2).</summary>
internal sealed record ControlActor(string Subject, string? Display, string Scheme, string? Reason);

internal sealed record CancelRequest(Guid JobId, int ExpectedGeneration, long ExpectedRevision, ControlActor Actor);

internal sealed record RetryRequest(Guid JobId, int ExpectedGeneration, long ExpectedRevision, ControlActor Actor);

internal sealed record QueueStateRequest(string Queue, bool Paused, long ExpectedRevision, ControlActor Actor);

internal sealed record ScopePolicyRequest(string ScopeKind, string ScopeKey, int Capacity, long ExpectedPolicyRevision, ControlActor Actor);

/// <summary>Effect: <c>cancelled</c> (T17), <c>cancel_requested</c> (T22), <c>retried</c> (T18) or <c>rerun</c> (T19).</summary>
internal sealed record ControlledJob(JobPosition Position, string Effect, string? TerminalReason, DateTimeOffset? CancelRequestedAt);

internal sealed record QueueState(string Queue, bool Paused, long Revision);

internal sealed record ScopePolicy(Guid ScopeId, string ScopeKind, string ScopeKey, int Capacity, long PolicyRevision, int LivePermits);

/// <summary>Commit-safe keyset position (08 §7): transaction ID, then event ID.</summary>
internal readonly record struct EventCursor(ulong TxId, long EventId);

internal sealed record JobEvent(
    long EventId, ulong TxId, int SchemaVersion, string Kind, Guid? JobId, Guid? AttemptId, Guid? CommandId,
    DateTimeOffset OccurredAt, string? ActorId, System.Text.Json.Nodes.JsonObject Details)
{
    public EventCursor Cursor => new(TxId, EventId);
}

internal sealed record EventPage(IReadOnlyList<JobEvent> Events, EventCursor? Next, bool Partial);

internal interface IJobControlStore
{
    Task<StoreReply<ControlledJob>> CancelAsync(StoreCommand<CancelRequest> command, CancellationToken ct);

    Task<StoreReply<ControlledJob>> ManualRetryAsync(StoreCommand<RetryRequest> command, CancellationToken ct);

    Task<StoreReply<ControlledJob>> RerunSucceededAsync(StoreCommand<RetryRequest> command, CancellationToken ct);

    Task<StoreReply<QueueState>> SetQueueStateAsync(StoreCommand<QueueStateRequest> command, CancellationToken ct);

    Task<StoreReply<ScopePolicy>> EditScopePolicyAsync(StoreCommand<ScopePolicyRequest> command, CancellationToken ct);

    Task<EventPage> EventsSinceAsync(string application, EventCursor? after, int limit, CancellationToken ct);
}

/// <summary>EditRatePolicy/1 (10 §6.2): count and window of one existing rate scope, under its policy revision.</summary>
internal sealed record RatePolicyRequest(string ScopeKind, string ScopeKey, int Limit, long WindowMs, long ExpectedPolicyRevision, ControlActor Actor);

/// <summary>
/// A rate scope's policy after an edit. <see cref="NotBefore"/> is the end of the edit freeze in the scope's logical
/// time (10 §6.3); <see cref="RetainedGrants"/> is the grant count inside the new window when the edit committed.
/// </summary>
internal sealed record RatePolicy(
    Guid RateScopeId, string ScopeKind, string ScopeKey, int Limit, long WindowMs, long PolicyRevision, DateTimeOffset? NotBefore, int RetainedGrants);

/// <summary>ActivateCapabilities/1 (10 §6.1): raise the application's required capabilities; never lowers one.</summary>
internal sealed record ActivateCapabilitiesRequest(IReadOnlyDictionary<string, int> Capabilities, ControlActor Actor);

internal sealed record ActivatedCapabilities(long Revision, IReadOnlyDictionary<string, int> RequiredCapabilities);

/// <summary>Rate administration (10 §6). Separate from <see cref="IJobControlStore"/>; PostgreSQL only in M3.</summary>
internal interface IRateControlStore
{
    Task<StoreReply<ActivatedCapabilities>> ActivateCapabilitiesAsync(StoreCommand<ActivateCapabilitiesRequest> command, CancellationToken ct);

    Task<StoreReply<RatePolicy>> EditRatePolicyAsync(StoreCommand<RatePolicyRequest> command, CancellationToken ct);
}

// ---- 3.5 readiness

internal sealed record LaneKey(string Queue, string Priority);

internal sealed record ReadyLane(LaneKey Lane, DateTimeOffset OldestEligibleAt);

/// <summary>
/// ReadinessProbe/2 queue row (09 §2): control revision and pause flag of a requested queue, and the queue scope's
/// live permit count bounded by its capacity. A hint only; Reserve stays authoritative.
/// </summary>
internal sealed record QueueReadiness(string Queue, bool Paused, long ControlRevision, Guid? QueueScopeId, int? Capacity, int? Live)
{
    public bool Saturated => Capacity is { } capacity && Live >= capacity;
}

/// <summary>
/// A probed hint. <see cref="Lanes"/> holds only lanes with a due supported job. Probe/2 adds <see cref="Queues"/> and
/// <see cref="UnsupportedLanes"/> (lanes whose oldest due job has a tuple this worker does not support); a driver
/// without probe/2 leaves both empty, which the engine reads as "no evidence".
/// </summary>
internal sealed record ReadySnapshot(DateTimeOffset ObservedAt, IReadOnlyList<ReadyLane> Lanes)
{
    public IReadOnlyList<QueueReadiness> Queues { get; init; } = [];

    public IReadOnlyList<LaneKey> UnsupportedLanes { get; init; } = [];
}

internal sealed record ExpiredLease(Guid JobId, Guid AttemptId, Guid OwnershipToken, string State, DateTimeOffset LeaseExpiresAt);

internal sealed record PresenceRecord(
    Guid InstanceId,
    long PresenceSequence,
    long CapabilityRevision,
    string Status,
    int ProcessId,
    string HostName,
    string BuildId,
    IReadOnlyList<string> SubscribedQueues,
    IReadOnlyDictionary<string, int> PoolTargets,
    IReadOnlyList<JobContract> Capabilities,
    TimeSpan Expiry);

internal sealed record PresenceAck(DateTimeOffset ExpiresAt, bool Accepted);

internal interface IWorkReadinessStore
{
    Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct);

    Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct);

    Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct);
}

/// <summary>One sampled backlog tuple that no live, subscribed worker advertises (09 §5).</summary>
internal sealed record UnsupportedBacklogEntry(
    string Queue, string Priority, JobContract Contract, DateTimeOffset OldestEligibleAt, int SampledJobs, bool Truncated, string Reason);

/// <summary>
/// ListUnsupportedBacklog/1 (09 §5): "no recently advertised capable worker", from a bounded sample per lane. It never
/// proves that no worker could run a job, and an empty list with <see cref="LiveWorkers"/> = 0 is not "all clear".
/// </summary>
internal sealed record UnsupportedBacklog(DateTimeOffset ObservedAt, int LiveWorkers, int SampledPerLane, IReadOnlyList<UnsupportedBacklogEntry> Entries);

/// <summary>Readiness diagnostics (09 §5). PostgreSQL only in M3; the management surface is SP-020.</summary>
internal interface IReadinessDiagnostics
{
    Task<UnsupportedBacklog> ListUnsupportedBacklogAsync(string application, int samplePerLane, CancellationToken ct);
}

// ---- scaling (11 §4–§6)

/// <summary>Due backlog of one supported tuple on one queue, and the live workers able to take it.</summary>
internal sealed record DemandRow(string Queue, JobContract Contract, long Due, int CapableInstances);

/// <summary>
/// One shared demand snapshot (11 §4). <see cref="Truncated"/> means some lane hit the per-lane count cap, so its rows
/// are lower bounds. <see cref="MembershipDigest"/> changes whenever the set of live workers changes.
/// </summary>
internal sealed record DemandEstimate(
    DateTimeOffset ObservedAt,
    IReadOnlyList<DemandRow> Backlog,
    bool Truncated,
    int LiveWorkers,
    long MembershipDigest,
    IReadOnlyDictionary<string, int> Overrides);

/// <summary>A revisioned manual target for one pool of one worker incarnation (11 §5). Target null = no override.</summary>
internal sealed record PoolOverride(Guid InstanceId, string Pool, int? Target, long Revision, DateTimeOffset? ExpiresAt);

internal sealed record PoolOverrideRequest(Guid InstanceId, string Pool, int? Target, DateTimeOffset? ExpiresAt, long ExpectedRevision, ControlActor Actor);

/// <summary>One applied target change (11 §6); unchanged samples write nothing.</summary>
internal sealed record ScalingEventRecord(
    Guid InstanceId, string Pool, int OldTarget, int NewTarget, int? Desired, int? Candidate, string Reason, DateTimeOffset? SnapshotAt, long? MembershipDigest);

/// <summary>Scaler inputs, overrides and history (11). PostgreSQL only in M3.</summary>
internal interface IScalingStore
{
    Task<DemandEstimate> EstimateDemandAsync(string application, Guid instanceId, IReadOnlyList<string> queues, IReadOnlyList<JobContract> contracts, CancellationToken ct);

    /// <summary>Appends one history row; true when written.</summary>
    Task<bool> RecordScalingEventAsync(string application, ScalingEventRecord record, CancellationToken ct);

    Task<StoreReply<PoolOverride>> SetPoolOverrideAsync(StoreCommand<PoolOverrideRequest> command, CancellationToken ct);
}

/// <summary>Everything a driver provides to the core.</summary>
internal interface IRunwickStore : IStoreHandshake, IJobDispatchStore, IJobReservationStore, IJobStateStore, IWorkReadinessStore
{
    Guid StoreId { get; }

    string AcknowledgementProfile { get; }
}

// ---- 8.6 receipt resolution

internal abstract record ReceiptResolution
{
    internal sealed record Recorded(string Operation, int OperationVersion, string Status, DateTimeOffset DecidedAt, JsonElement Result) : ReceiptResolution;

    internal sealed record Conflict : ReceiptResolution;

    internal sealed record NotObserved : ReceiptResolution;

    internal sealed record NotApplied : ReceiptResolution;

    internal sealed record ExpiredUnknown : ReceiptResolution;
}

// ---- 9.1 transport failures

internal sealed class StoreNotAppliedException(string code, Exception? inner = null) : Exception(code, inner)
{
    public string Code { get; } = code;
}

internal sealed class StoreOutcomeUnknownException(string code, Guid commandId, string operation, Exception? inner = null)
    : Exception(code, inner)
{
    public string Code { get; } = code;

    public Guid CommandId { get; } = commandId;

    public string Operation { get; } = operation;
}

/// <summary>Operation names and receipt classes (spec 03 §4, §8.4).</summary>
internal static class Operations
{
    public const string RegisterApplication = "RegisterApplication";
    public const string RegisterQueue = "RegisterQueue";
    public const string RegisterScope = "RegisterScope";
    public const string Dispatch = "Dispatch";
    public const string DispatchMany = "DispatchMany";
    public const string Reserve = "Reserve";
    public const string Begin = "Begin";
    public const string Renew = "Renew";
    public const string Finish = "Finish";
    public const string Recover = "Recover";
    public const string ReleaseReservation = "ReleaseReservation";
    public const string SetQueueState = "SetQueueState";
    public const string Cancel = "Cancel";
    public const string ManualRetry = "ManualRetry";
    public const string RerunSucceeded = "RerunSucceeded";
    public const string EditScopePolicy = "EditScopePolicy";
    public const string EventsSince = "EventsSince";
    public const string EditRatePolicy = "EditRatePolicy";
    public const string ActivateCapabilities = "ActivateCapabilities";
    public const string SetPoolOverride = "SetPoolOverride";

    /// <summary>Administrative mutations (08 §3.1): durable class, 5 min default NotAfter, 10 min cap.</summary>
    public static bool IsAdministrative(string operation) =>
        operation is Cancel or ManualRetry or RerunSucceeded or SetQueueState or EditScopePolicy or EditRatePolicy or ActivateCapabilities or SetPoolOverride;

    public static readonly TimeSpan AdministrativeLifetime = TimeSpan.FromMinutes(5);

    public const string OwnershipClass = "ownership";
    public const string DurableClass = "durable";

    public static string RetentionClass(string operation) =>
        operation is Reserve or Begin or Recover or ReleaseReservation ? OwnershipClass : DurableClass;

    public static TimeSpan RetentionFloor(string operation) =>
        RetentionClass(operation) == OwnershipClass ? TimeSpan.FromMinutes(10) : TimeSpan.FromHours(24);

    public static TimeSpan DeadlineCap(string operation) =>
        operation is Reserve or Begin or Renew or Finish or Recover or ReleaseReservation ? TimeSpan.FromSeconds(60)
        : operation == OutboxOperations.ReceiveOutbox ? OutboxOperations.TransportCap
        : ScheduleOperations.IsScheduler(operation) ? ScheduleOperations.SchedulerCap
        : TimeSpan.FromMinutes(10);
}
