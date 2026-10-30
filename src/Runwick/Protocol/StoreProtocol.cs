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
}

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
internal sealed record BeginReply(StoreReply<BegunExecution> Reply, BeginClosed? Closed);

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

// ---- 3.5 readiness

internal sealed record LaneKey(string Queue, string Priority);

internal sealed record ReadyLane(LaneKey Lane, DateTimeOffset OldestEligibleAt);

internal sealed record ReadySnapshot(DateTimeOffset ObservedAt, IReadOnlyList<ReadyLane> Lanes);

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

    public const string OwnershipClass = "ownership";
    public const string DurableClass = "durable";

    public static string RetentionClass(string operation) =>
        operation is Reserve or Begin or Recover or ReleaseReservation ? OwnershipClass : DurableClass;

    public static TimeSpan RetentionFloor(string operation) =>
        RetentionClass(operation) == OwnershipClass ? TimeSpan.FromMinutes(10) : TimeSpan.FromHours(24);

    public static TimeSpan DeadlineCap(string operation) =>
        operation is Reserve or Begin or Renew or Finish or Recover or ReleaseReservation ? TimeSpan.FromSeconds(60) : TimeSpan.FromMinutes(10);
}
