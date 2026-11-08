using System.Text;
using System.Text.Json.Nodes;

namespace Runwick.Management;

// SP-020 (16): the management API's caller model, read models and outcomes. Transport-free.

/// <summary>Permission names (16 §1).</summary>
public static class ManagementPermissions
{
    public const string JobsRead = "Jobs.Read", JobsReadPayload = "Jobs.ReadPayload", JobsReadError = "Jobs.ReadError",
        JobsCancel = "Jobs.Cancel", JobsRetry = "Jobs.Retry", JobsRerun = "Jobs.Rerun", JobsDispatch = "Jobs.Dispatch",
        QueuesRead = "Queues.Read", QueuesControl = "Queues.Control",
        SchedulesRead = "Schedules.Read", SchedulesTrigger = "Schedules.Trigger", SchedulesControl = "Schedules.Control", SchedulesEdit = "Schedules.Edit",
        CapacityRead = "Capacity.Read", CapacityEdit = "Capacity.Edit",
        SettingsRead = "Settings.Read", RetentionManage = "Retention.Manage", AuditRead = "Audit.Read";

    /// <summary>Permissions a grant may scope to queues: jobs in a queue and that queue's control.</summary>
    public static IReadOnlySet<string> QueueScoped { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        JobsRead, JobsReadPayload, JobsReadError, JobsCancel, JobsRetry, JobsRerun, JobsDispatch, QueuesRead, QueuesControl,
    };

    public static IReadOnlySet<string> All { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        JobsRead, JobsReadPayload, JobsReadError, JobsCancel, JobsRetry, JobsRerun, JobsDispatch, QueuesRead, QueuesControl,
        SchedulesRead, SchedulesTrigger, SchedulesControl, SchedulesEdit, CapacityRead, CapacityEdit, SettingsRead, RetentionManage, AuditRead,
    };
}

/// <summary>
/// One permission, for every queue (<paramref name="Queues"/> null) or for the listed queues. Application-wide
/// permissions (schedules, capacity, settings, retention, audit) cannot be queue-scoped: constructing one is a
/// configuration error, never a silent widening (16 §1).
/// </summary>
public sealed record ManagementGrant(string Permission, IReadOnlySet<string>? Queues = null)
{
    public string Permission { get; } = Validate(Permission, Queues);

    static string Validate(string permission, IReadOnlySet<string>? queues)
    {
        if (!ManagementPermissions.All.Contains(permission))
        {
            throw new ArgumentException($"Unknown management permission '{permission}'.", nameof(permission));
        }

        return queues is not null && !ManagementPermissions.QueueScoped.Contains(permission)
            ? throw new ArgumentException($"Permission '{permission}' is application-wide and cannot be scoped to queues.", nameof(queues))
            : permission;
    }
}

/// <summary>A trusted caller: the authenticated actor and its grants, both from the host's principal mapping.</summary>
public sealed record ManagementCaller(ManagementActor Actor, IReadOnlyList<ManagementGrant> Grants);

/// <summary>Preset grant sets (16 §1). Payload, error and audit reads and <c>Jobs.Dispatch</c> are never in a preset.</summary>
public static class ManagementRoles
{
    static IReadOnlyList<ManagementGrant> Of(params string[] permissions) => [.. permissions.Select(p => new ManagementGrant(p))];

    public static IReadOnlyList<ManagementGrant> Viewer { get; } = Of(
        ManagementPermissions.JobsRead, ManagementPermissions.QueuesRead, ManagementPermissions.SchedulesRead,
        ManagementPermissions.CapacityRead, ManagementPermissions.SettingsRead);

    public static IReadOnlyList<ManagementGrant> Operator { get; } = [.. Viewer, .. Of(
        ManagementPermissions.JobsCancel, ManagementPermissions.JobsRetry, ManagementPermissions.JobsRerun, ManagementPermissions.QueuesControl,
        ManagementPermissions.SchedulesTrigger, ManagementPermissions.SchedulesControl)];

    public static IReadOnlyList<ManagementGrant> Administrator { get; } = [.. Operator, .. Of(
        ManagementPermissions.SchedulesEdit, ManagementPermissions.CapacityEdit, ManagementPermissions.RetentionManage)];
}

/// <summary>Management-session options (16 §2.1, §5). Limits are process-local.</summary>
public sealed class ManagementOptions
{
    /// <summary>HMAC-SHA256 key for list cursors, 32 bytes or more. When null the host derives one (Data Protection) or a per-process key is used.</summary>
    public byte[]? CursorKey { get; set; }

    public int ReadsPerMinute { get; set; } = 120;

    public int MutationsPerMinute { get; set; } = 30;

    public int RevealsPerMinute { get; set; } = 10;

    public int MaxTrackedPrincipals { get; set; } = 10_000;

    public int MaxActiveRequests { get; set; } = 16;

    public int MaxWaitingRequests { get; set; } = 32;

    public TimeSpan MaxWait { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>Caller is authenticated and the resource is visible, but the permission is missing (→ 403).</summary>
public sealed class ManagementDeniedException(string permission)
    : RunwickException("forbidden", ErrorDisposition.KnownNotApplied, $"The caller lacks permission {permission}.")
{
    public string Permission { get; } = permission;
}

/// <summary>Missing, or outside the caller's queues (→ 404). The same either way: hidden IDs are never confirmed.</summary>
public sealed class ManagementNotFoundException(string code = "not_found", string message = "Not found.")
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);

/// <summary>The management host cannot take the request now (→ 503 with <c>busy</c> or <c>admission_closed</c>); nothing was applied.</summary>
public sealed class ManagementUnavailableException(string code, string message)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);

/// <summary>The store refused a dispatch because of the store's state (→ 409 <c>unique_conflict</c>, <c>command_conflict</c>).</summary>
public sealed class ManagementConflictException(string code, string message)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);

/// <summary>A request the session rejects before any store call (→ 400).</summary>
public sealed class ManagementValidationException(string code, string message)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);

/// <summary>A read did not finish within its 1 s statement budget (→ 503 <c>query_budget_exceeded</c>); no partial page.</summary>
public sealed class ManagementQueryBudgetException()
    : RunwickException("query_budget_exceeded", ErrorDisposition.KnownNotApplied, "The query did not finish within its budget.");

/// <summary>A revealed detail was removed by retention (→ 410 <c>detail_expired</c>).</summary>
public sealed class ManagementDetailExpiredException(Guid jobId)
    : RunwickException("detail_expired", ErrorDisposition.KnownNotApplied, "The job's detail has expired.")
{
    public Guid JobId { get; } = jobId;
}

// ---- reads

/// <summary>One keyset page. <see cref="Next"/> is null at the end; there is never a total.</summary>
public sealed record ManagementPage<T>(IReadOnlyList<T> Items, DateTimeOffset ObservedAt, string? Next);

public sealed record JobListQuery(
    string? Queue = null, string? State = null, string? JobName = null, DateTimeOffset? From = null, DateTimeOffset? To = null,
    int? Limit = null, string? Cursor = null);

public sealed record AuditQuery(
    string? TargetKind = null, string? TargetId = null, DateTimeOffset? From = null, DateTimeOffset? To = null, int? Limit = null, string? Cursor = null);

public sealed record JobSummary(
    Guid JobId, string JobName, int PayloadVersion, string Queue, string Priority, string State, int Generation, long Revision,
    DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt, DateTimeOffset EligibleAt, DateTimeOffset? TerminalAt, string? TerminalReason,
    bool CancelRequested, long ExecutionCount, DateTimeOffset? DetailExpiredAt = null);

public sealed record JobGenerationInfo(int Generation, string CreatedReason, string? CreatedBy, DateTimeOffset CreatedAt, int StartedCount, int MaxExecutions);

/// <summary>An attempt without its error text: only the error's code (reveal it with <c>RevealError</c>).</summary>
public sealed record JobAttemptInfo(
    Guid AttemptId, int Generation, long ReservationNumber, int? ExecutionNumber, Guid WorkerInstanceId,
    DateTimeOffset ReservedAt, DateTimeOffset? AuthorizedAt, DateTimeOffset? FinishedAt,
    string? Outcome, string? Disposition, string? AppliedReason, string? ErrorCode);

public sealed record JobDetail(JobSummary Job, IReadOnlyList<JobGenerationInfo> Generations, IReadOnlyList<JobAttemptInfo> Attempts);

public sealed record QueueSummary(string Name, bool Paused, long Revision);

public sealed record WorkerSummary(
    Guid InstanceId, string HostName, int ProcessId, string BuildId, string Status, JsonNode? Subscriptions, JsonNode? PoolTargets,
    DateTimeOffset StartedAt, DateTimeOffset HeartbeatAt, DateTimeOffset ExpiresAt);

public sealed record ScopeSummary(string Kind, string Key, int Capacity, long PolicyRevision, int LivePermits);

public sealed record ScheduleSummary(
    string Key, bool Paused, bool NeedsReview, long DefinitionRevision, long ControlRevision, DateTimeOffset? NextDueAt, DateTimeOffset? LastMaterializedAt);

public sealed record AuditEntry(
    long AuditId, Guid CommandId, string Operation, string TargetKind, string TargetId, string ActorSubject, string? ActorDisplay,
    string ActorScheme, string? Reason, long? ExpectedRevision, long? ResultingRevision, JsonObject Details, DateTimeOffset OccurredAt);

public sealed record ManagementSettings(
    string Application, string StoreKind, long SchemaRevision, string PersistenceProfile, IReadOnlyList<string> Roles,
    IReadOnlyDictionary<string, long> Limits, long RetentionPolicyRevision = 0);

/// <summary>A revealed payload: the stored JSON value and headers, returned only after the disclosure audit committed.</summary>
public sealed record RevealedPayload(Guid JobId, JsonNode? Payload, JsonNode? Headers);

public sealed record RevealedError(Guid JobId, Guid AttemptId, JsonNode? Error);

// ---- mutations

/// <summary>A job control from a client: its own command ID (resend it unchanged to resolve an unknown outcome).</summary>
public sealed record JobControlRequest(Guid CommandId, int ExpectedGeneration, long ExpectedRevision, string? Reason = null);

public sealed record BulkCancelItem(Guid JobId, Guid CommandId, int ExpectedGeneration, long ExpectedRevision);

/// <summary>
/// One bulk item: <c>applied</c>, <c>accepted</c> (cancel requested on owned work), <c>rejected</c> (with <see cref="Code"/>),
/// <c>not_found</c>, <c>forbidden</c> or <c>unavailable</c>.
/// </summary>
public sealed record BulkCancelResult(Guid JobId, string Status, string? Code, JobControlResult? Result);

/// <summary>
/// An audited dispatch by job name (19 §6.2). <see cref="CommandId"/> is the dispatch operation ID; resend it unchanged
/// to resolve an unknown outcome. <see cref="Payload"/> is bound to the registered job type.
/// </summary>
public sealed record ManagementDispatchRequest(
    Guid CommandId, string JobName, JsonNode? Payload, int? PayloadVersion = null, string? Queue = null, string? Priority = null,
    DateTimeOffset? NotBefore = null, string? Reason = null);

/// <summary><see cref="Disposition"/> is <c>created</c> or <c>reused</c> (an existing job holding the uniqueness key).</summary>
public sealed record ManagementDispatchResult(Guid JobId, string Disposition, int Generation, long Revision, string Queue, bool Replayed, Guid CommandId);

/// <summary>One job event (08 §7). <see cref="ActorId"/> is null unless the caller may read audit.</summary>
public sealed record ManagementEvent(
    long EventId, string Kind, Guid? JobId, Guid? AttemptId, Guid? CommandId, DateTimeOffset OccurredAt, string? ActorId, JsonObject Details);

/// <summary>
/// A page of events. <see cref="Next"/> is a plain position <c>"&lt;txId&gt;.&lt;eventId&gt;"</c> (the input cursor when
/// the page is empty); <see cref="Partial"/> means newer events may not be visible yet.
/// </summary>
public sealed record EventsPage(IReadOnlyList<ManagementEvent> Items, DateTimeOffset ObservedAt, string Next, bool Partial);

internal static class ManagementText
{
    public const int MaxReasonBytes = 512;

    public static void Reason(string? reason)
    {
        if (reason is not null && Encoding.UTF8.GetByteCount(reason) > MaxReasonBytes)
        {
            throw new ManagementValidationException("invalid_request", "reason exceeds 512 UTF-8 bytes.");
        }
    }
}
