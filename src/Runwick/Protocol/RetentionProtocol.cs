using Runwick.Management;

namespace Runwick.Protocol;

// SP-021 (17): retention policy, cleanup batches and progress. A separate interface so drivers without cleanup need no stubs.

/// <summary>Record classes (17 §1), in the order the cleanup loop visits them.</summary>
internal static class RecordClasses
{
    public const string JobDetailCompleted = "job_detail_completed", JobDetailFailed = "job_detail_failed", JobHistory = "job_history",
        JobEvents = "job_events", ManagementAudit = "management_audit", ScheduleAudit = "schedule_audit", ScalingEvents = "scaling_events",
        ScheduleOccurrences = "schedule_occurrences", WorkerPresence = "worker_presence", ReceiptsOwnership = "receipts_ownership",
        ReceiptsDurable = "receipts_durable", DispatchKeys = "dispatch_keys", RateGrants = "rate_grants", OutboxDeliveries = "outbox_deliveries";

    public static IReadOnlyList<string> All { get; } =
    [
        JobDetailCompleted, JobDetailFailed, JobHistory, JobEvents, ManagementAudit, ScheduleAudit, ScalingEvents, ScheduleOccurrences,
        WorkerPresence, ReceiptsOwnership, ReceiptsDurable, DispatchKeys, RateGrants, OutboxDeliveries,
    ];

    /// <summary>Classes whose age comes from the policy; the rest have fixed predicates no policy can change.</summary>
    public static bool IsPolicy(string recordClass) => All.Take(9).Contains(recordClass, StringComparer.Ordinal);
}

/// <summary>One batch's budget (17 §4.4): root rows, decoded detail bytes, and the statement timeout.</summary>
internal sealed record CleanupLimits(int Rows, int DetailBytes, TimeSpan StatementTimeout);

/// <summary>
/// <see cref="Status"/>: <c>done</c>, <c>skipped_locked</c> (another process holds the class) or <c>failed</c> (rolled back,
/// <see cref="ErrorCode"/> recorded). <see cref="CaughtUp"/>: the pass completed and deleted nothing.
/// </summary>
internal sealed record CleanupBatchResult(string Status, int Rows, bool PassComplete, bool CaughtUp, string? ErrorCode);

internal sealed record RetentionPolicyRequest(RetentionPolicy Policy, long ExpectedRevision, ControlActor Actor);

/// <summary>The policy in force; revision 0 means no row (the defaults).</summary>
internal sealed record StoredRetentionPolicy(RetentionPolicy Policy, long Revision);

internal sealed record RetentionState(StoredRetentionPolicy Policy, IReadOnlyList<RetentionClassProgress> Classes);

internal interface IRetentionStore
{
    /// <summary>CleanupBatch/1 (17 §4.1): one transaction, no receipt; every deletion re-checks its predicate under its own lock.</summary>
    Task<CleanupBatchResult> RunCleanupBatchAsync(string application, string recordClass, CleanupLimits limits, CancellationToken ct);

    Task<StoreRead<RetentionState>> GetRetentionAsync(string application, CancellationToken ct);

    /// <summary>EditRetentionPolicy/1 (17 §2.2): administrative, durable receipt, audited.</summary>
    Task<StoreReply<StoredRetentionPolicy>> EditRetentionPolicyAsync(StoreCommand<RetentionPolicyRequest> command, CancellationToken ct);
}
