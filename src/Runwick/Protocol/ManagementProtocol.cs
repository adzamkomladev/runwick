using System.Text.Json.Nodes;
using Runwick.Management;

namespace Runwick.Protocol;

// SP-020 (16 §6): bounded management reads and RecordDisclosure/1. Every read takes the caller's allowed queues
// (null = every queue) as a SQL predicate before its LIMIT, and returns the store time it observed.

internal sealed record StoreRead<T>(T Value, DateTimeOffset ObservedAt);

internal sealed record JobListFilter(string? Queue, string? State, string? JobName, DateTimeOffset From, DateTimeOffset To);

internal sealed record ScalingEventFilter(Guid? InstanceId, string? Pool, DateTimeOffset From, DateTimeOffset To);

internal sealed record AuditFilter(string? TargetKind, string? TargetId, DateTimeOffset? From, DateTimeOffset? To);

/// <summary>RecordDisclosure/1: <see cref="Kind"/> is <c>RevealPayload</c> (target a job) or <c>RevealError</c> (target an attempt).</summary>
internal sealed record DisclosureRequest(string Kind, Guid TargetId, IReadOnlyList<string>? Queues, ControlActor Actor);

/// <summary><see cref="Status"/>: <c>disclosed</c>, <c>not_found</c> or <c>detail_expired</c>; data only when disclosed.</summary>
internal sealed record Disclosure(string Status, Guid? JobId, JsonNode? Payload, JsonNode? Headers, JsonNode? Error);

internal interface IManagementQueryStore
{
    Task<StoreRead<IReadOnlyList<JobSummary>>> ListJobsAsync(string application, IReadOnlyList<string>? queues, JobListFilter filter,
        (DateTimeOffset CreatedAt, Guid JobId)? after, int limit, CancellationToken ct);

    Task<StoreRead<JobDetail?>> GetJobAsync(string application, Guid jobId, CancellationToken ct);

    /// <summary>The queue of a job, or of an attempt's job; null when neither exists.</summary>
    Task<string?> QueueOfAsync(string application, Guid? jobId, Guid? attemptId, CancellationToken ct);

    /// <summary>The NotAfter of an existing receipt for this command ID, so a resent client command fingerprints the same.</summary>
    Task<DateTimeOffset?> CommandNotAfterAsync(string application, Guid commandId, CancellationToken ct);

    Task<StoreRead<IReadOnlyList<QueueSummary>>> ListQueuesAsync(string application, IReadOnlyList<string>? queues, string? after, int limit, CancellationToken ct);

    Task<StoreRead<IReadOnlyList<WorkerSummary>>> ListWorkersAsync(string application, bool includeExpired, (DateTimeOffset HeartbeatAt, Guid InstanceId)? after,
        int limit, CancellationToken ct);

    Task<StoreRead<IReadOnlyList<ScopeSummary>>> ListScopesAsync(string application, string? kind, (string Kind, string Key)? after, int limit, CancellationToken ct);

    Task<StoreRead<IReadOnlyList<ScheduleSummary>>> ListSchedulesAsync(string application, string? after, int limit, CancellationToken ct);

    Task<StoreRead<IReadOnlyList<AuditEntry>>> ListAuditAsync(string application, AuditFilter filter, long? after, int limit, CancellationToken ct);

    /// <summary>Scaling history, <c>created_at DESC, event_id DESC</c> (21 §6).</summary>
    Task<StoreRead<IReadOnlyList<ScalingEventSummary>>> ListScalingEventsAsync(string application, ScalingEventFilter filter,
        (DateTimeOffset CreatedAt, Guid EventId)? after, int limit, CancellationToken ct);

    /// <summary>One transaction: the audit row, then the read; data leaves only after the commit (16 §3).</summary>
    Task<StoreReply<Disclosure>> RecordDisclosureAsync(StoreCommand<DisclosureRequest> command, CancellationToken ct);
}
