using System.Diagnostics;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Management;

/// <summary>
/// Management telemetry (18 §2.1, §3.1): every session call counts in <c>runwick.management.requests</c> and
/// <c>runwick.management.duration</c> by method name and bounded result; mutations get a <c>runwick.management.command</c>
/// span under the caller's (ASP.NET Core request) activity. Bodies, reasons and actors never become attributes.
/// </summary>
internal sealed class MeteredManagementSession(IManagementSession inner, Microsoft.Extensions.Logging.ILogger? logger = null) : IManagementSession
{
    async Task<TResult> Measure<TResult>(string operation, bool mutation, Func<Task<TResult>> call, Func<TResult, (string Result, bool Replayed)>? describe = null)
    {
        var start = Stopwatch.GetTimestamp();
        using var span = mutation ? T.Span("runwick.management.command") : default;
        span.Activity?.SetTag("runwick.management.operation", operation);
        var result = "error";
        try
        {
            var value = await call().ConfigureAwait(false);
            var (code, replayed) = describe?.Invoke(value) ?? ("ok", false);
            result = code;
            span.Activity?.SetTag("runwick.replayed", replayed);
            return value;
        }
        catch (Exception ex)
        {
            result = ex switch
            {
                ManagementDeniedException => "forbidden",
                ManagementNotFoundException => "not_found",
                ManagementValidationException => "invalid_request",
                ManagementDetailExpiredException => "detail_expired",
                ManagementQueryBudgetException => "query_budget_exceeded",
                ControlOutcomeUnknownException => "outcome_unknown",
                RunwickException => "unavailable",
                OperationCanceledException => "cancelled",
                _ => "error",
            };
            span.Activity?.SetTag("error.type", result).SetStatus(ActivityStatusCode.Error);
            if (logger is not null)
            {
                switch (ex)
                {
                    // 1402: the target kind is the permission's area (e.g. Jobs, Queues), never the target's identity.
                    case ManagementDeniedException d:
                        Telemetry.RunwickLog.ManagementAccessDenied(logger, operation, d.Permission.Split('.')[0]);
                        break;
                    case ControlOutcomeUnknownException u:
                        Telemetry.RunwickLog.ManagementOutcomeUnknown(logger, operation, u.CommandId);
                        break;
                }
            }

            throw;
        }
        finally
        {
            span.Activity?.SetTag("runwick.result", result);
            T.ManagementRequests.Add(1, operation, result);
            T.ManagementDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, operation);
        }
    }

    // Control results: "applied" or the bounded rejection code the session maps (16 §4).
    static (string, bool) Control(bool succeeded, string? code, bool replayed) => (succeeded ? "applied" : code ?? "rejected", replayed);

    public Task<ManagementPage<JobSummary>> ListJobsAsync(ManagementCaller caller, JobListQuery query, CancellationToken cancellationToken = default) =>
        Measure("ListJobs", false, () => inner.ListJobsAsync(caller, query, cancellationToken));

    public Task<ManagementPage<ScalingEventSummary>> ListScalingEventsAsync(ManagementCaller caller, ScalingEventQuery query,
        CancellationToken cancellationToken = default) =>
        Measure("ListScalingEvents", false, () => inner.ListScalingEventsAsync(caller, query, cancellationToken));

    public Task<JobDetail> GetJobAsync(ManagementCaller caller, Guid jobId, CancellationToken cancellationToken = default) =>
        Measure("GetJob", false, () => inner.GetJobAsync(caller, jobId, cancellationToken));

    public Task<ManagementPage<QueueSummary>> ListQueuesAsync(ManagementCaller caller, int? limit = null, string? cursor = null, CancellationToken cancellationToken = default) =>
        Measure("ListQueues", false, () => inner.ListQueuesAsync(caller, limit, cursor, cancellationToken));

    public Task<ManagementPage<WorkerSummary>> ListWorkersAsync(ManagementCaller caller, bool includeExpired = false, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default) =>
        Measure("ListWorkers", false, () => inner.ListWorkersAsync(caller, includeExpired, limit, cursor, cancellationToken));

    public Task<ManagementPage<ScopeSummary>> ListScopesAsync(ManagementCaller caller, string? kind = null, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default) =>
        Measure("ListScopes", false, () => inner.ListScopesAsync(caller, kind, limit, cursor, cancellationToken));

    public Task<ManagementPage<ScheduleSummary>> ListSchedulesAsync(ManagementCaller caller, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default) =>
        Measure("ListSchedules", false, () => inner.ListSchedulesAsync(caller, limit, cursor, cancellationToken));

    public Task<ManagementPage<AuditEntry>> ListAuditAsync(ManagementCaller caller, AuditQuery query, CancellationToken cancellationToken = default) =>
        Measure("ListAudit", false, () => inner.ListAuditAsync(caller, query, cancellationToken));

    public Task<ManagementSettings> GetSettingsAsync(ManagementCaller caller, CancellationToken cancellationToken = default) =>
        Measure("GetSettings", false, () => inner.GetSettingsAsync(caller, cancellationToken));

    public Task<RevealedPayload> RevealPayloadAsync(ManagementCaller caller, Guid jobId, string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("RevealPayload", true, () => inner.RevealPayloadAsync(caller, jobId, reason, cancellationToken));

    public Task<RevealedError> RevealErrorAsync(ManagementCaller caller, Guid attemptId, string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("RevealError", true, () => inner.RevealErrorAsync(caller, attemptId, reason, cancellationToken));

    public Task<JobControlResult> CancelJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        Measure("CancelJob", true, () => inner.CancelJobAsync(caller, jobId, request, cancellationToken), r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<JobControlResult> RetryJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        Measure("RetryJob", true, () => inner.RetryJobAsync(caller, jobId, request, cancellationToken), r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<JobControlResult> RerunJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        Measure("RerunJob", true, () => inner.RerunJobAsync(caller, jobId, request, cancellationToken), r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<IReadOnlyList<BulkCancelResult>> CancelJobsAsync(ManagementCaller caller, IReadOnlyList<BulkCancelItem> items, string? reason = null,
        CancellationToken cancellationToken = default) =>
        Measure("CancelJobs", true, () => inner.CancelJobsAsync(caller, items, reason, cancellationToken));

    public Task<QueueControlResult> PauseQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default) =>
        Measure("PauseQueue", true, () => inner.PauseQueueAsync(caller, queue, commandId, expectedRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<QueueControlResult> ResumeQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default) =>
        Measure("ResumeQueue", true, () => inner.ResumeQueueAsync(caller, queue, commandId, expectedRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<CapacityControlResult> SetScopeCapacityAsync(ManagementCaller caller, string scopeKind, string scopeKey, Guid commandId, int capacity,
        long expectedPolicyRevision, string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("SetScopeCapacity", true,
            () => inner.SetScopeCapacityAsync(caller, scopeKind, scopeKey, commandId, capacity, expectedPolicyRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<ScheduleControlResult> PauseScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision, string? reason = null,
        CancellationToken cancellationToken = default) =>
        Measure("PauseSchedule", true, () => inner.PauseScheduleAsync(caller, key, commandId, expectedControlRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<ScheduleControlResult> ResumeScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision, ResumePolicy policy,
        string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("ResumeSchedule", true, () => inner.ResumeScheduleAsync(caller, key, commandId, expectedControlRevision, policy, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<ScheduleControlResult> TriggerScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision, string? reason = null,
        CancellationToken cancellationToken = default) =>
        Measure("TriggerSchedule", true, () => inner.TriggerScheduleAsync(caller, key, commandId, expectedDefinitionRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<ScheduleControlResult> RevalidateScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision,
        string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("RevalidateSchedule", true, () => inner.RevalidateScheduleAsync(caller, key, commandId, expectedDefinitionRevision, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<RetentionView> GetRetentionAsync(ManagementCaller caller, CancellationToken cancellationToken = default) =>
        Measure("GetRetention", false, () => inner.GetRetentionAsync(caller, cancellationToken));

    public Task<RetentionPolicyResult> EditRetentionPolicyAsync(ManagementCaller caller, Guid commandId, long expectedRevision, RetentionPolicy policy,
        string? reason = null, CancellationToken cancellationToken = default) =>
        Measure("EditRetentionPolicy", true, () => inner.EditRetentionPolicyAsync(caller, commandId, expectedRevision, policy, reason, cancellationToken),
            r => Control(r.Succeeded, r.Code, r.Replayed));

    public Task<EventsPage> ListEventsAsync(ManagementCaller caller, string? after = null, int? limit = null, CancellationToken cancellationToken = default) =>
        Measure("ListEvents", false, () => inner.ListEventsAsync(caller, after, limit, cancellationToken));

    public Task<ManagementDispatchResult> DispatchAsync(ManagementCaller caller, ManagementDispatchRequest request, CancellationToken cancellationToken = default) =>
        Measure("Dispatch", true, () => inner.DispatchAsync(caller, request, cancellationToken), r => (r.Disposition, r.Replayed));
}
