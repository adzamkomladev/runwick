using System.Globalization;
using System.Security.Cryptography;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Management;

/// <summary>
/// The management API in core (16): transport-free, every call carries a trusted <see cref="ManagementCaller"/>.
/// Returns a result or throws <see cref="ManagementDeniedException"/>, <see cref="ManagementNotFoundException"/> or
/// <see cref="ManagementValidationException"/>; store unavailability and unknown outcomes keep their own exceptions.
/// </summary>
public interface IManagementSession
{
    Task<ManagementPage<JobSummary>> ListJobsAsync(ManagementCaller caller, JobListQuery query, CancellationToken cancellationToken = default);

    Task<JobDetail> GetJobAsync(ManagementCaller caller, Guid jobId, CancellationToken cancellationToken = default);

    Task<ManagementPage<QueueSummary>> ListQueuesAsync(ManagementCaller caller, int? limit = null, string? cursor = null, CancellationToken cancellationToken = default);

    Task<ManagementPage<WorkerSummary>> ListWorkersAsync(ManagementCaller caller, bool includeExpired = false, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default);

    Task<ManagementPage<ScopeSummary>> ListScopesAsync(ManagementCaller caller, string? kind = null, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default);

    Task<ManagementPage<ScheduleSummary>> ListSchedulesAsync(ManagementCaller caller, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default);

    Task<ManagementPage<AuditEntry>> ListAuditAsync(ManagementCaller caller, AuditQuery query, CancellationToken cancellationToken = default);

    Task<ManagementSettings> GetSettingsAsync(ManagementCaller caller, CancellationToken cancellationToken = default);

    /// <summary>The retention policy and per-class cleanup progress (17 §7). Needs <c>Settings.Read</c>.</summary>
    Task<RetentionView> GetRetentionAsync(ManagementCaller caller, CancellationToken cancellationToken = default);

    /// <summary>A revisioned, audited policy edit (17 §2.2). Needs <c>Retention.Manage</c>; an invalid policy is <c>invalid_retention_policy</c>.</summary>
    Task<RetentionPolicyResult> EditRetentionPolicyAsync(ManagementCaller caller, Guid commandId, long expectedRevision, RetentionPolicy policy,
        string? reason = null, CancellationToken cancellationToken = default);

    Task<RevealedPayload> RevealPayloadAsync(ManagementCaller caller, Guid jobId, string? reason = null, CancellationToken cancellationToken = default);

    Task<RevealedError> RevealErrorAsync(ManagementCaller caller, Guid attemptId, string? reason = null, CancellationToken cancellationToken = default);

    Task<JobControlResult> CancelJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default);

    Task<JobControlResult> RetryJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default);

    Task<JobControlResult> RerunJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default);

    /// <summary>At most 100 items, 4 at a time; one command, receipt and audit row each; results in request order.</summary>
    Task<IReadOnlyList<BulkCancelResult>> CancelJobsAsync(ManagementCaller caller, IReadOnlyList<BulkCancelItem> items, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<QueueControlResult> PauseQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<QueueControlResult> ResumeQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<CapacityControlResult> SetScopeCapacityAsync(ManagementCaller caller, string scopeKind, string scopeKey, Guid commandId, int capacity,
        long expectedPolicyRevision, string? reason = null, CancellationToken cancellationToken = default);

    Task<ScheduleControlResult> PauseScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<ScheduleControlResult> ResumeScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision, ResumePolicy policy,
        string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>The command ID is also the trigger ID.</summary>
    Task<ScheduleControlResult> TriggerScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<ScheduleControlResult> RevalidateScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision,
        string? reason = null, CancellationToken cancellationToken = default);
}

internal sealed class ManagementSession(
    RunwickRuntime runtime, RunwickBuilder builder, IManagementQueryStore? store, ControlService controls, ScheduleControlService schedules,
    ManagementCursors cursors, ManagementOptions options, TimeProvider time) : IManagementSession
{
    public const int DefaultPageSize = 50, MaxPageSize = 200, MaxBulkItems = 100;
    static readonly TimeSpan DefaultRange = TimeSpan.FromDays(7), MaxRange = TimeSpan.FromDays(31);
    static readonly string[] JobStateNames = ["pending", "reserved", "running", "retrying", "succeeded", "failed", "cancelled"];
    static readonly string[] AllowedAuditDetails =
    [
        "effect", "previousState", "previous", "capacity", "livePermits", "generation", "revision", "policyRevision",
        "before", "after", "day", "rows", "batches", "failures",
    ];

    IManagementQueryStore Store => store ?? throw new InvalidOperationException("The configured store does not support the management API.");

    string App => runtime.Registry.Application;

    // ------------------------------------------------------------------ grants

    /// <summary>False when no grant carries the permission; <paramref name="queues"/> null means every queue.</summary>
    static bool TryScope(ManagementCaller caller, string permission, out IReadOnlyList<string>? queues)
    {
        ArgumentNullException.ThrowIfNull(caller);
        var grants = caller.Grants.Where(g => g.Permission == permission).ToList();
        queues = grants.Any(g => g.Queues is null) ? null : [.. grants.SelectMany(g => g.Queues!).Distinct(StringComparer.Ordinal)];
        return grants.Count > 0;
    }

    static IReadOnlyList<string>? Require(ManagementCaller caller, string permission) =>
        TryScope(caller, permission, out var queues) ? queues : throw new ManagementDeniedException(permission);

    static bool Covers(ManagementCaller caller, string permission, string queue) =>
        TryScope(caller, permission, out var queues) && (queues is null || queues.Contains(queue, StringComparer.Ordinal));

    /// <summary>A job's queue must be readable (else 404, never confirming the ID), then the permission must cover it (else 403).</summary>
    async Task<string> AuthorizeJobAsync(ManagementCaller caller, Guid? jobId, Guid? attemptId, string permission, CancellationToken ct)
    {
        await runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        var queue = await Read(() => Store.QueueOfAsync(App, jobId, attemptId, ct)).ConfigureAwait(false);
        if (queue is null || !Covers(caller, ManagementPermissions.JobsRead, queue))
        {
            throw new ManagementNotFoundException();
        }

        return Covers(caller, permission, queue) ? queue : throw new ManagementDeniedException(permission);
    }

    static int PageSize(int? limit) => limit switch
    {
        null => DefaultPageSize,
        < 1 => throw new ManagementValidationException("invalid_request", "limit must be at least 1."),
        _ => Math.Min(limit.Value, MaxPageSize),
    };

    /// <summary>Runs a read; a statement timeout is <c>query_budget_exceeded</c>, other store failures stay unavailable.</summary>
    static async Task<T> Read<T>(Func<Task<T>> read)
    {
        try
        {
            return await read().ConfigureAwait(false);
        }
        catch (StoreNotAppliedException ex) when (ex.Code is "statement_timeout" or "command_expired")
        {
            throw new ManagementQueryBudgetException();
        }
        catch (StoreNotAppliedException ex)
        {
            throw new StoreUnavailableException($"The management read failed ({ex.Code}).", ex);
        }
    }

    static string Ms(DateTimeOffset t) => t.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    static DateTimeOffset FromMs(string s) => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(s, CultureInfo.InvariantCulture));

    ManagementPage<TOut> Page<TOut>(StoreRead<IReadOnlyList<TOut>> read, int limit, string name, string filters, string queueHash, Func<TOut, string[]> key) =>
        new([.. read.Value.Take(limit)], read.ObservedAt,
            read.Value.Count > limit ? cursors.Seal(name, App, filters, queueHash, key(read.Value[limit - 1])) : null);

    // ------------------------------------------------------------------ reads (16 §2)

    public async Task<ManagementPage<JobSummary>> ListJobsAsync(ManagementCaller caller, JobListQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var queues = Require(caller, ManagementPermissions.JobsRead);
        var limit = PageSize(query.Limit);
        if (query.State is not null && !JobStateNames.Contains(query.State))
        {
            throw new ManagementValidationException("invalid_request", "Unknown job state.");
        }

        var filters = string.Join('|', query.Queue, query.State, query.JobName, query.From is { } f ? Ms(f) : "", query.To is { } t ? Ms(t) : "");
        var hash = ManagementCursors.QueueHash(queues);
        DateTimeOffset from, to;
        (DateTimeOffset, Guid)? after = null;
        if (query.Cursor is { } cursor)
        {
            // The cursor keeps the resolved range, so a defaulted range does not drift between pages.
            var k = cursors.Open(cursor, "jobs", App, filters, hash);
            (from, to, after) = (FromMs(k[0]), FromMs(k[1]), (FromMs(k[2]), Guid.Parse(k[3])));
        }
        else
        {
            to = query.To ?? time.GetUtcNow();
            from = query.From ?? to - DefaultRange;
            if (from > to || to - from > MaxRange)
            {
                throw new ManagementValidationException("invalid_request", "The created range must be ordered and at most 31 days.");
            }
        }

        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        if (query.Queue is { } q && queues is not null && !queues.Contains(q, StringComparer.Ordinal))
        {
            // A queue outside the caller's scope lists nothing, as if it were empty.
            return new ManagementPage<JobSummary>([], time.GetUtcNow(), null);
        }

        var read = await Read(() => Store.ListJobsAsync(App, queues, new JobListFilter(query.Queue, query.State, query.JobName, from, to), after, limit + 1,
            cancellationToken)).ConfigureAwait(false);
        return Page(read, limit, "jobs", filters, hash, j => [Ms(from), Ms(to), Ms(j.CreatedAt), j.JobId.ToString("D")]);
    }

    public async Task<JobDetail> GetJobAsync(ManagementCaller caller, Guid jobId, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.JobsRead);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.GetJobAsync(App, jobId, cancellationToken)).ConfigureAwait(false);
        return read.Value is { } detail && Covers(caller, ManagementPermissions.JobsRead, detail.Job.Queue) ? detail : throw new ManagementNotFoundException();
    }

    public async Task<ManagementPage<QueueSummary>> ListQueuesAsync(ManagementCaller caller, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        var queues = Require(caller, ManagementPermissions.QueuesRead);
        var size = PageSize(limit);
        var hash = ManagementCursors.QueueHash(queues);
        var after = cursor is null ? null : cursors.Open(cursor, "queues", App, "", hash)[0];
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListQueuesAsync(App, queues, after, size + 1, cancellationToken)).ConfigureAwait(false);
        return Page(read, size, "queues", "", hash, x => [x.Name]);
    }

    public async Task<ManagementPage<WorkerSummary>> ListWorkersAsync(ManagementCaller caller, bool includeExpired = false, int? limit = null,
        string? cursor = null, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.CapacityRead);
        var size = PageSize(limit);
        var filters = includeExpired ? "expired" : "";
        (DateTimeOffset, Guid)? after = cursor is null ? null : cursors.Open(cursor, "workers", App, filters, "*") is var k ? (FromMs(k[0]), Guid.Parse(k[1])) : null;
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListWorkersAsync(App, includeExpired, after, size + 1, cancellationToken)).ConfigureAwait(false);
        return Page(read, size, "workers", filters, "*", w => [Ms(w.HeartbeatAt), w.InstanceId.ToString("D")]);
    }

    public async Task<ManagementPage<ScopeSummary>> ListScopesAsync(ManagementCaller caller, string? kind = null, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.CapacityRead);
        var size = PageSize(limit);
        (string, string)? after = cursor is null ? null : cursors.Open(cursor, "scopes", App, kind ?? "", "*") is var k ? (k[0], k[1]) : null;
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListScopesAsync(App, kind, after, size + 1, cancellationToken)).ConfigureAwait(false);
        return Page(read, size, "scopes", kind ?? "", "*", s => [s.Kind, s.Key]);
    }

    public async Task<ManagementPage<ScheduleSummary>> ListSchedulesAsync(ManagementCaller caller, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SchedulesRead);
        var size = PageSize(limit);
        var after = cursor is null ? null : cursors.Open(cursor, "schedules", App, "", "*")[0];
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListSchedulesAsync(App, after, size + 1, cancellationToken)).ConfigureAwait(false);
        return Page(read, size, "schedules", "", "*", s => [s.Key]);
    }

    public async Task<ManagementPage<AuditEntry>> ListAuditAsync(ManagementCaller caller, AuditQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        Require(caller, ManagementPermissions.AuditRead);
        var size = PageSize(query.Limit);
        var filters = string.Join('|', query.TargetKind, query.TargetId, query.From is { } f ? Ms(f) : "", query.To is { } t ? Ms(t) : "");
        long? after = query.Cursor is null ? null : long.Parse(cursors.Open(query.Cursor, "audit", App, filters, "*")[0], CultureInfo.InvariantCulture);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListAuditAsync(App, new AuditFilter(query.TargetKind, query.TargetId, query.From, query.To), after, size + 1,
            cancellationToken)).ConfigureAwait(false);
        var items = read.Value.Select(a => a with
        {
            // Only allowlisted detail keys leave the store.
            Details = new([.. a.Details.Where(d => AllowedAuditDetails.Contains(d.Key)).Select(d => KeyValuePair.Create(d.Key, d.Value?.DeepClone()))]),
        }).ToList();
        return Page(new StoreRead<IReadOnlyList<AuditEntry>>(items, read.ObservedAt), size, "audit", filters, "*",
            a => [a.AuditId.ToString(CultureInfo.InvariantCulture)]);
    }

    public async Task<ManagementSettings> GetSettingsAsync(ManagementCaller caller, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SettingsRead);
        var descriptor = await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        List<string> roles = ["producer", "management"];
        if (builder.Workers.Count > 0)
        {
            roles.Add("worker");
        }

        if (builder.Services.Any(d => d.ServiceType == typeof(Scheduling.SchedulerService)))
        {
            roles.Add("scheduler");
        }

        var retention = Store is IRetentionStore r ? (await Read(() => r.GetRetentionAsync(App, cancellationToken)).ConfigureAwait(false)).Value.Policy.Revision : 0;
        return new ManagementSettings(App, builder.StoreProviders.FirstOrDefault()?.Name ?? "unknown", descriptor.SchemaRevision,
            runtime.Store.AcknowledgementProfile, roles, new Dictionary<string, long>
            {
                ["defaultPageSize"] = DefaultPageSize, ["maxPageSize"] = MaxPageSize, ["maxBulkItems"] = MaxBulkItems,
                ["readsPerMinute"] = options.ReadsPerMinute, ["mutationsPerMinute"] = options.MutationsPerMinute,
                ["revealsPerMinute"] = options.RevealsPerMinute, ["maxActiveRequests"] = options.MaxActiveRequests,
                ["maxWaitingRequests"] = options.MaxWaitingRequests,
            }, retention);
    }

    // ------------------------------------------------------------------ retention (17 §7)

    IRetentionStore Retention => Store as IRetentionStore ?? throw new InvalidOperationException("The configured store does not support retention.");

    public async Task<RetentionView> GetRetentionAsync(ManagementCaller caller, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SettingsRead);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Retention.GetRetentionAsync(App, cancellationToken)).ConfigureAwait(false);
        return new RetentionView(read.Value.Policy.Policy, read.Value.Policy.Revision, read.Value.Classes, read.ObservedAt);
    }

    public async Task<RetentionPolicyResult> EditRetentionPolicyAsync(ManagementCaller caller, Guid commandId, long expectedRevision, RetentionPolicy policy,
        string? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ManagementText.Reason(reason);
        Require(caller, ManagementPermissions.RetentionManage);
        if (policy.Problem() is { } problem)
        {
            throw new ManagementValidationException("invalid_retention_policy", problem);
        }

        var seed = await SeedAsync(commandId, Operations.AdministrativeLifetime, cancellationToken).ConfigureAwait(false);
        var r = await controls.SendAsync(Actor(caller, reason), a => new RetentionPolicyRequest(policy, expectedRevision, a), Retention.EditRetentionPolicyAsync,
            seed, cancellationToken).ConfigureAwait(false);
        return r.Value is { } v
            ? new RetentionPolicyResult(true, null, v.Revision, v.Policy, r.Replayed)
            : new RetentionPolicyResult(false, Map(r.Error?.Code), r.Error?.ActualRevision, null, false);
    }

    // ------------------------------------------------------------------ reveal (16 §3)

    public async Task<RevealedPayload> RevealPayloadAsync(ManagementCaller caller, Guid jobId, string? reason = null, CancellationToken cancellationToken = default)
    {
        var d = await DiscloseAsync(caller, "RevealPayload", jobId, null, ManagementPermissions.JobsReadPayload, reason, cancellationToken).ConfigureAwait(false);
        return new RevealedPayload(jobId, d.Payload, d.Headers);
    }

    public async Task<RevealedError> RevealErrorAsync(ManagementCaller caller, Guid attemptId, string? reason = null, CancellationToken cancellationToken = default)
    {
        var d = await DiscloseAsync(caller, "RevealError", null, attemptId, ManagementPermissions.JobsReadError, reason, cancellationToken).ConfigureAwait(false);
        return new RevealedError(d.JobId!.Value, attemptId, d.Error);
    }

    async Task<Disclosure> DiscloseAsync(ManagementCaller caller, string kind, Guid? jobId, Guid? attemptId, string permission, string? reason,
        CancellationToken ct)
    {
        ManagementText.Reason(reason);
        var queue = await AuthorizeJobAsync(caller, jobId, attemptId, permission, ct).ConfigureAwait(false);
        var notAfter = CommandSeed.Ms(time.GetUtcNow() + TimeSpan.FromSeconds(5));
        var command = new StoreCommand<DisclosureRequest>(App, Guid.CreateVersion7(), notAfter,
            new DisclosureRequest(kind, (jobId ?? attemptId)!.Value, [queue], Actor(caller, reason)));
        StoreReply<Disclosure> reply;
        try
        {
            reply = await Store.RecordDisclosureAsync(command, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException)
        {
            // Nothing is returned unless the audit row is known to be committed (MGMT-09).
            throw new StoreUnavailableException("The disclosure was not recorded.", ex);
        }

        return reply.Value switch
        {
            { Status: "disclosed" } d => d,
            { Status: "detail_expired", JobId: { } expired } => throw new ManagementDetailExpiredException(expired),
            { Status: "not_found" } => throw new ManagementNotFoundException(),
            _ => throw new StoreUnavailableException($"The disclosure was not recorded ({reply.Error?.Code})."),
        };
    }

    static ControlActor Actor(ManagementCaller caller, string? reason) =>
        new(caller.Actor.Subject, caller.Actor.Display, caller.Actor.Scheme, reason);

    // ------------------------------------------------------------------ mutations (16 §4)

    public Task<JobControlResult> CancelJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        JobAsync(caller, jobId, request, Operations.Cancel, ManagementPermissions.JobsCancel, cancellationToken);

    public Task<JobControlResult> RetryJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        JobAsync(caller, jobId, request, Operations.ManualRetry, ManagementPermissions.JobsRetry, cancellationToken);

    public Task<JobControlResult> RerunJobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, CancellationToken cancellationToken = default) =>
        JobAsync(caller, jobId, request, Operations.RerunSucceeded, ManagementPermissions.JobsRerun, cancellationToken);

    async Task<JobControlResult> JobAsync(ManagementCaller caller, Guid jobId, JobControlRequest request, string operation, string permission,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ManagementText.Reason(request.Reason);
        await AuthorizeJobAsync(caller, jobId, null, permission, ct).ConfigureAwait(false);
        var seed = await SeedAsync(request.CommandId, Operations.AdministrativeLifetime, ct).ConfigureAwait(false);
        var result = await controls.JobAsync(operation, new JobTarget(new JobId(jobId), request.ExpectedGeneration, request.ExpectedRevision), caller.Actor,
            request.Reason, seed, ct).ConfigureAwait(false);
        return result.Succeeded ? result : result with
        {
            Code = result.Code == "revision_conflict" && result.ActualGeneration is { } g && g != request.ExpectedGeneration ? "stale_generation" : Map(result.Code),
        };
    }

    public async Task<IReadOnlyList<BulkCancelResult>> CancelJobsAsync(ManagementCaller caller, IReadOnlyList<BulkCancelItem> items, string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ManagementText.Reason(reason);
        if (items.Count is 0 or > MaxBulkItems)
        {
            throw new ManagementValidationException("invalid_request", "A bulk cancel carries 1-100 items.");
        }

        var results = new BulkCancelResult[items.Count];
        await Parallel.ForEachAsync(Enumerable.Range(0, items.Count), new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = cancellationToken },
            async (i, ct) =>
            {
                var item = items[i];
                results[i] = await BulkItemAsync(caller, item, reason, ct).ConfigureAwait(false);
            }).ConfigureAwait(false);
        return results;
    }

    async Task<BulkCancelResult> BulkItemAsync(ManagementCaller caller, BulkCancelItem item, string? reason, CancellationToken ct)
    {
        try
        {
            var r = await CancelJobAsync(caller, item.JobId, new JobControlRequest(item.CommandId, item.ExpectedGeneration, item.ExpectedRevision, reason), ct)
                .ConfigureAwait(false);
            return new BulkCancelResult(item.JobId, !r.Succeeded ? "rejected" : r.Effect == "cancel_requested" ? "accepted" : "applied", r.Code, r);
        }
        catch (ManagementNotFoundException)
        {
            return new BulkCancelResult(item.JobId, "not_found", "not_found", null);
        }
        catch (ManagementDeniedException)
        {
            return new BulkCancelResult(item.JobId, "forbidden", "forbidden", null);
        }
        catch (RunwickException ex) when (ex is StoreUnavailableException or ControlOutcomeUnknownException or ManagementQueryBudgetException)
        {
            return new BulkCancelResult(item.JobId, "unavailable", ex.Code, null);
        }
    }

    public Task<QueueControlResult> PauseQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default) => QueueAsync(caller, queue, true, commandId, expectedRevision, reason, cancellationToken);

    public Task<QueueControlResult> ResumeQueueAsync(ManagementCaller caller, string queue, Guid commandId, long expectedRevision, string? reason = null,
        CancellationToken cancellationToken = default) => QueueAsync(caller, queue, false, commandId, expectedRevision, reason, cancellationToken);

    async Task<QueueControlResult> QueueAsync(ManagementCaller caller, string queue, bool paused, Guid commandId, long expectedRevision, string? reason,
        CancellationToken ct)
    {
        ManagementText.Reason(reason);
        if (!Covers(caller, ManagementPermissions.QueuesRead, queue) && !Covers(caller, ManagementPermissions.QueuesControl, queue))
        {
            throw new ManagementNotFoundException();
        }

        if (!Covers(caller, ManagementPermissions.QueuesControl, queue))
        {
            throw new ManagementDeniedException(ManagementPermissions.QueuesControl);
        }

        var seed = await SeedAsync(commandId, Operations.AdministrativeLifetime, ct).ConfigureAwait(false);
        var r = await controls.QueueAsync(queue, paused, expectedRevision, caller.Actor, reason, seed, ct).ConfigureAwait(false);
        return r.Succeeded ? r : r with { Code = Map(r.Code) };
    }

    public async Task<CapacityControlResult> SetScopeCapacityAsync(ManagementCaller caller, string scopeKind, string scopeKey, Guid commandId, int capacity,
        long expectedPolicyRevision, string? reason = null, CancellationToken cancellationToken = default)
    {
        ManagementText.Reason(reason);
        Require(caller, ManagementPermissions.CapacityEdit);
        var seed = await SeedAsync(commandId, Operations.AdministrativeLifetime, cancellationToken).ConfigureAwait(false);
        var r = await controls.ScopeAsync(scopeKind, scopeKey, capacity, expectedPolicyRevision, caller.Actor, reason, seed, cancellationToken)
            .ConfigureAwait(false);
        return r.Succeeded ? r : r with { Code = Map(r.Code) };
    }

    public Task<ScheduleControlResult> PauseScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision,
        string? reason = null, CancellationToken cancellationToken = default) =>
        ScheduleAsync(caller, ManagementPermissions.SchedulesControl, commandId, reason,
            (seed, ct) => schedules.PauseAsync(key, expectedControlRevision, caller.Actor, reason, seed, ct), cancellationToken);

    public Task<ScheduleControlResult> ResumeScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedControlRevision,
        ResumePolicy policy, string? reason = null, CancellationToken cancellationToken = default) =>
        ScheduleAsync(caller, ManagementPermissions.SchedulesControl, commandId, reason,
            (seed, ct) => schedules.ResumeAsync(key, expectedControlRevision, policy, caller.Actor, reason, seed, ct), cancellationToken);

    public Task<ScheduleControlResult> TriggerScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision,
        string? reason = null, CancellationToken cancellationToken = default) =>
        ScheduleAsync(caller, ManagementPermissions.SchedulesTrigger, commandId, reason,
            (seed, ct) => schedules.TriggerAsync(key, expectedDefinitionRevision, commandId, ProposedJobId(commandId), caller.Actor, reason, seed, ct),
            cancellationToken);

    public Task<ScheduleControlResult> RevalidateScheduleAsync(ManagementCaller caller, string key, Guid commandId, long expectedDefinitionRevision,
        string? reason = null, CancellationToken cancellationToken = default) =>
        ScheduleAsync(caller, ManagementPermissions.SchedulesEdit, commandId, reason,
            (seed, ct) => schedules.RevalidateAsync(key, expectedDefinitionRevision, caller.Actor, reason, seed, ct), cancellationToken);

    async Task<ScheduleControlResult> ScheduleAsync(ManagementCaller caller, string permission, Guid commandId, string? reason,
        Func<CommandSeed, CancellationToken, Task<ScheduleControlResult>> send, CancellationToken ct)
    {
        ManagementText.Reason(reason);
        Require(caller, permission);
        var seed = await SeedAsync(commandId, ScheduleOperations.SchedulerLifetime, ct).ConfigureAwait(false);
        var r = await send(seed, ct).ConfigureAwait(false);
        return r.Succeeded ? r : r with { Code = Map(r.Code) };
    }

    /// <summary>
    /// The client's command ID with the NotAfter of its existing receipt, if any: the fingerprint covers NotAfter, so a
    /// resent command must carry the first send's deadline to replay rather than conflict.
    /// </summary>
    async Task<CommandSeed> SeedAsync(Guid commandId, TimeSpan lifetime, CancellationToken ct)
    {
        if (commandId == Guid.Empty)
        {
            throw new ManagementValidationException("invalid_request", "commandId is required.");
        }

        await runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        var existing = await Read(() => Store.CommandNotAfterAsync(App, commandId, ct)).ConfigureAwait(false);
        return new CommandSeed(commandId, existing ?? CommandSeed.Ms(time.GetUtcNow() + lifetime));
    }

    /// <summary>A trigger's proposed job ID, derived from its command ID so a resent trigger has the same body.</summary>
    static Guid ProposedJobId(Guid commandId)
    {
        Span<byte> bytes = stackalloc byte[32];
        SHA256.HashData([.. "runwick.trigger-job:"u8, .. commandId.ToByteArray()], bytes);
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x80); // version 8 (custom), little-endian field layout
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes[..16]);
    }

    /// <summary>Store rejection codes to the API's (16 §4); not-found codes become <see cref="ManagementNotFoundException"/>.</summary>
    static string? Map(string? code) => code switch
    {
        "job_not_found" or "queue_not_registered" or "scope_not_registered" or "schedule_not_found" => throw new ManagementNotFoundException(),
        "invalid_request" => throw new ManagementValidationException("invalid_request", "The store rejected the request."),
        "invalid_retention_policy" => throw new ManagementValidationException("invalid_retention_policy", "The retention policy is not valid."),
        "command_expired" => throw new StoreUnavailableException("The command expired before it applied."),
        "revision_conflict" => "stale_revision",
        "job_terminal" or "schedule_disabled" or "episode_unresolved" or "cutoff_in_future" => "invalid_state",
        "uniqueness_conflict" => "unique_conflict",
        _ => code,
    };
}
