using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Serialization;

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

    /// <summary>
    /// Exact job figures over a created/ended range (16 §2.4); the range defaults to the last 24 hours, at most 31 days, in 1-96
    /// buckets. Needs <c>Jobs.Read</c>; only the queues it covers are counted (a queue outside it reads as empty).
    /// </summary>
    Task<JobStats> GetJobStatsAsync(ManagementCaller caller, JobStatsQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Every job name the registry, the stored jobs or a schedule knows, by name (16 §2.6). Needs <c>Jobs.Read</c>; a name shows
    /// when one of its queues is covered.
    /// </summary>
    Task<IReadOnlyList<JobTypeSummary>> ListJobTypesAsync(ManagementCaller caller, CancellationToken cancellationToken = default);

    /// <summary>Schedule counts now and occurrences in the range (default the last 24 hours, at most 31 days). Needs <c>Schedules.Read</c>.</summary>
    Task<ScheduleStats> GetScheduleStatsAsync(ManagementCaller caller, DateTimeOffset? from = null, DateTimeOffset? to = null,
        CancellationToken cancellationToken = default);

    /// <summary>Audit counts in the range (default the last 24 hours, at most 31 days). Needs <c>Audit.Read</c>.</summary>
    Task<AuditStats> GetAuditStatsAsync(ManagementCaller caller, DateTimeOffset? from = null, DateTimeOffset? to = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// A schedule with its current definition and its newest occurrences (default 50, at most 200). Needs <c>Schedules.Read</c>;
    /// each occurrence's job state is shown only with <c>Jobs.Read</c> on the definition's queue.
    /// </summary>
    Task<ScheduleDetail> GetScheduleAsync(ManagementCaller caller, string key, int? limit = null, CancellationToken cancellationToken = default);

    Task<ManagementPage<AuditEntry>> ListAuditAsync(ManagementCaller caller, AuditQuery query, CancellationToken cancellationToken = default);

    /// <summary>Autoscaler target changes, newest first (21 §6). Needs <c>Capacity.Read</c>.</summary>
    Task<ManagementPage<ScalingEventSummary>> ListScalingEventsAsync(ManagementCaller caller, ScalingEventQuery query,
        CancellationToken cancellationToken = default);

    /// <summary>Reads the host's cached observation without querying the store.</summary>
    Task<ManagementStoreSnapshot> GetStoreSnapshotAsync(ManagementCaller caller, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(caller);
        if (!caller.Grants.Any(g => g.Permission == ManagementPermissions.SettingsRead))
        {
            throw new ManagementDeniedException(ManagementPermissions.SettingsRead);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ManagementStoreSnapshot.Unobserved(DateTimeOffset.UtcNow));
    }

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

    /// <summary>Manual retries as <see cref="CancelJobsAsync"/>: at most 100 items, 4 at a time, each authorized and audited alone.</summary>
    Task<IReadOnlyList<BulkRetryResult>> RetryJobsAsync(ManagementCaller caller, IReadOnlyList<BulkRetryItem> items, string? reason = null,
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

    /// <summary>
    /// Job events after a plain position cursor (19 §6.1). Needs <c>Jobs.Read</c> for every queue; <c>ActorId</c> is
    /// filled only with <c>Audit.Read</c>. Limit 1-1,000 (default 100).
    /// </summary>
    Task<EventsPage> ListEventsAsync(ManagementCaller caller, string? after = null, int? limit = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a registered job by name with a JSON payload and writes a <c>Dispatch</c> audit row in the same
    /// transaction (19 §6.2). Needs <c>Jobs.Dispatch</c> on the resulting queue.
    /// </summary>
    Task<ManagementDispatchResult> DispatchAsync(ManagementCaller caller, ManagementDispatchRequest request, CancellationToken cancellationToken = default);
}

internal sealed class ManagementSession(
    RunwickRuntime runtime, RunwickBuilder builder, IManagementQueryStore? store, ControlService controls, ScheduleControlService schedules,
    ManagementCursors cursors, ManagementOptions options, TimeProvider time, DispatchPreparer preparer, LocalAdmission admission, RunwickHealthService health) : IManagementSession
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
        var queue = await Read(() => Store.QueueOfAsync(App, jobId, attemptId, ct), ct).ConfigureAwait(false);
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

    /// <summary>
    /// Runs a read; a statement timeout is <c>query_budget_exceeded</c>, other store failures stay unavailable. A read the
    /// caller cancelled is a cancellation: the stores report it as not applied, which is not the store being unavailable.
    /// </summary>
    static async Task<T> Read<T>(Func<Task<T>> read, CancellationToken ct)
    {
        try
        {
            return await read().ConfigureAwait(false);
        }
        catch (StoreNotAppliedException ex) when (ct.IsCancellationRequested)
        {
            throw new OperationCanceledException("The caller cancelled the management read.", ex, ct);
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
            (from, to) = Range(query.From, query.To);
        }

        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        if (query.Queue is { } q && queues is not null && !queues.Contains(q, StringComparer.Ordinal))
        {
            // A queue outside the caller's scope lists nothing, as if it were empty.
            return new ManagementPage<JobSummary>([], time.GetUtcNow(), null);
        }

        var read = await Read(() => Store.ListJobsAsync(App, queues, new JobListFilter(query.Queue, query.State, query.JobName, from, to), after, limit + 1,
            cancellationToken), cancellationToken).ConfigureAwait(false);
        return Page(read, limit, "jobs", filters, hash, j => [Ms(from), Ms(to), Ms(j.CreatedAt), j.JobId.ToString("D")]);
    }

    public async Task<JobDetail> GetJobAsync(ManagementCaller caller, Guid jobId, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.JobsRead);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.GetJobAsync(App, jobId, cancellationToken), cancellationToken).ConfigureAwait(false);
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
        var read = await Read(() => Store.ListQueuesAsync(App, queues, after, size + 1, cancellationToken), cancellationToken).ConfigureAwait(false);
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
        var read = await Read(() => Store.ListWorkersAsync(App, includeExpired, after, size + 1, cancellationToken), cancellationToken).ConfigureAwait(false);
        var jobs = TryScope(caller, ManagementPermissions.JobsRead, out var readable);
        var items = read.Value.Select(w => w.Detail is JsonObject d ? w with { Detail = VisibleDetail(d, jobs, readable) } : w).ToList();
        return Page(new StoreRead<IReadOnlyList<WorkerSummary>>(items, read.ObservedAt), size, "workers", filters, "*",
            w => [Ms(w.HeartbeatAt), w.InstanceId.ToString("D")]);
    }

    /// <summary>Running and recent attempts name jobs: only those in queues the caller may read jobs in, none without <c>Jobs.Read</c>.</summary>
    static JsonObject VisibleDetail(JsonObject detail, bool jobs, IReadOnlyList<string>? queues)
    {
        foreach (var list in (string[])["running", "recent"])
        {
            if (!jobs)
            {
                detail.Remove(list);
            }
            else if (queues is not null && detail[list] is JsonArray items)
            {
                detail[list] = new JsonArray([.. items.Where(x => x is JsonObject o && o["queue"] is JsonValue v && v.TryGetValue<string>(out var q)
                    && queues.Contains(q, StringComparer.Ordinal)).Select(x => x!.DeepClone())]);
            }
        }

        return detail;
    }

    public const int MaxStatsBuckets = 96;
    static readonly TimeSpan DefaultStatsRange = TimeSpan.FromDays(1);
    static readonly string[] LiveKinds = ["pending", "scheduled", "reserved", "running"];

    public async Task<JobStats> GetJobStatsAsync(ManagementCaller caller, JobStatsQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var queues = Require(caller, ManagementPermissions.JobsRead);
        if (query.Buckets is < 1 or > MaxStatsBuckets)
        {
            throw new ManagementValidationException("invalid_request", "buckets must be 1-96.");
        }

        var (from, to) = StatsRange(query.From, query.To);
        var groups = query.WorkerInstanceId is null;
        var filter = new JobStatsFilter(from, to, query.Buckets, query.Queue, query.JobName, query.WorkerInstanceId, queues,
            groups && (query.ByQueue || query.ByJobName));
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        if (query.Queue is { } q && queues is not null && !queues.Contains(q, StringComparer.Ordinal))
        {
            // A queue outside the caller's scope reads as empty, as ListJobs does.
            return BuildStats(filter, new JobStatsCells([], [], [], [], [], []), time.GetUtcNow(), false, false);
        }

        var read = await Read(() => Store.GetJobStatsAsync(App, filter, cancellationToken), cancellationToken).ConfigureAwait(false);
        return BuildStats(filter, read.Value, read.ObservedAt, groups && query.ByQueue, groups && query.ByJobName);
    }

    /// <summary>A stats range at millisecond precision: default the last 24 hours, From before To, at most 31 days.</summary>
    (DateTimeOffset From, DateTimeOffset To) StatsRange(DateTimeOffset? from, DateTimeOffset? to)
    {
        var end = DateTimeOffset.FromUnixTimeMilliseconds((to ?? time.GetUtcNow()).ToUnixTimeMilliseconds());
        var start = DateTimeOffset.FromUnixTimeMilliseconds((from ?? end - DefaultStatsRange).ToUnixTimeMilliseconds());
        return start >= end || end - start > MaxRange
            ? throw new ManagementValidationException("invalid_request", "The range must be ordered, not empty and at most 31 days.")
            : (start, end);
    }

    static JobStats BuildStats(JobStatsFilter f, JobStatsCells c, DateTimeOffset observedAt, bool byQueue, bool byName)
    {
        var durations = c.Durations.ToDictionary(d => (d.Queue, d.JobName, d.Slice));
        JobDurationStats Duration(string? queue, string? name, int? slice) =>
            durations.TryGetValue((queue, name, slice), out var d) ? new JobDurationStats(d.Count, d.MedianMs, d.P95Ms, d.MaxMs) : new JobDurationStats(0, null, null, null);

        (Dictionary<string, long> States, long Retries, JobStatsBucket[] Buckets, Dictionary<string, long> Now, DateTimeOffset? Oldest) Figures(
            Func<string, string, bool> match, string? queue, string? name)
        {
            var states = JobStateNames.ToDictionary(n => n, _ => 0L, StringComparer.Ordinal);
            var now = LiveKinds.ToDictionary(n => n, _ => 0L, StringComparer.Ordinal);
            var created = new long[f.Buckets];
            var ended = new long[f.Buckets, 3];
            long retries = 0;
            DateTimeOffset? oldest = null;
            foreach (var x in c.Created.Where(x => match(x.Queue, x.JobName)))
            {
                states[x.State] = states.GetValueOrDefault(x.State) + x.Jobs;
                created[x.Slice] += x.Jobs;
                retries += x.Retries;
            }

            foreach (var x in c.Ended.Where(x => match(x.Queue, x.JobName)))
            {
                ended[x.Slice, x.State switch { "succeeded" => 0, "failed" => 1, _ => 2 }] += x.Jobs;
            }

            foreach (var x in c.Live.Where(x => match(x.Queue, x.JobName)))
            {
                now[x.Kind] = now.GetValueOrDefault(x.Kind) + x.Jobs;
                if (x.OldestEligibleAt is { } e && (oldest is null || e < oldest))
                {
                    oldest = e;
                }
            }

            var buckets = new JobStatsBucket[f.Buckets];
            for (var i = 0; i < f.Buckets; i++)
            {
                var d = Duration(queue, name, i);
                buckets[i] = new JobStatsBucket(f.SliceStart(i), created[i], ended[i, 0], ended[i, 1], ended[i, 2], d.MedianMs, d.P95Ms);
            }

            return (states, retries, buckets, now, oldest);
        }

        JobStatsGroup Group(string key, Func<JobStatsLast, bool> mine, Func<string, string, bool> match, string? queue, string? name)
        {
            var g = Figures(match, queue, name);
            var last = c.Last.Where(mine).OrderByDescending(l => l.CreatedAt).ThenByDescending(l => l.JobId).Select(l => (JobStatsLast?)l).FirstOrDefault();
            return new JobStatsGroup(key, g.States, g.Retries, Duration(queue, name, null), last?.CreatedAt, last?.State, last?.JobId, g.Buckets, g.Now,
                g.Oldest);
        }

        IEnumerable<string> Keys(Func<(string Queue, string JobName), string> key) =>
            c.Created.Select(x => key((x.Queue, x.JobName))).Concat(c.Ended.Select(x => key((x.Queue, x.JobName))))
                .Concat(c.Live.Select(x => key((x.Queue, x.JobName)))).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal);

        var all = Figures((_, _) => true, null, null);
        return new JobStats(f.From, f.To, observedAt, all.States, all.Retries, Duration(null, null, null), all.Buckets, all.Now, all.Oldest,
            byQueue ? [.. Keys(x => x.Queue).Select(q => Group(q, l => l.Queue == q, (xq, _) => xq == q, q, null))] : null,
            byName ? [.. Keys(x => x.JobName).Select(n => Group(n, l => l.JobName == n, (_, xn) => xn == n, null, n))] : null,
            c.ErrorCodes);
    }

    public async Task<IReadOnlyList<JobTypeSummary>> ListJobTypesAsync(ManagementCaller caller, CancellationToken cancellationToken = default)
    {
        var queues = Require(caller, ManagementPermissions.JobsRead);
        bool Readable(string queue) => queues is null || queues.Contains(queue, StringComparer.Ordinal);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListJobNamesAsync(App, cancellationToken), cancellationToken).ConfigureAwait(false);
        var names = runtime.Registry.Compiled.Where(j => Readable(j.Queue)).Select(j => j.Name)
            .Concat(read.Value.Stored.Where(s => Readable(s.Queue)).Select(s => s.JobName))
            .Concat(read.Value.Schedules.Where(s => Readable(s.Queue)).Select(s => s.JobName))
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal);
        return [.. names.Select(name =>
        {
            var job = runtime.Registry.ByName(name);
            return new JobTypeSummary(name, job is not null, job?.Queue, job?.Priority, job?.CurrentVersion, job is null ? null : JobDefinition(job),
                [.. read.Value.Schedules.Where(s => s.JobName == name).Select(s => s.Key).Order(StringComparer.Ordinal)],
                read.Value.Stored.Any(s => s.JobName == name));
        })];
    }

    /// <summary>The registered contract as plain values: policy, scopes and type names, never payload values or selector code.</summary>
    static JsonObject JobDefinition(CompiledJob job)
    {
        static JsonObject Retry(RetryPolicy r) => new()
        {
            ["backoff"] = r.Backoff, ["jitter"] = r.JitterKind, ["initialDelayMs"] = r.InitialDelayMs, ["maximumDelayMs"] = r.MaximumDelayMs,
        };

        var p = job.Policy;
        return new JsonObject
        {
            ["payloadType"] = job.PayloadType.FullName,
            ["payloadVersions"] = new JsonArray([.. job.Routes.Keys.Order().Select(v => (JsonNode?)v)]),
            ["handlerType"] = job.HandlerType?.FullName,
            ["maxExecutions"] = p.MaxExecutions,
            ["executionTimeoutMs"] = p.ExecutionTimeoutMs,
            ["retry"] = Retry(p.Retry),
            ["maxUnstartedAbandonments"] = p.MaxConsecutiveUnstartedAbandonments,
            ["unstartedRetry"] = Retry(p.UnstartedRetry),
            ["jobTypeCapacity"] = job.JobTypeCapacity,
            ["resources"] = new JsonArray([.. job.Resources.Select(r => (JsonNode?)new JsonObject { ["capacity"] = r.Capacity })]),
            ["rates"] = new JsonArray([.. job.Rates.Select(r => (JsonNode?)new JsonObject { ["kind"] = r.Kind, ["limit"] = r.Limit, ["windowMs"] = r.WindowMs })]),
            ["unique"] = job.ActiveSelector is not null,
            ["windowMs"] = job.WindowSelector is { } w ? (long)w.Window.TotalMilliseconds : null,
            ["middleware"] = new JsonArray([.. job.MiddlewareChain.Select(t => (JsonNode?)t.FullName)]),
        };
    }

    public async Task<ScheduleStats> GetScheduleStatsAsync(ManagementCaller caller, DateTimeOffset? from = null, DateTimeOffset? to = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SchedulesRead);
        var (start, end) = StatsRange(from, to);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        return (await Read(() => Store.GetScheduleStatsAsync(App, start, end, cancellationToken), cancellationToken).ConfigureAwait(false)).Value;
    }

    public const int TopAuditActors = 5;

    public async Task<AuditStats> GetAuditStatsAsync(ManagementCaller caller, DateTimeOffset? from = null, DateTimeOffset? to = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.AuditRead);
        var (start, end) = StatsRange(from, to);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var s = (await Read(() => Store.GetAuditStatsAsync(App, start, end, cancellationToken), cancellationToken).ConfigureAwait(false)).Value;
        return s with
        {
            Operations = [.. s.Operations.OrderByDescending(o => o.Count).ThenBy(o => o.Operation, StringComparer.Ordinal)],
            Actors = [.. s.Actors.OrderByDescending(a => a.Count).ThenBy(a => a.Actor, StringComparer.Ordinal).Take(TopAuditActors)],
        };
    }

    public async Task<ScheduleDetail> GetScheduleAsync(ManagementCaller caller, string key, int? limit = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        Require(caller, ManagementPermissions.SchedulesRead);
        var size = PageSize(limit);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.GetScheduleAsync(App, key, size, cancellationToken), cancellationToken).ConfigureAwait(false);
        var detail = read.Value ?? throw new ManagementNotFoundException();
        return Covers(caller, ManagementPermissions.JobsRead, detail.Definition.Queue)
            ? detail
            : detail with { Occurrences = [.. detail.Occurrences.Select(o => o with { JobState = null })] };
    }

    public async Task<ManagementPage<ScopeSummary>> ListScopesAsync(ManagementCaller caller, string? kind = null, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.CapacityRead);
        var size = PageSize(limit);
        (string, string)? after = cursor is null ? null : cursors.Open(cursor, "scopes", App, kind ?? "", "*") is var k ? (k[0], k[1]) : null;
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListScopesAsync(App, kind, after, size + 1, cancellationToken), cancellationToken).ConfigureAwait(false);
        return Page(read, size, "scopes", kind ?? "", "*", s => [s.Kind, s.Key]);
    }

    public async Task<ManagementPage<ScheduleSummary>> ListSchedulesAsync(ManagementCaller caller, int? limit = null, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SchedulesRead);
        var size = PageSize(limit);
        var after = cursor is null ? null : cursors.Open(cursor, "schedules", App, "", "*")[0];
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListSchedulesAsync(App, after, size + 1, cancellationToken), cancellationToken).ConfigureAwait(false);
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
            cancellationToken), cancellationToken).ConfigureAwait(false);
        var items = read.Value.Select(a => a with
        {
            // Only allowlisted detail keys leave the store.
            Details = new([.. a.Details.Where(d => AllowedAuditDetails.Contains(d.Key)).Select(d => KeyValuePair.Create(d.Key, d.Value?.DeepClone()))]),
        }).ToList();
        return Page(new StoreRead<IReadOnlyList<AuditEntry>>(items, read.ObservedAt), size, "audit", filters, "*",
            a => [a.AuditId.ToString(CultureInfo.InvariantCulture)]);
    }

    public async Task<ManagementPage<ScalingEventSummary>> ListScalingEventsAsync(ManagementCaller caller, ScalingEventQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        Require(caller, ManagementPermissions.CapacityRead);
        var limit = PageSize(query.Limit);
        var filters = string.Join('|', query.InstanceId?.ToString("D"), query.Pool, query.From is { } f ? Ms(f) : "", query.To is { } t ? Ms(t) : "");
        DateTimeOffset from, to;
        (DateTimeOffset, Guid)? after = null;
        if (query.Cursor is { } cursor)
        {
            var k = cursors.Open(cursor, "scaling-events", App, filters, "*");
            (from, to, after) = (FromMs(k[0]), FromMs(k[1]), (FromMs(k[2]), Guid.Parse(k[3])));
        }
        else
        {
            (from, to) = Range(query.From, query.To);
        }

        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var read = await Read(() => Store.ListScalingEventsAsync(App, new ScalingEventFilter(query.InstanceId, query.Pool, from, to), after, limit + 1,
            cancellationToken), cancellationToken).ConfigureAwait(false);
        return Page(read, limit, "scaling-events", filters, "*", e => [Ms(from), Ms(to), Ms(e.CreatedAt), e.EventId.ToString("D")]);
    }

    /// <summary>The created range: default the last 7 days, ordered, at most 31 days.</summary>
    (DateTimeOffset From, DateTimeOffset To) Range(DateTimeOffset? from, DateTimeOffset? to)
    {
        var end = to ?? time.GetUtcNow();
        var start = from ?? end - DefaultRange;
        return start > end || end - start > MaxRange
            ? throw new ManagementValidationException("invalid_request", "The created range must be ordered and at most 31 days.")
            : (start, end);
    }

    public Task<ManagementStoreSnapshot> GetStoreSnapshotAsync(ManagementCaller caller, CancellationToken cancellationToken = default)
    {
        Require(caller, ManagementPermissions.SettingsRead);
        cancellationToken.ThrowIfCancellationRequested();
        var (last, successfulAt, age, failure) = health.StoreObservation();
        var fresh = failure is null && age is { } observed && observed < 3 * RunwickHealthService.RefreshPeriod;
        var retained = age < TimeSpan.FromMinutes(5) ? last : null;
        var availability = fresh ? StoreSnapshotStatus.Available : retained is null ? StoreSnapshotStatus.Unavailable : StoreSnapshotStatus.Stale;
        var connection = new StoreConnectionObservation(StoreSnapshotScope.Process,
            fresh ? StoreReachability.Reachable : failure == "store_unavailable" ? StoreReachability.Unavailable : StoreReachability.Unknown,
            fresh ? "ok" : failure ?? (age is null ? "not_observed" : "stale"));
        var provider = builder.StoreProviders.FirstOrDefault()?.Name switch
        {
            "postgres" => "postgres",
            "redis" => "redis",
            "sqlite" => "sqlite",
            _ => "unknown",
        };
        var profile = runtime.Store.AcknowledgementProfile switch
        {
            AcknowledgementProfiles.PostgresSynchronousCommit => AcknowledgementProfiles.PostgresSynchronousCommit,
            AcknowledgementProfiles.SqliteWalFull => AcknowledgementProfiles.SqliteWalFull,
            AcknowledgementProfiles.RedisAofAlways => AcknowledgementProfiles.RedisAofAlways,
            _ => null,
        };
        var migration = retained?.MigrationMode is "normal" or "expand" or "maintenance" ? retained.MigrationMode : "unknown";
        var gate = retained?.Application;
        var admission = gate?.AdmissionMode is "open" or "drain" or "maintenance" ? gate.AdmissionMode : "unknown";

        var store = runtime.Store;
        List<StoreProviderCapability> capabilities = [];
        if (store is IReserveAndBeginStore) capabilities.Add(StoreProviderCapability.BatchClaim);
        if (store is IFinishManyStore) capabilities.Add(StoreProviderCapability.BatchFinish);
        if (store is ILeaseBatchStore) capabilities.Add(StoreProviderCapability.BatchRenewal);
        if (store is IRateControlStore) capabilities.Add(StoreProviderCapability.RateAdmission);
        if (store is IScheduleStore) capabilities.Add(StoreProviderCapability.Scheduling);
        if (store is IRetentionStore) capabilities.Add(StoreProviderCapability.Retention);
        if (store is IWakeNotificationSource) capabilities.Add(StoreProviderCapability.WakeNotifications);
        if (store is IOutboxDestinationStore) capabilities.Add(StoreProviderCapability.OutboxDestination);
        var engineVersion = retained?.EngineVersion;
        return Task.FromResult(new ManagementStoreSnapshot(App, provider, time.GetUtcNow(), successfulAt, age?.TotalSeconds, connection,
            [.. health.Readiness().Select(r => new StoreRoleObservation(StoreSnapshotScope.Process, r))],
            new(StoreSnapshotScope.Application, gate is null ? StoreSnapshotStatus.Unavailable : availability, gate is null ? null :
                new StoreApplicationObservation(admission, gate.RequiredProtocolEpoch,
                    gate.RequiredProtocolEpoch == retained!.ProtocolEpoch &&
                    gate.RequiredCapabilities.All(c => RunwickRuntime.KnownCapabilities.TryGetValue(c.Key, out var v) && v >= c.Value))),
            new(StoreSnapshotScope.Store, availability, retained is null ? null :
                new StoreIdentityObservation(retained.StoreId, retained.SchemaRevision, retained.ProtocolEpoch, migration)),
            new(StoreSnapshotScope.Process, profile is null ? StoreSnapshotStatus.Unavailable : StoreSnapshotStatus.Available, profile is null ? null :
                new StoreProfileObservation(profile, provider == "sqlite" && profile == AcknowledgementProfiles.SqliteWalFull && retained is not null
                    ? availability : StoreSnapshotStatus.Unavailable)),
            new(StoreSnapshotScope.Process, StoreSnapshotStatus.Available, capabilities.AsReadOnly()),
            new(provider == "sqlite" ? StoreSnapshotScope.Process : StoreSnapshotScope.ProviderNode,
                engineVersion is null ? StoreSnapshotStatus.Unavailable : availability, engineVersion),
            new(StoreSnapshotScope.Store, StoreSnapshotStatus.Unavailable, null),
            new(StoreSnapshotScope.ProviderNode, provider == "sqlite" ? StoreSnapshotStatus.Unsupported : StoreSnapshotStatus.Unavailable, null)));
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

        var retention = Store is IRetentionStore r ? (await Read(() => r.GetRetentionAsync(App, cancellationToken), cancellationToken).ConfigureAwait(false)).Value.Policy.Revision : 0;
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
        var read = await Read(() => Retention.GetRetentionAsync(App, cancellationToken), cancellationToken).ConfigureAwait(false);
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
        if (r.Value is { } edited && !r.Replayed)
        {
            Telemetry.RunwickLog.RetentionPolicyEdited(runtime.Logger, edited.Revision);
        }

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

        if (reply.Value is { Status: "disclosed" })
        {
            Telemetry.RunwickLog.ManagementDisclosure(runtime.Logger, kind, jobId is null ? "attempt" : "job", (jobId ?? attemptId)!.Value, command.CommandId);
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

    public Task<IReadOnlyList<BulkCancelResult>> CancelJobsAsync(ManagementCaller caller, IReadOnlyList<BulkCancelItem> items, string? reason = null,
        CancellationToken cancellationToken = default) =>
        BulkAsync(items, reason, "cancel", (item, ct) => BulkItemAsync(item.JobId,
            ct => CancelJobAsync(caller, item.JobId, new JobControlRequest(item.CommandId, item.ExpectedGeneration, item.ExpectedRevision, reason), ct),
            r => r.Effect == "cancel_requested" ? "accepted" : "applied", (id, status, code, r) => new BulkCancelResult(id, status, code, r), ct),
            cancellationToken);

    public Task<IReadOnlyList<BulkRetryResult>> RetryJobsAsync(ManagementCaller caller, IReadOnlyList<BulkRetryItem> items, string? reason = null,
        CancellationToken cancellationToken = default) =>
        BulkAsync(items, reason, "retry", (item, ct) => BulkItemAsync(item.JobId,
            ct => RetryJobAsync(caller, item.JobId, new JobControlRequest(item.CommandId, item.ExpectedGeneration, item.ExpectedRevision, reason), ct),
            _ => "applied", (id, status, code, r) => new BulkRetryResult(id, status, code, r), ct),
            cancellationToken);

    static async Task<IReadOnlyList<TResult>> BulkAsync<TItem, TResult>(IReadOnlyList<TItem> items, string? reason, string verb,
        Func<TItem, CancellationToken, Task<TResult>> one, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(items);
        ManagementText.Reason(reason);
        if (items.Count is 0 or > MaxBulkItems)
        {
            throw new ManagementValidationException("invalid_request", $"A bulk {verb} carries 1-100 items.");
        }

        var results = new TResult[items.Count];
        await Parallel.ForEachAsync(Enumerable.Range(0, items.Count), new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = cancellationToken },
            async (i, ct) => results[i] = await one(items[i], ct).ConfigureAwait(false)).ConfigureAwait(false);
        return results;
    }

    static async Task<TResult> BulkItemAsync<TResult>(Guid jobId, Func<CancellationToken, Task<JobControlResult>> control, Func<JobControlResult, string> succeeded,
        Func<Guid, string, string?, JobControlResult?, TResult> result, CancellationToken ct)
    {
        try
        {
            var r = await control(ct).ConfigureAwait(false);
            return result(jobId, r.Succeeded ? succeeded(r) : "rejected", r.Code, r);
        }
        catch (ManagementNotFoundException)
        {
            return result(jobId, "not_found", "not_found", null);
        }
        catch (ManagementDeniedException)
        {
            return result(jobId, "forbidden", "forbidden", null);
        }
        catch (RunwickException ex) when (ex is StoreUnavailableException or ControlOutcomeUnknownException or ManagementQueryBudgetException)
        {
            return result(jobId, "unavailable", ex.Code, null);
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
        var existing = await Read(() => Store.CommandNotAfterAsync(App, commandId, ct), ct).ConfigureAwait(false);
        return new CommandSeed(commandId, existing ?? CommandSeed.Ms(time.GetUtcNow() + lifetime));
    }

    // ------------------------------------------------------------------ events and dispatch (SP-032, 19 §6)

    public const int DefaultEventPage = 100, MaxEventPage = 1_000;

    public async Task<EventsPage> ListEventsAsync(ManagementCaller caller, string? after = null, int? limit = null, CancellationToken cancellationToken = default)
    {
        // Events carry no queue column: only an application-wide Jobs.Read may read them.
        if (Require(caller, ManagementPermissions.JobsRead) is not null)
        {
            throw new ManagementDeniedException(ManagementPermissions.JobsRead);
        }

        var size = limit switch
        {
            null => DefaultEventPage,
            < 1 => throw new ManagementValidationException("invalid_request", "limit must be at least 1."),
            _ => Math.Min(limit.Value, MaxEventPage),
        };
        var cursor = after is null ? (EventCursor?)null : ParseEventCursor(after);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        var page = await Read(() => controls.Store.EventsSinceAsync(App, cursor, size, cancellationToken), cancellationToken).ConfigureAwait(false);
        var actors = TryScope(caller, ManagementPermissions.AuditRead, out _);
        return new EventsPage(
            [.. page.Events.Select(e => new ManagementEvent(e.EventId, e.Kind, e.JobId, e.AttemptId, e.CommandId, e.OccurredAt, actors ? e.ActorId : null, e.Details))],
            time.GetUtcNow(), page.Next is { } n ? FormatEventCursor(n) : after ?? "0.0", page.Partial);
    }

    internal static string FormatEventCursor(EventCursor c) =>
        c.TxId.ToString(CultureInfo.InvariantCulture) + "." + c.EventId.ToString(CultureInfo.InvariantCulture);

    static EventCursor ParseEventCursor(string s) =>
        s.Split('.') is [var tx, var id]
        && ulong.TryParse(tx, NumberStyles.None, CultureInfo.InvariantCulture, out var t)
        && long.TryParse(id, NumberStyles.None, CultureInfo.InvariantCulture, out var e)
            ? new EventCursor(t, e)
            : throw new ManagementValidationException("invalid_cursor", "The events cursor is not valid.");

    public async Task<ManagementDispatchResult> DispatchAsync(ManagementCaller caller, ManagementDispatchRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ManagementText.Reason(request.Reason);
        if (request.CommandId == Guid.Empty)
        {
            throw new ManagementValidationException("invalid_request", "commandId is required.");
        }

        // Job names are not hidden resources: an unknown name is 404 for every caller.
        var job = (request.JobName is { } name ? runtime.Registry.ByName(name) : null)
            ?? throw new ManagementNotFoundException("job_not_registered", "The job name is not registered in this management host.");
        var queue = request.Queue ?? job.Queue;
        if (!Covers(caller, ManagementPermissions.JobsDispatch, queue))
        {
            throw new ManagementDeniedException(ManagementPermissions.JobsDispatch);
        }

        if (request.PayloadVersion is { } version && version != job.CurrentVersion)
        {
            throw new ManagementValidationException("payload_version_unsupported",
                $"The current payload version of {job.Name} is {job.CurrentVersion.ToString(CultureInfo.InvariantCulture)}.");
        }

        var bound = Bind(job, request.Payload);
        await runtime.EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        using var lease = await admission.TryEnterAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new ManagementUnavailableException("busy", "Local producer admission is full.");
        DispatchIntent intent;
        IReadOnlyList<ScopeRequirement> scopes;
        CanonicalValue canonical;
        try
        {
            (intent, scopes, canonical) = preparer.PrepareItem(bound,
                new DispatchOptions { Queue = request.Queue, Priority = request.Priority, NotBefore = request.NotBefore });
        }
        catch (DispatchValidationException ex)
        {
            throw new ManagementValidationException(
                ex.Code == RunwickErrorCodes.PayloadTooLarge ? ex.Code : ex.Code.StartsWith("payload_", StringComparison.Ordinal) ? "payload_invalid" : "invalid_request",
                $"The dispatch is not valid ({ex.Code}).");
        }

        // A resend must carry the same body: the job ID comes from the command ID, and no per-request trace context is kept.
        intent = intent with { ProposedJobId = DispatchJobId(request.CommandId), DiagnosticMetadata = new Dictionary<string, string>(StringComparer.Ordinal) };
        var seed = await SeedAsync(request.CommandId, Operations.AdministrativeLifetime, cancellationToken).ConfigureAwait(false);
        var digests = Fingerprints.ForDispatch(runtime.StoreId, App, seed.CommandId, seed.NotAfter.ToUnixTimeMilliseconds(), intent, canonical);
        var item = new DispatchItem(intent, scopes, digests) { Rates = preparer.RatesFor(intent) };
        var command = new StoreCommand<ManagementDispatchStoreRequest>(App, seed.CommandId, seed.NotAfter,
            new ManagementDispatchStoreRequest(item, Actor(caller, request.Reason)));
        var reply = await controls.ResendAsync(seed.NotAfter, t => controls.Store.ManagementDispatchAsync(command, t), cancellationToken).ConfigureAwait(false);
        if (reply.Value is { } v)
        {
            return new ManagementDispatchResult(v.Dispatch.JobId, v.Dispatch.Disposition == DispatchDisposition.Created ? "created" : "reused",
                v.Generation, v.Revision, v.Queue, reply.Replayed, request.CommandId);
        }

        throw reply.Status switch
        {
            StoreStatus.UniquenessConflict => new ManagementConflictException("unique_conflict", "A live uniqueness binding conflicts with this dispatch."),
            StoreStatus.CommandConflict => new ManagementConflictException("command_conflict", "This command ID was used with another body."),
            StoreStatus.AdmissionClosed => new ManagementUnavailableException("admission_closed", "Application admission is closed."),
            StoreStatus.InvalidRequest => new ManagementValidationException("invalid_request", "The store rejected the dispatch."),
            _ => new StoreUnavailableException($"The dispatch was not applied ({reply.Error?.Code})."),
        };
    }

    /// <summary>Binds the JSON payload to the registered type; errors name the JSON path, never a value.</summary>
    static IQueueJob Bind(CompiledJob job, System.Text.Json.Nodes.JsonNode? payload)
    {
        var utf8 = payload is null ? "null"u8.ToArray() : JsonSerializer.SerializeToUtf8Bytes(payload);
        if (utf8.Length > PayloadBounds.MaxPayloadBytes)
        {
            throw new ManagementValidationException(RunwickErrorCodes.PayloadTooLarge, $"The payload exceeds {PayloadBounds.MaxPayloadBytes} bytes.");
        }

        try
        {
            JsonPayloadScanner.ScanPayload(utf8);
            return JsonSerializer.Deserialize(utf8, job.PayloadType, RunwickJsonProfile.V1.Options) as IQueueJob
                ?? throw new ManagementValidationException("payload_invalid", $"The payload does not bind to {job.Name} at $.");
        }
        catch (JsonException ex)
        {
            throw new ManagementValidationException("payload_invalid", $"The payload does not bind to {job.Name} at {ex.Path ?? "$"}.");
        }
        catch (PayloadValidationException ex)
        {
            throw new ManagementValidationException(ex.Code == RunwickErrorCodes.PayloadTooLarge ? ex.Code : "payload_invalid", $"The payload is not valid ({ex.Code}).");
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
        {
            throw new ManagementValidationException("payload_invalid", $"The payload does not bind to {job.Name}.");
        }
    }

    /// <summary>The dispatch's proposed job ID (19 §6.2): SHA-256(<c>runwick.mgmt.dispatch.v1</c> ‖ 0x00 ‖ command ID, big-endian), version 8.</summary>
    internal static Guid DispatchJobId(Guid commandId)
    {
        Span<byte> bytes = stackalloc byte[32];
        SHA256.HashData([.. "runwick.mgmt.dispatch.v1"u8, 0, .. commandId.ToByteArray(bigEndian: true)], bytes);
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes[..16], bigEndian: true);
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
