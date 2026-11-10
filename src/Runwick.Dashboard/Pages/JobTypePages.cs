using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Jobs (21 §4.11): <c>/jobs?range|from&amp;to</c>. One row per job type: registered here, handled by a live worker, seen in stored
/// runs or used by a schedule (16 ListJobTypes), with exact figures per type over the range (16 GetJobStats by job name).
/// </summary>
internal sealed class JobTypesPage : DashboardPage
{
    public static readonly JobTypesPage Instance = new();

    public override string Component => "Jobs";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("types", P.JobsRead), ("stats", P.JobsRead), ("workers", P.CapacityRead), ("query", P.JobsRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "types" => await r.Session.ListJobTypesAsync(r.Caller, r.Ct),
        "stats" => await Stats(r, jobName: null, byJobName: true),
        "workers" => await QueuePage.Live(r),
        _ => new { range = r.Q("range") ?? (r.Q("from") is null ? "1d" : null), from = r.Q("from"), to = r.Q("to") },
    };

    /// <summary>Exact figures over the page's range (default the last 24 hours, as the picker shows).</summary>
    internal static Task<JobStats> Stats(DashboardRequest r, string? jobName, bool byJobName = false, string? queue = null, bool byQueue = false,
        int fallbackDays = 1)
    {
        var (from, to) = r.Range(TimeSpan.FromDays(fallbackDays));
        return r.StatsAsync(() => r.Session.GetJobStatsAsync(r.Caller,
            new JobStatsQuery(from, to, Buckets: 24, Queue: queue, JobName: jobName, ByQueue: byQueue, ByJobName: byJobName), r.Ct), () =>
        {
            var now = DateTimeOffset.UtcNow;
            return new JobStats(now, now, now, new Dictionary<string, long>(), 0, new JobDurationStats(0, null, null, null), [],
                new Dictionary<string, long>(), null, byQueue ? [] : null, byJobName ? [] : null, []);
        });
    }
}

/// <summary>
/// Job (21 §4.11): <c>/jobs/{name}</c>. The type's definition, exact figures and charts over the range, its top error codes, the
/// workers and pools that handle it, its schedules and its runs. Trigger a run, retry the failed runs in the range and cancel the
/// pending runs: each item checked and audited by the session as a single control would be.
/// </summary>
internal sealed class JobTypePage(string name) : DashboardPage
{
    /// <summary>At most this many runs are retried or cancelled by one bulk action (ten session bulk calls).</summary>
    internal const int MaxBulkRuns = 10 * ManagementSession.MaxBulkItems;

    public override string Component => "Job";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(
        ("type", P.JobsRead), ("stats", P.JobsRead), ("runs", P.JobsRead), ("workers", P.CapacityRead), ("query", P.JobsRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "type" => (await r.Session.ListJobTypesAsync(r.Caller, r.Ct)).FirstOrDefault(t => t.Name == name)
            ?? throw new ManagementNotFoundException("not_found", "No job type has this name."),
        "stats" => await JobTypesPage.Stats(r, name),
        "runs" => await r.ListAsync(cursor =>
        {
            var (from, to) = r.Range();
            return r.Session.ListJobsAsync(r.Caller, new JobListQuery(r.Q("queue"), r.Q("state"), name, from, to, r.QInt("limit"), cursor), r.Ct);
        }),
        "workers" => (await QueuePage.Live(r)).Where(w => w.Detail?["handlers"] is JsonArray h && h.Any(x => x?.GetValue<string>() == name)).ToList(),
        _ => new { name, queue = r.Q("queue"), state = r.Q("state"), range = r.Q("range") ?? (r.Q("from") is null ? "1d" : null), from = r.Q("from"), to = r.Q("to") },
    };

    /// <summary><c>POST /jobs/{name}/dispatch|retry-failed|cancel-pending</c>.</summary>
    public static async Task ControlAsync(DashboardRequest r, string name, string operation)
    {
        if (await DashboardControls.ReadAsync<Body>(r) is not { } read)
        {
            return;
        }

        var body = read.Body;
        var path = "/jobs/" + DashboardControls.Escape(name);
        var page = new JobTypePage(name);
        if (operation == "dispatch")
        {
            ManagementDispatchResult? created = null;
            await DashboardControls.RunAsync(r, read.CommandId, async () =>
            {
                created = await r.Session.DispatchAsync(r.Caller,
                    new ManagementDispatchRequest(read.CommandId, name, body.Payload, null, body.Queue, body.Priority, body.NotBefore, body.Reason), r.Ct);
                return (true, null, null);
            }, path, page, path);
            if (created is not null)
            {
                r.Http.Response.Headers.Location = r.Base + "/runs/" + created.JobId.ToString("D");
            }

            return;
        }

        // The runs to act on, listed exactly as the page shows them: this type, the state, the range the reader confirmed.
        var state = operation == "retry-failed" ? "failed" : "pending";
        var runs = new List<JobSummary>();
        string? cursor = null;
        do
        {
            var batch = await r.Session.ListJobsAsync(r.Caller,
                new JobListQuery(null, state, name, operation == "retry-failed" ? body.From : null, operation == "retry-failed" ? body.To : null,
                    ManagementSession.MaxPageSize, cursor), r.Ct);
            runs.AddRange(batch.Items);
            cursor = batch.Next;
        }
        while (cursor is not null && runs.Count <= MaxBulkRuns);

        if (runs.Count > MaxBulkRuns)
        {
            r.Outcome = new { code = "too_many_runs", commandId = read.CommandId, limit = MaxBulkRuns };
            await DashboardControls.RenderAtAsync(r, page, path);
            return;
        }

        // One command ID per run, derived from the request's, so sending the same request again replays rather than repeats.
        var results = new List<object>();
        foreach (var chunk in runs.Chunk(ManagementSession.MaxBulkItems))
        {
            if (operation == "retry-failed")
            {
                var items = chunk.Select(j => new BulkRetryItem(j.JobId, ItemCommand(read.CommandId, j.JobId), j.Generation, j.Revision)).ToList();
                results.AddRange((await r.Session.RetryJobsAsync(r.Caller, items, body.Reason, r.Ct)).Select(x => (object)new { jobId = x.JobId, status = x.Status, code = x.Code }));
            }
            else
            {
                var items = chunk.Select(j => new BulkCancelItem(j.JobId, ItemCommand(read.CommandId, j.JobId), j.Generation, j.Revision)).ToList();
                results.AddRange((await r.Session.CancelJobsAsync(r.Caller, items, body.Reason, r.Ct)).Select(x => (object)new { jobId = x.JobId, status = x.Status, code = x.Code }));
            }
        }

        r.Outcome = new { commandId = read.CommandId, operation, expected = body.ExpectedCount, items = results };
        await DashboardControls.RenderAtAsync(r, page, path);
    }

    static Guid ItemCommand(Guid command, Guid job)
    {
        Span<byte> bytes = stackalloc byte[32];
        command.TryWriteBytes(bytes);
        job.TryWriteBytes(bytes[16..]);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(bytes, hash);
        hash[7] = (byte)((hash[7] & 0x0F) | 0x50); // version 5 style (name-based), variant RFC 4122
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);
        return new Guid(hash[..16]);
    }

    internal sealed class Body
    {
        public JsonNode? Payload { get; set; }

        public string? Queue { get; set; }

        public string? Priority { get; set; }

        public DateTimeOffset? NotBefore { get; set; }

        public DateTimeOffset? From { get; set; }

        public DateTimeOffset? To { get; set; }

        public long? ExpectedCount { get; set; }

        public string? Reason { get; set; }
    }
}
