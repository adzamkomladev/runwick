using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Http;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Jobs (21 §4): <c>/</c>, <c>/jobs?queue&amp;state&amp;jobName&amp;range|from&amp;to&amp;cursor</c>; bulk cancel of up to 100 rows.
/// <c>jobs</c> is a merge prop: "Load more" asks for the next page and the client appends its rows (matched on the job ID).
/// The queue names for the filter are a once prop, kept by the client for five minutes.
/// </summary>
internal sealed class JobsPage : DashboardPage
{
    public static readonly JobsPage Instance = new();

    public override string Component => "Jobs";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("jobs", P.JobsRead), ("query", P.JobsRead), ("queueNames", P.QueuesRead));

    protected override InertiaProp Wrap(string prop, Func<CancellationToken, Task<JsonElement>> load) => prop switch
    {
        "jobs" => Inertia.Net.Inertia.Merge(load).Append("items", "jobId"),
        "queueNames" => Inertia.Net.Inertia.Once(load).Until(TimeSpan.FromMinutes(5)),
        _ => base.Wrap(prop, load),
    };

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "jobs" => await r.ListAsync(cursor =>
        {
            var (from, to) = r.Range();
            return r.Session.ListJobsAsync(r.Caller, new JobListQuery(r.Q("queue"), r.Q("state"), r.Q("jobName"), from, to, r.QInt("limit"), cursor), r.Ct);
        }),
        "queueNames" => r.Has(P.QueuesRead)
            ? (await r.Session.ListQueuesAsync(r.Caller, ManagementSession.MaxPageSize, null, r.Ct)).Items.Select(q => q.Name).ToList()
            : null,
        _ => new { queue = r.Q("queue"), state = r.Q("state"), jobName = r.Q("jobName"), range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
    };

    /// <summary><c>POST /jobs:cancel</c>: one command ID per item, in the body; results in request order (16 §4).</summary>
    public static async Task BulkCancelAsync(DashboardRequest r)
    {
        if (await DashboardControls.ReadAsync<BulkBody>(r, commandId: false) is not { } read)
        {
            return;
        }

        var items = read.Body.Items ?? [];
        if (items.Count is 0 or > ManagementSession.MaxBulkItems || items.Any(i => i.CommandId == Guid.Empty))
        {
            await DashboardMount.WriteErrorAsync(r.Http, 400, "invalid_request", "A bulk cancel carries 1-100 items, each with a command ID.");
            return;
        }

        var results = await r.Session.CancelJobsAsync(r.Caller, items, read.Body.Reason, r.Ct);
        r.Outcome = new { items = results.Select(x => new { jobId = x.JobId, status = x.Status, code = x.Code }) };
        await DashboardControls.RenderAtAsync(r, Instance, "/jobs");
    }

    internal sealed class BulkBody
    {
        public List<BulkCancelItem>? Items { get; set; }

        public string? Reason { get; set; }
    }
}

/// <summary>Job (21 §4): <c>/jobs/{id}</c>; cancel, retry, rerun; payload and error reveals. The job's audit is an optional prop.</summary>
internal sealed class JobPage(Guid jobId) : DashboardPage
{
    public override string Component => "Job";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("job", P.JobsRead), ("audit", P.AuditRead));

    public override IReadOnlySet<string> Optional { get; } = new HashSet<string>(StringComparer.Ordinal) { "audit" };

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "job" => await r.Session.GetJobAsync(r.Caller, jobId, r.Ct),
        _ => await r.Session.ListAuditAsync(r.Caller, new AuditQuery("job", jobId.ToString("D")), r.Ct),
    };

    /// <summary><c>POST /jobs/{id}/cancel|retry|rerun</c>.</summary>
    public static async Task ControlAsync(DashboardRequest r, Guid jobId, string operation)
    {
        if (await DashboardControls.ReadAsync<JobBody>(r) is not { } read)
        {
            return;
        }

        var request = new JobControlRequest(read.CommandId, read.Body.ExpectedGeneration, read.Body.ExpectedRevision, read.Body.Reason);
        var session = r.Session;
        var path = "/jobs/" + jobId.ToString("D");
        await DashboardControls.RunAsync(r, read.CommandId, async () =>
        {
            var result = operation switch
            {
                "cancel" => await session.CancelJobAsync(r.Caller, jobId, request, r.Ct),
                "retry" => await session.RetryJobAsync(r.Caller, jobId, request, r.Ct),
                _ => await session.RerunJobAsync(r.Caller, jobId, request, r.Ct),
            };
            return (result.Succeeded, result.Code, result.Revision);
        }, path, new JobPage(jobId), path);
    }

    /// <summary><c>POST /jobs/{id}/payload:reveal</c> and <c>POST /attempts/{id}/error:reveal</c>: plain JSON, never a prop (21 §5.1).</summary>
    public static async Task RevealAsync(DashboardRequest r, Guid id, bool payload)
    {
        if (await DashboardControls.ReadAsync<ReasonBody>(r, commandId: false) is not { } read)
        {
            return;
        }

        object value = payload
            ? await r.Session.RevealPayloadAsync(r.Caller, id, read.Body.Reason, r.Ct)
            : await r.Session.RevealErrorAsync(r.Caller, id, read.Body.Reason, r.Ct);
        r.Http.Response.StatusCode = StatusCodes.Status200OK;
        await r.Http.Response.WriteAsJsonAsync(value, DashboardMount.Json, r.Ct);
    }

    internal sealed class JobBody
    {
        public int ExpectedGeneration { get; set; }

        public long ExpectedRevision { get; set; }

        public string? Reason { get; set; }
    }
}
