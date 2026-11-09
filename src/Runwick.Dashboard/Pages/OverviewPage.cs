using System.Text.Json;
using Inertia.Net;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Overview (21 §4): <c>/overview?range|from&amp;to</c>. First pages, at the largest page size (200), of reads that already exist (no
/// count read, 16): jobs created in the range (default the last 24 hours), failed jobs created in the range, pending and running jobs, queues, live workers and
/// scaling events. The client charts these samples and says what span each covers; it never shows a total. A prop the caller may
/// not read is null.
/// </summary>
internal sealed class OverviewPage : DashboardPage
{
    public static readonly OverviewPage Instance = new();

    public override string Component => "Overview";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("recent", P.JobsRead), ("failed", P.JobsRead),
        ("pending", P.JobsRead), ("running", P.JobsRead), ("queues", P.QueuesRead), ("workers", P.CapacityRead), ("events", P.CapacityRead));

    /// <summary>Every prop is deferred, in three groups that load together: the charts, the in-flight counts, and the side panels.</summary>
    protected override InertiaProp Wrap(string prop, Func<CancellationToken, Task<JsonElement>> load) =>
        Inertia.Net.Inertia.Defer(load, prop switch { "recent" or "failed" => "charts", "pending" or "running" => "flight", _ => "side" });

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop)
    {
        if (!r.Has(Props[prop]))
        {
            return null;
        }

        var (from, to) = r.Range(TimeSpan.FromDays(1));
        const int Max = ManagementSession.MaxPageSize;
        return prop switch
        {
            "recent" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(From: from, To: to, Limit: Max), r.Ct),
            "failed" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "failed", From: from, To: to, Limit: Max), r.Ct),
            "pending" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "pending", Limit: Max), r.Ct),
            "running" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "running", Limit: Max), r.Ct),
            "queues" => await r.Session.ListQueuesAsync(r.Caller, Max, null, r.Ct),
            "workers" => await r.Session.ListWorkersAsync(r.Caller, false, Max, null, r.Ct),
            _ => await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(From: from, To: to, Limit: Max), r.Ct),
        };
    }
}
