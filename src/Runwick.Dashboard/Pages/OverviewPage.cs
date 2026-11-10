using System.Text.Json;
using Inertia.Net;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Overview (21 §4): <c>/overview?range|from&amp;to</c>. Exact figures over the range (default the last 24 hours; 16 GetJobStats by
/// queue and job name), the six newest failures, queues, every live worker and the scaling events. A prop the caller may not read is null.
/// </summary>
internal sealed class OverviewPage : DashboardPage
{
    public static readonly OverviewPage Instance = new();

    public override string Component => "Overview";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("stats", P.JobsRead), ("failed", P.JobsRead),
        ("queues", P.QueuesRead), ("workers", P.CapacityRead), ("events", P.CapacityRead));

    /// <summary>Every prop is deferred, in two groups that load together: the figures and charts, and the side panels.</summary>
    protected override InertiaProp Wrap(string prop, Func<CancellationToken, Task<JsonElement>> load) =>
        Inertia.Net.Inertia.Defer(load, prop switch { "stats" or "failed" => "charts", _ => "side" });

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
            "stats" => await JobTypesPage.Stats(r, null, byJobName: true, byQueue: true),
            "failed" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "failed", From: from, To: to, Limit: 6), r.Ct),
            "queues" => await r.Session.ListQueuesAsync(r.Caller, Max, null, r.Ct),
            "workers" => await QueuePage.Live(r),
            _ => await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(From: from, To: to, Limit: Max), r.Ct),
        };
    }
}
