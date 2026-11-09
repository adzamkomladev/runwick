using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Overview (21 §4): <c>/overview</c>. First pages of reads that already exist (no count read, 16): jobs created in the last hour,
/// failed jobs created in the last 24 hours, pending jobs, queues, live workers and scaling events. The client shows each as a
/// tally of at most one page ("50+" when there is a next page), never a total. A prop the caller may not read is null.
/// </summary>
internal sealed class OverviewPage : DashboardPage
{
    public static readonly OverviewPage Instance = new();

    public override string Component => "Overview";

    public override string Permission => P.JobsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("recent", P.JobsRead), ("failed", P.JobsRead),
        ("pending", P.JobsRead), ("queues", P.QueuesRead), ("workers", P.CapacityRead), ("events", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop)
    {
        if (!r.Has(Props[prop]))
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow;
        return prop switch
        {
            "recent" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(From: now.AddHours(-1)), r.Ct),
            "failed" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "failed", From: now.AddHours(-24)), r.Ct),
            "pending" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "pending"), r.Ct),
            "queues" => await r.Session.ListQueuesAsync(r.Caller, null, null, r.Ct),
            "workers" => await r.Session.ListWorkersAsync(r.Caller, false, null, null, r.Ct),
            _ => await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(), r.Ct),
        };
    }
}
