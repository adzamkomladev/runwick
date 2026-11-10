using System.Text.Json;
using System.Text.Json.Nodes;
using Inertia.Net;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// Queue (21 §4.9): <c>/queues/{name}?range|from&amp;to</c>. Its state and revision, its exact figures over the range (16 GetJobStats:
/// counts per state, throughput buckets, pending now and the oldest pending), its newest jobs, and the live workers whose pools serve it. Pause and resume
/// post to the Queues controls with <c>view: "detail"</c>, so the answer is this page.
/// </summary>
internal sealed class QueuePage(string name) : DashboardPage
{
    const int RecentJobs = 10;

    public override string Component => "Queue";

    public override string Permission => P.QueuesRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("queue", P.QueuesRead), ("stats", P.JobsRead), ("jobs", P.JobsRead), ("workers", P.CapacityRead), ("query", P.QueuesRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop)
    {
        if (!r.Has(Props[prop]))
        {
            return null;
        }

        var (from, to) = r.Range(TimeSpan.FromDays(1));
        return prop switch
        {
            "queue" => (await All(cursor => r.Session.ListQueuesAsync(r.Caller, ManagementSession.MaxPageSize, cursor, r.Ct))).FirstOrDefault(q => q.Name == name)
                ?? throw new ManagementNotFoundException(),
            "stats" => await JobTypesPage.Stats(r, null, queue: name),
            "jobs" => await r.Session.ListJobsAsync(r.Caller, new JobListQuery(Queue: name, From: from, To: to, Limit: RecentJobs), r.Ct),
            "workers" => (await Live(r)).Where(w => Serves(w, name)).ToList(),
            _ => new { name, range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
        };
    }

    /// <summary>Every live worker (paged through; 21 §4.9).</summary>
    internal static Task<IReadOnlyList<WorkerSummary>> Live(DashboardRequest r) =>
        All(cursor => r.Session.ListWorkersAsync(r.Caller, false, ManagementSession.MaxPageSize, cursor, r.Ct));

    /// <summary>
    /// Every item of a paged read, for figures that must be exact. shortcut: at most 25 pages of 200 (5,000 items), then
    /// <c>too_many</c>; a fleet or schedule set that large needs a count read in 16.
    /// </summary>
    internal static async Task<IReadOnlyList<T>> All<T>(Func<string?, Task<ManagementPage<T>>> read)
    {
        var items = new List<T>();
        string? cursor = null;
        for (var i = 0; i < 25; i++)
        {
            var page = await read(cursor);
            items.AddRange(page.Items);
            if ((cursor = page.Next) is null)
            {
                return items;
            }
        }

        throw new ManagementUnavailableException("too_many", "More than 5,000 items; the dashboard does not total this many.");
    }

    static bool Serves(WorkerSummary w, string queue) =>
        w.Subscriptions is JsonArray subscribed && subscribed.Any(q => q?.GetValueKind() == JsonValueKind.String && q.GetValue<string>() == queue);
}

/// <summary>
/// Worker (21 §4.9): <c>/workers/{instance}</c>. Its presence row with the detail it publishes (host, pools, settings, running and
/// recent attempts; 16 WorkerSummary.Detail) and its scaling changes of the last 24 hours. Expired workers are found too.
/// </summary>
internal sealed class WorkerPage(Guid instanceId) : DashboardPage
{
    const int MaxPages = 10;

    public override string Component => "Worker";

    public override string Permission => P.CapacityRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("worker", P.CapacityRead), ("events", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "worker" => await FindAsync(r) ?? throw new ManagementNotFoundException(),
        _ => await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(InstanceId: instanceId, From: DateTimeOffset.UtcNow.AddDays(-1), Limit: 50), r.Ct),
    };

    // 16 has no single-worker read: the presence list is paged (newest heartbeat first) until the instance turns up.
    // shortcut: at most 10 pages of 200 (2,000 presence rows, live and expired); add GetWorker to 16 if that ever falls short.
    async Task<WorkerSummary?> FindAsync(DashboardRequest r)
    {
        string? cursor = null;
        for (var i = 0; i < MaxPages; i++)
        {
            var page = await r.Session.ListWorkersAsync(r.Caller, true, ManagementSession.MaxPageSize, cursor, r.Ct);
            if (page.Items.FirstOrDefault(w => w.InstanceId == instanceId) is { } found)
            {
                return found;
            }

            if ((cursor = page.Next) is null)
            {
                break;
            }
        }

        return null;
    }
}

/// <summary>
/// Schedule (21 §4.9): <c>/schedules/{key}</c>. Its definition, state and newest occurrences with each job's state (16 GetSchedule).
/// Pause, resume, trigger and revalidate post to the Schedules controls with <c>view: "detail"</c>. There is no edit read or
/// control in 16 beyond these.
/// </summary>
internal sealed class SchedulePage(string key) : DashboardPage
{
    public override string Component => "Schedule";

    public override string Permission => P.SchedulesRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("schedule", P.SchedulesRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) =>
        await r.Session.GetScheduleAsync(r.Caller, key, 100, r.Ct);
}

/// <summary>
/// Pools (21 §4.9): <c>/pools</c>. Pools are worker configuration, not stored rows: the page groups the live workers' published pools
/// (presence detail, or only the pool targets for a worker that publishes none) by name.
/// </summary>
internal sealed class PoolsPage : DashboardPage
{
    public static readonly PoolsPage Instance = new();

    public override string Component => "Pools";

    public override string Permission => P.CapacityRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("workers", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => await QueuePage.Live(r);
}

/// <summary>Pool (21 §4.9): <c>/pools/{name}</c>. The live workers running it, and its scaling changes of the last 24 hours. Read only.</summary>
internal sealed class PoolPage(string name) : DashboardPage
{
    public override string Component => "Pool";

    public override string Permission => P.CapacityRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("workers", P.CapacityRead), ("events", P.CapacityRead), ("query", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "workers" => (await QueuePage.Live(r)).Where(w => w.PoolTargets is JsonObject pools && pools.ContainsKey(name)).ToList(),
        "events" => await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(Pool: name, From: DateTimeOffset.UtcNow.AddDays(-1), Limit: 100), r.Ct),
        _ => new { name },
    };
}

/// <summary>
/// The Workers page header (21 §4.12): exact totals over every live worker. Concurrency is the summed pool targets (capacity) and
/// in-flight attempts; machine memory counts each host once; CPU is the mean of the processes' shares of their machines.
/// </summary>
internal static class Fleet
{
    public static object Of(IReadOnlyList<WorkerSummary> workers)
    {
        long target = 0, inFlight = 0, workingSet = 0;
        double cpu = 0;
        var cpuCount = 0;
        var hosts = new Dictionary<string, (long Total, long Available)>(StringComparer.Ordinal);
        (string Host, Guid Id, long InFlight)? busiest = null;
        foreach (var w in workers)
        {
            long mine = 0;
            if (w.PoolTargets is JsonObject targets)
            {
                target += targets.Sum(t => t.Value?.GetValueKind() == JsonValueKind.Number ? t.Value.GetValue<long>() : 0);
            }

            if (w.Detail?["pools"] is JsonArray pools)
            {
                mine = pools.Sum(p => p?["inFlight"]?.GetValueKind() == JsonValueKind.Number ? p["inFlight"]!.GetValue<long>() : 0);
            }

            inFlight += mine;
            if (busiest is null || mine > busiest.Value.InFlight)
            {
                busiest = (w.HostName, w.InstanceId, mine);
            }

            if (w.Detail?["host"] is JsonObject host)
            {
                if (host["processCpuPercent"]?.GetValueKind() == JsonValueKind.Number)
                {
                    cpu += host["processCpuPercent"]!.GetValue<double>();
                    cpuCount++;
                }

                workingSet += host["workingSetBytes"]?.GetValueKind() == JsonValueKind.Number ? host["workingSetBytes"]!.GetValue<long>() : 0;
                if (host["memoryTotalBytes"]?.GetValueKind() == JsonValueKind.Number)
                {
                    hosts[w.HostName] = (host["memoryTotalBytes"]!.GetValue<long>(),
                        host["memoryAvailableBytes"]?.GetValueKind() == JsonValueKind.Number ? host["memoryAvailableBytes"]!.GetValue<long>() : 0);
                }
            }
        }

        return new
        {
            live = workers.Count,
            hosts = workers.Select(w => w.HostName).Distinct(StringComparer.Ordinal).Count(),
            target, inFlight, workingSetBytes = workingSet,
            cpuPercent = cpuCount == 0 ? (double?)null : Math.Round(cpu / cpuCount, 1),
            memoryTotalBytes = hosts.Values.Sum(h => h.Total), memoryAvailableBytes = hosts.Values.Sum(h => h.Available),
            busiest = busiest is { InFlight: > 0 } b ? new { hostName = b.Host, instanceId = b.Id, inFlight = b.InFlight } : null,
        };
    }
}
