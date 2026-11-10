using System.Text.Json;
using Inertia.Net;
using Runwick.Management;
using Runwick.Scheduling;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>Queues (21 §4): <c>/queues?cursor</c>; pause and resume. Paused state and revision, no depth (16 has no count read).</summary>
internal sealed class QueuesPage : DashboardPage
{
    public static readonly QueuesPage Instance = new();

    public override string Component => "Queues";

    public override string Permission => P.QueuesRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("queues", P.QueuesRead), ("counts", P.QueuesRead), ("stats", P.JobsRead), ("query", P.QueuesRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "queues" => await r.ListAsync(cursor => r.Session.ListQueuesAsync(r.Caller, r.QInt("limit"), cursor, r.Ct)),
        "counts" => await Counts(r),
        "stats" => r.Has(P.JobsRead) ? await JobTypesPage.Stats(r, null, queue: null, byQueue: true) : null,
        _ => new { range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
    };

    static async Task<object> Counts(DashboardRequest r)
    {
        var all = await QueuePage.All(cursor => r.Session.ListQueuesAsync(r.Caller, ManagementSession.MaxPageSize, cursor, r.Ct));
        return new { total = all.Count, paused = all.Count(q => q.Paused) };
    }

    /// <summary><c>POST /queues/{name}/pause|resume</c>.</summary>
    public static async Task ControlAsync(DashboardRequest r, string queue, bool pause)
    {
        if (await DashboardControls.ReadAsync<RevisionBody>(r) is not { } read)
        {
            return;
        }

        var detail = read.Body.View == "detail" ? "/queues/" + DashboardControls.Escape(queue) : null;
        await DashboardControls.RunAsync(r, read.CommandId, async () =>
        {
            var result = pause
                ? await r.Session.PauseQueueAsync(r.Caller, queue, read.CommandId, read.Body.ExpectedRevision, read.Body.Reason, r.Ct)
                : await r.Session.ResumeQueueAsync(r.Caller, queue, read.CommandId, read.Body.ExpectedRevision, read.Body.Reason, r.Ct);
            return (result.Succeeded, result.Code, result.Revision);
        }, detail ?? "/queues", detail is null ? Instance : new QueuePage(queue), detail ?? "/queues");
    }
}

/// <summary>Workers and capacity (21 §4): <c>/workers?expired&amp;cursor&amp;scopeCursor</c>; set a scope's capacity.</summary>
internal sealed class WorkersPage : DashboardPage
{
    public static readonly WorkersPage Instance = new();

    public override string Component => "Workers";

    public override string Permission => P.CapacityRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("workers", P.CapacityRead), ("fleet", P.CapacityRead), ("scopes", P.CapacityRead), ("query", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "workers" => await r.ListAsync(cursor => r.Session.ListWorkersAsync(r.Caller, r.Q("expired") is "true" or "1", r.QInt("limit"), cursor, r.Ct)),
        "scopes" => await r.ListAsync(cursor => r.Session.ListScopesAsync(r.Caller, null, r.QInt("limit"), cursor, r.Ct), "scopeCursor"),
        "fleet" => Fleet.Of(await QueuePage.Live(r)),
        _ => new { expired = r.Q("expired") is "true" or "1" },
    };

    /// <summary><c>POST /scopes/{kind}/{key}/capacity</c>.</summary>
    public static async Task CapacityAsync(DashboardRequest r, string kind, string key)
    {
        if (await DashboardControls.ReadAsync<CapacityBody>(r) is not { } read)
        {
            return;
        }

        await DashboardControls.RunAsync(r, read.CommandId, async () =>
        {
            var result = await r.Session.SetScopeCapacityAsync(r.Caller, kind, key, read.CommandId, read.Body.Capacity, read.Body.ExpectedPolicyRevision,
                read.Body.Reason, r.Ct);
            return (result.Succeeded, result.Code, result.PolicyRevision);
        }, "/workers", Instance, "/workers");
    }

    internal sealed class CapacityBody
    {
        public int Capacity { get; set; }

        public long ExpectedPolicyRevision { get; set; }

        public string? Reason { get; set; }
    }
}

/// <summary>Schedules (21 §4): <c>/schedules?cursor</c>; pause, resume with policy, trigger, revalidate.</summary>
internal sealed class SchedulesPage : DashboardPage
{
    public static readonly SchedulesPage Instance = new();

    public override string Component => "Schedules";

    public override string Permission => P.SchedulesRead;

    public override IReadOnlyDictionary<string, string> Props { get; } =
        Pages.Props.Of(("schedules", P.SchedulesRead), ("stats", P.SchedulesRead), ("query", P.SchedulesRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop)
    {
        var (from, to) = r.Range(TimeSpan.FromDays(1));
        return prop switch
        {
            "schedules" => await r.ListAsync(cursor => r.Session.ListSchedulesAsync(r.Caller, r.QInt("limit"), cursor, r.Ct)),
            "stats" => await r.StatsAsync(() => r.Session.GetScheduleStatsAsync(r.Caller, from, to, r.Ct),
                () => new ScheduleStats(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, 0, 0, 0, null, new Dictionary<string, long>(), 0)),
            _ => new { range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
        };
    }

    /// <summary><c>POST /schedules/{key}/pause|resume|trigger|revalidate</c>.</summary>
    public static async Task ControlAsync(DashboardRequest r, string key, string operation)
    {
        if (await DashboardControls.ReadAsync<RevisionBody>(r) is not { } read)
        {
            return;
        }

        var (s, c, id, b) = (r.Session, r.Caller, read.CommandId, read.Body);
        var detail = b.View == "detail" ? "/schedules/" + DashboardControls.Escape(key) : null;
        await DashboardControls.RunAsync(r, id, async () =>
        {
            var result = operation switch
            {
                "pause" => await s.PauseScheduleAsync(c, key, id, b.ExpectedRevision, b.Reason, r.Ct),
                "resume" => await s.ResumeScheduleAsync(c, key, id, b.ExpectedRevision, Policy(b.Policy), b.Reason, r.Ct),
                "trigger" => await s.TriggerScheduleAsync(c, key, id, b.ExpectedRevision, b.Reason, r.Ct),
                _ => await s.RevalidateScheduleAsync(c, key, id, b.ExpectedRevision, b.Reason, r.Ct),
            };
            return (result.Succeeded, result.Code, operation is "pause" or "resume" ? result.ControlRevision : result.DefinitionRevision);
        }, detail ?? "/schedules", detail is null ? Instance : new SchedulePage(key), detail ?? "/schedules");
    }

    static ResumePolicy Policy(string? policy) => policy switch
    {
        null or "skipPaused" => ResumePolicy.SkipPaused,
        "runOnce" => ResumePolicy.RunOnce,
        "catchUp" => ResumePolicy.CatchUp,
        _ => throw new ManagementValidationException("invalid_request", "policy must be skipPaused, runOnce or catchUp."),
    };
}

/// <summary>Scaling (21 §4, §6): <c>/scaling?instance&amp;pool&amp;range|from&amp;to&amp;cursor</c>. Read only.</summary>
internal sealed class ScalingPage : DashboardPage
{
    public static readonly ScalingPage Instance = new();

    public override string Component => "Scaling";

    public override string Permission => P.CapacityRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("events", P.CapacityRead), ("query", P.CapacityRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "events" => await r.ListAsync(cursor =>
        {
            var (from, to) = r.Range();
            return r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(r.QGuid("instance"), r.Q("pool"), from, to, r.QInt("limit"), cursor), r.Ct);
        }),
        _ => new { instance = r.Q("instance"), pool = r.Q("pool"), range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
    };
}

/// <summary>Audit (21 §4): <c>/audit?targetKind&amp;targetId&amp;range|from&amp;to&amp;cursor</c>. Read only.</summary>
internal sealed class AuditPage : DashboardPage
{
    public static readonly AuditPage Instance = new();

    public override string Component => "Audit";

    public override string Permission => P.AuditRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("audit", P.AuditRead), ("stats", P.AuditRead), ("query", P.AuditRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => prop switch
    {
        "stats" => await r.StatsAsync(() => r.Session.GetAuditStatsAsync(r.Caller, r.Range(TimeSpan.FromDays(1)).From, r.Range(TimeSpan.FromDays(1)).To, r.Ct),
            () => new AuditStats(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, 0, [], [], null)),
        "audit" => await r.ListAsync(cursor =>
        {
            var (from, to) = r.Range();
            // The page keeps the 31-day range cap of Jobs and Scaling (21 §4); the audit read itself takes any range.
            if (from is { } f && ((to ?? DateTimeOffset.UtcNow) - f > TimeSpan.FromDays(31) || f > (to ?? DateTimeOffset.MaxValue)))
            {
                throw new ManagementValidationException("invalid_request", "The range must be ordered and at most 31 days.");
            }

            return r.Session.ListAuditAsync(r.Caller, new AuditQuery(r.Q("targetKind"), r.Q("targetId"), from, to, r.QInt("limit"), cursor), r.Ct);
        }),
        _ => new { targetKind = r.Q("targetKind"), targetId = r.Q("targetId"), range = r.Q("range"), from = r.Q("from"), to = r.Q("to") },
    };
}

/// <summary>Settings (21 §4): <c>/settings</c>. Read only, not polled; a once prop the client keeps for five minutes.</summary>
internal sealed class SettingsPage : DashboardPage
{
    public static readonly SettingsPage Instance = new();

    public override string Component => "Settings";

    public override string Permission => P.SettingsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("settings", P.SettingsRead));

    protected override InertiaProp Wrap(string prop, Func<CancellationToken, Task<JsonElement>> load) =>
        Inertia.Net.Inertia.Once(load).Until(TimeSpan.FromMinutes(5));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) => await r.Session.GetSettingsAsync(r.Caller, r.Ct);
}

/// <summary>Retention (21 §4): <c>/retention</c>; policy (whole hours, null = forever) and progress; edit the policy.</summary>
internal sealed class RetentionPage : DashboardPage
{
    public static readonly RetentionPage Instance = new();

    public override string Component => "Retention";

    public override string Permission => P.SettingsRead;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("retention", P.SettingsRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop)
    {
        var v = await r.Session.GetRetentionAsync(r.Caller, r.Ct);
        return new { policy = Hours(v.Policy), revision = v.Revision, classes = v.Classes, observedAt = v.ObservedAt };
    }

    static Dictionary<string, int?> Hours(RetentionPolicy p) => Names.Zip(p.Hours()).ToDictionary(x => x.First, x => x.Second, StringComparer.Ordinal);

    static readonly string[] Names =
        ["completedDetailHours", "failedDetailHours", "jobHistoryHours", "auditHours", "scalingEventsHours", "scheduleOccurrencesHours", "workerPresenceHours"];

    /// <summary><c>POST /retention/policy</c>: the seven values in hours (null keeps forever).</summary>
    public static async Task EditAsync(DashboardRequest r)
    {
        if (await DashboardControls.ReadAsync<PolicyBody>(r) is not { } read)
        {
            return;
        }

        var b = read.Body;
        await DashboardControls.RunAsync(r, read.CommandId, async () =>
        {
            var policy = b.WorkerPresenceHours is null
                ? throw new ManagementValidationException("invalid_retention_policy", "workerPresenceHours is required.")
                : RetentionPolicy.FromHours([b.CompletedDetailHours, b.FailedDetailHours, b.JobHistoryHours, b.AuditHours, b.ScalingEventsHours,
                    b.ScheduleOccurrencesHours, b.WorkerPresenceHours]);
            var result = await r.Session.EditRetentionPolicyAsync(r.Caller, read.CommandId, b.ExpectedRevision, policy, b.Reason, r.Ct);
            return (result.Succeeded, result.Code, result.Revision);
        }, "/retention", Instance, "/retention");
    }

    internal sealed class PolicyBody
    {
        public long ExpectedRevision { get; set; }

        public int? CompletedDetailHours { get; set; }

        public int? FailedDetailHours { get; set; }

        public int? JobHistoryHours { get; set; }

        public int? AuditHours { get; set; }

        public int? ScalingEventsHours { get; set; }

        public int? ScheduleOccurrencesHours { get; set; }

        public int? WorkerPresenceHours { get; set; }

        public string? Reason { get; set; }
    }
}

/// <summary>Body of queue and schedule controls; only these fields are read (an actor field is ignored).</summary>
internal sealed class RevisionBody
{
    public long ExpectedRevision { get; set; }

    public string? Policy { get; set; }

    public string? Reason { get; set; }

    /// <summary>"detail" when the control was sent from the resource's own page, so the answer is that page.</summary>
    public string? View { get; set; }
}
