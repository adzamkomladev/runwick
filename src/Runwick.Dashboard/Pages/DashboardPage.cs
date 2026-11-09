using System.Text.Json;
using Inertia.Net;
using Runwick.Management;
using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

/// <summary>
/// One page (21 §4): its component, the permission to read it, and its props with the permission each needs. A partial reload
/// naming a prop the page does not have, or one the caller may not read, is 403 before anything is read (sp-023 gap 5).
/// </summary>
internal abstract class DashboardPage
{
    static readonly HashSet<string> Shared = new(StringComparer.Ordinal) { "base", "title", "user", "userDetail", "can", "signOut", "notifications", "notice", "outcome", "errors" };

    /// <summary>Page and action names to the permission each needs: the <c>can</c> map and the navigation (a display hint only).</summary>
    public static readonly IReadOnlyDictionary<string, string> Abilities = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["overview"] = P.JobsRead, ["jobs"] = P.JobsRead, ["queues"] = P.QueuesRead, ["workers"] = P.CapacityRead, ["schedules"] = P.SchedulesRead,
        ["scaling"] = P.CapacityRead, ["settings"] = P.SettingsRead, ["retention"] = P.SettingsRead, ["audit"] = P.AuditRead,
        ["cancel"] = P.JobsCancel, ["retry"] = P.JobsRetry, ["rerun"] = P.JobsRerun, ["revealPayload"] = P.JobsReadPayload,
        ["revealError"] = P.JobsReadError, ["queueControl"] = P.QueuesControl, ["capacityEdit"] = P.CapacityEdit,
        ["scheduleControl"] = P.SchedulesControl, ["scheduleTrigger"] = P.SchedulesTrigger, ["scheduleEdit"] = P.SchedulesEdit,
        ["retentionManage"] = P.RetentionManage,
    };

    public abstract string Component { get; }

    /// <summary>The permission to read the page at all.</summary>
    public abstract string Permission { get; }

    /// <summary>Data props and the permission each needs.</summary>
    public abstract IReadOnlyDictionary<string, string> Props { get; }

    /// <summary>Props sent only when a partial reload asks for them.</summary>
    public virtual IReadOnlySet<string> Optional { get; } = new HashSet<string>();

    protected abstract Task<object?> LoadAsync(DashboardRequest r, string prop);

    /// <summary>
    /// How a prop travels (Inertia.Net factories; 21 §4.3): by default with the page, or only on a partial reload when it is in
    /// <see cref="Optional"/>. Pages override this for deferred, once and merge props.
    /// </summary>
    protected virtual InertiaProp Wrap(string prop, Func<CancellationToken, Task<JsonElement>> load) =>
        Optional.Contains(prop) ? Inertia.Net.Inertia.Optional(load) : Inertia.Net.Inertia.Prop(load);

    public async Task RenderAsync(DashboardRequest r)
    {
        var partial = r.Partial(Component);
        if (partial is not null && partial.Any(name => !Shared.Contains(name) && (!Props.TryGetValue(name, out var permission) || !r.Has(permission))))
        {
            await DashboardMount.WriteErrorAsync(r.Http, 403, "forbidden", "The caller may not read this prop.");
            return;
        }

        if (!r.Has(Permission))
        {
            throw new ManagementDeniedException(Permission);
        }

        var props = new InertiaProps
        {
            ["base"] = r.Base,
            ["title"] = r.Mount.Title,
            ["user"] = r.Caller.Actor.Display ?? r.Caller.Actor.Subject,
            // The account menu's second line: the subject under a display name, else how the caller signed in.
            ["userDetail"] = r.Caller.Actor.Display is { } shown && shown != r.Caller.Actor.Subject ? r.Caller.Actor.Subject : r.Caller.Actor.Scheme,
            ["can"] = DashboardRequest.Element(Abilities.ToDictionary(a => a.Key, a => r.Has(a.Value))),
            ["signOut"] = r.Mount.SignOutPath,
            // The sidebar's notifications: loaded after the page, then kept by the client for a minute (21 §4.4).
            ["notifications"] = Inertia.Net.Inertia.Defer(async _ => DashboardRequest.Element(await Notifications.LoadAsync(r)), "notifications")
                .Once("runwick-notifications", TimeSpan.FromMinutes(1)),
        };
        foreach (var prop in Props.Keys)
        {
            props[prop] = Wrap(prop, async _ => DashboardRequest.Element(await LoadAsync(r, prop)));
        }

        props["notice"] = r.Notice;
        props["outcome"] = r.Outcome is null ? null : DashboardRequest.Element(r.Outcome);
        await Inertia.Net.Inertia.Render(Component, props).ExecuteAsync(r.Http);
    }
}

/// <summary>
/// What the sidebar bell shows (21 §4.4): failed jobs and scaling changes of the last 24 hours (the newest 20 of each) and the
/// queues paused now. A part the caller may not read is null. The unread count is the client's (localStorage).
/// </summary>
internal static class Notifications
{
    const int Newest = 20;

    public static async Task<object> LoadAsync(DashboardRequest r)
    {
        var since = DateTimeOffset.UtcNow.AddHours(-24);
        object? failed = null, scaling = null, paused = null;
        if (r.Has(P.JobsRead))
        {
            var page = await r.Session.ListJobsAsync(r.Caller, new JobListQuery(State: "failed", From: since, Limit: Newest), r.Ct);
            failed = page.Items.Select(j => new { j.JobId, j.JobName, j.Queue, At = j.TerminalAt ?? j.UpdatedAt });
        }

        if (r.Has(P.CapacityRead))
        {
            var page = await r.Session.ListScalingEventsAsync(r.Caller, new ScalingEventQuery(From: since, Limit: Newest), r.Ct);
            scaling = page.Items.Select(e => new { e.EventId, e.Pool, e.OldTarget, e.NewTarget, e.Reason, At = e.CreatedAt });
        }

        if (r.Has(P.QueuesRead))
        {
            var page = await r.Session.ListQueuesAsync(r.Caller, ManagementSession.MaxPageSize, null, r.Ct);
            paused = page.Items.Where(q => q.Paused).Select(q => new { q.Name, q.Revision });
        }

        return new { failed, scaling, paused };
    }
}

internal static class Props
{
    public static Dictionary<string, string> Of(params (string Name, string Permission)[] props) =>
        props.ToDictionary(p => p.Name, p => p.Permission, StringComparer.Ordinal);
}
