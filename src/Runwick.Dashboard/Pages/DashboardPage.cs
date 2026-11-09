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
    static readonly HashSet<string> Shared = new(StringComparer.Ordinal) { "base", "title", "user", "can", "notice", "outcome", "errors" };

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
            ["can"] = DashboardRequest.Element(Abilities.ToDictionary(a => a.Key, a => r.Has(a.Value))),
        };
        foreach (var prop in Props.Keys)
        {
            if (partial?.Contains(prop) ?? !Optional.Contains(prop))
            {
                props[prop] = DashboardRequest.Element(await LoadAsync(r, prop));
            }
        }

        props["notice"] = r.Notice;
        props["outcome"] = r.Outcome is null ? null : DashboardRequest.Element(r.Outcome);
        await Inertia.Net.Inertia.Render(Component, props).ExecuteAsync(r.Http);
    }
}

internal static class Props
{
    public static Dictionary<string, string> Of(params (string Name, string Permission)[] props) =>
        props.ToDictionary(p => p.Name, p => p.Permission, StringComparer.Ordinal);
}
