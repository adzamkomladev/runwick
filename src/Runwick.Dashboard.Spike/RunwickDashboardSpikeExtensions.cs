using System.Text.RegularExpressions;
using Inertia.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Dashboard.Spike;

/// <summary>Registers and mounts the dashboard spike.</summary>
public static partial class RunwickDashboardSpikeExtensions
{
    /// <summary>
    /// Registers dashboard mount <paramref name="mountId"/> as an Inertia.Net named mount (<c>runwick-{id}</c>) with its own
    /// options, asset version and root view. The host's own Inertia setup (the default mount) is not changed.
    /// </summary>
    public static IServiceCollection AddRunwickDashboardSpike(this IServiceCollection services, string mountId, Action<DashboardMountOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (!MountId().IsMatch(mountId))
        {
            throw new ArgumentException($"Mount id '{mountId}' must be 1–32 of a–z, 0–9 and '-'.", nameof(mountId));
        }

        if (services.Any(d => d.IsKeyedService && d.ServiceType == typeof(DashboardMount) && Equals(d.ServiceKey, mountId)))
        {
            throw new InvalidOperationException($"Dashboard mount '{mountId}' is already registered.");
        }

        var options = new DashboardMountOptions();
        configure(options);

        // No implicit anonymous mode: a mount without both policies and a data source is not registered.
        if (options.ViewPolicy is null || options.OperatePolicy is null || options.Data is null)
        {
            throw new InvalidOperationException($"Dashboard mount '{mountId}' needs ViewPolicy, OperatePolicy and Data.");
        }

        var name = DashboardMount.InertiaName(mountId);
        services.AddDataProtection();
        // Not AddAuthorizationCore: WebApplication then auto-adds UseAuthorization, which throws at startup without AddAuthorization.
        services.AddAuthorization();
        services.AddKeyedSingleton(mountId, (sp, _) => new DashboardMount(mountId, options, sp.GetRequiredService<IDataProtectionProvider>()));
        // Before AddInertia: it only adds its template root view when the mount has none.
        services.AddKeyedSingleton<IInertiaRootView>(name, (sp, _) => sp.GetRequiredKeyedService<DashboardMount>(mountId));
        services.AddInertia(name, o =>
        {
            o.Version = options.Assets.Version;
            o.EncryptHistory = true; // supplementary: props are already filtered and carry no payload
        });
        return services;
    }

    /// <summary>
    /// Mounts dashboard <paramref name="mountId"/> at <paramref name="path"/> as a terminal branch. Call it before the host's own
    /// <c>UseInertia()</c>: that middleware version-checks every Inertia request it sees, including this mount's.
    /// </summary>
    public static IApplicationBuilder UseRunwickDashboardSpike(this IApplicationBuilder app, PathString path, string mountId)
    {
        ArgumentNullException.ThrowIfNull(app);
        if (!path.HasValue || path.Value!.EndsWith('/') || !MountPath().IsMatch(path.Value))
        {
            throw new ArgumentException($"Dashboard path '{path}' must look like '/runwick': lower-case segments, no trailing slash.", nameof(path));
        }

        var mount = app.ApplicationServices.GetKeyedService<DashboardMount>(mountId)
            ?? throw new InvalidOperationException($"Dashboard mount '{mountId}' is not registered. Call services.AddRunwickDashboardSpike(\"{mountId}\", ...).");
        return app.Map(path, mount.Configure);
    }

    [GeneratedRegex("^(/[a-z0-9][a-z0-9-]*)+$")]
    private static partial Regex MountPath();

    [GeneratedRegex("^[a-z0-9-]{1,32}$")]
    private static partial Regex MountId();
}
