using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Dashboard.Spike;

/// <summary>Mounts the dashboard spike.</summary>
public static partial class RunwickDashboardSpikeExtensions
{
    /// <summary>
    /// Adds what the mounts need and nothing global: Data Protection and the authorization services (both TryAdd, no options
    /// changed). No hosted service, startup filter, MVC filter, shared props or default authentication scheme.
    /// </summary>
    public static IServiceCollection AddRunwickDashboardSpike(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddDataProtection();
        // Not AddAuthorizationCore: WebApplication then auto-adds UseAuthorization, which throws at startup without AddAuthorization.
        services.AddAuthorization();
        return services;
    }

    /// <summary>
    /// Mounts a dashboard at <paramref name="path"/> as a terminal branch. Call it before the host's own Inertia middleware
    /// (<c>UseInertia()</c>) and fallback routes, so nothing the host configures for its pages runs for the mount.
    /// </summary>
    public static IApplicationBuilder UseRunwickDashboardSpike(this IApplicationBuilder app, PathString path, Action<DashboardMountOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new DashboardMountOptions();
        configure(options);
        if (!path.HasValue || path.Value == "/" || path.Value!.EndsWith('/') || !MountPath().IsMatch(path.Value))
        {
            throw new ArgumentException($"Dashboard path '{path}' must look like '/runwick': lower-case segments, no trailing slash.", nameof(path));
        }

        if (!MountId().IsMatch(options.MountId))
        {
            throw new ArgumentException($"Mount id '{options.MountId}' must be 1–32 of a–z, 0–9 and '-'.", nameof(configure));
        }

        // No implicit anonymous mode: a mount without both policies and a data source is not mapped.
        if (options.ViewPolicy is null || options.OperatePolicy is null || options.Data is null)
        {
            throw new InvalidOperationException($"Dashboard mount '{options.MountId}' needs ViewPolicy, OperatePolicy and Data.");
        }

        var mount = new DashboardMount(options, app.ApplicationServices.GetRequiredService<IDataProtectionProvider>());
        return app.Map(path, branch => branch.Run(mount.InvokeAsync));
    }

    [GeneratedRegex("^(/[a-z0-9][a-z0-9-]*)+$")]
    private static partial Regex MountPath();

    [GeneratedRegex("^[a-z0-9-]{1,32}$")]
    private static partial Regex MountId();
}
