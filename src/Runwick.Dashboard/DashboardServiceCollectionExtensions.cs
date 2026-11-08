using System.Text.RegularExpressions;
using Inertia.Net;
using Microsoft.AspNetCore.DataProtection;
using Runwick.Dashboard;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers dashboard mounts (21 §2.1).</summary>
public static partial class DashboardServiceCollectionExtensions
{
    /// <summary>
    /// Registers dashboard mount <paramref name="mountId"/> as the Inertia.Net named mount <c>runwick-{id}</c> with its own asset
    /// version, root view and state cookies. Requires <c>AddRunwickManagement</c>. Map it with <c>MapRunwickDashboard</c>.
    /// </summary>
    public static IServiceCollection AddRunwickDashboard(this IServiceCollection services, string mountId, Action<DashboardOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (mountId is null || !MountId().IsMatch(mountId))
        {
            throw new ArgumentException($"Mount id '{mountId}' must be 1-32 of a-z, 0-9 and '-'.", nameof(mountId));
        }

        if (services.Any(d => d.IsKeyedService && d.ServiceType == typeof(DashboardMount) && Equals(d.ServiceKey, mountId)))
        {
            throw new InvalidOperationException($"Dashboard mount '{mountId}' is already registered.");
        }

        var options = new DashboardOptions();
        configure(options);
        var name = DashboardMount.InertiaName(mountId);
        services.AddDataProtection();
        // Not AddAuthorizationCore: WebApplication then auto-adds UseAuthorization, which throws at startup without AddAuthorization.
        services.AddAuthorization();
        services.AddKeyedSingleton(mountId, (sp, _) => new DashboardMount(mountId, options, sp));
        // Before AddInertia: it adds its template root view only when the mount has none.
        services.AddKeyedSingleton<IInertiaRootView>(name, (sp, _) => sp.GetRequiredKeyedService<DashboardMount>(mountId));
        services.AddInertia(name, o =>
        {
            o.Version = options.Assets.Version;
            o.EncryptHistory = true; // an extra layer: props are already safe DTOs
        });
        return services;
    }

    [GeneratedRegex("^[a-z0-9-]{1,32}$")]
    private static partial Regex MountId();
}
