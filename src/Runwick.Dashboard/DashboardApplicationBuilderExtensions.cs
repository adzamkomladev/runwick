using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Dashboard;

namespace Microsoft.AspNetCore.Builder;

/// <summary>Maps dashboard mounts (21 §2).</summary>
public static partial class DashboardApplicationBuilderExtensions
{
    /// <summary>
    /// Mounts dashboard <paramref name="mountId"/> at <paramref name="path"/> as a terminal branch. Call it <b>before</b> the
    /// host's own <c>app.UseInertia()</c>: that middleware version-checks every Inertia request it sees, including this
    /// mount's, and would send dashboard pages into a reload loop. Runwick cannot detect the wrong order.
    /// A host endpoint whose route lies under <paramref name="path"/> stops the application once it has started.
    /// </summary>
    public static IApplicationBuilder MapRunwickDashboard(this IApplicationBuilder app, PathString path, string mountId = "default")
    {
        ArgumentNullException.ThrowIfNull(app);
        if (!path.HasValue || !MountPath().IsMatch(path.Value!))
        {
            throw new ArgumentException($"Dashboard path '{path}' must look like '/runwick': lower-case segments, no trailing slash.", nameof(path));
        }

        var sp = app.ApplicationServices;
        var mount = sp.GetKeyedService<DashboardMount>(mountId)
            ?? throw new InvalidOperationException($"Dashboard mount '{mountId}' is not registered. Call services.AddRunwickDashboard(\"{mountId}\", ...).");
        mount.Validate();

        var branch = new DashboardBranch(mount);
        sp.GetRequiredService<IHostApplicationLifetime>().ApplicationStarted.Register(() => branch.CheckRoutes(sp, path));
        return app.Map(path, branch.Configure);
    }

    [GeneratedRegex("^(/[a-z0-9][a-z0-9-]*)+$")]
    private static partial Regex MountPath();
}

/// <summary>One mapping of a mount: answers 503 <c>starting</c> until the route check has run (21 §2.2).</summary>
internal sealed class DashboardBranch(DashboardMount mount)
{
    volatile bool _ready;

    public void Configure(IApplicationBuilder branch) => mount.Configure(branch, () => _ready);

    public void CheckRoutes(IServiceProvider sp, PathString path)
    {
        var prefix = path.Value!;
        foreach (var source in sp.GetServices<EndpointDataSource>())
        {
            foreach (var endpoint in source.Endpoints.OfType<RouteEndpoint>())
            {
                var raw = "/" + (endpoint.RoutePattern.RawText ?? "").TrimStart('/');
                if (raw.Equals(prefix, StringComparison.OrdinalIgnoreCase) || raw.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase))
                {
                    sp.GetRequiredService<ILoggerFactory>().CreateLogger("Runwick.Dashboard").LogCritical(
                        "The host route {Route} lies under the dashboard mount {Mount} and would be unreachable; stopping the application.",
                        endpoint.RoutePattern.RawText, prefix);
                    sp.GetRequiredService<IHostApplicationLifetime>().StopApplication();
                    return;
                }
            }
        }

        _ready = true;
    }
}
