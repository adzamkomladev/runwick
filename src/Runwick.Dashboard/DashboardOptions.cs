using System.Security.Claims;
using Runwick.Management;

namespace Runwick.Dashboard;

/// <summary>
/// One dashboard mount (21 §2). Schemes, grant map and actor map default to those of <c>AddRunwickManagement</c>; there is
/// no anonymous mode.
/// </summary>
public sealed class DashboardOptions
{
    /// <summary>The page title. Default "Runwick".</summary>
    public string Title { get; set; } = "Runwick";

    internal List<string> Schemes { get; } = [];

    internal Func<ClaimsPrincipal, IReadOnlyList<ManagementGrant>>? Grants { get; private set; }

    internal Func<ClaimsPrincipal, ManagementActor>? ActorMap { get; private set; }

    /// <summary>The client build; tests and the sample host swap in a rebuilt one to stand in for a deploy.</summary>
    internal DashboardAssets Assets { get; set; } = DashboardAssets.Embedded;

    /// <summary>The host authentication schemes this mount accepts. Default: the management schemes.</summary>
    public DashboardOptions AuthenticationSchemes(params string[] schemes)
    {
        ArgumentNullException.ThrowIfNull(schemes);
        Schemes.AddRange(schemes);
        return this;
    }

    /// <summary>An authenticated principal's grants on this mount. Default: the management grant map.</summary>
    public DashboardOptions MapGrants(Func<ClaimsPrincipal, IReadOnlyList<ManagementGrant>> map)
    {
        Grants = map ?? throw new ArgumentNullException(nameof(map));
        return this;
    }

    /// <summary>The actor recorded in audit. Default: the management actor map.</summary>
    public DashboardOptions Actor(Func<ClaimsPrincipal, ManagementActor> map)
    {
        ActorMap = map ?? throw new ArgumentNullException(nameof(map));
        return this;
    }
}
