using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Management;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Configures the management API's authentication schemes, grant mapping and limits (16 §5).</summary>
public sealed class RunwickManagementBuilder
{
    internal List<string> Schemes { get; } = [];

    internal Func<ClaimsPrincipal, IReadOnlyList<ManagementGrant>>? Grants { get; private set; }

    internal Func<ClaimsPrincipal, ManagementActor>? ActorMap { get; private set; }

    internal ManagementOptions Options { get; } = new();

    /// <summary>Required: the host authentication schemes this API accepts. The host's default scheme is not changed.</summary>
    public RunwickManagementBuilder AuthenticationSchemes(params string[] schemes)
    {
        ArgumentNullException.ThrowIfNull(schemes);
        Schemes.AddRange(schemes);
        return this;
    }

    /// <summary>Required: an authenticated principal's grants. Request bodies and headers never supply grants.</summary>
    public RunwickManagementBuilder MapGrants(Func<ClaimsPrincipal, IReadOnlyList<ManagementGrant>> map)
    {
        Grants = map ?? throw new ArgumentNullException(nameof(map));
        return this;
    }

    /// <summary>Optional: the actor recorded in audit. Default: NameIdentifier (else Name), Name, and the authenticating scheme.</summary>
    public RunwickManagementBuilder Actor(Func<ClaimsPrincipal, ManagementActor> map)
    {
        ActorMap = map ?? throw new ArgumentNullException(nameof(map));
        return this;
    }

    /// <summary>Optional: cursor key and process-local limits.</summary>
    public RunwickManagementBuilder Configure(Action<ManagementOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(Options);
        return this;
    }
}

public static class RunwickManagementServiceCollectionExtensions
{
    internal const string CursorPurpose = "Runwick.Management.Cursor.v1";

    /// <summary>Registers the management API services. Map it with <c>MapRunwickManagement</c>.</summary>
    public static IServiceCollection AddRunwickManagement(this IServiceCollection services, Action<RunwickManagementBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new RunwickManagementBuilder();
        configure(builder);
        services.AddSingleton(builder);
        services.AddSingleton(builder.Options);
        services.AddSingleton<Runwick.ManagementRoleMarker>();
        services.AddSingleton(sp => new ManagementLimiter(builder.Options, sp.GetService<TimeProvider>() ?? TimeProvider.System));
        services.AddAntiforgery();
        services.AddDataProtection();
        if (builder.Options.CursorKey is null)
        {
            // Replicas sharing a Data Protection key ring accept each other's cursors.
            services.AddSingleton<IManagementCursorProtector>(sp =>
                new DataProtectionCursorProtector(sp.GetRequiredService<IDataProtectionProvider>().CreateProtector(CursorPurpose)));
        }

        return services;
    }

    /// <summary>
    /// Adds the Runwick Basic scheme: an explicit verifier is required, there are no default credentials, non-HTTPS
    /// requests are refused, and failures are limited to 10 per minute per source address and user name.
    /// </summary>
    public static AuthenticationBuilder AddRunwickBasic(this AuthenticationBuilder builder, Action<RunwickBasicOptions> configure,
        string scheme = RunwickBasicDefaults.Scheme)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<BasicFailureLimiter>();
        return builder.AddScheme<RunwickBasicOptions, RunwickBasicHandler>(scheme, configure);
    }
}

internal sealed class DataProtectionCursorProtector(IDataProtector protector) : IManagementCursorProtector
{
    public byte[] Protect(byte[] payload) => protector.Protect(payload);

    public byte[]? Unprotect(byte[] sealedCursor) => protector.Unprotect(sealedCursor);
}
