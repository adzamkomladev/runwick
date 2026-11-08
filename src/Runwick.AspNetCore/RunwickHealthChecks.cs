using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runwick;
using Runwick.Scheduling;
using Runwick.Worker;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>ASP.NET Core health checks over <see cref="IRunwickHealth"/> (18 §7).</summary>
public static class RunwickHealthCheckExtensions
{
    /// <summary>
    /// Adds <c>runwick-live</c> (tag <c>live</c>) and <c>runwick-{role}</c> (tag <c>ready</c>) for every role this host
    /// enables. Call after <c>AddRunwick</c> (and <c>AddRunwickManagement</c>). Checks return cached state only.
    /// </summary>
    public static IHealthChecksBuilder AddRunwick(this IHealthChecksBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        var roles = new List<string> { "producer" };
        if (services.Any(d => d.ServiceType == typeof(WorkerEngine)))
        {
            roles.Add("worker");
        }

        if (services.Any(d => d.ServiceType == typeof(SchedulerService)))
        {
            roles.Add("scheduler");
        }

        if (services.Any(d => d.ServiceType == typeof(ManagementRoleMarker)))
        {
            roles.Add("management");
        }

        builder.Add(new HealthCheckRegistration("runwick-live", sp => new RoleCheck(sp.GetRequiredService<IRunwickHealth>(), null), null, ["live"]));
        foreach (var role in roles)
        {
            builder.Add(new HealthCheckRegistration("runwick-" + role, sp => new RoleCheck(sp.GetRequiredService<IRunwickHealth>(), role), null, ["ready"]));
        }

        return builder;
    }

    sealed class RoleCheck(IRunwickHealth health, string? role) : IHealthCheck
    {
        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var h = role is null ? health.Liveness() : health.Readiness().FirstOrDefault(r => r.Role == role)
                ?? new RunwickRoleHealth(role, RunwickHealthStatus.Unhealthy, "not_enabled", DateTimeOffset.UtcNow);

            // The bounded reason goes only into the description, which the default response writer never prints (18 §7).
            var result = h.Status switch
            {
                RunwickHealthStatus.Healthy => HealthCheckResult.Healthy(h.Reason),
                RunwickHealthStatus.Degraded => HealthCheckResult.Degraded(h.Reason),
                _ => new HealthCheckResult(context.Registration.FailureStatus, h.Reason),
            };
            return Task.FromResult(result);
        }
    }
}
