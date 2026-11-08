using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runwick.Worker;

namespace Runwick.AspNetCore.Tests;

/// <summary>OBS-07: the ASP.NET Core checks map per-role Runwick health to tagged checks and never print the reason by default (18 §7).</summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-022")]
[Trait("Case", "OBS-07")]
public sealed class HealthCheckTests
{
    sealed class FakeHealth : IRunwickHealth
    {
        public RunwickRoleHealth Live = new("live", RunwickHealthStatus.Healthy, "ok", DateTimeOffset.UnixEpoch);
        public List<RunwickRoleHealth> Ready = [];

        public RunwickRoleHealth Liveness() => Live;

        public IReadOnlyList<RunwickRoleHealth> Readiness() => Ready;
    }

    [Fact]
    public async Task Checks_follow_roles_and_status()
    {
        var fake = new FakeHealth
        {
            Ready =
            [
                new("producer", RunwickHealthStatus.Degraded, "store_unavailable", DateTimeOffset.UnixEpoch),
                new("worker", RunwickHealthStatus.Unhealthy, "draining", DateTimeOffset.UnixEpoch),
            ],
        };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IRunwickHealth>(fake);
        services.AddSingleton<WorkerEngine>(_ => throw new InvalidOperationException("only the descriptor is read"));
        services.AddHealthChecks().AddRunwick();
        await using var sp = services.BuildServiceProvider();
        var checks = sp.GetRequiredService<HealthCheckService>();

        var report = await checks.CheckHealthAsync(TestContext.Current.CancellationToken);
        Assert.Equal(["runwick-live", "runwick-producer", "runwick-worker"], report.Entries.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(HealthStatus.Healthy, report.Entries["runwick-live"].Status);
        Assert.Equal((HealthStatus.Degraded, "store_unavailable"), (report.Entries["runwick-producer"].Status, report.Entries["runwick-producer"].Description));
        Assert.Equal((HealthStatus.Unhealthy, "draining"), (report.Entries["runwick-worker"].Status, report.Entries["runwick-worker"].Description));

        var live = await checks.CheckHealthAsync(r => r.Tags.Contains("live"), TestContext.Current.CancellationToken);
        Assert.Equal(HealthStatus.Healthy, live.Status);
        Assert.Equal(["runwick-live"], live.Entries.Keys);

        // A role the host enables but the health service does not report is Unhealthy, never silently Healthy.
        fake.Ready.RemoveAt(1);
        var ready = await checks.CheckHealthAsync(r => r.Tags.Contains("ready"), TestContext.Current.CancellationToken);
        Assert.Equal((HealthStatus.Unhealthy, "not_enabled"), (ready.Entries["runwick-worker"].Status, ready.Entries["runwick-worker"].Description));
    }
}
