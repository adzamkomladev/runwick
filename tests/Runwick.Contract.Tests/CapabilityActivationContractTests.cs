using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// The operator capability call (10 §6.1, 23 §3.6) on every driver: it raises the capability in the store's own format, a repeat
/// changes nothing, and non-activatable or unknown names and unknown applications are refused.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class CapabilityActivationContractTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static Task<IReadOnlyList<CapabilityChange>> Activate(DriverFixture f, string? app, string name)
    {
        var cs = f.Kind == StoreKind.Redis ? RedisServer.ConfigurationOf(f.AdminConnectionString) : f.AdminConnectionString;
        return f.Kind switch
        {
            StoreKind.Postgres => RunwickPostgresMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
            StoreKind.Sqlite => RunwickSqliteMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
            _ => RunwickRedisMigrations.ActivateCapabilityAsync(cs, app, name, "ops", Ct),
        };
    }

    static string CodeOf(Exception e) => e switch
    {
        PostgresMigrationException p => p.Code,
        SqliteMigrationException s => s.Code,
        RedisMigrationException r => r.Code,
        _ => throw new InvalidOperationException("not a migration exception: " + e),
    };

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-ACT")]
    public async Task Operator_activation_raises_the_capability_once_and_is_seen_by_the_store(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("capact_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        var first = (await Activate(f, f.Application, "scheduler")).Single();
        Assert.Equal((f.Application, "scheduler", 1, true), (first.Application, first.Capability, first.Version, first.Changed));
        var again = (await Activate(f, f.Application, "scheduler")).Single();
        Assert.False(again.Changed);
        Assert.Equal(first.Revision, again.Revision);

        // The store's own operation sees the capability already there, so it is stored in the driver's format.
        var rates = (IRateControlStore)c.Driver;
        var repeat = await rates.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, new ControlActor("user:ops", null, "host", "t"))), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (repeat.Status, repeat.Error!.Code));

        // A second capability merges with the first; every application when none is named.
        var rate = (await Activate(f, null, "rate_admission")).Single();
        Assert.True(rate.Changed);
        Assert.Equal(first.Revision + 1, rate.Revision);

        Assert.Equal("invalid_capability", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, f.Application, "execution"))));
        Assert.Equal("invalid_capability", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, f.Application, "teleport"))));
        Assert.Equal("application_not_registered", CodeOf(await Assert.ThrowsAnyAsync<Exception>(() => Activate(f, "nobody", "scheduler"))));
    }
}
