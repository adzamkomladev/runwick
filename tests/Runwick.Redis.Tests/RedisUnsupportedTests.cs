using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>
/// Declared-unsupported Redis topologies and capabilities fail with an explicit code (22 §8, §13; driver-capabilities §7).
/// Every case carries the <c>Unsupported</c> trait so the conformance report can tell them from ordinary passes.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
[Trait("Unsupported", "redis")]
[Collection(nameof(RedisServerState))]
public sealed class RedisUnsupportedTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [Trait("Case", "UNS-RD-01")]
    [InlineData("10.0.0.1:6379,10.0.0.2:6379")]
    [InlineData("10.0.0.1:26379,serviceName=mymaster")]
    public async Task Several_endpoints_and_a_Sentinel_service_name_are_refused_with_topology_unsupported(string configuration)
    {
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.Options(configuration, TimeSpan.FromSeconds(1))).Code);

        // The same refusal through the public builder: startup reports it before any command is sent.
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("uns");
            TestJobs.Register(r);
            r.UseRedis(configuration);
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains("topology_unsupported", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "UNS-RD-02")]
    public async Task A_server_running_appendfsync_everysec_is_refused_with_store_profile_mismatch_and_nothing_is_written()
    {
        var f = await DriverFixture.StartAsync("uns_rd2_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = c.Services.GetRequiredService<RedisStore>();
        var mux = await RedisServer.AdminAsync();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        try
        {
            await server.ConfigSetAsync("appendfsync", "everysec");
            await store.CheckProfileAsync(Ct);
            var refused = await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.ReserveAsync());
            Assert.Equal("store_profile_mismatch", refused.Code);
        }
        finally
        {
            await server.ConfigSetAsync("appendfsync", "always");
        }

        await store.CheckProfileAsync(Ct);
        Assert.Empty((await f.ReadCommittedSnapshot()).Model.Jobs);
    }

    [Fact]
    [Trait("Case", "UNS-RD-03")]
    [Trait("Case", "TX-01")]
    [Trait("Case", "TX-02")]
    [Trait("Case", "TX-03")]
    [Trait("Case", "TX-04")]
    public async Task Caller_enlistment_is_not_offered_UseRedis_registers_no_staging_dispatcher_or_completion_service()
    {
        var f = await DriverFixture.StartAsync("uns_rd_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        // Nothing to stage into: a caller asking the container for either driver's staging API gets a hard failure, not a fallback.
        Assert.Null(c.Services.GetService<Runwick.Postgres.IPostgresJobDispatcher>());
        Assert.Null(c.Services.GetService<Runwick.Sqlite.ISqliteJobDispatcher>());
        Assert.Throws<InvalidOperationException>(() => c.Services.GetRequiredService<Runwick.Postgres.IPostgresJobDispatcher>());
        Assert.Throws<InvalidOperationException>(() => c.Services.GetRequiredService<Runwick.Sqlite.ISqliteJobDispatcher>());

        // The ordinary dispatch path still works and leaves one committed job: nothing was written by the failed lookups.
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        Assert.Single((await f.ReadCommittedSnapshot()).Model.Jobs);
    }
}
