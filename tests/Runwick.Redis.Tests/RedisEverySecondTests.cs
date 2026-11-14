using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>G4: the opt-in <c>redis-aof-everysec-single-primary-v1</c> persistence profile. The default profile still refuses everysec.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-G4")]
public sealed class RedisEverySecondTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly string[] EverySecondProfile = [.. RedisNode.Profile, "--appendfsync", "everysec"];

    static async Task<RxProfileReport> CheckAsync(RedisNode node, RedisPersistence persistence)
    {
        await using var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(node.Configuration + ",allowAdmin=true");
        return await RxProfile.CheckAsync(mux.GetServer(mux.GetEndPoints()[0]), mux.GetDatabase(), persistence);
    }

    static async Task<ServiceProvider> HostAsync(RedisNode node, string application)
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(node.Configuration, o =>
            {
                o.Persistence = RedisPersistence.AofEverySecond;
                o.ProfileRecheckInterval = TimeSpan.FromSeconds(5);
            });
        });
        var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct);
        return provider;
    }

    [Fact]
    public async Task Each_persistence_profile_accepts_only_its_own_appendfsync_and_the_migrator_agrees()
    {
        await using var always = await RedisNode.StartAsync();
        await using var everysec = await RedisNode.StartAsync(EverySecondProfile);

        Assert.True((await CheckAsync(everysec, RedisPersistence.AofEverySecond)).Verified);
        var facts = (await CheckAsync(everysec, RedisPersistence.AofEverySecond)).Facts;
        Assert.Equal(("redis-aof-everysec-single-primary-v1", "everysec"), (facts["profile"], facts["config.appendfsync"]));
        Assert.Equal(("store_profile_mismatch", "appendfsync=everysec"), Describe(await CheckAsync(everysec, RedisPersistence.AofAlways)));
        Assert.Equal(("store_profile_mismatch", "appendfsync=always"), Describe(await CheckAsync(always, RedisPersistence.AofEverySecond)));

        var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(everysec.Configuration, Ct));
        Assert.Equal("store_profile_mismatch", refused.Code);
        await RunwickRedisMigrations.MigrateAsync(everysec.Configuration, RedisPersistence.AofEverySecond, Ct);
    }

    static (string? Code, string? Failure) Describe(RxProfileReport r) => (r.FailureCode, r.Failure);

    [Fact]
    public async Task A_host_on_the_everysec_profile_reports_it_and_is_refused_when_the_server_changes_back_and_forth()
    {
        await using var node = await RedisNode.StartAsync(EverySecondProfile);
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, RedisPersistence.AofEverySecond, Ct);
        await using var provider = await HostAsync(node, "g4_" + Guid.NewGuid().ToString("N")[..8]);
        var store = provider.GetRequiredService<RedisStore>();
        Assert.Equal(Runwick.AcknowledgementProfiles.RedisAofEverySecond, store.AcknowledgementProfile);

        var receipt = await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("one"), cancellationToken: Ct);
        Assert.Equal(Runwick.AcknowledgementProfiles.RedisAofEverySecond, receipt.ConfirmedAcknowledgementProfile);

        // Someone switches the server to appendfsync always: the host's own check now refuses further writes.
        await node.CliAsync("config", "set", "appendfsync", "always");
        await store.CheckProfileAsync(Ct);
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("two"), cancellationToken: Ct));
        Assert.Contains("store_profile_mismatch", ex.ToString(), StringComparison.Ordinal);

        await node.CliAsync("config", "set", "appendfsync", "everysec");
        await store.CheckProfileAsync(Ct);
        await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("three"), cancellationToken: Ct);
    }

    [Fact]
    public async Task Acknowledged_dispatches_survive_a_kill_of_the_Redis_process_but_the_profile_makes_no_machine_crash_claim()
    {
        await using var node = await RedisNode.StartAsync(EverySecondProfile);
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, RedisPersistence.AofEverySecond, Ct);
        await using var provider = await HostAsync(node, "g4k_" + Guid.NewGuid().ToString("N")[..8]);
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var operations = new List<DispatchOperationId>();
        for (var i = 0; i < 200; i++)
        {
            var p = prepared.Prepare(new PingJob("k" + i));
            await prepared.DispatchAsync(p, Ct);
            operations.Add(p.OperationId);
        }

        // SIGKILL leaves the appended bytes with the kernel, so the AOF reload sees every acknowledged write. A power loss or kernel
        // crash would not be covered: that exposure (about one second) is the documented cost of the profile and is not simulated here.
        await node.CrashAndRestartAsync();
        var confirmed = 0;
        foreach (var id in operations)
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    confirmed += (await prepared.ResolveAsync(id, Ct)).Status == DispatchResolutionStatus.Confirmed ? 1 : 0;
                    break;
                }
                catch (Exception) when (attempt < 40)
                {
                    await Task.Delay(250, Ct);
                }
            }
        }

        Assert.Equal(200, confirmed);
    }
}
