using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>
/// Perf round two, work area R (22 §2.5): strict write authority on a Sentinel-managed primary. Real containers: one primary, two
/// replicas and three Sentinels.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-R")]
public sealed class RedisSentinelTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<ServiceProvider> HostAsync(SentinelSet set, string application, string? configuration = null)
    {
        await RunwickRedisMigrations.MigrateAsync(set.Primary, Ct);
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(configuration ?? set.Configuration, o =>
            {
                o.Sentinel = new RedisSentinelOptions { AuthorityRecheckInterval = TimeSpan.FromMilliseconds(500), AuthorityTtl = TimeSpan.FromSeconds(2) };
            });
        });
        var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct);
        return provider;
    }

    static async Task<RxAuthoritySnapshot> WaitForAsync(RedisStore store, Func<RxAuthoritySnapshot, bool> condition, string what, int seconds = 45)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (true)
        {
            var s = store.AuthoritySnapshot!;
            if (condition(s))
            {
                return s;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out waiting for {what}; authority is {s.State} ({s.Reason}), primary {s.Primary}.");
            }

            await Task.Delay(200, Ct);
        }
    }

    static async Task<string?> TryDispatchAsync(ServiceProvider provider, string name)
    {
        try
        {
            await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob(name), cancellationToken: Ct);
            return null;
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
    }

    [Fact]
    public async Task A_host_verifies_the_Sentinel_primary_before_it_writes_and_reports_the_Sentinel_profile()
    {
        await using var set = await SentinelSet.StartAsync();
        await using var provider = await HostAsync(set, "r1_" + Guid.NewGuid().ToString("N")[..8]);
        var store = provider.GetRequiredService<RedisStore>();

        var snapshot = store.AuthoritySnapshot!;
        Assert.Equal(RxAuthorityState.Verified, snapshot.State);
        Assert.Equal(set.Primary, snapshot.Primary);
        Assert.Equal(3, snapshot.SentinelsConfigured);
        Assert.True(snapshot.SentinelsAgreeing >= 2);
        Assert.Equal(AcknowledgementProfiles.RedisAofAlwaysSentinel, store.AcknowledgementProfile);
        Assert.Equal(("redis-aof-always-sentinel-v1", "sentinel-primary"), (store.EngineFacts["profile"], store.EngineFacts["topology"]));

        var receipt = await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("one"), cancellationToken: Ct);
        Assert.Equal(AcknowledgementProfiles.RedisAofAlwaysSentinel, receipt.ConfirmedAcknowledgementProfile);
    }

    [Fact]
    public async Task A_promotion_that_continues_the_history_pauses_writes_then_resumes_on_the_new_primary_with_every_job_intact()
    {
        await using var set = await SentinelSet.StartAsync();
        await using var provider = await HostAsync(set, "r2_" + Guid.NewGuid().ToString("N")[..8]);
        var store = provider.GetRequiredService<RedisStore>();
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var before = new List<DispatchOperationId>();
        for (var i = 0; i < 30; i++)
        {
            var p = prepared.Prepare(new PingJob("pre" + i));
            await prepared.DispatchAsync(p, Ct);
            before.Add(p.OperationId);
        }

        await set.WaitReplicasSyncedAsync();
        var oldPrimary = set.NodePorts[0];
        var epoch = store.AuthoritySnapshot!.Epoch;
        await set.KillNodeAsync(oldPrimary);

        // Until a replacement is verified, nothing is accepted: every attempt is refused or fails, and the refusal for no authority shows up.
        var refusals = new List<string>();
        var resumed = false;
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline && !resumed)
        {
            var failure = await TryDispatchAsync(provider, "during");
            if (failure is null)
            {
                resumed = true;
            }
            else
            {
                refusals.Add(failure);
                await Task.Delay(250, Ct);
            }
        }

        Assert.True(resumed, "writes never resumed on the new primary");
        Assert.Contains(refusals, r => r.Contains("store_authority_unverified", StringComparison.Ordinal));
        var after = store.AuthoritySnapshot!;
        Assert.Equal(RxAuthorityState.Verified, after.State);
        Assert.NotEqual(set.Primary, after.Primary);
        Assert.True(after.Epoch > epoch);
        Assert.Equal("127.0.0.1:" + await set.PrimaryPortAsync(), after.Primary);

        foreach (var id in before)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await prepared.ResolveAsync(id, Ct)).Status);
        }

        // The old primary comes back as a replica of the new one; the host keeps using the new primary.
        await set.StartNodeAsync(oldPrimary);
        await set.WaitUntilAsync("old primary demoted", async () => (await set.NodeCliAsync(oldPrimary, "role")).StartsWith("slave", StringComparison.Ordinal), 60);
        Assert.Null(await TryDispatchAsync(provider, "after"));
        Assert.Equal(after.Primary, store.AuthoritySnapshot!.Primary);
    }

    [Fact]
    public async Task A_promotion_that_does_not_cover_the_verified_history_stops_all_writes_until_an_operator_acts()
    {
        await using var set = await SentinelSet.StartAsync();
        await using var provider = await HostAsync(set, "r3_" + Guid.NewGuid().ToString("N")[..8]);
        var store = provider.GetRequiredService<RedisStore>();
        Assert.Null(await TryDispatchAsync(provider, "synced"));
        await set.WaitReplicasSyncedAsync();

        // Both replicas are pointed away from the primary; it acknowledges more writes that they never see, and the host verifies the primary
        // at that offset. (A paused container would not do: its socket still receives and buffers the stream.)
        await set.NodeCliAsync(set.NodePorts[1], "replicaof", "127.0.0.1", "1");
        await set.NodeCliAsync(set.NodePorts[2], "replicaof", "127.0.0.1", "1");
        for (var i = 0; i < 20; i++)
        {
            Assert.Null(await TryDispatchAsync(provider, "unreplicated" + i));
        }

        var verifiedAt = store.AuthoritySnapshot!.VerifiedAt;
        await WaitForAsync(store, s => s.VerifiedAt > verifiedAt, "a verification after the unreplicated writes");
        await set.KillNodeAsync(set.NodePorts[0]);

        var blocked = await WaitForAsync(store, s => s.State == RxAuthorityState.Blocked, "the host to refuse the lossy promotion", 90);
        Assert.Contains("authority_continuity_unverified", blocked.Reason, StringComparison.Ordinal);

        // Nothing is accepted, even though the new primary is healthy and Sentinel names it.
        var failure = await TryDispatchAsync(provider, "after-promotion");
        Assert.NotNull(failure);
        Assert.Contains("authority_continuity_unverified", failure, StringComparison.Ordinal);
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        Assert.Equal(RxAuthorityState.Blocked, store.AuthoritySnapshot!.State);
        var newPrimary = await set.PrimaryPortAsync();
        Assert.Equal("0", (await set.NodeCliAsync(newPrimary, "eval", "return #redis.call('keys','*after-promotion*')", "0")).Replace("(integer) ", "", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_process_cut_off_from_the_Sentinels_stops_writing_when_its_lease_runs_out_and_resumes_when_they_return()
    {
        await using var set = await SentinelSet.StartAsync();
        await using var provider = await HostAsync(set, "r4_" + Guid.NewGuid().ToString("N")[..8]);
        var store = provider.GetRequiredService<RedisStore>();
        Assert.Null(await TryDispatchAsync(provider, "before"));

        await set.PauseSentinelsAsync();
        try
        {
            await WaitForAsync(store, s => s.State != RxAuthorityState.Verified, "the lease to run out");
            var sizeBefore = await set.NodeCliAsync(set.NodePorts[0], "dbsize");
            var refused = await TryDispatchAsync(provider, "cut-off");
            Assert.NotNull(refused);
            Assert.Contains("store_authority_unverified", refused, StringComparison.Ordinal);
            Assert.Equal(sizeBefore, await set.NodeCliAsync(set.NodePorts[0], "dbsize"));
        }
        finally
        {
            await set.UnpauseSentinelsAsync();
        }

        await WaitForAsync(store, s => s.State == RxAuthorityState.Verified, "authority after the Sentinels return");
        Assert.Null(await TryDispatchAsync(provider, "after"));
        Assert.Equal(set.Primary, store.AuthoritySnapshot!.Primary);
    }

    [Fact]
    public async Task A_host_that_cannot_reach_any_Sentinel_does_not_start()
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("r5_" + Guid.NewGuid().ToString("N")[..8]);
            TestJobs.Register(r);
            r.UseRedis("127.0.0.1:1,127.0.0.1:2,127.0.0.1:3,serviceName=mymaster,connectTimeout=300", o =>
                o.Sentinel = new RedisSentinelOptions { AuthorityRecheckInterval = TimeSpan.FromMilliseconds(500), AuthorityTtl = TimeSpan.FromSeconds(2) });
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains("sentinel_majority_unreachable", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Sentinel_mode_needs_Sentinel_endpoints_a_service_name_and_the_always_profile_and_standalone_still_refuses_them()
    {
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.SentinelOptions("10.0.0.1:6379", TimeSpan.FromSeconds(1))).Code);
        var (data, seeds) = RxClient.SentinelOptions("10.0.0.1:26379,10.0.0.2:26379,serviceName=main,password=data,sentinelPassword=sent", TimeSpan.FromSeconds(1));
        Assert.Empty(data.EndPoints);
        Assert.Equal(("main", "data", "sent", 2), (seeds.ServiceName, data.Password, seeds.Password, seeds.Endpoints.Count));
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.Options("10.0.0.1:26379,serviceName=mymaster", TimeSpan.FromSeconds(1))).Code);

    }

    [Theory]
    [InlineData("persistence")]
    [InlineData("lease")]
    public async Task Invalid_Sentinel_settings_are_refused_at_startup(string which)
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("r6");
            TestJobs.Register(r);
            r.UseRedis("10.0.0.1:26379,serviceName=m", o =>
            {
                o.Sentinel = which == "lease"
                    ? new RedisSentinelOptions { AuthorityRecheckInterval = TimeSpan.FromSeconds(3), AuthorityTtl = TimeSpan.FromSeconds(4) }
                    : new RedisSentinelOptions();
                o.Persistence = which == "persistence" ? RedisPersistence.AofEverySecond : RedisPersistence.AofAlways;
            });
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains(which == "lease" ? "AuthorityTtl" : "appendfsync always", ex.ToString(), StringComparison.Ordinal);
    }
}
