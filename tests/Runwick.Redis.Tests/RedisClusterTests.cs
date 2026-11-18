using System.Globalization;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Protocol;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Perf round two, work areas R and M (22 §2.6): opt-in Redis Cluster mode. Real containers: three primaries and one replica of the first
/// primary. All of an application's keys share one slot, so one primary owns the whole application; writes go out only while a majority of the
/// listed nodes name the same healthy owner for that slot and its profile and history are verified.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-R")]
public sealed class RedisClusterTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<ServiceProvider> HostAsync(ClusterSet set, string application, bool migrate = true)
    {
        if (migrate)
        {
            await RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct);
        }

        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(set.Configuration, o =>
            {
                o.Cluster = new RedisClusterOptions { AuthorityRecheckInterval = TimeSpan.FromMilliseconds(500), AuthorityTtl = TimeSpan.FromSeconds(2) };
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
                throw new TimeoutException($"Timed out waiting for {what}; authority is {s.State} ({s.Reason}), primary {s.Primary}; events: " + string.Join(" | ", s.Recent.Select(e => e.Text)));
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

    /// <summary>The number of keys of the application's slot held by the node on <paramref name="port"/> (zero on a node that does not own it).</summary>
    static async Task<long> KeysInSlotAsync(ClusterSet set, int port, string application) =>
        long.Parse(await set.CliAsync(port, "cluster", "countkeysinslot", RxClient.SlotOf(application).ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);

    [Fact]
    public async Task A_host_verifies_the_owner_of_its_slot_before_it_writes_and_reports_the_cluster_profile_and_topology()
    {
        await using var set = await ClusterSet.StartAsync();
        var application = ClusterSet.ApplicationOn(1, "c1_");
        await using var provider = await HostAsync(set, application);
        var store = provider.GetRequiredService<RedisStore>();

        var snapshot = store.AuthoritySnapshot!;
        Assert.Equal(RxAuthorityState.Verified, snapshot.State);
        Assert.Equal("127.0.0.1:" + set.Ports[1], snapshot.Primary);
        Assert.Equal(("cluster", RxClient.SlotOf(application)), (snapshot.Mode, snapshot.Slot));
        Assert.Equal(3, snapshot.SentinelsConfigured);
        Assert.True(snapshot.SentinelsAgreeing >= 2);
        Assert.Equal(AcknowledgementProfiles.RedisAofAlwaysCluster, store.AcknowledgementProfile);
        Assert.Equal(("redis-aof-always-cluster-v1", "cluster-primary"), (store.EngineFacts["profile"], store.EngineFacts["topology"]));

        // The verified topology: four nodes, one of them the owner of this application's slot.
        Assert.Equal(4, snapshot.Nodes!.Count);
        Assert.Equal(3, snapshot.Nodes.Count(n => n.Role == "primary"));
        var owner = Assert.Single(snapshot.Nodes, n => n.OwnsApplicationSlot);
        Assert.Equal(snapshot.Primary, owner.Address);

        var receipt = await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("one"), cancellationToken: Ct);
        Assert.Equal(AcknowledgementProfiles.RedisAofAlwaysCluster, receipt.ConfirmedAcknowledgementProfile);
        Assert.True(await KeysInSlotAsync(set, set.Ports[1], application) > 0);
        Assert.Equal(0, await KeysInSlotAsync(set, set.Ports[0], application));
        Assert.Equal(0, await KeysInSlotAsync(set, set.Ports[2], application));
    }

    [Fact]
    public async Task Two_applications_whose_slots_land_on_different_primaries_run_independently_and_each_is_pinned_to_its_own_owner()
    {
        await using var set = await ClusterSet.StartAsync();
        var first = ClusterSet.ApplicationOn(0, "ca_");
        var second = ClusterSet.ApplicationOn(2, "cb_");
        await using var a = await HostAsync(set, first);
        await using var b = await HostAsync(set, second, migrate: false);
        var storeA = a.GetRequiredService<RedisStore>();
        var storeB = b.GetRequiredService<RedisStore>();
        Assert.Equal("127.0.0.1:" + set.Ports[0], storeA.AuthoritySnapshot!.Primary);
        Assert.Equal("127.0.0.1:" + set.Ports[2], storeB.AuthoritySnapshot!.Primary);

        for (var i = 0; i < 10; i++)
        {
            Assert.Null(await TryDispatchAsync(a, "a" + i));
            Assert.Null(await TryDispatchAsync(b, "b" + i));
        }

        Assert.True(await KeysInSlotAsync(set, set.Ports[0], first) > 0);
        Assert.True(await KeysInSlotAsync(set, set.Ports[2], second) > 0);
        Assert.Equal(0, await KeysInSlotAsync(set, set.Ports[2], first));
        Assert.Equal(0, await KeysInSlotAsync(set, set.Ports[0], second));

        // One application's primary going quiet pauses that application only.
        await set.PauseNodeAsync(set.Ports[0]);
        try
        {
            await WaitForAsync(storeA, s => s.State != RxAuthorityState.Verified, "the first application's authority to close");
            Assert.NotNull(await TryDispatchAsync(a, "a-paused"));
            Assert.Null(await TryDispatchAsync(b, "b-while-a-is-paused"));
            Assert.Equal(RxAuthorityState.Verified, storeB.AuthoritySnapshot!.State);
        }
        finally
        {
            await set.UnpauseNodeAsync(set.Ports[0]);
        }
    }

    [Fact]
    public async Task A_forged_key_is_refused_before_any_write_in_a_cluster()
    {
        await using var set = await ClusterSet.StartAsync();
        await using var provider = await HostAsync(set, ClusterSet.ApplicationOn(0, "cf_"));
        var store = provider.GetRequiredService<RedisStore>();
        long Total() => set.Ports.Take(3).Select(p => set.DbSizeAsync(p).GetAwaiter().GetResult()).Sum();
        var before = Total();
        foreach (var forged in new[] { "rw:{4A}:meta", "rw:{zz}:meta", "rw:{6162}:meta:x", "other:{6162}:meta", "rw:{6162}:meta\nrw:{6162}:" })
        {
            var op = new RxOp("forged", Guid.NewGuid(), Operations.Dispatch, DateTimeOffset.UtcNow.AddSeconds(30), new byte[32], RxLane.Producer, 'c');
            var frame = store.Runner.Client.Frame(op, store.StoreId, "0", "0", "initial", "", 0);
            var a = await store.Runner.Client.EvalAsync(RxLane.Producer, RxScript.Write("dispatch"), forged, frame, null, Ct);
            Assert.Equal(["R", "InvariantFault", "N", "0", "invalid_transition"], a.Take(5).Select(x => (string)x!));
        }

        Assert.Equal(before, Total());
    }

    [Fact]
    public async Task Moving_the_slot_pauses_admission_until_the_new_owner_is_verified_and_no_job_is_lost()
    {
        await using var set = await ClusterSet.StartAsync();
        var application = ClusterSet.ApplicationOn(0, "cm_");
        var slot = RxClient.SlotOf(application);
        await using var provider = await HostAsync(set, application);
        var store = provider.GetRequiredService<RedisStore>();
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var before = new List<DispatchOperationId>();
        for (var i = 0; i < 20; i++)
        {
            var p = prepared.Prepare(new PingJob("pre" + i));
            await prepared.DispatchAsync(p, Ct);
            before.Add(p.OperationId);
        }

        var epoch = store.AuthoritySnapshot!.Epoch;
        var (from, to) = (set.Ports[0], set.Ports[1]);
        var (fromId, toId) = (await set.NodeIdAsync(from), await set.NodeIdAsync(to));
        var slotText = slot.ToString(CultureInfo.InvariantCulture);

        // The slot starts moving: the owner is still the owner, but the move is unresolved, so nothing is sent.
        await set.CliAsync(to, "cluster", "setslot", slotText, "importing", fromId);
        await set.CliAsync(from, "cluster", "setslot", slotText, "migrating", toId);
        await WaitForAsync(store, s => s.State == RxAuthorityState.Unresolved, "the move to close authority");
        Assert.Equal("slot_owner_failing_or_moving", store.AuthoritySnapshot!.Reason);
        var heldBefore = await KeysInSlotAsync(set, from, application);
        var refused = await TryDispatchAsync(provider, "during-move");
        Assert.NotNull(refused);
        Assert.Contains("store_authority_unverified", refused, StringComparison.Ordinal);
        Assert.Equal(heldBefore, await KeysInSlotAsync(set, from, application));

        // The keys move, then ownership changes. The old owner stays a healthy primary, so the history check is the proof of continuity.
        while (true)
        {
            var keys = (await set.CliAsync(from, "cluster", "getkeysinslot", slotText, "100")).Split('\n', StringSplitOptions.RemoveEmptyEntries);
            if (keys.Length == 0)
            {
                break;
            }

            var migrate = await set.CliAsync(from, ["migrate", "127.0.0.1", to.ToString(CultureInfo.InvariantCulture), "", "0", "5000", "keys", .. keys]);
            Assert.Contains("OK", migrate, StringComparison.Ordinal);
        }

        foreach (var node in set.Ports.Take(3).OrderBy(p => p == to ? 0 : 1))
        {
            await set.CliAsync(node, "cluster", "setslot", slotText, "node", toId);
        }

        var after = await WaitForAsync(store, s => s.State == RxAuthorityState.Verified && s.Primary == "127.0.0.1:" + to, "authority on the new owner", 60);
        Assert.True(after.Epoch > epoch);
        Assert.Null(await TryDispatchAsync(provider, "after-move"));
        foreach (var id in before)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await prepared.ResolveAsync(id, Ct)).Status);
        }

        Assert.True(await KeysInSlotAsync(set, to, application) > 0);
        Assert.Equal(0, await KeysInSlotAsync(set, from, application));
    }

    [Fact]
    public async Task A_failover_that_continues_the_history_pauses_writes_then_resumes_on_the_promoted_replica_with_every_job_intact()
    {
        await using var set = await ClusterSet.StartAsync();
        var application = ClusterSet.ApplicationOn(0, "cv_");
        await using var provider = await HostAsync(set, application);
        var store = provider.GetRequiredService<RedisStore>();
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var before = new List<DispatchOperationId>();
        for (var i = 0; i < 30; i++)
        {
            var p = prepared.Prepare(new PingJob("pre" + i));
            await prepared.DispatchAsync(p, Ct);
            before.Add(p.OperationId);
        }

        await set.WaitReplicaSyncedAsync();
        var epoch = store.AuthoritySnapshot!.Epoch;
        await set.KillNodeAsync(set.Ports[0]);

        var refusals = new List<string>();
        var resumed = false;
        var deadline = DateTime.UtcNow.AddSeconds(90);
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

        var seen = store.AuthoritySnapshot!;
        Assert.True(resumed, $"writes never resumed; authority {seen.State} ({seen.Reason}) primary {seen.Primary}; events: " + string.Join(" | ", seen.Recent.Select(e => e.Text)));
        Assert.Contains(refusals, r => r.Contains("store_authority_unverified", StringComparison.Ordinal));
        Assert.Equal(RxAuthorityState.Verified, seen.State);
        Assert.Equal("127.0.0.1:" + set.ReplicaPort, seen.Primary);
        Assert.True(seen.Epoch > epoch);
        foreach (var id in before)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await prepared.ResolveAsync(id, Ct)).Status);
        }
    }

    [Fact]
    public async Task A_promotion_that_does_not_cover_the_verified_history_stops_all_writes_until_an_operator_acts()
    {
        await using var set = await ClusterSet.StartAsync();
        var application = ClusterSet.ApplicationOn(0, "cl_");
        await using var provider = await HostAsync(set, application);
        var store = provider.GetRequiredService<RedisStore>();
        Assert.Null(await TryDispatchAsync(provider, "synced"));
        await set.WaitReplicaSyncedAsync();

        // The replica can no longer follow its primary (a wrong masterauth fails every new sync), and the primary acknowledges more writes that
        // the replica never sees. The host verifies the primary at that offset.
        await set.CliAsync(set.ReplicaPort, "config", "set", "masterauth", "not-the-password");
        await set.CliAsync(set.Ports[0], "client", "kill", "type", "replica");
        await set.WaitUntilAsync("replica link down", async () => (await set.CliAsync(set.ReplicaPort, "info", "replication")).Contains("master_link_status:down", StringComparison.Ordinal), 30);
        for (var i = 0; i < 20; i++)
        {
            Assert.Null(await TryDispatchAsync(provider, "unreplicated" + i));
        }

        var verifiedAt = store.AuthoritySnapshot!.VerifiedAt;
        await WaitForAsync(store, s => s.VerifiedAt > verifiedAt, "a verification after the unreplicated writes");
        await set.KillNodeAsync(set.Ports[0]);

        var blocked = await WaitForAsync(store, s => s.State == RxAuthorityState.Blocked, "the host to refuse the lossy promotion", 120);
        Assert.Contains("authority_continuity_unverified", blocked.Reason, StringComparison.Ordinal);

        var failure = await TryDispatchAsync(provider, "after-promotion");
        Assert.NotNull(failure);
        Assert.Contains("authority_continuity_unverified", failure, StringComparison.Ordinal);
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        Assert.Equal(RxAuthorityState.Blocked, store.AuthoritySnapshot!.State);
        Assert.Equal("0", (await set.CliAsync(set.ReplicaPort, "eval", "return #redis.call('keys','*after-promotion*')", "0")).Replace("(integer) ", "", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_process_that_cannot_reach_a_majority_of_the_listed_nodes_stops_writing_when_its_lease_runs_out_and_resumes_when_they_return()
    {
        await using var set = await ClusterSet.StartAsync();
        var application = ClusterSet.ApplicationOn(0, "cc_");
        await using var provider = await HostAsync(set, application);
        var store = provider.GetRequiredService<RedisStore>();
        Assert.Null(await TryDispatchAsync(provider, "before"));

        await set.PauseNodeAsync(set.Ports[1]);
        await set.PauseNodeAsync(set.Ports[2]);
        try
        {
            await WaitForAsync(store, s => s.State != RxAuthorityState.Verified, "the lease to run out");
            var sizeBefore = await set.DbSizeAsync(set.Ports[0]);
            var refused = await TryDispatchAsync(provider, "cut-off");
            Assert.NotNull(refused);
            Assert.Contains("store_authority_unverified", refused, StringComparison.Ordinal);
            Assert.Equal(sizeBefore, await set.DbSizeAsync(set.Ports[0]));
        }
        finally
        {
            await set.UnpauseNodeAsync(set.Ports[1]);
            await set.UnpauseNodeAsync(set.Ports[2]);
        }

        await WaitForAsync(store, s => s.State == RxAuthorityState.Verified, "authority after the nodes return", 60);
        Assert.Null(await TryDispatchAsync(provider, "after"));
        Assert.Equal("127.0.0.1:" + set.Ports[0], store.AuthoritySnapshot!.Primary);
    }

    [Fact]
    public async Task The_standalone_migrator_refuses_a_cluster_node_and_the_cluster_migrator_installs_the_store_once()
    {
        await using var set = await ClusterSet.StartAsync();
        var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync("127.0.0.1:" + set.Ports[0], Ct));
        Assert.Equal("topology_unsupported", refused.Code);
        var first = await RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct);
        Assert.Equal(["expand", "validate", "activate"], first.AppliedPhases);
        var second = await RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct);
        Assert.Empty(second.AppliedPhases);
        Assert.Equal(first.StoreId, second.StoreId);
    }

    [Fact]
    public async Task A_cluster_node_outside_the_always_profile_stops_the_cluster_migration()
    {
        await using var set = await ClusterSet.StartAsync();
        await set.CliAsync(set.Ports[2], "config", "set", "appendfsync", "everysec");
        var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct));
        Assert.Equal("store_profile_mismatch", refused.Code);
    }

    [Fact]
    public async Task The_slot_the_client_computes_is_the_slot_a_cluster_node_reports_for_the_key()
    {
        await using var set = await ClusterSet.StartAsync();
        foreach (var name in new[] { "shop", "r1_abcdef01", "Ünï", "a" })
        {
            var reported = await set.CliAsync(set.Ports[0], "cluster", "keyslot", RxClient.KeyPrefix(name) + "meta");
            Assert.Equal(RxClient.SlotOf(name), int.Parse(reported, CultureInfo.InvariantCulture));
        }
    }

    const string View = """
        aaaa 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460
        bbbb 127.0.0.1:7001@17001 master - 0 1 2 connected 5461-10922 [100->-cccc]
        cccc 127.0.0.1:7002@17002 master,fail - 0 1 3 connected 10923-16383
        dddd 127.0.0.1:7003@17003 slave aaaa 0 1 1 connected
        """;

    [Fact]
    public void A_vote_names_the_owner_of_the_slot_and_is_troubled_when_the_owner_is_failing_or_the_slot_is_moving()
    {
        var seed = new IPEndPoint(IPAddress.Loopback, 7000);
        var healthy = RxAuthority.ParseClusterView(View, seed, 6000);
        Assert.Equal(("127.0.0.1:7001", false), (healthy.Primary, healthy.Troubled));
        Assert.Equal(4, healthy.Nodes!.Count);
        Assert.Contains(healthy.Nodes, n => n is { Address: "127.0.0.1:7003", Role: "replica", Slots: 0 });
        Assert.Contains(healthy.Nodes, n => n is { Address: "127.0.0.1:7000", Role: "primary", Slots: 5461, OwnsApplicationSlot: false });

        var moving = RxAuthority.ParseClusterView(View, seed, 100);
        Assert.Equal(("127.0.0.1:7000", true), (moving.Primary, moving.Troubled));
        Assert.Contains(moving.Nodes!, n => n is { Address: "127.0.0.1:7001", MovingApplicationSlot: true });

        var failing = RxAuthority.ParseClusterView(View, seed, 12000);
        Assert.Equal(("127.0.0.1:7002", true), (failing.Primary, failing.Troubled));
        Assert.Contains(failing.Nodes!, n => n is { Address: "127.0.0.1:7002", Health: "fail", OwnsApplicationSlot: true });

        var importing = RxAuthority.ParseClusterView(View.Replace("[100->-cccc]", "[100-<-aaaa]", StringComparison.Ordinal), seed, 100);
        Assert.True(importing.Troubled);

        var uncovered = RxAuthority.ParseClusterView("aaaa 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-99", seed, 100);
        Assert.Null(uncovered.Primary);
        Assert.False(uncovered.Troubled);
    }

    [Fact]
    public void Cluster_mode_needs_the_cluster_nodes_and_no_service_name_and_standalone_still_refuses_several_endpoints()
    {
        var (data, seeds) = RxClient.ClusterOptions("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000,password=data", TimeSpan.FromSeconds(1));
        Assert.Equal(3, data.EndPoints.Count);
        Assert.Equal(("data", 3), (data.Password, seeds.Endpoints.Count));
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.ClusterOptions("10.0.0.1:7000,serviceName=m", TimeSpan.FromSeconds(1))).Code);
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.Options("10.0.0.1:7000,10.0.0.2:7000", TimeSpan.FromSeconds(1))).Code);
    }

    [Theory]
    [InlineData("both")]
    [InlineData("persistence")]
    [InlineData("lease")]
    public async Task Invalid_Cluster_settings_are_refused_at_startup(string which)
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("cx");
            TestJobs.Register(r);
            r.UseRedis("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000", o =>
            {
                o.Cluster = which == "lease"
                    ? new RedisClusterOptions { AuthorityRecheckInterval = TimeSpan.FromSeconds(3), AuthorityTtl = TimeSpan.FromSeconds(4) }
                    : new RedisClusterOptions();
                o.Persistence = which == "persistence" ? RedisPersistence.AofEverySecond : RedisPersistence.AofAlways;
                if (which == "both")
                {
                    o.Sentinel = new RedisSentinelOptions();
                }
            });
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains(which switch { "lease" => "AuthorityTtl", "both" => "cannot both be set", _ => "appendfsync always" }, ex.ToString(), StringComparison.Ordinal);
    }
}
