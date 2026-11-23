using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Protocol;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>
/// The Sentinel and Cluster gaps of perf round two (22 §2.5, §2.6): operator commands that read every primary of a cluster or find the primary
/// through the Sentinels, a <c>MOVED</c> reply closing authority, and refusals for a wrong password, a missing ACL permission and a TLS mismatch.
/// Real throwaway containers (<see cref="ClusterSet"/>, <see cref="SentinelSet"/>).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-R")]
public sealed class RedisHaGapsTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly RedisOptions ClusterTopology = new() { Cluster = new() };

    static readonly RedisOptions SentinelTopology = new() { Sentinel = new() };

    static ServiceProvider Build(string application, string configuration, Action<RedisOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(configuration, configure);
        });
        return services.BuildServiceProvider();
    }

    static async Task<ServiceProvider> StartAsync(string application, string configuration, Action<RedisOptions> configure)
    {
        var provider = Build(application, configuration, configure);
        await provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct);
        return provider;
    }

    static void Cluster(RedisOptions o) =>
        o.Cluster = new RedisClusterOptions { AuthorityRecheckInterval = TimeSpan.FromMilliseconds(500), AuthorityTtl = TimeSpan.FromSeconds(2) };

    static void Sentinel(RedisOptions o) =>
        o.Sentinel = new RedisSentinelOptions { AuthorityRecheckInterval = TimeSpan.FromMilliseconds(500), AuthorityTtl = TimeSpan.FromSeconds(2) };

    /// <summary>Startup of a host on <paramref name="configuration"/> fails; returns the whole exception text.</summary>
    static async Task<string> RefusedAtStartupAsync(string application, string configuration, Action<RedisOptions> configure)
    {
        await using var provider = Build(application, configuration, configure);
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        return ex.ToString();
    }

    static async Task DispatchAsync(ServiceProvider provider, string name) =>
        await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob(name), cancellationToken: Ct);

    static async Task<string?> TryDispatchAsync(ServiceProvider provider, string name)
    {
        try
        {
            await DispatchAsync(provider, name);
            return null;
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
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

    static async Task<long> KeysInSlotAsync(ClusterSet set, int port, string application) =>
        long.Parse(await set.CliAsync(port, "cluster", "countkeysinslot", RxClient.SlotOf(application).ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);

    [Fact]
    public async Task Operator_commands_read_every_primary_of_a_cluster_and_refuse_when_a_primary_that_owns_slots_is_unreachable()
    {
        await using var set = await ClusterSet.StartAsync();
        await RunwickRedisMigrations.MigrateAsync(set.Configuration, ClusterTopology, Ct);
        string[] apps = [ClusterSet.ApplicationOn(0, "m0_"), ClusterSet.ApplicationOn(1, "m1_"), ClusterSet.ApplicationOn(2, "m2_")];
        foreach (var app in apps)
        {
            await using var host = await StartAsync(app, set.Configuration, Cluster);
            await DispatchAsync(host, "one");
        }

        // Each application lives on a different primary; a one-node scan would have found one of them.
        var status = await RunwickRedisMigrations.GetStatusAsync(set.Configuration, ClusterTopology, Ct);
        Assert.Equal(apps.Order(StringComparer.Ordinal), status.Applications.Select(a => a.Application));

        var drained = await RunwickRedisMigrations.SetAdmissionModeAsync(set.Configuration, null, "drain", "test", ClusterTopology, Ct);
        Assert.Equal(3, drained.Count(c => c.Changed && c.Mode == "drain"));
        var raised = await RunwickRedisMigrations.ActivateCapabilityAsync(set.Configuration, null, "scheduler", "test", ClusterTopology, Ct);
        Assert.Equal(3, raised.Count(c => c.Changed));
        var one = await RunwickRedisMigrations.SetAdmissionModeAsync(set.Configuration, apps[2], "open", "test", ClusterTopology, Ct);
        Assert.Equal(apps[2], Assert.Single(one).Application);

        await RunwickRedisMigrations.SetAdmissionModeAsync(set.Configuration, apps[2], "drain", "test", ClusterTopology, Ct);
        await RunwickRedisMigrations.SetAdmissionModeAsync(set.Configuration, null, "maintenance", "test", ClusterTopology, Ct);
        var change = await RunwickRedisMigrations.ReassignStoreIdAsync(set.Configuration, ClusterTopology, Ct);
        Assert.Equal(change.NewStoreId, (await RunwickRedisMigrations.GetStatusAsync(set.Configuration, ClusterTopology, Ct)).StoreId);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(RxClient.Hex(change.NewStoreId), await set.CliAsync(set.Ports[i], "hget", RxClient.KeyPrefix(apps[i]) + "meta", "store"));
        }

        // A primary that owns slots and cannot be reached: the command refuses instead of reporting a partial store.
        await set.PauseNodeAsync(set.Ports[2]);
        try
        {
            var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.GetStatusAsync(set.Configuration, ClusterTopology, Ct));
            Assert.Equal("store_unavailable", refused.Code);
        }
        finally
        {
            await set.UnpauseNodeAsync(set.Ports[2]);
        }
    }

    [Fact]
    public async Task A_MOVED_reply_closes_authority_at_once_before_the_voters_report_the_move()
    {
        await using var set = await ClusterSet.StartAsync();
        await RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct);
        var application = ClusterSet.ApplicationOn(0, "mv_");
        // A long lease, so only the MOVED reply (not the lease or a voter) can close authority while verifications are held below.
        await using var provider = await StartAsync(application, set.Configuration,
            o => o.Cluster = new RedisClusterOptions { AuthorityRecheckInterval = TimeSpan.FromSeconds(1), AuthorityTtl = TimeSpan.FromSeconds(30) });
        var store = provider.GetRequiredService<RedisStore>();
        var authority = store.Runner.Client.Authority!;
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var before = new List<DispatchOperationId>();
        for (var i = 0; i < 5; i++)
        {
            var p = prepared.Prepare(new PingJob("b" + i));
            await prepared.DispatchAsync(p, Ct);
            before.Add(p.OperationId);
        }

        // Hold the next verification just before it would open authority: the voters cannot see the move while it is held.
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hold = true;
        authority.BeforeAllow = async () =>
        {
            if (Volatile.Read(ref hold))
            {
                held.TrySetResult();
                await release.Task;
            }
        };
        await held.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        Assert.Equal(RxAuthorityState.Verified, store.AuthoritySnapshot!.State);

        // Move the slot with its keys in one go (importing, migrating, MIGRATE, then the new owner everywhere).
        int from = set.Ports[0], to = set.Ports[1];
        var slot = RxClient.SlotOf(application).ToString(CultureInfo.InvariantCulture);
        string fromId = await set.NodeIdAsync(from), toId = await set.NodeIdAsync(to);
        var script = $"redis-cli -p {to} cluster setslot {slot} importing {fromId} && redis-cli -p {from} cluster setslot {slot} migrating {toId} && "
            + $"redis-cli -p {from} cluster getkeysinslot {slot} 1000 | xargs redis-cli -p {from} migrate 127.0.0.1 {to} '' 0 5000 keys && "
            + string.Join(" && ", new[] { to, from, set.Ports[2] }.Select(p => $"redis-cli -p {p} cluster setslot {slot} node {toId}"));
        var moved = await RedisNode.Docker("exec", set.NodeName(from), "sh", "-c", script);
        Assert.True(moved.Code == 0, moved.Output);
        Assert.Equal(0, await KeysInSlotAsync(set, from, application));

        // The next command is sent to the old owner, which answers MOVED: authority closes on that reply alone.
        await TryDispatchAsync(provider, "first-after-move"); // the client follows the redirect, so this one may land on the new owner (22 §2.6)
        var closed = await WaitForAsync(store, s => s.State != RxAuthorityState.Verified, "the MOVED reply to close authority", 15);
        Assert.Equal("hash_slot_moved", closed.Reason);
        Assert.Contains(closed.Recent, e => e.Text == "authority closed: hash_slot_moved");
        var refused = await TryDispatchAsync(provider, "while-closed");
        Assert.NotNull(refused);
        Assert.Contains("store_authority_unverified", refused, StringComparison.Ordinal);

        // The held verification predates the MOVED reply, so releasing it opens nothing; the next one verifies the new owner.
        Volatile.Write(ref hold, false);
        release.SetResult();
        var after = await WaitForAsync(store, s => s.State == RxAuthorityState.Verified && s.Primary == "127.0.0.1:" + to, "authority on the new owner", 60);
        Assert.DoesNotContain(after.Recent, e => e.Text.Contains("verified primary 127.0.0.1:" + from, StringComparison.Ordinal)
            && e.At > closed.Recent.First(r => r.Text == "authority closed: hash_slot_moved").At);
        await DispatchAsync(provider, "after-move");
        foreach (var id in before)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await prepared.ResolveAsync(id, Ct)).Status);
        }
    }

    [Fact]
    public async Task A_cluster_owner_whose_ACL_denies_CONFIG_or_a_TLS_mismatch_is_refused_before_anything_is_written()
    {
        await using var set = await ClusterSet.StartAsync();
        await RunwickRedisMigrations.MigrateClusterAsync(set.Configuration, Ct);
        var application = ClusterSet.ApplicationOn(1, "acl_");
        var owner = set.Ports[1];
        foreach (var port in set.Ports)
        {
            await set.CliAsync(port, "acl", "setuser", "rw", "on", ">pw", "~*", "&*", "+@all");
        }

        await set.CliAsync(owner, "acl", "setuser", "rw", "-config"); // one node lacks one permission
        var restricted = set.Configuration + ",user=rw,password=pw";

        // The migrator checks every primary, so the one node is enough to stop it.
        var migrate = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(restricted, ClusterTopology, Ct));
        Assert.Equal("store_profile_mismatch", migrate.Code);
        Assert.Contains("of 3", migrate.Message, StringComparison.Ordinal);

        // A host whose slot owner cannot be verified does not start and writes nothing there.
        var host = await RefusedAtStartupAsync(application, restricted, Cluster);
        Assert.Contains("store_profile_mismatch", host, StringComparison.Ordinal);
        Assert.Equal(0, await KeysInSlotAsync(set, owner, application));

        // TLS asked of nodes that do not speak it: no node votes, nothing is sent.
        var tls = await RefusedAtStartupAsync(application, set.Configuration + ",ssl=true,connectTimeout=1000", Cluster);
        Assert.Contains("cluster_majority_unreachable", tls, StringComparison.Ordinal);
        Assert.Equal(0, await KeysInSlotAsync(set, owner, application));
    }

    [Fact]
    public async Task Operator_commands_find_the_primary_through_the_Sentinels_follow_a_failover_and_refuse_a_wrong_password_or_a_TLS_mismatch()
    {
        await using var set = await SentinelSet.StartAsync();
        var first = await RunwickRedisMigrations.MigrateAsync(set.Configuration, SentinelTopology, Ct);
        Assert.Equal(3, first.AppliedPhases.Count);
        Assert.Empty((await RunwickRedisMigrations.MigrateAsync(set.Configuration, SentinelTopology, Ct)).AppliedPhases);
        Assert.Equal("1", await set.NodeCliAsync(set.NodePorts[0], "exists", "rw:store"));

        var application = "sg_" + Guid.NewGuid().ToString("N")[..8];
        await using (var host = await StartAsync(application, set.Configuration, Sentinel))
        {
            await DispatchAsync(host, "one");
        }

        var status = await RunwickRedisMigrations.GetStatusAsync(set.Configuration, SentinelTopology, Ct);
        Assert.Equal(first.StoreId, status.StoreId);
        Assert.Equal(application, Assert.Single(status.Applications).Application);

        // A wrong password for the data node: named as such, and the primary is not touched.
        await set.NodeCliAsync(set.NodePorts[0], "acl", "setuser", "rw", "on", ">right", "~*", "&*", "+@all");
        var wrong = set.Configuration + ",user=rw,password=wrong,sentinelUser=default,sentinelPassword=unused";
        var size = await set.NodeCliAsync(set.NodePorts[0], "dbsize");
        var migrate = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(wrong, SentinelTopology, Ct));
        Assert.Equal("primary_authentication_failed", migrate.Code);
        Assert.Contains("primary_authentication_failed", await RefusedAtStartupAsync(application, wrong, Sentinel), StringComparison.Ordinal);
        Assert.Equal(size, await set.NodeCliAsync(set.NodePorts[0], "dbsize"));

        // TLS asked of Sentinels that do not speak it: no Sentinel answers, so there is no primary to write to.
        var tls = set.Configuration + ",ssl=true,connectTimeout=1000";
        Assert.Contains("sentinel_majority_unreachable", await RefusedAtStartupAsync(application, tls, Sentinel), StringComparison.Ordinal);
        Assert.Equal("store_unavailable", (await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.GetStatusAsync(tls, SentinelTopology, Ct))).Code);
        Assert.Equal(size, await set.NodeCliAsync(set.NodePorts[0], "dbsize"));

        // After a failover the same string reaches the promoted primary.
        await set.WaitReplicasSyncedAsync();
        await set.KillNodeAsync(set.NodePorts[0]);
        StoreAdminStatus? moved = null;
        await set.WaitUntilAsync("status through the promoted primary", async () =>
        {
            try
            {
                moved = await RunwickRedisMigrations.GetStatusAsync(set.Configuration, SentinelTopology, Ct);
                return true;
            }
            catch (RedisMigrationException)
            {
                return false;
            }
        }, 90);
        Assert.NotEqual(set.NodePorts[0], await set.PrimaryPortAsync());
        Assert.Equal(first.StoreId, moved!.StoreId);
        Assert.Equal(application, Assert.Single(moved.Applications).Application);
    }
}
