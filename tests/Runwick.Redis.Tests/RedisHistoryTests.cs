using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>REDIS-15 with real servers: a lossy replica promotion and an older AOF restored at the same endpoint (22 §2.4, §12).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisHistoryTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static async Task<TestClient> ClientOnAsync(RedisNode node, string name)
    {
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct);
        return await TestClient.CreateAsync(RedisServer.Prefix + node.Configuration, name + "_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r => TestJobs.Register(r));
    }

    static async Task Dispatch(TestClient c, string tag, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var job = c.Prepared.Prepare(new PingJob($"{tag}{i}"));
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await c.Prepared.DispatchAsync(job, Ct); // the same operation is resent while the client reconnects
                    break;
                }
                catch (Exception) when (attempt < 60)
                {
                    await Task.Delay(500, Ct);
                }
            }
        }

        // Reading the descriptor records the revision this process has now seen (22 §2.4).
        await Store(c).DescribeAsync(c.Application, Ct);
    }

    /// <summary>After the server came back with an older history: the client detects it and refuses everything.</summary>
    static async Task AssertDiscontinuity(TestClient c)
    {
        var until = DateTime.UtcNow.AddSeconds(45);
        while (Store(c).Runner.HistoryFailure is null && DateTime.UtcNow < until)
        {
            await Task.Delay(250, Ct);
        }

        Assert.Equal("history_discontinuity", Store(c).Runner.HistoryFailure);
        var refused = await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.ReserveAsync());
        Assert.Contains("history_discontinuity", refused.Message, StringComparison.Ordinal);
        await Assert.ThrowsAnyAsync<Exception>(() => c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct));
    }

    [Fact]
    [Trait("Case", "REDIS-15")]
    [Trait("Limitation", "lossy-replica-promotion")]
    public async Task A_deliberately_lossy_replica_promotion_is_detected_where_the_revision_went_back_and_reported_as_a_limitation()
    {
        await using var primary = await RedisNode.StartAsync([.. RedisNode.Profile, "--protected-mode", "no"]);
        await using var replica = await RedisNode.StartAsync([.. RedisNode.Profile, "--replicaof", await primary.IpAsync(), "6379", "--protected-mode", "no"]);
        await using var c = await ClientOnAsync(primary, "redis15rep");
        await Dispatch(c, "kept", 3);
        for (var i = 0; i < 100; i++)
        {
            var info = await replica.CliAsync("info", "replication");
            if (info.Contains("master_link_status:up", StringComparison.Ordinal) && info.Contains("master_sync_in_progress:0", StringComparison.Ordinal)
                && await primary.CliAsync("dbsize") == await replica.CliAsync("dbsize"))
            {
                break;
            }

            await Task.Delay(100, Ct);
        }

        Assert.Equal(await primary.CliAsync("dbsize"), await replica.CliAsync("dbsize"));
        await replica.CliAsync("replicaof", "127.0.0.1", "9"); // replication stops; the primary goes on acknowledging commands
        await Dispatch(c, "lost", 3);

        // The primary is lost and the lagging replica is promoted to take over its endpoint (its AOF becomes the primary's).
        var port = primary.Port;
        await primary.KillAsync();
        await replica.KillAsync();
        await primary.DisposeAsync();
        await using var promoted = await RedisNode.StartAsync(volumesFrom: replica.Name, port: port);
        await AssertDiscontinuity(c);

        // Limitation (22 §2.4, driver-contract-suite §5, §7): detection needs a client that had seen the higher revision. A process
        // that never saw it, or a promotion that lands on a revision at or above the last one seen, goes undetected, so a replica
        // promotion is never counted as no-history-loss conformance.
        TestContext.Current.SendDiagnosticMessage("REDIS-15 limitation: lossy replica promotion detected only because this process had seen the higher revision; not counted as no-history-loss conformance.");
    }

    [Fact]
    [Trait("Case", "REDIS-15")]
    public async Task An_older_AOF_restored_at_the_same_endpoint_is_history_discontinuity()
    {
        var backup = Path.Combine(Path.GetTempPath(), "g4c-aof-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(backup);
        try
        {
            await using var node = await RedisNode.StartAsync();
            await using var c = await ClientOnAsync(node, "redis15aof");
            await Dispatch(c, "old", 3);
            await node.KillAsync();
            await node.WithDataAndBackupAsync(backup, "cp -a /data/. /backup/");
            await node.StartAgainAsync();
            await Dispatch(c, "new", 3);
            await node.KillAsync();
            await node.WithDataAndBackupAsync(backup, "rm -rf /data/* && cp -a /backup/. /data/");
            await node.StartAgainAsync();
            await AssertDiscontinuity(c);
        }
        finally
        {
            await RedisNode.Run("rm", "-rf", backup);
        }
    }
}
