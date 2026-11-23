using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// The lean worker operations as a Redis Functions library (22 §4.1): loaded by the profile check at startup, called by FCALL, loaded
/// again when a server answers "Function not found", kept by the AOF across a restart and copied to a new replica, and replaced by EVALSHA
/// when the server refuses FUNCTION or FCALL. Every case runs on its own throwaway server (FUNCTION FLUSH and ACLs stay off the shared one).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisFunctionsTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static string Library => RxScript.Library.Name;

    static async Task<TestClient> ClientOnAsync(RedisNode node, string name, string? configuration = null)
    {
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct);
        return await TestClient.CreateAsync(RedisServer.Prefix + (configuration ?? node.Configuration), name + "_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r => TestJobs.Register(r));
    }

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    /// <summary>Dispatches one job, claims it and finishes it: the lean claim and finish.</summary>
    static async Task ClaimAndFinishAsync(TestClient c, string id)
    {
        await c.Dispatcher.DispatchAsync(new PingJob(id), cancellationToken: Ct);
        var claimed = await c.ReserveAndBeginAsync(1);
        Assert.Equal(StoreStatus.Applied, claimed.Status);
        var lease = claimed.Value!.Outcomes.Single().Begun!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
    }

    /// <summary>calls, and failed plus rejected calls, of one command in INFO commandstats (0, 0 when it never ran).</summary>
    static async Task<(long Calls, long Failed)> StatAsync(RedisNode node, string command)
    {
        var line = (await node.CliAsync("INFO", "commandstats")).Split('\n').FirstOrDefault(l => l.StartsWith("cmdstat_" + command + ":", StringComparison.Ordinal));
        if (line is null)
        {
            return (0, 0);
        }

        var f = line.Trim().Split(':')[1].Split(',').Select(p => p.Split('=')).ToDictionary(p => p[0], p => p[1]);
        long N(string k) => long.Parse(f[k], System.Globalization.CultureInfo.InvariantCulture);
        return (N("calls"), N("failed_calls") + N("rejected_calls"));
    }

    static async Task<bool> HasLibraryAsync(RedisNode node) =>
        (await node.CliAsync("FUNCTION", "LIST", "LIBRARYNAME", Library)).Contains(Library, StringComparison.Ordinal);

    [Fact]
    [Trait("Case", "REDIS-24")]
    public async Task The_library_is_loaded_at_startup_and_the_worker_commands_go_by_FCALL()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "rf01");
        Assert.True(await HasLibraryAsync(node));
        Assert.Equal(Library, Store(c).EngineFacts["functions"]);
        Assert.Matches("^rw_[0-9a-f]{16}$", Library);

        await ClaimAndFinishAsync(c, "a");
        var (calls, failed) = await StatAsync(node, "fcall");
        Assert.True(calls >= 2, $"{calls} FCALLs");
        Assert.Equal(0, failed);
        Assert.Null(Store(c).Runner.Client.FunctionsRefused);
    }

    [Fact]
    [Trait("Case", "REDIS-24")]
    public async Task After_FUNCTION_FLUSH_the_next_call_loads_the_library_again_and_is_answered()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "rf02");
        await ClaimAndFinishAsync(c, "a");
        Assert.Equal("OK", await node.CliAsync("FUNCTION", "FLUSH"));
        Assert.False(await HasLibraryAsync(node));

        await ClaimAndFinishAsync(c, "b");
        Assert.True(await HasLibraryAsync(node));
        Assert.Null(Store(c).Runner.Client.FunctionsRefused);
        // One call answered "Function not found" (the claim after the flush), which was resent once the library was loaded again.
        Assert.Equal(1, (await StatAsync(node, "fcall")).Failed);
    }

    [Fact]
    [Trait("Case", "REDIS-24")]
    public async Task A_user_refused_FUNCTION_and_FCALL_runs_the_same_commands_by_EVALSHA_and_Describe_says_so()
    {
        await using var node = await RedisNode.StartAsync();
        var password = Guid.NewGuid().ToString("N");
        Assert.Equal("OK", await node.CliAsync("ACL", "SETUSER", "rw_nofn", "on", ">" + password, "~*", "&*", "+@all", "-function", "-fcall", "-fcall_ro"));
        var configuration = ConfigurationOptions.Parse(node.Configuration);
        configuration.User = "rw_nofn";
        configuration.Password = password;
        await using var c = await ClientOnAsync(node, "rf03", configuration.ToString(includePassword: true));
        Assert.StartsWith("evalsha: FUNCTION LOAD refused: NOPERM", Store(c).EngineFacts["functions"], StringComparison.Ordinal);
        Assert.True(await HasLibraryAsync(node)); // the migration's own check (default user) loaded it: FCALL itself is what is refused

        await ClaimAndFinishAsync(c, "a");
        await ClaimAndFinishAsync(c, "b");
        Assert.StartsWith("FCALL refused: NOPERM", Store(c).Runner.Client.FunctionsRefused, StringComparison.Ordinal);
        Assert.True((await StatAsync(node, "evalsha")).Calls >= 4);
        var fcall = await StatAsync(node, "fcall");
        Assert.True(fcall.Calls <= 1, "only the first FCALL is tried");
    }

    [Fact]
    [Trait("Case", "REDIS-24")]
    public async Task The_library_survives_a_kill_9_restart_from_the_AOF_and_a_new_replica_receives_it()
    {
        await using var node = await RedisNode.StartAsync(); // appendonly yes, appendfsync always, no RDB snapshots
        await using (var c = await ClientOnAsync(node, "rf04"))
        {
            await ClaimAndFinishAsync(c, "a");
        }

        await node.CrashAndRestartAsync();
        Assert.True(await HasLibraryAsync(node)); // read back from the AOF, before any client loads it again

        await using var replica = await RedisNode.StartAsync([.. RedisNode.Profile, "--replicaof", await node.IpAsync(), "6379"]);
        for (var i = 0; !(await replica.CliAsync("INFO", "replication")).Contains("master_link_status:up", StringComparison.Ordinal); i++)
        {
            Assert.True(i < 300, "the replica did not sync");
            await Task.Delay(100, Ct);
        }

        Assert.True(await HasLibraryAsync(replica));

        await using var again = await ClientOnAsync(node, "rf04b");
        await ClaimAndFinishAsync(again, "b");
        Assert.Equal(0, (await StatAsync(node, "fcall")).Failed);
    }
}
