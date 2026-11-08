using System.Collections.Concurrent;
using Runwick.Redis.Prototype;
using StackExchange.Redis;

namespace Runwick.Redis.Prototype.Tests;

/// <summary>REDIS-06: schedule, outbox and audit writes use the same publication protocol as execution, concurrently and under failure.</summary>
public sealed class Redis06ParityTests(ITestOutputHelper output)
{
    [Fact]
    public async Task REDIS06_concurrent_schedule_outbox_audit_and_execution_writes_under_injected_failures_publish_only_whole_revisions()
    {
        await using var node = await RedisNode.StartAsync();
        await node.Store("parity").InitAsync();
        var applied = new ConcurrentDictionary<string, int>();
        var injected = 0;
        var nextSlot = 0;
        using var stop = new CancellationTokenSource();

        async Task<StoreReply> Send(RedisPrototypeStore store, StoreCommand c, Random rng)
        {
            var fail = rng.Next(5) == 0 ? rng.Next(0, 12) : -1;
            while (true)
            {
                try
                {
                    var r = await store.ExecuteAsync(c, failAfter: fail);
                    if (r.Status == "Applied") applied.AddOrUpdate(c.Op, 1, (_, n) => n + 1);
                    return r;
                }
                catch (StoreOutcomeUnknownException)
                {
                    Interlocked.Increment(ref injected);
                    fail = -1; // resolve by resubmitting the same command
                }
            }
        }

        async Task Worker(int w)
        {
            // Each worker has its own connection, like separate processes.
            using var mux = await ConnectionMultiplexer.ConnectAsync(RedisNode.Options(node.Port));
            var store = new RedisPrototypeStore(mux.GetDatabase(), "store-1", "parity");
            var rng = new Random(w);
            for (var i = 0; i < 150; i++)
            {
                switch (rng.Next(4))
                {
                    case 0:
                        // Two workers often race for the same slot: exactly one occurrence may exist.
                        var slot = rng.Next(2) == 0 ? Volatile.Read(ref nextSlot) : Interlocked.Increment(ref nextSlot);
                        await Send(store, StoreCommand.Create("scheduleFire", TimeSpan.FromSeconds(60), "nightly", $"{slot}", $"s{w}-{i}", "p"), rng);
                        break;
                    case 1:
                        await Send(store, StoreCommand.Create("outboxAppend", TimeSpan.FromSeconds(60), $"m{w}-{i}", "body"), rng);
                        break;
                    case 2:
                        await Send(store, StoreCommand.Create("dispatch", TimeSpan.FromSeconds(60), $"d{w}-{i}", "default", "p", "3", ""), rng);
                        break;
                    default:
                        var res = await Send(store, StoreCommand.Create("reserve", TimeSpan.FromSeconds(30), $"w{w}", $"t{w}-{i}", $"a{w}-{i}", "30000"), rng);
                        if (res.Status != "Applied") break;
                        string[] h = [res.Json.GetProperty("jobId").GetString()!, $"a{w}-{i}", $"w{w}", $"t{w}-{i}", "1"];
                        await Send(store, StoreCommand.Create("begin", TimeSpan.FromSeconds(30), h), rng);
                        await Send(store, StoreCommand.Create("finish", TimeSpan.FromSeconds(30), [.. h, rng.Next(3) == 0 ? "failed" : "succeeded"]), rng);
                        break;
                }
            }
        }

        var reads = 0;
        var recoveringReads = 0;
        var reader = Task.Run(async () =>
        {
            var store = node.Store("parity");
            while (!stop.IsCancellationRequested)
            {
                var s = await store.ReadSummaryAsync();
                reads++;
                if (s.Recovering)
                {
                    recoveringReads++;
                    continue;
                }

                AssertWhole(s);
            }
        });

        await Task.WhenAll(Enumerable.Range(0, 8).Select(Worker));
        stop.Cancel();
        await reader;

        var final = node.Store("parity");
        await final.RecoverAsync();
        var f = await final.ReadSummaryAsync();
        AssertWhole(f);

        // Schedule/outbox/audit parity with what callers saw applied.
        var fires = applied.GetValueOrDefault("scheduleFire");
        var appends = applied.GetValueOrDefault("outboxAppend");
        var sched = await node.Db.HashGetAllAsync($"{final.Prefix}sched:nightly");
        Assert.Equal(fires.ToString(System.Globalization.CultureInfo.InvariantCulture), sched.Single(e => e.Name == "fired").Value);
        Assert.Equal(fires + appends, f.Audit);
        Assert.Equal(fires + appends, f.Outbox);
        Assert.Equal(fires + applied.GetValueOrDefault("dispatch"), f.Counter("created"));
        Assert.Equal(applied.GetValueOrDefault("finish"), f.Counter("succeeded") + f.Counter("failed"));

        output.WriteLine($"applied: {string.Join(", ", applied.OrderBy(e => e.Key).Select(e => $"{e.Key}={e.Value}"))}; injected failures resolved: {injected}; reads: {reads} ({recoveringReads} Recovering, the rest whole revisions)");
        Assert.True(injected > 50);
    }

    /// <summary>Every count in one read belongs to the same fully published revision.</summary>
    private static void AssertWhole(Summary s)
    {
        Assert.Equal(s.Counter("pending") + s.Counter("retrying"), s.Pending);
        Assert.Equal(s.Counter("reserved") + s.Counter("running"), s.Leases);
        Assert.Equal(s.Counter("outbox"), s.Outbox);
        Assert.Equal(s.Counter("audit"), s.Audit);
        Assert.Equal(
            s.Counter("created"),
            s.Counter("pending") + s.Counter("retrying") + s.Counter("reserved") + s.Counter("running") + s.Counter("succeeded") + s.Counter("failed"));
    }
}
