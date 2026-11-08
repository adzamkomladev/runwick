using System.Globalization;
using Runwick.Redis.Prototype;
using StackExchange.Redis;

namespace Runwick.Redis.Prototype.Tests;

/// <summary>REDIS-01: every write prefix of every operation, across crash/restart; AOF rewrite and truncation.</summary>
public sealed class Redis01WritePrefixTests(ITestOutputHelper output)
{
    private const long T0 = 1_000_000_000_000;

    /// <summary>A deterministic command sequence touching every mutation shape (clock pinned so states compare exactly).</summary>
    internal static (StoreCommand Cmd, long Clock)[] Sequence()
    {
        var steps = new (string Op, string[] Args)[]
        {
            ("dispatch", ["j1", "default", "payload-1", "3", ""]),
            ("dispatch", ["j2", "default", "payload-2", "3", ""]),
            ("reserve", ["w1", "t1", "a1", "60000"]),
            ("begin", ["j1", "a1", "w1", "t1", "1"]),
            ("renew", ["j1", "a1", "w1", "t1", "1", "60000"]),
            ("finish", ["j1", "a1", "w1", "t1", "1", "succeeded"]),
            ("reserve", ["w2", "t2", "a2", "1000"]),
            ("recover", []),
            ("scheduleFire", ["nightly", "1", "s1", "payload-s"]),
            ("outboxAppend", ["m1", "hello"]),
        };
        return steps.Select((s, i) =>
        {
            var clock = T0 + (i * 1000);
            return (new StoreCommand(s.Op, Id(i), clock + 30_000, s.Args), clock);
        }).ToArray();
    }

    internal static Guid Id(int i) => Guid.Parse($"00000000-0000-0000-0000-{i:D12}");

    [Fact]
    public async Task Every_write_prefix_of_every_operation_is_hidden_survives_a_crash_and_recovers_to_the_clean_result()
    {
        await using var node = await RedisNode.StartAsync();
        var seq = Sequence();

        // Reference: the clean run, snapshotted after every step.
        var reference = node.Store("ref");
        await reference.InitAsync();
        var refSnaps = new List<SortedDictionary<string, string>> { await node.SnapshotAsync("ref") };
        var refReplies = new List<StoreReply>();
        foreach (var (cmd, clock) in seq)
        {
            reference.Clock = clock;
            var r = await reference.ExecuteAsync(cmd);
            Assert.Equal("APPLIED", r.Kind);
            refReplies.Add(r);
            refSnaps.Add(await node.SnapshotAsync("ref"));
        }

        // Inject: for step i and prefix k, a fresh application replays steps < i, then step i fails after k writes.
        var cases = new List<(int Step, int K, string App, SortedDictionary<string, string> Raw)>();
        for (var i = 0; i < seq.Length; i++)
        {
            for (var k = 0; ; k++)
            {
                var app = $"p{i}-{k}";
                var store = node.Store(app);
                await store.InitAsync();
                for (var j = 0; j < i; j++)
                {
                    store.Clock = seq[j].Clock;
                    await store.ExecuteAsync(seq[j].Cmd);
                }

                store.Clock = seq[i].Clock;
                try
                {
                    await store.ExecuteAsync(seq[i].Cmd, failAfter: k);
                }
                catch (StoreOutcomeUnknownException)
                {
                    var raw = await node.SnapshotAsync(app);
                    await AssertHiddenAsync(store, k, raw, refSnaps[i], refSnaps[i + 1]);
                    cases.Add((i, k, app, raw));
                    continue;
                }

                // k exceeded the operation's write count: this app holds a clean run; drop it.
                await node.Db.ExecuteAsync("EVAL", "for _,k in ipairs(redis.call('KEYS', ARGV[1])) do redis.call('DEL', k) end", 0, $"rw:{{{app}}}:*");
                output.WriteLine($"step {i} {seq[i].Cmd.Op}: {k} write boundaries (accept + projection + publish), {k + 1} prefixes incl. reply loss");
                break;
            }
        }

        // Crash with every interrupted prefix on disk, restart from the AOF.
        await node.CrashAndRestartAsync();

        var partialRaw = 0;
        foreach (var (step, k, app, raw) in cases)
        {
            var store = node.Store(app);
            Assert.Equal(raw, await node.SnapshotAsync(app)); // every write that was made survived the crash, nothing more
            await AssertHiddenAsync(store, k, raw, refSnaps[step], refSnaps[step + 1]);
            if (k > 0 && !Data(raw).SequenceEqual(Data(refSnaps[step])) && !Data(raw).SequenceEqual(Data(refSnaps[step + 1]))) partialRaw++;

            // The caller resolves the unknown outcome by resubmitting the same command.
            store.Clock = seq[step].Clock;
            var r = await store.ExecuteAsync(seq[step].Cmd);
            Assert.Equal(k == 0 || seq[step].Cmd.Op == "renew" ? "APPLIED" : "REPLAYED", r.Kind); // Renew has no receipt (R1): it extends again
            Assert.Equal(refReplies[step].Result, r.Result);
            Assert.Equal(refReplies[step].DecidedAt, r.DecidedAt);
            if (seq[step].Cmd.Op == "renew" && k > 0) Assert.Equal(Data(refSnaps[step + 1]), Data(await node.SnapshotAsync(app))); // second Renew = one more publication
            else Assert.Equal(refSnaps[step + 1], await node.SnapshotAsync(app));
        }

        output.WriteLine($"{cases.Count} interrupted prefixes injected, crashed, restarted and recovered; {partialRaw} had raw partial state that reads hid");
        Assert.True(cases.Count > 60);
    }

    /// <summary>Authoritative reads show the pre-state (nothing written) or Recovering (guard written), never a partial state.</summary>
    internal static async Task AssertHiddenAsync(RedisPrototypeStore store, int k, SortedDictionary<string, string> raw, SortedDictionary<string, string> before, SortedDictionary<string, string> after)
    {
        var summary = await store.ReadSummaryAsync();
        var job = await store.ReadJobAsync("j1");
        if (k == 0)
        {
            Assert.Equal(before, raw);
            Assert.False(summary.Recovering);
            Assert.False(job.Recovering);
        }
        else if (summary.Recovering)
        {
            Assert.True(job.Recovering);
        }
        else
        {
            // Only the last prefix (every write made, reply lost) may be visible, and then completely.
            Assert.Equal(after, raw);
            Assert.False(job.Recovering);
        }
    }

    [Fact]
    public async Task Large_intent_projected_in_batches_is_hidden_at_every_batch_boundary_and_recovers()
    {
        await using var node = await RedisNode.StartAsync();
        string[] Items(int n) => Enumerable.Range(0, n).SelectMany(i => new[] { $"b{i}", $"payload-{i}" }).ToArray();
        var cmd = new StoreCommand("dispatchMany", Id(100), T0 + 30_000, ["default", "3", .. Items(120)]);

        var reference = node.Store("bref");
        await reference.InitAsync();
        reference.Clock = T0;
        var refReply = await reference.ExecuteAsync(cmd);
        var refSnap = await node.SnapshotAsync("bref");
        Assert.Equal(120, (await reference.ReadSummaryAsync()).Counter("pending"));

        // (mutate failAfter, first-batch failAfter): nothing; accepted only; accepted + reply lost;
        // mid first batch; end of first batch before reply; inside a 2-write after-image.
        (int M, int R)[] points = [(0, -1), (1, -1), (-1, 0), (-1, 1), (-1, 75), (-1, 149), (-1, 150)];
        var apps = new List<(string App, int M)>();
        foreach (var (m, rf) in points)
        {
            var app = $"b{m}_{rf}";
            var store = node.Store(app);
            await store.InitAsync();
            store.Clock = T0;
            await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(cmd, m, rf));
            Assert.Equal(m != 0, (await store.ReadSummaryAsync()).Recovering);
            apps.Add((app, m));
        }

        await node.CrashAndRestartAsync();
        foreach (var (app, m) in apps)
        {
            var store = node.Store(app);
            Assert.Equal(m != 0, (await store.ReadSummaryAsync()).Recovering);
            store.Clock = T0;
            var r = await store.ExecuteAsync(cmd);
            Assert.Equal(refReply.Result, r.Result);
            Assert.Equal(refSnap, await node.SnapshotAsync(app));
        }
    }

    [Fact]
    public async Task Acknowledged_commands_survive_kill_during_load_and_AOF_rewrites()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("load");
        await store.InitAsync();
        var acked = new System.Collections.Concurrent.ConcurrentBag<(StoreCommand Cmd, StoreReply Reply)>();
        var unknown = new System.Collections.Concurrent.ConcurrentBag<StoreCommand>();
        using var stop = new CancellationTokenSource();

        async Task Producer(int w)
        {
            for (var i = 0; !stop.IsCancellationRequested; i++)
            {
                var c = StoreCommand.Create("dispatch", TimeSpan.FromSeconds(60), $"w{w}-{i}", "default", new string('x', 512), "3", "");
                try
                {
                    acked.Add((c, await store.ExecuteAsync(c)));
                }
                catch (StoreOutcomeUnknownException)
                {
                    unknown.Add(c);
                    return;
                }
                catch (StoreNotAppliedException)
                {
                    return;
                }
            }
        }

        var producers = Enumerable.Range(0, 4).Select(Producer).ToArray();
        for (var r = 0; r < 3; r++)
        {
            await Task.Delay(400);
            try
            {
                await node.Db.ExecuteAsync("BGREWRITEAOF");
            }
            catch (RedisServerException)
            {
                // a rewrite is already running
            }
        }

        await Task.Delay(300);
        var rewrites = (await ProfileValidator.InfoAsync(node.Db))["aof_rewrites"];
        await node.KillAsync(); // mid-load, possibly mid-rewrite
        await Task.WhenAll(producers);
        stop.Cancel();
        await node.StartAgainAsync();

        foreach (var (cmd, reply) in acked)
        {
            Assert.Equal("Recorded", await store.ResolveAsync(cmd, 86_400_000));
            Assert.Equal(reply.Result, (await store.ExecuteAsync(cmd)).Result);
        }

        foreach (var cmd in unknown)
        {
            var resolution = await store.ResolveAsync(cmd, 86_400_000);
            Assert.Contains(resolution, new[] { "Recorded", "NotObserved", "RECOVERING" });
            await store.ExecuteAsync(cmd); // resolves to exactly one outcome
        }

        var s = await store.ReadSummaryAsync();
        Assert.False(s.Recovering);
        Assert.Equal(s.Counter("created"), s.Pending);
        Assert.Equal(acked.Count + unknown.Count, s.Counter("created"));
        output.WriteLine($"{acked.Count} acknowledged dispatches all survived kill -9; {unknown.Count} in flight resolved; AOF rewrites completed before the kill: {rewrites}");
        Assert.NotEqual("0", rewrites);
    }

    [Fact]
    public async Task Truncated_AOF_makes_the_profile_unavailable_and_check_aof_fix_never_exposes_a_partial_script()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("trunc");
        await store.InitAsync();
        for (var i = 0; i < 20; i++)
        {
            await store.ExecuteAsync(StoreCommand.Create("dispatch", TimeSpan.FromSeconds(60), $"j{i}", "default", "p", "3", ""));
        }

        var before = await store.ReadSummaryAsync();
        await node.KillAsync();

        // Tear the tail of the last write (inside its MULTI/EXEC block), as a torn final write would.
        await node.WithDataAsync("f=$(ls /data/appendonlydir/*.incr.aof | tail -1); s=$(stat -c %s $f); truncate -s $((s-7)) $f");
        await RedisNode.Docker("start", node.Id);
        await Task.Delay(2000);
        Assert.False(await node.IsRunningAsync()); // aof-load-truncated no: refuses to start
        var logs = await node.LogsAsync();
        output.WriteLine(logs.Split('\n').Last(l => l.Contains("AOF", StringComparison.Ordinal)));

        // Operator procedure: redis-check-aof --fix drops the incomplete transaction, then start.
        output.WriteLine(await node.WithDataAsync("echo y | redis-check-aof --fix /data/appendonlydir/appendonly.aof.manifest 2>&1 | tail -3"));
        await node.StartAgainAsync();

        var after = await store.ReadSummaryAsync();
        Assert.False(after.Recovering);
        Assert.Equal(after.Counter("created"), after.Pending);
        var lost = BigIntegerDiff(before.Revision, after.Revision);
        Assert.Equal(1, lost); // exactly the torn last command, as a whole
        Assert.Equal(before.Counter("created") - 1, after.Counter("created"));
    }

    private static IEnumerable<KeyValuePair<string, string>> Data(SortedDictionary<string, string> snap) =>
        snap.Where(e => e.Key != "meta");

    private static long BigIntegerDiff(string a, string b) =>
        long.Parse(a, CultureInfo.InvariantCulture) - long.Parse(b, CultureInfo.InvariantCulture);
}
