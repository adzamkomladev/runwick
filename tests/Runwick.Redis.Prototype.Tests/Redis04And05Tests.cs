using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using Runwick.Redis.Prototype;
using StackExchange.Redis;

namespace Runwick.Redis.Prototype.Tests;

/// <summary>REDIS-04 bounded scripts, memory, key types, counters, control pressure; REDIS-05 topology/configuration rejection.</summary>
public sealed class Redis04And05Tests(ITestOutputHelper output)
{
    private static StoreCommand Dispatch(string id, string payload = "p") =>
        StoreCommand.Create("dispatch", TimeSpan.FromSeconds(60), id, "default", payload, "3", "");

    private static StoreCommand Many(string tag, int n, int payloadBytes = 8) => StoreCommand.Create(
        "dispatchMany", TimeSpan.FromSeconds(60),
        ["default", "3", .. Enumerable.Range(0, n).SelectMany(i => new[] { $"{tag}{i}", new string('x', payloadBytes) })]);

    [Fact]
    public async Task REDIS04_oversized_commands_fail_before_any_intent_is_written()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("limits");
        await store.InitAsync();

        var keys = await store.ExecuteAsync(Many("k", 300)); // 300 jobs + indexes > 256 keys
        Assert.Equal(("InvalidRequest", "intent_too_many_keys"), (keys.Status, keys.Code));
        var bytes = await store.ExecuteAsync(Many("b", 5, 1024 * 1024)); // > 4 MiB serialized intent
        Assert.Equal(("InvalidRequest", "intent_too_large"), (bytes.Status, bytes.Code));
        var ok = await store.ExecuteAsync(Many("ok", 250)); // 254 keys, 503 mutations: accepted, projected in batches
        Assert.Equal("APPLIED", ok.Kind);

        var s = await store.ReadSummaryAsync();
        Assert.Equal((250, 250L), (s.Counter("created"), s.Pending));
        Assert.Equal("1", s.Revision); // the rejected commands published nothing
    }

    [Fact]
    public async Task REDIS04_wrong_key_type_keeps_the_application_recovering_until_repaired_and_a_plain_script_would_leak_partial_state()
    {
        await using var node = await RedisNode.StartAsync();

        // Contrast: one multi-write script without an intent leaves its first write visible after an error.
        await node.Db.StringSetAsync("naive:pending", "not-a-zset");
        await Assert.ThrowsAsync<RedisServerException>(() => node.Db.ScriptEvaluateAsync(
            "redis.call('HSET', KEYS[1], 'state', 'pending'); redis.call('ZADD', KEYS[2], 1, 'j1')", ["naive:job:j1", "naive:pending"]));
        Assert.True(await node.Db.KeyExistsAsync("naive:job:j1")); // half a dispatch, visible

        var store = node.Store("wrongtype");
        await store.InitAsync();
        await node.Db.StringSetAsync($"{store.Prefix}pending", "not-a-zset"); // foreign writer corrupted a protocol key
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(Dispatch("j1")));
        Assert.True((await store.ReadSummaryAsync()).Recovering);
        Assert.True((await store.ReadJobAsync("j1")).Recovering); // the job hash exists raw, but is not readable
        await Assert.ThrowsAsync<StoreRecoveringException>(() => store.ExecuteAsync(Dispatch("j2")));

        await node.Db.KeyDeleteAsync($"{store.Prefix}pending"); // operator repairs the key (never the intent)
        Assert.NotNull(await store.RecoverAsync());
        var s = await store.ReadSummaryAsync();
        Assert.Equal((1, 1L), (s.Counter("created"), s.Pending));
        Assert.Equal("APPLIED", (await store.ExecuteAsync(Dispatch("j2"))).Kind);
    }

    [Fact]
    public async Task REDIS04_memory_exhaustion_refuses_new_commands_and_blocks_recovery_until_capacity_is_added()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("oom");
        await store.InitAsync();
        await store.ExecuteAsync(Dispatch("j0"));

        async Task SetLimitBelowUsedAsync()
        {
            var used = long.Parse((await ProfileValidator.InfoAsync(node.Db))["used_memory"], CultureInfo.InvariantCulture);
            await node.Db.ExecuteAsync("CONFIG", "SET", "maxmemory", (used / 2).ToString(CultureInfo.InvariantCulture));
        }

        // New command while full: refused before the script runs, nothing written.
        await SetLimitBelowUsedAsync();
        var refused = await Assert.ThrowsAsync<StoreNotAppliedException>(() => store.ExecuteAsync(Dispatch("j1")));
        Assert.Equal("oom", refused.Code);
        Assert.False((await store.ReadSummaryAsync()).Recovering);

        // A large intent accepted while there was room, then memory runs out before it is projected.
        await node.Db.ExecuteAsync("CONFIG", "SET", "maxmemory", "0");
        var big = Many("m", 120);
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(big, recoverFailAfter: 0));
        await SetLimitBelowUsedAsync();
        await Assert.ThrowsAsync<StoreRecoveringException>(() => store.RecoverAsync());
        Assert.True((await store.ReadSummaryAsync()).Recovering);

        await node.Db.ExecuteAsync("CONFIG", "SET", "maxmemory", "0"); // operator adds capacity
        var replay = await store.ExecuteAsync(big);
        Assert.True(replay.Replayed);
        var s = await store.ReadSummaryAsync();
        Assert.Equal((121, 121L), (s.Counter("created"), s.Pending));
    }

    [Fact]
    public async Task REDIS04_counters_are_exact_beyond_2_pow_53_and_refuse_to_pass_int64()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("counters");
        await store.InitAsync();
        await node.Db.HashSetAsync($"{store.Prefix}counters", [new("created", "9223372036854775806"), new("pending", "9007199254740993")]);

        Assert.Equal("APPLIED", (await store.ExecuteAsync(Dispatch("j1"))).Kind);
        var s = await store.ReadSummaryAsync();
        Assert.Equal("9223372036854775807", s.Counters["created"]);
        Assert.Equal("9007199254740994", s.Counters["pending"]); // a double would round this

        var over = await store.ExecuteAsync(Dispatch("j2"));
        Assert.Equal(("InvariantFault", "counter_limit"), (over.Status, over.Code));
        Assert.Equal(s.Revision, (await store.ReadSummaryAsync()).Revision);
    }

    [Fact]
    public async Task REDIS04_script_latency_and_renewal_under_large_projections()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("latency");
        await store.InitAsync();
        var lat = new Dictionary<string, List<double>>();

        async Task<StoreReply> Timed(string name, StoreCommand c)
        {
            var sw = Stopwatch.StartNew();
            var r = await store.ExecuteAsync(c);
            (lat.TryGetValue(name, out var l) ? l : lat[name] = []).Add(sw.Elapsed.TotalMilliseconds);
            return r;
        }

        for (var i = 0; i < 500; i++)
        {
            await Timed("dispatch", Dispatch($"j{i}", new string('x', 256)));
            var res = await Timed("reserve", StoreCommand.Create("reserve", TimeSpan.FromSeconds(30), "w1", $"t{i}", $"a{i}", "30000"));
            string[] h = [res.Json.GetProperty("jobId").GetString()!, $"a{i}", "w1", $"t{i}", "1"];
            await Timed("begin", StoreCommand.Create("begin", TimeSpan.FromSeconds(30), h));
            await Timed("renew", StoreCommand.Create("renew", TimeSpan.FromSeconds(30), [.. h, "30000"]));
            await Timed("finish", StoreCommand.Create("finish", TimeSpan.FromSeconds(30), [.. h, "succeeded"]));
        }

        // Control pressure: renewals on a second connection while 250-job intents (503 mutations) are projected.
        await store.ExecuteAsync(Dispatch("held"));
        var held = await store.ExecuteAsync(StoreCommand.Create("reserve", TimeSpan.FromSeconds(30), "w9", "t9", "a9", "60000"));
        using var control = await ConnectionMultiplexer.ConnectAsync(RedisNode.Options(node.Port));
        var controlStore = new RedisPrototypeStore(control.GetDatabase(), "store-1", "latency");
        using var stop = new CancellationTokenSource();
        var renewLat = new List<double>();
        var renewer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                var r = await controlStore.ExecuteAsync(StoreCommand.Create("renew", TimeSpan.FromSeconds(30), held.Json.GetProperty("jobId").GetString()!, "a9", "w9", "t9", "1", "60000"));
                Assert.Equal("APPLIED", r.Kind);
                renewLat.Add(sw.Elapsed.TotalMilliseconds);
            }
        });
        for (var b = 0; b < 20; b++) await Timed("dispatchMany250", Many($"m{b}-", 250, 256));
        stop.Cancel();
        await renewer;
        lat["renew-under-batches"] = renewLat;

        var slow = (RedisResult[])(await node.Db.ExecuteAsync("SLOWLOG", "GET", "128"))!;
        var stats = await ProfileValidator.InfoAsync(node.Db);
        foreach (var (name, l) in lat)
        {
            l.Sort();
            output.WriteLine($"{name,-22} n={l.Count,4} p50={l[l.Count / 2],7:F2} ms p99={l[(int)(l.Count * 0.99)],7:F2} ms max={l[^1],7:F2} ms");
        }

        output.WriteLine($"server: cmdstat_evalsha {stats.GetValueOrDefault("cmdstat_evalsha")}; cmdstat_eval {stats.GetValueOrDefault("cmdstat_eval")}");
        output.WriteLine($"SLOWLOG (>10 ms server-side): {slow.Length} entries, slowest {(slow.Length > 0 ? slow.Max(e => (long)((RedisResult[])e!)[2]) : 0)} µs");
        Assert.True(renewLat.Count > 0 && renewLat.Max() < 1000);
    }

    [Fact]
    public async Task REDIS05_profile_validation_accepts_only_the_selected_profile()
    {
        await using var good = await RedisNode.StartAsync();
        Assert.Empty(await ProfileValidator.ValidateAsync(good.Db));

        // Replica role: refused.
        await good.Db.ExecuteAsync("REPLICAOF", "127.0.0.1", "1");
        Assert.Contains("role=slave", await ProfileValidator.ValidateAsync(good.Db));
        await good.Db.ExecuteAsync("REPLICAOF", "NO", "ONE");

        await using var everysec = await RedisNode.StartAsync([.. RedisNode.Profile, "--appendfsync", "everysec"]);
        Assert.Equal(["appendfsync=everysec"], await ProfileValidator.ValidateAsync(everysec.Db));

        await using var defaults = await RedisNode.StartAsync([]);
        var v = await ProfileValidator.ValidateAsync(defaults.Db);
        output.WriteLine("default image config: " + string.Join(", ", v));
        Assert.Contains("appendonly=no", v);
        Assert.Contains("aof_enabled=0", v);

        await using var eviction = await RedisNode.StartAsync([.. RedisNode.Profile, "--maxmemory-policy", "allkeys-lru"]);
        Assert.Equal(["maxmemory-policy=allkeys-lru"], await ProfileValidator.ValidateAsync(eviction.Db));

        // CONFIG blocked (as managed services often do): the profile is unverifiable, so unavailable.
        // (Renaming CONFIG away with --rename-command makes the redis:8.2 image segfault at startup.)
        await good.Db.ExecuteAsync("ACL", "SETUSER", "default", "-config");
        var hidden = await ProfileValidator.ValidateAsync(good.Db);
        Assert.Equal(5, hidden.Count);
        Assert.All(hidden, x => Assert.EndsWith(":unverifiable", x, StringComparison.Ordinal));
        await good.Db.ExecuteAsync("ACL", "SETUSER", "default", "+config");
        Assert.Empty(await ProfileValidator.ValidateAsync(good.Db));
    }

    [Fact]
    public async Task REDIS05_restart_needs_intact_history_not_just_PING_and_store_identity_is_checked()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("history");
        await store.InitAsync();
        for (var i = 0; i < 5; i++) await store.ExecuteAsync(Dispatch($"j{i}"));
        var seen = BigInteger.Parse((await store.ReadSummaryAsync()).Revision, CultureInfo.InvariantCulture);
        var runId = (await ProfileValidator.InfoAsync(node.Db))["run_id"];

        await node.CrashAndRestartAsync();
        Assert.NotEqual(runId, (await ProfileValidator.InfoAsync(node.Db))["run_id"]); // endpoint history changed: re-verify
        Assert.True(await store.HistoryIntactAsync(seen));

        await node.Db.ExecuteAsync("FLUSHALL"); // same endpoint, empty history (e.g. a replacement server)
        await node.Db.PingAsync();
        Assert.False(await store.HistoryIntactAsync(seen));

        await store.InitAsync(); // even a re-registered namespace has lost publications
        Assert.False(await store.HistoryIntactAsync(seen));

        var other = node.Store("history", storeId: "store-2");
        var r = await other.ExecuteAsync(Dispatch("x"));
        Assert.Equal(("UnsupportedVersion", "store_identity_mismatch"), (r.Status, r.Code));
    }
}
