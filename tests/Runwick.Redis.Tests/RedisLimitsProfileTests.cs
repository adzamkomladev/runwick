using System.Globalization;
using System.Reflection;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>REDIS-04 (intent limits, wrong key type, memory full) and REDIS-05 (profile and topology refusals) of 22 §12.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisLimitsProfileTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static async Task<TestClient> ClientOnAsync(RedisNode node, string name)
    {
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct);
        return await TestClient.CreateAsync(RedisServer.Prefix + node.Configuration, name + "_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r => TestJobs.Register(r));
    }

    static async Task<ConnectionMultiplexer> AdminAsync(RedisNode node) =>
        await ConnectionMultiplexer.ConnectAsync(node.Configuration + ",allowAdmin=true,abortConnect=false,connectTimeout=5000");

    /// <summary>One DispatchMany/1 command over <paramref name="count"/> jobs, built below the public 100-item cap of 02 §4.1.</summary>
    static StoreCommand<DispatchBatchRequest> Many(TestClient c, string tag, int count, int payloadBytes, int chunk = 100)
    {
        var preparer = c.Services.GetRequiredService<DispatchPreparer>();
        var batches = Enumerable.Range(0, (count + chunk - 1) / chunk).Select(b => preparer.PrepareMany(c.Runtime.StoreId,
            [.. Enumerable.Range(0, Math.Min(chunk, count - b * chunk)).Select(i => new BatchDispatchItem(new PingJob($"{tag}{b}-{i}-" + new string('x', payloadBytes))))],
            null)).ToList();
        var items = batches.SelectMany(p => p.Items.Select((intent, i) => new DispatchItem(intent, p.Scopes[i], p.ItemDigests[i]) { Rates = preparer.RatesFor(intent) })).ToList();
        return c.Command(new DispatchBatchRequest(items, false, c.Runtime.StoreId, batches[0].CommandDigest));
    }

    static async Task<(string Guard, string Intent)> Pending(RedisNode node, string app) =>
        (await node.CliAsync("hget", RxClient.Meta(app).ToString(), "guard"), await node.CliAsync("hget", RxClient.Meta(app).ToString(), "intentsha"));

    // ------------------------------------------------------------------ REDIS-04 limits

    [Fact]
    [Trait("Case", "REDIS-04")]
    public async Task Intents_over_256_keys_or_4_MiB_are_refused_before_any_write_with_their_codes()
    {
        var f = await DriverFixture.StartAsync("redis04l_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("seed"), cancellationToken: Ct);
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var meta = RxClient.Meta(f.Application);
        var rev = (string?)await db.HashGetAsync(meta, "rev");
        var before = await f.ReadCommittedSnapshot();

        var tooManyKeys = await c.Driver.DispatchManyAsync(Many(c, "k", 300, 1), Ct);
        Assert.Equal((StoreStatus.InvalidRequest, "intent_too_many_keys"), (tooManyKeys.Status, tooManyKeys.Error!.Code));

        var tooLarge = await c.Driver.DispatchManyAsync(Many(c, "b", 120, 36_000, chunk: 40), Ct);
        Assert.Equal((StoreStatus.InvalidRequest, "intent_too_large"), (tooLarge.Status, tooLarge.Error!.Code));

        // Nothing was written: no guard, the same revision, the same rows.
        Assert.Equal(rev, (string?)await db.HashGetAsync(meta, "rev"));
        Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
        Assert.Equal(before.Model.Jobs.Count, (await f.ReadCommittedSnapshot()).Model.Jobs.Count);

        // Just under the key limit (two keys per job, 22 §3.1) is accepted and projected in bounded batches (100 after-images per call).
        var ok = await c.Driver.DispatchManyAsync(Many(c, "ok", 120, 1), Ct);
        Assert.Equal(StoreStatus.Applied, ok.Status);
        Assert.Equal(121, (await f.ReadCommittedSnapshot()).Model.Jobs.Count);
    }

    [Fact]
    [Trait("Case", "REDIS-04")]
    public async Task The_1025_after_image_refusal_uses_the_same_check_with_the_limit_lowered_in_the_script_text()
    {
        // No operation of the current set reaches 1,025 after-images under 257 keys (one write per key and kind, at most 2 per key),
        // so the check itself is exercised by running the bundled script with only MAX_OPS lowered; the real constant is asserted too.
        const string Constants = "local INLINE, MAX_OPS, MAX_KEYS, MAX_BYTES, MAX_SCORE = 100, 1024, 256, 4194304, 9007199254740991";
        var original = RxScript.Write("dispatch");
        Assert.Contains(Constants, original.Text, StringComparison.Ordinal);
        Assert.Contains("if #ops > MAX_OPS then return U.noeffect('InvalidRequest', 'intent_too_many_mutations') end", original.Text, StringComparison.Ordinal);

        var ctor = typeof(RxScript).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(string), typeof(string)])!;
        var cache = (ConcurrentDictionary<string, RxScript>)typeof(RxScript).GetField("Cache", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        var lowered = (RxScript)ctor.Invoke(["dispatch", original.Text.Replace(Constants, Constants.Replace("100, 1024, 256", "100, 40, 256", StringComparison.Ordinal), StringComparison.Ordinal)]);

        var f = await DriverFixture.StartAsync("redis04m_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var rev = (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "rev");
        cache["w:dispatch"] = lowered;
        try
        {
            var refused = await c.Driver.DispatchManyAsync(Many(c, "m", 100, 1), Ct);
            Assert.Equal((StoreStatus.InvalidRequest, "intent_too_many_mutations"), (refused.Status, refused.Error!.Code));
        }
        finally
        {
            cache["w:dispatch"] = original;
        }

        Assert.Equal(rev, (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "rev"));
        Assert.Equal(StoreStatus.Applied, (await c.Driver.DispatchManyAsync(Many(c, "n", 100, 1), Ct)).Status);
    }

    // ------------------------------------------------------------------ REDIS-04 wrong key type

    [Fact]
    [Trait("Case", "REDIS-04")]
    public async Task A_wrong_key_type_after_the_intent_keeps_the_application_Recovering_until_repaired_then_publishes()
    {
        var f = await DriverFixture.StartAsync("redis04t_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("first"), cancellationToken: Ct);
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var meta = RxClient.Meta(f.Application);
        var index = RxClient.KeyPrefix(f.Application) + "idx:created";
        var saved = await db.SortedSetRangeByRankWithScoresAsync(index);
        Assert.NotEmpty(saved);

        await db.KeyDeleteAsync(index); // a foreign writer replaces a protocol key with a string
        await db.StringSetAsync(index, "not-a-zset");
        var prepared = c.Prepared.Prepare(new PingJob("second"));
        await Assert.ThrowsAnyAsync<Exception>(() => c.Prepared.DispatchAsync(prepared, Ct)); // the projection hit WRONGTYPE after the intent
        Assert.NotEqual("", (string?)await db.HashGetAsync(meta, "guard") ?? "");

        // Reads never return empty work, a stale value or a partial projection; writes cannot proceed; nobody deletes the intent.
        var store = Store(c);
        var raw = await store.Runner.Client.EvalAsync(RxLane.Background, RxScript.Read("describe"), meta, RxClient.ReadFrame(store.Runner.StoreId), null, Ct);
        Assert.Equal("RECOVERING", (string)raw[0]!);
        await Assert.ThrowsAnyAsync<Exception>(() => c.Dispatcher.DispatchAsync(new PingJob("third"), cancellationToken: Ct));
        await Assert.ThrowsAnyAsync<Exception>(() => c.ReserveAsync());
        Assert.NotEqual("", (string?)await db.HashGetAsync(meta, "intentsha") ?? "");

        await db.KeyDeleteAsync(index); // the operator repairs the key (never the intent)
        await db.SortedSetAddAsync(index, [.. saved]);
        var receipt = await c.Prepared.DispatchAsync(prepared, Ct); // the resend publishes the stored decision
        Assert.Equal(DispatchDisposition.Created, receipt.Disposition);
        Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Equal(2, snapshot.Model.Jobs.Count);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal(DispatchDisposition.Created, (await c.Dispatcher.DispatchAsync(new PingJob("third"), cancellationToken: Ct)).Disposition);
    }

    // ------------------------------------------------------------------ REDIS-04 memory full

    [Fact]
    [Trait("Case", "REDIS-04")]
    public async Task Memory_full_refuses_new_commands_before_running_and_an_accepted_intent_stays_Recovering_until_maxmemory_is_raised()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis04mem");
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);

        async Task LimitBelowUsed()
        {
            var info = await node.CliAsync("info", "memory");
            var used = long.Parse(info.Split('\n').Single(l => l.StartsWith("used_memory:", StringComparison.Ordinal))[12..].Trim(), CultureInfo.InvariantCulture);
            await node.CliAsync("config", "set", "maxmemory", (used / 2).ToString(CultureInfo.InvariantCulture));
        }

        // A new command while full: refused before the script runs, nothing written.
        var revBefore = await node.CliAsync("hget", RxClient.Meta(c.Application).ToString(), "rev");
        await LimitBelowUsed();
        await Assert.ThrowsAnyAsync<Exception>(() => c.Dispatcher.DispatchAsync(new PingJob("refused"), cancellationToken: Ct));
        Assert.Equal(revBefore, await node.CliAsync("hget", RxClient.Meta(c.Application).ToString(), "rev"));
        Assert.Equal("", (await Pending(node, c.Application)).Guard);

        // A large intent accepted while there was room, then memory runs out before it is projected.
        await node.CliAsync("config", "set", "maxmemory", "0");
        var batch = c.Prepared.PrepareMany([.. Enumerable.Range(0, 100).Select(i => new BatchDispatchItem(new PingJob("m" + i)))]);
        Store(c).Runner.Client.FailAfter = 1;
        try
        {
            await Assert.ThrowsAnyAsync<Exception>(() => c.Prepared.DispatchAsync(batch, Ct));
        }
        finally
        {
            Store(c).Runner.Client.FailAfter = 0;
        }

        var (guard, sha) = await Pending(node, c.Application);
        Assert.NotEqual("", guard);
        await LimitBelowUsed();
        await Assert.ThrowsAnyAsync<Exception>(() => c.Prepared.DispatchAsync(batch, Ct)); // recovery cannot run: stays Recovering
        var stuck = await Pending(node, c.Application);
        Assert.Equal((guard, sha), stuck); // never deleted, never replaced

        await node.CliAsync("config", "set", "maxmemory", "0"); // the operator adds capacity
        var receipt = await c.Prepared.DispatchAsync(batch, Ct);
        Assert.Equal(100, receipt.Items.Count);
        Assert.Equal("", (await Pending(node, c.Application)).Guard);
        await using var mux = await AdminAsync(node);
        var snapshot = await RedisSnapshotReader.ReadAsync(mux, c.Application);
        Assert.Equal(101, snapshot.Model.Jobs.Count);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
    }

    // ------------------------------------------------------------------ REDIS-05

    static async Task<RxProfileReport> CheckAsync(RedisNode node)
    {
        await using var mux = await AdminAsync(node);
        return await RxProfile.CheckAsync(mux.GetServer(mux.GetEndPoints()[0]), mux.GetDatabase());
    }

    [Fact]
    [Trait("Case", "REDIS-05")]
    public async Task The_selected_profile_passes_and_a_replica_everysec_image_defaults_LRU_and_Cluster_are_each_refused_with_their_reason()
    {
        await using var good = await RedisNode.StartAsync();
        var ok = await CheckAsync(good);
        Assert.True(ok.Verified, ok.Failure);
        Assert.Equal("verified", ok.Facts["profile_status"]);
        await RunwickRedisMigrations.MigrateAsync(good.Configuration, Ct);

        // A replica endpoint.
        await good.CliAsync("replicaof", "127.0.0.1", "1");
        var replica = await CheckAsync(good);
        Assert.Equal(("topology_unsupported", "role=slave"), (replica.FailureCode, replica.Failure));
        await good.CliAsync("replicaof", "no", "one");
        Assert.True((await CheckAsync(good)).Verified);

        await using var everysec = await RedisNode.StartAsync([.. RedisNode.Profile, "--appendfsync", "everysec"]);
        var r1 = await CheckAsync(everysec);
        Assert.Equal(("store_profile_mismatch", "appendfsync=everysec"), (r1.FailureCode, r1.Failure));

        await using var defaults = await RedisNode.StartAsync([]);
        var r2 = await CheckAsync(defaults);
        Assert.Equal("store_profile_mismatch", r2.FailureCode);
        Assert.Contains("appendonly=no", r2.Failure + string.Join(',', r2.Facts.Select(p => p.Key + "=" + p.Value)), StringComparison.Ordinal);

        await using var lru = await RedisNode.StartAsync([.. RedisNode.Profile, "--maxmemory-policy", "allkeys-lru"]);
        var r3 = await CheckAsync(lru);
        Assert.Equal(("store_profile_mismatch", "maxmemory-policy=allkeys-lru"), (r3.FailureCode, r3.Failure));

        await using var cluster = await RedisNode.StartAsync([.. RedisNode.Profile, "--cluster-enabled", "yes"]);
        var r4 = await CheckAsync(cluster);
        Assert.Equal("topology_unsupported", r4.FailureCode);
        Assert.Contains("redis_mode=cluster", r4.Failure + r4.Facts["profile_status"], StringComparison.Ordinal);

        // The migrator (the first write path) refuses each of them with the same code before writing anything.
        foreach (var (node, code) in new[] { (everysec, "store_profile_mismatch"), (defaults, "store_profile_mismatch"), (lru, "store_profile_mismatch"), (cluster, "topology_unsupported") })
        {
            var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct));
            Assert.Equal(code, refused.Code);
            Assert.Equal("0", (await node.CliAsync("dbsize")).Replace("(integer) ", "", StringComparison.Ordinal));
        }

        // A Sentinel service name (and several endpoints) is refused when the client is configured, before any command.
        Assert.Equal("topology_unsupported", Assert.Throws<RxProfileException>(() => RxClient.Options("10.0.0.1:26379,serviceName=mymaster", TimeSpan.FromSeconds(1))).Code);
    }

    [Fact]
    [Trait("Case", "REDIS-05")]
    public async Task An_ACL_that_denies_CONFIG_or_MODULE_makes_the_profile_unverifiable_and_unavailable()
    {
        await using var node = await RedisNode.StartAsync();
        Assert.True((await CheckAsync(node)).Verified);

        await node.CliAsync("acl", "setuser", "default", "-config");
        var noConfig = await CheckAsync(node);
        Assert.False(noConfig.Verified);
        Assert.Equal("store_profile_mismatch", noConfig.FailureCode);
        Assert.Equal(5, noConfig.Facts.Count(p => p.Key.StartsWith("config.", StringComparison.Ordinal) && p.Value == "unverifiable"));
        Assert.StartsWith("unavailable", noConfig.Facts["profile_status"], StringComparison.Ordinal);
        var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct));
        Assert.Equal("store_profile_mismatch", refused.Code);
        await node.CliAsync("acl", "setuser", "default", "+config");
        Assert.True((await CheckAsync(node)).Verified);

        // REDIS-10's form: an ACL that denies MODULE LIST makes the profile unavailable.
        await node.CliAsync("acl", "setuser", "default", "-module");
        var noModules = await CheckAsync(node);
        Assert.False(noModules.Verified);
        Assert.Equal("unverifiable", noModules.Facts["modules"]);
        await node.CliAsync("acl", "setuser", "default", "+module");
        Assert.True((await CheckAsync(node)).Verified);
    }

    [Fact]
    [Trait("Case", "REDIS-05")]
    public async Task A_runtime_switch_to_everysec_stops_admission_within_one_recheck_interval_without_any_explicit_check()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis05timer");
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);

        await node.CliAsync("config", "set", "appendfsync", "everysec");
        var switched = DateTime.UtcNow;
        StoreNotAppliedException? refused = null;
        while (refused is null && DateTime.UtcNow - switched < TimeSpan.FromSeconds(45))
        {
            try
            {
                await c.ReserveAsync();
                await Task.Delay(500, Ct);
            }
            catch (StoreNotAppliedException ex)
            {
                refused = ex;
            }
        }

        Assert.NotNull(refused);
        Assert.Equal("store_profile_mismatch", refused!.Code);
        // The default recheck interval is 30 s (22 §10); allow one interval plus the call that discovers it.
        Assert.True(DateTime.UtcNow - switched <= TimeSpan.FromSeconds(35), $"admission closed after {DateTime.UtcNow - switched}");

        await node.CliAsync("config", "set", "appendfsync", "always");
        var reopened = DateTime.UtcNow;
        while (true)
        {
            try
            {
                Assert.True((await c.ReserveAsync()).Status is StoreStatus.Applied or StoreStatus.NoWork);
                break;
            }
            catch (StoreNotAppliedException) when (DateTime.UtcNow - reopened < TimeSpan.FromSeconds(45))
            {
                await Task.Delay(500, Ct);
            }
        }
    }
}
