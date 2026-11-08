using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>These cases change server settings and spare databases of the test server, so they run one at a time.</summary>
[CollectionDefinition(nameof(RedisServerState), DisableParallelization = true)]
public sealed class RedisServerState;

/// <summary>
/// Redis-only cases of 22 §11 that cover the parts built so far (the core protocol, recovery, profile, migration, history).
/// Uses RUNWICK_TEST_REDIS or the local AOF-always container; databases 9–11 are scratch databases these tests flush.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisStoreTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        return (f, c);
    }

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static string Db(int n) => RedisServer.Configuration + ",defaultDatabase=" + n;

    static async Task FlushAsync(int db)
    {
        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(db);
    }

    static async Task NoViolations(DriverFixture f) => Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    // ------------------------------------------------------------------ REDIS-01 / REDIS-02: every write prefix, lost reply

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Every_write_prefix_of_Finish_recovers_and_the_resend_returns_the_original_result()
    {
        var (f, c) = await OpenAsync("redis01");
        await using var _ = f;
        var store = Store(c);
        var sawUnknown = 0;
        for (var n = 1; n <= 40; n++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + n), cancellationToken: Ct);
            var reserved = await c.ReserveAsync();
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            var lease = reserved.Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);

            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            store.Runner.Client.FailAfter = n;
            StoreReply<FinishedAttempt>? first = null;
            try
            {
                first = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
            }
            catch (StoreOutcomeUnknownException)
            {
                sawUnknown++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            // The resend recovers any pending intent first and then replays the receipt.
            var again = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal("succeeded", again.Value!.Disposition);
            Assert.True(again.Replayed);
            if (first is not null)
            {
                Assert.Equal(first.Value!, again.Value);
            }
        }

        Assert.True(sawUnknown > 5, $"only {sawUnknown} interrupted calls");
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.All(snapshot.Model.Jobs, j => Assert.Equal("succeeded", j.State));
        Assert.Equal(40, snapshot.Model.Jobs.Count);
    }

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task An_interrupted_DispatchMany_leaves_Recovering_until_any_writer_publishes_it()
    {
        var (f, c) = await OpenAsync("redis01b");
        await using var _ = f;
        var store = Store(c);
        store.Runner.Client.FailAfter = 3;
        try
        {
            await Assert.ThrowsAnyAsync<Exception>(() => c.Dispatcher.DispatchManyAsync(
                [.. Enumerable.Range(0, 20).Select(i => new BatchDispatchItem(new PingJob("m" + i)))], cancellationToken: Ct));
        }
        finally
        {
            store.Runner.Client.FailAfter = 0;
        }

        var mux = await RedisServer.AdminAsync();
        Assert.NotEqual("", (string?)await mux.GetDatabase().HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");

        // A different command (Reserve) finds the guard, publishes the intent, then runs.
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        Assert.True(store.Runner.RecoveringReplies > 0);
        Assert.Equal("", (string?)await mux.GetDatabase().HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Equal(20, snapshot.Model.Jobs.Count);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
    }

    // ------------------------------------------------------------------ REDIS-04 / REDIS-07 limits

    [Fact]
    [Trait("Case", "REDIS-07")]
    public async Task The_largest_DispatchMany_fits_the_intent_limits_and_publishes_in_one_call()
    {
        // The dispatcher caps a batch at 100 items, which stays under 256 keys and 1,024 after-images; the limit refusals
        // themselves are reachable only through larger intents than any current operation builds (see the SP-025 report).
        var (f, c) = await OpenAsync("redis07");
        await using var _ = f;
        var receipt = await c.Dispatcher.DispatchManyAsync(
            [.. Enumerable.Range(0, 100).Select(i => new BatchDispatchItem(new PingJob("k" + i)))], cancellationToken: Ct);
        Assert.Equal(100, receipt.Items.Count);
        var mux = await RedisServer.AdminAsync();
        Assert.Equal("", (string?)await mux.GetDatabase().HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");
        Assert.Equal(0, Store(c).Runner.RecoveringReplies);
        Assert.Equal(100, (await f.ReadCommittedSnapshot()).Model.Jobs.Count);
        await NoViolations(f);
    }

    // ------------------------------------------------------------------ REDIS-05 profile check

    [Fact]
    [Trait("Case", "REDIS-05")]
    public async Task A_runtime_switch_to_everysec_closes_admission_until_the_profile_is_restored()
    {
        var (f, c) = await OpenAsync("redis05");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var store = Store(c);
        var mux = await RedisServer.AdminAsync();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        try
        {
            await server.ConfigSetAsync("appendfsync", "everysec");
            await store.CheckProfileAsync(Ct);
            Assert.Contains("appendfsync=everysec", store.Runner.ProfileFailure, StringComparison.Ordinal);
            Assert.StartsWith("unavailable", store.EngineFacts["profile_status"], StringComparison.Ordinal);
            var refused = await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.ReserveAsync());
            Assert.Equal("store_profile_mismatch", refused.Code);
        }
        finally
        {
            await server.ConfigSetAsync("appendfsync", "always");
        }

        await store.CheckProfileAsync(Ct);
        Assert.Null(store.Runner.ProfileFailure);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
    }

    // ------------------------------------------------------------------ REDIS-08 exact counters

    [Fact]
    [Trait("Case", "REDIS-08")]
    public async Task The_application_revision_stops_at_int64_max_with_counter_limit_and_nothing_written()
    {
        var (f, c) = await OpenAsync("redis08");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var meta = RxClient.Meta(f.Application);
        await db.HashSetAsync(meta, "rev", "9223372036854775806");
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);
        Assert.Equal("9223372036854775807", (string?)await db.HashGetAsync(meta, "rev"));

        var refused = await c.ReserveAsync();
        Assert.Equal(StoreStatus.InvariantFault, refused.Status);
        Assert.Equal("counter_limit", refused.Error!.Code);
        Assert.Equal("9223372036854775807", (string?)await db.HashGetAsync(meta, "rev"));
        Assert.All((await f.ReadCommittedSnapshot()).Model.Jobs, j => Assert.Equal("pending", j.State));
    }

    // ------------------------------------------------------------------ REDIS-10 evidence

    [Fact]
    [Trait("Case", "REDIS-10")]
    public async Task Describe_reports_version_run_id_modules_and_profile_status()
    {
        var (f, c) = await OpenAsync("redis10");
        await using var _ = f;
        var facts = Store(c).EngineFacts;
        Assert.Equal("verified", facts["profile_status"]);
        Assert.True(Version.Parse(facts["redis_version"]) >= new Version(8, 2));
        Assert.Matches("^[0-9a-f]{40}$", facts["run_id"]);
        Assert.True(facts.ContainsKey("modules"));
        Assert.Equal("always", facts["config.appendfsync"]);
        Assert.Equal(AcknowledgementProfiles.RedisAofAlways, Store(c).AcknowledgementProfile);
    }

    // ------------------------------------------------------------------ REDIS-11 encodings

    [Fact]
    [Trait("Case", "REDIS-11")]
    public async Task Application_names_that_differ_only_in_case_get_distinct_hash_tags_and_data()
    {
        Assert.Equal("rw:{417070}:", RxClient.KeyPrefix("App"));
        Assert.Equal("rw:{617070}:", RxClient.KeyPrefix("app"));
        var suffix = Guid.NewGuid().ToString("N")[..8];
        await using var upper = await DriverFixture.StartAsync("Redis11" + suffix, StoreKind.Redis);
        await using var lower = await DriverFixture.StartAsync("redis11" + suffix, StoreKind.Redis);
        var cu = await upper.OpenClient(configure: r => TestJobs.Register(r));
        var cl = await lower.OpenClient(configure: r => TestJobs.Register(r));
        await cu.Dispatcher.DispatchAsync(new PingJob("u"), cancellationToken: Ct);
        Assert.Single((await upper.ReadCommittedSnapshot()).Model.Jobs);
        Assert.Empty((await lower.ReadCommittedSnapshot()).Model.Jobs);
        Assert.Equal(StoreStatus.NoWork, (await cl.ReserveAsync()).Status);
    }

    // ------------------------------------------------------------------ REDIS-13 no TTLs

    [Fact]
    [Trait("Case", "REDIS-13")]
    public async Task No_protocol_key_has_a_TTL()
    {
        var (f, c) = await OpenAsync("redis13");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var reserved = await c.ReserveAsync();
        await c.BeginAsync(reserved.Value!.Lease.Handle);
        await c.FinishAsync(reserved.Value.Lease.Handle, AttemptOutcome.RetryableFailure);
        var mux = await RedisServer.AdminAsync();
        var keys = new List<RedisKey>();
        await foreach (var k in mux.GetServer(mux.GetEndPoints()[0]).KeysAsync(0, RxClient.KeyPrefix(f.Application) + "*"))
        {
            keys.Add(k);
        }

        Assert.NotEmpty(keys);
        foreach (var k in keys.Append("rw:store"))
        {
            Assert.Null(await mux.GetDatabase().KeyTimeToLiveAsync(k));
        }
    }

    // ------------------------------------------------------------------ REDIS-14 migration

    [Fact]
    [Trait("Case", "REDIS-14")]
    public async Task Migration_installs_once_reruns_as_a_no_op_and_refuses_a_foreign_key()
    {
        await FlushAsync(9);
        var first = await RunwickRedisMigrations.MigrateAsync(Db(9), Ct);
        Assert.Equal(["expand", "validate", "activate"], first.AppliedPhases);
        var again = await RunwickRedisMigrations.MigrateAsync(Db(9), Ct);
        Assert.Empty(again.AppliedPhases);
        Assert.Equal(first.StoreId, again.StoreId);

        await FlushAsync(10);
        var db10 = (await RedisServer.AdminAsync()).GetDatabase(10);
        await db10.StringSetAsync("rw:x", "foreign");
        var ex = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(Db(10), Ct));
        Assert.Equal("schema_not_empty", ex.Code);
        await FlushAsync(10);

        // Two concurrent migrators: one installs, the other is a no-op on the same store.
        var both = await Task.WhenAll(RunwickRedisMigrations.MigrateAsync(Db(10), Ct), RunwickRedisMigrations.MigrateAsync(Db(10), Ct));
        Assert.Equal(both[0].StoreId, both[1].StoreId);
        Assert.Single(both, r => r.AppliedPhases.Count > 0);
        await FlushAsync(9);
        await FlushAsync(10);
    }

    // ------------------------------------------------------------------ REDIS-15 history

    [Fact]
    [Trait("Case", "REDIS-15")]
    public async Task A_flushed_store_is_store_identity_mismatch_and_a_revision_that_went_back_is_history_discontinuity()
    {
        await FlushAsync(11);
        await RunwickRedisMigrations.MigrateAsync(Db(11), Ct);
        var app = "redis15_" + Guid.NewGuid().ToString("N")[..8];
        await using (var c = await TestClient.CreateAsync(RedisServer.Prefix + Db(11), app, new HarnessFaultInjector(), r => TestJobs.Register(r)))
        {
            await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
            var store = Store(c);
            var db = (await RedisServer.AdminAsync()).GetDatabase(11);
            await db.HashSetAsync(RxClient.Meta(app), "rev", "0");
            await store.CheckHistoryAsync();
            Assert.Equal("history_discontinuity", store.Runner.HistoryFailure);
            var refused = await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.ReserveAsync());
            Assert.Contains("history_discontinuity", refused.Message, StringComparison.Ordinal);
        }

        await using (var c = await TestClient.CreateAsync(RedisServer.Prefix + Db(11), app + "b", new HarnessFaultInjector(), r => TestJobs.Register(r)))
        {
            var store = Store(c);
            await FlushAsync(11);
            await RunwickRedisMigrations.MigrateAsync(Db(11), Ct);
            await store.CheckHistoryAsync();
            Assert.Equal("store_identity_mismatch", store.Runner.HistoryFailure);
        }

        await FlushAsync(11);
    }
}
