using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>UPG-02, UPG-03 (the pending-intent form), UPG-09 and UPG-10 on Redis (23 §2, §11). Database 12 of the test server is flushed.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-026")]
[Collection(nameof(RedisServerState))]
public sealed class RedisUpgradeTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    const int Database = 12;

    static string Config => RedisServer.Configuration + ",defaultDatabase=" + Database;

    static async Task<IDatabase> FreshAsync()
    {
        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(Database);
        await RunwickRedisMigrations.MigrateAsync(Config, Ct);
        return mux.GetDatabase(Database);
    }

    static Task<TestClient> ClientAsync(string app) =>
        TestClient.CreateAsync(RedisServer.Prefix + Config, app, new HarnessFaultInjector(), r => TestJobs.Register(r));

    [Fact]
    [Trait("Case", "UPG-02")]
    public async Task A_binary_runs_only_on_its_own_layout_and_expand_mode_admits_nobody()
    {
        var db = await FreshAsync();
        var app = "redisupg02_" + Guid.NewGuid().ToString("N")[..8];
        await using var c = await ClientAsync(app);
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var meta = RxClient.Meta(app);

        // The layout and epoch live in the application's meta copy and in rw:store; the gate reads the meta copy.
        await db.HashSetAsync(meta, "layout", "2");
        var wrongLayout = await c.ReserveAsync();
        Assert.Equal(("UnsupportedVersion", "schema_incompatible"), (wrongLayout.Status.ToString(), wrongLayout.Error!.Code));
        await db.HashSetAsync(meta, [new HashEntry("layout", "1"), new HashEntry("mode", "expand")]);
        var expand = await c.ReserveAsync();
        Assert.Equal(("AdmissionClosed", "store_maintenance"), (expand.Status.ToString(), expand.Error!.Code));
        await db.HashSetAsync(meta, "mode", "normal");
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
    }

    [Fact]
    [Trait("Case", "UPG-03")]
    public async Task A_pending_intent_blocks_the_change_with_nothing_changed()
    {
        var db = await FreshAsync();
        var app = "redisupg03_" + Guid.NewGuid().ToString("N")[..8];
        await using var c = await ClientAsync(app);
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var meta = RxClient.Meta(app);

        await db.HashSetAsync(meta, "intent", "pending");
        var refused = await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "drain", "ops", Ct));
        Assert.Equal("intent_pending", refused.Code);
        Assert.Equal("open", (string?)await db.HashGetAsync(meta, "admission"));
        await db.HashSetAsync(meta, "intent", "");
        Assert.True((await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "drain", "ops", Ct)).Single().Changed);
    }

    [Fact]
    [Trait("Case", "UPG-09")]
    public async Task A_new_store_identity_refuses_everything_bound_to_the_old_one()
    {
        await FreshAsync();
        var app = "redisupg09_" + Guid.NewGuid().ToString("N")[..8];
        await using var c = await ClientAsync(app);
        var oldEnvelope = c.Prepared.Prepare(new PingJob("old"));

        Assert.Equal("store_not_quiescent", (await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.ReassignStoreIdAsync(Config, Ct))).Code);
        await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "drain", "ops", Ct);
        await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "maintenance", "ops", Ct);
        var change = await RunwickRedisMigrations.ReassignStoreIdAsync(Config, Ct);
        Assert.NotEqual(change.OldStoreId, change.NewStoreId);
        Assert.Equal(change.NewStoreId, (await RunwickRedisMigrations.GetStatusAsync(Config, Ct)).StoreId);

        var refused = await c.ReserveAsync();
        Assert.Equal(("UnsupportedVersion", "store_identity_mismatch"), (refused.Status.ToString(), refused.Error!.Code));
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Prepared.DispatchAsync(oldEnvelope, Ct));

        await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "open", "ops", Ct);
        await using var fresh = await ClientAsync(app);
        Assert.Equal(DispatchDisposition.Created, (await fresh.Dispatcher.DispatchAsync(new PingJob("new"), cancellationToken: Ct)).Disposition);
        await Assert.ThrowsAnyAsync<RunwickException>(() => fresh.Prepared.DispatchAsync(oldEnvelope, Ct));

        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(Database);
    }

    [Fact]
    [Trait("Case", "UPG-08")]
    public async Task Status_counts_leases_and_presences_with_the_notice_and_an_expired_lease_is_neither_proof_nor_permission()
    {
        await FreshAsync();
        var app = "redisupg08_" + Guid.NewGuid().ToString("N")[..8];
        await using var c = await ClientAsync(app);
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready", Environment.ProcessId,
            "host", "build", ["default"], new Dictionary<string, int>(), c.Contracts, TimeSpan.FromSeconds(3)), Ct)).Accepted);

        // A running handler does not hold up drain or maintenance, and a live lease is not what refuses the identity change:
        // the admission mode is (the counts are reported, 23 §6).
        Assert.Equal("store_not_quiescent", (await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.ReassignStoreIdAsync(Config, Ct))).Code);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "drain", "ops", Ct);
        await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, "maintenance", "ops", Ct);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"drain and maintenance took {clock.Elapsed}");

        var status = await RunwickRedisMigrations.GetStatusAsync(Config, Ct);
        Assert.Equal((1, 1, 0), (status.OwnedLeases, status.WorkerPresences, status.ExpiredWorkerPresences));
        Assert.Equal(StoreAdminRules.QuiescenceNotice, status.Quiescence);
        Assert.StartsWith("unverified", status.Quiescence, StringComparison.Ordinal);

        // The lease has expired, and it is still a lease row until Recover moves the job: expiry is reported, not acted on.
        await Task.Delay(3500, Ct);
        Assert.Single(await c.Driver.ListExpiredLeasesAsync(app, 10, Ct));
        var expired = await RunwickRedisMigrations.GetStatusAsync(Config, Ct);
        Assert.Equal((1, 1, 1), (expired.OwnedLeases, expired.WorkerPresences, expired.ExpiredWorkerPresences));
        var changed = await RunwickRedisMigrations.ReassignStoreIdAsync(Config, Ct);
        Assert.NotEqual(changed.OldStoreId, changed.NewStoreId);
        // The holder, still bound to the old identity, is refused rather than trusted.
        Assert.NotEqual(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);

        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(Database);
    }

    [Fact]
    [Trait("Case", "UPG-10")]
    public async Task Admission_changes_during_renewal_load_stay_bounded()
    {
        await FreshAsync();
        var app = "redisupg10_" + Guid.NewGuid().ToString("N")[..8];
        await using var c = await ClientAsync(app);
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 8; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        for (var i = 0; i < 8; i++)
        {
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
            leases.Add(lease);
        }

        using var stop = new CancellationTokenSource();
        var worst = TimeSpan.Zero;
        var renewals = 0;
        var notApplied = new List<string>();
        var renew = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var t = System.Diagnostics.Stopwatch.StartNew();
                var reply = await c.RenewManyAsync(leases);
                worst = t.Elapsed > worst ? t.Elapsed : worst;
                renewals++;
                if (reply.Status != StoreStatus.Applied)
                {
                    notApplied.Add(reply.Status + "/" + reply.Error?.Code);
                }

                await Task.Delay(30);
            }
        }, Ct);

        var slowest = TimeSpan.Zero;
        for (var i = 0; i < 10; i++)
        {
            foreach (var mode in new[] { "drain", "open" })
            {
                var t = System.Diagnostics.Stopwatch.StartNew();
                await RunwickRedisMigrations.SetAdmissionModeAsync(Config, app, mode, "ops", Ct);
                slowest = t.Elapsed > slowest ? t.Elapsed : slowest;
            }
        }

        await stop.CancelAsync();
        await renew;
        Assert.Empty(notApplied);
        Assert.True(renewals > 10);
        Assert.True(worst < TimeSpan.FromSeconds(2), $"slowest RenewMany {worst}");
        Assert.True(slowest < TimeSpan.FromSeconds(5), $"slowest admission change {slowest}");
        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(Database);
    }
}
