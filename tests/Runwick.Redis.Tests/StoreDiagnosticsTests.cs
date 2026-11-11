using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

public sealed class StoreDiagnosticsTests
{
    [Fact]
    public async Task Connected_node_statistics_are_selected_and_info_denial_is_safe()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = NewStore(RedisServer.Configuration);
        var result = await ((IRedisStoreDiagnosticsSource)store).ObserveDiagnosticsAsync(ct);
        Assert.True(result.Memory.Value?.UsedBytes > 0);
        Assert.True(result.Activity.Value?.ConnectedClients > 0);
        Assert.True(result.Aof.Value?.Enabled);
        Assert.Equal(RedisAofStatus.Ok, result.Aof.Value?.LastWrite);
        Assert.Equal(RedisNodeRole.Primary, result.Replication.Value?.Role);
        Assert.True(result.Replication.Value?.ReplicationOffset >= 0);

        var user = "rw_diag_" + Guid.NewGuid().ToString("N");
        var password = Guid.NewGuid().ToString("N");
        var admin = await RedisServer.AdminAsync();
        var db = admin.GetDatabase();
        await db.ExecuteAsync("ACL", "SETUSER", user, "on", ">" + password, "+@all", "-info");
        try
        {
            var configuration = ConfigurationOptions.Parse(RedisServer.Configuration);
            configuration.User = user;
            configuration.Password = password;
            await using var restricted = NewStore(configuration.ToString(includePassword: true));
            var denied = await ((IRedisStoreDiagnosticsSource)restricted).ObserveDiagnosticsAsync(ct);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, denied.Memory.Status);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, denied.Activity.Status);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, denied.Aof.Status);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, denied.Replication.Status);
            Assert.Null(denied.Replication.Value);
        }
        finally
        {
            await db.ExecuteAsync("ACL", "DELUSER", user);
        }
    }

    [Fact]
    public async Task Pending_intent_observation_is_app_scoped_and_hmget_denial_leaves_node_facts_available()
    {
        var ct = TestContext.Current.CancellationToken;
        var application = "diagnostics-" + Guid.NewGuid().ToString("N");
        var meta = RxClient.Meta(application);
        var user = "rw_diag_" + Guid.NewGuid().ToString("N");
        var password = Guid.NewGuid().ToString("N");
        var mux = await RedisServer.AdminAsync();
        var db = mux.GetDatabase();
        await using var store = NewStore(RedisServer.Configuration, application);
        await using var other = NewStore(RedisServer.Configuration, application + "-other");
        var source = (IRedisStoreDiagnosticsSource)store;
        var storeId = Guid.NewGuid();
        store.Runner.StoreId = storeId;
        other.Runner.StoreId = storeId;
        var guard = Guid.NewGuid().ToString("N");
        try
        {
            await db.HashSetAsync(meta, [new("store", storeId.ToString("N")), new("guard", guard), new("ipos", "101")]);
            var pending = (await source.ObserveDiagnosticsAsync(ct)).IntentRecovery;
            Assert.Equal(StoreSnapshotScope.Application, pending.Scope);
            Assert.Equal(StoreSnapshotStatus.Available, pending.Status);
            Assert.Equal(new RedisIntentRecovery(true, 101), pending.Value);
            Assert.Equal(guard, (string?)await db.HashGetAsync(meta, "guard"));

            var missing = (await ((IRedisStoreDiagnosticsSource)other).ObserveDiagnosticsAsync(ct)).IntentRecovery;
            Assert.Equal(StoreSnapshotStatus.Unavailable, missing.Status);
            Assert.Null(missing.Value);

            await db.HashSetAsync(meta, "store", Guid.NewGuid().ToString("N"));
            var mismatched = (await source.ObserveDiagnosticsAsync(ct)).IntentRecovery;
            Assert.Equal(StoreSnapshotStatus.Unavailable, mismatched.Status);
            Assert.Null(mismatched.Value);
            await db.HashSetAsync(meta, "store", storeId.ToString("N"));

            await db.HashSetAsync(meta, "ipos", "1025");
            Assert.Equal(new RedisIntentRecovery(true, null), (await source.ObserveDiagnosticsAsync(ct)).IntentRecovery.Value);
            await db.HashSetAsync(meta, "guard", "");
            Assert.Equal(new RedisIntentRecovery(false, null), (await source.ObserveDiagnosticsAsync(ct)).IntentRecovery.Value);

            await db.ExecuteAsync("ACL", "SETUSER", user, "on", ">" + password, "~" + meta.ToString(), "+@all", "-hmget");
            var configuration = ConfigurationOptions.Parse(RedisServer.Configuration);
            configuration.User = user;
            configuration.Password = password;
            await using var restricted = NewStore(configuration.ToString(includePassword: true), application);
            restricted.Runner.StoreId = storeId;
            var denied = await ((IRedisStoreDiagnosticsSource)restricted).ObserveDiagnosticsAsync(ct);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, denied.IntentRecovery.Status);
            Assert.Null(denied.IntentRecovery.Value);
            Assert.Equal(StoreSnapshotStatus.Available, denied.Memory.Status);

            await db.KeyDeleteAsync(meta);
            await db.StringSetAsync(meta, "wrong-type");
            var wrongType = await source.ObserveDiagnosticsAsync(ct);
            Assert.Equal(StoreSnapshotStatus.Unavailable, wrongType.IntentRecovery.Status);
            Assert.Equal(StoreSnapshotStatus.Available, wrongType.Memory.Status);
        }
        finally
        {
            await db.ExecuteAsync("ACL", "DELUSER", user);
            await db.KeyDeleteAsync(meta);
        }
    }

    [Fact]
    public void Missing_or_unknown_values_are_not_reported_as_zero()
    {
        var result = RedisStore.ParseDiagnostics("""
            used_memory:42
            mem_fragmentation_ratio:NaN
            aof_enabled:1
            aof_last_write_status:unknown
            role:slave
            connected_slaves:0
            slave_repl_offset:123
            master_link_status:down
            slave0:ip=private.example,port=1234,offset=12
            """);
        Assert.Equal(42L, result.Memory.Value?.UsedBytes);
        Assert.Null(result.Memory.Value?.LimitBytes);
        Assert.Null(result.Memory.Value?.FragmentationRatio);
        Assert.Equal(StoreSnapshotStatus.Unavailable, result.Activity.Status);
        Assert.Null(result.Aof.Value?.LastWrite);
        Assert.Equal(RedisNodeRole.Replica, result.Replication.Value?.Role);
        Assert.Equal(0L, result.Replication.Value?.ConnectedReplicas);
        Assert.Equal(123L, result.Replication.Value?.ReplicaAppliedOffset);
        Assert.False(result.Replication.Value?.PrimaryLinkUp);
    }

    static RedisStore NewStore(string configuration, string application = "diagnostics") =>
        new(new RxRunner(new RxClient(RxClient.Options(configuration, TimeSpan.FromSeconds(2)), application, 1),
            TimeProvider.System), new RedisOptions(), TimeProvider.System);
}
