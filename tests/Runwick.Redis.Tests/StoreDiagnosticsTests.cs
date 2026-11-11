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

    static RedisStore NewStore(string configuration) =>
        new(new RxRunner(new RxClient(RxClient.Options(configuration, TimeSpan.FromSeconds(2)), "diagnostics", 1),
            TimeProvider.System), new RedisOptions(), TimeProvider.System);
}
