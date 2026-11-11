using Npgsql;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Postgres.Store;

namespace Runwick.Postgres.Tests;

public sealed class StoreDiagnosticsTests
{
    [Fact]
    public async Task Restricted_roles_keep_database_statistics_and_cannot_read_privileged_fields()
    {
        var suffix = Guid.NewGuid().ToString("N")[..12];
        await using var fixture = await DriverFixture.StartAsync("diag_" + suffix);
        var role = "rw_diag_" + suffix;
        await StoreServer.ExecAsync(fixture.AdminConnectionString, $"CREATE ROLE {role}");
        try
        {
            await StoreServer.ExecAsync(fixture.AdminConnectionString, $"GRANT USAGE ON SCHEMA runwick TO {role}");
            await StoreServer.ExecAsync(fixture.AdminConnectionString, "REVOKE SELECT ON pg_catalog.pg_stat_wal FROM PUBLIC");
            var connection = new NpgsqlConnectionStringBuilder(fixture.ClientConnectionString("diagnostics"))
            {
                Options = $"-c role={role}",
            };
            var sources = new PgDataSources(connection.ConnectionString, "diagnostics", 1);
            var runner = new PgRunner(sources, fixture.Faults, TimeProvider.System);
            await using var store = new PgStore(sources, runner, TimeProvider.System);
            var ct = TestContext.Current.CancellationToken;
            var stats = await ((IPostgresStoreDiagnosticsSource)store).ObserveDiagnosticsAsync(ct);

            Assert.True(stats.Sessions.Value?.ConnectedSessions > 0);
            Assert.True(stats.DatabaseSize.Value?.Bytes > 0);
            Assert.True(stats.JobsTable.Value?.TotalBytes > 0);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, stats.LockWaits.Status);
            Assert.Null(stats.LockWaits.Value);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, stats.Replication.Status);
            Assert.Null(stats.Replication.Value);
            Assert.Equal(StoreSnapshotStatus.PermissionDenied, stats.Wal.Status);
            Assert.Null(stats.Wal.Value);
            Assert.False(stats.Recovery.Value?.InRecovery);
        }
        finally
        {
            await StoreServer.ExecAsync(fixture.AdminConnectionString, $"DROP OWNED BY {role}");
            await StoreServer.ExecAsync(fixture.AdminConnectionString, $"DROP ROLE {role}");
        }
    }
}
