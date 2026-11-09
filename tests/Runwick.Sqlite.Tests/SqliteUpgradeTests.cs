using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Sqlite.Tests;

/// <summary>UPG-02, UPG-03, UPG-08 and UPG-10 on SQLite (23 §2, §11). UPG-04 and UPG-09 are in Runwick.Contract.Tests.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-026")]
public sealed class SqliteUpgradeTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    static Task<IReadOnlyList<AdmissionChange>> Admission(DriverFixture f, string mode) =>
        RunwickSqliteMigrations.SetAdmissionModeAsync(f.AdminConnectionString, f.Application, mode, "ops", Ct);

    static string Reply<T>(StoreReply<T> r) where T : class => r.Status + "/" + r.Error?.Code;

    [Fact]
    [Trait("Case", "UPG-02")]
    public async Task A_binary_runs_only_on_its_own_revision()
    {
        var (f, c) = await OpenAsync("squpg02");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var before = await f.ReadCommittedSnapshot();
        await f.ExecAsync("UPDATE runwick_store_metadata SET schema_revision = 3");
        Assert.Equal("UnsupportedVersion/schema_incompatible", Reply(await c.ReserveAsync()));
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("x"), cancellationToken: Ct));
        // Revision 0 is an unvalidated install: maintenance.
        await f.ExecAsync("UPDATE runwick_store_metadata SET schema_revision = 0, migration_mode = 'maintenance'");
        Assert.Equal("AdmissionClosed/store_maintenance", Reply(await c.ReserveAsync()));
        await f.ExecAsync("UPDATE runwick_store_metadata SET schema_revision = 2, migration_mode = 'expand'");
        Assert.Equal("AdmissionClosed/store_maintenance", Reply(await c.ReserveAsync()));
        await f.ExecAsync("UPDATE runwick_store_metadata SET migration_mode = 'normal'");
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        Assert.Contains("jobs", before.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Case", "UPG-03")]
    public async Task A_writer_holding_the_file_delays_the_change_then_it_times_out_with_nothing_changed()
    {
        var (f, _) = await OpenAsync("squpg03");
        await using var _f = f;
        using var holder = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False");
        holder.Open();
        using (var begin = holder.CreateCommand())
        {
            begin.CommandText = "BEGIN IMMEDIATE";
            begin.ExecuteNonQuery();
        }

        var clock = Stopwatch.StartNew();
        var timeout = await Assert.ThrowsAsync<SqliteMigrationException>(() => Admission(f, "drain"));
        Assert.Equal("migration_lock_timeout", timeout.Code);
        Assert.InRange(clock.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(15));

        using (var rollback = holder.CreateCommand())
        {
            rollback.CommandText = "ROLLBACK";
            rollback.ExecuteNonQuery();
        }

        var status = await RunwickSqliteMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal("open", status.Applications.Single().AdmissionMode);
        Assert.True((await Admission(f, "drain")).Single().Changed);
    }

    [Fact]
    [Trait("Case", "UPG-08")]
    public async Task Status_counts_expired_presence_and_leases_and_the_identity_change_refuses_live_rows()
    {
        var (f, c) = await OpenAsync("squpg08");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready", Environment.ProcessId,
            "host", "build", ["default"], new Dictionary<string, int>(), c.Contracts, TimeSpan.FromSeconds(3)), Ct)).Accepted);
        await Admission(f, "drain");
        await Admission(f, "maintenance");

        var live = await RunwickSqliteMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal((1, 1, 0), (live.OwnedLeases, live.WorkerPresences, live.ExpiredWorkerPresences));
        Assert.Equal("store_not_quiescent", (await Assert.ThrowsAsync<SqliteMigrationException>(() => RunwickSqliteMigrations.ReassignStoreIdAsync(f.AdminConnectionString, Ct))).Code);
        await Task.Delay(3500, Ct);
        var expired = await RunwickSqliteMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal((1, 1, 1), (expired.OwnedLeases, expired.WorkerPresences, expired.ExpiredWorkerPresences));
        Assert.NotEqual(live.StoreId, (await RunwickSqliteMigrations.ReassignStoreIdAsync(f.AdminConnectionString, Ct)).NewStoreId);
        Assert.Equal(1, (await RunwickSqliteMigrations.GetStatusAsync(f.AdminConnectionString, Ct)).OwnedLeases);
    }

    [Fact]
    [Trait("Case", "UPG-10")]
    public async Task Admission_changes_during_renewal_load_stay_bounded()
    {
        var (f, c) = await OpenAsync("squpg10");
        await using var _ = f;
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
                var t = Stopwatch.StartNew();
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
        for (var i = 0; i < 40; i++)
        {
            foreach (var mode in new[] { "drain", "open" })
            {
                var t = Stopwatch.StartNew();
                await Admission(f, mode);
                await RunwickSqliteMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
                slowest = t.Elapsed > slowest ? t.Elapsed : slowest;
            }
        }

        await stop.CancelAsync();
        await renew;
        Assert.Empty(notApplied);
        Assert.True(renewals >= 5, $"only {renewals} renewals ran");
        Assert.True(worst < TimeSpan.FromSeconds(2), $"slowest RenewMany {worst}");
        Assert.True(slowest < TimeSpan.FromSeconds(5), $"slowest admission change {slowest}");
    }
}
