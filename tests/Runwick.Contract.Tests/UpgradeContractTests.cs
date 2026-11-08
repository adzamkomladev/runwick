using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// UPG-04 (drain and maintenance on every driver) and UPG-09 (identity change on the SQL drivers), 23 §11. The same operator
/// calls run against each driver's own migrator class; the client calls are the real protocol.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-026")]
public sealed class UpgradeContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    public static TheoryData<StoreKind> SqlKinds =>
        [.. Enum.GetValues<StoreKind>().Where(k => k != StoreKind.Redis && (Environment.GetEnvironmentVariable("RUNWICK_TEST_STORES") is not { Length: > 0 } only
            || only.Split(',').Contains(k.ToString(), StringComparer.OrdinalIgnoreCase)))];

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    static string Conn(DriverFixture f) => f.Kind == StoreKind.Redis ? RedisServer.ConfigurationOf(f.AdminConnectionString) : f.AdminConnectionString;

    static Task<IReadOnlyList<AdmissionChange>> Set(DriverFixture f, string mode) => f.Kind switch
    {
        StoreKind.Postgres => RunwickPostgresMigrations.SetAdmissionModeAsync(Conn(f), f.Application, mode, "ops", Ct),
        StoreKind.Sqlite => RunwickSqliteMigrations.SetAdmissionModeAsync(Conn(f), f.Application, mode, "ops", Ct),
        _ => RunwickRedisMigrations.SetAdmissionModeAsync(Conn(f), f.Application, mode, "ops", Ct),
    };

    static string CodeOf(Exception e) => e switch
    {
        PostgresMigrationException p => p.Code,
        SqliteMigrationException s => s.Code,
        RedisMigrationException r => r.Code,
        _ => throw new InvalidOperationException("not a migration exception: " + e),
    };

    static async Task<string> FailsWith(Func<Task> call) => CodeOf(await Assert.ThrowsAnyAsync<Exception>(call));

    static string Reply<T>(StoreReply<T> r) where T : class => r.Status + "/" + r.Error?.Code;

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UPG-04")]
    public async Task Drain_and_maintenance_block_every_creation_path_and_keep_the_settlement_paths_the_table_allows(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "upg04");
        await using var _ = f;
        foreach (var name in new[] { "a", "b", "c", "d" })
        {
            await c.Dispatcher.DispatchAsync(new PingJob(name), cancellationToken: Ct);
        }

        var a = (await c.ReserveAsync()).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(a)).Reply.Status);
        var d = (await c.ReserveAsync()).Value!.Lease.Handle; // second job; kept running for the maintenance checks
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(d)).Reply.Status);
        var b = (await c.ReserveAsync()).Value!.Lease.Handle; // reserved, not begun

        // Transitions: open to maintenance is refused, an unknown mode is refused, a repeat changes nothing.
        Assert.Equal("drain_first", await FailsWith(() => Set(f, "maintenance")));
        Assert.Equal("invalid_mode", await FailsWith(() => Set(f, "paused")));
        Assert.Equal("application_not_registered", await FailsWith(() => f.Kind switch
        {
            StoreKind.Postgres => RunwickPostgresMigrations.SetAdmissionModeAsync(Conn(f), "nobody", "drain", "ops", Ct),
            StoreKind.Sqlite => RunwickSqliteMigrations.SetAdmissionModeAsync(Conn(f), "nobody", "drain", "ops", Ct),
            _ => RunwickRedisMigrations.SetAdmissionModeAsync(Conn(f), "nobody", "drain", "ops", Ct),
        }));
        var drained = (await Set(f, "drain")).Single();
        Assert.Equal(("open", "drain", true), (drained.PreviousMode, drained.Mode, drained.Changed));
        Assert.False((await Set(f, "drain")).Single().Changed);

        // Drain: creation paths refused with nothing written.
        var pre = await f.ReadCommittedSnapshot();
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("late"), cancellationToken: Ct));
        Assert.Equal("AdmissionClosed/application_draining", Reply(await c.ReserveAsync()));
        Assert.Equal("AdmissionClosed/application_draining", Reply((await c.BeginAsync(b)).Reply));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        // Drain: settlement and receipt paths continue.
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(a)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(b)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(a, AttemptOutcome.Succeeded)).Status);

        // Maintenance: nothing changes, not even renewal.
        Assert.Equal(("drain", "maintenance", true), (await Set(f, "maintenance")).Select(x => (x.PreviousMode, x.Mode, x.Changed)).Single());
        pre = await f.ReadCommittedSnapshot();
        Assert.Equal("AdmissionClosed/application_maintenance", Reply(await c.RenewAsync(d)));
        Assert.Equal("AdmissionClosed/application_maintenance", Reply(await c.FinishAsync(d, AttemptOutcome.Succeeded)));
        Assert.Equal("AdmissionClosed/application_maintenance", Reply(await c.ReserveAsync()));
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("later"), cancellationToken: Ct));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // Back through drain to open: the running job settles, new work flows.
        await Set(f, "drain");
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(d, AttemptOutcome.Succeeded)).Status);
        await Set(f, "open");
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        Assert.Equal(DispatchDisposition.Created, (await c.Dispatcher.DispatchAsync(new PingJob("again"), cancellationToken: Ct)).Disposition);
        var status = f.Kind == StoreKind.Postgres ? await RunwickPostgresMigrations.GetStatusAsync(Conn(f), Ct)
            : f.Kind == StoreKind.Sqlite ? await RunwickSqliteMigrations.GetStatusAsync(Conn(f), Ct)
            : await RunwickRedisMigrations.GetStatusAsync(Conn(f), Ct);
        Assert.Equal("open", status.Applications.Single(x => x.Application == f.Application).AdmissionMode);
        Assert.StartsWith("unverified", status.Quiescence, StringComparison.Ordinal);
        Assert.Equal(f.Kind == StoreKind.Redis, status.OwnedLeases is null);
    }

    [Theory]
    [MemberData(nameof(SqlKinds))]
    [Trait("Case", "UPG-09")]
    public async Task A_new_store_identity_refuses_everything_bound_to_the_old_one(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "upg09");
        await using var _ = f;
        var oldEnvelope = c.Prepared.Prepare(new PingJob("old"));
        var cs = Conn(f);

        // Offline only: every application must be in maintenance first.
        Assert.Equal("store_not_quiescent", await FailsWith(() => Reassign(kind, cs)));
        await Set(f, "drain");
        Assert.Equal("store_not_quiescent", await FailsWith(() => Reassign(kind, cs)));
        await Set(f, "maintenance");

        var change = await Reassign(kind, cs);
        Assert.NotEqual(change.OldStoreId, change.NewStoreId);
        var status = kind == StoreKind.Postgres ? await RunwickPostgresMigrations.GetStatusAsync(cs, Ct) : await RunwickSqliteMigrations.GetStatusAsync(cs, Ct);
        Assert.Equal(change.NewStoreId, status.StoreId);

        // The process still bound to the old identity is refused with nothing written; the old envelope is too.
        Assert.Equal("UnsupportedVersion/store_identity_mismatch", Reply(await c.ReserveAsync()));
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Prepared.DispatchAsync(oldEnvelope, Ct));

        // A new process learns the new identity and works once the application is reopened.
        await Set(f, "open");
        var fresh = await f.OpenClient(configure: r => TestJobs.Register(r));
        Assert.Equal(DispatchDisposition.Created, (await fresh.Dispatcher.DispatchAsync(new PingJob("new"), cancellationToken: Ct)).Disposition);
        await Assert.ThrowsAnyAsync<RunwickException>(() => fresh.Prepared.DispatchAsync(oldEnvelope, Ct));
    }

    static Task<StoreIdentityChange> Reassign(StoreKind kind, string cs) =>
        kind == StoreKind.Postgres ? RunwickPostgresMigrations.ReassignStoreIdAsync(cs, Ct) : RunwickSqliteMigrations.ReassignStoreIdAsync(cs, Ct);
}
