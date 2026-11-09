using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Microsoft.Data.Sqlite;
using Runwick.Harness.Peer;
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

    public static TheoryData<StoreKind> NonPostgresKinds =>
        [.. Enum.GetValues<StoreKind>().Where(k => k != StoreKind.Postgres && (Environment.GetEnvironmentVariable("RUNWICK_TEST_STORES") is not { Length: > 0 } only
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
        Assert.NotNull(status.OwnedLeases); // every store reports lease counts (Redis since SP-025)
    }

    static Task<StoreAdminStatus> StatusOf(DriverFixture f) => f.Kind switch
    {
        StoreKind.Postgres => RunwickPostgresMigrations.GetStatusAsync(Conn(f), Ct),
        StoreKind.Sqlite => RunwickSqliteMigrations.GetStatusAsync(Conn(f), Ct),
        _ => RunwickRedisMigrations.GetStatusAsync(Conn(f), Ct),
    };

    /// <summary>Plants what a newer binary leaves behind: an applied migration (revision) this binary does not know.</summary>
    static async Task PlantNewerStoreAsync(DriverFixture f)
    {
        switch (f.Kind)
        {
            case StoreKind.Sqlite:
                using (var conn = new SqliteConnection(f.AdminConnectionString))
                {
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = """
                        INSERT INTO runwick_schema_migrations (migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at)
                        VALUES ('9999_future', 'expand', zeroblob(32), 9999, 'applied', 'runwick/future', 1, 1);
                        UPDATE runwick_store_metadata SET schema_revision = 9999;
                        """;
                    cmd.ExecuteNonQuery();
                }

                break;
            case StoreKind.Redis:
                await (await RedisServer.AdminAsync()).GetDatabase().HashSetAsync("rw:store", "layout", "9999");
                break;
            default:
                await f.ExecAsync("""
                    INSERT INTO runwick.schema_migrations (migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at)
                    VALUES ('9999_future', 'expand', sha256('x'), 9999, 'applied', 'runwick/future', clock_timestamp(), clock_timestamp());
                    UPDATE runwick.store_metadata SET schema_revision = 9999;
                    """);
                break;
        }
    }

    static async Task Migrate(DriverFixture f)
    {
        switch (f.Kind)
        {
            case StoreKind.Postgres:
                await RunwickPostgresMigrations.MigrateAsync(Conn(f), Ct);
                break;
            case StoreKind.Sqlite:
                await RunwickSqliteMigrations.MigrateAsync(Conn(f), Ct);
                break;
            default:
                await RunwickRedisMigrations.MigrateAsync(Conn(f), Ct);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UPG-06")]
    public async Task Rollback_after_activation_is_rejected(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "upg06");
        await using var _ = f;
        var rates = (IRateControlStore)c.Driver;
        var actor = new ControlActor("user:ops", null, "host", "t");
        Task<StoreReply<ActivatedCapabilities>> Activate(int version) =>
            rates.ActivateCapabilitiesAsync(c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = version }, actor)), Ct);
        Assert.Equal(StoreStatus.Applied, (await Activate(1)).Status);

        // Deactivation: a capability is never lowered, and a repeat changes nothing.
        var lower = await Activate(0);
        Assert.Equal((StoreStatus.InvalidRequest, StoreEffect.ObservedNoEffect), (lower.Status, lower.Effect));
        var same = await Activate(1);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));

        // Binary rollback: the old binary cannot complete its handshake (RegisterQueue is a creation path).
        var old = await f.StartPeer("old-1");
        var ex = await Assert.ThrowsAsync<PeerCommandException>(() => old.OpenOldBinaryAsync(f.ClientConnectionString("old-1"), f.Application, "rate_admission"));
        Assert.Contains("capability_unsupported", ex.Error, StringComparison.Ordinal);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'ActivateCapabilities'"));

        // The current binary still runs work.
        await c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);

    }

    /// <summary>
    /// A migrator meeting a store written by a newer binary fails migration_unknown; status shows the store ahead of the binary.
    /// PostgreSQL has its own form on a fresh database (Runwick.Postgres.Tests): the contract fixtures there are migrated with fault hooks.
    /// </summary>
    [Theory]
    [MemberData(nameof(NonPostgresKinds))]
    [Trait("Case", "UPG-06")]
    public async Task A_migrator_refuses_a_store_that_is_ahead_of_it_and_status_shows_the_floor(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "upg06m");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        await Migrate(f); // the same store is current: nothing to do, no refusal

        await PlantNewerStoreAsync(f);
        try
        {
            Assert.Equal("migration_unknown", await FailsWith(() => Migrate(f)));
            var status = await StatusOf(f);
            Assert.Equal(9999L, status.StoreRevision);
            Assert.True(status.StoreRevision > status.BinaryRevision);
        }
        finally
        {
            if (kind == StoreKind.Redis)
            {
                // rw:store is shared by every application on the server: put the layout back.
                await (await RedisServer.AdminAsync()).GetDatabase().HashSetAsync("rw:store", "layout", "1");
            }
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UPG-07")]
    public async Task Old_receipts_resolve_without_reapplication_and_an_unknown_codec_is_explicit(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "upg07");
        await using var _ = f;
        var p = c.Prepared.Prepare(new PingJob("x"));
        var first = await c.Prepared.DispatchAsync(p, Ct);
        var snapshot = await f.ReadCommittedSnapshot();

        foreach (var mode in new[] { "drain", "maintenance", "drain", "open" })
        {
            await Set(f, mode);
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(p.OperationId, Ct)).Status);
        }

        var replay = await c.Prepared.DispatchAsync(p, Ct);
        Assert.Equal(first.JobId, replay.JobId);
        Assert.Single((await f.ReadCommittedSnapshot()).Model.Jobs);
        // Redis marks a resolved receipt as confirmed (22 §4.5); that marker is the only change a resolve makes.
        var changed = snapshot.DiffTables(await f.ReadCommittedSnapshot()).Where(t => !t.Contains("application_controls", StringComparison.Ordinal) && t != "meta" && !t.StartsWith("rcpt:", StringComparison.Ordinal)); // Redis keeps the application gate in meta
        Assert.Empty(changed);

        // A receipt from a codec this binary does not know is named, not guessed at, and nothing changes.
        await ForgeReceiptVersionAsync(f, p.OperationId.Value, 2);
        await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.Prepared.ResolveAsync(p.OperationId, Ct));
        Assert.Single((await f.ReadCommittedSnapshot()).Model.Jobs);
        await ForgeReceiptVersionAsync(f, p.OperationId.Value, 1);
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(p.OperationId, Ct)).Status);
    }

    /// <summary>Rewrites the receipt's codec version (PostgreSQL, SQLite) or operation version (Redis keeps no codec field).</summary>
    static async Task ForgeReceiptVersionAsync(DriverFixture f, Guid command, int version)
    {
        switch (f.Kind)
        {
            case StoreKind.Sqlite:
                using (var conn = new SqliteConnection(f.AdminConnectionString))
                {
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "UPDATE runwick_storage_commands SET receipt_codec_version = @v WHERE command_id = @c";
                    cmd.Parameters.AddWithValue("@v", version);
                    cmd.Parameters.AddWithValue("@c", command.ToByteArray(bigEndian: true));
                    cmd.ExecuteNonQuery();
                }

                break;
            case StoreKind.Redis:
                await (await RedisServer.AdminAsync()).GetDatabase().HashSetAsync(RedisServer.Key(f.Application, "rcpt:" + command.ToString("N")), "ver", version);
                break;
            default:
                await f.ExecAsync("UPDATE runwick.storage_commands SET receipt_codec_version = @v WHERE command_id = @c", ("c", command), ("v", version));
                break;
        }
    }

    static Task<long> Count(DriverFixture f, string sql) =>
        f.Kind == StoreKind.Sqlite ? f.CountAsync(sql.Replace("runwick.", "runwick_", StringComparison.Ordinal)) : f.CountAsync(sql);

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
