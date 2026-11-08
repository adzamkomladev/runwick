using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// UPG-01 … UPG-10 on PostgreSQL (23 §11) and the scheduler drain case. The store-agnostic admission and identity cases are in
/// Runwick.Contract.Tests; the older INIT-01, PGX-01 and RATE tests that also carry UPG evidence are named in 23 §11.
/// </summary>
[Trait("Item", "SP-026")]
public sealed class UpgradeTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly string[] AllPhases = [.. PgMigrator.Migrations.SelectMany(m => new[] { "expand", "validate", "activate" }.Select(p => m.Id + ":" + p))];

    static async Task<(DriverFixture F, TestClient C)> StartAsync(string name)
    {
        var f = await DriverFixture.StartAsync(Ns(name));
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    static async Task<PostgresMigrationException> CodeAsync(Func<Task> call) => await Assert.ThrowsAsync<PostgresMigrationException>(call);

    static Task<IReadOnlyList<AdmissionChange>> Admission(DriverFixture f, string mode) =>
        RunwickPostgresMigrations.SetAdmissionModeAsync(f.AdminConnectionString, f.Application, mode, "ops", Ct);

    // ------------------------------------------------------------------ UPG-01

    [Fact]
    [Trait("Case", "UPG-01")]
    public async Task A_lost_migrator_session_hands_over_to_the_waiting_migrator_and_each_phase_is_recorded_once()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync(Ns("upg01"));
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            var admin = server.ConnectionStringFor(db, "rw:harness:admin", pooling: false);
            var holding = new TaskCompletionSource();
            var secondWaiting = new TaskCompletionSource();
            var first = new PgMigrator(cs, async (phase, conn) =>
            {
                if (phase == "0003_outbox:validate")
                {
                    holding.TrySetResult();
                    await secondWaiting.Task;
                    await Task.Delay(700, Ct);
                    await StoreServer.ExecAsync(admin, $"SELECT pg_terminate_backend({conn.ProcessID})");
                }
            }).RunAsync(Ct);
            await holding.Task.WaitAsync(Wait, Ct);
            var second = RunwickPostgresMigrations.MigrateAsync(cs, Ct);
            secondWaiting.SetResult();

            Assert.Equal("migration_connection_lost", (await Assert.ThrowsAsync<PostgresMigrationException>(() => first)).Code);
            var resumed = await second.WaitAsync(Wait, Ct);
            Assert.Equal(AllPhases.SkipWhile(p => p != "0003_outbox:validate").Skip(1), resumed.AppliedPhases);

            // Every (migration, phase) is applied exactly once, and nothing diverged.
            Assert.Equal(4L * PgMigrator.Migrations.Count, await Count(admin, "SELECT count(*) FROM runwick.schema_migrations WHERE state = 'applied'"));
            Assert.Equal(PgMigrator.CurrentRevision, await Count(admin, "SELECT schema_revision FROM runwick.store_metadata"));
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    static async Task<long> Count(string cs, string sql)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(Ct);
        using var cmd = new NpgsqlCommand(sql, conn);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    // ------------------------------------------------------------------ UPG-02

    [Fact]
    [Trait("Case", "UPG-02")]
    public async Task A_binary_runs_only_on_its_own_revision_and_expand_mode_admits_nobody()
    {
        var (f, c) = await StartAsync("upg02");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var before = await f.ReadCommittedSnapshot();

        async Task AssertRefused(string status, string code)
        {
            // Creation and settlement paths alike.
            foreach (var reply in new[] { (await c.ReserveAsync()).Status.ToString() + "/" + (await c.ReserveAsync()).Error?.Code,
                                          (await c.RenewAsync(lease)).Status + "/" + (await c.RenewAsync(lease)).Error?.Code,
                                          (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status + "/" + (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Error?.Code })
            {
                Assert.Equal(status + "/" + code, reply);
            }

            await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("x"), cancellationToken: Ct));
            List<string> changed = [.. before.DiffTables(await f.ReadCommittedSnapshot()).Where(t => t != "store_metadata")];
            Assert.Empty(changed);
        }

        // New schema, old binary: the binary's revision is one behind the store's.
        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = schema_revision + 1");
        await AssertRefused("UnsupportedVersion", "schema_incompatible");
        // Old schema, new binary.
        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = schema_revision - 2");
        await AssertRefused("UnsupportedVersion", "schema_incompatible");
        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = schema_revision + 1");
        // Expand mode with no declared compatible binary (the manifest list is empty): nobody runs.
        await f.ExecAsync("UPDATE runwick.store_metadata SET migration_mode = 'expand'");
        await AssertRefused("AdmissionClosed", "store_maintenance");
        await f.ExecAsync("UPDATE runwick.store_metadata SET migration_mode = 'normal'");

        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
    }

    // ------------------------------------------------------------------ UPG-03

    [Fact]
    [Trait("Case", "UPG-03")]
    public async Task A_caller_transaction_holding_the_gate_delays_the_change_and_nothing_half_commits()
    {
        var (f, c) = await StartAsync("upg03");
        await using var _ = f;
        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await Staging(c).StageAsync(new PingJob("staged"), tx);

        // (a) The caller keeps its transaction past the 5 s lock timeout: the change gives up, nothing changed.
        var clock = Stopwatch.StartNew();
        var timeout = await CodeAsync(() => Admission(f, "drain"));
        Assert.Equal("migration_lock_timeout", timeout.Code);
        Assert.InRange(clock.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(15));
        var status = await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal(("open", 1L), (status.Applications.Single().AdmissionMode, status.Applications.Single().GateRevision));
        Assert.Equal(0, await Jobs(f));

        // (b) The caller commits within the wait: the change then applies, and the staged job is whole under the old mode.
        var change = Task.Run(() => Admission(f, "drain"), Ct);
        await Task.Delay(1000, Ct);
        Assert.False(change.IsCompleted);
        await tx.CommitAsync(Ct);
        Assert.True((await change.WaitAsync(Wait, Ct)).Single().Changed);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'dispatched'"));
        await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("late"), cancellationToken: Ct));
    }

    // ------------------------------------------------------------------ UPG-05

    [Fact]
    [Trait("Case", "UPG-05")]
    public async Task A_failed_validate_keeps_the_last_revision_and_a_rerun_after_repair_completes()
    {
        var server = await StoreServer.SharedAsync();
        var ns = Ns("upg05");
        var db = await server.CreateEmptyDatabaseAsync(ns);
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            var admin = server.ConnectionStringFor(db, "rw:harness:admin", pooling: false);
            var last = PgMigrator.Migrations[^1].Id;
            string? damaged = null;
            var migrator = new PgMigrator(cs, async (phase, conn) =>
            {
                if (phase == last + ":expand")
                {
                    using var pick = new NpgsqlCommand("""
                        SELECT c.relname FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indexrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'runwick' AND NOT i.indisprimary AND NOT i.indisunique ORDER BY c.relname LIMIT 1
                        """, conn);
                    damaged = (string)(await pick.ExecuteScalarAsync(Ct))!;
                    using var damage = new NpgsqlCommand($"UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'runwick.{damaged}'::regclass", conn);
                    await damage.ExecuteNonQueryAsync(Ct);
                }
            });
            var failure = await CodeAsync(() => migrator.RunAsync(Ct));
            Assert.Equal("schema_validation_failed", failure.Code);
            Assert.Contains("invalid index " + damaged, failure.Message, StringComparison.Ordinal);

            // The last recorded safe mode: previous revision, normal, a failed ledger row, and a client of this binary refuses the store.
            var revision = PgMigrator.CurrentRevision - 1;
            Assert.Equal(revision, await Count(admin, "SELECT schema_revision FROM runwick.store_metadata"));
            Assert.Equal(1, await Count(admin, $"SELECT count(*) FROM runwick.store_metadata WHERE migration_mode = 'normal'"));
            Assert.Equal(1, await Count(admin, $"SELECT count(*) FROM runwick.schema_migrations WHERE migration_id = '{last}' AND phase = 'validate' AND state = 'failed'"));
            Assert.Equal(0, await Count(admin, $"SELECT count(*) FROM runwick.schema_migrations WHERE migration_id = '{last}' AND phase = 'validate' AND state = 'applied'"));
            await using (var client = new BareClient(server, db, ns))
            {
                await Assert.ThrowsAsync<StoreIncompatibleException>(client.EnsureReadyAsync);
            }

            // Repair, rerun: continues from the ledger, the failed row becomes applied.
            await StoreServer.ExecAsync(admin, $"UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'runwick.{damaged}'::regclass");
            var rerun = await RunwickPostgresMigrations.MigrateAsync(cs, Ct);
            Assert.Equal([last + ":validate", last + ":activate"], rerun.AppliedPhases);
            Assert.Equal(PgMigrator.CurrentRevision, await Count(admin, "SELECT schema_revision FROM runwick.store_metadata"));
            Assert.Equal(0, await Count(admin, "SELECT count(*) FROM runwick.schema_migrations WHERE state = 'failed'"));
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    /// <summary>A client against a database that is not a template clone.</summary>
    sealed class BareClient(StoreServer server, string database, string application) : IAsyncDisposable
    {
        readonly ServiceProvider _provider = Build(server, database, application);

        static ServiceProvider Build(StoreServer server, string database, string application)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddRunwick(r => r.Application(application).UsePostgres(server.ConnectionStringFor(database, "rw:local:work")));
            return services.BuildServiceProvider();
        }

        public Task EnsureReadyAsync() => _provider.GetRequiredService<Runwick.Hosting.RunwickRuntime>().EnsureReadyAsync(CancellationToken.None);

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    // ------------------------------------------------------------------ UPG-06

    [Fact]
    [Trait("Case", "UPG-06")]
    public async Task A_migrator_refuses_a_store_that_is_ahead_of_it_and_status_shows_the_floor()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync(Ns("upg06"));
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            await RunwickPostgresMigrations.MigrateAsync(cs, Ct);
            await StoreServer.ExecAsync(cs, """
                INSERT INTO runwick.schema_migrations (migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at)
                VALUES ('9999_future', 'expand', sha256('x'), 9999, 'applied', 'runwick/future', clock_timestamp(), clock_timestamp());
                UPDATE runwick.store_metadata SET schema_revision = 9999;
                """);
            var refused = await CodeAsync(() => RunwickPostgresMigrations.MigrateAsync(cs, Ct));
            Assert.Equal("migration_unknown", refused.Code);

            var status = await RunwickPostgresMigrations.GetStatusAsync(cs, Ct);
            Assert.Equal((9999L, PgMigrator.CurrentRevision), (status.StoreRevision, status.BinaryRevision));
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    /// <summary>Reserves until a fenced job comes up, finishing the unfenced ones in front of it.</summary>
    static async Task<ReservedJob> ReserveFencedAsync(TestClient c)
    {
        for (var i = 0; i < 6; i++)
        {
            var reserved = (await c.ReserveAsync()).Value!;
            if (reserved.Fences.Count > 0)
            {
                return reserved;
            }

            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(reserved.Lease.Handle)).Reply.Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(reserved.Lease.Handle, AttemptOutcome.Succeeded)).Status);
        }

        throw new InvalidOperationException("no fenced job came up");
    }

    // ------------------------------------------------------------------ UPG-07

    [Fact]
    [Trait("Case", "UPG-07")]
    public async Task Old_receipts_resolve_without_reapplication_and_an_unknown_codec_is_explicit()
    {
        var (f, c) = await StartAsync("upg07");
        await using var _ = f;
        var p = c.Prepared.Prepare(new PingJob("x"));
        var first = await SendAsync(c, p);
        Assert.Equal(StoreStatus.Applied, first.Status);
        var snapshot = await f.ReadCommittedSnapshot();

        foreach (var mode in new[] { "drain", "maintenance", "drain", "open" })
        {
            await Admission(f, mode);
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(p.OperationId, Ct)).Status);
        }

        var replay = await SendAsync(c, p);
        Assert.True(replay.Replayed);
        Assert.Equal(1, await Jobs(f));
        List<string> changed = [.. snapshot.DiffTables(await f.ReadCommittedSnapshot()).Where(t => !t.Contains("application_controls", StringComparison.Ordinal))];
        Assert.Empty(changed);

        // A receipt from a codec this binary does not know is named, not guessed at, and nothing changes.
        await f.ExecAsync("UPDATE runwick.storage_commands SET receipt_codec_version = 2 WHERE command_id = @c", ("c", p.OperationId.Value));
        await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.Prepared.ResolveAsync(p.OperationId, Ct));
        Assert.Equal(1, await Jobs(f));
        await f.ExecAsync("UPDATE runwick.storage_commands SET receipt_codec_version = 1 WHERE command_id = @c", ("c", p.OperationId.Value));
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(p.OperationId, Ct)).Status);
    }

    // ------------------------------------------------------------------ UPG-08

    [Fact]
    [Trait("Case", "UPG-08")]
    public async Task Expired_presence_and_leases_are_reported_never_taken_as_proof()
    {
        var (f, c) = await StartAsync("upg08");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.True((await c.Store.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready", Environment.ProcessId,
            "host", "build", ["default"], new Dictionary<string, int>(), c.Contracts, TimeSpan.FromSeconds(3)), Ct)).Accepted);

        // Drain does not wait for the running handler.
        var clock = Stopwatch.StartNew();
        await Admission(f, "drain");
        await Admission(f, "maintenance");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2));
        var live = await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal((1, 1, 0), (live.OwnedLeases, live.WorkerPresences, live.ExpiredWorkerPresences));
        Assert.StartsWith("unverified", live.Quiescence, StringComparison.Ordinal);

        // Live rows refuse the identity change; once they expire it is allowed, and the lease row is still there.
        var refused = await CodeAsync(() => RunwickPostgresMigrations.ReassignStoreIdAsync(f.AdminConnectionString, Ct));
        Assert.Equal("store_not_quiescent", refused.Code);
        await Task.Delay(3500, Ct);
        var expired = await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal((1, 1, 1), (expired.OwnedLeases, expired.WorkerPresences, expired.ExpiredWorkerPresences));
        var changed = await RunwickPostgresMigrations.ReassignStoreIdAsync(f.AdminConnectionString, Ct);
        Assert.NotEqual(changed.OldStoreId, changed.NewStoreId);
        Assert.Equal(1, (await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct)).OwnedLeases);
        // The holder, still bound to the old identity, is refused rather than trusted (no-effect).
        var late = await c.FinishAsync(lease, AttemptOutcome.Succeeded);
        Assert.NotEqual(StoreStatus.Applied, late.Status);
    }

    // ------------------------------------------------------------------ UPG-09

    [Fact]
    [Trait("Case", "UPG-09")]
    public async Task A_copy_needs_a_new_identity_and_a_restore_loses_what_the_snapshot_never_had()
    {
        var (f, c) = await StartAsync("upg09");
        await using var _ = f;
        var server = f.Server;
        var applied = c.Prepared.Prepare(new PingJob("before"));
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, applied)).Status);
        var original = (await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct)).StoreId;

        // The snapshot (a database copy), taken with no client attached.
        await c.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
        var restored = "rw_" + Ns("upg09r");
        var copy = "rw_" + Ns("upg09c");
        await StoreServer.ExecAsync(server.ConnectionStringFor("postgres", pooling: false), $"CREATE DATABASE {restored} TEMPLATE {f.DatabaseName}");
        await StoreServer.ExecAsync(server.ConnectionStringFor("postgres", pooling: false), $"CREATE DATABASE {copy} TEMPLATE {f.DatabaseName}");
        try
        {
            // After the snapshot the original acknowledges a command and issues fences 1 then 2.
            var c1 = await f.OpenClient(configure: r => TestJobs.Register(r));
            var lost = c1.Prepared.Prepare(new PingJob("after"));
            Assert.Equal(StoreStatus.Applied, (await SendAsync(c1, lost)).Status);
            await c1.Dispatcher.DispatchAsync(new ResourceJob("r", "res"), cancellationToken: Ct);
            var first = await ReserveFencedAsync(c1);
            await c1.ReleaseAsync(first.Lease.Handle);
            var second = await ReserveFencedAsync(c1);
            Assert.True(second.Fences.Single().Value > first.Fences.Single().Value);
            var fenceSeen = second.Fences.Single().Value;

            // (a) The copy has the same identity until the operator changes it: indistinguishable from the original.
            var copyCs = server.ConnectionStringFor(copy, pooling: false);
            Assert.Equal(original, (await RunwickPostgresMigrations.GetStatusAsync(copyCs, Ct)).StoreId);
            var app = f.Application;
            await RunwickPostgresMigrations.SetAdmissionModeAsync(copyCs, app, "drain", "ops", Ct);
            await RunwickPostgresMigrations.SetAdmissionModeAsync(copyCs, app, "maintenance", "ops", Ct);
            var id = await RunwickPostgresMigrations.ReassignStoreIdAsync(copyCs, Ct);
            Assert.Equal(original, id.OldStoreId);
            await RunwickPostgresMigrations.SetAdmissionModeAsync(copyCs, app, "open", "ops", Ct);
            await using (var clone = await TestClient.CreateAsync(server.ConnectionStringFor(copy, "rw:clone"), app, new HarnessFaultInjector(), r => TestJobs.Register(r)))
            {
                // The envelope prepared for the original is refused by the clone; a fresh one works.
                await Assert.ThrowsAsync<StoreIncompatibleException>(() => clone.Prepared.DispatchAsync(applied, Ct));
                Assert.Equal(DispatchDisposition.Created, (await clone.Dispatcher.DispatchAsync(new PingJob("clone"), cancellationToken: Ct)).Disposition);
            }

            // (b) The restore (same identity): the acknowledged command is unknown, resending creates it again, the fence value repeats.
            await using var back = await TestClient.CreateAsync(server.ConnectionStringFor(restored, "rw:back"), app, new HarnessFaultInjector(), r => TestJobs.Register(r));
            Assert.NotEqual(DispatchResolutionStatus.Confirmed, (await back.Prepared.ResolveAsync(lost.OperationId, Ct)).Status);
            Assert.Equal(DispatchDisposition.Created, (await back.Prepared.DispatchAsync(lost, Ct)).Disposition);
            await back.Dispatcher.DispatchAsync(new ResourceJob("r", "res"), cancellationToken: Ct);
            var repeated = (await ReserveFencedAsync(back)).Fences.Single().Value;
            Assert.True(repeated <= fenceSeen, $"the restored counter issued {repeated}, the original had already issued {fenceSeen}");
        }
        finally
        {
            await server.DropDatabaseAsync(restored);
            await server.DropDatabaseAsync(copy);
        }
    }

    // ------------------------------------------------------------------ UPG-10

    [Fact]
    [Trait("Case", "UPG-10")]
    public async Task Admission_changes_during_renewal_load_stay_bounded()
    {
        var (f, c) = await StartAsync("upg10");
        await using var _ = f;
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 8; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        for (var i = 0; i < 8; i++)
        {
            var reserved = (await c.ReserveAsync()).Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(reserved)).Reply.Status);
            leases.Add(reserved);
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

        var slowestChange = TimeSpan.Zero;
        for (var i = 0; i < 10; i++)
        {
            foreach (var mode in new[] { "drain", "open" })
            {
                var t = Stopwatch.StartNew();
                await Admission(f, mode);
                await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
                slowestChange = t.Elapsed > slowestChange ? t.Elapsed : slowestChange;
            }
        }

        await stop.CancelAsync();
        await renew;
        Assert.Empty(notApplied);
        Assert.True(renewals > 10, $"only {renewals} renewals ran");
        // The renewal margin is a third of the 30 s lease (06); the bound used here is the 2 s statement budget.
        Assert.True(worst < TimeSpan.FromSeconds(2), $"slowest RenewMany {worst}");
        Assert.True(slowestChange < TimeSpan.FromSeconds(5), $"slowest admission change {slowestChange}");
    }

    // ------------------------------------------------------------------ scheduler

    [Fact]
    [Trait("Case", "SCHED-13")]
    public async Task A_drain_pauses_materialization_without_moving_the_cursor()
    {
        await using var f = await DriverFixture.StartAsync(Ns("upgsch"));
        var c = await ScheduleStoreTests.OpenAsync(f);
        Assert.Equal(StoreStatus.Applied, (await ScheduleStoreTests.RegisterAsync(c, "nightly")).Status);
        await ScheduleStoreTests.RewindAsync(f, "nightly", 3);
        var (before, _, plan) = await ScheduleStoreTests.PlanAsync(c, "nightly");
        Assert.NotNull(plan);

        await Admission(f, "drain");
        var refused = await ScheduleStoreTests.SendAsync(c, plan);
        Assert.Equal((StoreStatus.AdmissionClosed, "application_draining"), (refused.Status, refused.Error!.Code));
        var held = await ScheduleStoreTests.ReadAsync(c, "nightly");
        Assert.Equal((before.EvaluatedThrough, before.CursorRevision), (held.EvaluatedThrough, held.CursorRevision));

        await Admission(f, "open");
        Assert.Equal(StoreStatus.Applied, (await ScheduleStoreTests.SendAsync(c, plan)).Status);
        Assert.True((await ScheduleStoreTests.ReadAsync(c, "nightly")).EvaluatedThrough > before.EvaluatedThrough);
    }
}
