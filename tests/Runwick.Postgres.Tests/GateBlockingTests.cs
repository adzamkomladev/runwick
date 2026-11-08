using System.Diagnostics;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>PGX-08 (04 §8.1, §14): a caller transaction left open by StageAsync blocks gate changes and the validate phase.</summary>
[Trait("Item", "SP-028")]
public sealed class GateBlockingTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    [Trait("Case", "PGX-08")]
    public async Task A_StageAsync_transaction_left_open_blocks_the_application_gate_change_and_the_validate_phase_with_bounded_timeouts_and_no_partial_commit()
    {
        await using var f = await DriverFixture.StartAsync("pgx08_" + Guid.NewGuid().ToString("N")[..8]);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync(Ct);
        await Staging(c).StageAsync(new PingJob("staged"), tx); // takes the store gate (FOR SHARE) and stays open

        var revision = await f.CountAsync("SELECT schema_revision FROM runwick.store_metadata");
        var metadataRevision = await f.CountAsync("SELECT revision FROM runwick.store_metadata");

        // (a) The application-gate change gives up at lock_timeout (5 s); the store stays in its last recorded mode.
        var clock = Stopwatch.StartNew();
        var gate = await Assert.ThrowsAsync<PostgresMigrationException>(() => RunwickPostgresMigrations.SetAdmissionModeAsync(f.AdminConnectionString, f.Application, "drain", "ops", Ct));
        Assert.Equal("migration_lock_timeout", gate.Code);
        Assert.InRange(clock.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(15));
        var status = await RunwickPostgresMigrations.GetStatusAsync(f.AdminConnectionString, Ct);
        Assert.Equal(("open", 1L), (status.Applications.Single().AdmissionMode, status.Applications.Single().GateRevision));

        // (b) The validate phase of the last migration, run again for a store whose validate and activate were not recorded,
        // waits for the same row lock and fails the same way, recording the failure and changing nothing else.
        var last = PgMigrator.Migrations[^1].Id;
        await f.ExecAsync("DELETE FROM runwick.schema_migrations WHERE migration_id = @id AND phase IN ('validate', 'activate')", ("id", last));
        clock.Restart();
        var validate = await Assert.ThrowsAsync<PostgresMigrationException>(() => new PgMigrator(f.AdminConnectionString) { FaultHooks = true }.RunAsync(Ct));
        Assert.Equal("migration_lock_timeout", validate.Code);
        Assert.InRange(clock.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(15));
        Assert.Equal(revision, await f.CountAsync("SELECT schema_revision FROM runwick.store_metadata"));
        Assert.Equal(metadataRevision, await f.CountAsync("SELECT revision FROM runwick.store_metadata"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.store_metadata WHERE migration_mode = 'normal'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.schema_migrations WHERE migration_id = @id AND phase = 'validate' AND state = 'applied'", ("id", last)));
        Assert.Equal(0, await Jobs(f));

        // (c) The caller commits; the validate phase then completes and the staged job is whole.
        await tx.CommitAsync(Ct);
        var rerun = await new PgMigrator(f.AdminConnectionString) { FaultHooks = true }.RunAsync(Ct);
        Assert.Contains(last + ":validate", rerun.AppliedPhases);
        Assert.Equal(revision, await f.CountAsync("SELECT schema_revision FROM runwick.store_metadata"));
        Assert.Equal(1, await Jobs(f));
        Assert.True((await RunwickPostgresMigrations.SetAdmissionModeAsync(f.AdminConnectionString, f.Application, "drain", "ops", Ct)).Single().Changed);
    }
}
