using System.Text;
using System.Text.RegularExpressions;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// Migration 0030_receipt_by_key (04 Changes 2026-11-23, receipt compaction): the command_by_id index is gone, the receipt
/// partitions have fillfactor 70, and every
/// receipt read names its retention class. Receipts written before the migration replay after it with the same result, and
/// the same command ID under the other class still conflicts, in both directions.
/// </summary>
[Trait("Item", "PF2-PG-AC")]
public sealed class ReceiptByKeyTests
{
    const string MigrationId = "0030_receipt_by_key";

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    // Under the 60 s cap of the worker operations; a replay reads the receipt before it looks at the deadline.
    static DateTimeOffset NotAfter() => DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(50)).ToUnixTimeMilliseconds());

    static Task<long> ByIdIndexes(DriverFixture f) =>
        f.CountAsync("SELECT count(*) FROM pg_class WHERE relname = 'command_by_id' AND relnamespace = 'runwick'::regnamespace");

    static Task<long> FilledTo70(DriverFixture f) =>
        f.CountAsync("""
            SELECT count(*) FROM pg_class WHERE relnamespace = 'runwick'::regnamespace
              AND relname IN ('storage_commands_ownership', 'storage_commands_durable') AND reloptions @> '{fillfactor=70}'
            """);

    /// <summary>Puts back what revision 29 had and 0030 changes: the index, the default fill factor and three function bodies (and drops what later migrations add).</summary>
    static async Task RestoreRevision29ShapeAsync(DriverFixture f)
    {
        var sql = new StringBuilder("""
            CREATE INDEX command_by_id ON runwick.storage_commands (application, command_id);
            ALTER TABLE runwick.storage_commands_ownership RESET (fillfactor);
            ALTER TABLE runwick.storage_commands_durable RESET (fillfactor);
            DROP FUNCTION runwick.finish_and_claim_v1; -- 0031, after revision 30

            """);
        var v30 = PgMigrator.Migrations.Single(m => m.Id == MigrationId).SqlFor(hooks: true);
        foreach (Match drop in Regex.Matches(v30, @"^DROP FUNCTION [^;]*;", RegexOptions.Multiline))
        {
            sql.AppendLine(drop.Value);
        }

        foreach (var (id, name) in new[] { ("0009_functions", "rw_receipt_v1"), ("0026_schedule_group_set", "rw_schedule_group_set_v1"), ("0029_dispatch_group", "dispatch_group_v1") })
        {
            var text = PgMigrator.Migrations.Single(m => m.Id == id).SqlFor(hooks: true);
            var body = Regex.Match(text, @"CREATE FUNCTION runwick\." + name + @"\(.*?\$fn\$;", RegexOptions.Singleline);
            Assert.True(body.Success, name);
            sql.AppendLine(body.Value);
        }

        await f.ExecAsync(sql.ToString());

        // The functions now are exactly revision 29's.
        var actual = (await TextAsync(f, PgMigrator.FunctionDigestSql)).Select(x => x.Split(':')).ToDictionary(x => x[0], x => x[1], StringComparer.Ordinal);
        var expected = PgMigrator.ExpectedFunctions(PgMigrator.Migrations.Where(m => m.TargetRevision <= 29), hooks: true);
        Assert.Empty(PgMigrator.FunctionProblems(expected, actual, PgMigrator.AllFunctionNames));
        Assert.Equal((1L, 0L), (await ByIdIndexes(f), await FilledTo70(f)));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "RCPT-01")]
    public async Task Receipts_written_at_revision_29_replay_after_the_upgrade_to_30()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rcpt01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await RestoreRevision29ShapeAsync(f);
        var na = NotAfter();

        // Populate with the revision 29 functions: a durable Dispatch, an ownership ReserveAndBegin, a durable Finish, and more jobs.
        var p = c.Prepared.Prepare(new PingJob("kept"));
        var dispatched = await SendAsync(c, p);
        Assert.Equal((StoreStatus.Applied, false), (dispatched.Status, dispatched.Replayed));
        var claimId = Guid.CreateVersion7();
        var claimed = await c.ReserveAndBeginAsync(1, commandId: claimId, notAfter: na);
        Assert.Equal((StoreStatus.Applied, false), (claimed.Status, claimed.Replayed));
        var lease = claimed.Value!.Outcomes.Single().Begun!.Lease.Handle;
        var finishId = Guid.CreateVersion7();
        var finished = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: finishId, notAfter: na);
        Assert.Equal((StoreStatus.Applied, false), (finished.Status, finished.Replayed));
        for (var i = 0; i < 40; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("fill-" + i), cancellationToken: Ct);
        }

        // Replay before the upgrade.
        Assert.True((await SendAsync(c, p)).Replayed);

        // Mark the store as revision 29 (0030 not applied), then upgrade it the way the migrator does.
        var later = PgMigrator.Migrations.Where(m => m.TargetRevision > 29).Select(m => m.Id).ToArray();
        await f.ExecAsync($"DELETE FROM runwick.schema_migrations WHERE migration_id IN ({string.Join(", ", later.Select(id => "'" + id + "'"))})");
        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = 29");
        var receiptsBefore = await f.CountAsync("SELECT count(*) FROM runwick.storage_commands");
        var result = await new PgMigrator(f.Server.ConnectionStringFor(f.DatabaseName, pooling: false)) { FaultHooks = true }.RunAsync(Ct);
        Assert.Equal([.. later.SelectMany(id => new[] { id + ":expand", id + ":validate", id + ":activate" })], result.AppliedPhases);
        Assert.Equal(PgMigrator.CurrentRevision, result.SchemaRevision);
        Assert.Equal((0L, 2L), (await ByIdIndexes(f), await FilledTo70(f)));
        Assert.Equal(receiptsBefore, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands"));
        var jobs = await Jobs(f);

        // Replay after the upgrade: the same decisions, nothing applied again.
        var d2 = await SendAsync(c, p);
        Assert.Equal((StoreStatus.Applied, true, dispatched.Value!.JobId, dispatched.DecidedAt), (d2.Status, d2.Replayed, d2.Value!.JobId, d2.DecidedAt));
        var c2 = await c.ReserveAndBeginAsync(1, commandId: claimId, notAfter: na);
        Assert.Equal((StoreStatus.Applied, true, claimed.DecidedAt), (c2.Status, c2.Replayed, c2.DecidedAt));
        var o = c2.Value!.Outcomes.Single();
        Assert.Equal((lease.JobId, lease.AttemptId), (o.JobId, o.Begun!.Lease.Handle.AttemptId));
        var f2 = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: finishId, notAfter: na);
        Assert.Equal((StoreStatus.Applied, true, finished.Value, finished.DecidedAt), (f2.Status, f2.Replayed, f2.Value, f2.DecidedAt));
        Assert.Equal(jobs, await Jobs(f));

        // Lookups by ID without a class (management) find receipts of either class.
        Assert.Equal(p.NotAfter, await c.Store.CommandNotAfterAsync(c.Application, p.OperationId.Value, Ct));
        Assert.Equal(na, await c.Store.CommandNotAfterAsync(c.Application, claimId, Ct));

        // A client that opens now accepts the store's functions, and new work runs.
        var fresh = await f.OpenClient(configure: r => TestJobs.Register(r));
        await fresh.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct);
        Assert.Equal(jobs + 1, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "RCPT-02")]
    public async Task The_same_id_under_the_other_retention_class_conflicts_in_both_directions()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rcpt02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        Assert.Equal((0L, 2L), (await ByIdIndexes(f), await FilledTo70(f)));
        var na = NotAfter();

        var p = c.Prepared.Prepare(new PingJob("a"));
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, p)).Status);
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);
        var claimId = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAndBeginAsync(1, commandId: claimId, notAfter: na)).Status);
        var receipts = await f.CountAsync("SELECT count(*) FROM runwick.storage_commands");
        var before = await f.ReadCommittedSnapshot();

        // Durable receipt first, ownership command second.
        var ownership = await c.ReserveAndBeginAsync(1, commandId: p.OperationId.Value, notAfter: na);
        Assert.Equal((StoreStatus.CommandConflict, StoreEffect.ObservedNoEffect), (ownership.Status, ownership.Effect));
        var reserve = await c.ReserveAsync(commandId: p.OperationId.Value, notAfter: na);
        Assert.Equal(StoreStatus.CommandConflict, reserve.Status);

        // Ownership receipt first, durable command second.
        var durable = await SendAsync(c, c.Prepared.Prepare(new PingJob("c")), commandId: claimId, notAfter: na);
        Assert.Equal((StoreStatus.CommandConflict, StoreEffect.ObservedNoEffect), (durable.Status, durable.Effect));

        Assert.Equal(receipts, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands"));
        Assert.Empty(before.DiffTables(await f.ReadCommittedSnapshot()));
    }
}
