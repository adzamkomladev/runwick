using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>BATCH-01..03 (inferred, 07 §13): batch atomicity, per-ordinal receipts, conflicts.</summary>
public sealed class BatchTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static DispatchOptions Window(string key) => new() { Uniqueness = new DispatchUniqueness(null, key, TimeSpan.FromHours(1)) };

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "BATCH-01")]
    public async Task Batch_is_all_or_nothing_and_bounds_are_enforced_before_writes()
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var items = Enumerable.Range(0, 100).Select(i => (i % 3) switch
        {
            0 => new BatchDispatchItem(new PingJob("p" + i)),
            1 => new BatchDispatchItem(new KeyedJob("k" + i, "key" + i)),
            _ => new BatchDispatchItem(new ResourceJob("r" + i, "res" + (i % 7))),
        }).ToList();
        var prepared = c.Prepared.PrepareMany(items);
        var pre = await f.ReadCommittedSnapshot();

        // Throw after every mutation step: each leaves the pre-state; then the batch commits all 100 at once.
        var steps = 0;
        for (var step = 1; step < 2000; step++)
        {
            var h = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.DispatchMany, Step = step });
            var ex = await Record.ExceptionAsync(() => c.Prepared.DispatchAsync(prepared));
            f.Faults.DisarmAll();
            if (h.Fired == 0)
            {
                Assert.Null(ex);
                break;
            }

            Assert.IsType<StoreUnavailableException>(ex);
            Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
            steps = step;
        }

        Assert.True(steps >= 1);
        var committed = await f.ReadCommittedSnapshot();
        Assert.Equal(100, await Jobs(f));
        Assert.Empty(SnapshotInvariants.Violations(committed.Model));

        // Invalid input: one bad item, one over the cap, an envelope over 2 MiB — rejected before any write.
        var bad = items.Take(10).Append(new BatchDispatchItem(new KeyedJob("bad", ""))).ToList();
        var invalid = Assert.Throws<DispatchValidationException>(() => { c.Prepared.PrepareMany(bad); });
        Assert.Equal(ErrorDisposition.KnownNotApplied, invalid.Disposition);
        Assert.Equal(10, Assert.Single(invalid.ItemFailures).Ordinal);
        await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(bad));

        var tooMany = Enumerable.Range(0, c.Runtime.MaxBatchItems + 1).Select(i => new BatchDispatchItem(new PingJob("x" + i))).ToList();
        Assert.Equal(RunwickErrorCodes.BatchTooLarge, (await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(tooMany))).Code);

        var big = new string('a', 250_000);
        var huge = Enumerable.Range(0, 9).Select(i => new BatchDispatchItem(new PingJob(big + i))).ToList();
        Assert.Equal(RunwickErrorCodes.BatchEnvelopeTooLarge,
            (await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(huge))).Code);
        Assert.Empty(committed.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "BATCH-02")]
    public async Task Batch_receipt_has_one_result_per_ordinal_and_replays_identically()
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var active = await c.Dispatcher.DispatchAsync(new KeyedJob("ka", "ka"));
        var windowed = await c.Dispatcher.DispatchAsync(new PingJob("w1"), Window("w"));

        // A process whose MaxExecutions default differs: the window item reports PolicyDrift.
        var c5 = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions: 5));
        var prepared = c5.Prepared.PrepareMany(
        [
            new BatchDispatchItem(new PingJob("new")),
            new BatchDispatchItem(new KeyedJob("other", "ka")),
            new BatchDispatchItem(new PingJob("w1"), Window("w")),
        ]);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(prepared.OperationId.Value));
        var unknown = await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => c5.Prepared.DispatchAsync(prepared));
        Assert.Equal(1, drop.Fired);
        Assert.Same(prepared, unknown.PreparedBatch);

        var first = await c5.Prepared.DispatchAsync(unknown.PreparedBatch!);
        var second = await c5.Prepared.DispatchAsync(prepared);
        Assert.Equal(first.Items, second.Items);
        Assert.Equal([0, 1, 2], first.Items.Select(i => i.Ordinal));
        Assert.All(first.Items, i => Assert.Null(i.Failure));
        var r = first.Items.Select(i => i.Receipt!).ToList();
        Assert.Equal((DispatchDisposition.Created, DuplicateMatch.None), (r[0].Disposition, r[0].DuplicateMatch));
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Active, active.JobId), (r[1].Disposition, r[1].DuplicateMatch, r[1].JobId));
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, true, windowed.JobId), (r[2].Disposition, r[2].DuplicateMatch, r[2].PolicyDrift, r[2].JobId));

        Assert.Equal(1, await Receipts(f, prepared.OperationId.Value));
        var resolution = await c5.Prepared.ResolveAsync(prepared.OperationId);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal(first.Items, resolution.BatchReceipt!.Items);
        Assert.Equal(3, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "BATCH-03")]
    public async Task Batch_conflicts_with_and_without_continue_and_concurrent_batches()
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var orig = await c.Dispatcher.DispatchAsync(new PingJob("orig"), Window("w"));
        List<BatchDispatchItem> items =
        [
            new(new KeyedJob("d1", "d")),
            new(new KeyedJob("d2", "d")),
            new(new PingJob("changed"), Window("w")),
        ];
        var pre = await f.ReadCommittedSnapshot();

        // Default: the whole batch fails.
        var conflict = await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchManyAsync(items));
        var failure = Assert.Single(conflict.ItemFailures);
        Assert.Equal((2, orig.JobId), (failure.Ordinal, failure.ConflictingJobId));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // ContinueOnConflict: the conflicting ordinal fails with no rows; duplicates within the batch resolve to one job.
        var partial = await c.Dispatcher.DispatchManyAsync(items, new BatchDispatchOptions { ContinueOnConflict = true });
        var d1 = partial.Items[0].Receipt!;
        var d2 = partial.Items[1].Receipt!;
        Assert.Equal(DispatchDisposition.Created, d1.Disposition);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Active, d1.JobId), (d2.Disposition, d2.DuplicateMatch, d2.JobId));
        Assert.Null(partial.Items[2].Receipt);
        Assert.Equal(orig.JobId, partial.Items[2].Failure!.ConflictingJobId);
        Assert.Equal(2, await Jobs(f));

        // Concurrent batches sharing keys in opposite orders never deadlock.
        var b = await f.OpenClient(configure: r => TestJobs.Register(r));
        var deadlocksBefore = Deadlocks(c) + Deadlocks(b);
        const int iterations = 30;
        for (var i = 0; i < iterations; i++)
        {
            var x = c.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new KeyedJob($"a1-{i}", $"s{i}")), new BatchDispatchItem(new KeyedJob($"a2-{i}", $"t{i}"))]);
            var y = b.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new KeyedJob($"b1-{i}", $"t{i}")), new BatchDispatchItem(new KeyedJob($"b2-{i}", $"s{i}"))]);
            var both = await WithTimeout(Task.WhenAll(x, y));
            Assert.All(both, batch => Assert.All(batch.Items, item => Assert.NotNull(item.Receipt)));
        }

        Assert.Equal(deadlocksBefore, Deadlocks(c) + Deadlocks(b));
        Assert.Equal(2 + 2 * iterations, await Jobs(f));
    }
}
