using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Serialization;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>PostgreSQL's public bulk cap is 1,000 items; the 2 MiB batch envelope still bounds the bytes (spec 04 changes 2026-11-14).</summary>
public sealed class BatchCapTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static DispatchOptions Window(string key) => new() { Uniqueness = new DispatchUniqueness(null, key, TimeSpan.FromHours(1)) };

    [Fact]
    [Trait("Item", "PF2-B2")]
    public async Task One_thousand_mixed_items_commit_once_and_replay_to_the_same_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap1000"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        Assert.Equal(PayloadBounds.PostgresMaxBatchItems, c.Runtime.MaxBatchItems);
        var items = Enumerable.Range(0, 1_000).Select(i => (i % 3) switch
        {
            0 => new BatchDispatchItem(new PingJob("p" + i)),
            1 => new BatchDispatchItem(new KeyedJob("k" + i, "key" + i)),
            _ => new BatchDispatchItem(new ResourceJob("r" + i, "res" + (i % 7))),
        }).ToList();

        var prepared = c.Prepared.PrepareMany(items);
        var receipt = await c.Prepared.DispatchAsync(prepared);

        Assert.Equal(1_000, receipt.Items.Count);
        Assert.All(receipt.Items, item => Assert.NotNull(item.Receipt));
        Assert.Equal(1_000, await Jobs(f));

        var replay = await c.Prepared.DispatchAsync(prepared);
        Assert.Equal(receipt.Items, replay.Items);
        Assert.Equal(1_000, await Jobs(f));

        // The exported envelope of a 1,000-item batch imports on this store, and replays to the same receipt.
        var imported = c.Prepared.ImportPreparedBatch(prepared.Export().Span);
        Assert.Equal(receipt.Items, (await c.Prepared.DispatchAsync(imported)).Items);
        Assert.Equal(1_000, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B2")]
    public async Task One_thousand_and_one_items_are_rejected_before_any_write()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap1001"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var pre = await f.ReadCommittedSnapshot();
        var tooMany = Enumerable.Range(0, 1_001).Select(i => new BatchDispatchItem(new PingJob("x" + i))).ToList();

        var ex = await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(tooMany));

        Assert.Equal(RunwickErrorCodes.BatchTooLarge, ex.Code);
        Assert.Equal(ErrorDisposition.KnownNotApplied, ex.Disposition);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Item", "PF2-B2")]
    public async Task A_conflict_at_the_last_ordinal_rolls_back_all_one_thousand_items()
    {
        await using var f = await DriverFixture.StartAsync(Ns("capconflict"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var orig = await c.Dispatcher.DispatchAsync(new PingJob("orig"), Window("w"));
        var items = Enumerable.Range(0, 999).Select(i => new BatchDispatchItem(new PingJob("p" + i))).ToList();
        items.Add(new BatchDispatchItem(new PingJob("changed"), Window("w")));
        var pre = await f.ReadCommittedSnapshot();

        var conflict = await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchManyAsync(items));

        var failure = Assert.Single(conflict.ItemFailures);
        Assert.Equal((999, orig.JobId), (failure.Ordinal, failure.ConflictingJobId));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        var partial = await c.Dispatcher.DispatchManyAsync(items, new BatchDispatchOptions { ContinueOnConflict = true });
        Assert.Equal(orig.JobId, partial.Items[999].Failure!.ConflictingJobId);
        Assert.Equal(1 + 999, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B2")]
    public async Task The_envelope_byte_cap_still_applies_below_the_item_cap()
    {
        await using var f = await DriverFixture.StartAsync(Ns("capbytes"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var big = new string('a', 250_000);
        var huge = Enumerable.Range(0, 9).Select(i => new BatchDispatchItem(new PingJob(big + i))).ToList();

        var ex = await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(huge));

        Assert.Equal(RunwickErrorCodes.BatchEnvelopeTooLarge, ex.Code);
        Assert.Equal(0, await Jobs(f));
    }
}
