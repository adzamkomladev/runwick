using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// BATCH-01..03 (07 §13) and UNI-02, UNI-03 (07 §7) as theories over <see cref="StoreKind"/>, ported from the PostgreSQL suite. Fault points exist on
/// PostgreSQL and SQLite; Redis uses its write-prefix seam where a case needs a fault (see <see cref="Cx.LoseReply"/>).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class BatchUniquenessContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static DispatchOptions Window(string key, TimeSpan? window = null, string? active = null) =>
        new() { Uniqueness = new DispatchUniqueness(active, key, window ?? TimeSpan.FromHours(1)) };

    static DispatchOptions Active(string key) => new() { Uniqueness = new DispatchUniqueness(key, null, null) };

    static List<BatchDispatchItem> HundredItems(string prefix = "") => [.. Enumerable.Range(0, 100).Select(i => (i % 3) switch
    {
        0 => new BatchDispatchItem(new PingJob(prefix + "p" + i)),
        1 => new BatchDispatchItem(new KeyedJob(prefix + "k" + i, prefix + "key" + i)),
        _ => new BatchDispatchItem(new ResourceJob(prefix + "r" + i, prefix + "res" + (i % 7))),
    })];

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "BATCH-01")]
    public async Task Batch_is_all_or_nothing_and_bounds_are_enforced_before_writes(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch01"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var items = HundredItems();
        var prepared = c.Prepared.PrepareMany(items);

        if (kind == StoreKind.Redis)
        {
            // No fault points on Redis: stop the client after n writes of the stored intent (22 §12). The call ends with an unknown outcome;
            // any later command publishes the whole intent, so a reader sees none of the batch or all of it, never a prefix.
            var store = c.Services.GetRequiredService<RedisStore>();
            var total = 0L;
            foreach (var n in new[] { 1, 2, 3, 7, 40, 150 })
            {
                var batch = HundredItems("n" + n + "-").Take(20).ToList();
                var p = c.Prepared.PrepareMany(batch);
                store.Runner.Client.FailAfter = n;
                Exception? interrupted;
                try
                {
                    interrupted = await Record.ExceptionAsync(() => c.Prepared.DispatchAsync(p));
                }
                finally
                {
                    store.Runner.Client.FailAfter = 0;
                }

                // A cut at n writes interrupts the call; a larger n than the intent has writes lets it finish.
                Assert.True(interrupted is null ? n >= 40 : interrupted is DispatchOutcomeUnknownException, $"n={n}: {interrupted}");

                // The resend finishes the interrupted intent and returns its receipt: exactly the 20 jobs, once.
                var receipt = await c.Prepared.DispatchAsync(p);
                Assert.Equal(20, receipt.Items.Count);
                total += 20;
                Assert.Equal(total, await Jobs(f));
            }

            // The full 100-item batch the SQL stores run below: interrupted once, then finished by its own resend.
            store.Runner.Client.FailAfter = 1;
            try
            {
                await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => c.Prepared.DispatchAsync(prepared));
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            await c.Prepared.DispatchAsync(prepared);
            Assert.Equal(total + 100, await Jobs(f));
        }
        else
        {
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
            Assert.Equal(100, await Jobs(f));
        }

        var committed = await f.ReadCommittedSnapshot();
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

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "BATCH-02")]
    public async Task Batch_receipt_has_one_result_per_ordinal_and_replays_identically(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch02"), kind);
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
        DispatchOutcomeUnknownException unknown;
        using (var loss = LoseReply(f, c5, prepared.OperationId.Value))
        {
            unknown = await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => c5.Prepared.DispatchAsync(prepared));
            if (kind != StoreKind.Redis)
            {
                Assert.Equal(1, loss.Fired);
            }
        }

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

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "BATCH-03")]
    public async Task Batch_conflicts_with_and_without_continue_and_concurrent_batches(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("batch03"), kind);
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

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UNI-02")]
    public async Task Window_key_reuse_conflict_policy_drift_and_trace(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni02"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var w = Window("w");
        var first = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal(DispatchDisposition.Created, first.Disposition);
        var pre = await f.ReadCommittedSnapshot();

        // (a) identical intent.
        var same = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, false, first.JobId), (same.Disposition, same.DuplicateMatch, same.PolicyDrift, same.JobId));

        // (b) changed payload.
        var conflict = await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchAsync(new PingJob("p2"), w));
        Assert.Equal(first.JobId, conflict.ConflictingJobId);

        // (c) policy-only change: another process registers the contract with a different MaxExecutions default.
        var c5 = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions: 5));
        var drift = await c5.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, true, first.JobId), (drift.Disposition, drift.DuplicateMatch, drift.PolicyDrift, drift.JobId));
        Assert.Equal(1, kind == StoreKind.Redis
            ? await RedisAdmin.CountAsync(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'policy_drift'", [])
            : await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'policy_drift'")));

        // (d) diagnostic-only change (traceparent).
        DispatchReceipt traced;
        using (new System.Diagnostics.Activity("uni02").SetIdFormat(System.Diagnostics.ActivityIdFormat.W3C).Start())
        {
            traced = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        }

        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, false), (traced.Disposition, traced.DuplicateMatch, traced.PolicyDrift));

        // Payload, binding, generation policy and original trace unchanged.
        var post = await f.ReadCommittedSnapshot();
        Assert.Empty(JobDataTables(f, pre.DiffTables(post)));

        // The binding stores both digests and their version.
        if (kind == StoreKind.Redis)
        {
            var row = JsonNode.Parse(Assert.Single((await RedisAdmin.Hash(f.Application, "keys:window")).Values))!;
            Assert.All(new[] { "biz", "pol", "ver" }, field => Assert.NotNull(row[field]));
        }
        else
        {
            Assert.Equal(1, await f.CountAsync(Q(f, """
                SELECT count(*) FROM runwick.dispatch_keys
                WHERE key_kind = 'window' AND business_fingerprint IS NOT NULL AND policy_fingerprint IS NOT NULL AND fingerprint_version IS NOT NULL
                """)));
        }

        Assert.Equal(1, post.Model.DispatchKeys.Count(k => k.Kind == "window"));
        Assert.Equal(1, await Jobs(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UNI-03")]
    public async Task Active_and_window_bindings_to_different_jobs_conflict(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni03"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var j1 = await c.Dispatcher.DispatchAsync(new PingJob("1"), Active("a"));
        var j2 = await c.Dispatcher.DispatchAsync(new PingJob("2"), Window("w"));
        var pre = await f.ReadCommittedSnapshot();

        var ex = await Assert.ThrowsAsync<UniquenessConflictException>(() =>
            c.Dispatcher.DispatchAsync(new PingJob("3"), Window("w", active: "a")));
        Assert.Contains(ex.ConflictingJobId, new JobId?[] { j1.JobId, j2.JobId });
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
    }
}
