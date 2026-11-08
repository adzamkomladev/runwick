using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// FM-11 and RAB-11 (04 "set-based finish" / "set-based claim"): a batch writes exactly what the per-job operations write. The PostgreSQL cases switch
/// its set-based statement path off and on over the same rows (a hook only that driver has). SQLite and Redis have one code path per batch, so the
/// same property is stated against the single operations (the SQLite spec's SQL-12 form): the same jobs go through the batch operation in one
/// application and through Finish/1 (or Reserve/1 then Begin/1) one by one in another, and the normalized rows, the per-item replies and the event
/// counts must be identical. Run on every driver; on PostgreSQL it also compares batch against singles, which the original case does not.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class BatchEquivalenceContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly ControlActor Actor = new("user:batch", null, "host", "equivalence");

    static readonly string[] EventKinds = ["dispatched", "attempt_reserved", "attempt_started", "attempt_finished", "attempt_abandoned", "rate_deferred"];

    /// <summary>Rows normalized by dispatch order: no IDs, tokens or instants, only what the operations decide.</summary>
    static async Task<List<string>> NormalizeAsync(DriverFixture f, IReadOnlyList<Guid> order)
    {
        var m = await Model(f);
        var lines = new List<string>();
        for (var i = 0; i < order.Count; i++)
        {
            var j = m.Job(order[i]);
            lines.Add($"job {i}: {j.State} gen={j.Generation} rev={j.Revision} started={j.StartedCount} lifetime={j.LifetimeExecutions} " +
                      $"unstarted={j.ConsecutiveUnstarted} cancel={j.CancelRequested} reason={j.TerminalReason} permits={m.Permits.Count(p => p.JobId == order[i])} " +
                      $"owned={j.CurrentAttemptId is not null}");
            foreach (var a in m.AttemptsOf(order[i]))
            {
                lines.Add($"  attempt {a.ReservationNumber}: exec={a.ExecutionNumber} outcome={a.Outcome} begun={a.BegunAt is not null} closed={a.ClosedAt is not null}");
            }
        }

        foreach (var k in m.DispatchKeys.OrderBy(k => k.Kind).ThenBy(k => k.Key))
        {
            lines.Add($"key {k.Kind}:{k.Key} active={k.Active}");
        }

        foreach (var kind in EventKinds)
        {
            lines.Add($"events {kind}: {await f.CountAsync(Q(f, $"SELECT count(*) FROM runwick.queue_events WHERE event_kind = '{kind}'"))}");
        }

        return lines;
    }

    // ------------------------------------------------------------------ FM-11

    static async Task<(List<string> Rows, List<string> Replies)> FinishScenarioAsync(StoreKind kind, bool batched)
    {
        await using var f = await DriverFixture.StartAsync(Ns(batched ? "fm11b" : "fm11s"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        // Runs of plain items (one keyed) sit around a scoped item, a permanent failure and a stale item.
        IQueueJob[] jobs = [new PingJob("a"), new PingJob("b"), new ResourceJob("c", "r"), new KeyedJob("d", "key-11"), new PingJob("e"), new PingJob("f"), new PingJob("g"), new PingJob("h")];
        var leases = await FinishManyContractTests.RunningAsync(c, jobs);
        var order = leases.Select(l => l.JobId).ToList();
        var real = leases[5];
        leases[5] = real with { OwnershipToken = Guid.NewGuid() };
        var items = leases.Select((l, i) => c.FinishCommand(l, i == 4 ? AttemptOutcome.PermanentFailure : AttemptOutcome.Succeeded)).ToList();

        var replies = new List<string>();
        if (batched)
        {
            var reply = await c.FinishManyAsync(items);
            replies.AddRange(reply.Value!.Outcomes.Select(o => $"{o.Status} {o.Error?.Code} {o.Value?.Disposition} {o.Value?.TerminalReason}"));
        }
        else
        {
            foreach (var item in items)
            {
                var one = await c.FinishAsync(item);
                replies.Add($"{one.Status} {one.Error?.Code} {one.Value?.Disposition} {one.Value?.TerminalReason}");
            }
        }

        var rows = await NormalizeAsync(f, order);
        var model = await Model(f);
        rows.Add($"finish receipts: {model.Receipts.Count(r => r.Operation == "Finish")}");
        Assert.Empty(await History(f));
        return (rows, replies);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-11")]
    public async Task The_batched_finish_writes_exactly_what_the_per_job_finish_writes(StoreKind kind)
    {
        var batched = await FinishScenarioAsync(kind, batched: true);
        var singles = await FinishScenarioAsync(kind, batched: false);
        Assert.Equal(singles.Replies, batched.Replies);
        Assert.Equal(singles.Rows, batched.Rows);
        Assert.Equal(7, batched.Replies.Count(r => r.StartsWith("Applied", StringComparison.Ordinal)));
        Assert.Contains(batched.Replies, r => r.Contains("ownership_mismatch", StringComparison.Ordinal));
        Assert.Equal(7, batched.Rows.Count(r => r.StartsWith("  attempt", StringComparison.Ordinal) && r.Contains("closed=True", StringComparison.Ordinal)));
    }

    // ------------------------------------------------------------------ RAB-11

    static async Task<(List<string> Rows, List<string> Replies)> ClaimScenarioAsync(StoreKind kind, bool batched)
    {
        await using var f = await DriverFixture.StartAsync(Ns(batched ? "rab11b" : "rab11s"), kind);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<BatchRatedJob>().Named("test.batch-rated").ToQueue("default").RollingWindow(2, TimeSpan.FromMinutes(1));
        });
        Assert.Equal(StoreStatus.Applied, (await ((IRateControlStore)c.Driver).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None)).Status);

        // Runs of plain jobs sit around a scoped and a rated job.
        IQueueJob[] jobs = [new PingJob("a"), new PingJob("b"), new ResourceJob("c", "r"), new PingJob("d"), new BatchRatedJob("e"), new PingJob("f"), new PingJob("g")];
        var order = new List<Guid>();
        foreach (var job in jobs)
        {
            order.Add((await c.Dispatcher.DispatchAsync((dynamic)job)).JobId.Value);
            await Task.Delay(2); // distinct eligible_at, so candidate order is dispatch order
        }

        var replies = new List<string>();
        if (batched)
        {
            var reply = await c.ReserveAndBeginAsync(8);
            Assert.Equal(order, reply.Value!.Outcomes.Select(o => o.JobId));
            replies.AddRange(reply.Value.Outcomes.Select(o => $"{o.Status} {o.Error?.Code} fences={string.Join(",", o.Reserved?.Fences.Select(x => x.Value) ?? [])} exec={o.Begun?.ExecutionNumber}/{o.Begun?.TotalExecutionNumber}"));
        }
        else
        {
            for (var i = 0; i < jobs.Length; i++)
            {
                var reserved = await c.ReserveAsync();
                Assert.Equal(order[i], reserved.Value!.Position.JobId);
                var begun = await c.BeginAsync(reserved.Value.Lease.Handle);
                replies.Add($"{begun.Reply.Status} {begun.Reply.Error?.Code} fences={string.Join(",", reserved.Value.Fences.Select(x => x.Value))} exec={begun.Reply.Value?.ExecutionNumber}/{begun.Reply.Value?.TotalExecutionNumber}");
            }
        }

        var rows = await NormalizeAsync(f, order);
        rows.Add($"grants: {(f.Kind == StoreKind.Redis ? (await f.ReadCommittedSnapshot()).Tables.Where(t => t.Key.StartsWith("grants:", StringComparison.Ordinal)).Sum(t => t.Value.Count) : await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.rate_grants")))}");
        Assert.Empty(await History(f));
        return (rows, replies);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-11")]
    public async Task The_batched_claim_writes_exactly_what_the_per_job_claim_writes(StoreKind kind)
    {
        var batched = await ClaimScenarioAsync(kind, batched: true);
        var singles = await ClaimScenarioAsync(kind, batched: false);
        Assert.Equal(singles.Replies, batched.Replies);
        Assert.Equal(singles.Rows, batched.Rows);
        Assert.Equal(7, batched.Replies.Count(r => r.StartsWith("Applied", StringComparison.Ordinal)));
        Assert.Contains("events attempt_started: 7", batched.Rows);
    }
}
