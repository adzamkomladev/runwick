using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// FM-01..07 (PF-06, 07 "batched finish"): FinishMany/1 settles up to 32 Finish/1 commands, each with its own command ID and receipt, and no item's
/// refusal or replay affects another. Ported from the PostgreSQL suite as theories over <see cref="StoreKind"/>. Where the PostgreSQL case holds a
/// command at a fault point, the Redis form uses the write-prefix seam or a server pause (Redis has no fault points, 22 §12).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class FinishManyContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static Task<string> StateAsync(DriverFixture f, Guid job) => Model(f).ContinueWith(m => m.Result.Job(job).State, TaskScheduler.Default);

    static async Task<long> FinishReceipts(DriverFixture f) => (await Model(f)).Receipts.Count(r => r.Operation == "Finish");

    /// <summary>Dispatches the jobs and claims them all with one ReserveAndBegin; returns the begun leases in dispatch order.</summary>
    internal static async Task<LeaseHandle[]> RunningAsync(TestClient c, params IQueueJob[] jobs)
    {
        foreach (var job in jobs)
        {
            await c.Dispatcher.DispatchAsync((dynamic)job);
            await Task.Delay(2);
        }

        var claimed = await c.ReserveAndBeginAsync(jobs.Length);
        Assert.Equal(StoreStatus.Applied, claimed.Status);
        Assert.All(claimed.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        return [.. claimed.Value.Outcomes.Select(o => o.Begun!.Lease.Handle)];
    }

    static IQueueJob[] Pings(int n, string prefix = "p") => [.. Enumerable.Range(0, n).Select(i => (IQueueJob)new PingJob(prefix + i))];

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-01")]
    public async Task A_stale_item_gets_its_own_refusal_and_never_affects_the_others(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm01"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"), new KeyedJob("d", "key-1"), new PingJob("e"));
        Assert.Contains((await Model(f)).DispatchKeys, k => k.Key == "key-1" && k.Active);

        // Item 2 carries a wrong ownership token (a stale lease); item 5's job was already finished by another command.
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(leases[4], AttemptOutcome.Succeeded)).Status);
        var items = new[]
        {
            c.FinishCommand(leases[0], AttemptOutcome.Succeeded),
            c.FinishCommand(leases[1] with { OwnershipToken = Guid.NewGuid() }, AttemptOutcome.Succeeded),
            c.FinishCommand(leases[2], AttemptOutcome.Succeeded),
            c.FinishCommand(leases[3], AttemptOutcome.Succeeded),
            c.FinishCommand(leases[4], AttemptOutcome.Succeeded),
        };
        var reply = await c.FinishManyAsync(items);
        Assert.Equal((StoreStatus.Applied, StoreEffect.Applied, false), (reply.Status, reply.Effect, reply.Replayed));
        var o = reply.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.StaleLease, StoreStatus.Applied, StoreStatus.Applied, StoreStatus.InvalidState],
            o.Select(x => x.Status));
        Assert.Equal(("ownership_mismatch", "job_terminal"), (o[1].Error!.Code, o[4].Error!.Code));
        Assert.All(new[] { o[1], o[4] }, x => Assert.Equal(StoreEffect.ObservedNoEffect, x.Effect));

        // Every applied item: its own receipt under its own command ID, operation Finish; its attempt closed.
        var model = await Model(f);
        foreach (var i in new[] { 0, 2, 3 })
        {
            Assert.Equal(JobStates.Succeeded, o[i].Value!.Position.State);
            Assert.Equal(reply.DecidedAt, o[i].DecidedAt);
            Assert.Equal("Finish", Assert.Single(model.Receipts, r => r.CommandId == items[i].CommandId).Operation);
            Assert.NotNull(model.Attempts.Single(a => a.AttemptId == items[i].Body.Lease.AttemptId).ClosedAt);
            Assert.Equal(JobStates.Succeeded, model.Job(items[i].Body.Lease.JobId).State);
        }

        if (kind != StoreKind.Redis)
        {
            foreach (var i in new[] { 0, 2, 3 })
            {
                Assert.Equal(1, await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.job_attempts WHERE closed_by_command_id = @c"), ("c", items[i].CommandId)));
            }
        }

        // The refused items left no receipt; the stale job is untouched and still holds its permit; the batch has none.
        Assert.Equal(0, await Receipts(f, items[1].CommandId));
        Assert.Equal(0, await Receipts(f, items[4].CommandId));
        Assert.Equal(0, await Receipts(f, c.History.Last().CommandId));
        Assert.Equal(JobStates.Running, model.Job(leases[1].JobId).State);
        Assert.Equal(1, model.Permits.Count(p => p.JobId == leases[1].JobId));
        Assert.DoesNotContain(model.DispatchKeys, k => k.Key == "key-1" && k.Active);

        // The refused item's ID was never reserved: the stale owner's real lease finishes under a new command, and a
        // correct body under the refused ID applies too (no receipt blocks it).
        var retry = c.FinishCommand(leases[1], AttemptOutcome.Succeeded, commandId: items[1].CommandId, notAfter: items[1].NotAfter);
        var retried = await c.FinishAsync(retry);
        Assert.Equal((StoreStatus.Applied, false), (retried.Status, retried.Replayed));
        Assert.Equal(4 + 1, await FinishReceipts(f));

        // Bounds: none, more than 32, a repeated command ID or job ID is InvalidRequest with no effect.
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishManyAsync([])).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishManyAsync([.. Enumerable.Range(0, 33).Select(_ => c.FinishCommand(leases[0], AttemptOutcome.Succeeded))])).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishManyAsync([items[0], items[0]])).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishManyAsync([items[0], c.FinishCommand(leases[0], AttemptOutcome.Succeeded)])).Status);
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-02")]
    public async Task A_resent_batch_replays_every_item_from_its_own_receipt(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm02"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, Pings(4));
        var items = new List<StoreCommand<FinishRequest>>
        {
            c.FinishCommand(leases[0], AttemptOutcome.Succeeded),
            c.FinishCommand(leases[1], AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(700),
                error: new AttemptError("boom", "System.Exception", "boom", null)),
            c.FinishCommand(leases[2], AttemptOutcome.Succeeded),
        };
        var first = await c.FinishManyAsync(items);
        Assert.All(first.Value!.Outcomes, x => Assert.Equal((StoreStatus.Applied, false), (x.Status, x.Replayed)));
        var attempts = (await Model(f)).Attempts.Count(a => a.ClosedAt is not null);
        var events = await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.queue_events"));

        // The same items again, in a new batch with a fourth new item: the three replay their recorded replies exactly;
        // the new one applies; nothing of the three is applied twice.
        var again = await c.FinishManyAsync([.. items, c.FinishCommand(leases[3], AttemptOutcome.Succeeded)]);
        var o = again.Value!.Outcomes;
        Assert.Equal([true, true, true, false], o.Select(x => x.Replayed));
        Assert.All(o, x => Assert.Equal(StoreStatus.Applied, x.Status));
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(first.Value.Outcomes[i].Value, o[i].Value);
            Assert.Equal(first.Value.Outcomes[i].DecidedAt, o[i].DecidedAt);
        }

        Assert.Equal((JobStates.Retrying, TimeSpan.FromMilliseconds(700)), (o[1].Value!.Disposition, o[1].Value!.EligibleAt!.Value - o[1].DecidedAt));
        Assert.Equal(attempts + 1, (await Model(f)).Attempts.Count(a => a.ClosedAt is not null));
        Assert.Equal(events + 1, await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.queue_events")));
        Assert.Equal(4, await FinishReceipts(f));

        // Everything replayed: the call commits nothing.
        var replayOnly = await c.FinishManyAsync(items);
        Assert.All(replayOnly.Value!.Outcomes, x => Assert.True(x.Replayed));

        // An item is a Finish/1: Finish/1 with the same command replays the batch's receipt; a changed body under an
        // item's ID is that item's CommandConflict, while the others still replay.
        var single = await c.FinishAsync(items[1]);
        Assert.Equal((StoreStatus.Applied, true, first.Value.Outcomes[1].Value), (single.Status, single.Replayed, single.Value));
        var changed = c.FinishCommand(leases[0], AttemptOutcome.PermanentFailure, commandId: items[0].CommandId, notAfter: items[0].NotAfter);
        var conflict = await c.FinishManyAsync([changed, items[2]]);
        Assert.Equal([StoreStatus.CommandConflict, StoreStatus.Applied], conflict.Value!.Outcomes.Select(x => x.Status));
        Assert.True(conflict.Value.Outcomes[1].Replayed);
        Assert.Equal(1, await Receipts(f, items[0].CommandId));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-03")]
    public async Task A_lost_batch_reply_is_resolved_per_item_by_its_receipt(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm03"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, Pings(3));
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(8);
        var items = new[]
        {
            c.FinishCommand(leases[0], AttemptOutcome.Succeeded, notAfter: notAfter),
            c.FinishCommand(leases[1] with { OwnershipToken = Guid.NewGuid() }, AttemptOutcome.Succeeded, notAfter: notAfter),
            c.FinishCommand(leases[2], AttemptOutcome.PermanentFailure, notAfter: notAfter),
        };
        var id = Guid.CreateVersion7();
        using (var loss = LoseReply(f, c, id))
        {
            var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishManyAsync(items, commandId: id));
            if (kind != StoreKind.Redis)
            {
                Assert.Equal(1, loss.Fired);
            }

            Assert.Equal((id, Operations.FinishMany), (unknown.CommandId, unknown.Operation));
        }

        // Each item resolves alone, exactly as an unknown Finish/1 does: the same command again. The two that applied
        // replay their receipts; the stale one had no receipt and is refused again.
        var r0 = await c.FinishAsync(items[0]);
        var r1 = await c.FinishAsync(items[1]);
        Assert.Equal((StoreStatus.Applied, true, JobStates.Succeeded), (r0.Status, r0.Replayed, r0.Value!.Disposition));
        Assert.Equal(StoreStatus.StaleLease, r1.Status);

        // After the items' NotAfter the receipt still answers (the existing result is read before the deadline check),
        // and an item without a receipt is definitively not applied.
        await Task.Delay(notAfter - DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(200));
        var r2 = await c.FinishAsync(items[2]);
        Assert.Equal((StoreStatus.Applied, true, JobStates.Failed), (r2.Status, r2.Replayed, r2.Value!.Disposition));
        Assert.Equal(StoreStatus.CommandExpired, (await c.FinishAsync(items[1])).Status);
        Assert.Equal(2, await FinishReceipts(f));
        Assert.Equal(JobStates.Running, await StateAsync(f, leases[1].JobId));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-04")]
    public async Task Mixed_outcomes_in_one_batch_follow_the_finish_precedence_table(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm04"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("ok"), new PingJob("retry"), new PingJob("perm"), new PingJob("user"),
            new PingJob("cancel-req"), new KeyedJob("key", "key-9"));

        // Job 5 carries a cancellation request (recorded while running, as Cancel/1 does for a running job).
        await ForceCancelRequestAsync(f, leases[4].JobId);
        var reply = await c.FinishManyAsync(
        [
            c.FinishCommand(leases[0], AttemptOutcome.Succeeded),
            c.FinishCommand(leases[1], AttemptOutcome.RetryableFailure),
            c.FinishCommand(leases[2], AttemptOutcome.PermanentFailure),
            c.FinishCommand(leases[3], AttemptOutcome.UserCancelled),
            c.FinishCommand(leases[4], AttemptOutcome.RetryableFailure),
            c.FinishCommand(leases[5], AttemptOutcome.PermanentFailure),
        ]);
        var o = reply.Value!.Outcomes;
        Assert.All(o, x => Assert.Equal(StoreStatus.Applied, x.Status));
        Assert.Equal(
            [(JobStates.Succeeded, "succeeded"), (JobStates.Retrying, null), (JobStates.Failed, "permanent_failure"),
             (JobStates.Cancelled, "user_cancelled"), (JobStates.Cancelled, "cancellation_requested"), (JobStates.Failed, "permanent_failure")],
            o.Select(x => (x.Value!.Disposition, x.Value.TerminalReason)));

        // The retry delay is drawn per item from the job's policy (100 ms initial, n = 1: 50-100 ms).
        var delay = o[1].Value!.EligibleAt!.Value - o[1].DecidedAt;
        Assert.InRange(delay, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100));
        Assert.Equal(6, await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_finished'")));

        // The terminal close of the keyed job released its key; every permit is gone.
        var model = await Model(f);
        Assert.DoesNotContain(model.DispatchKeys, k => k.Key == "key-9" && k.Active);
        Assert.Empty(model.Permits);

        // The forced cancel request is outside the ReferenceModel's reachable commands; every other history is accepted.
        var violations = await History(f);
        Assert.DoesNotContain(violations, v => !v.Contains(leases[4].JobId.ToString("N"), StringComparison.Ordinal));
    }

    /// <summary>Setup: records a cancellation request on a job the way Cancel/1 does for a running job (the request is a field of the job).</summary>
    internal static async Task ForceCancelRequestAsync(DriverFixture f, Guid job)
    {
        switch (f.Kind)
        {
            case StoreKind.Redis:
                await RedisAdmin.SetCancelRequested(f.Application, job);
                break;
            case StoreKind.Sqlite:
                await f.ExecAsync("UPDATE runwick_jobs SET cancel_requested_at = @t WHERE job_id = @j", ("j", job), ("t", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
                break;
            default:
                await f.ExecAsync("UPDATE runwick.jobs SET cancel_requested_at = now() WHERE job_id = @j", ("j", job));
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-05")]
    public async Task A_fault_after_the_second_item_rolls_the_whole_call_back(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm05"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"));
        var items = leases.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var id = Guid.CreateVersion7();

        if (kind == StoreKind.Redis)
        {
            // Redis writes the whole decision as a stored intent and finishes it on the next command (22 §4): there is no rollback, so a cut after
            // two writes leaves the call unknown and any later command publishes the complete batch; a reader never sees item 1 or 2 alone.
            var store = c.Services.GetRequiredService<RedisStore>();
            store.Runner.Client.FailAfter = 2;
            try
            {
                await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishManyAsync(items, commandId: id));
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            var resent = await c.FinishManyAsync(items, commandId: id);
            Assert.All(resent.Value!.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
            Assert.True(resent.Replayed || resent.Value.Outcomes.All(x => x.Replayed), "the interrupted call was completed, not applied a second time");
            Assert.Equal(3, await FinishReceipts(f));
            Assert.Empty((await Model(f)).Permits);
            Assert.Equal(3, (await Model(f)).Jobs.Count(j => j.State == "succeeded"));
            Assert.Empty(await History(f));
        }
        else
        {
            // The hook after item 2's close and receipt (AfterMutation step 2) throws: nothing of items 1 and 2 commits.
            var hook = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { CommandId = id, Step = 2 });
            await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.FinishManyAsync(items, commandId: id));
            Assert.Equal(1, hook.Fired);
            Assert.Equal(0, await FinishReceipts(f));
            Assert.Equal(3, (await Model(f)).Jobs.Count(j => j.State == "running"));
            Assert.Single((await Model(f)).Permits);

            // Known not applied: the same items (in a new call, or one by one) apply once.
            var again = await c.FinishManyAsync(items);
            Assert.All(again.Value!.Outcomes, x => Assert.Equal((StoreStatus.Applied, false), (x.Status, x.Replayed)));
            Assert.Equal(3, await FinishReceipts(f));
            Assert.Empty((await Model(f)).Permits);
            Assert.Empty(await History(f));
        }
    }

    /// <summary>
    /// Starts <paramref name="first"/> and holds it inside the store, starts <paramref name="second"/> (which must wait), then lets both go.
    /// PostgreSQL and SQLite hold the first at AfterLocksBeforeFreshTime; Redis has no such point, so the server pauses writes, the two scripts queue
    /// in arrival order, and the unpause runs them first-then-second.
    /// </summary>
    internal static async Task<(T1 First, T2 Second)> OverlapAsync<T1, T2>(DriverFixture f, TestClient c, string firstOperation, Func<Task<T1>> first, Func<Task<T2>> second)
    {
        if (f.Kind == StoreKind.Redis)
        {
            var mux = await RedisServer.AdminAsync();
            var server = mux.GetServer(mux.GetEndPoints()[0]);
            await server.ExecuteAsync("CLIENT", "PAUSE", 600_000, "WRITE");
            var a = Task.Run(first);
            await Task.Delay(300);
            var b = Task.Run(second);
            await Task.Delay(300);
            Assert.False(a.IsCompleted || b.IsCompleted, "the commands did not wait for the server");
            await server.ExecuteAsync("CLIENT", "UNPAUSE");
            return (await WithTimeout(a), await WithTimeout(b));
        }

        var hold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(firstOperation));
        var held = Task.Run(first);
        await DriverFixture.WaitAtBarrier(hold);
        var waiting = Task.Run(second);
        await Task.Delay(300);
        Assert.False(waiting.IsCompleted, "the second command did not wait for the first");
        DriverFixture.ReleaseBarrier(hold);
        return (await WithTimeout(held), await WithTimeout(waiting));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-06")]
    public async Task A_renewal_racing_a_batched_finish_waits_and_then_finds_the_jobs_closed(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm06"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new ResourceJob("a", "r1"), new PingJob("b"), new SharedJob("c", "r2"));
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));

        // The batch is held first; a RenewMany and a Renew of the same leases wait on it, then find the jobs closed (InvalidState).
        var (batch, renewals) = await OverlapAsync(f, c, Operations.FinishMany,
            () => c.FinishManyAsync([c.FinishCommand(leases[0], AttemptOutcome.Succeeded), c.FinishCommand(leases[1], AttemptOutcome.Succeeded)]),
            async () => (Many: await other.RenewManyAsync([leases[0], leases[2]]), One: await other.RenewAsync(leases[1])));
        Assert.All(batch.Value!.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
        Assert.Equal([StoreStatus.InvalidState, StoreStatus.Applied], renewals.Many.Value!.Outcomes.Select(x => x.Status));
        Assert.Equal(StoreStatus.InvalidState, renewals.One.Status);

        // The other way round: a renewal first holds the lock; the batch waits, then finishes on the renewed lease.
        var (renewal, late) = await OverlapAsync(f, c, Operations.RenewMany,
            () => other.RenewManyAsync([leases[2]]),
            () => c.FinishManyAsync([c.FinishCommand(leases[2], AttemptOutcome.Succeeded)]));
        Assert.Equal(StoreStatus.Applied, renewal.Value!.Outcomes.Single().Status);
        Assert.Equal(StoreStatus.Applied, late.Value!.Outcomes.Single().Status);
        Assert.Equal(0, Deadlocks(c) + Deadlocks(other));
        await other.DisposeAsync();
        Assert.Equal(3, (await Model(f)).Jobs.Count(j => j.State == "succeeded"));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-07")]
    public async Task Concurrent_batches_renewals_and_single_finishes_never_deadlock(StoreKind kind)
    {
        // 48 jobs on shared and paired scopes (pairs in both orders); four clients finish their jobs in batches, in
        // reverse order, while two others renew everything with RenewMany and finish single jobs.
        await using var f = await DriverFixture.StartAsync(Ns("fm07"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var jobs = new List<IQueueJob>();
        for (var i = 0; i < 36; i++)
        {
            jobs.Add(new SharedJob("s" + i, "res" + (i % 6)));
        }

        for (var i = 0; i < 12; i++)
        {
            jobs.Add(i % 2 == 0 ? new PairJob("q" + i, "pa" + (i % 3), "pb" + (i % 3)) : new PairJob("q" + i, "pb" + (i % 3), "pa" + (i % 3)));
        }

        foreach (var job in jobs)
        {
            await c.Dispatcher.DispatchAsync((dynamic)job);
        }

        var clients = new List<TestClient>();
        for (var i = 0; i < 6; i++)
        {
            clients.Add(await f.OpenClient(configure: r => TestJobs.Register(r)));
        }

        var tasks = clients.Select((w, i) => Task.Run(async () =>
        {
            for (var round = 0; round < 400 && await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'pending'")) > 0; round++)
            {
                var r = await w.ReserveAndBeginAsync(8);
                var begun = (r.Value?.Outcomes ?? []).Where(x => x.Begun is not null).Select(x => x.Begun!.Lease.Handle).ToList();
                if (begun.Count == 0)
                {
                    await Task.Delay(5);
                    continue;
                }

                if (i < 4)
                {
                    begun.Reverse();
                    var done = await w.FinishManyAsync([.. begun.Select(l => w.FinishCommand(l, AttemptOutcome.Succeeded))]);
                    Assert.All(done.Value!.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
                }
                else
                {
                    Assert.Equal(StoreStatus.Applied, (await w.RenewManyAsync(begun)).Status);
                    foreach (var l in begun)
                    {
                        Assert.Equal(StoreStatus.Applied, (await w.FinishAsync(l, AttemptOutcome.Succeeded)).Status);
                    }
                }
            }
        })).ToList();
        await Task.WhenAll(tasks);
        var deadlocks = clients.Sum(Deadlocks);
        foreach (var w in clients)
        {
            await w.DisposeAsync();
        }

        Assert.Equal(0, deadlocks);
        Assert.Equal(48, (await Model(f)).Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(48, await FinishReceipts(f));
        Assert.Empty(await History(f));
    }
}
