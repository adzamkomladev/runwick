using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// FinishMany/1 (performance plan PF-06 batched finish, 03/04/05/07 "Changes (2026-11-02, PF-06 batched finish)"): one call
/// settles up to 32 Finish/1 commands; every item is exactly a Finish/1 with its own command ID and its own durable receipt,
/// and no item's refusal or replay affects another. Each case ends with the ReferenceModel check of every committed history.
/// </summary>
public sealed class FinishManyTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<string> StateAsync(DriverFixture f, Guid job) =>
        (await TextAsync(f, "SELECT state FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single();

    static Task<long> FinishReceipts(DriverFixture f) => f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'");

    /// <summary>Dispatches the jobs and claims them all with one ReserveAndBegin; returns the begun leases in dispatch order.</summary>
    static async Task<LeaseHandle[]> RunningAsync(TestClient c, params IQueueJob[] jobs)
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

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-01")]
    public async Task A_stale_item_gets_its_own_refusal_and_never_affects_the_others()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"), new KeyedJob("d", "key-1"), new PingJob("e"));

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

        // Every applied item: its own receipt under its own command ID, operation Finish; its attempt closed by that ID.
        foreach (var i in new[] { 0, 2, 3 })
        {
            Assert.Equal(JobStates.Succeeded, o[i].Value!.Position.State);
            Assert.Equal(reply.DecidedAt, o[i].DecidedAt);
            Assert.Equal(["Finish"], await TextAsync(f, "SELECT operation FROM runwick.storage_commands WHERE command_id = @c", ("c", items[i].CommandId)));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE closed_by_command_id = @c", ("c", items[i].CommandId)));
            Assert.Equal(JobStates.Succeeded, await StateAsync(f, items[i].Body.Lease.JobId));
        }

        // The refused items left no receipt; the stale job is untouched and still holds its permit; the batch has none.
        Assert.Equal(0, await Receipts(f, items[1].CommandId));
        Assert.Equal(0, await Receipts(f, items[4].CommandId));
        Assert.Equal(0, await Receipts(f, c.History.Last().CommandId));
        Assert.Equal(JobStates.Running, await StateAsync(f, leases[1].JobId));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", leases[1].JobId)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'key-1' AND bound_job_id IS NULL"));

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
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-02")]
    public async Task A_resent_batch_replays_every_item_from_its_own_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm02"));
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
        var attempts = await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE finished_at IS NOT NULL");
        var events = await f.CountAsync("SELECT count(*) FROM runwick.queue_events");

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
        Assert.Equal(attempts + 1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE finished_at IS NOT NULL"));
        Assert.Equal(events + 1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events"));
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
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-03")]
    public async Task A_lost_batch_reply_is_resolved_per_item_by_its_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, Pings(3));
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(4);
        var items = new[]
        {
            c.FinishCommand(leases[0], AttemptOutcome.Succeeded, notAfter: notAfter),
            c.FinishCommand(leases[1] with { OwnershipToken = Guid.NewGuid() }, AttemptOutcome.Succeeded, notAfter: notAfter),
            c.FinishCommand(leases[2], AttemptOutcome.PermanentFailure, notAfter: notAfter),
        };
        var id = Guid.CreateVersion7();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishManyAsync(items, commandId: id));
        Assert.Equal(1, drop.Fired);
        Assert.Equal((id, Operations.FinishMany), (unknown.CommandId, unknown.Operation));

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
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-04")]
    public async Task Mixed_outcomes_in_one_batch_follow_the_finish_precedence_table()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("ok"), new PingJob("retry"), new PingJob("perm"), new PingJob("user"),
            new PingJob("cancel-req"), new KeyedJob("key", "key-9"));

        // Job 5 carries a cancellation request (recorded while running, as Cancel/1 does for a running job).
        await f.ExecAsync("UPDATE runwick.jobs SET cancel_requested_at = now() WHERE job_id = @j", ("j", leases[4].JobId));
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
        Assert.Equal(6, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_finished'"));

        // The terminal close of the keyed job released its key; every permit is gone.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'key-9' AND bound_job_id IS NULL"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));

        // The forced cancel request is outside the ReferenceModel's reachable commands; every other history is accepted.
        var violations = await f.HistoryViolations();
        Assert.DoesNotContain(violations, v => !v.Contains(leases[4].JobId.ToString("N"), StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-05")]
    public async Task A_fault_after_the_second_item_rolls_the_whole_call_back()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm05"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"));
        var items = leases.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var id = Guid.CreateVersion7();

        // The server hook after item 2's close and receipt (AfterMutation step 2) throws: nothing of items 1 and 2 commits.
        var hook = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { CommandId = id, Step = 2 });
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.FinishManyAsync(items, commandId: id));
        Assert.Equal(1, hook.Fired);
        Assert.Equal(0, await FinishReceipts(f));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));

        // Known not applied: the same items (in a new call, or one by one) apply once.
        var again = await c.FinishManyAsync(items);
        Assert.All(again.Value!.Outcomes, x => Assert.Equal((StoreStatus.Applied, false), (x.Status, x.Replayed)));
        Assert.Equal(3, await FinishReceipts(f));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-06")]
    public async Task A_renewal_racing_a_batched_finish_waits_and_then_finds_the_jobs_closed()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm06"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await RunningAsync(c, new ResourceJob("a", "r1"), new PingJob("b"), new SharedJob("c", "r2"));

        // The batch holds every job and scope lock at the barrier; a RenewMany and a Renew of the same leases wait on it.
        var hold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.FinishMany));
        var batch = Task.Run(() => c.FinishManyAsync([c.FinishCommand(leases[0], AttemptOutcome.Succeeded), c.FinishCommand(leases[1], AttemptOutcome.Succeeded)]));
        await DriverFixture.WaitAtBarrier(hold);
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));
        var many = Task.Run(() => other.RenewManyAsync([leases[0], leases[2]]));
        var one = Task.Run(() => other.RenewAsync(leases[1]));
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype IN ('tuple', 'transactionid')", 2, "renewals waiting on the batch");
        DriverFixture.ReleaseBarrier(hold);

        Assert.All((await batch).Value!.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
        var renewed = (await many).Value!.Outcomes;
        Assert.Equal([StoreStatus.InvalidState, StoreStatus.Applied], renewed.Select(x => x.Status));
        Assert.Equal(StoreStatus.InvalidState, (await one).Status);

        // The other way round: a renewal first holds the lock; the batch waits, then finishes on the renewed lease.
        var renewHold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.RenewMany));
        var renewal = Task.Run(() => other.RenewManyAsync([leases[2]]));
        await DriverFixture.WaitAtBarrier(renewHold);
        var late = Task.Run(() => c.FinishManyAsync([c.FinishCommand(leases[2], AttemptOutcome.Succeeded)]));
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype IN ('tuple', 'transactionid')", 1, "batch waiting on the renewal");
        DriverFixture.ReleaseBarrier(renewHold);
        Assert.Equal(StoreStatus.Applied, (await renewal).Value!.Outcomes.Single().Status);
        Assert.Equal(StoreStatus.Applied, (await late).Value!.Outcomes.Single().Status);
        Assert.Equal(0, c.Store.Runner.Deadlocks + other.Store.Runner.Deadlocks);
        await other.DisposeAsync();
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-07")]
    public async Task Concurrent_batches_renewals_and_single_finishes_never_deadlock()
    {
        // 48 jobs on shared and paired scopes (pairs in both orders); four clients finish their jobs in batches, in
        // reverse order, while two others renew everything with RenewMany and finish single jobs.
        await using var f = await DriverFixture.StartAsync(Ns("fm07"));
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
            for (var round = 0; round < 400 && await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'") > 0; round++)
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
        var deadlocks = clients.Sum(w => w.Store.Runner.Deadlocks);
        foreach (var w in clients)
        {
            await w.DisposeAsync();
        }

        Assert.Equal(0, deadlocks);
        Assert.Equal(48, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'"));
        Assert.Equal(48, await FinishReceipts(f));
        Assert.Empty(await f.HistoryViolations());
    }

    static Runwick.Harness.Engine.WorkerHost Host(DriverFixture f, int concurrency) =>
        Runwick.Harness.Engine.WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency);

    /// <summary>
    /// Holds the first two settlement calls (Finish/1 or FinishMany) 1.5 s on the client, so both settlement calls are busy
    /// and the Finishes published meanwhile queue.
    /// </summary>
    static void BusyLane(DriverFixture f)
    {
        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromMilliseconds(1500)), FaultTarget.Op(Operations.Finish), times: 2);
        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromMilliseconds(1500)), FaultTarget.Op(Operations.FinishMany), times: 2);
    }

    static async Task RunJobsAsync(DriverFixture f, Runwick.Harness.Engine.WorkerHost w, int jobs, bool start = true)
    {
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new Runwick.Harness.Engine.WorkJob("j" + n)))]);
        if (start)
        {
            await w.StartAsync();
        }

        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == jobs,
            TimeSpan.FromSeconds(90), "every job settled");
        Assert.Equal(jobs, w.Script.Invocations.Count);
        Assert.Equal(jobs, await FinishReceipts(f));
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));

        // The held lane makes control waits long, so the pressure gate may stop admission for a while (05 §15); it
        // reopens by itself once the lane is healthy. No admission close is latched.
        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => w.Engine.AdmissionOpen, TimeSpan.FromSeconds(30), "admission open again");
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("admission closed", StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-08")]
    public async Task Finishes_that_queue_behind_a_busy_lane_are_sent_in_one_call()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm08"));
        await using var w = Host(f, concurrency: 16);
        BusyLane(f);
        await RunJobsAsync(f, w, 40);
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchesApplied) > 0);
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchedItemsSettled) >= 2);
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-09")]
    public async Task A_lost_batch_reply_in_the_worker_resolves_each_finish_by_its_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm09"));
        await using var w = Host(f, concurrency: 16);
        BusyLane(f);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.FinishMany));
        await RunJobsAsync(f, w, 24);
        Assert.Equal(1, drop.Fired);

        // The dropped call committed: its items were resolved by Finish/1 replays of their own receipts, not reapplied.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchesSent) > Interlocked.Read(ref w.Engine.Completion.BatchesApplied));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "FM-10")]
    public async Task A_refused_batch_turns_batching_off_and_every_finish_settles_alone()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm10"));
        await using var w = Host(f, concurrency: 16);
        await w.StartAsync();

        // After startup's schema check, before any finish: the store now answers 42883 for the batched finish.
        await f.ExecAsync("ALTER FUNCTION runwick.finish_many_v1 RENAME TO finish_many_v1_hidden");
        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromMilliseconds(1500)), FaultTarget.Op(Operations.Finish), times: 2);
        await RunJobsAsync(f, w, 24, start: false);
        var completion = w.Engine.Completion;
        Assert.True(Interlocked.Read(ref completion.BatchesSent) > 0);
        Assert.Equal(0, Interlocked.Read(ref completion.BatchesApplied));
        Assert.True(w.Engine.Health().Diagnostics.Any(d => d.Contains("FinishMany returned UnsupportedVersion", StringComparison.Ordinal)),
            $"last batch failure: {completion.LastBatchFailure}");
        Assert.Empty(await f.HistoryViolations());
    }
}
