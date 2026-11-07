using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Registration;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>A job-type rate scope of 2 per minute, for deferral inside a batch.</summary>
public sealed record BatchRatedJob(string Id) : IQueueJob;

/// <summary>
/// ReserveAndBegin/1 (performance plan PF-06 batched claim, 03/04/05/07 "Changes (2026-11-02, PF-06 batched claim)"):
/// one command claims up to N jobs and begins each; every job gets its own outcome, exactly the one Reserve/1 then Begin/1
/// would give it, and no outcome affects another. Each case ends with the ReferenceModel check of every committed history.
/// </summary>
public sealed class ReserveAndBeginTests
{
    static readonly ControlActor Actor = new("user:batch", null, "host", "rab");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<BatchRatedJob>().Named("test.batch-rated").ToQueue("default").RollingWindow(2, TimeSpan.FromMinutes(1));
    }

    static async Task<string> StateAsync(DriverFixture f, Guid job) =>
        (await TextAsync(f, "SELECT state FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single();

    static Task<long> Attempts(DriverFixture f, Guid job) =>
        f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", job));

    static async Task<JobPosition> PositionAsync(DriverFixture f, Guid job)
    {
        var row = (await TextAsync(f, "SELECT generation || ',' || revision || ',' || state FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single().Split(',');
        return new JobPosition(job, int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(row[1], System.Globalization.CultureInfo.InvariantCulture), row[2]);
    }

    static async Task<Guid[]> DispatchAsync(TestClient c, params IQueueJob[] jobs)
    {
        var ids = new List<Guid>();
        foreach (var job in jobs)
        {
            ids.Add((await c.Dispatcher.DispatchAsync((dynamic)job)).JobId.Value);
            await Task.Delay(2); // distinct eligible_at, so candidate order is dispatch order
        }

        return [.. ids];
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-01")]
    public async Task Each_job_gets_its_own_outcome_and_a_blocked_job_never_affects_the_others()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab01"));
        var c = await f.OpenClient(configure: Register);

        // Two jobs share a capacity-1 resource scope (fenced); two have no scope.
        var ids = await DispatchAsync(c, new ResourceJob("a", "r"), new PingJob("b"), new ResourceJob("c", "r"), new PingJob("d"));
        var reply = await c.ReserveAndBeginAsync(4);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.False(reply.Replayed);
        var o = reply.Value!.Outcomes;
        Assert.Equal(ids, o.Select(x => x.JobId));
        Assert.Equal([StoreStatus.Applied, StoreStatus.Applied, StoreStatus.CapacityBlocked, StoreStatus.Applied], o.Select(x => x.Status));

        // The begun jobs: running, one execution counted, the lease and fence of a Reserve, the Begin's numbers.
        foreach (var x in o.Where(x => x.Status == StoreStatus.Applied))
        {
            Assert.Equal(JobStates.Running, await StateAsync(f, x.JobId));
            Assert.Equal(x.Reserved!.Lease.Handle, x.Begun!.Lease.Handle);
            Assert.Equal((1, 1L), (x.Begun.ExecutionNumber, x.Begun.TotalExecutionNumber));
            Assert.Equal(reply.DecidedAt, x.Begun.AuthorizedAt);
            Assert.True(x.Reserved.Lease.ExpiresAt >= reply.DecidedAt + TimeSpan.FromSeconds(29));
            Assert.NotEmpty(x.Reserved.PayloadUtf8);
        }

        Assert.Single(o[0].Reserved!.Fences);
        Assert.Empty(o[1].Reserved!.Fences);

        // The blocked job: untouched, the first blocked scope and its own cursor, exactly as a blocked Reserve reports it.
        Assert.Equal("capacity_blocked", o[2].Error!.Code);
        Assert.Equal(o[0].Reserved!.Fences[0].ScopeId, o[2].Error!.BlockedScopeId);
        Assert.Equal(ids[2], o[2].Error!.ResumeAfter!.JobId);
        Assert.Equal(JobStates.Pending, await StateAsync(f, ids[2]));
        Assert.Equal(0, await Attempts(f, ids[2]));

        // One receipt for the command; the attempts' events name it.
        Assert.Equal(1, await Receipts(f, c.History.Last().CommandId));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_started'"));

        // The begun leases are ordinary leases: Renew and Finish them; the blocked job is claimable once the scope frees.
        foreach (var x in o.Where(x => x.Status == StoreStatus.Applied))
        {
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(x.Begun!.Lease.Handle)).Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(x.Begun.Lease.Handle, AttemptOutcome.Succeeded)).Status);
        }

        var next = await c.ReserveAndBeginAsync(4);
        Assert.Equal(StoreStatus.Applied, next.Status);
        Assert.Equal([(ids[2], StoreStatus.Applied)], next.Value!.Outcomes.Select(x => (x.JobId, x.Status)));
        Assert.True(next.Value.Outcomes[0].Reserved!.Fences[0].Value > o[0].Reserved!.Fences[0].Value);

        // Every candidate blocked: no effect at all, reported as a blocked Reserve that resumes after the last candidate.
        var more = await DispatchAsync(c, new ResourceJob("e", "r"), new ResourceJob("f", "r"));
        var blocked = await c.ReserveAndBeginAsync(4);
        Assert.Equal((StoreStatus.CapacityBlocked, StoreEffect.ObservedNoEffect), (blocked.Status, blocked.Effect));
        Assert.Equal(more[1], blocked.Error!.ResumeAfter!.JobId);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = ANY(@j)", ("j", more)));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-02")]
    public async Task N_bounds_the_claim_and_a_locked_candidate_is_skipped_without_waiting()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab02"));
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, [.. Enumerable.Range(0, 7).Select(i => (IQueueJob)new PingJob("n" + i))]);

        Assert.Equal(StoreStatus.InvalidRequest, (await c.ReserveAndBeginAsync(0)).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.ReserveAndBeginAsync(IReserveAndBeginStore.MaxJobs + 1)).Status);

        var three = await c.ReserveAndBeginAsync(3);
        Assert.Equal(ids[..3], three.Value!.Outcomes.Select(x => x.JobId));

        // A batch held after its locks keeps its two candidates locked; a second batch takes the rest at once.
        var other = await f.OpenClient(configure: Register);
        var hold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.ReserveAndBegin));
        var held = Task.Run(() => c.ReserveAndBeginAsync(2));
        await DriverFixture.WaitAtBarrier(hold);
        var rest = await other.ReserveAndBeginAsync(IReserveAndBeginStore.MaxJobs);
        Assert.Equal(ids[5..], rest.Value!.Outcomes.Select(x => x.JobId));
        DriverFixture.ReleaseBarrier(hold);
        Assert.Equal(ids[3..5], (await held).Value!.Outcomes.Select(x => x.JobId));
        Assert.All(three.Value.Outcomes.Concat(rest.Value.Outcomes), x => Assert.Equal(StoreStatus.Applied, x.Status));

        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAndBeginAsync(4)).Status);
        Assert.Equal(7, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-03")]
    public async Task A_rate_deferral_inside_a_batch_charges_nothing_and_leaves_the_others_begun()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab03"));
        var c = await f.OpenClient(configure: Register);
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None)).Status);

        // Rate 2 per minute: the first two rated jobs are granted, the third and fourth deferred; the unrated job begins.
        var ids = await DispatchAsync(c, new BatchRatedJob("r1"), new PingJob("p"), new BatchRatedJob("r2"), new BatchRatedJob("r3"),
            new BatchRatedJob("r4"));
        var reply = await c.ReserveAndBeginAsync(8);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.Applied, StoreStatus.Applied, StoreStatus.RateLimited, StoreStatus.RateLimited],
            o.Select(x => x.Status));
        foreach (var x in o.Where(x => x.Status == StoreStatus.RateLimited))
        {
            Assert.Equal("rate_limited", x.Error!.Code);
            Assert.True(x.Deferred!.EligibleAt >= reply.DecidedAt + TimeSpan.FromMilliseconds(250));
            Assert.Single(x.Deferred.BlockingScopes);
            Assert.Equal(JobStates.Pending, await StateAsync(f, x.JobId));
            Assert.Equal(0, await f.CountAsync("SELECT started_count FROM runwick.job_generations WHERE job_id = @j", ("j", x.JobId)));
            Assert.Equal(1, await f.CountAsync(
                "SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j AND outcome = 'admission_deferred'", ("j", x.JobId)));
        }

        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.rate_grants"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-04")]
    public async Task Cancellation_inside_a_batch_reaches_only_its_own_job()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab04"));
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("w"), new KeyedJob("k", "key-1"), new PingJob("x"), new PingJob("y"), new PingJob("z"));

        // A cancel of a waiting job before the batch: it is closed at once and never a candidate.
        Assert.Equal(StoreStatus.Applied, (await c.Store.CancelAsync(c.Command(new CancelRequest(ids[0], 1, (await PositionAsync(f, ids[0])).Revision, Actor)),
            CancellationToken.None)).Status);

        // A cancel that arrives while the batch holds its locks waits for it, then finds its job running: only that job
        // carries the request, and only its next Renew reports it.
        var hold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.ReserveAndBegin));
        var batch = Task.Run(() => c.ReserveAndBeginAsync(8));
        await DriverFixture.WaitAtBarrier(hold);
        var position = await PositionAsync(f, ids[2]);
        var cancel = Task.Run(() => c.Store.CancelAsync(c.Command(new CancelRequest(ids[2], 1, position.Revision + 2, Actor)), CancellationToken.None));
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype IN ('tuple', 'transactionid')", 1, "cancel waiting on the batch");
        DriverFixture.ReleaseBarrier(hold);
        var reply = await batch;
        Assert.Equal(ids[1..], reply.Value!.Outcomes.Select(x => x.JobId));
        Assert.All(reply.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
        var cancelled = await cancel;
        Assert.Equal(StoreStatus.Applied, cancelled.Status);
        Assert.Equal("cancel_requested", cancelled.Value!.Effect);
        foreach (var x in reply.Value.Outcomes)
        {
            var renewed = await c.RenewAsync(x.Begun!.Lease.Handle);
            Assert.Equal(x.JobId == ids[2], renewed.Value!.CancellationRequested);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(x.Begun.Lease.Handle,
                x.JobId == ids[2] ? AttemptOutcome.UserCancelled : AttemptOutcome.Succeeded)).Status);
        }

        Assert.Equal(JobStates.Cancelled, await StateAsync(f, ids[2]));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-05")]
    public async Task Begins_that_close_get_their_own_outcome_and_release_their_key()
    {
        // Begin's closing plans, reached by setting their causes on waiting rows (a waiting job never carries them in
        // normal operation; Begin/1 checks them all the same): a recorded cancellation request, and a spent budget.
        await using var f = await DriverFixture.StartAsync(Ns("rab05"));
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("a"), new KeyedJob("k", "key-1"), new PingJob("b"), new PingJob("c"));
        await f.ExecAsync("UPDATE runwick.jobs SET cancel_requested_at = now() WHERE job_id = @j", ("j", ids[1]));
        await f.ExecAsync("UPDATE runwick.job_generations SET started_count = max_executions WHERE job_id = @j", ("j", ids[2]));

        var reply = await c.ReserveAndBeginAsync(4);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.InvalidState, StoreStatus.BudgetExhausted, StoreStatus.Applied], o.Select(x => x.Status));
        Assert.Equal(("cancelled_before_start", "cancelled_before_start"), (o[1].Error!.Code, o[1].Closed!.TerminalReason));
        Assert.Equal(("execution_budget_exhausted", "abandoned_before_start"), (o[2].Error!.Code, o[2].Closed!.AttemptOutcome));
        Assert.Equal(JobStates.Cancelled, await StateAsync(f, ids[1]));
        Assert.Equal(JobStates.Failed, await StateAsync(f, ids[2]));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'key-1' AND bound_job_id IS NULL"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = ANY(@j)", ("j", ids[1..3])));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'"));

        // The two forced rows are outside the ReferenceModel's reachable states; every other history must be accepted.
        var violations = await f.HistoryViolations();
        Assert.DoesNotContain(violations, v => !v.Contains(ids[1].ToString("N"), StringComparison.Ordinal) && !v.Contains(ids[2].ToString("N"), StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-06")]
    public async Task A_lost_reply_is_resolved_by_the_receipt_with_every_outcome()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab06"));
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new ResourceJob("a", "r"), new ResourceJob("b", "r"), new PingJob("c"));
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
        Assert.Equal(1, drop.Fired);
        Assert.Equal((id, Operations.ReserveAndBegin), (unknown.CommandId, unknown.Operation));

        // The same command again: the recorded outcomes, with the payloads, and nothing new applied.
        var replay = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        var o = replay.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.CapacityBlocked, StoreStatus.Applied], o.Select(x => x.Status));
        var attempts = await TextAsync(f, "SELECT attempt_id::text FROM runwick.job_attempts ORDER BY attempt_id");
        Assert.Equal(attempts, o.Where(x => x.Begun is not null).Select(x => x.Begun!.Lease.Handle.AttemptId.ToString()).Order());
        Assert.All(o.Where(x => x.Reserved is not null), x => Assert.NotEmpty(x.Reserved!.PayloadUtf8));
        Assert.Equal(1, await Receipts(f, id));
        Assert.Equal(JobStates.Pending, await StateAsync(f, ids[1]));

        // A replayed lease is historical: the worker confirms it with a fresh Renew before running anything.
        foreach (var x in o.Where(x => x.Begun is not null))
        {
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(x.Begun!.Lease.Handle)).Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(x.Begun.Lease.Handle, AttemptOutcome.Succeeded)).Status);
        }

        // A changed body under the same ID is a conflict.
        Assert.Equal(StoreStatus.CommandConflict, (await c.ReserveAndBeginAsync(2, commandId: id, notAfter: notAfter)).Status);
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-07")]
    public async Task A_fault_after_the_second_job_rolls_the_whole_command_back()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab07"));
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"));
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);

        // The server hook after job 2's Reserve and Begin (AfterMutation step 2) throws: nothing of jobs 1 and 2 commits.
        var hook = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { CommandId = id, Step = 2 });
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
        Assert.Equal(1, hook.Fired);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));
        Assert.Equal(0, await Receipts(f, id));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE fencing_counter > 0"));

        // Known not applied: the same command may be sent again and applies once.
        var again = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, false), (again.Status, again.Replayed));
        Assert.Equal(ids, again.Value!.Outcomes.Select(x => x.JobId));
        Assert.All(again.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-08")]
    public async Task Concurrent_batches_and_single_claims_never_deadlock_or_overrun_a_scope()
    {
        // 48 jobs over 6 shared resources of capacity 2 and 12 pair jobs locking two resources in both declaration orders,
        // claimed by four batches and two single-claim loops at once.
        await using var f = await DriverFixture.StartAsync(Ns("rab08"));
        var c = await f.OpenClient(configure: Register);
        var jobs = new List<IQueueJob>();
        for (var i = 0; i < 36; i++)
        {
            jobs.Add(new SharedJob("s" + i, "res" + (i % 6)));
        }

        for (var i = 0; i < 12; i++)
        {
            jobs.Add(i % 2 == 0 ? new PairJob("p" + i, "pa" + (i % 3), "pb" + (i % 3)) : new PairJob("p" + i, "pb" + (i % 3), "pa" + (i % 3)));
        }

        await DispatchAsync(c, [.. jobs]);
        var clients = new List<TestClient>();
        for (var i = 0; i < 6; i++)
        {
            clients.Add(await f.OpenClient(configure: Register));
        }

        var tasks = clients.Select((w, i) => Task.Run(async () =>
        {
            for (var round = 0; round < 40; round++)
            {
                var begun = new List<LeaseHandle>();
                if (i < 4)
                {
                    var r = await w.ReserveAndBeginAsync(8);
                    begun.AddRange((r.Value?.Outcomes ?? []).Where(x => x.Begun is not null).Select(x => x.Begun!.Lease.Handle));
                }
                else if ((await w.ReserveAsync()).Value is { } reserved && (await w.BeginAsync(reserved.Lease.Handle)).Reply.Value is { } b)
                {
                    begun.Add(b.Lease.Handle);
                }

                // Live permits never exceed a scope's capacity.
                Assert.Equal(0, await f.CountAsync("""
                    SELECT count(*) FROM runwick.concurrency_scopes AS s
                    WHERE (SELECT count(*) FROM runwick.concurrency_permits AS p
                           WHERE p.scope_id = s.scope_id AND p.lease_expires_at > clock_timestamp()) > s.capacity
                    """));
                foreach (var l in begun)
                {
                    Assert.Equal(StoreStatus.Applied, (await w.FinishAsync(l, AttemptOutcome.Succeeded)).Status);
                }
            }
        })).ToList();
        await Task.WhenAll(tasks);
        var deadlocks = clients.Sum(w => w.Store.Runner.Deadlocks);
        foreach (var w in clients)
        {
            await w.DisposeAsync();
        }

        // Lock order holds: no deadlock was detected (the runner would have retried one), and every job ran exactly once.
        Assert.Equal(0, deadlocks);
        Assert.Equal(48, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts WHERE authorized_at IS NOT NULL GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(await f.HistoryViolations());
    }

    static Runwick.Harness.Engine.WorkerHost Host(DriverFixture f, int concurrency) =>
        Runwick.Harness.Engine.WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency);

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-09")]
    public async Task A_worker_with_free_slots_claims_them_in_one_command()
    {
        // 05 §9.6: with more than one free slot the worker sends ReserveAndBegin for those slots; every job runs once.
        await using var f = await DriverFixture.StartAsync(Ns("rab09"));
        await using var w = Host(f, concurrency: 8);
        const int jobs = 40;
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new Runwick.Harness.Engine.WorkJob("j" + n)))]);
        await w.StartAsync();
        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => w.Script.Invocations.Count >= jobs, TimeSpan.FromSeconds(90), "every job invoked");
        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == jobs,
            TimeSpan.FromSeconds(60), "every job settled");

        var pool = w.Engine.Pools.Single();
        Assert.True(Interlocked.Read(ref pool.BatchClaims) > 0);
        Assert.True(await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'ReserveAndBegin'") > 0);
        Assert.True(w.Engine.AdmissionOpen);
        Assert.Equal(jobs, w.Script.Invocations.Count);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts WHERE authorized_at IS NOT NULL GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-10")]
    public async Task A_non_applied_batch_reply_falls_back_to_reserve_and_begin()
    {
        // Any reply other than Applied or a Reserve-like no-effect (here: the operation missing from the schema, which
        // the store reports as UnsupportedVersion) turns the pool to Reserve then Begin; admission stays open.
        await using var f = await DriverFixture.StartAsync(Ns("rab10"));
        await using var w = Host(f, concurrency: 8);
        await w.StartAsync();

        // After startup's schema check, before any claim: the store now answers 42883 for the batched claim.
        await f.ExecAsync("ALTER FUNCTION runwick.reserve_and_begin_v2 RENAME TO reserve_and_begin_v2_hidden");
        const int jobs = 12;
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new Runwick.Harness.Engine.WorkJob("j" + n)))]);
        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == jobs,
            TimeSpan.FromSeconds(90), "every job settled through Reserve and Begin");

        var pool = w.Engine.Pools.Single();
        Assert.Equal(0, Interlocked.Read(ref pool.BatchClaims));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'ReserveAndBegin'"));
        Assert.True(await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Begin'") >= jobs);
        Assert.True(w.Engine.AdmissionOpen);
        Assert.Contains(w.Engine.Health().Diagnostics, d => d.Contains("ReserveAndBegin returned UnsupportedVersion", StringComparison.Ordinal));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RAB-11")]
    public async Task The_set_based_claim_writes_exactly_what_the_per_job_claim_writes()
    {
        // 04 Changes (2026-11-07, set-based claim): runs of plain jobs are claimed by a few statements each, the others one
        // by one. From the same rows, the same batch with the set path off (the fault-hooks variant's switch) and on must
        // leave identical rows in every table the claim writes and give an identical reply, apart from the fresh attempt
        // IDs and the decision time, which are replaced by names. Runs of plain jobs sit around a scoped and a rated job.
        await using var f = await DriverFixture.StartAsync(Ns("rab11"));
        var c = await f.OpenClient(configure: Register);
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None)).Status);
        var ids = await DispatchAsync(c, new PingJob("a"), new PingJob("b"), new ResourceJob("c", "r"), new PingJob("d"),
            new BatchRatedJob("e"), new PingJob("f"), new PingJob("g"));

        var worker = Guid.CreateVersion7();
        var tokens = Enumerable.Range(0, 8).Select(_ => Guid.NewGuid()).ToArray();
        var perJob = await ClaimInRolledBackTransactionAsync(f, ids, worker, tokens, perJob: true);
        var setBased = await ClaimInRolledBackTransactionAsync(f, ids, worker, tokens, perJob: false);
        Assert.Equal(perJob.Dump, setBased.Dump);
        Assert.Equal(7, perJob.Dump.Split("\"attempt_started\"").Length - 1);
        Assert.Single(perJob.Dump.Split("\"fencing_value\"").Skip(1));

        // Which path ran: per job, Reserve and Begin seven times; set-based, three runs and only the scoped and rated jobs alone.
        Assert.Equal((0L, 7L, 7L), perJob.Calls);
        Assert.Equal((3L, 2L, 2L), setBased.Calls);

        // Committed through the store, the history is one the ReferenceModel accepts.
        var reply = await c.ReserveAndBeginAsync(8);
        Assert.Equal(ids, reply.Value!.Outcomes.Select(x => x.JobId));
        Assert.All(reply.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
        Assert.Empty(await f.HistoryViolations());
    }

    /// <summary>
    /// Calls reserve_and_begin_v2 directly in a transaction that is rolled back, so both paths start from the same rows.
    /// Returns the reply and every row the claim writes as normalized JSON, and the calls of the three claim functions.
    /// </summary>
    static async Task<(string Dump, (long Plain, long Reserve, long Begin) Calls)> ClaimInRolledBackTransactionAsync(
        DriverFixture f, Guid[] ids, Guid worker, Guid[] tokens, bool perJob)
    {
        await using var conn = new NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        var command = Guid.CreateVersion7();

        async Task<string> Scalar(string sql)
        {
            await using var cmd = new NpgsqlCommand(sql, conn, tx);
            cmd.Parameters.AddWithValue("app", f.Application);
            cmd.Parameters.AddWithValue("cmd", command);
            cmd.Parameters.AddWithValue("ids", ids);
            cmd.Parameters.AddWithValue("worker", worker);
            cmd.Parameters.AddWithValue("tokens", tokens);
            cmd.Parameters.AddWithValue("perjob", perJob ? "on" : "off");
            cmd.Parameters.AddWithValue("fp", System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
            return (await cmd.ExecuteScalarAsync())?.ToString() ?? "";
        }

        // The backend's unflushed function counts may include an earlier transaction's: count the difference.
        const string CallsSql = """
            SELECT concat_ws(',', coalesce(pg_stat_get_xact_function_calls('runwick.rw_claim_plain_v1'::regproc), 0),
                coalesce(pg_stat_get_xact_function_calls('runwick.rw_reserve_job_v1'::regproc), 0),
                coalesce(pg_stat_get_xact_function_calls('runwick.rw_begin_job_v1'::regproc), 0))
            """;
        static long[] Parse(string s) => [.. s.Split(',').Select(x => long.Parse(x, System.Globalization.CultureInfo.InvariantCulture))];
        await Scalar("SELECT set_config('runwick.claim_per_job', @perjob, true) || set_config('track_functions', 'pl', true)");
        var before = Parse(await Scalar(CallsSql));
        var reply = await Scalar("""
            SELECT jsonb_pretty(jsonb_agg(to_jsonb(r) ORDER BY r.status = 'Payload', r.conflicting_job_id))
            FROM runwick.reserve_and_begin_v2(@app, @cmd, 'ReserveAndBegin', runwick.ms_floor(clock_timestamp() + interval '30 seconds'), @fp,
                'ownership', interval '60 seconds', NULL, ARRAY[(SELECT schema_revision FROM runwick.store_metadata)],
                (SELECT protocol_epoch FROM runwick.store_metadata), '{"rw:harness": 1}'::jsonb, false, false, false,
                'default', (SELECT min(priority) FROM runwick.jobs WHERE job_id = ANY (@ids)), NULL, NULL,
                ARRAY(SELECT job_name::text FROM runwick.jobs WHERE job_id = ANY (@ids) ORDER BY job_id),
                ARRAY(SELECT payload_version FROM runwick.jobs WHERE job_id = ANY (@ids) ORDER BY job_id),
                ARRAY(SELECT serializer_profile_id::text FROM runwick.jobs WHERE job_id = ANY (@ids) ORDER BY job_id),
                ARRAY(SELECT serializer_profile_version FROM runwick.jobs WHERE job_id = ANY (@ids) ORDER BY job_id),
                interval '30 seconds', @worker, @tokens, interval '250 milliseconds', array_fill(0, ARRAY[8])) AS r
            """);
        var calls = Parse(await Scalar(CallsSql)).Zip(before, (a, b) => a - b).ToArray();
        var dump = await Scalar("""
            SELECT jsonb_pretty(jsonb_build_object(
                'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY j.job_id) FROM runwick.jobs AS j WHERE j.job_id = ANY (@ids)),
                'generations', (SELECT jsonb_agg(to_jsonb(g) ORDER BY g.job_id) FROM runwick.job_generations AS g WHERE g.job_id = ANY (@ids)),
                'attempts', (SELECT jsonb_agg(to_jsonb(a) ORDER BY a.job_id) FROM runwick.job_attempts AS a WHERE a.job_id = ANY (@ids)),
                'events', (SELECT jsonb_agg(to_jsonb(e) - 'event_id' || jsonb_build_object('seq', e.event_id - m.first) ORDER BY e.event_id)
                           FROM runwick.queue_events AS e,
                                (SELECT min(x.event_id) AS first FROM runwick.queue_events AS x WHERE x.command_id = @cmd) AS m
                           WHERE e.command_id = @cmd),
                'scopes', (SELECT jsonb_agg(to_jsonb(s) ORDER BY s.scope_id) FROM runwick.concurrency_scopes AS s),
                'permits', (SELECT jsonb_agg(to_jsonb(p) ORDER BY p.scope_id, p.job_id) FROM runwick.concurrency_permits AS p),
                'rateScopes', (SELECT jsonb_agg(to_jsonb(s) ORDER BY s.rate_scope_id) FROM runwick.rate_scopes AS s),
                'grants', (SELECT jsonb_agg(to_jsonb(g) ORDER BY g.rate_scope_id, g.job_id) FROM runwick.rate_grants AS g),
                'receipt', (SELECT jsonb_build_object('status', s.status, 'result', s.result, 'decided_at', s.decided_at)
                            FROM runwick.storage_commands AS s WHERE s.command_id = @cmd)))
            """);
        var names = await Scalar("""
            SELECT jsonb_build_object(
                'attempts', (SELECT jsonb_object_agg(a.attempt_id, a.job_id) FROM runwick.job_attempts AS a WHERE a.job_id = ANY (@ids)),
                'now', to_jsonb(s.decided_at) #>> '{}', 'nowMs', runwick.rw_ms_v1(s.decided_at)::text,
                'lease', to_jsonb(runwick.ms_floor(s.decided_at + interval '30 seconds')) #>> '{}',
                'leaseMs', runwick.rw_ms_v1(runwick.ms_floor(s.decided_at + interval '30 seconds'))::text,
                'tx', '"tx_id": "' || pg_current_xact_id() || '"')::text
            FROM runwick.storage_commands AS s WHERE s.command_id = @cmd
            """);
        await tx.RollbackAsync();

        var n = System.Text.Json.Nodes.JsonNode.Parse(names)!;
        var text = reply + "\n" + dump;
        foreach (var (attempt, job) in n["attempts"]!.AsObject())
        {
            text = text.Replace(attempt, "attempt-of-" + job!.GetValue<string>(), StringComparison.Ordinal);
        }

        foreach (var key in new[] { "now", "nowMs", "lease", "leaseMs", "tx" })
        {
            text = text.Replace(n[key]!.GetValue<string>(), "<" + key + ">", StringComparison.Ordinal);
        }

        return (text.Replace(command.ToString(), "<command>", StringComparison.Ordinal), (calls[0], calls[1], calls[2]));
    }
}
