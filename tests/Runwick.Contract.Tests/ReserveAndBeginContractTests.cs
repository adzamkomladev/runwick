using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Registration;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>A job-type rate scope of 2 per minute, for deferral inside a batch.</summary>
public sealed record BatchRatedJob(string Id) : IQueueJob;

/// <summary>
/// RAB-02..09 (PF-06, 07 "batched claim"): ReserveAndBegin/1 claims up to N jobs and begins each; every job gets exactly the outcome Reserve/1
/// then Begin/1 would give it, and no outcome affects another. Ported from the PostgreSQL suite as theories over <see cref="StoreKind"/>.
/// PostgreSQL skips locked candidates; SQLite and Redis serialize whole commands (20 §1, 22 §4.2), so there a second command waits and then
/// claims the rest: the claimed sets are the same, only the waiting differs.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class ReserveAndBeginContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly ControlActor Actor = new("user:batch", null, "host", "rab");

    static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<BatchRatedJob>().Named("test.batch-rated").ToQueue("default").RollingWindow(2, TimeSpan.FromMinutes(1));
    }

    static async Task<string> StateAsync(DriverFixture f, Guid job) => (await Model(f)).Job(job).State;

    static async Task<int> AttemptsOf(DriverFixture f, Guid job) => (await Model(f)).AttemptsOf(job).Count();

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

    static IJobControlStore Control(TestClient c) => (IJobControlStore)c.Driver;

    static IRateControlStore Rates(TestClient c) => (IRateControlStore)c.Driver;

    /// <summary>Every rate grant in the store (SQL stores count the table; Redis sums its per-scope grant sets).</summary>
    static async Task<long> Grants(DriverFixture f) => f.Kind == StoreKind.Redis
        ? (await f.ReadCommittedSnapshot()).Tables.Where(t => t.Key.StartsWith("grants:", StringComparison.Ordinal)).Sum(t => t.Value.Count)
        : await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.rate_grants"));

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-02")]
    public async Task N_bounds_the_claim_and_a_locked_candidate_is_skipped_without_waiting(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab02"), kind);
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, [.. Enumerable.Range(0, 7).Select(i => (IQueueJob)new PingJob("n" + i))]);

        Assert.Equal(StoreStatus.InvalidRequest, (await c.ReserveAndBeginAsync(0)).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.ReserveAndBeginAsync(IReserveAndBeginStore.MaxJobs + 1)).Status);

        var three = await c.ReserveAndBeginAsync(3);
        Assert.Equal(ids[..3], three.Value!.Outcomes.Select(x => x.JobId));

        // A batch held inside the store keeps its two candidates; a second batch gets the rest.
        var other = await f.OpenClient(configure: Register);
        StoreReply<ClaimedJobs> held;
        StoreReply<ClaimedJobs> rest;
        if (kind == StoreKind.Postgres)
        {
            var hold = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.ReserveAndBegin));
            var heldTask = Task.Run(() => c.ReserveAndBeginAsync(2));
            await DriverFixture.WaitAtBarrier(hold);
            rest = await other.ReserveAndBeginAsync(IReserveAndBeginStore.MaxJobs);
            DriverFixture.ReleaseBarrier(hold);
            held = await heldTask;
        }
        else
        {
            (held, rest) = await FinishManyContractTests.OverlapAsync(f, c, Operations.ReserveAndBegin,
                () => c.ReserveAndBeginAsync(2), () => other.ReserveAndBeginAsync(IReserveAndBeginStore.MaxJobs));
        }

        Assert.Equal(ids[5..], rest.Value!.Outcomes.Select(x => x.JobId));
        Assert.Equal(ids[3..5], held.Value!.Outcomes.Select(x => x.JobId));
        Assert.All(three.Value.Outcomes.Concat(rest.Value.Outcomes), x => Assert.Equal(StoreStatus.Applied, x.Status));

        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAndBeginAsync(4)).Status);
        Assert.Equal(7, (await Model(f)).Jobs.Count(j => j.State == "running"));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-03")]
    public async Task A_rate_deferral_inside_a_batch_charges_nothing_and_leaves_the_others_begun(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab03"), kind);
        var c = await f.OpenClient(configure: Register);
        Assert.Equal(StoreStatus.Applied, (await Rates(c).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None)).Status);

        // Rate 2 per minute: the first two rated jobs are granted, the third and fourth deferred; the unrated job begins.
        var ids = await DispatchAsync(c, new BatchRatedJob("r1"), new PingJob("p"), new BatchRatedJob("r2"), new BatchRatedJob("r3"),
            new BatchRatedJob("r4"));
        var reply = await c.ReserveAndBeginAsync(8);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.Applied, StoreStatus.Applied, StoreStatus.RateLimited, StoreStatus.RateLimited],
            o.Select(x => x.Status));
        var model = await Model(f);
        foreach (var x in o.Where(x => x.Status == StoreStatus.RateLimited))
        {
            Assert.Equal("rate_limited", x.Error!.Code);
            Assert.True(x.Deferred!.EligibleAt >= reply.DecidedAt + TimeSpan.FromMilliseconds(250));
            Assert.Single(x.Deferred.BlockingScopes);
            Assert.Equal(JobStates.Pending, model.Job(x.JobId).State);
            Assert.Equal(0, model.Job(x.JobId).StartedCount);
            Assert.Equal(1, model.AttemptsOf(x.JobId).Count(a => a.Outcome == "admission_deferred"));
        }

        Assert.Equal(2, await Grants(f));
        Assert.Equal(3, model.Jobs.Count(j => j.State == "running"));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-04")]
    public async Task Cancellation_inside_a_batch_reaches_only_its_own_job(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab04"), kind);
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("w"), new KeyedJob("k", "key-1"), new PingJob("x"), new PingJob("y"), new PingJob("z"));

        // A cancel of a waiting job before the batch: it is closed at once and never a candidate.
        var first = (await Model(f)).Job(ids[0]);
        Assert.Equal(StoreStatus.Applied, (await Control(c).CancelAsync(c.Command(new CancelRequest(ids[0], 1, first.Revision, Actor)), CancellationToken.None)).Status);

        // A cancel that arrives while the batch holds its locks waits for it, then finds its job running: only that job
        // carries the request, and only its next Renew reports it.
        var revision = (await Model(f)).Job(ids[2]).Revision;
        var (reply, cancelled) = await FinishManyContractTests.OverlapAsync(f, c, Operations.ReserveAndBegin,
            () => c.ReserveAndBeginAsync(8),
            () => Control(c).CancelAsync(c.Command(new CancelRequest(ids[2], 1, revision + 2, Actor)), CancellationToken.None));
        Assert.Equal(ids[1..], reply.Value!.Outcomes.Select(x => x.JobId));
        Assert.All(reply.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
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
        Assert.Equal(3, (await Model(f)).Jobs.Count(j => j.State == "succeeded"));
        Assert.Empty(await History(f));
    }

    /// <summary>Setup: gives a waiting job a spent execution budget (the generation's started count reaches its maximum).</summary>
    static Task SpendBudgetAsync(DriverFixture f, Guid job) => f.Kind switch
    {
        StoreKind.Redis => RedisAdmin.SpendBudget(f.Application, job),
        StoreKind.Sqlite => f.ExecAsync("UPDATE runwick_job_generations SET started_count = max_executions WHERE job_id = @j", ("j", job)),
        _ => f.ExecAsync("UPDATE runwick.job_generations SET started_count = max_executions WHERE job_id = @j", ("j", job)),
    };

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-05")]
    public async Task Begins_that_close_get_their_own_outcome_and_release_their_key(StoreKind kind)
    {
        // Begin's closing plans, reached by setting their causes on waiting rows (a waiting job never carries them in
        // normal operation; Begin/1 checks them all the same): a recorded cancellation request, and a spent budget.
        await using var f = await DriverFixture.StartAsync(Ns("rab05"), kind);
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("a"), new KeyedJob("k", "key-1"), new PingJob("b"), new PingJob("c"));
        await FinishManyContractTests.ForceCancelRequestAsync(f, ids[1]);
        await SpendBudgetAsync(f, ids[2]);

        var reply = await c.ReserveAndBeginAsync(4);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.InvalidState, StoreStatus.BudgetExhausted, StoreStatus.Applied], o.Select(x => x.Status));
        Assert.Equal(("cancelled_before_start", "cancelled_before_start"), (o[1].Error!.Code, o[1].Closed!.TerminalReason));
        Assert.Equal(("execution_budget_exhausted", "abandoned_before_start"), (o[2].Error!.Code, o[2].Closed!.AttemptOutcome));
        var model = await Model(f);
        Assert.Equal(JobStates.Cancelled, model.Job(ids[1]).State);
        Assert.Equal(JobStates.Failed, model.Job(ids[2]).State);
        Assert.DoesNotContain(model.DispatchKeys, k => k.Key == "key-1" && k.Active);
        Assert.DoesNotContain(model.Permits, p => p.JobId == ids[1] || p.JobId == ids[2]);
        Assert.Equal(2, model.Jobs.Count(j => j.State == "running"));

        // The two forced rows are outside the ReferenceModel's reachable states; every other history must be accepted.
        var violations = await History(f);
        Assert.DoesNotContain(violations, v => !v.Contains(ids[1].ToString("N"), StringComparison.Ordinal) && !v.Contains(ids[2].ToString("N"), StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-06")]
    public async Task A_lost_reply_is_resolved_by_the_receipt_with_every_outcome(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab06"), kind);
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new ResourceJob("a", "r"), new ResourceJob("b", "r"), new PingJob("c"));
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        using (var loss = LoseReply(f, c, id))
        {
            var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
            if (kind != StoreKind.Redis)
            {
                Assert.Equal(1, loss.Fired);
            }

            Assert.Equal((id, Operations.ReserveAndBegin), (unknown.CommandId, unknown.Operation));
        }

        // The same command again: the recorded outcomes, with the payloads, and nothing new applied.
        var replay = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        var o = replay.Value!.Outcomes;
        Assert.Equal([StoreStatus.Applied, StoreStatus.CapacityBlocked, StoreStatus.Applied], o.Select(x => x.Status));
        var model = await Model(f);
        Assert.Equal(model.Attempts.Select(a => a.AttemptId).Order(), o.Where(x => x.Begun is not null).Select(x => x.Begun!.Lease.Handle.AttemptId).Order());
        Assert.All(o.Where(x => x.Reserved is not null), x => Assert.NotEmpty(x.Reserved!.PayloadUtf8));
        Assert.Equal(1, await Receipts(f, id));
        Assert.Equal(JobStates.Pending, model.Job(ids[1]).State);

        // A replayed lease is historical: the worker confirms it with a fresh Renew before running anything.
        foreach (var x in o.Where(x => x.Begun is not null))
        {
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(x.Begun!.Lease.Handle)).Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(x.Begun.Lease.Handle, AttemptOutcome.Succeeded)).Status);
        }

        // A changed body under the same ID is a conflict.
        Assert.Equal(StoreStatus.CommandConflict, (await c.ReserveAndBeginAsync(2, commandId: id, notAfter: notAfter)).Status);
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-07")]
    public async Task A_fault_after_the_second_job_rolls_the_whole_command_back(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rab07"), kind);
        var c = await f.OpenClient(configure: Register);
        var ids = await DispatchAsync(c, new PingJob("a"), new ResourceJob("b", "r"), new PingJob("c"));
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);

        if (kind == StoreKind.Redis)
        {
            // Redis keeps the whole decision as a stored intent and finishes it on the next command (22 §4), so there is no rollback: a cut after two
            // writes leaves the call unknown, a reader sees none of the jobs or all of them, and the same command completes the intent exactly once.
            var store = c.Services.GetRequiredService<RedisStore>();
            store.Runner.Client.FailAfter = 2;
            try
            {
                await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            var done = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
            Assert.Equal(ids, done.Value!.Outcomes.Select(x => x.JobId));
            Assert.All(done.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
            Assert.Equal(3, (await Model(f)).Attempts.Count);
            Assert.Equal(1, await Receipts(f, id));
            Assert.Empty(await History(f));
        }
        else
        {
            // The server hook after job 2's Reserve and Begin (AfterMutation step 2) throws: nothing of jobs 1 and 2 commits.
            var hook = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { CommandId = id, Step = 2 });
            await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
            Assert.Equal(1, hook.Fired);
            Assert.Equal(0, await Attempts(f));
            Assert.Equal(3, (await Model(f)).Jobs.Count(j => j.State == "pending"));
            Assert.Equal(0, await Receipts(f, id));
            Assert.Equal(0, await f.CountAsync(Q(f, "SELECT count(*) FROM runwick.concurrency_scopes WHERE fencing_counter > 0")));

            // Known not applied: the same command may be sent again and applies once.
            var again = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
            Assert.Equal((StoreStatus.Applied, false), (again.Status, again.Replayed));
            Assert.Equal(ids, again.Value!.Outcomes.Select(x => x.JobId));
            Assert.All(again.Value.Outcomes, x => Assert.Equal(StoreStatus.Applied, x.Status));
            Assert.Empty(await History(f));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-08")]
    public async Task Concurrent_batches_and_single_claims_never_deadlock_or_overrun_a_scope(StoreKind kind)
    {
        // 48 jobs over 6 shared resources of capacity 2 and 12 pair jobs locking two resources in both declaration orders,
        // claimed by four batches and two single-claim loops at once.
        await using var f = await DriverFixture.StartAsync(Ns("rab08"), kind);
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
                Assert.Equal(0, await OverCapacityScopes(f));
                foreach (var l in begun)
                {
                    Assert.Equal(StoreStatus.Applied, (await w.FinishAsync(l, AttemptOutcome.Succeeded)).Status);
                }
            }
        })).ToList();
        await Task.WhenAll(tasks);
        var deadlocks = clients.Sum(Deadlocks);
        foreach (var w in clients)
        {
            await w.DisposeAsync();
        }

        // Lock order holds: no deadlock was detected (the runner would have retried one), and every job ran exactly once.
        Assert.Equal(0, deadlocks);
        var model = await Model(f);
        Assert.Equal(48, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.DoesNotContain(model.Jobs, j => model.AttemptsOf(j.JobId).Count(a => a.BegunAt is not null) > 1);
        Assert.Empty(await History(f));
    }

    /// <summary>Scopes with more live permits than their capacity (should be none).</summary>
    static async Task<long> OverCapacityScopes(DriverFixture f) => f.Kind switch
    {
        StoreKind.Redis => await RedisAdmin.OverCapacityScopes(f.Application, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
        StoreKind.Sqlite => await f.CountAsync("""
            SELECT count(*) FROM runwick_concurrency_scopes AS s
            WHERE (SELECT count(*) FROM runwick_concurrency_permits AS p WHERE p.scope_id = s.scope_id AND p.lease_expires_at > @now) > s.capacity
            """, ("now", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())),
        _ => await f.CountAsync("""
            SELECT count(*) FROM runwick.concurrency_scopes AS s
            WHERE (SELECT count(*) FROM runwick.concurrency_permits AS p
                   WHERE p.scope_id = s.scope_id AND p.lease_expires_at > clock_timestamp()) > s.capacity
            """),
    };

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-09")]
    public async Task A_worker_with_free_slots_claims_them_in_one_command(StoreKind kind)
    {
        // 05 §9.6: with more than one free slot the worker sends ReserveAndBegin for those slots; every job runs once.
        await using var f = await DriverFixture.StartAsync(Ns("rab09"), kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 8);
        const int jobs = 40;
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count >= jobs, TimeSpan.FromSeconds(90), "every job invoked");
        await WorkerHost.WaitUntil(() => Model(f).Result.Jobs.Count(j => j.State == "succeeded") == jobs, TimeSpan.FromSeconds(60), "every job settled");

        var pool = w.Engine.Pools.Single();
        Assert.True(Interlocked.Read(ref pool.BatchClaims) > 0);
        Assert.Contains((await Model(f)).Receipts, r => r.Operation == "ReserveAndBegin");
        Assert.True(w.Engine.AdmissionOpen);
        Assert.Equal(jobs, w.Script.Invocations.Count);
        var model = await Model(f);
        Assert.DoesNotContain(model.Jobs, j => model.AttemptsOf(j.JobId).Count(a => a.BegunAt is not null) > 1);
        Assert.Empty(await History(f));
    }
}
