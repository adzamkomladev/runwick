using System.Text.Json.Nodes;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Harness.Timing;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Worker;

namespace Runwick.Postgres.Tests;

/// <summary>The worker engine against the real store with Fast timing (07 §10, §11, OWN-04, END-01/04): SP-009.</summary>
public sealed class WorkerTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static WorkerHost Host(DriverFixture f, int concurrency = 4, Action<JobBuilder<WorkJob>>? jobs = null,
        Action<WorkerOptions>? options = null, Action<RunwickBuilder>? configure = null,
        Action<Microsoft.Extensions.DependencyInjection.IServiceCollection>? services = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, jobs, options, configure, services);

    static async Task<Guid> Dispatch(WorkerHost w, string key) => (await w.DispatchAsync(key)).JobId.Value;

    static async Task<string> State(DriverFixture f, Guid job) =>
        (await TextAsync(f, "SELECT state FROM runwick.jobs WHERE job_id = @j", job)).Single();

    static async Task<List<string>> Outcomes(DriverFixture f, Guid job) =>
        await TextAsync(f, "SELECT coalesce(outcome, '<open>') FROM runwick.job_attempts WHERE job_id = @j ORDER BY reserved_at", job);

    static async Task<List<string>> TextAsync(DriverFixture f, string sql, Guid job)
    {
        await using var conn = new Npgsql.NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("j", job);
        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            list.Add(r.GetString(0));
        }

        return list;
    }

    static async Task WaitState(DriverFixture f, Guid job, string state) =>
        await WorkerHost.WaitUntil(() => State(f, job).GetAwaiter().GetResult() == state, Wait, $"job {job} {state}");

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "SMOKE")]
    public async Task Dispatched_jobs_run_to_success()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wsmoke"));
        await using var w = Host(f);
        await w.StartAsync();
        var ids = new List<Guid>();
        for (var i = 0; i < 10; i++)
        {
            ids.Add(await Dispatch(w, "j" + i));
        }

        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 10, Wait, "10 succeeded");
        Assert.Equal(10, w.Script.Invocations.Count);
        await w.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-01")]
    public async Task One_slow_handler_does_not_hold_the_other_tickets()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk01"));
        await using var w = Host(f, concurrency: 4);
        w.Script.On("slow", (_, _, _) => Task.Delay(TimeSpan.FromSeconds(10), CancellationToken.None));
        w.Script.Default = (_, _, _) => Task.Delay(10, CancellationToken.None);
        var slow = await Dispatch(w, "slow");
        for (var i = 0; i < 40; i++)
        {
            await Dispatch(w, "fast" + i);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Completed.Keys.Count(k => k.StartsWith("fast", StringComparison.Ordinal)) == 40, Wait, "40 fast jobs");
        var slowDone = w.Script.Completed.ContainsKey("slow");
        await WaitState(f, slow, JobStates.Succeeded);
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 41, Wait, "41 succeeded");
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", slow)));

        // Fast jobs did not wait for the slow one (unless the whole batch drained inside its 10 s), and renewals kept it alive.
        Assert.True(!slowDone || w.EventCount("renewed") > 0);
        Assert.True(w.EventCount("renewed") > 0, "the 10 s attempt renewed its 6 s lease");
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-03")]
    public async Task Lost_reserve_and_begin_replies_resolve_by_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk03"));
        await using var w = Host(f, concurrency: 1);
        var job = await Dispatch(w, "j");
        var reserve = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.Reserve));
        var begin = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.Begin));
        await w.StartAsync();
        await WaitState(f, job, JobStates.Succeeded);
        Assert.Equal((1, 1), (reserve.Fired, begin.Fired));
        Assert.Equal(1, w.Script.InvocationsOf("j"));
        Assert.Equal(["succeeded"], await Outcomes(f, job));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-04")]
    public async Task Shutdown_while_reserve_commits_releases_the_returned_claim()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk04"));
        await using var w = Host(f, concurrency: 1);
        var job = await Dispatch(w, "j");
        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Reserve));
        await w.StartAsync();
        await DriverFixture.WaitAtBarrier(h, Wait);
        var stop = w.StopAsync();
        await WorkerHost.WaitUntil(() => w.Engine.Draining, Wait, "drain");
        DriverFixture.ReleaseBarrier(h);
        await stop.WaitAsync(Wait);

        Assert.Equal(0, w.Script.InvocationsOf("j"));
        Assert.Equal(["released"], await Outcomes(f, job));
        Assert.Equal(JobStates.Pending, await State(f, job));
        Assert.Equal(0, await f.CountAsync("SELECT started_count + consecutive_unstarted_count FROM runwick.job_generations WHERE job_id = @j", ("j", job)));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-14")]
    public async Task Later_unblocked_type_is_discovered_under_a_type_ceiling()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk14"));
        var release = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 4, jobs: j => j.MaxConcurrent(1),
            configure: r => r.AddHandler<PingJob, ScriptedPingHandler>());
        w.Script.Default = (job, _, _) => job.Id.StartsWith("ping:", StringComparison.Ordinal) ? Task.CompletedTask : release.Task;

        // 20 capped jobs ahead of one ping job in the same lane; the first capped job holds the type scope.
        for (var i = 0; i < 20; i++)
        {
            await Dispatch(w, "a" + i);
        }

        await w.Dispatcher.DispatchAsync(new PingJob("b"));
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("ping:b"), Wait, "ping job discovered past the blocked candidates");
        Assert.Equal(1, w.Script.Invocations.Count(i => !i.JobKey.StartsWith("ping:", StringComparison.Ordinal)));
        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 21, TimeSpan.FromMinutes(3), "all succeeded");
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-16")]
    public async Task Polling_discovers_work_without_wake_signals()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk16"));
        await using var w = Host(f);
        var producer = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<WorkJob>().Named("test.work").ToQueue("default").WithMaxExecutions(3).WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        });
        await w.StartAsync();
        await Task.Delay(TimeSpan.FromSeconds(6));

        // Dispatched by another process-equivalent client: no local post-commit wake reaches the worker.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var job = (await producer.Dispatcher.DispatchAsync(new WorkJob("late"))).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("late") == 1, Wait, "discovery");
        var discovered = sw.Elapsed;
        await WaitState(f, job, JobStates.Succeeded);
        Assert.Equal(1, w.Script.InvocationsOf("late"));

        // Idle backoff max (2 s + 20 %) + readiness hint age (2 s) + one claim; slack for this host's measured RTT.
        Assert.True(discovered <= TimeSpan.FromSeconds(2.4 + 2 + 2), $"discovered after {discovered}");
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-18")]
    public async Task Host_killed_in_each_shutdown_phase_leaves_recoverable_leases()
    {
        // Phases from the stop signal at the scaled defaults (15 / 5 / 5 s ÷ divisor): kill inside each one.
        var div = TestTiming.RealStoreDivisor;
        int Ms(double seconds) => (int)(seconds * 1000 / div);
        foreach (var (phase, killAfter) in new[] { ("drain", Ms(4)), ("cancellation", Ms(17.5)), ("finalization", Ms(22.5)) })
        {
            await using var f = await DriverFixture.StartAsync(Ns("wrk18" + phase[..3]));
            var local = await f.OpenClient(configure: r =>
            {
                TestJobs.Register(r);
                r.AddJob<WorkJob>().Named("test.work").ToQueue("default").WithMaxExecutions(3).WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
            });
            var peer = await f.OpenPeer("w-" + phase);

            // Finishing: the handler is held until the unbegun claim exists (a Finish waiting ≥ 5 s stops new
            // reservations through the pressure gate, 05 §15), then returns, and its Finish waits in the store.
            await peer.SendAsync("armFault", new JsonObject { ["point"] = "BeforeGateLock", ["action"] = "Delay", ["operation"] = Operations.Finish, ["delayMs"] = 60_000, ["times"] = 100 });
            var finishing = (await local.Dispatcher.DispatchAsync(new WorkJob("hold-fin"))).JobId.Value;
            var begun = (await local.Dispatcher.DispatchAsync(new WorkJob("block"))).JobId.Value;
            await peer.SendAsync("startWorker", new JsonObject { ["cs"] = f.ClientConnectionString("w-" + phase), ["app"] = f.Application, ["concurrency"] = 3 });
            await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'").Result == 2, Wait, "two begun attempts");

            // Unbegun: its Begin waits in the store.
            await peer.SendAsync("armFault", new JsonObject { ["point"] = "BeforeGateLock", ["action"] = "Delay", ["operation"] = Operations.Begin, ["delayMs"] = 60_000, ["times"] = 100 });
            var unbegun = (await local.Dispatcher.DispatchAsync(new WorkJob("unbegun"))).JobId.Value;
            await WorkerHost.WaitUntil(() => State(f, unbegun).GetAwaiter().GetResult() == JobStates.Reserved, Wait, "reserved, not begun");
            await peer.SendAsync("releaseHeld");
            await Task.Delay(200);

            await peer.SendAsync("stopWorker");
            await Task.Delay(killAfter);
            peer.Kill();

            // Leases are recoverable afterwards (RUN-01 rules); nothing reports a success that did not durably happen.
            var deadline = DateTime.UtcNow + Wait;
            while (await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state IN ('reserved', 'running')") > 0)
            {
                Assert.True(DateTime.UtcNow < deadline, $"{phase}: leases never became recoverable");
                foreach (var expired in await local.Store.ListExpiredLeasesAsync(local.Application, 10, CancellationToken.None))
                {
                    await local.RecoverAsync(expired);
                }

                await Task.Delay(500);
            }

            // The blocked handler never finished: never Succeeded. The finishing attempt's handler did finish, so its frozen
            // Finish may still commit while resolution continues; otherwise it is abandoned_running. Recovery may have run it
            // again before the kill (drain then releases that reservation).
            Assert.Equal(["abandoned_running"], await Outcomes(f, begun));
            string[] allowed = ["abandoned_running", "abandoned_before_start", "released"];
            var fin = await Outcomes(f, finishing);
            Assert.Contains(fin[0], new[] { "abandoned_running", "succeeded" });
            Assert.All(fin.Skip(1), o => Assert.Contains(o, allowed));
            Assert.All(await Outcomes(f, unbegun), o => Assert.Contains(o, allowed));
            Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-10")]
    public async Task Long_job_without_timeout_renews_and_short_timeout_times_out()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time10"));

        // (a) No timeout; a 40 s handler outlives several lease durations (≈ 6.7 under Fast, ≈ 2.7 at ÷4) and renews
        //     at least once per RenewalIntervalMax (≥ 20 under Fast).
        await using (var w = Host(f, concurrency: 1))
        {
            w.Script.On("long", (_, _, _) => Task.Delay(TimeSpan.FromSeconds(40), CancellationToken.None));
            var job = await Dispatch(w, "long");
            await w.StartAsync();
            await WaitState(f, job, JobStates.Succeeded);
            var minimum = (int)(40 / w.Engine.Settings.RenewalIntervalMax.TotalSeconds) - 1;
            Assert.True(w.EventCount("renewed") >= minimum, $"{w.EventCount("renewed")} renewals; expected ≥ {minimum}");
            Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
            Assert.Equal(["succeeded"], await Outcomes(f, job));
        }

        // (b) ExecutionTimeout 2 s with lease 6 s: cancellation at 2 s, TimedOut.
        await using (var w = Host(f, concurrency: 1, jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(2))))
        {
            var cancelledAfter = new TaskCompletionSource<TimeSpan>();
            w.Script.On("short", async (_, _, ct) =>
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), ct);
                }
                catch (OperationCanceledException)
                {
                    cancelledAfter.TrySetResult(sw.Elapsed);
                }
            });
            var job = await Dispatch(w, "short");
            await w.StartAsync();
            await WaitState(f, job, JobStates.Retrying);
            Assert.Equal(["timed_out"], await Outcomes(f, job));
            var after = await cancelledAfter.Task.WaitAsync(Wait);
            Assert.InRange(after, TimeSpan.FromSeconds(1.9), TimeSpan.FromSeconds(4));
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "OWN-04")]
    public async Task Renewing_three_leases_with_one_stale_member()
    {
        await using var f = await DriverFixture.StartAsync(Ns("own04"));
        await using var w = Host(f, concurrency: 3);
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));
        var cancelled = new TaskCompletionSource();
        w.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromSeconds(30), CancellationToken.None);
        w.Script.On("own2", async (_, _, ct) =>
        {
            await using (ct.Register(() => cancelled.TrySetResult()))
            {
                await Task.Delay(TimeSpan.FromSeconds(30), CancellationToken.None);
            }
        });
        var ids = new[] { await Dispatch(w, "own1"), await Dispatch(w, "own2"), await Dispatch(w, "own3") };
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 3, Wait, "three running attempts");
        var attempts = ids.Select(j => w.Engine.Entries.Single(e => e.Lease.JobId == j)).ToArray();

        // Attempt 2 is taken over: expired in storage, recovered and reserved by another client.
        await f.ExecAsync("UPDATE runwick.jobs SET lease_expires_at = runwick.ms_floor(now() - interval '1 second') WHERE job_id = @j", ("j", ids[1]));
        var expired = (await other.Store.ListExpiredLeasesAsync(other.Application, 10, CancellationToken.None)).Single(x => x.JobId == ids[1]);
        Assert.Equal(StoreStatus.Applied, (await other.RecoverAsync(expired)).Status);
        var deadline = DateTime.UtcNow + Wait;
        StoreReply<ReservedJob> taken;
        do
        {
            await Task.Delay(100);
            taken = await other.ReserveAsync(contracts: w.Engine.Contracts);
            Assert.True(DateTime.UtcNow < deadline, "the recovered job never became reservable");
        }
        while (taken.Status != StoreStatus.Applied);
        Assert.Equal(ids[1], taken.Value!.Position.JobId);

        var renewedBefore = attempts.Select(a => a.Renewals).ToArray();
        await WorkerHost.WaitUntil(() => attempts[1].IsLost, Wait, "attempt 2 latched lost");
        Assert.Equal(LossCause.StaleLease, attempts[1].Loss);
        await cancelled.Task.WaitAsync(Wait);

        // Attempts 1 and 3 keep renewing (Applied, extended expiries) and stay owned.
        await WorkerHost.WaitUntil(() => attempts[0].Renewals > renewedBefore[0] + 1 && attempts[2].Renewals > renewedBefore[2] + 1, Wait, "healthy renewals");
        Assert.False(attempts[0].IsLost);
        Assert.False(attempts[2].IsLost);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE attempt_id = @a AND outcome IS NOT NULL AND outcome <> 'abandoned_running'",
            ("a", attempts[1].AttemptId)));
        Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "END-01")]
    public async Task Finish_after_observed_success_with_the_reply_lost()
    {
        await using var f = await DriverFixture.StartAsync(Ns("end01"));
        await using var w = Host(f, concurrency: 1);
        var job = await Dispatch(w, "j");
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.Finish));
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.EventCount("finishReplayed") == 1, Wait, "Finish resolved by replay");
        Assert.Equal(1, drop.Fired);
        Assert.Equal(JobStates.Succeeded, await State(f, job));
        Assert.Equal(["succeeded"], await Outcomes(f, job));
        Assert.Equal(1, w.Script.InvocationsOf("j"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'"));

        // No Renew between the unknown Finish and its resolution: no renewal event after the handler ran.
        var events = w.Events.Select(e => e.Name).ToList();
        Assert.DoesNotContain("renewed", events.SkipWhile(n => n != "handlerInvoked"));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "END-04")]
    public async Task Timeout_versus_delayed_acknowledgement()
    {
        await using var f = await DriverFixture.StartAsync(Ns("end04"));

        // (a) Timeout 1 s; the handler ignores cancellation and returns success at 1.5 s: TimedOut.
        await using (var w = Host(f, concurrency: 1, jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(1))))
        {
            w.Script.On("late", (_, _, _) => Task.Delay(1500, CancellationToken.None));
            var job = await Dispatch(w, "late");
            await w.StartAsync();
            await WaitState(f, job, JobStates.Retrying);
            Assert.Equal(["timed_out"], await Outcomes(f, job));
        }

        // (b) Timeout 2 s; the handler returns at 0.5 s; Finish delayed 3 s in the store: Succeeded.
        await using (var w = Host(f, concurrency: 1, jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(2))))
        {
            w.Script.On("quick", (_, _, _) => Task.Delay(500, CancellationToken.None));
            var job = await Dispatch(w, "quick");
            f.ArmFault(FaultPoint.BeforeGateLock, new FaultSpec(FaultAction.Delay, Delay: TimeSpan.FromSeconds(3)), FaultTarget.Op(Operations.Finish));
            await w.StartAsync();
            await WaitState(f, job, JobStates.Succeeded);
            Assert.Equal(["succeeded"], await Outcomes(f, job));
            var elapsed = await f.CountAsync("SELECT execution_elapsed_ms FROM runwick.job_attempts WHERE job_id = @j", ("j", job));
            Assert.InRange(elapsed, 450, 1999);
        }
    }

    sealed class ScopeLog
    {
        public System.Collections.Concurrent.ConcurrentQueue<(Guid Instance, Guid Attempt, string Event)> Entries { get; } = new();
    }

    sealed class ScopedResource(ScopeLog log, IJobContext context) : IAsyncDisposable
    {
        public Guid Instance { get; } = Guid.NewGuid();

        public Guid Attempt => context.AttemptId.Value;

        public ValueTask DisposeAsync()
        {
            log.Entries.Enqueue((Instance, Attempt, "disposed"));
            return ValueTask.CompletedTask;
        }
    }

    sealed class UsesResource(ScopedResource resource, ScopeLog log) : IJobMiddleware
    {
        public async Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            var result = await next();
            log.Entries.Enqueue((resource.Instance, resource.Attempt, "pipelineEnded"));
            return result;
        }
    }

    sealed class FaultyResource(Func<ValueTask> dispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => dispose();
    }

    sealed class UsesFaulty(FaultyResource resource) : IJobMiddleware
    {
        public Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            _ = resource;
            return next();
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-11")]
    public async Task One_async_scope_per_attempt_on_the_real_store()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk11"));
        var log = new ScopeLog();
        await using var w = Host(f, services: s => s.AddSingleton(log).AddScoped<ScopedResource>(), configure: r => r.UseMiddleware<UsesResource>());
        for (var i = 0; i < 6; i++)
        {
            await Dispatch(w, "s" + i);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => log.Entries.Count(e => e.Event == "disposed") == 6, Wait, "six disposals");
        var ended = log.Entries.Where(e => e.Event == "pipelineEnded").ToList();
        var disposed = log.Entries.Where(e => e.Event == "disposed").ToList();
        Assert.Equal(6, ended.Select(e => e.Instance).Distinct().Count());
        Assert.Equal(6, ended.Select(e => e.Attempt).Distinct().Count());
        Assert.All(ended, e => Assert.Single(disposed, d => d.Instance == e.Instance));
        var order = log.Entries.ToList();
        Assert.All(ended, e => Assert.True(order.IndexOf(e) < order.FindIndex(d => d.Event == "disposed" && d.Instance == e.Instance)));
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 6, Wait, "six succeeded");
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-17")]
    public async Task Cleanup_failure_after_timely_success_is_recorded_as_success()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wrk17"));
        await using var w = Host(f, services: s => s.AddScoped(_ => new FaultyResource(() => throw new InvalidOperationException("dispose fault"))),
            configure: r => r.UseMiddleware<UsesFaulty>());
        var job = await Dispatch(w, "j");
        await w.StartAsync();
        await WaitState(f, job, JobStates.Succeeded);
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal(["succeeded"], await Outcomes(f, job));
        Assert.Equal(1, w.Script.InvocationsOf("j"));
        Assert.Contains(w.Engine.Health().Diagnostics, d => d.Contains("dispose fault", StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-07")]
    public async Task Ignored_cancellation_keeps_permits_until_store_side_recovery()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time07"));
        var release = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 1, jobs: j => j.MaxConcurrent(1).WithExecutionTimeout(TimeSpan.FromSeconds(1)));
        w.Script.On("stubborn", (_, _, _) => release.Task);
        var job = await Dispatch(w, "stubborn");
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("stubborn") == 1, Wait, "invocation");
        var entry = w.Engine.Entries.Single(e => e.Lease.JobId == job);

        // Timeout → cancellation → grace expires with the handler still running: renewal stops, nothing is deleted locally.
        await WorkerHost.WaitUntil(() => entry.IsLost, Wait, "grace expiry");
        Assert.Equal(LossCause.GraceExpired, entry.Loss);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE attempt_id = @a", ("a", entry.AttemptId)));
        Assert.Equal(1, w.Engine.Pools[0].Gate.Outstanding);

        // The permit stays until the lease expires in storage and Recover releases it.
        await WorkerHost.WaitUntil(() => Outcomes(f, job).GetAwaiter().GetResult().FirstOrDefault() == "abandoned_running", Wait, "store-side recovery");
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE attempt_id = @a", ("a", entry.AttemptId)));
        Assert.True(entry.IsLost);
        Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
        release.SetResult();
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-08")]
    public async Task Hung_disposal_after_success_is_never_a_retryable_finish()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time08"));
        var hang = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 1, services: s => s.AddScoped(_ => new FaultyResource(() => new ValueTask(hang.Task))),
            configure: r => r.UseMiddleware<UsesFaulty>());
        var job = await Dispatch(w, "j");
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Engine.Entries.Any(e => e.Lease.JobId == job && e.IsLost), Wait, "cleanup budget");
        var entry = w.Engine.Entries.Single(e => e.Lease.JobId == job);
        Assert.Equal(LossCause.CleanupExpired, entry.Loss);
        Assert.Equal(1, w.Engine.Pools[0].Gate.Outstanding);
        Assert.Contains(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed completion", StringComparison.Ordinal));
        await WorkerHost.WaitUntil(() => Outcomes(f, job).GetAwaiter().GetResult().FirstOrDefault() == "abandoned_running", Wait, "store-side recovery");
        Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
        Assert.DoesNotContain("retryable_failure", await Outcomes(f, job));
        hang.SetResult();
    }
}
