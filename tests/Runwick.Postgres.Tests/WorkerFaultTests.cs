using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Harness.Timing;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;

namespace Runwick.Postgres.Tests;

/// <summary>Store restarts affect every test sharing the server: these run alone.</summary>
[CollectionDefinition("StoreRestart", DisableParallelization = true)]
public sealed class StoreRestartCollection;

/// <summary>
/// Real-store sub-parts of the worker cases that SP-009 covered with fake time only (07 §11): the wire variant of END-01,
/// TIME-02 under proxy latency, TIME-09 and TIME-12 resolution budgets, TIME-11 host-token phases, TIME-13 runtime
/// safety, WRK-08 store outage. Delivered with SP-010 for the G-1 run.
/// </summary>
public sealed class WorkerFaultTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(120);

    static WorkerHost Host(DriverFixture f, int concurrency, int? divisor = null, Action<JobBuilder<WorkJob>>? jobs = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, jobs, divisor: divisor);

    static async Task<Guid> Dispatch(WorkerHost w, string key) => (await w.DispatchAsync(key)).JobId.Value;

    static async Task<long> Count(DriverFixture f, string sql, Guid job) => await f.CountAsync(sql, ("j", job));

    static async Task<string> State(DriverFixture f, Guid job)
    {
        await using var conn = new Npgsql.NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand("SELECT state FROM runwick.jobs WHERE job_id = @j", conn);
        cmd.Parameters.AddWithValue("j", job);
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    static async Task<List<string>> Outcomes(DriverFixture f, Guid job)
    {
        await using var conn = new Npgsql.NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand("SELECT coalesce(outcome, '<open>') FROM runwick.job_attempts WHERE job_id = @j ORDER BY reserved_at", conn);
        cmd.Parameters.AddWithValue("j", job);
        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            list.Add(r.GetString(0));
        }

        return list;
    }

    static Task WaitState(DriverFixture f, Guid job, string state) =>
        WorkerHost.WaitUntil(() => State(f, job).GetAwaiter().GetResult() == state, Wait, $"job {job} {state}");

    static bool Diagnosed(WorkerHost w, string text) => w.Engine.Health().Diagnostics.Any(d => d.Contains(text, StringComparison.Ordinal));

    /// <summary>Waits for a diagnostic; on timeout the failure carries the engine's diagnostics and last events.</summary>
    static async Task WaitDiagnosed(WorkerHost w, string text)
    {
        try
        {
            await WorkerHost.WaitUntil(() => Diagnosed(w, text), Wait, $"diagnostic '{text}'");
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"{ex.Message}; diagnostics: [{string.Join(" | ", w.Engine.Health().Diagnostics)}]; last events: "
                + string.Join(", ", w.Events.TakeLast(30).Select(e => $"{e.Name}@{e.At.TotalSeconds:F1}")), ex);
        }
    }

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    /// <summary>Events of one attempt after the first event named <paramref name="after"/>.</summary>
    static List<string> EventsAfter(WorkerHost w, string after) =>
        [.. w.Events.Select(e => e.Name).SkipWhile(n => n != after).Skip(1)];

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "END-01")]
    public async Task Finish_commit_reply_dropped_on_the_wire_resolves_by_replay()
    {
        // Wire variant: the proxy drops the COMMIT reply of the worker's Finish on its control connection.
        await using var f = await DriverFixture.StartAsync(Ns("end01w"), StoreProfile.DirectPrimaryViaProxy);
        // Unscaled timing: the first renewal is ~15 s away, so the armed drop can only hit the Finish's COMMIT.
        await using var w = Host(f, concurrency: 1, divisor: 1);
        var job = await Dispatch(w, "j");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.On("j", (_, _, _) => release.Task);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("j") == 1, Wait, "handler running");
        var dropped = f.Proxy!.ArmCommitReplyDrop(1, PgFaultProxy.Role("control"));
        release.SetResult();
        await dropped.WaitAsync(Wait);

        await WaitState(f, job, JobStates.Succeeded);
        await WorkerHost.WaitUntil(() => w.EventCount("finishReplayed") + w.EventCount("finished") >= 1, Wait, "Finish resolved");
        Assert.True(f.Proxy.CommitRepliesDropped >= 1, "no COMMIT reply was dropped");
        Assert.Equal(1, w.EventCount("finishReplayed"));
        Assert.Equal(["succeeded"], await Outcomes(f, job));
        Assert.Equal(1, w.Script.InvocationsOf("j"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'"));
        Assert.DoesNotContain("renewed", EventsAfter(w, "handlerInvoked"));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-02")]
    public async Task Renew_replies_delayed_by_the_proxy_keep_the_deadline_from_the_send_instant()
    {
        // Default timing; replies on the worker's control connections arrive 4 s late. d = s + (ExpiresAt − DecidedAt) − 10 s
        // = s + 50 s, so the usable margin at receive is ≈ 46 s; the next Renew is due ≤ s + 18 s.
        await using var f = await DriverFixture.StartAsync(Ns("time02"), StoreProfile.DirectPrimaryViaProxy);
        await using var w = Host(f, concurrency: 1, divisor: 1);
        var margins = new List<(TimeSpan At, TimeSpan Margin)>();
        var inner = w.Engine.Hooks.Event;
        w.Engine.Hooks.Event = (name, entry) =>
        {
            inner?.Invoke(name, entry);
            if (name == "renewed" && entry?.Deadline is { } d)
            {
                lock (margins)
                {
                    margins.Add((w.Engine.Clock.Now, d - w.Engine.Clock.Now));
                }
            }
        };
        w.Script.On("long", (_, _, _) => Task.Delay(TimeSpan.FromSeconds(45), CancellationToken.None));
        var job = await Dispatch(w, "long");
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("long") == 1, Wait, "handler running");
        // The reply (COMMIT completion) of every control-connection command arrives 4 s late; per-message latency would
        // delay each of a command's ~10 round trips (07 §2.5 note, 2026-10-30).
        f.Proxy!.SetCommitReplyLatency(TimeSpan.FromSeconds(4), PgFaultProxy.Role("control"));

        await WaitState(f, job, JobStates.Succeeded);
        f.Proxy.SetCommitReplyLatency(TimeSpan.Zero);
        Assert.True(margins.Count >= 2, $"{margins.Count} renewals");
        foreach (var (_, margin) in margins)
        {
            // ≤ 50 s (deadline counted from the send instant, never the receive instant); ≥ 46 s less scheduling slack.
            Assert.InRange(margin.TotalSeconds, 40, 50.05);
        }

        // Renewal cadence: due ≤ s + 18 s, observed at receive (+4 s proxy) with slack for the loaded host.
        foreach (var (a, b) in margins.Zip(margins.Skip(1)))
        {
            Assert.True((b.At - a.At).TotalSeconds <= 18 + 4 + 3, $"renewals {(b.At - a.At).TotalSeconds:F1} s apart");
        }

        Assert.Equal(0, w.Events.Count(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)));
        Assert.Equal(["succeeded"], await Outcomes(f, job));
        await AssertInvariants(f);
    }

    /// <summary>
    /// The first <paramref name="operation"/> commits and its reply is lost (DropReply); every resubmission then fails
    /// before the gate lock (Throw Io from the second hit on) until the faults are disarmed.
    /// </summary>
    static FaultHandle LoseReplyThenFailResolution(DriverFixture f, string operation)
    {
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(operation));
        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Throw("Io"), FaultTarget.Op(operation) with { Occurrence = 2 }, times: 100_000);
        return drop;
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-09")]
    public async Task Unknown_reserve_begin_and_finish_exhaust_their_budgets_on_the_real_store()
    {
        // (a) Reserve: the claim stays unresolved past ClaimResolutionBudget; pool admission is suspended.
        await using (var f = await DriverFixture.StartAsync(Ns("time09r")))
        await using (var w = Host(f, concurrency: 2))
        {
            var r1 = await Dispatch(w, "r1");
            LoseReplyThenFailResolution(f, Operations.Reserve);
            await w.StartAsync();
            await WaitDiagnosed(w, "unresolved claim");
            Assert.True(w.Engine.Pools[0].Suspended);
            var r2 = await Dispatch(w, "r2");
            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.Equal(JobStates.Pending, await State(f, r2));
            Assert.Empty(w.Script.Invocations);
            Assert.True(w.Engine.Entries.Count <= 2);

            f.Faults.DisarmAll();
            await WaitState(f, r1, JobStates.Succeeded);
            await WaitState(f, r2, JobStates.Succeeded);
            Assert.Equal(1, w.Script.InvocationsOf("r1"));
            Assert.Equal(1, w.Script.InvocationsOf("r2"));
            await w.StopAsync();
            await AssertInvariants(f);
        }

        // (b) Begin: unresolved past BeginResolutionBudget; never invoked; renewal stops; recovered and run once later.
        await using (var f = await DriverFixture.StartAsync(Ns("time09b")))
        await using (var w = Host(f, concurrency: 2))
        {
            var b1 = await Dispatch(w, "b1");
            LoseReplyThenFailResolution(f, Operations.Begin);
            await w.StartAsync();
            await WaitDiagnosed(w, "unresolved Begin");
            Assert.Equal(0, w.Script.InvocationsOf("b1"));
            Assert.Contains("lost:BeginUnresolved", w.Events.Select(e => e.Name));
            Assert.DoesNotContain("renewed", EventsAfter(w, "lost:BeginUnresolved"));

            f.Faults.DisarmAll();
            await WaitState(f, b1, JobStates.Succeeded);
            Assert.Equal(1, w.Script.InvocationsOf("b1"));
            var outcomes = await Outcomes(f, b1);
            Assert.Equal("abandoned_running", outcomes[0]);
            Assert.Equal("succeeded", outcomes[^1]);
            await w.StopAsync();
            await AssertInvariants(f);
        }

        // (c) Finish: unresolved past CompletionResolutionBudget; no Renew, no rerun; the committed outcome stands.
        await using (var f = await DriverFixture.StartAsync(Ns("time09f")))
        await using (var w = Host(f, concurrency: 2))
        {
            var f1 = await Dispatch(w, "f1");
            LoseReplyThenFailResolution(f, Operations.Finish);
            await w.StartAsync();
            await WaitDiagnosed(w, "unconfirmed outcome");
            Assert.Equal(1, w.Script.InvocationsOf("f1"));
            Assert.DoesNotContain("renewed", EventsAfter(w, "handlerInvoked"));

            f.Faults.DisarmAll();
            await Task.Delay(TimeSpan.FromSeconds(3));
            Assert.Equal(JobStates.Succeeded, await State(f, f1));
            Assert.Equal(["succeeded"], await Outcomes(f, f1));
            Assert.Equal(1, w.Script.InvocationsOf("f1"));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-12")]
    public async Task Finish_receipt_found_after_local_loss_and_after_receipt_expiry()
    {
        // Unscaled timing (5 s request timeout): with ÷4 a loaded host can time the original Finish out before its
        // COMMIT, so no reply is dropped and the case degenerates into a fresh apply.
        // Run 1: Finish(Succeeded) commits, its reply is lost and resolution fails until the local deadline has passed;
        // resolution then replays the receipt: historical success, never a failure or a second Finish.
        await using (var f = await DriverFixture.StartAsync(Ns("time12a")))
        await using (var w = Host(f, concurrency: 1, divisor: 1))
        {
            var job = await Dispatch(w, "j");
            var drop = LoseReplyThenFailResolution(f, Operations.Finish);
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => drop.Fired == 1, Wait, "the original Finish committed and its reply dropped");
            await WorkerHost.WaitUntil(() => w.Events.Any(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)) || Diagnosed(w, "unconfirmed outcome"),
                Wait, "local loss");
            var entry = w.Engine.Entries.SingleOrDefault();
            await WorkerHost.WaitUntil(() => entry is null || entry.Deadline is null || w.Engine.Clock.Now > entry.Deadline, Wait, "local deadline passed");

            f.Faults.DisarmAll();
            await WorkerHost.WaitUntil(() => w.EventCount("finishReplayed") == 1, Wait, "late replay of the Finish receipt");
            Assert.Equal(JobStates.Succeeded, await State(f, job));
            Assert.Equal(["succeeded"], await Outcomes(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("j"));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'"));
            Assert.False(Diagnosed(w, "expired without a receipt"));
            await w.StopAsync();
            await AssertInvariants(f);
        }

        // Run 2: as run 1, and the receipt is pruned after NotAfter before resolution: explicit unknown durability
        // (CommandExpired, no receipt), the Finish is not reapplied, the stored state is the committed one.
        await using (var f = await DriverFixture.StartAsync(Ns("time12b")))
        await using (var w = Host(f, concurrency: 1, divisor: 1))
        {
            var job = await Dispatch(w, "j");
            var drop = LoseReplyThenFailResolution(f, Operations.Finish);
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => drop.Fired == 1, Wait, "the original Finish committed and its reply dropped");

            // NotAfter = send instant + lifetime, and the receipt commits after the send: waiting a lifetime from the
            // observed receipt guarantees every resubmission after the prune is past NotAfter.
            await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'").GetAwaiter().GetResult() == 1,
                Wait, "Finish receipt committed");
            await Task.Delay(w.Engine.Options.OwnershipCommandLifetime + TimeSpan.FromSeconds(1));
            // Finish receipts are in the durable partition (retain_until ≥ not_after + 24 h, 04 §4.7).
            Assert.True(await f.PruneReceipts("durable", DateTimeOffset.UtcNow.AddDays(1)) >= 1);

            f.Faults.DisarmAll();
            await WaitDiagnosed(w, "expired without a receipt");
            Assert.Equal(0, w.EventCount("finishReplayed"));
            Assert.Equal(JobStates.Succeeded, await State(f, job));
            Assert.Equal(["succeeded"], await Outcomes(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("j"));
            Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'"));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Theory]
    [InlineData("before-stop")]
    [InlineData("drain")]
    [InlineData("cancellation")]
    [InlineData("finalization")]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-11")]
    public async Task Host_token_in_each_phase_bounds_shutdown_on_the_real_store(string phase)
    {
        await using var f = await DriverFixture.StartAsync(Ns("time11" + phase[..3]));
        await using var w = Host(f, concurrency: 3);
        var o = w.Engine.Options;
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.On("block", (_, _, _) => Task.Delay(Timeout.InfiniteTimeSpan, CancellationToken.None));
        w.Script.On("fin", (_, _, _) => held.Task);

        // A begun attempt that ignores cancellation, and a finishing attempt whose Finish waits in the store.
        var block = await Dispatch(w, "block");
        var fin = await Dispatch(w, "fin");
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("block") == 1 && w.Script.InvocationsOf("fin") == 1, Wait, "two running");
        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromSeconds(60)), FaultTarget.Op(Operations.Finish), times: 1_000);
        held.SetResult();
        await WorkerHost.WaitUntil(() => w.Engine.Entries.Any(e => e.Phase == AttemptPhase.Finishing), Wait, "finishing");

        using var host = new CancellationTokenSource();
        var offset = phase switch
        {
            "drain" => o.ShutdownDrain / 2,
            "cancellation" => o.ShutdownDrain + o.ShutdownCancellation / 2,
            "finalization" => o.ShutdownDrain + o.ShutdownCancellation + o.ShutdownFinalization / 2,
            _ => TimeSpan.Zero,
        };
        if (phase == "before-stop")
        {
            await host.CancelAsync();
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        var stop = w.StopAsync(host.Token);
        var signal = w.Engine.StopSignalAt;
        Assert.NotNull(signal);
        if (offset > TimeSpan.Zero)
        {
            await Task.Delay(offset);
            await host.CancelAsync();
        }

        await stop.WaitAsync(TimeSpan.FromSeconds(60));
        var elapsed = started.Elapsed;

        // Cutoffs from the first stop signal, never restarted by StopAsync; StopAsync returns by the host deadline (or
        // the shutdown budget) plus SchedulerAllowance, with slack for the loaded host.
        Assert.Equal(signal, w.Engine.StopSignalAt);
        var bound = (phase == "before-stop" ? TimeSpan.Zero : offset < o.ShutdownBudget ? offset : o.ShutdownBudget) + o.SchedulerAllowance;
        Assert.True(elapsed <= bound + TimeSpan.FromSeconds(3), $"StopAsync took {elapsed.TotalSeconds:F1} s; bound {bound.TotalSeconds:F1} s");

        // Unresolved work (the held Finish, the blocked handler) is reported; nothing was invoked twice.
        Assert.True(Diagnosed(w, "unresolved") || w.Engine.Entries.Count > 0, "unresolved work not reported");
        Assert.Equal(1, w.Script.InvocationsOf("block"));
        Assert.Equal(1, w.Script.InvocationsOf("fin"));

        // Leases stay recoverable; the blocked handler is never reported succeeded.
        f.Faults.DisarmAll();
        var client = await f.OpenClient(configure: r =>
        {
            Runwick.Harness.Jobs.TestJobs.Register(r);
            r.AddJob<WorkJob>().Named("test.work").ToQueue("default").WithMaxExecutions(3)
                .WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        });
        var deadline = DateTime.UtcNow + Wait;
        while (await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state IN ('reserved', 'running')") > 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "leases never became recoverable");
            foreach (var expired in await client.Store.ListExpiredLeasesAsync(client.Application, 10, CancellationToken.None))
            {
                await client.RecoverAsync(expired);
            }

            await Task.Delay(500);
        }

        Assert.Equal(["abandoned_running"], await Outcomes(f, block));
        Assert.Contains((await Outcomes(f, fin))[0], new[] { "succeeded", "abandoned_running" });
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-13")]
    public async Task Runtime_safety_under_saturation_lock_waits_and_thread_pool_stall()
    {
        // (a) The worker's work data source (8 connections) is saturated by its own producer calls held in the store
        //     while 16 attempts renew: renewal and settlement use the control data source and keep going.
        await using (var f = await DriverFixture.StartAsync(Ns("time13a")))
        await using (var w = Host(f, concurrency: 16))
        {
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            w.Script.Default = (_, _, _) => release.Task;
            var jobs = new List<Guid>();
            for (var i = 0; i < 16; i++)
            {
                jobs.Add(await Dispatch(w, "a" + i));
            }

            await w.StartAsync();
            await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, Wait, "16 running");
            f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromSeconds(20)), FaultTarget.Op(Operations.Dispatch), times: 1_000);
            var flood = Enumerable.Range(0, 24).Select(i => w.DispatchAsync("flood" + i)).ToList();
            var renewedBefore = w.EventCount("renewed");
            await Task.Delay(TimeSpan.FromSeconds(15));
            Assert.True(w.EventCount("renewed") - renewedBefore >= 16, $"{w.EventCount("renewed") - renewedBefore} renewals during saturation");
            Assert.Equal(0, w.Events.Count(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)));
            release.SetResult();
            foreach (var job in jobs)
            {
                await WaitState(f, job, JobStates.Succeeded);
            }

            f.Faults.DisarmAll();
            await Task.WhenAll(flood.Select(t => t.ContinueWith(_ => { }, TaskScheduler.Default)));
            await w.StopAsync();
            await AssertInvariants(f);
        }

        // (b) A held lock blocks Renew past the local deadline: authority is latched lost and no Finish follows.
        await using (var f = await DriverFixture.StartAsync(Ns("time13b")))
        await using (var w = Host(f, concurrency: 1))
        {
            w.Script.On("held", (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(60), ct));
            var job = await Dispatch(w, "held");
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("held") == 1, Wait, "running");
            await using var locker = new Npgsql.NpgsqlConnection(f.AdminConnectionString);
            await locker.OpenAsync();
            await using var tx = await locker.BeginTransactionAsync();
            await using (var cmd = new Npgsql.NpgsqlCommand("SELECT 1 FROM runwick.jobs WHERE job_id = @j FOR UPDATE", locker, tx))
            {
                cmd.Parameters.AddWithValue("j", job);
                await cmd.ExecuteNonQueryAsync();
            }

            await WorkerHost.WaitUntil(() => w.EventCount("lost:SafetyDeadline") == 1, Wait, "loss latched by the deadline");
            await tx.RollbackAsync();
            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.DoesNotContain("finished", EventsAfter(w, "lost:SafetyDeadline"));
            Assert.DoesNotContain("renewed", EventsAfter(w, "lost:SafetyDeadline"));
            await w.StopAsync();
        }

        // (d) Every thread-pool worker blocked for 3 s while Renews are delayed past the deadline (Fast timing): the
        //     separate watchdog latches the loss; no Finish is sent after the latch.
        await using (var f = await DriverFixture.StartAsync(Ns("time13d")))
        await using (var w = Host(f, concurrency: 1, divisor: 10))
        {
            w.Script.On("stall", (_, _, _) => Task.Delay(TimeSpan.FromSeconds(12), CancellationToken.None));
            var job = await Dispatch(w, "stall");
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("stall") == 1, Wait, "running");
            f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromSeconds(30)), FaultTarget.Op(Operations.Renew), times: 1_000);
            ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
            var blockers = Math.Max(ThreadPool.ThreadCount, minWorkers) + 4;
            using var gate = new ManualResetEventSlim();
            for (var i = 0; i < blockers; i++)
            {
                ThreadPool.UnsafeQueueUserWorkItem(_ => gate.Wait(TimeSpan.FromSeconds(3)), null);
            }

            await WorkerHost.WaitUntil(() => w.EventCount("lost:SafetyDeadline") == 1, Wait, "watchdog latched the loss");
            gate.Set();
            f.Faults.DisarmAll();
            await Task.Delay(TimeSpan.FromSeconds(13));
            Assert.DoesNotContain("finished", EventsAfter(w, "lost:SafetyDeadline"));
            Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish'"));
            await w.StopAsync();
        }
    }
}

public sealed record VerOne(string Id) : IQueueJob;

public sealed record VerTwo(string Id, int N) : IQueueJob;

public sealed record VerThree(string Id, int N, string Tag) : IQueueJob;

internal sealed class VerThreeHandler : IJobHandler<VerThree>
{
    public static readonly System.Collections.Concurrent.ConcurrentQueue<VerThree> Received = new();

    public Task HandleAsync(VerThree job, CancellationToken cancellationToken)
    {
        Received.Enqueue(job);
        return Task.CompletedTask;
    }
}

/// <summary>WRK-13 on the real store: upgrade chains, a chain gap and malformed bytes (07 §11).</summary>
public sealed class UpgradeChainTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static void V3(RunwickBuilder r, bool withV1Step)
    {
        var job = r.AddJob<VerThree>().Named("test.ver").ToQueue("default").AtVersion(3).WithMaxExecutions(1)
            .Upgrade<VerTwo, VerThree>(2, 3, v2 => new VerThree(v2.Id, v2.N, "up"));
        if (withV1Step)
        {
            job.Upgrade<VerOne, VerTwo>(1, 2, v1 => new VerTwo(v1.Id, 0));
        }

        r.AddHandler<VerThree, VerThreeHandler>();
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-13")]
    public async Task Upgrade_gap_keeps_old_jobs_pending_and_complete_chains_upgrade()
    {
        await using var f = await DriverFixture.StartAsync("wrk13_" + Guid.NewGuid().ToString("N")[..8]);
        var v1 = await f.OpenClient(configure: r => r.AddJob<VerOne>().Named("test.ver").ToQueue("default").AtVersion(1).WithMaxExecutions(1));
        var v2 = await f.OpenClient(configure: r => r.AddJob<VerTwo>().Named("test.ver").ToQueue("default").AtVersion(2).WithMaxExecutions(1));
        var old = (await v1.Dispatcher.DispatchAsync(new VerOne("old"))).JobId.Value;
        var mid = (await v2.Dispatcher.DispatchAsync(new VerTwo("mid", 7))).JobId.Value;
        var bad = (await v2.Dispatcher.DispatchAsync(new VerTwo("bad", 1))).JobId.Value;
        var badBytes = "{\"id\":"u8.ToArray();
        await f.ExecAsync("UPDATE runwick.job_payloads SET payload_utf8 = @b WHERE job_id = @j", ("b", badBytes), ("j", bad));

        // Gap: the worker decodes v2 and v3 only. The v1 job is never reserved.
        await using (var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 2, configure: r => V3(r, withV1Step: false)))
        {
            Assert.Equal([2, 3], w.Engine.Contracts.Where(c => c.Name == "test.ver").Select(c => c.PayloadVersion).Order());
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", mid)).Result == 1, Wait, "v2 job upgraded and run");
            await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'failed'", ("j", bad)).Result == 1, Wait, "malformed job failed");
            await Task.Delay(TimeSpan.FromSeconds(3));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'pending'", ("j", old)));
            Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", old)));
            await w.StopAsync();
        }

        Assert.Contains(VerThreeHandler.Received, j => j == new VerThree("mid", 7, "up"));
        Assert.DoesNotContain(VerThreeHandler.Received, j => j.Id == "bad");

        // Malformed data: Begin (execution counted), then PermanentFailure; the stored bytes are unchanged.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j AND execution_number = 1 AND outcome = 'permanent_failure'", ("j", bad)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_payloads WHERE job_id = @j AND payload_utf8 = @b", ("j", bad), ("b", badBytes)));

        // Complete chain v1 → v2 → v3: the old job is reserved, upgraded and run.
        await using (var w = WorkerHost.Create(f.ClientConnectionString("worker2"), f.Application, f.Faults, 2, configure: r => V3(r, withV1Step: true)))
        {
            Assert.Equal([1, 2, 3], w.Engine.Contracts.Where(c => c.Name == "test.ver").Select(c => c.PayloadVersion).Order());
            await w.StartAsync();
            await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", old)).Result == 1, Wait, "v1 job upgraded and run");
            await w.StopAsync();
        }

        Assert.Contains(VerThreeHandler.Received, j => j == new VerThree("old", 0, "up"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}

/// <summary>WRK-08: a store outage after 16 handlers succeed (07 §11). Restarts the shared server, so it runs alone.</summary>
[Collection("StoreRestart")]
public sealed class StoreOutageTests
{
    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-08")]
    [Trait("RequiresDocker", "true")]
    public async Task Store_outage_after_handler_success_resolves_every_finish_without_replay()
    {
        await using var f = await DriverFixture.StartAsync("wrk08_" + Guid.NewGuid().ToString("N")[..8]);
        Assert.True(f.Server.Database.CanRestart, "WRK-08 needs a Docker-managed server (no RUNWICK_TEST_PG)");
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 16, divisor: 1);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.Default = (_, _, _) => release.Task;
        var jobs = new List<Guid>();
        for (var i = 0; i < 16; i++)
        {
            jobs.Add((await w.DispatchAsync("g" + i)).JobId.Value);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, TimeSpan.FromSeconds(120), "16 running");

        // Kill the store; the handlers succeed during the outage.
        var maxEntries = 0;
        var maxFinishing = 0;
        using var sampling = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                maxEntries = Math.Max(maxEntries, w.Engine.Entries.Count);
                maxFinishing = Math.Max(maxFinishing, w.Engine.Entries.Count(e => e.Phase == AttemptPhase.Finishing));
                await Task.Delay(100);
            }
        });
        var outage = System.Diagnostics.Stopwatch.StartNew();
        var restart = f.RestartStore(RestartMode.Kill, TimeSpan.FromSeconds(20));
        await Task.Delay(TimeSpan.FromSeconds(5));
        release.SetResult();
        await restart;
        var restored = outage.Elapsed;

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(120);
        while (await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'") < 16)
        {
            Assert.True(DateTime.UtcNow < deadline, "Finishes did not resolve after the restart");
            await Task.Delay(500);
        }

        await sampling.CancelAsync();
        await sampler;
        Assert.True(maxEntries <= 16, $"{maxEntries} entries");
        Assert.True(maxFinishing <= 16, $"{maxFinishing} pending outcomes");
        // The Finish commands are sent ~5 s into the outage with a 30 s lifetime: a restore later than ~35 s
        // legitimately expires them (unknown durability, recovery reruns the job), which is not this case.
        Assert.True(w.Script.Invocations.Count == 16,
            $"{w.Script.Invocations.Count} invocations; store restored {restored.TotalSeconds:F1} s after the kill; diagnostics: "
            + string.Join(" | ", w.Engine.Health().Diagnostics.Take(6)));
        Assert.True(w.Events.Any(e => e.Name.StartsWith("pressureStop", StringComparison.Ordinal)), "admission did not back off");
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        await w.StopAsync();
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}

/// <summary>TIME-13 (c): store-clock steps on the libfaketime server (07 I-9). Uses its own container.</summary>
public sealed class StoreClockStepTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(120);

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-13")]
    [Trait("RequiresDocker", "true")]
    public async Task Store_clock_steps_decide_on_stored_instants_without_local_revival()
    {
        await using var db = await TestDatabase.StartFakeTimeAsync();
        await Runwick.Postgres.Migrations.RunwickPostgresMigrations.MigrateAsync(db.For("rw:harness:migrate", pooling: false));
        await using var w = WorkerHost.Create(db.For("rw:worker"), "clock", new HarnessFaultInjector(), 2);
        w.Script.On("fwd", (_, _, _) => Task.Delay(TimeSpan.FromSeconds(40), CancellationToken.None));
        var fwd = (await w.DispatchAsync("fwd")).JobId.Value;
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.EventCount("renewed") >= 1, Wait, "first renewal");
        var attempt = w.Events.First(e => e.Name == "renewed").AttemptId;

        // Forward step beyond the lease: the next Renew is decided at the stored (stepped) instant and finds the lease
        // expired; authority is latched lost and no Finish follows.
        await db.StepClockAsync(TimeSpan.FromSeconds(30));
        await WorkerHost.WaitUntil(() => w.Events.Any(e => e.AttemptId == attempt && e.Name.StartsWith("lost:", StringComparison.Ordinal)), Wait, "loss after the forward step");
        var lostAt = w.Events.First(e => e.AttemptId == attempt && e.Name.StartsWith("lost:", StringComparison.Ordinal)).At;

        // Backward step: nothing revives the latched attempt.
        await db.StepClockAsync(TimeSpan.FromSeconds(-30));
        var back = (await w.DispatchAsync("back")).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("back") == 1, Wait, "work after the backward step");
        await Task.Delay(TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(w.Events, e => e.AttemptId == attempt && e.At > lostAt && e.Name is "renewed" or "finished" or "finishReplayed");
        Assert.NotEqual(Guid.Empty, fwd);
        Assert.NotEqual(Guid.Empty, back);
        await db.StepClockAsync(TimeSpan.Zero);
        await w.StopAsync();
        Assert.Empty(SnapshotInvariants.Violations((await SnapshotReader.ReadAsync(db.For("rw:harness:admin"))).Model));
    }
}
