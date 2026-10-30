using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Core.Tests;

/// <summary>Worker engine cases over fake time and a scripted store (07 §2.8, §10, §11): SP-009.</summary>
public sealed class WorkerEngineTests
{
    static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    static Task Forever(EngineFixture e, CancellationToken ct) => Task.Delay(TimeSpan.FromHours(1), e.Time, ct);

    /// <summary>Waits on fake time and ignores cancellation (a handler that does not observe its token).</summary>
    static Task Ignoring(EngineFixture e, TimeSpan duration) => Task.Delay(duration, e.Time, CancellationToken.None);

    static AttemptEntry EntryOf(EngineFixture e, Guid jobId) =>
        e.Events.First(x => x.Entry is { } en && en.Lease.JobId == jobId).Entry!;

    static List<StoreCall> CallsFor(EngineFixture e, string operation, Guid jobId) =>
        [.. e.Store.Calls.Where(c => c.Operation == operation && c.JobId == jobId)];

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "SMOKE")]
    public async Task Scripted_jobs_run_to_success()
    {
        await using var e = EngineFixture.Create();
        for (var i = 0; i < 10; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        Assert.Equal(10, e.Script.Invocations.Count);
        Assert.All(e.Store.Jobs, j => Assert.Equal(AttemptOutcome.Succeeded, Assert.Single(j.Finishes).Outcome));
        await e.StopAsync();
        Assert.Empty(e.Engine.Entries);
    }

    // ------------------------------------------------------------------ TIME

    static List<ConfigurationError> Validate(Action<WorkerOptions> configure)
    {
        var builder = new WorkerBuilder().Pool("p", p => p.Queues("default").FixedConcurrency(1)).Configure(configure);
        try
        {
            WorkerSettings.Validate(builder, ["default"]);
            return [];
        }
        catch (RunwickConfigurationException ex)
        {
            return [.. ex.Errors];
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-01")]
    public void Startup_validation_of_timing_relationships()
    {
        var defaults = WorkerSettings.Validate(new WorkerBuilder().Pool("p", p => p.Queues("default")), ["default"]);
        Assert.Equal(TimeSpan.FromSeconds(25), defaults.StopMargin);
        Assert.Equal(TimeSpan.FromSeconds(30), defaults.ResumeMargin);
        Assert.Equal(TimeSpan.FromSeconds(60), defaults.Options.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(10), defaults.Options.LeaseSafetyMargin);

        (string Rule, Action<WorkerOptions> Set)[] invalid =
        [
            ("V-02", o => o.LeaseSafetyMargin = o.LeaseDuration),
            ("V-06", o => { o.LeaseDuration = TimeSpan.FromSeconds(30); o.LeaseSafetyMargin = TimeSpan.FromSeconds(10); }),
            ("V-0", o => o.MinimumInvocationMargin = TimeSpan.FromSeconds(12)),
            ("V-", o => { o.ShutdownDrain = TimeSpan.FromSeconds(15); o.ShutdownCancellation = TimeSpan.FromSeconds(6); o.ShutdownFinalization = TimeSpan.FromSeconds(5); }),
            ("V-", o => o.RenewalJitter = o.RenewalInterval + TimeSpan.FromSeconds(1)),
            ("V-", o => o.RenewalInterval = TimeSpan.Zero),
            ("V-", o => o.ControlRequestTimeout = TimeSpan.FromSeconds(-1)),
            ("V-", o => o.RenewalInterval = TimeSpan.MaxValue),
            ("V-", o => o.ControlRequestTimeout = TimeSpan.FromDays(6_000_000)),
        ];
        foreach (var (rule, set) in invalid)
        {
            var errors = Validate(set);
            Assert.NotEmpty(errors);
            Assert.All(errors, err => Assert.StartsWith("V-", err.Message, StringComparison.Ordinal));
            Assert.Contains(errors, err => err.Message.StartsWith(rule, StringComparison.Ordinal));
        }

        // Validation never adjusts a value: the lease and safety margin stay as configured.
        var lease = new WorkerOptions { LeaseDuration = TimeSpan.FromSeconds(30), LeaseSafetyMargin = TimeSpan.FromSeconds(10) };
        Assert.NotEmpty(Validate(o => { o.LeaseDuration = lease.LeaseDuration; o.LeaseSafetyMargin = lease.LeaseSafetyMargin; }));
        Assert.Equal(TimeSpan.FromSeconds(30), lease.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(10), lease.LeaseSafetyMargin);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-02")]
    public async Task Delayed_fresh_reply_replay_and_late_responses()
    {
        await using var e = EngineFixture.Create();
        e.Step = TimeSpan.FromMilliseconds(10);
        var id = e.Add("long");
        e.Script.On("long", (_, _, ct) => Forever(e, ct));
        var renewSentAt = new ConcurrentQueue<TimeSpan>();
        var phase = 0;
        e.Store.Before = async call =>
        {
            if (call.Operation != Operations.Renew)
            {
                return CallEffect.Proceed;
            }

            renewSentAt.Enqueue(e.Engine.Clock.Now);
            switch (Volatile.Read(ref phase))
            {
                case 0:
                    await Task.Delay(TimeSpan.FromSeconds(4), e.Time);
                    break;
                case 2:
                    // Past the local deadline: the reply arrives after d.
                    await Task.Delay(TimeSpan.FromSeconds(60), e.Time);
                    break;
            }

            return CallEffect.Proceed;
        };
        e.Store.Rewrite = (call, reply) => Volatile.Read(ref phase) == 1 && reply is StoreReply<RenewedLease> r
            ? r with { Replayed = true, Value = r.Value! with { Lease = r.Value.Lease with { ExpiresAt = r.Value.Lease.ExpiresAt.AddHours(1) } } }
            : reply;

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("long") == 1, Long, "invocation");
        var entry = EntryOf(e, id);

        // (1) Renew sent at s, reply at s + 4 s with r = 60 s: d = s + 50 s; next renewal due ≤ s + 18 s and ≤ d − 13 s.
        await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "renewed"), Long, "first renewal");
        var s = renewSentAt.First();
        Assert.InRange(entry.Deadline!.Value, s + TimeSpan.FromSeconds(50) - e.Step, s + TimeSpan.FromSeconds(50) + e.Step);
        Volatile.Write(ref phase, 1);
        e.Step = TimeSpan.FromMilliseconds(50);
        await e.RunUntilAsync(() => renewSentAt.Count >= 2, Long, "second renewal");
        var s2 = renewSentAt.ElementAt(1);
        Assert.True(s2 - s <= TimeSpan.FromSeconds(18) + e.Step, $"next renewal at s + {s2 - s}");
        Assert.True(s2 <= entry.Deadline!.Value - TimeSpan.FromSeconds(13) + e.Step);
        e.Step = TimeSpan.FromMilliseconds(200);

        // (2) A Replayed = true renewal reply never moves d.
        var before = entry.Deadline;
        await e.AdvanceAsync(TimeSpan.FromMilliseconds(200));
        Assert.Equal(before, entry.Deadline);

        // (3) A reply after d does not revive authority: loss stays latched, cancellation requested, no Finish.
        Volatile.Write(ref phase, 2);
        await e.RunUntilAsync(() => entry.IsLost, Long, "loss at the deadline");
        Assert.Equal(LossCause.SafetyDeadline, entry.Loss);
        await e.AdvanceAsync(TimeSpan.FromSeconds(70));
        Assert.True(entry.IsLost);
        Assert.True(entry.ExecutionToken.IsCancellationRequested);
        Assert.Equal(0, e.Store.Count(Operations.Finish));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-03")]
    public async Task Deadline_or_timeout_passes_while_the_runner_is_queued()
    {
        // (a) Local authority deadline passes before the runner starts: no decode, scope or handler; Begin stays counted.
        await using (var e = EngineFixture.Create())
        {
            var gate = new TaskBarrier("runner");
            e.Engine.Hooks.BeforeRunnerStart = _ => gate.ArriveAndWaitAsync();
            var id = e.Add("a");
            await e.StartAsync();
            await e.RunUntilAsync(() => gate.Reached.IsCompleted, Long, "runner barrier");
            e.Store.Before = call => Task.FromResult(call.Operation == Operations.Renew ? CallEffect.Hang : CallEffect.Proceed);
            var entry = EntryOf(e, id);
            await e.RunUntilAsync(() => entry.IsLost, Long, "deadline");
            gate.Release();
            await e.AdvanceAsync(TimeSpan.FromSeconds(2));
            Assert.Empty(e.Script.Invocations);
            Assert.Equal(0, Interlocked.Read(ref e.Engine.ScopesCreated));
            Assert.Equal(1, e.Store.JobOf(id).Executions);
            Assert.Equal(0, e.Store.Count(Operations.Finish));
        }

        // (b) ExecutionTimeout passes while queued: finalized TimedOut without creating a scope.
        await using (var e = EngineFixture.Create(jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(2))))
        {
            var gate = new TaskBarrier("runner");
            e.Engine.Hooks.BeforeRunnerStart = _ => gate.ArriveAndWaitAsync();
            var id = e.Add("b");
            await e.StartAsync();
            await e.RunUntilAsync(() => gate.Reached.IsCompleted, Long, "runner barrier");
            await e.AdvanceAsync(TimeSpan.FromSeconds(3));
            gate.Release();
            await e.RunUntilAsync(() => e.Store.JobOf(id).Finishes.Count == 1, Long, "finish");
            Assert.Equal(AttemptOutcome.TimedOut, e.Store.JobOf(id).Finishes[0].Outcome);
            Assert.Empty(e.Script.Invocations);
            Assert.Equal(0, Interlocked.Read(ref e.Engine.ScopesCreated));
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-04")]
    public async Task Classification_at_the_timeout_boundary()
    {
        await using var e = EngineFixture.Create(jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(1)));
        e.Step = TimeSpan.FromMilliseconds(1);
        var exact = e.Add("exact");
        var early = e.Add("early");
        var slowFinish = e.Add("slow");
        e.Script.On("exact", (_, _, _) => Ignoring(e, TimeSpan.FromSeconds(1)));
        e.Script.On("early", (_, _, _) => Ignoring(e, TimeSpan.FromMilliseconds(999)));
        e.Script.On("slow", (_, _, _) => Ignoring(e, TimeSpan.FromMilliseconds(100)));
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.Finish && call.JobId == slowFinish)
            {
                await Task.Delay(TimeSpan.FromSeconds(3), e.Time);
            }

            return CallEffect.Proceed;
        };

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.Finishes.Count == 1), Long, "three finishes", TimeSpan.FromMilliseconds(1));

        // (a) elapsed = timeout → TimedOut; (b) recorded completion 1 ms before → Succeeded; (c) slow Finish → Succeeded.
        Assert.Equal(AttemptOutcome.TimedOut, e.Store.JobOf(exact).Finishes[0].Outcome);
        Assert.Equal(AttemptOutcome.Succeeded, e.Store.JobOf(early).Finishes[0].Outcome);
        Assert.Equal(TimeSpan.FromMilliseconds(999), e.Store.JobOf(early).Finishes[0].ExecutionElapsed);
        Assert.Equal(AttemptOutcome.Succeeded, e.Store.JobOf(slowFinish).Finishes[0].Outcome);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-07")]
    public async Task Ignored_cancellation_stops_renewal_after_grace_and_keeps_the_ticket()
    {
        await using var e = EngineFixture.Create(concurrency: 1, jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(5)));
        var id = e.Add("stubborn");
        var next = e.Add("next");
        var release = new TaskCompletionSource();
        e.Script.On("stubborn", (_, _, _) => release.Task);
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("stubborn") == 1, Long, "invocation");
        var entry = EntryOf(e, id);

        // Timeout at 5 s requests cancellation; grace (10 s) expires with the handler still running.
        await e.RunUntilAsync(() => entry.IsLost, Long, "grace expiry");
        Assert.Equal(LossCause.GraceExpired, entry.Loss);
        Assert.True(entry.ExecutionToken.IsCancellationRequested);
        var renews = CallsFor(e, Operations.Renew, id).Count;
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.Equal(renews, CallsFor(e, Operations.Renew, id).Count);

        // Ticket and scope retained: the single slot is not replaced while user code runs.
        Assert.Equal(1, e.Engine.Pools[0].Gate.Outstanding);
        Assert.Equal(0, e.Script.InvocationsOf("next"));
        Assert.Equal(0, e.Store.Count(Operations.Finish));

        // The lease expires in storage; this worker's own recovery loop recovers it (another process may equally do so).
        await e.RunUntilAsync(() => e.Store.JobOf(id).History.Contains("recovered:abandoned_running"), Long, "store-side recovery");
        release.SetResult();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("next") >= 1 || e.Script.InvocationsOf("stubborn") >= 2, Long, "slot freed after the handler ends");
        Assert.DoesNotContain(e.Store.JobOf(id).Finishes, f => f.Lease.AttemptId == entry.AttemptId);
        _ = next;
    }

    sealed class HangingDisposable(TaskCompletionSource release) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => new(release.Task);
    }

    sealed class UsesHanging(HangingDisposable d) : IJobMiddleware
    {
        public Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            _ = d;
            return next();
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-08")]
    public async Task Hung_disposal_latches_unconfirmed_completion_without_a_retryable_finish()
    {
        var release = new TaskCompletionSource();
        await using var e = EngineFixture.Create(concurrency: 1, services: s => s.AddScoped(_ => new HangingDisposable(release)),
            configure: r => r.UseMiddleware<UsesHanging>());
        var id = e.Add("hung");
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "handlerInvoked"), Long, "invocation");
        await e.RunUntilAsync(() => EntryOf(e, id).IsLost, Long, "cleanup budget");
        var entry = EntryOf(e, id);
        Assert.Equal(LossCause.CleanupExpired, entry.Loss);
        Assert.Equal(0, e.Store.Count(Operations.Finish));
        Assert.Equal(1, e.Engine.Pools[0].Gate.Outstanding);
        Assert.Contains(e.Engine.Health().Diagnostics, d => d.Contains("unconfirmed completion", StringComparison.Ordinal));
        release.SetResult();
        await e.RunUntilAsync(() => e.Engine.Pools[0].Gate.Outstanding == 0, Long, "ticket released after disposal ends");
        Assert.Equal(0, e.Store.Count(Operations.Finish));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-11")]
    public async Task Shutdown_phases_are_measured_from_the_first_stop_signal()
    {
        await using var e = EngineFixture.Create(concurrency: 3);
        e.Add("running");
        e.Script.On("running", (_, _, ct) => Forever(e, ct));
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("running") == 1, Long, "running");

        // An unbegun reservation: the claim for it commits but its reply is held until the stop signal (WRK-04 shape).
        var hold = new TaskCompletionSource();
        var held = 0;
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.Reserve && e.Store.Jobs.Any(j => j.State == JobStates.Pending))
            {
                Volatile.Write(ref held, 1);
                await hold.Task;
            }

            return CallEffect.Proceed;
        };
        var unbegun = e.Add("unbegun");
        await e.RunUntilAsync(() => Volatile.Read(ref held) == 1, Long, "claim held");

        e.Engine.SignalStop();
        var t0 = e.Engine.StopSignalAt!.Value;
        await e.AdvanceAsync(TimeSpan.FromSeconds(3));
        var stop = Task.Run(() => e.Engine.StopAsync(CancellationToken.None));
        hold.SetResult();
        await e.RunUntilAsync(() => stop.IsCompleted, TimeSpan.FromSeconds(40), "StopAsync");
        await stop;

        // T0 is not restarted by StopAsync; StopAsync returns by T0 + 25 s (+ allowance).
        Assert.Equal(t0, e.Engine.StopSignalAt);
        Assert.True(e.Engine.Clock.Now <= t0 + TimeSpan.FromSeconds(25) + TimeSpan.FromSeconds(2), $"stopped at T0 + {e.Engine.Clock.Now - t0}");

        // The unbegun reservation was released in drain; the running one finished Interrupted after shutdown cancellation.
        Assert.Contains("released", e.Store.JobOf(unbegun).History);
        Assert.Equal(0, e.Script.InvocationsOf("unbegun"));
        var running = e.Store.Jobs.Single(j => j.Finishes.Count > 0);
        Assert.Equal(AttemptOutcome.Interrupted, running.Finishes[0].Outcome);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-09")]
    public async Task Unknown_reserve_begin_and_finish_outcomes_respect_their_budgets()
    {
        // (a) Reserve: every submission hangs; the same command is resubmitted; past the claim budget the pool suspends.
        await using (var e = EngineFixture.Create())
        {
            var stuck = true;
            e.Store.Before = call => Task.FromResult(call.Operation == Operations.Reserve && Volatile.Read(ref stuck) ? CallEffect.Hang : CallEffect.Proceed);
            var id = e.Add("r");
            await e.StartAsync();
            await e.RunUntilAsync(() => e.Engine.Pools[0].ClaimUnresolved, Long, "claim unresolved");
            var reserves = e.Store.Calls.Where(c => c.Operation == Operations.Reserve).ToList();
            Assert.True(reserves.Count >= 2);
            Assert.All(reserves, r => Assert.Equal((reserves[0].CommandId, reserves[0].Body), (r.CommandId, r.Body)));
            Assert.True(e.Engine.Pools[0].Suspended);
            Assert.Equal(0, e.Store.Count(Operations.Begin));
            Volatile.Write(ref stuck, false);
            await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "claim resolves");
            Assert.Equal(1, e.Store.JobOf(id).Reservations);
            Assert.True(e.Engine.Pools[0].Gate.Outstanding <= 4);
        }

        // (b) Begin: past the Begin budget the attempt is lost, never invoked, renewal stops; the pool stays suspended
        //     until the same Begin command resolves.
        await using (var e = EngineFixture.Create())
        {
            var stuck = true;
            e.Store.Before = call => Task.FromResult(call.Operation == Operations.Begin && Volatile.Read(ref stuck) ? CallEffect.Hang : CallEffect.Proceed);
            var id = e.Add("b");
            await e.StartAsync();
            await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "lost:BeginUnresolved"), Long, "Begin budget");
            var begins = CallsFor(e, Operations.Begin, id);
            Assert.All(begins, b => Assert.Equal(begins[0].CommandId, b.CommandId));
            Assert.True(e.Engine.Pools[0].Suspended);
            var renews = CallsFor(e, Operations.Renew, id).Count;
            await e.AdvanceAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(renews, CallsFor(e, Operations.Renew, id).Count);
            Volatile.Write(ref stuck, false);
            await e.RunUntilAsync(() => !e.Engine.Pools[0].Suspended, Long, "Begin resolves late");
            Assert.All(CallsFor(e, Operations.Begin, id), b => Assert.Equal(begins[0].CommandId, b.CommandId));
            Assert.Equal(0, e.Script.InvocationsOf("b"));
        }

        // (c) Finish: past the completion budget renewal stays stopped and the pipeline never reruns.
        await using (var e = EngineFixture.Create())
        {
            e.Store.Before = call => Task.FromResult(call.Operation == Operations.Finish ? CallEffect.Hang : CallEffect.Proceed);
            var id = e.Add("f");
            await e.StartAsync();
            await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "retired"), Long, "completion budget");
            var entry = EntryOf(e, id);
            Assert.Equal(LossCause.CompletionUnresolved, entry.Loss);
            var calls = e.Store.Calls.Where(c => c.JobId == id).ToList();
            var finishes = calls.Where(c => c.Operation == Operations.Finish).ToList();
            Assert.All(finishes, f => Assert.Equal((finishes[0].CommandId, finishes[0].Body), (f.CommandId, f.Body)));
            Assert.DoesNotContain(calls.SkipWhile(c => c.Operation != Operations.Finish), c => c.Operation == Operations.Renew);
            await e.AdvanceAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(1, e.Script.InvocationsOf("f"));
            Assert.Contains(e.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-12")]
    public async Task Finish_receipt_resolved_after_local_loss_reports_historical_success()
    {
        await using var e = EngineFixture.Create();
        var id = e.Add("j");
        var open = false;
        e.Store.DropReply = call => call.Operation == Operations.Finish && call.Submission == 1;
        e.Store.Before = call => Task.FromResult(call.Operation == Operations.Finish && call.Submission > 1 && !Volatile.Read(ref open)
            ? CallEffect.Hang : CallEffect.Proceed);
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "Finish committed");
        var entry = EntryOf(e, id);
        var d = entry.Deadline!.Value;

        // Resolution keeps failing until the local deadline has passed, then the receipt replays.
        await e.RunUntilAsync(() => e.Engine.Clock.Now > d + TimeSpan.FromSeconds(1), Long, "local deadline passes");
        Assert.True(entry.IsLost);
        Volatile.Write(ref open, true);
        await e.RunUntilAsync(() => e.Engine.Health().Diagnostics.Any(x => x.Contains("resolved late as Applied", StringComparison.Ordinal)), Long, "late resolution");
        Assert.Single(e.Store.JobOf(id).Finishes);
        Assert.Equal(AttemptOutcome.Succeeded, e.Store.JobOf(id).Finishes[0].Outcome);
        Assert.Equal(1, e.Script.InvocationsOf("j"));
    }

    // ------------------------------------------------------------------ WRK

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-01")]
    public async Task One_slow_handler_does_not_hold_the_other_tickets()
    {
        await using var e = EngineFixture.Create(concurrency: 4);
        var slow = e.Add("slow");
        e.Script.On("slow", (_, _, _) => Ignoring(e, TimeSpan.FromSeconds(10)));
        e.Script.Default = (_, _, _) => Ignoring(e, TimeSpan.FromMilliseconds(10));
        for (var i = 0; i < 40; i++)
        {
            e.Add("fast" + i);
        }

        await e.StartAsync();
        e.Step = TimeSpan.FromMilliseconds(10);
        await e.RunUntilAsync(() => e.Script.Completed.Count(c => c.Key.StartsWith("fast", StringComparison.Ordinal)) == 40,
            TimeSpan.FromSeconds(9), "40 fast jobs while the slow one runs");
        Assert.False(e.Script.Completed.ContainsKey("slow"));
        Assert.Equal(JobStates.Running, e.Store.JobOf(slow).State);

        // Each retirement is followed by a new Reserve (per-ticket refill; no batch-wide wait).
        var retired = e.EventCount("retired");
        Assert.True(e.Store.Count(Operations.Reserve) >= retired, $"{e.Store.Count(Operations.Reserve)} reserves for {retired} retirements");
        Assert.True(e.Engine.Pools[0].Gate.Outstanding <= 4);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-03")]
    public async Task Lost_reserve_and_begin_replies_resolve_with_the_same_command()
    {
        await using var e = EngineFixture.Create(concurrency: 1);
        var id = e.Add("j");
        e.Store.DropReply = call => call.Submission == 1 && call.Operation is Operations.Begin
            || (call.Operation == Operations.Reserve && call.Submission == 1 && !e.Store.Calls.Any(c => c.Operation == Operations.Reserve && c.Submission > 1));
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "success");

        var reserves = e.Store.Calls.Where(c => c.Operation == Operations.Reserve).ToList();
        var begins = CallsFor(e, Operations.Begin, id);
        Assert.True(reserves.Count >= 2);
        Assert.Equal(reserves[0].CommandId, reserves[1].CommandId);
        Assert.Equal(reserves[0].Body, reserves[1].Body);
        Assert.True(begins.Count >= 2);
        Assert.All(begins, b => Assert.Equal(begins[0].CommandId, b.CommandId));
        Assert.Equal(1, e.Store.JobOf(id).Reservations);
        Assert.Equal(1, e.Store.JobOf(id).Executions);
        Assert.Equal(1, e.Script.InvocationsOf("j"));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-05")]
    public async Task Synchronous_handler_prefix_runs_off_the_coordinators()
    {
        using var block = new ManualResetEventSlim();
        await using var e = EngineFixture.Create(concurrency: 2);
        var blocked = e.Add("blocking");
        e.Script.On("blocking", (_, _, _) =>
        {
            block.Wait(TimeSpan.FromSeconds(30), CancellationToken.None);
            return Task.CompletedTask;
        });
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("blocking") == 1, Long, "blocking handler entered");

        // While the handler blocks synchronously, the pool claims and completes other work and the lease renews.
        for (var i = 0; i < 5; i++)
        {
            e.Add("other" + i);
        }

        await e.RunUntilAsync(() => e.Script.Completed.Count == 5, Long, "other jobs");
        await e.RunUntilAsync(() => CallsFor(e, Operations.Renew, blocked).Count >= 2, Long, "renewals of the blocked attempt");
        Assert.False(EntryOf(e, blocked).IsLost);
        block.Set();
        await e.RunUntilAsync(() => e.Store.JobOf(blocked).State == JobStates.Succeeded, Long, "blocked job completes");
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-06")]
    public async Task Renewal_deadline_during_a_slow_pipeline_latches_loss()
    {
        await using var e = EngineFixture.Create();
        var id = e.Add("slow");
        var cancelled = new TaskCompletionSource();
        e.Script.On("slow", async (_, _, ct) =>
        {
            await using (ct.Register(() => cancelled.TrySetResult()))
            {
                await Ignoring(e, TimeSpan.FromMinutes(3));
            }
        });
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("slow") == 1, Long, "invocation");

        // Every Renew is delayed past the local safety deadline; the late replies are applied by the store.
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.Renew)
            {
                await Task.Delay(TimeSpan.FromSeconds(70), e.Time);
            }

            return CallEffect.Proceed;
        };
        var entry = EntryOf(e, id);
        var d = entry.Deadline!.Value;
        await e.RunUntilAsync(() => entry.IsLost, Long, "loss");
        Assert.Equal(LossCause.SafetyDeadline, entry.Loss);
        Assert.True(e.Engine.Clock.Now - d <= TimeSpan.FromSeconds(1) + e.Step, "loss latched by the watchdog within one interval of d");
        await e.RunUntilAsync(() => cancelled.Task.IsCompleted, TimeSpan.FromSeconds(1), "cancellation requested");
        await e.AdvanceAsync(TimeSpan.FromMinutes(4), TimeSpan.FromMilliseconds(500));
        Assert.True(entry.IsLost);
        Assert.Equal(0, e.Store.Count(Operations.Finish));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-07")]
    public async Task Lost_finish_reply_blocks_renewal_until_resolved()
    {
        await using var e = EngineFixture.Create();
        var id = e.Add("j");
        e.Store.DropReply = call => call.Operation == Operations.Finish && call.Submission <= 2;
        await e.StartAsync();
        await e.RunUntilAsync(() => e.EventCount("finishReplayed") == 1, Long, "resolution");

        var calls = e.Store.Calls.Where(c => c.JobId == id).ToList();
        var firstFinish = calls.FindIndex(c => c.Operation == Operations.Finish);
        var finishes = calls.Where(c => c.Operation == Operations.Finish).ToList();
        Assert.Equal(3, finishes.Count);
        Assert.All(finishes, f => Assert.Equal((finishes[0].CommandId, finishes[0].Body), (f.CommandId, f.Body)));
        Assert.DoesNotContain(calls.Skip(firstFinish), c => c.Operation == Operations.Renew);
        Assert.Equal(JobStates.Succeeded, e.Store.JobOf(id).State);
        Assert.Single(e.Store.JobOf(id).Finishes);
        Assert.Equal(1, e.Script.InvocationsOf("j"));
    }

    sealed class Probe
    {
        public ConcurrentQueue<(Guid Instance, Guid Attempt, TimeSpan At)> Disposed { get; } = new();
    }

    sealed class ScopedProbe(Probe probe, IJobContext context, EngineFixtureClock clock) : IAsyncDisposable
    {
        public Guid Instance { get; } = Guid.NewGuid();

        public IJobContext Context => context;

        public ValueTask DisposeAsync()
        {
            probe.Disposed.Enqueue((Instance, context.AttemptId.Value, clock.Now()));
            return ValueTask.CompletedTask;
        }
    }

    sealed class EngineFixtureClock(TimeProvider time)
    {
        readonly long _origin = time.GetTimestamp();

        public TimeSpan Now() => time.GetElapsedTime(_origin);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-09")]
    public async Task Ignored_timeout_holds_the_ticket_and_scope_until_the_handler_returns()
    {
        await using var e = EngineFixture.Create(concurrency: 1, jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(1)));
        var id = e.Add("ignore");
        e.Add("queued");
        TimeSpan? returnedAt = null;
        var clock = new EngineFixtureClock(e.Time);
        e.Script.On("ignore", async (_, ctx, _) =>
        {
            await Ignoring(e, TimeSpan.FromSeconds(5));
            returnedAt = clock.Now();
        });
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.InvocationsOf("ignore") == 1, Long, "invocation");
        await e.AdvanceAsync(TimeSpan.FromSeconds(3));

        // Cancellation requested at 1 s; the ticket is still held and the queued job is not reserved.
        Assert.True(EntryOf(e, id).ExecutionToken.IsCancellationRequested);
        Assert.Equal(1, e.Engine.Pools[0].Gate.Outstanding);
        Assert.Equal(0, e.Script.InvocationsOf("queued"));
        Assert.Equal(0, e.Store.Count(Operations.Finish));

        await e.RunUntilAsync(() => e.Store.JobOf(id).Finishes.Count == 1, Long, "finish");
        Assert.Equal(AttemptOutcome.TimedOut, e.Store.JobOf(id).Finishes[0].Outcome);
        Assert.NotNull(returnedAt);
        Assert.True(e.Store.JobOf(id).Finishes[0].ExecutionElapsed >= TimeSpan.FromSeconds(5));
        await e.RunUntilAsync(() => e.Script.InvocationsOf("queued") == 1, Long, "queued job after the slot frees");
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-10")]
    public async Task Slow_and_throwing_cancellation_callbacks_do_not_delay_other_leases()
    {
        using var block = new ManualResetEventSlim();
        await using var e = EngineFixture.Create();
        e.Step = TimeSpan.FromMilliseconds(100);
        var slow = e.Add("slowcb");
        var throwing = e.Add("throwcb");
        var healthy = e.Add("healthy");
        async Task UntilCancelled(CancellationToken ct)
        {
            try
            {
                await Forever(e, ct);
            }
            catch (OperationCanceledException)
            {
                await Ignoring(e, TimeSpan.FromSeconds(2));
            }
        }

        e.Script.On("slowcb", async (_, _, ct) =>
        {
            await using (ct.Register(() => block.Wait(TimeSpan.FromSeconds(60), CancellationToken.None)))
            {
                await UntilCancelled(ct);
            }
        });
        e.Script.On("throwcb", async (_, _, ct) =>
        {
            await using (ct.Register(() => throw new InvalidOperationException("callback fault")))
            {
                await UntilCancelled(ct);
            }
        });
        e.Script.On("healthy", (_, _, _) => Ignoring(e, TimeSpan.FromMinutes(3)));
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.Invocations.Count == 3, Long, "three invocations");

        // Durable cancellation for two attempts: the next Renew reports it and the engine cancels their tokens.
        e.Store.RequestCancellation(slow);
        e.Store.RequestCancellation(throwing);
        await e.RunUntilAsync(() => EntryOf(e, slow).ExecutionToken.IsCancellationRequested && EntryOf(e, throwing).ExecutionToken.IsCancellationRequested,
            Long, "cancellation requested");

        // The slow callback holds a thread-pool thread; the healthy lease keeps renewing on schedule.
        var before = CallsFor(e, Operations.Renew, healthy).Count;
        await e.AdvanceAsync(TimeSpan.FromSeconds(40));
        var renewals = CallsFor(e, Operations.Renew, healthy).Count - before;
        Assert.True(renewals >= 2, $"healthy lease renewed {renewals} times in 40 s");
        Assert.False(EntryOf(e, healthy).IsLost);
        block.Set();
        await e.RunUntilAsync(() => e.Store.JobOf(throwing).Finishes.Count == 1, Long, "finish of the throwing-callback attempt");
        Assert.Contains("callback fault", EntryOf(e, throwing).Diagnostic ?? "", StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-11")]
    public async Task One_async_scope_per_attempt_with_job_context_disposed_once_after_the_pipeline()
    {
        var probe = new Probe();
        await using var e = EngineFixture.Create(
            services: s => s.AddSingleton(probe).AddSingleton<ProbeLog>()
                .AddSingleton(sp => new EngineFixtureClock(sp.GetRequiredService<TimeProvider>())).AddScoped<ScopedProbe>(),
            configure: r => r.UseMiddleware<ProbeMiddleware>());
        e.Script.Default = (_, _, _) => Ignoring(e, TimeSpan.FromMilliseconds(100));
        for (var i = 0; i < 6; i++)
        {
            e.Add("s" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded) && probe.Disposed.Count == 6, Long, "all succeeded");
        var log = e.Services.GetRequiredService<ProbeLog>();
        Assert.Equal(6, log.Entries.Count);
        Assert.Equal(6, log.Entries.Select(x => x.Instance).Distinct().Count());
        Assert.Equal(6, log.Entries.Select(x => x.Attempt).Distinct().Count());
        Assert.All(log.Entries, x => Assert.Equal("test.work", x.JobName));
        Assert.Equal(6, probe.Disposed.Select(x => x.Instance).Distinct().Count());
        Assert.All(log.Entries, x => Assert.Single(probe.Disposed, d => d.Instance == x.Instance && d.At >= x.PipelineEnd));
        Assert.Equal(6, Interlocked.Read(ref e.Engine.ScopesCreated));
    }

    sealed class ProbeLog
    {
        public ConcurrentQueue<(Guid Instance, Guid Attempt, string JobName, TimeSpan PipelineEnd)> Entries { get; } = new();
    }

    sealed class ProbeMiddleware(ScopedProbe probe, ProbeLog log, EngineFixtureClock clock) : IJobMiddleware
    {
        public async Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            var result = await next();
            log.Entries.Enqueue((probe.Instance, probe.Context.AttemptId.Value, context.JobName, clock.Now()));
            return result;
        }
    }

    sealed class TwiceMiddleware : IJobMiddleware
    {
        public async Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            await next();
            return await next();
        }
    }

    sealed class EarlyReturnMiddleware : IJobMiddleware
    {
        public Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            // Starts the downstream pipeline and returns without awaiting it or an explicit outcome.
            _ = next();
            return Task.FromResult<JobExecutionResult>(null!);
        }
    }

    [Theory]
    [InlineData("twice")]
    [InlineData("early")]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-12")]
    public async Task Middleware_misuse_is_a_permanent_failure_without_a_second_handler_call(string variant)
    {
        var handlerDone = new TaskCompletionSource();
        await using var e = EngineFixture.Create(configure: r =>
        {
            if (variant == "twice")
            {
                r.UseMiddleware<TwiceMiddleware>();
            }
            else
            {
                r.UseMiddleware<EarlyReturnMiddleware>();
            }
        });
        var id = e.Add("m");
        e.Script.On("m", async (_, _, _) =>
        {
            await Ignoring(e, TimeSpan.FromMilliseconds(500));
            handlerDone.TrySetResult();
        });
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).Finishes.Count == 1, Long, "finish");
        var finish = e.Store.JobOf(id).Finishes[0];
        Assert.Equal(AttemptOutcome.PermanentFailure, finish.Outcome);
        Assert.Equal("pipeline_contract_fault", finish.Error!.Code);
        Assert.Equal(1, e.Script.InvocationsOf("m"));

        // The downstream task was joined before the scope ended: the handler finished before Finish was sent.
        Assert.True(handlerDone.Task.IsCompleted);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-13")]
    public async Task Malformed_payload_begins_then_fails_permanently_with_bytes_unchanged()
    {
        await using var e = EngineFixture.Create();
        var good = e.Add("good");
        var intent = e.Store.JobOf(good).Intent;
        var badBytes = "{\"id\":"u8.ToArray();
        var bad = e.Store.Add(intent with { PayloadUtf8 = badBytes });
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.Finishes.Count == 1), Long, "finishes");
        Assert.Equal(1, e.Store.JobOf(bad).Executions);
        Assert.Equal(AttemptOutcome.PermanentFailure, e.Store.JobOf(bad).Finishes[0].Outcome);
        Assert.Equal(badBytes, e.Store.JobOf(bad).Intent.PayloadUtf8);
        Assert.Equal(AttemptOutcome.Succeeded, e.Store.JobOf(good).Finishes[0].Outcome);
        Assert.Single(e.Script.Invocations);

        // Capabilities advertise the registered tuple only.
        var contract = Assert.Single(e.Engine.Contracts, c => c.Name == "test.work");
        Assert.Equal(intent.PayloadVersion, contract.PayloadVersion);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-16")]
    public async Task Polling_discovers_work_without_wake_signals()
    {
        await using var e = EngineFixture.Create();
        e.Step = TimeSpan.FromMilliseconds(20);
        await e.StartAsync();
        await e.AdvanceAsync(TimeSpan.FromSeconds(30));

        // Idle backoff 100 ms doubling to 2 s (±20 %): returning an unused ticket must not wake the loop early.
        // 30 s idle is ~19 probes at nominal spacing; a self-woken loop probes every step.
        Assert.InRange(e.Engine.Readiness.Probes, 5, 30);

        // No local post-commit wake: the job appears in the store only.
        var added = e.Engine.Clock.Now;
        var id = e.Add("late");
        await e.RunUntilAsync(() => e.Store.Calls.Any(c => c.Operation == Operations.Begin && c.JobId == id), Long, "discovery");
        var reserved = e.Events.First(x => x.Name == "reserved" && x.Entry!.Lease.JobId == id).At;
        Assert.True(reserved - added <= TimeSpan.FromSeconds(2.4) + WorkerSettings.HintMaxAge + e.Step,
            $"discovered after {reserved - added}");
        await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "success");
        Assert.Equal(1, e.Script.InvocationsOf("late"));
    }

    sealed class ThrowingDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => throw new InvalidOperationException("dispose fault");
    }

    sealed class UsesThrowing(ThrowingDisposable d) : IJobMiddleware
    {
        public Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
        {
            _ = d;
            return next();
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "WRK-17")]
    public async Task Scope_cleanup_failure_after_timely_success_keeps_success()
    {
        await using var e = EngineFixture.Create(services: s => s.AddScoped<ThrowingDisposable>(), configure: r => r.UseMiddleware<UsesThrowing>());
        var id = e.Add("j");
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).Finishes.Count == 1, Long, "finish");
        await e.AdvanceAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(AttemptOutcome.Succeeded, Assert.Single(e.Store.JobOf(id).Finishes).Outcome);
        Assert.Equal(1, e.Script.InvocationsOf("j"));
        Assert.Contains(e.Engine.Health().Diagnostics, d => d.Contains("dispose fault", StringComparison.Ordinal));
    }
}
