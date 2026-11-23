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

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_single_free_slot_uses_reserve_and_begin()
    {
        await using var e = EngineFixture.Create(concurrency: 1, batch: true);
        var id = e.Add("single");
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "single job succeeded");

        var claim = e.Store.Calls.First(c => c.Operation is Operations.ReserveAndBegin or Operations.Reserve);
        Assert.Equal(1, Assert.IsType<ReserveAndBeginRequest>(claim.Body).MaxJobs);
        Assert.Equal(0, e.Store.Count(Operations.Begin));
        Assert.Equal(1, e.Script.InvocationsOf("single"));
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_batch_in_flight_resolves_after_admission_closes_without_sending_a_fresh_claim()
    {
        await using var e = EngineFixture.Create(concurrency: 2, batch: true);
        var ids = new[] { e.Add("first"), e.Add("second"), e.Add("pending") };
        var batch = new TaskBarrier("ReserveAndBegin");
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.ReserveAndBegin)
            {
                await batch.ArriveAndWaitAsync(call.Token);
            }

            return CallEffect.Proceed;
        };
        e.Store.DropReply = call => call.Operation == Operations.ReserveAndBegin && call.Submission == 1;

        await e.StartAsync();
        await e.RunUntilAsync(() => batch.Reached.IsCompleted, Long, "batch reached the store");
        e.Engine.CloseAdmission("test closed admission during claim");
        batch.Release();

        var pool = Assert.Single(e.Engine.Pools);
        await e.RunUntilAsync(() => e.EventCount("retired") == 2 && pool.Gate.Outstanding == 0, Long, "both tickets retired");
        var claims = e.Store.Calls.Where(c => c.Operation == Operations.ReserveAndBegin).ToArray();
        Assert.Equal(2, claims.Length);
        Assert.Equal(claims[0].CommandId, claims[1].CommandId);
        Assert.Equal(claims[0].Body, claims[1].Body);
        Assert.Equal(2, Assert.IsType<ReserveAndBeginRequest>(claims[0].Body).MaxJobs);
        Assert.Equal(0, e.Store.Count(Operations.Reserve));
        Assert.Equal(0, e.Store.Count(Operations.Begin));
        Assert.All(ids.Take(2), id =>
        {
            var job = e.Store.JobOf(id);
            Assert.Equal(JobStates.Succeeded, job.State);
            Assert.Equal(1, job.Executions);
            Assert.Single(job.Finishes);
        });
        Assert.Equal(JobStates.Pending, e.Store.JobOf(ids[2]).State);
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_refused_batch_does_not_fall_back_after_admission_closes()
    {
        await using var e = EngineFixture.Create(concurrency: 2, batch: true);
        var stale = e.Add("stale");
        var batch = new TaskBarrier("ReserveAndBegin");
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.ReserveAndBegin)
            {
                e.Store.JobOf(stale).State = JobStates.Cancelled;
                await batch.ArriveAndWaitAsync(call.Token);
            }

            return CallEffect.Proceed;
        };
        e.Store.Rewrite = (call, reply) => call.Operation == Operations.ReserveAndBegin
            && reply is StoreReply<ClaimedJobs> { Status: StoreStatus.NoWork } r
            ? StoreReply<ClaimedJobs>.NoEffect(StoreStatus.UnsupportedVersion, new StoreError("unsupported_version"), r.DecidedAt)
            : reply;

        await e.StartAsync();
        await e.RunUntilAsync(() => batch.Reached.IsCompleted, Long, "batch reached the store");
        e.Engine.CloseAdmission("test closed admission before a batch refusal");
        batch.Release();

        await e.RunUntilAsync(() => e.Engine.Pools.Single().Gate.Outstanding == 0, Long, "all tickets returned");
        Assert.Equal(1, e.Store.Count(Operations.ReserveAndBegin));
        Assert.Equal(0, e.Store.Count(Operations.Reserve));
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_probe_returning_ready_work_does_not_start_a_claim_after_admission_closes()
    {
        await using var e = EngineFixture.Create(concurrency: 1, batch: true);
        var listener = new ScriptedListener();
        e.Store.Listener = listener;
        await e.StartAsync();
        await e.RunUntilAsync(() => listener.Running.IsCompleted && e.Engine.Readiness.Probes > 0, Long, "initial empty probe");

        var background = e.Engine.BackgroundIo;
        using var first = await background.AcquireAsync(0, CancellationToken.None);
        using var second = await background.AcquireAsync(0, CancellationToken.None);
        var probes = Interlocked.Read(ref e.Engine.Readiness.Probes);
        var id = e.Add("waiting");
        listener.Notify("default");
        await e.RunUntilAsync(() => e.Engine.Readiness.Probes > probes && background.Waiting > 0, Long, "next probe blocked on I/O");
        e.Engine.CloseAdmission("test closed admission while readiness was being fetched");
        first.Dispose();
        second.Dispose();

        await e.RunUntilAsync(() => e.Engine.Pools.Single().Gate.Outstanding == 0, Long, "the opportunity ticket returned");
        Assert.Equal(0, e.Store.Count(Operations.ReserveAndBegin));
        Assert.Equal(0, e.Store.Count(Operations.Reserve));
        Assert.Equal(JobStates.Pending, e.Store.JobOf(id).State);
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_claim_waiting_for_admission_io_is_not_sent_after_admission_closes()
    {
        await using var e = EngineFixture.Create(concurrency: 1, batch: true);
        var id = e.Add("waiting");
        var admission = e.Engine.AdmissionIo;
        using var first = await admission.AcquireAsync(0, CancellationToken.None);
        using var second = await admission.AcquireAsync(0, CancellationToken.None);
        using var third = await admission.AcquireAsync(0, CancellationToken.None);
        using var fourth = await admission.AcquireAsync(0, CancellationToken.None);
        await e.StartAsync();
        await e.RunUntilAsync(() => admission.Waiting == 1, Long, "claim waiting for its I/O slot");

        e.Engine.CloseAdmission("test closed admission while the claim was queued");
        first.Dispose();

        await e.RunUntilAsync(() => e.Engine.Pools.Single().Gate.Outstanding == 0, Long, "the opportunity ticket returned");
        Assert.Equal(0, e.Store.Count(Operations.ReserveAndBegin));
        Assert.Equal(0, e.Store.Count(Operations.Reserve));
        Assert.Equal(JobStates.Pending, e.Store.JobOf(id).State);
    }

    // ------------------------------------------------------------------ claims in flight (05 §9.7)

    /// <summary>Counts batched claims between arrival at the store and their reply, and checks no claim asks for more than the free slots.</summary>
    sealed class ClaimWatch(EngineFixture e, int slots)
    {
        int _inFlight, _inFlightJobs;

        public int MaxInFlight;

        public ConcurrentQueue<string> Violations { get; } = new();

        // Arrival and reply are serialized: a reply landing between reading "asked" and "held" would count its jobs in both
        // (CI and local reruns: "3 jobs asked by 2 claims while 2 slots are held"). Entries appear only after their reply.
        readonly Lock _lock = new();

        public void Arrived(StoreCall call)
        {
            var jobs = Assert.IsType<ReserveAndBeginRequest>(call.Body).MaxJobs;
            lock (_lock)
            {
                var n = ++_inFlight;
                var asked = _inFlightJobs += jobs;
                MaxInFlight = Math.Max(MaxInFlight, n);
                // A lower bound of the slots held by running attempts: an admitted entry holds its ticket until it retires.
                var held = e.Engine.Entries.Count(x => !x.Ticket.IsReleased);
                if (asked + held > slots)
                {
                    Violations.Enqueue($"{asked} jobs asked by {n} claims while {held} slots are held by attempts");
                }
            }
        }

        public void Replied(StoreCall call)
        {
            var jobs = Assert.IsType<ReserveAndBeginRequest>(call.Body).MaxJobs;
            lock (_lock)
            {
                _inFlight--;
                _inFlightJobs -= jobs;
            }
        }
    }

    /// <summary>
    /// Handlers of 30, 80 and 130 ms in turn. Equal handlers free every slot in the same 50 ms fake-time step, and a loaded
    /// runner then wakes the pool only after all are free, so it sends one full claim each time and never pipelines (CI).
    /// </summary>
    static Func<WorkJob, IJobContext, CancellationToken, Task> StaggeredHandlers(EngineFixture e)
    {
        var n = 0;
        return (_, _, _) => Task.Delay(TimeSpan.FromMilliseconds(30 + 50 * (Interlocked.Increment(ref n) % 3)), e.Time, CancellationToken.None);
    }

    static ClaimWatch WatchClaims(EngineFixture e, int slots, TimeSpan claimTime, Func<StoreCall, Task<CallEffect>>? also = null)
    {
        var watch = new ClaimWatch(e, slots);
        e.Store.Before = async call =>
        {
            if (call.Operation != Operations.ReserveAndBegin)
            {
                return CallEffect.Proceed;
            }

            watch.Arrived(call);
            var effect = also is null ? CallEffect.Proceed : await also(call);
            if (effect != CallEffect.Proceed)
            {
                watch.Replied(call);
                return effect;
            }

            await Task.Delay(claimTime, e.Time, CancellationToken.None);
            return CallEffect.Proceed;
        };
        e.Store.Rewrite = (call, reply) =>
        {
            if (call.Operation == Operations.ReserveAndBegin)
            {
                watch.Replied(call);
            }

            return reply;
        };
        return watch;
    }

    [Fact]
    [Trait("Category", "Pure")]
    public void Claims_in_flight_defaults_to_one_and_is_validated()
    {
        Assert.Equal(1, new WorkerOptions().ClaimsInFlight);
        Assert.Empty(Validate(o => o.ClaimsInFlight = 4));
        Assert.Contains(Validate(o => o.ClaimsInFlight = 0), x => x.Message.StartsWith("V-25", StringComparison.Ordinal));
        Assert.Contains(Validate(o => o.ClaimsInFlight = 5), x => x.Message.StartsWith("V-25", StringComparison.Ordinal));
    }

    [Theory]
    [Trait("Category", "Pure")]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public async Task Pipelined_claims_only_fill_free_slots(int claimsInFlight)
    {
        await using var e = EngineFixture.Create(concurrency: 4, batch: true, options: o => o.ClaimsInFlight = claimsInFlight);
        // Claims take 100 ms and handlers 30-130 ms, so slots free up while a claim waits for its reply.
        e.Script.Default = StaggeredHandlers(e);
        var watch = WatchClaims(e, 4, TimeSpan.FromMilliseconds(100));
        var ids = Enumerable.Range(0, 80).Select(i => e.Add("p" + i)).ToList();

        await e.StartAsync();
        await e.RunUntilAsync(() => ids.All(id => e.Store.JobOf(id).State == JobStates.Succeeded), Long, "all succeeded");

        var pool = e.Engine.Pools.Single();
        Assert.Empty(watch.Violations);
        // Reaching the full cap needs every slot free while claims are pending, which a loaded runner can miss by one
        // (CI: 3 of 4); the cap is the bound, and more than one in flight proves pipelining happened.
        Assert.InRange(watch.MaxInFlight, Math.Min(2, claimsInFlight), claimsInFlight);
        // The pool counts a claim from send to settlement, the store from arrival to reply, so the pool may see one more.
        if (claimsInFlight == 1)
        {
            Assert.Equal(0, pool.ClaimsInFlightHighWater);
        }
        else
        {
            Assert.InRange(pool.ClaimsInFlightHighWater, watch.MaxInFlight, claimsInFlight);
        }
        Assert.All(ids, id => Assert.Equal(1, e.Store.JobOf(id).Executions));
        Assert.Equal(0, e.Store.Count(Operations.Reserve));
        await e.StopAsync();
        Assert.Equal(0, pool.Gate.Outstanding);
        Assert.Equal(0, pool.ClaimsInFlight);
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task Pipelining_claims_more_jobs_per_slot_second_than_one_claim_at_a_time()
    {
        async Task<int> Claims(int claimsInFlight)
        {
            await using var e = EngineFixture.Create(concurrency: 8, batch: true, options: o => o.ClaimsInFlight = claimsInFlight);
            e.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromMilliseconds(20), e.Time, CancellationToken.None);
            WatchClaims(e, 8, TimeSpan.FromMilliseconds(100));
            var ids = Enumerable.Range(0, 200).Select(i => e.Add("q" + i)).ToList();
            await e.StartAsync();
            var start = e.Time.GetUtcNow();
            await e.RunUntilAsync(() => ids.All(id => e.Store.JobOf(id).State == JobStates.Succeeded), Long, "all succeeded", TimeSpan.FromMilliseconds(10));
            return (int)(e.Time.GetUtcNow() - start).TotalMilliseconds;
        }

        // Fake time only moves in the test, so this compares slot idle time, not machine speed.
        var one = await Claims(1);
        var two = await Claims(2);
        Assert.True(two < one, $"two claims in flight took {two} ms of fake time, one took {one} ms");
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task Shutdown_waits_for_a_pipelined_claim_and_runs_what_it_claimed()
    {
        await using var e = EngineFixture.Create(concurrency: 4, batch: true, options: o => o.ClaimsInFlight = 2);
        e.Script.Default = StaggeredHandlers(e);
        var ids = Enumerable.Range(0, 40).Select(i => e.Add("s" + i)).ToList();
        var held = new TaskCompletionSource<StoreCall>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sentWhileDraining = 0;
        var watch = WatchClaims(e, 4, TimeSpan.FromMilliseconds(100), async call =>
        {
            if (call.Submission == 1 && e.Engine.Draining)
            {
                Interlocked.Increment(ref sentWhileDraining);
            }

            // The first claim sent while another is in flight is held until the drain has begun.
            if (call.Submission == 1 && e.Engine.Pools.Single().ClaimsInFlight == 2 && held.TrySetResult(call))
            {
                while (!e.Engine.Draining)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10), e.Time, CancellationToken.None);
                }
            }

            return CallEffect.Proceed;
        });

        await e.StartAsync();
        await e.RunUntilAsync(() => held.Task.IsCompleted, Long, "a second claim in flight");
        var claim = await held.Task;
        await e.StopAsync();

        var pool = e.Engine.Pools.Single();
        Assert.Empty(watch.Violations);
        Assert.Equal(0, sentWhileDraining);
        Assert.Equal(0, pool.Gate.Outstanding);
        Assert.Equal(0, pool.ClaimsInFlight);
        Assert.Empty(e.Engine.Entries);
        // Everything claimed ran once and finished; nothing is left reserved or running in the store.
        Assert.All(ids, id => Assert.Contains(e.Store.JobOf(id).State, new[] { JobStates.Succeeded, JobStates.Pending }));
        Assert.All(ids, id => Assert.Equal(e.Store.JobOf(id).State == JobStates.Succeeded ? 1 : 0, e.Store.JobOf(id).Executions));
        Assert.Contains(ids, id => e.Store.JobOf(id).State == JobStates.Pending);
        var claimed = Assert.IsType<ReserveAndBeginRequest>(claim.Body).MaxJobs;
        Assert.True(ids.Count(id => e.Store.JobOf(id).State == JobStates.Succeeded) >= claimed);
    }

    [Theory]
    [Trait("Category", "Pure")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_claim_that_fails_while_another_is_in_flight_resolves_by_resubmitting_the_same_command(bool timesOut)
    {
        await using var e = EngineFixture.Create(concurrency: 4, batch: true, options: o => o.ClaimsInFlight = 2);
        e.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromMilliseconds(30), e.Time, CancellationToken.None);
        var ids = Enumerable.Range(0, 40).Select(i => e.Add("f" + i)).ToList();
        Guid? failing = null;
        var gate = new Lock();
        var watch = WatchClaims(e, 4, TimeSpan.FromMilliseconds(50), call =>
        {
            // The first claim sent while another is in flight fails: its reply is lost, or it hangs until its request timeout.
            lock (gate)
            {
                if (failing is null && call.Submission == 1 && e.Engine.Pools.Single().ClaimsInFlight == 2)
                {
                    failing = call.CommandId;
                }
            }

            return Task.FromResult(timesOut && call.CommandId == failing && call.Submission == 1 ? CallEffect.Hang : CallEffect.Proceed);
        });
        e.Store.DropReply = call => !timesOut && call.CommandId == failing && call.Submission == 1;

        await e.StartAsync();
        // A hung claim reserved nothing, so the others can drain every job before its request timeout ends and it is resubmitted.
        await e.RunUntilAsync(() => ids.All(id => e.Store.JobOf(id).State == JobStates.Succeeded)
            && e.Store.Calls.Count(c => c.CommandId == failing) >= 2 && e.Engine.Pools.Single().ClaimsInFlight == 0, Long, "all succeeded");

        var calls = e.Store.Calls.Where(c => c.CommandId == failing).ToList();
        Assert.True(calls.Count >= 2, "the failed claim was resubmitted");
        Assert.All(calls, c => Assert.Equal(calls[0].Body, c.Body));
        Assert.Empty(watch.Violations);
        Assert.Equal(2, e.Engine.Pools.Single().ClaimsInFlightHighWater);
        Assert.All(ids, id => Assert.Equal(1, e.Store.JobOf(id).Executions));
        await e.StopAsync();
        Assert.Equal(0, e.Engine.Pools.Single().Gate.Outstanding);
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
    [Trait("Item", "PF2-G5")]
    public async Task Fewer_renewals_profile_sends_a_sixth_of_the_renewals_and_keeps_the_timing_rules()
    {
        // The profile passes the same V-01..V-13 rules the defaults do, and keeps a positive stop margin.
        var settings = WorkerSettings.Validate(new WorkerBuilder().Pool("p", p => p.Queues("default")).Configure(o => o.UseFewerLeaseRenewals()), ["default"]);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.Options.LeaseDuration);
        Assert.True(settings.StopMargin > TimeSpan.Zero);
        Assert.Empty(Validate(o => o.UseFewerLeaseRenewals()));

        async Task<int> RenewalsInTenMinutes(Action<WorkerOptions>? options)
        {
            await using var e = EngineFixture.Create(options: options);
            e.Step = TimeSpan.FromMilliseconds(250);
            var id = e.Add("long");
            e.Script.On("long", (_, _, ct) => Forever(e, ct));
            await e.StartAsync();
            await e.RunUntilAsync(() => e.Script.InvocationsOf("long") == 1, Long, "invocation");
            await e.AdvanceAsync(TimeSpan.FromMinutes(10));
            Assert.False(EntryOf(e, id).IsLost);
            return CallsFor(e, Operations.Renew, id).Count;
        }

        var normal = await RenewalsInTenMinutes(null);
        var fewer = await RenewalsInTenMinutes(o => o.UseFewerLeaseRenewals());

        Assert.InRange(normal, 30, 55);
        Assert.InRange(fewer, 5, 9);
        Assert.True(normal >= 4 * fewer, $"default {normal} renewals, fewer-renewals profile {fewer}");
    }

    static Func<StoreCall, Task<CallEffect>> HoldFinishes(Func<bool> open) =>
        call => Task.FromResult(call.Operation is Operations.Finish or Operations.FinishMany && !open() ? CallEffect.Hang : CallEffect.Proceed);

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public void Buffered_finish_is_off_by_default_and_its_bounds_are_validated()
    {
        Assert.False(new WorkerOptions().BufferedFinish);
        Assert.Empty(Validate(o => o.UseBufferedFinish()));
        Assert.Contains(Validate(o => o.UseBufferedFinish(TimeSpan.Zero)), x => x.Message.StartsWith("V-22", StringComparison.Ordinal));
        Assert.Contains(Validate(o => o.UseBufferedFinish(TimeSpan.FromSeconds(2))), x => x.Message.StartsWith("V-22", StringComparison.Ordinal));
        Assert.Contains(Validate(o => o.UseBufferedFinish(maxCount: 0)), x => x.Message.StartsWith("V-22", StringComparison.Ordinal));
        Assert.Contains(Validate(o => o.UseBufferedFinish(maxBytes: 1024)), x => x.Message.StartsWith("V-22", StringComparison.Ordinal));
        // Outside the profile the three bounds are not looked at.
        Assert.Empty(Validate(o => o.FinishBufferMaxCount = 0));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Buffered_finish_reuses_the_slot_before_the_finish_settles()
    {
        async Task<int> StartedWhileFinishesAreHeld(Action<WorkerOptions>? options)
        {
            await using var e = EngineFixture.Create(concurrency: 1, options: options, finishMany: true);
            for (var i = 0; i < 6; i++)
            {
                e.Add("j" + i);
            }

            e.Store.Before = HoldFinishes(() => false);
            await e.StartAsync();
            await e.AdvanceAsync(TimeSpan.FromSeconds(2));
            return e.Script.Invocations.Count;
        }

        Assert.Equal(1, await StartedWhileFinishesAreHeld(null));
        Assert.Equal(6, await StartedWhileFinishesAreHeld(o => o.UseBufferedFinish()));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Buffered_finish_stops_reusing_slots_at_its_count_bound_and_resumes_when_the_store_accepts()
    {
        var open = false;
        await using var e = EngineFixture.Create(concurrency: 1, options: o => o.UseBufferedFinish(maxCount: 3), finishMany: true);
        var ids = Enumerable.Range(0, 10).Select(i => e.Add("j" + i)).ToList();
        e.Store.Before = HoldFinishes(() => Volatile.Read(ref open));
        await e.StartAsync();
        await e.AdvanceAsync(TimeSpan.FromSeconds(2));

        // Three finishes buffered, so the slot was given back three times: the fourth job ran and kept its slot.
        Assert.Equal(4, e.Script.Invocations.Count);
        Assert.Equal(3, e.Engine.Completion.Buffered);
        Assert.Equal(3, e.Engine.Completion.BufferedHighWater);
        Assert.True(Interlocked.Read(ref e.Engine.Completion.BufferFullRefusals) >= 1);

        Volatile.Write(ref open, true);
        await e.RunUntilAsync(() => ids.All(id => e.Store.JobOf(id).State == JobStates.Succeeded), Long, "every job settled after the store accepted");
        Assert.Equal(10, e.Script.Invocations.Count);
        Assert.Equal(0, e.Engine.Completion.Buffered);
        Assert.All(ids, id => Assert.Single(e.Store.JobOf(id).Finishes));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Buffered_finish_bounds_the_bytes_it_holds()
    {
        var open = false;
        // Each finish is estimated at 256 bytes plus its error text; a 64 KiB bound allows only a few handlers that fail with long messages.
        await using var e = EngineFixture.Create(concurrency: 1, options: o => o.UseBufferedFinish(maxBytes: 64 * 1024), finishMany: true);
        for (var i = 0; i < 6; i++)
        {
            var id = e.Add("big" + i);
            e.Script.On("big" + i, (_, _, _) => throw new InvalidOperationException(new string('x', 20_000)));
        }

        e.Store.Before = HoldFinishes(() => Volatile.Read(ref open));
        await e.StartAsync();
        await e.AdvanceAsync(TimeSpan.FromSeconds(2));

        // About 40 KB of text per failed attempt: one fits under 64 KiB, the second does not.
        Assert.True(Interlocked.Read(ref e.Engine.Completion.BufferFullRefusals) >= 1);
        Assert.InRange(e.Engine.Completion.Buffered, 1, 2);
        Assert.True(e.Script.Invocations.Count <= 3, $"{e.Script.Invocations.Count} handlers started");
        Volatile.Write(ref open, true);
        await e.RunUntilAsync(() => e.Engine.Completion.Buffered == 0, Long, "buffer empties");
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Buffered_finish_sends_finishes_that_finish_together_in_one_call_after_the_flush_interval()
    {
        await using var e = EngineFixture.Create(concurrency: 8, options: o => o.UseBufferedFinish(TimeSpan.FromMilliseconds(100)), finishMany: true);
        for (var i = 0; i < 8; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        // The store flips the jobs inside the FinishMany call; the engine counts them only after the reply, so wait for that too.
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded) && e.Engine.Completion.Buffered == 0, Long, "all succeeded and unbuffered");
        Assert.True(e.Store.Count(Operations.FinishMany) >= 1);
        Assert.True(Interlocked.Read(ref e.Engine.Completion.BatchedItemsSettled) >= 4, $"{e.Engine.Completion.BatchedItemsSettled} items batched");
        Assert.Equal(0, e.Engine.Completion.Buffered);

        // The default sends a lone finish at once; buffered finish waits the interval for company, then goes alone.
        await using var one = EngineFixture.Create(concurrency: 1, options: o => o.UseBufferedFinish(TimeSpan.FromMilliseconds(100)), finishMany: true);
        var id = one.Add("lone");
        await one.StartAsync();
        await one.RunUntilAsync(() => one.Store.JobOf(id).State == JobStates.Succeeded, Long, "lone job settled");
        Assert.True(Interlocked.Read(ref one.Engine.Completion.FlushesByAge) >= 1);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Buffered_finish_is_flushed_at_once_when_the_worker_drains()
    {
        // A one-second interval would hold these finishes for a second; the stop signal sends them now.
        await using var e = EngineFixture.Create(concurrency: 4, options: o => o.UseBufferedFinish(TimeSpan.FromSeconds(1)), finishMany: true);
        e.Step = TimeSpan.FromMilliseconds(10);
        for (var i = 0; i < 4; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.Invocations.Count == 4 && e.Engine.Completion.Buffered > 0, Long, "handlers done, finishes buffered");
        await e.StopAsync();
        Assert.All(e.Store.Jobs, j => Assert.Equal(JobStates.Succeeded, j.State));
        Assert.True(Interlocked.Read(ref e.Engine.Completion.FlushesForced) >= 1);
        Assert.Equal(0, e.Engine.Completion.Buffered);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task A_buffered_attempt_stays_tracked_and_fenced_until_the_store_accepts_the_finish()
    {
        var open = false;
        await using var e = EngineFixture.Create(concurrency: 1, options: o => o.UseBufferedFinish(), finishMany: true);
        e.Step = TimeSpan.FromMilliseconds(250);
        var id = e.Add("held");
        e.Store.Before = HoldFinishes(() => Volatile.Read(ref open));
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Engine.Completion.Buffered == 1, Long, "finish buffered");

        // The slot is back (ticket released) but the attempt is still in the engine's table, in Finishing, with its authority.
        // Renewals stop while the Finish call is in flight, exactly as without the profile (05 §11), so the lease is what bounds it.
        await e.AdvanceAsync(TimeSpan.FromSeconds(20));
        var entry = EntryOf(e, id);
        Assert.True(entry.Ticket.IsReleased);
        Assert.Equal(AttemptPhase.Finishing, entry.Phase);
        Assert.False(entry.IsLost);
        Assert.Contains(entry, e.Engine.Entries);
        Assert.Equal(JobStates.Running, e.Store.JobOf(id).State);

        Volatile.Write(ref open, true);
        await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "finish accepted");
        Assert.Single(e.Store.JobOf(id).Finishes);
        await e.RunUntilAsync(() => e.Engine.Entries.Count == 0, Long, "attempt retired");
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF2-G1")]
    public async Task Finishes_that_never_reach_the_store_leave_the_job_to_expire_and_be_recovered()
    {
        // The process dies (or the store never answers) while the finish is in memory: nothing was accepted, so the lease
        // expires and recovery takes the job back to retry. The handler already ran once, so it runs again: the at-least-once
        // cost of the mode.
        await using var e = EngineFixture.Create(concurrency: 1, options: o => o.UseBufferedFinish(), finishMany: true);
        var id = e.Add("rerun");
        e.Store.Before = HoldFinishes(() => false);
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.JobOf(id).History.Contains("recovered:abandoned_running"), TimeSpan.FromMinutes(10), "recovery after expiry");
        Assert.Empty(e.Store.JobOf(id).Finishes);
        Assert.Equal(JobStates.Retrying, e.Store.JobOf(id).State);
        Assert.Equal(1, e.Script.InvocationsOf("rerun"));
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

    [Theory]
    [InlineData(1000, 0, true)]
    [InlineData(999, 0, false)]
    [InlineData(100, 3000, false)]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "TIME-04")]
    public async Task Classification_at_the_timeout_boundary(int elapsedMs, int finishDelayMs, bool timedOut)
    {
        await using var e = EngineFixture.Create(jobs: j => j.WithExecutionTimeout(TimeSpan.FromSeconds(1)));
        var id = e.Add("boundary");
        var handler = new TaskBarrier("handler entered");
        var finish = new TaskBarrier("Finish sent");
        e.Script.On("boundary", (_, _, _) => handler.ArriveAndWaitAsync(CancellationToken.None));
        e.Store.Before = async call =>
        {
            if (call.Operation == Operations.Finish && call.JobId == id)
            {
                await finish.ArriveAndWaitAsync(call.Token);
            }

            return CallEffect.Proceed;
        };

        try
        {
            await e.StartAsync();
            await handler.WaitReachedAsync();
            var elapsed = TimeSpan.FromMilliseconds(elapsedMs);
            e.Time.Advance(elapsed);
            handler.Release();

            // Freeze time at the intended boundary until the pipeline has recorded its observation.
            await finish.WaitReachedAsync();
            if (finishDelayMs > 0)
            {
                await e.AdvanceAsync(TimeSpan.FromMilliseconds(finishDelayMs), TimeSpan.FromSeconds(1));
                Assert.Empty(e.Store.JobOf(id).Finishes);
            }

            finish.Release();
            await e.RunUntilAsync(() => e.Store.JobOf(id).Finishes.Count == 1, Long, "finish settled");
            var settled = Assert.Single(e.Store.JobOf(id).Finishes);
            Assert.Equal(timedOut ? AttemptOutcome.TimedOut : AttemptOutcome.Succeeded, settled.Outcome);
            Assert.Equal(elapsed, settled.ExecutionElapsed);
        }
        finally
        {
            handler.Release();
            finish.Release();
        }
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
        // The loss is marked just before the diagnostic is recorded, so wait for both.
        await e.RunUntilAsync(() => EntryOf(e, id).IsLost
            && e.Engine.Health().Diagnostics.Any(d => d.Contains("unconfirmed completion", StringComparison.Ordinal)), Long, "cleanup budget");
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

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_store_that_names_the_next_due_job_ends_the_idle_wait_at_that_time()
    {
        // Without the hint the idle backoff climbs to 2 s, so 12 s of idleness is about ten probes. A probe that says the next job falls
        // due 600 ms later makes the pool sleep exactly that long (never under the 50 ms probe spacing), so it probes about every 0.6 s.
        await using var e = EngineFixture.Create();
        e.Step = TimeSpan.FromMilliseconds(20);
        e.Store.NextDueIn = TimeSpan.FromMilliseconds(600);
        await e.StartAsync();
        await e.AdvanceAsync(TimeSpan.FromSeconds(12));
        Assert.InRange(e.Engine.Readiness.Probes, 15, 25);
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_pool_idle_at_the_long_cap_starts_a_delayed_job_at_its_due_time_after_its_wake_hint()
    {
        // With a healthy listener the idle wait climbs to 10 s (±20 %). A delayed dispatch is not due yet, so its wake hint (published when
        // it becomes the earliest not-yet-due job of its lane) and the probe's NextDueAt are what end that wait on time. Without the hint
        // the job waited out the back-off: the delayed-pickup tail (docs/reports/fragments/delayed-pickup.md).
        await using var e = EngineFixture.Create();
        e.Step = TimeSpan.FromMilliseconds(20);
        var listener = new ScriptedListener();
        e.Store.Listener = listener;
        await e.StartAsync();
        await e.RunUntilAsync(() => listener.Running.IsCompleted, Long, "listener started");
        listener.Subscribe();
        await e.AdvanceAsync(TimeSpan.FromSeconds(30));

        e.Store.NextDueAt = e.Time.GetUtcNow() + TimeSpan.FromSeconds(3);
        listener.Notify("default");
        await e.AdvanceAsync(TimeSpan.FromSeconds(3));
        var due = e.Engine.Clock.Now;
        var id = e.Add("delayed"); // falls due now, with no notification
        await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "reserved" && x.Entry!.Lease.JobId == id), Long, "reserved");
        var late = e.Events.First(x => x.Name == "reserved" && x.Entry!.Lease.JobId == id).At - due;
        Assert.True(late <= TimeSpan.FromMilliseconds(200), $"started {late} after its due time");
    }

    [Fact]
    [Trait("Category", "Pure")]
    public async Task A_notification_reactivates_an_excluded_lane_while_another_queue_is_cooling()
    {
        await using var e = EngineFixture.Create(configure: r => r.Queue("other"), pool: p => p.Queue("other", 1));
        var listener = new ScriptedListener();
        e.Store.Listener = listener;
        var wake = Assert.Single(e.Engine.Pools).Wake;
        wake.Suppressed = true;
        var stale = new Dictionary<string, Guid>
        {
            ["default"] = e.Add("stale"),
            ["other"] = e.AddTo("other", "paused"),
        };
        var firstClaim = new TaskBarrier("first stale claim");
        var secondClaim = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var notifiedClaim = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Both jobs are visible in the probe and are gone when their Reserve runs.
        e.Store.Before = async call =>
        {
            if (call.Body is ReserveRequest reserve)
            {
                if (stale.Count == 2)
                {
                    await firstClaim.ArriveAndWaitAsync(call.Token);
                }

                if (stale.Remove(reserve.Queue, out var staleId))
                {
                    e.Store.JobOf(staleId).State = JobStates.Cancelled;
                    if (stale.Count == 0)
                    {
                        secondClaim.TrySetResult();
                    }
                }
                else if (reserve.Queue == "default")
                {
                    notifiedClaim.TrySetResult();
                }
            }

            return CallEffect.Proceed;
        };
        e.Store.Rewrite = (call, reply) => call.Body is ReserveRequest { Queue: "other" }
            && reply is StoreReply<ReservedJob> { Status: StoreStatus.NoWork } r
            ? StoreReply<ReservedJob>.NoEffect(StoreStatus.QueuePaused, new StoreError("queue_paused"), r.DecidedAt)
            : reply;

        try
        {
            await e.StartAsync();
            await e.RunUntilAsync(() => listener.Running.IsCompleted && firstClaim.Reached.IsCompleted, Long, "stale probe consumed");
            e.Time.Advance(WorkerSettings.ProbeSpacing);
            firstClaim.Release();
            await secondClaim.Task.WaitAsync(TimeSpan.FromSeconds(10));

            // A new probe is allowed now. Keep fake time frozen so only the notification can end the idle wait.
            wake.Suppressed = false;
            var id = e.Add("notified");
            listener.Notify("default");
            await notifiedClaim.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await e.RunUntilAsync(() => e.Store.JobOf(id).State == JobStates.Succeeded, Long, "notified job settled");
        }
        finally
        {
            firstClaim.Release();
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF-07")]
    [Trait("Case", "WAKE-03")]
    public async Task A_healthy_listener_lengthens_the_idle_cap_and_a_lost_one_restores_the_short_cap_at_once()
    {
        // 05 Changes 2026-11-02 (idle caps): 10 s while the wake listener is subscribed, 2 s before the first
        // subscription and from the moment the session is lost.
        await using var e = EngineFixture.Create();
        var listener = new ScriptedListener();
        e.Store.Listener = listener;
        e.Step = TimeSpan.FromMilliseconds(20);
        await e.StartAsync();
        await e.RunUntilAsync(() => listener.Running.IsCompleted, Long, "the listener started");
        Assert.False(e.Engine.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(2), e.Engine.IdleBackoffCap);

        // Healthy: after the ramp, 60 s idle is 6 probes at nominal spacing (5–8 with ±20 % jitter); 2 s would be ~30.
        listener.Subscribe();
        Assert.True(e.Engine.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(10), e.Engine.IdleBackoffCap);
        await e.AdvanceAsync(TimeSpan.FromSeconds(30));
        var before = e.Engine.Readiness.Probes;
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.InRange(e.Engine.Readiness.Probes - before, 4, 8);

        // A notification still ends the long wait at once.
        var added = e.Engine.Clock.Now;
        var notified = e.Add("notified");
        listener.Notify("default");
        await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "reserved" && x.Entry!.Lease.JobId == notified), Long, "the notified job");
        Assert.True(e.Events.First(x => x.Name == "reserved" && x.Entry!.Lease.JobId == notified).At - added <= TimeSpan.FromMilliseconds(500));
        await e.RunUntilAsync(() => e.Store.JobOf(notified).State == JobStates.Succeeded, Long, "success");
        await e.AdvanceAsync(TimeSpan.FromSeconds(30));

        // Lost: the pools are woken at once (no wait begun under the long cap outlives the session), then poll at 2 s:
        // 60 s is 30 probes at nominal spacing (25–38 with jitter).
        before = e.Engine.Readiness.Probes;
        listener.Lose();
        Assert.False(e.Engine.ListenerHealthy);
        Assert.Equal(1, Interlocked.Read(ref e.Engine.ListenerLosses));
        Assert.Equal(TimeSpan.FromSeconds(2), e.Engine.IdleBackoffCap);
        await e.AdvanceAsync(TimeSpan.FromMilliseconds(200));
        Assert.True(e.Engine.Readiness.Probes > before, "a lost session wakes the pools");
        await e.AdvanceAsync(TimeSpan.FromSeconds(10));
        before = e.Engine.Readiness.Probes;
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.InRange(e.Engine.Readiness.Probes - before, 24, 40);

        // While lost, un-notified work is found within today's bound: 2 s + 20 % + hint age + one step.
        added = e.Engine.Clock.Now;
        var polled = e.Add("polled");
        await e.RunUntilAsync(() => e.Events.Any(x => x.Name == "reserved" && x.Entry!.Lease.JobId == polled), Long, "the polled job");
        Assert.True(e.Events.First(x => x.Name == "reserved" && x.Entry!.Lease.JobId == polled).At - added
            <= TimeSpan.FromSeconds(2.4) + WorkerSettings.HintMaxAge + e.Step);
        await e.RunUntilAsync(() => e.Store.JobOf(polled).State == JobStates.Succeeded, Long, "the polled job succeeded");

        // Resubscribed: the long cap again.
        listener.Subscribe();
        Assert.True(e.Engine.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(10), e.Engine.IdleBackoffCap);
        Assert.Equal(1, e.Script.InvocationsOf("notified"));
        Assert.Equal(1, e.Script.InvocationsOf("polled"));
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
        Assert.Contains(e.Engine.Health().Diagnostics, d => d.Contains("threw InvalidOperationException", StringComparison.Ordinal));
    }
}
