using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;

namespace Runwick.Postgres.Tests;

/// <summary>CAP-01 (real-store part), CAP-10 (subset) and CAP-11 (subset) (07 §11): SP-006, SP-009.</summary>
public sealed class CapacityFaultTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(180);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    internal static async Task WaitCount(DriverFixture f, string sql, long expected, string what)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (await f.CountAsync(sql) != expected)
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            await Task.Delay(250);
        }
    }

    /// <summary>Samples <paramref name="sample"/> every few milliseconds until disposed.</summary>
    internal sealed class Sampler : IAsyncDisposable
    {
        readonly CancellationTokenSource _stop = new();
        readonly Task _loop;

        public Sampler(Action sample, int periodMs = 5) => _loop = Task.Run(async () =>
        {
            while (!_stop.IsCancellationRequested)
            {
                sample();
                await Task.Delay(periodMs);
            }
        });

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await _loop;
            _stop.Dispose();
        }
    }

    // ------------------------------------------------------------------ CAP-01

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-01")]
    public async Task Fixed_sixteen_with_hung_handlers_admits_no_replacement()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap01"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 16);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.Default = (_, _, _) => release.Task;
        for (var i = 0; i < 24; i++)
        {
            await w.DispatchAsync("hung" + i);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, Wait, "16 hung handlers");
        var maxEntries = 0;
        await using (new Sampler(() => maxEntries = Math.Max(maxEntries, w.Engine.Entries.Count)))
        {
            await Task.Delay(TimeSpan.FromSeconds(20));
        }

        Assert.Equal(16, maxEntries);
        Assert.Equal(16, w.Script.Invocations.Count);
        Assert.Equal(16, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.Equal(8, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));
        release.SetResult();
        await WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 24, "all 24 succeeded");
        await w.StopAsync();
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    // ------------------------------------------------------------------ CAP-10 (subset)

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-10")]
    public async Task Renewal_waves_and_concurrent_finishes_stay_in_their_lanes_and_gate_admission()
    {
        // Default timing (StopMargin 25 s, ResumeMargin 30 s); every reply to a control-connection COMMIT arrives 1 s late
        // (07 §2.5 note: CommitReplyLatency, never per-message latency, models a slow control round trip).
        await using var f = await DriverFixture.StartAsync(Ns("cap10"), StoreProfile.DirectPrimaryViaProxy);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 16, divisor: 1);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var i = 0; i < 16; i++)
        {
            w.Script.On("wave" + i, (_, _, _) => release.Task);
            await w.DispatchAsync("wave" + i);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, Wait, "16 long handlers");
        var maxRenewal = 0;
        var maxSettlement = 0;
        var maxControlWait = TimeSpan.Zero;
        var sampler = new Sampler(() =>
        {
            maxRenewal = Math.Max(maxRenewal, w.Engine.RenewalIo.Held);
            maxSettlement = Math.Max(maxSettlement, w.Engine.SettlementIo.Held);
            var wait = w.Engine.Lease.OldestWait(w.Engine.Clock.Now);
            maxControlWait = wait > maxControlWait ? wait : maxControlWait;
        });

        // The renewal wave: every attempt renews once with 1 s replies.
        f.Proxy!.SetCommitReplyLatency(TimeSpan.FromSeconds(1), PgFaultProxy.Role("control"));
        await WorkerHost.WaitUntil(() => w.EventCount("renewed") >= 16, Wait, "one renewal per attempt");

        // Concurrent Finishes through the settlement lane: the oldest published Finish waits ≥ 5 s, which stops
        // admission; the jobs dispatched now must not be reserved until three healthy checks resume it.
        release.SetResult();
        for (var i = 0; i < 4; i++)
        {
            await w.DispatchAsync("after" + i);
        }

        await WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 20, "all 20 succeeded");
        f.Proxy.SetCommitReplyLatency(TimeSpan.Zero);
        await sampler.DisposeAsync();

        // A stop raised by the last slow Finishes resumes only after three healthy checks, so the events are read once
        // admission has resumed (G-1 run 3 read them while the last stop was still in force).
        await WorkerHost.WaitUntil(() => !w.Engine.PressureStopped, Wait, "admission resumed");
        var events = w.Events.ToList();
        Assert.InRange(maxRenewal, 1, 4);
        Assert.Equal(2, maxSettlement);
        Assert.True(maxControlWait > TimeSpan.Zero, "no renewal queued behind the lane");
        Assert.Contains(events, e => e.Name.StartsWith("pressureStop", StringComparison.Ordinal));
        CheckStops(w, events);

        // Without latency the gates do not flap: 16 handlers started together through two renewal waves. Their Finishes
        // are 1 s apart: 16 simultaneous Finishes through two settlement lanes are the overload of the first phase, not
        // ordinary jitter, and queue ≥ 5 s on a host whose Finish takes 0.6-1 s (G-1 run 3).
        var renewP90 = await RenewRoundTripP90Async();
        var calmFrom = w.Engine.Clock.Now;
        for (var i = 0; i < 16; i++)
        {
            var hold = TimeSpan.FromSeconds(40 + i);
            w.Script.On("calm" + i, (_, _, _) => Task.Delay(hold, CancellationToken.None));
            await w.DispatchAsync("calm" + i);
        }

        await WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 36, "all 36 succeeded");
        await WorkerHost.WaitUntil(() => !w.Engine.PressureStopped, Wait, "admission resumed after the calm phase");
        var calm = w.Events.Where(e => e.At > calmFrom).ToList();
        Assert.True(calm.Count(e => e.Name == "renewed") >= 16, "calm renewals");
        Assert.DoesNotContain(calm, e => e.Name.StartsWith("lost:", StringComparison.Ordinal));
        var calmStops = calm.Where(e => e.Name.StartsWith("pressureStop", StringComparison.Ordinal)).ToList();

        // 16 renewals due together through 4 renewal lanes: the last waits three Renew round trips. Where that stays well
        // inside the 1 s controlWait stop (the reference host), no stop is allowed. Where the store's round trip alone
        // carries the wave past it (07 C-9), every stop must be a lane-queueing stop that gates and resumes correctly.
        var strict = 3 * renewP90 < TimeSpan.FromMilliseconds(500);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"CAP-10 no-latency phase: Renew p90 {renewP90.TotalMilliseconds:F0} ms, {(strict ? "strict" : "store-bound")} branch, {calmStops.Count} stops");
        if (strict)
        {
            Assert.Empty(calmStops);
        }
        else
        {
            // Lane queueing (controlWait) or runtime timer lag from the suite's own parallel load (lag): both are
            // environment stops, and neither may admit work while stopped or skip the resume checks.
            Assert.All(calmStops, e => Assert.Contains(e.Name, new[] { "pressureStop:controlWait", "pressureStop:lag" }));
            CheckStops(w, calm);
        }

        await w.StopAsync();
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    /// <summary>Each stop resumes after three healthy checks and is overtaken by at most the claim in flight; no lost attempt renews.</summary>
    static void CheckStops(WorkerHost w, List<(string Name, Guid? AttemptId, TimeSpan At)> events)
    {
        foreach (var stop in events.Where(e => e.Name.StartsWith("pressureStop", StringComparison.Ordinal)))
        {
            var resume = events.FirstOrDefault(e => e.Name == "pressureResume" && e.At > stop.At);
            Assert.True(resume.Name is not null, $"no resume after {stop.Name} at {stop.At}");
            Assert.True(resume.At - stop.At >= TimeSpan.FromSeconds(3), $"resumed {(resume.At - stop.At).TotalSeconds:F1} s after {stop.Name}");
            // The pool checks the gate before every Reserve and claims serially, so a stop can only be overtaken by the
            // one claim already in flight; no Reserve is sent while stopped.
            var late = events.Where(e => e.Name == "reserved" && e.At > stop.At && e.At < resume.At).ToList();
            Assert.True(late.Count <= 1, $"{late.Count} reservations while stopped at {stop.At}");
            Assert.All(late, e => Assert.True(e.At - stop.At <= w.Engine.Options.ControlRequestTimeout + w.Engine.Options.SchedulerAllowance,
                $"reservation {(e.At - stop.At).TotalSeconds:F1} s into the stop"));
        }

        // A late Renew reply never revives an attempt whose authority was latched lost.
        foreach (var lost in events.Where(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain(events, e => e.AttemptId == lost.AttemptId && e.At > lost.At && e.Name == "renewed");
        }
    }

    /// <summary>p90 of 20 sequential Renew round trips on a separate namespace of the same server, without injected latency.</summary>
    static async Task<TimeSpan> RenewRoundTripP90Async()
    {
        await using var probe = await DriverFixture.StartAsync(Ns("cap10rtt"), StoreProfile.DirectPrimaryViaProxy);
        var c = await probe.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("rtt", "r"));
        var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(60))).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        var samples = new List<TimeSpan>();
        for (var i = 0; i < 20; i++)
        {
            var started = Stopwatch.GetTimestamp();
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease, TimeSpan.FromSeconds(60))).Status);
            samples.Add(Stopwatch.GetElapsedTime(started));
        }

        samples.Sort();
        return samples[17];
    }

    // ------------------------------------------------------------------ CAP-11 (subset)

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-11")]
    public async Task Io_slot_is_held_until_the_provider_task_ends()
    {
        // (a) 10 s per message on the control connections: the Renew outlives its local timeout (1.25 s at test timing).
        await using var f = await DriverFixture.StartAsync(Ns("cap11a"), StoreProfile.DirectPrimaryViaProxy);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 1);
        w.Script.On("long", (_, _, ct) => Task.Delay(TimeSpan.FromSeconds(60), ct));
        await w.DispatchAsync("long");
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("long") == 1, Wait, "handler running");

        f.Proxy!.SetLatency(TimeSpan.FromSeconds(10), LatencyDirection.Both, PgFaultProxy.Role("control"));
        await WorkerHost.WaitUntil(() => w.Engine.RenewalIo.Held == 1, Wait, "a Renew in flight");
        var held = System.Diagnostics.Stopwatch.StartNew();
        var maxHeld = 0;
        await using (new Sampler(() => maxHeld = Math.Max(maxHeld, w.Engine.RenewalIo.Held)))
        {
            // Well past ControlRequestTimeout + SchedulerAllowance the slot is still held: a timed-out wait never frees it.
            await Task.Delay(w.Engine.Options.ControlRequestTimeout + w.Engine.Options.SchedulerAllowance + TimeSpan.FromSeconds(3));
            Assert.Equal(1, w.Engine.RenewalIo.Held);
            f.Proxy.SetLatency(TimeSpan.Zero);
            await WorkerHost.WaitUntil(() => w.Engine.RenewalIo.Held == 0, Wait, "the provider task ended");
        }

        Assert.Equal(1, maxHeld);
        Assert.True(held.Elapsed > w.Engine.Options.ControlRequestTimeout, $"slot freed after {held.Elapsed.TotalSeconds:F1} s");
        await w.StopAsync();
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "CAP-11")]
    public async Task Waiting_dispatch_beyond_the_bound_is_known_not_submitted()
    {
        // (b) Two calls in flight (MaxConcurrentCalls 2) held at a barrier, 64 waiting (MaxWaitingOperations 64):
        // one more waiting dispatch returns local_capacity_exceeded at once and never reaches the store.
        await using var f = await DriverFixture.StartAsync(Ns("cap11b"));
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(120));
        });
        var admission = c.Services.GetRequiredService<LocalAdmission>();
        var hold = f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch), times: 2);
        var calls = Enumerable.Range(0, 66).Select(i => c.Dispatcher.DispatchAsync(new PingJob("p" + i))).ToList();
        await WorkerHost.WaitUntil(() => hold.Fired == 2 && admission.Waiting == 64, Wait, "2 in flight and 64 waiting");

        var overflow = c.Prepared.Prepare(new PingJob("overflow"));
        var ex = await Assert.ThrowsAsync<LocalCapacityExceededException>(() => c.Prepared.DispatchAsync(overflow));
        Assert.Equal(RunwickErrorCodes.LocalCapacityExceeded, ex.Code);
        Assert.Equal(ErrorDisposition.KnownNotApplied, ex.Disposition);
        Assert.Same(overflow, ex.Prepared);

        DriverFixture.ReleaseBarrier(hold);
        await Task.WhenAll(calls);
        Assert.Equal(66, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @id", ("id", overflow.OperationId.Value)));
        Assert.Equal(0, admission.Waiting);
    }
}

/// <summary>CAP-11 (c): restarts the shared server, so it runs alone (07 §2.3, collection StoreRestart).</summary>
[Collection("StoreRestart")]
public sealed class CapacityOutageTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(180);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-11")]
    [Trait("RequiresDocker", "true")]
    public async Task Outage_under_load_keeps_every_queue_within_its_bound()
    {
        // (c) RestartStore(Kill, 60 s) under steady producer load and a full worker: tickets, the completion channel,
        // pending outcomes and waiting producer operations stay within 16 / 16 / 16 / 64 throughout.
        await using var f = await DriverFixture.StartAsync(Ns("cap11c"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 16);
        w.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken.None);
        var admission = w.Services.GetRequiredService<LocalAdmission>();
        await w.StartAsync();

        var accepted = 0;
        var rejected = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
        using var producing = new CancellationTokenSource();
        var producer = Task.Run(async () =>
        {
            var pending = new List<Task>();
            for (var i = 0; !producing.IsCancellationRequested; i++)
            {
                var key = "load" + i;
                pending.Add(Task.Run(async () =>
                {
                    try
                    {
                        await w.DispatchAsync(key);
                        Interlocked.Increment(ref accepted);
                    }
                    catch (RunwickException ex)
                    {
                        rejected.AddOrUpdate(ex.Code, 1, (_, n) => n + 1);
                    }
                }));
                await Task.Delay(50);
            }

            await Task.WhenAll(pending);
        });

        int maxEntries = 0, maxCompletion = 0, maxFinishing = 0, maxWaiting = 0;
        await using (new CapacityFaultTests.Sampler(() =>
        {
            maxEntries = Math.Max(maxEntries, w.Engine.Entries.Count);
            maxCompletion = Math.Max(maxCompletion, w.Engine.Completion.Pending);
            maxFinishing = Math.Max(maxFinishing, w.Engine.Entries.Count(e => e.Phase == AttemptPhase.Finishing));
            maxWaiting = Math.Max(maxWaiting, admission.Waiting);
        }, periodMs: 20))
        {
            await WorkerHost.WaitUntil(() => Volatile.Read(ref accepted) >= 100, Wait, "steady load");
            await f.RestartStore(RestartMode.Kill, TimeSpan.FromSeconds(60));
            await Task.Delay(TimeSpan.FromSeconds(20));
            await producing.CancelAsync();
            await producer;
            await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state NOT IN ('succeeded', 'failed')", 0, "drained after the outage");
        }

        await w.StopAsync();
        Assert.InRange(maxEntries, 1, 16);
        Assert.InRange(maxCompletion, 0, 16);
        Assert.InRange(maxFinishing, 0, 16);
        Assert.InRange(maxWaiting, 0, 64);
        Assert.True(rejected.ContainsKey(RunwickErrorCodes.StoreUnavailable) || rejected.ContainsKey(RunwickErrorCodes.LocalCapacityExceeded),
            "the outage produced no producer rejections: " + string.Join(", ", rejected));
        // An outcome-unknown dispatch may or may not have applied; nothing else is in the store.
        var jobs = await f.CountAsync("SELECT count(*) FROM runwick.jobs");
        Assert.InRange(jobs, accepted, accepted + rejected.GetValueOrDefault(RunwickErrorCodes.OutcomeUnknown));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
