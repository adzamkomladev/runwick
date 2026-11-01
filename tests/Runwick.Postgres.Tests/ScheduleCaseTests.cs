using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Dispatching;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>What scheduled handlers observed: the job's name and its <c>IJobContext.Schedule</c>.</summary>
internal sealed class ScheduleProbe
{
    int _running;
    int _maxRunning;

    public ConcurrentQueue<(string Name, JobScheduleInfo? Schedule)> Seen { get; } = new();

    /// <summary>How long each handler holds its job; zero by default.</summary>
    public TimeSpan Hold { get; init; }

    public int MaxRunning => Volatile.Read(ref _maxRunning);

    public async Task RunAsync(string name, JobScheduleInfo? schedule, CancellationToken ct)
    {
        var running = Interlocked.Increment(ref _running);
        int seen;
        while (running > (seen = Volatile.Read(ref _maxRunning)) && Interlocked.CompareExchange(ref _maxRunning, running, seen) != seen)
        {
        }

        try
        {
            if (Hold > TimeSpan.Zero)
            {
                await Task.Delay(Hold, ct);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _running);
        }

        Seen.Enqueue((name, schedule));
    }
}

internal sealed class TickProbeHandler(ScheduleProbe probe, IJobContext context) : IJobHandler<TickJob>
{
    public Task HandleAsync(TickJob job, CancellationToken cancellationToken) => probe.RunAsync(job.Name, context.Schedule, cancellationToken);
}

/// <summary>Timestamps run normally until <see cref="Slow"/> is set; then every read is 10 s after the previous one.</summary>
internal sealed class SwitchableTime : TimeProvider
{
    long _slowAt;

    public volatile bool Slow;

    public override long GetTimestamp()
    {
        if (!Slow)
        {
            return System.GetTimestamp();
        }

        var step = TimestampFrequency * 10;
        var now = System.GetTimestamp();
        long current, next;
        do
        {
            current = Interlocked.Read(ref _slowAt);
            next = Math.Max(now, current) + step;
        }
        while (Interlocked.CompareExchange(ref _slowAt, next, current) != current);
        return next;
    }
}

/// <summary>
/// SP-016 owed cases (15 §9, 13 §11): the hosted scheduler path (seed and loop) and <c>IJobContext.Schedule</c>, SCHED-11,
/// the scheduler half of SCHED-14, PGX-04 materialization against scope creation and edits in both orders, and CMD-04 and
/// CMD-05 on the schedule operations.
/// </summary>
public sealed class ScheduleCaseTests
{
    static readonly ControlActor Actor = new("user:ops", null, "host", "schedules");

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
    }

    static async Task ActivateAsync(TestClient c)
    {
        var reply = await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, reply.Status);
    }

    static async Task<TestClient> OpenAsync(DriverFixture f)
    {
        var c = await f.OpenClient(configure: Register);
        await ActivateAsync(c);
        return c;
    }

    static ScheduleDefinitionRecord Definition(TestClient c, string key, IQueueJob? job = null, Action<ScheduleDeclaration>? configure = null)
    {
        var declaration = new ScheduleDeclaration(key, job ?? new TickJob(key)) { Cron = "* * * * *" };
        configure?.Invoke(declaration);
        return ScheduleCompiler.Compile(declaration, c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
    }

    static async Task<StoreReply<ScheduleRegistration>> RegisterAsync(TestClient c, string key, IQueueJob? job = null,
        Action<ScheduleDeclaration>? configure = null)
    {
        var definition = Definition(c, key, job, configure);
        var cutoff = await c.Store.StoreNowAsync(CancellationToken.None);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance)(cutoff)?.At;
        return await c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), CancellationToken.None);
    }

    /// <summary>Moves the cursor back so exactly <paramref name="minutes"/> whole-minute occurrences are missed (as ScheduleStoreTests).</summary>
    static Task RewindAsync(DriverFixture f, string key, int minutes) => f.ExecAsync("""
        UPDATE runwick.schedules
        SET evaluated_through = date_trunc('minute', clock_timestamp()) - make_interval(mins => @k),
            next_due_at = date_trunc('minute', clock_timestamp()) - make_interval(mins => @k - 1),
            scan_after_at = clock_timestamp() - interval '1 second'
        WHERE schedule_key = @key
        """, ("k", minutes), ("key", key));

    static async Task<ScheduleSnapshot> ReadAsync(TestClient c, string key) =>
        await c.Store.ReadScheduleAsync(c.Application, key, CancellationToken.None) ?? throw new InvalidOperationException("no schedule");

    static async Task<MaterializeRequest> PlanAsync(TestClient c, string key)
    {
        var s = await ReadAsync(c, key);
        var t = await c.Store.StoreNowAsync(CancellationToken.None);
        return SchedulePlanner.Plan(s, t, ScheduleRecurrence.For(s.Definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance), Guid.CreateVersion7)
            ?? throw new InvalidOperationException("nothing to plan");
    }

    static long EpochMs(string text) => long.Parse(text, CultureInfo.InvariantCulture);

    /// <summary>A scheduler-only host (no worker): the role runs independently of workers (13 §2).</summary>
    sealed class SchedulerHost : IAsyncDisposable
    {
        readonly ServiceProvider _provider;
        readonly IHostedService _hosted;
        bool _started;

        public SchedulerHost(DriverFixture f, string identity, Action<RunwickBuilder> schedules, TimeProvider? time = null)
        {
            var sc = new ServiceCollection();
            sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
            sc.AddSingleton<IFaultInjector>(f.Faults);
            if (time is not null)
            {
                sc.AddSingleton(time);
            }

            sc.AddRunwick(r =>
            {
                r.Application(f.Application).UsePostgres(f.ClientConnectionString(identity));
                Register(r);
                schedules(r);
                r.AddScheduler(o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(200);
                    o.IdlePollInterval = TimeSpan.FromSeconds(1);
                });
            });
            _provider = sc.BuildServiceProvider();
            _hosted = _provider.GetServices<IHostedService>().Single();
        }

        public SchedulerService Scheduler => _provider.GetRequiredService<SchedulerService>();

        public async Task StartAsync()
        {
            await _hosted.StartAsync(CancellationToken.None);
            _started = true;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_started)
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    await _hosted.StopAsync(cts.Token);
                }
            }
            finally
            {
                await _provider.DisposeAsync();
            }
        }
    }

    // ------------------------------------------------------------------ hosted path, IJobContext.Schedule

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-09")]
    [Trait("Case", "SCHED-13")]
    [Trait("Case", "SCHED-15")]
    public async Task The_hosted_scheduler_seeds_once_materializes_and_jobs_see_their_occurrence()
    {
        await using var f = await DriverFixture.StartAsync(Ns("schhost"));
        var c = await OpenAsync(f);
        var probe = new ScheduleProbe();
        WorkerHost Host(string identity, string cron) => WorkerHost.Create(f.ClientConnectionString(identity), f.Application, f.Faults,
            services: sc => sc.AddSingleton(probe),
            configure: r =>
            {
                r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
                r.AddHandler<TickJob, TickProbeHandler>();
                r.AddSchedule("tick", new TickJob("tick")).Cron(cron);
                r.AddScheduler(o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(200);
                    o.IdlePollInterval = TimeSpan.FromSeconds(1);
                });
            });

        // Seed: the first start registers revision 1 with no backfill.
        await using (var w = Host("sched1", "* * * * *"))
        {
            var unscheduled = new ConcurrentQueue<JobScheduleInfo?>();
            w.Script.Default = (_, ctx, _) =>
            {
                unscheduled.Enqueue(ctx.Schedule);
                return Task.CompletedTask;
            };
            await w.StartAsync();
            var seeded = await ReadAsync(c, "tick");
            Assert.Equal((1L, false), (seeded.DefinitionRevision, seeded.Paused));
            Assert.True(seeded.NextDueAt > seeded.EvaluatedThrough);
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_registered'"));

            // Loop: ten missed minutes (run-once default) become one job covering through the cutoff, and its handler sees it.
            await RewindAsync(f, "tick", 10);
            await WorkerHost.WaitUntil(() => !probe.Seen.IsEmpty, Wait, "the loop materializes and the job runs");
            Assert.True(Interlocked.Read(ref w.Services.GetRequiredService<SchedulerService>().Materializations) >= 1);
            var (name, info) = Assert.Single(probe.Seen);
            Assert.Equal("tick", name);
            var row = (await TextAsync(f, """
                SELECT (extract(epoch FROM scheduled_at) * 1000)::bigint || '/' || (extract(epoch FROM covered_from) * 1000)::bigint
                       || '/' || (extract(epoch FROM covered_through) * 1000)::bigint
                FROM runwick.schedule_occurrences WHERE occurrence_kind = 'scheduled'
                """)).Single().Split('/');
            Assert.NotNull(info);
            Assert.Equal("tick", info.ScheduleName);
            Assert.Equal((EpochMs(row[0]), EpochMs(row[1]), EpochMs(row[2])),
                (info.ScheduledAt.ToUnixTimeMilliseconds(), info.CoveredFrom.ToUnixTimeMilliseconds(), info.CoveredUntil.ToUnixTimeMilliseconds()));
            Assert.True(info.CoveredUntil - info.ScheduledAt >= TimeSpan.FromMinutes(9), "run-once covers the missed range");

            // A manual trigger through the public preview control: its occurrence is the trigger's store time, covering itself.
            var control = w.Services.GetRequiredService<IScheduleControl>();
            var before = await f.StoreNow();
            var trigger = await control.TriggerAsync("tick", 1, Guid.CreateVersion7(), new ManagementActor("user:ops", null, "host"), "manual");
            var after = await f.StoreNow();
            Assert.True(trigger.Succeeded);
            await WorkerHost.WaitUntil(() => probe.Seen.Count == 2, Wait, "the manual job runs");
            var manual = probe.Seen.Last().Schedule!;
            Assert.Equal(("tick", manual.ScheduledAt, manual.ScheduledAt), (manual.ScheduleName, manual.CoveredFrom, manual.CoveredUntil));
            Assert.InRange(manual.ScheduledAt, before.AddMilliseconds(-1), after);

            // A job that no schedule produced sees null.
            await w.DispatchAsync("plain");
            await WorkerHost.WaitUntil(() => unscheduled.Count == 1, Wait, "the ordinary job runs");
            Assert.Null(Assert.Single(unscheduled));
        }

        // Pause it, then restart with the same declaration: the seed reports `same`, writes nothing and resets no control.
        var s = await ReadAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await c.Store.SetScheduleStateAsync(
            c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), CancellationToken.None)).Status);
        await using (var again = Host("sched2", "* * * * *"))
        {
            await again.StartAsync();
            Assert.True((await ReadAsync(c, "tick")).Paused);
        }

        // A drifted declaration starts (with a warning) and never overwrites the stored definition.
        await using (var drift = Host("sched3", "*/5 * * * *"))
        {
            await drift.StartAsync();
        }

        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.schedule_definitions"));
        Assert.Equal(["* * * * *"], await TextAsync(f, "SELECT cron_text FROM runwick.schedule_definitions"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_registered'"));
        Assert.True((await ReadAsync(c, "tick")).Paused);
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE created_reason = 'scheduled'"));
    }

    // ------------------------------------------------------------------ SCHED-11

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-11")]
    public async Task A_missing_profile_is_a_local_diagnostic_only()
    {
        // Two stored definitions this process cannot evaluate under its own rules (another evaluator profile; another TZDB
        // artifact hash), as a replica with different data would have written them. They are skipped locally, never
        // materialized under substitute rules, never marked for review; a valid schedule keeps running beside them.
        await using var f = await DriverFixture.StartAsync(Ns("sched11"));
        var c = await OpenAsync(f);
        await using var host = new SchedulerHost(f, "sched", r =>
        {
            foreach (var key in new[] { "good", "profile", "data" })
            {
                r.AddSchedule(key, new TickJob(key)).Cron("* * * * *");
            }
        });
        await host.StartAsync();
        await f.ExecAsync("""
            UPDATE runwick.schedule_definitions AS d SET evaluator_profile = 'runwick.cron/99'
            FROM runwick.schedules AS s WHERE s.schedule_id = d.schedule_id AND s.schedule_key = 'profile'
            """);
        await f.ExecAsync("""
            UPDATE runwick.schedule_definitions AS d SET tzdb_sha256 = decode(repeat('ab', 32), 'hex')
            FROM runwick.schedules AS s WHERE s.schedule_id = d.schedule_id AND s.schedule_key = 'data'
            """);
        var before = new Dictionary<string, ScheduleSnapshot>(StringComparer.Ordinal);
        foreach (var key in new[] { "good", "profile", "data" })
        {
            await RewindAsync(f, key, 1);
            before[key] = await ReadAsync(c, key);
        }

        var scheduler = host.Scheduler;
        await WorkerHost.WaitUntil(() => scheduler.LocalDiagnostics.Count == 2 && Interlocked.Read(ref scheduler.Materializations) >= 1, Wait,
            "two local diagnostics and the good schedule materialized");
        Assert.Equal("schedule_missing_profile", scheduler.LocalDiagnostics[before["profile"].ScheduleId]);
        Assert.Equal("schedule_missing_artifact", scheduler.LocalDiagnostics[before["data"].ScheduleId]);

        // Keep polling for a while: still nothing for the unevaluable two.
        await Task.Delay(TimeSpan.FromSeconds(3));
        foreach (var key in new[] { "profile", "data" })
        {
            var after = await ReadAsync(c, key);
            Assert.False(after.NeedsReview);
            Assert.Equal((before[key].CursorRevision, before[key].EvaluatedThrough), (after.CursorRevision, after.EvaluatedThrough));
        }

        Assert.Equal(["good"], await TextAsync(f, """
            SELECT DISTINCT s.schedule_key FROM runwick.schedule_occurrences AS o JOIN runwick.schedules AS s USING (application, schedule_id)
            """));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_needs_review'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'RecordScheduleEvaluationFailure'"));

        // The row was never globally paused: once this process can evaluate it again, the same scheduler materializes it.
        await f.ExecAsync("""
            UPDATE runwick.schedule_definitions AS d SET evaluator_profile = (
                SELECT g.evaluator_profile FROM runwick.schedule_definitions AS g JOIN runwick.schedules AS gs USING (application, schedule_id)
                WHERE gs.schedule_key = 'good')
            FROM runwick.schedules AS s WHERE s.schedule_id = d.schedule_id AND s.schedule_key = 'profile'
            """);
        await CapacityFaultTests.WaitCount(f, """
            SELECT count(*) FROM runwick.schedule_occurrences AS o JOIN runwick.schedules AS s USING (application, schedule_id)
            WHERE s.schedule_key = 'profile'
            """, 1, "the restored profile materializes");
    }

    // ------------------------------------------------------------------ SCHED-14 (scheduler half)

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-14")]
    public async Task An_elapsed_evaluation_budget_backs_off_locally_and_writes_nothing()
    {
        // scheduler-evaluator §7 SCHED-14, SP-016 half: slow evaluator work is LocalPressure (12 §10). The scheduler skips
        // the schedule with a local diagnostic only: no evaluation-failure command, no review, no jobs, no cursor change.
        // The evaluator slot (one task per group) and its no-replacement rule are SP-015's EvaluatorBoundsTests.
        await using var f = await DriverFixture.StartAsync(Ns("sched14"));
        var c = await OpenAsync(f);
        var time = new SwitchableTime();
        await using var host = new SchedulerHost(f, "sched", r => r.AddSchedule("tick", new TickJob("tick")).Cron("* * * * *"), time);
        await host.StartAsync();
        // Slow first: otherwise the running loop can materialize the rewound minute before the budget starts failing.
        time.Slow = true;
        await RewindAsync(f, "tick", 1);
        var before = await ReadAsync(c, "tick");

        var scheduler = host.Scheduler;
        await WorkerHost.WaitUntil(() => scheduler.LocalDiagnostics.ContainsKey(before.ScheduleId), Wait, "a local budget diagnostic");
        Assert.Equal("schedule_budget_elapsed", scheduler.LocalDiagnostics[before.ScheduleId]);
        await Task.Delay(TimeSpan.FromSeconds(3));

        var after = await ReadAsync(c, "tick");
        Assert.False(after.NeedsReview);
        Assert.Equal((before.CursorRevision, before.EvaluatedThrough), (after.CursorRevision, after.EvaluatedThrough));
        Assert.Equal(0, Interlocked.Read(ref scheduler.Materializations));
        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await f.CountAsync("""
            SELECT count(*) FROM runwick.storage_commands WHERE operation IN ('MaterializeOccurrences', 'RecordScheduleEvaluationFailure')
            """));
    }

    // ------------------------------------------------------------------ PGX-04 (materialization sub-part)

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "PGX-04")]
    public async Task Materialization_and_scope_creation_in_opposite_orders_never_deadlock()
    {
        // Each round registers a serialized schedule whose template needs two fresh resource scopes (r1, r2), and races its
        // first materialization with a dispatch that needs the same scopes declared in the opposite order (r2, r1). Both
        // create the scope rows. With the lock order of 13 §8 (scopes in UUID order after the schedule row) both apply and
        // no deadlock is ever detected.
        await using var f = await DriverFixture.StartAsync(Ns("pgx04m"));
        var a = await OpenAsync(f);
        var b = await f.OpenClient(configure: Register);
        const int rounds = 30;
        for (var i = 0; i < rounds; i++)
        {
            var key = $"pair{i}";
            Assert.Equal(StoreStatus.Applied, (await RegisterAsync(a, key, new PairJob(key, $"r1-{i}", $"r2-{i}"), d => d.Overlap = ScheduleOverlap.Serialize)).Status);
            await RewindAsync(f, key, 1);
            var plan = await PlanAsync(a, key);
            var materialize = a.Store.MaterializeOccurrencesAsync(a.Command(plan), CancellationToken.None);
            var dispatch = b.Dispatcher.DispatchAsync(new PairJob($"d{i}", $"r2-{i}", $"r1-{i}"));
            Assert.Equal(StoreStatus.Applied, (await WithTimeout(materialize)).Status);
            Assert.Equal(DispatchDisposition.Created, (await WithTimeout(dispatch)).Disposition);
        }

        Assert.Equal(0, Deadlocks(a) + Deadlocks(b));
        Assert.Equal(2 * rounds, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE scope_kind = 'resource'"));
        Assert.Equal(rounds, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE scope_kind = 'schedule'"));
        Assert.Equal(2 * rounds, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "PGX-04")]
    public async Task Materialization_versus_scope_creation_and_edits_in_both_orders()
    {
        // Barrier-ordered: (1) materialization holds every lock before COMMIT; an opposite-order dispatch on the same new
        // scopes and an edit of the schedule both wait, then apply. (2) The dispatch creates the scopes and holds them; the
        // materialization waits on them, then applies. No deadlock either way, one serial outcome each.
        await using var f = await DriverFixture.StartAsync(Ns("pgx04o"));
        var a = await OpenAsync(f);
        var b = await f.OpenClient(configure: Register);

        // (1) Materialization first.
        await RegisterAsync(a, "m1", new PairJob("m1", "x1", "x2"), d => d.Overlap = ScheduleOverlap.Serialize);
        await RewindAsync(f, "m1", 1);
        var plan = await PlanAsync(a, "m1");
        var id = Guid.CreateVersion7();
        var held = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(id));
        var materialize = a.Store.MaterializeOccurrencesAsync(a.Command(plan, id), CancellationToken.None);
        await DriverFixture.WaitAtBarrier(held, Wait);
        var dispatch = b.Dispatcher.DispatchAsync(new PairJob("d1", "x2", "x1"));
        var s = await ReadAsync(b, "m1");
        var edited = Definition(b, "m1", new PairJob("m1", "x1", "x2"), d => { d.Overlap = ScheduleOverlap.Serialize; d.Cron = "*/5 * * * *"; });
        var cutoff = await b.Store.StoreNowAsync(CancellationToken.None);
        var first = ScheduleRecurrence.For(edited, TzdbArtifact.Pinned, FrozenBudgetClock.Instance)(cutoff)!.Value.At;
        var edit = b.Store.UpdateScheduleAsync(b.Command(new UpdateScheduleRequest("m1", edited, s.DefinitionRevision, cutoff, first, Actor)),
            CancellationToken.None);
        await Task.Delay(500);
        Assert.False(dispatch.IsCompleted, "the dispatch did not wait for the scopes created by materialization");
        Assert.False(edit.IsCompleted, "the edit did not wait for the schedule row");
        DriverFixture.ReleaseBarrier(held);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(materialize)).Status);
        Assert.Equal(DispatchDisposition.Created, (await WithTimeout(dispatch)).Disposition);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(edit)).Status);
        var afterEdit = await ReadAsync(a, "m1");
        // s was read while the materialization was still uncommitted: it and the edit each advance the cursor once.
        Assert.Equal((2L, s.CursorRevision + 2), (afterEdit.DefinitionRevision, afterEdit.CursorRevision));

        // (2) Dispatch first: it creates and holds the new scopes; the materialization needing them waits.
        await RegisterAsync(a, "m2", new PairJob("m2", "y1", "y2"));
        await RewindAsync(f, "m2", 1);
        var plan2 = await PlanAsync(a, "m2");
        var dispatchHeld = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
        var dispatch2 = b.Dispatcher.DispatchAsync(new PairJob("d2", "y2", "y1"));
        await DriverFixture.WaitAtBarrier(dispatchHeld, Wait);
        var materialize2 = a.Store.MaterializeOccurrencesAsync(a.Command(plan2), CancellationToken.None);
        await Task.Delay(500);
        Assert.False(materialize2.IsCompleted, "materialization did not wait for the scopes created by the dispatch");
        DriverFixture.ReleaseBarrier(dispatchHeld);
        Assert.Equal(DispatchDisposition.Created, (await WithTimeout(dispatch2)).Disposition);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(materialize2)).Status);

        Assert.Equal(0, Deadlocks(a) + Deadlocks(b));
        Assert.Equal(4, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE scope_kind = 'resource'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE scope_kind = 'schedule'"));
    }

    // ------------------------------------------------------------------ CMD-04, CMD-05

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "CMD-04")]
    public async Task A_schedule_command_whose_deadline_passes_during_a_lock_wait_expires_without_effect()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04s"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var before = await ReadAsync(c, "tick");
        var plan = await PlanAsync(c, "tick");

        // (a) An admin transaction holds the schedule row; MaterializeOccurrences waits on it past its NotAfter.
        await using var holder = new NpgsqlConnection(f.AdminConnectionString);
        await holder.OpenAsync();
        var tx = await holder.BeginTransactionAsync();
        await using (var l = new NpgsqlCommand("SELECT 1 FROM runwick.schedules WHERE schedule_key = 'tick' FOR UPDATE", holder, tx))
        {
            await l.ExecuteNonQueryAsync();
        }

        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await f.StoreNow() + TimeSpan.FromSeconds(2)).ToUnixTimeMilliseconds());
        var command = c.Command(plan, notAfter: notAfter);
        var pending = c.Store.MaterializeOccurrencesAsync(command, CancellationToken.None);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        await tx.RollbackAsync();
        var expired = await WithTimeout(pending);
        Assert.Equal((StoreStatus.CommandExpired, StoreEffect.ObservedNoEffect), (expired.Status, expired.Effect));
        Assert.True(expired.DecidedAt >= notAfter);
        Assert.Equal(0, await Receipts(f, command.CommandId));
        Assert.Equal(0, await Jobs(f));
        var after = await ReadAsync(c, "tick");
        Assert.Equal((before.CursorRevision, before.EvaluatedThrough), (after.CursorRevision, after.EvaluatedThrough));

        // (c) An applied command replayed after its NotAfter returns the recorded receipt.
        var shortLived = DateTimeOffset.FromUnixTimeMilliseconds((await f.StoreNow() + TimeSpan.FromSeconds(6)).ToUnixTimeMilliseconds());
        var applied = c.Command(await PlanAsync(c, "tick"), notAfter: shortLived);
        var first = await c.Store.MaterializeOccurrencesAsync(applied, CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, first.Status);
        while (await f.StoreNow() <= shortLived)
        {
            await Task.Delay(100);
        }

        var replay = await c.Store.MaterializeOccurrencesAsync(applied, CancellationToken.None);
        Assert.True(replay.Replayed);
        Assert.Equal((StoreStatus.Applied, first.DecidedAt), (replay.Status, replay.DecidedAt));
        Assert.Equal(first.Value!.Occurrences.Select(o => o.JobId), replay.Value!.Occurrences.Select(o => o.JobId));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "CMD-05")]
    public async Task Pruned_schedule_receipts_replay_as_expired_and_never_reapply()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05s"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var s = await ReadAsync(c, "tick");

        DateTimeOffset NotAfter() => DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(3)).ToUnixTimeMilliseconds());
        var materialize = c.Command(await PlanAsync(c, "tick"), notAfter: NotAfter());
        Assert.Equal(StoreStatus.Applied, (await c.Store.MaterializeOccurrencesAsync(materialize, CancellationToken.None)).Status);
        var pause = c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor), notAfter: NotAfter());
        Assert.Equal(StoreStatus.Applied, (await c.Store.SetScheduleStateAsync(pause, CancellationToken.None)).Status);
        var trigger = c.Command(new TriggerScheduleRequest("tick", 1, Guid.CreateVersion7(), Guid.CreateVersion7(), Actor), notAfter: NotAfter());
        Assert.Equal(StoreStatus.Applied, (await c.Store.TriggerScheduleAsync(trigger, CancellationToken.None)).Status);
        var latest = new[] { materialize.NotAfter, pause.NotAfter, trigger.NotAfter }.Max();
        while (await f.StoreNow() <= latest)
        {
            await Task.Delay(100);
        }

        // Harness pruning stands in for receipt retention (07 I-3; cleanup is SP-021).
        Assert.True(await f.PruneReceipts("durable", DateTimeOffset.UtcNow + TimeSpan.FromDays(3650)) >= 3);
        var jobs = await Jobs(f);
        var mid = await ReadAsync(c, "tick");

        Assert.Equal(StoreStatus.CommandExpired, (await c.Store.MaterializeOccurrencesAsync(materialize, CancellationToken.None)).Status);
        Assert.Equal(StoreStatus.CommandExpired, (await c.Store.SetScheduleStateAsync(pause, CancellationToken.None)).Status);
        Assert.Equal(StoreStatus.CommandExpired, (await c.Store.TriggerScheduleAsync(trigger, CancellationToken.None)).Status);

        var after = await ReadAsync(c, "tick");
        Assert.Equal(jobs, await Jobs(f));
        Assert.Equal((mid.ControlRevision, mid.CursorRevision, mid.EvaluatedThrough), (after.ControlRevision, after.CursorRevision, after.EvaluatedThrough));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetScheduleState'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_kind = 'manual'"));
    }

    // ------------------------------------------------------------------ scheduling-internals §10 rows

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-13")]
    public async Task A_plan_cut_off_after_store_time_is_rejected_and_the_cursor_waits()
    {
        // §10 "clock moves backward": a plan whose cutoff is later than the store's fresh time (the scheduler's clock ahead,
        // or the store's moved back) is rejected with `cutoff_in_future`; nothing is created and the cursor never moves.
        await using var f = await DriverFixture.StartAsync(Ns("schclk"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 2);
        var before = await ReadAsync(c, "tick");
        var ahead = await c.Store.StoreNowAsync(CancellationToken.None) + TimeSpan.FromMinutes(5);
        var plan = SchedulePlanner.Plan(before, ahead, ScheduleRecurrence.For(before.Definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance),
            Guid.CreateVersion7)!;

        var reply = await c.Store.MaterializeOccurrencesAsync(c.Command(plan), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidState, "cutoff_in_future"), (reply.Status, reply.Error!.Code));
        Assert.Equal(0, await Jobs(f));
        var after = await ReadAsync(c, "tick");
        Assert.Equal((before.CursorRevision, before.EvaluatedThrough, before.NextDueAt), (after.CursorRevision, after.EvaluatedThrough, after.NextDueAt));

        // Planned against authoritative time, the same backlog applies.
        Assert.Equal(StoreStatus.Applied, (await c.Store.MaterializeOccurrencesAsync(c.Command(await PlanAsync(c, "tick")), CancellationToken.None)).Status);
        Assert.True((await ReadAsync(c, "tick")).EvaluatedThrough > before.EvaluatedThrough);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-03")]
    public async Task A_paused_queue_keeps_materializing_while_execution_waits()
    {
        // §10 "queue paused but scheduler running": materialization continues, Reserve refuses, and the same plan sent
        // again as a new command creates nothing more.
        await using var f = await DriverFixture.StartAsync(Ns("schqp"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        var revision = await f.CountAsync("SELECT revision FROM runwick.queue_controls WHERE queue = 'default'");
        Assert.Equal(StoreStatus.Applied,
            (await c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision, Actor)), CancellationToken.None)).Status);
        await RewindAsync(f, "tick", 1);
        var plan = await PlanAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await c.Store.MaterializeOccurrencesAsync(c.Command(plan), CancellationToken.None)).Status);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(StoreStatus.QueuePaused, (await c.ReserveAsync()).Status);
        Assert.NotEqual(StoreStatus.Applied, (await c.Store.MaterializeOccurrencesAsync(c.Command(plan), CancellationToken.None)).Status);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(["pending"], await TextAsync(f, "SELECT state FROM runwick.jobs"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-15")]
    public async Task Serialized_catch_up_jobs_never_run_concurrently()
    {
        // §10 "slow serialized job": three caught-up occurrences of a serialized schedule on a worker with four slots run one
        // at a time (the `schedule` scope has capacity 1); all three complete.
        await using var f = await DriverFixture.StartAsync(Ns("schser"));
        var c = await OpenAsync(f);
        var probe = new ScheduleProbe { Hold = TimeSpan.FromMilliseconds(700) };
        await using var w = WorkerHost.Create(f.ClientConnectionString("sched"), f.Application, f.Faults, concurrency: 4,
            services: sc => sc.AddSingleton(probe),
            configure: r =>
            {
                r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
                r.AddHandler<TickJob, TickProbeHandler>();
                r.AddSchedule("tick", new TickJob("tick")).Cron("* * * * *").OnMisfire(MisfirePolicy.CatchUp).Overlap(ScheduleOverlap.Serialize);
                r.AddScheduler(o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(200);
                    o.IdlePollInterval = TimeSpan.FromSeconds(1);
                });
            });
        await w.StartAsync();
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.schedules"));
        await RewindAsync(f, "tick", 3);
        await WorkerHost.WaitUntil(() => probe.Seen.Count >= 3, Wait, "three serialized jobs complete");
        Assert.Equal(1, probe.MaxRunning);
        Assert.All(probe.Seen, x => Assert.Equal("tick", x.Schedule!.ScheduleName));
        Assert.Equal(0, Deadlocks(c));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-01")]
    public async Task Cross_occurrence_uniqueness_needs_explicit_reuse_and_then_links()
    {
        // §10 "cross-occurrence uniqueness requested": an active key without AllowOccurrenceReuse fails validation; with it,
        // later occurrences link to the active job instead of creating new ones.
        await using var f = await DriverFixture.StartAsync(Ns("schuni"));
        var c = await OpenAsync(f);
        var refused = Assert.Throws<RunwickConfigurationException>(() => Definition(c, "keyed", new KeyedJob("k", "one")));
        Assert.Equal("schedule_occurrence_reuse_required", Assert.Single(refused.Errors).Code);

        Assert.Equal(StoreStatus.Applied, (await RegisterAsync(c, "keyed", new KeyedJob("k", "one"), d =>
        {
            d.AllowReuse = true;
            d.Misfire = MisfirePolicy.CatchUp;
        })).Status);
        await RewindAsync(f, "keyed", 3);
        var reply = await c.Store.MaterializeOccurrencesAsync(c.Command(await PlanAsync(c, "keyed")), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var dispositions = reply.Value!.Occurrences.Select(o => o.Disposition).ToList();
        Assert.True(dispositions.Count >= 2, "a catch-up of several occurrences");
        Assert.Equal(["created", .. Enumerable.Repeat("linked_existing", dispositions.Count - 1)], dispositions);
        Assert.Single(reply.Value.Occurrences.Select(o => o.JobId).Distinct());
        Assert.Equal(1, await Jobs(f));
    }

    /// <summary>
    /// The planner's 250 ms cooperative budget (12 §8) is wall-clock; these store cases are not about it (the SCHED-14
    /// case drives the budget through its own clock), so their plans run on a timestamp that never advances.
    /// </summary>
    sealed class FrozenBudgetClock : TimeProvider
    {
        public static readonly FrozenBudgetClock Instance = new();

        public override long GetTimestamp() => 0;
    }
}
