using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// Five deployments over one store (08 §9): MODE-01 … MODE-05. Each role is its own service provider with its own
/// connection identity; a job dispatched by one role is processed by another, and a control from the control role is
/// observed through the event cursor.
/// </summary>
public sealed class DeploymentModeTests
{
    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static readonly ManagementActor Operator = new("user:ops", "Ops", "host");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>A producer- or control-only role: its own provider and connection identity, no worker (dispose it).</summary>
    static Task<TestClient> Producer(DriverFixture f, string identity, string queue = "default") =>
        TestClient.CreateAsync(f.ClientConnectionString(identity), f.Application, f.Faults, r => WorkerHost.RegisterWork(r, queue));

    static IJobControl Jobs(IServiceProvider s) => s.GetRequiredService<IJobControl>();

    static IQueueControl Queues(IServiceProvider s) => s.GetRequiredService<IQueueControl>();

    static async Task<long> QueueRevision(DriverFixture f, string queue) =>
        long.Parse((await TextAsync(f, "SELECT revision::text FROM runwick.queue_controls WHERE application = @a AND queue = @q",
            ("a", f.Application), ("q", queue))).Single(), System.Globalization.CultureInfo.InvariantCulture);

    static async Task<JobTarget> Target(DriverFixture f, JobId job)
    {
        var row = (await TextAsync(f, "SELECT generation || '/' || revision FROM runwick.jobs WHERE job_id = @j", ("j", job.Value))).Single().Split('/');
        return new JobTarget(job, int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(row[1], System.Globalization.CultureInfo.InvariantCulture));
    }

    static async Task<string> State(DriverFixture f, JobId job) =>
        (await TextAsync(f, "SELECT state FROM runwick.jobs WHERE job_id = @j", ("j", job.Value))).Single();

    static Task WaitState(DriverFixture f, JobId job, string state) =>
        WorkerHost.WaitUntil(() => State(f, job).GetAwaiter().GetResult() == state, Long, $"job {job.Value} {state}");

    /// <summary>Reads the cursor from the start until an event of <paramref name="kind"/> (for <paramref name="job"/>, if given) is visible.</summary>
    static async Task<JobEvent> ObserveAsync(IServiceProvider reader, string application, string kind, JobId? job = null)
    {
        var store = reader.GetRequiredService<PgStore>();
        var deadline = DateTime.UtcNow + Long;
        EventCursor? cursor = null;
        while (DateTime.UtcNow < deadline)
        {
            var page = await store.EventsSinceAsync(application, cursor, 100, CancellationToken.None);
            var hit = page.Events.FirstOrDefault(e => e.Kind == kind && (job is null || e.JobId == job.Value.Value));
            if (hit is not null)
            {
                return hit;
            }

            cursor = page.Next;
            if (page.Events.Count == 0)
            {
                await Task.Delay(50);
            }
        }

        throw new TimeoutException($"event {kind} not observed through the cursor");
    }

    /// <summary>Committed-snapshot invariants and the ReferenceModel's acceptance of every committed job history (15 §12.2 rule 4).</summary>
    static async Task AssertInvariants(DriverFixture f)
    {
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
        Assert.Empty(await f.HistoryViolations());
    }

    /// <summary>Pauses <paramref name="queue"/> from <paramref name="control"/> and checks the pause is observed.</summary>
    static async Task<QueueControlResult> PauseAsync(DriverFixture f, IServiceProvider control, IServiceProvider reader, string queue = "default")
    {
        var paused = await Queues(control).PauseAsync(queue, await QueueRevision(f, queue), Operator, "mode test");
        Assert.True(paused.Succeeded, paused.Code);
        var seen = await ObserveAsync(reader, f.Application, "queue_paused");
        Assert.Equal(Operator.Subject, seen.ActorId);
        return paused;
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MODE-01")]
    public async Task One_process_produces_works_and_controls()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mode01"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("all"), f.Application, f.Faults);
        await w.StartAsync();

        var done = (await w.DispatchAsync("one")).JobId;
        await WaitState(f, done, "succeeded");

        var paused = await PauseAsync(f, w.Services, w.Services);
        var held = (await w.DispatchAsync("held")).JobId;
        var cancelled = await Jobs(w.Services).CancelAsync(await Target(f, held), Operator);
        Assert.True(cancelled.Succeeded, cancelled.Code);
        Assert.Equal("cancelled", await State(f, held));
        await ObserveAsync(w.Services, f.Application, "job_cancelled", held);

        Assert.True((await Queues(w.Services).ResumeAsync("default", paused.Revision!.Value, Operator)).Succeeded);
        var after = (await w.DispatchAsync("after")).JobId;
        await WaitState(f, after, "succeeded");
        Assert.Equal(0, w.Script.InvocationsOf("held"));
        await w.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MODE-02")]
    public async Task A_producer_only_process_feeds_separate_workers()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mode02"));
        await using var producer = await Producer(f, "producer");
        await using var w1 = WorkerHost.Create(f.ClientConnectionString("worker1"), f.Application, f.Faults, concurrency: 2);
        await using var w2 = WorkerHost.Create(f.ClientConnectionString("worker2"), f.Application, f.Faults, concurrency: 2);
        await w1.StartAsync();
        await w2.StartAsync();

        var jobs = new List<JobId>();
        for (var i = 0; i < 20; i++)
        {
            jobs.Add((await producer.Dispatcher.DispatchAsync(new WorkJob("p" + i))).JobId);
        }

        foreach (var job in jobs)
        {
            await WaitState(f, job, "succeeded");
        }

        Assert.Equal(20, w1.Script.Completed.Count + w2.Script.Completed.Count);
        Assert.Empty(w1.Script.Completed.Keys.Intersect(w2.Script.Completed.Keys));
        Assert.Null(producer.Services.GetService<WorkerEngine>());

        // The producer process also hosts the control services; both workers' store sees the pause through the cursor.
        await PauseAsync(f, producer.Services, w1.Services);
        await w1.StopAsync();
        await w2.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MODE-03")]
    public async Task Worker_only_processes_on_disjoint_queues_take_only_their_own_lane()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mode03"));
        await using var toA = await Producer(f, "producer-a");
        await using var toB = await Producer(f, "producer-b", queue: "lane_b");
        await using var a = WorkerHost.Create(f.ClientConnectionString("worker-a"), f.Application, f.Faults, concurrency: 2);
        await using var b = WorkerHost.Create(f.ClientConnectionString("worker-b"), f.Application, f.Faults, concurrency: 2, queue: "lane_b");
        await a.StartAsync();
        await b.StartAsync();

        var jobs = new List<JobId>();
        for (var i = 0; i < 5; i++)
        {
            jobs.Add((await toA.Dispatcher.DispatchAsync(new WorkJob("a" + i))).JobId);
            jobs.Add((await toB.Dispatcher.DispatchAsync(new WorkJob("b" + i))).JobId);
        }

        foreach (var job in jobs)
        {
            await WaitState(f, job, "succeeded");
        }

        Assert.All(a.Script.Completed.Keys, k => Assert.StartsWith("a", k, StringComparison.Ordinal));
        Assert.All(b.Script.Completed.Keys, k => Assert.StartsWith("b", k, StringComparison.Ordinal));
        Assert.Equal((5, 5), (a.Script.Completed.Count, b.Script.Completed.Count));

        // Pausing lane B stops B only; A keeps draining its lane.
        var paused = await PauseAsync(f, toB.Services, a.Services, "lane_b");
        var heldB = (await toB.Dispatcher.DispatchAsync(new WorkJob("b-held"))).JobId;
        var freeA = (await toA.Dispatcher.DispatchAsync(new WorkJob("a-free"))).JobId;
        await WaitState(f, freeA, "succeeded");
        Assert.Equal("pending", await State(f, heldB));
        Assert.True((await Queues(toB.Services).ResumeAsync("lane_b", paused.Revision!.Value, Operator)).Succeeded);
        await WaitState(f, heldB, "succeeded");

        await a.StopAsync();
        await b.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MODE-04")]
    public async Task A_control_only_process_steers_a_separate_worker()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mode04"));
        await using var control = await Producer(f, "control");
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults);
        await w.StartAsync();
        Assert.Null(control.Services.GetService<WorkerEngine>());

        var paused = await PauseAsync(f, control.Services, w.Services);
        var held = (await w.DispatchAsync("held")).JobId;
        var kept = (await w.DispatchAsync("kept")).JobId;
        var cancelled = await Jobs(control.Services).CancelAsync(await Target(f, held), Operator, "not needed");
        Assert.True(cancelled.Succeeded, cancelled.Code);
        Assert.Equal("cancelled", cancelled.Effect);
        var seen = await ObserveAsync(w.Services, f.Application, "job_cancelled", held);
        Assert.Equal(Operator.Subject, seen.ActorId);

        Assert.True((await Queues(control.Services).ResumeAsync("default", paused.Revision!.Value, Operator)).Succeeded);
        await WaitState(f, kept, "succeeded");

        // A job dispatched by the control process is processed by the worker.
        var fromControl = (await control.Dispatcher.DispatchAsync(new WorkJob("from-control"))).JobId;
        await WaitState(f, fromControl, "succeeded");
        Assert.Equal(0, w.Script.InvocationsOf("held"));

        // A running job: Cancel records the request (T22); the worker observes it on its next Renew and cancels the
        // handler token. The handler honours it here; nothing requires a handler to stop.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.On("long", async (_, _, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
        });
        var running = (await w.DispatchAsync("long")).JobId;
        await started.Task.WaitAsync(Long);
        var requested = await Jobs(control.Services).CancelAsync(await Target(f, running), Operator, "stop it");
        Assert.True(requested.Succeeded, requested.Code);
        Assert.Equal("cancel_requested", requested.Effect);
        await ObserveAsync(w.Services, f.Application, "job_cancel_requested", running);
        await WaitState(f, running, "cancelled");
        // The handler stopped on the token, so the outcome is UserCancelled (04 Finish mapping).
        Assert.Equal("user_cancelled",
            (await TextAsync(f, "SELECT terminal_reason FROM runwick.jobs WHERE job_id = @j", ("j", running.Value))).Single());
        Assert.Equal(4, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE application = @a", ("a", f.Application)));
        await w.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MODE-05")]
    public async Task Producer_staged_completion_worker_and_control_run_together()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mode05"));
        await f.ExecAsync("CREATE TABLE public.ledger (job_id uuid NOT NULL, attempt_id uuid NOT NULL)");
        await using var producer = await Producer(f, "producer");
        await using var control = await Producer(f, "control");
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 4);
        w.Script.Default = async (_, ctx, ct) =>
        {
            await using var conn = await OpenBusinessAsync(f);
            await using var tx = await conn.BeginTransactionAsync(ct);
            using (var cmd = new NpgsqlCommand("INSERT INTO public.ledger VALUES (@j, @a)", conn, tx))
            {
                cmd.Parameters.AddWithValue("j", ctx.JobId.Value);
                cmd.Parameters.AddWithValue("a", ctx.AttemptId.Value);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            var completion = ((IServiceProvider)ctx.Items[typeof(IServiceProvider)]!).GetRequiredService<IPostgresJobCompletion>();
            await completion.CompleteInTransactionAsync(tx, ct);
            await tx.CommitAsync(ct);
        };
        await w.StartAsync();

        // The control role pauses, the producer fills the queue, the control role cancels every third job and resumes,
        // while the producer keeps dispatching against the running worker.
        var paused = await PauseAsync(f, control.Services, producer.Services);
        var first = new List<JobId>();
        for (var i = 0; i < 12; i++)
        {
            first.Add((await producer.Dispatcher.DispatchAsync(new WorkJob("q" + i))).JobId);
        }

        var victims = first.Where((_, i) => i % 3 == 0).ToList();
        var results = await Jobs(control.Services).CancelManyAsync([.. await Task.WhenAll(victims.Select(v => Target(f, v)))], Operator);
        Assert.All(results, r => Assert.True(r.Succeeded, r.Code));

        var more = Task.Run(async () =>
        {
            var ids = new List<JobId>();
            for (var i = 0; i < 12; i++)
            {
                ids.Add((await producer.Dispatcher.DispatchAsync(new WorkJob("r" + i))).JobId);
            }

            return ids;
        });
        Assert.True((await Queues(control.Services).ResumeAsync("default", paused.Revision!.Value, Operator)).Succeeded);
        var all = first.Concat(await more).ToList();

        foreach (var job in all.Except(victims))
        {
            await WaitState(f, job, "succeeded");
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM public.ledger WHERE job_id = @j", ("j", job.Value)));
        }

        foreach (var victim in victims)
        {
            Assert.Equal("cancelled", await State(f, victim));
            await ObserveAsync(producer.Services, f.Application, "job_cancelled", victim);
        }

        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM public.ledger WHERE job_id = ANY(@v)", ("v", victims.Select(v => v.Value).ToArray())));
        // The caller's commit makes the job succeeded before the engine settles the attempt (as TXC-02). Each attempt is
        // settled by observing its staged receipt or, when the lookup stays unresolved under load, by the normal-path
        // Finish that storage rejects because the staged Finish already closed the attempt (08 §8.2); either way the
        // ledger and state checks above hold.
        await WorkerHost.WaitUntil(
            () => Interlocked.Read(ref w.Engine.StagedCompletionsObserved) + Interlocked.Read(ref w.Engine.FinishesSent) == all.Count - victims.Count,
            TimeSpan.FromSeconds(10), "every committed stage settled by the engine");
        Assert.True(Interlocked.Read(ref w.Engine.StagedCompletionsObserved) > 0);
        await w.StopAsync();
        await AssertInvariants(f);
    }
}
