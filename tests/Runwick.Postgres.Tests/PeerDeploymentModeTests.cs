using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.ReferenceModel;
using Runwick.Worker;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// The SP-012 rerun of MODE-01 … MODE-05 (15 §2 "Multi-replica peers (≥ 3 worker processes per store)", 15 §5): every
/// worker role runs in its own OS process (<c>Runwick.Harness.Peer</c>, 07 §2.4); producer- and control-only roles are
/// separate providers in the test process. Each case also checks the presence rows each worker process publishes
/// (09 §3): one live instance per worker process, its exact queue subscriptions, none for producer or control roles.
/// </summary>
[Trait("Item", "SP-012")]
public sealed class PeerDeploymentModeTests
{
    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static readonly ManagementActor Operator = new("user:ops", "Ops", "host");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static Task<TestClient> Role(DriverFixture f, string identity, string queue = "default") =>
        TestClient.CreateAsync(f.ClientConnectionString(identity), f.Application, f.Faults, r => WorkerHost.RegisterWork(r, queue));

    static IJobControl Jobs(IServiceProvider s) => s.GetRequiredService<IJobControl>();

    static IQueueControl Queues(IServiceProvider s) => s.GetRequiredService<IQueueControl>();

    /// <summary>A peer process hosting a worker (and, through its provider, every other role) on <paramref name="queue"/>.</summary>
    static async Task<PeerWorkerHandle> StartWorker(DriverFixture f, string identity, string queue = "default", int concurrency = 2,
        string? businessConnectionString = null)
    {
        var peer = await f.OpenPeer(identity);
        var args = new JsonObject
        {
            ["cs"] = f.ClientConnectionString(identity),
            ["app"] = f.Application,
            ["concurrency"] = concurrency,
            ["queue"] = queue,
        };
        if (businessConnectionString is not null)
        {
            args["bcs"] = businessConnectionString;
        }

        var started = await peer.SendAsync("startWorker", args, Long);
        return new PeerWorkerHandle(peer, Guid.Parse(started["workerInstanceId"]!.GetValue<string>()), queue);
    }

    sealed record PeerWorkerHandle(PeerClient Peer, Guid InstanceId, string Queue)
    {
        public async Task<Stats> StatsAsync()
        {
            var s = await Peer.SendAsync("workerStats");
            return new Stats(
                s["completed"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>(), StringComparer.Ordinal),
                s["invoked"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>(), StringComparer.Ordinal),
                s["finishesSent"]!.GetValue<long>(), s["stagedObserved"]!.GetValue<long>());
        }

        public async Task<JobId> DispatchAsync(string key) =>
            new(Guid.Parse((await Peer.SendAsync("workerDispatch", new JsonObject { ["jobKey"] = key }))["jobId"]!.GetValue<string>()));

        /// <summary>The worker process reads the event cursor from the start until it sees <paramref name="kind"/>; returns the actor.</summary>
        public async Task<string?> ObserveAsync(string kind, JobId? job = null)
        {
            // The peer reads for up to 3 minutes (a loaded shared host); the channel waits a little longer.
            var args = new JsonObject { ["kind"] = kind, ["timeoutSeconds"] = 180 };
            if (job is { } j)
            {
                args["jobId"] = j.Value.ToString("D");
            }

            return (await Peer.SendAsync("workerObserve", args, TimeSpan.FromSeconds(200)))["actorId"]?.GetValue<string>();
        }

        public Task StopAsync() => Peer.SendAsync("stopWorker", new JsonObject { ["wait"] = true }, Long);
    }

    sealed record Stats(Dictionary<string, int> Completed, Dictionary<string, int> Invoked, long FinishesSent, long StagedObserved);

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

    static async Task WaitState(DriverFixture f, JobId job, string state)
    {
        var deadline = DateTime.UtcNow + Long;
        while (await State(f, job) != state)
        {
            Assert.True(DateTime.UtcNow < deadline, $"job {job.Value} never reached {state}");
            await Task.Delay(50);
        }
    }

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    /// <summary>Presence (09 §3): exactly the given worker processes are live, each subscribed to exactly its own queue.</summary>
    static async Task AssertPresence(DriverFixture f, params PeerWorkerHandle[] workers)
    {
        // Live as 09 §5 defines it: unexpired, status starting, ready or degraded.
        var live = await f.CountAsync(
            "SELECT count(*) FROM runwick.worker_instances WHERE application = @a AND status IN ('starting', 'ready', 'degraded') AND expires_at > clock_timestamp()",
            ("a", f.Application));
        Assert.True(workers.Length == live, $"{live} live of {workers.Length}: " + string.Join("; ", await TextAsync(f,
            "SELECT instance_id || ' ' || status || ' ' || (expires_at - clock_timestamp())::text FROM runwick.worker_instances")));
        Assert.Equal(workers.Length, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions WHERE application = @a", ("a", f.Application)));
        foreach (var w in workers)
        {
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions WHERE instance_id = @i AND queue = @q",
                ("i", w.InstanceId), ("q", w.Queue)));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_capabilities WHERE instance_id = @i AND job_name = 'test.work'",
                ("i", w.InstanceId)));
        }
    }

    static async Task<QueueControlResult> PauseAsync(DriverFixture f, IServiceProvider control, PeerWorkerHandle observer, string queue = "default")
    {
        var paused = await Queues(control).PauseAsync(queue, await QueueRevision(f, queue), Operator, "mode test");
        Assert.True(paused.Succeeded, paused.Code);
        Assert.Equal(Operator.Subject, await observer.ObserveAsync("queue_paused"));
        return paused;
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-01")]
    public async Task One_peer_process_produces_works_and_controls()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pmode01"));
        var all = await StartWorker(f, "all");
        await AssertPresence(f, all);

        var done = await all.DispatchAsync("one");
        await WaitState(f, done, "succeeded");

        var paused = await all.Peer.SendAsync("workerPause", new JsonObject { ["revision"] = await QueueRevision(f, "default") });
        Assert.True(paused["succeeded"]!.GetValue<bool>(), paused["code"]?.GetValue<string>());
        Assert.Equal(Operator.Subject, await all.ObserveAsync("queue_paused"));
        var held = await all.DispatchAsync("held");
        var target = await Target(f, held);
        var cancelled = await all.Peer.SendAsync("workerCancel", new JsonObject
        {
            ["jobId"] = held.Value.ToString("D"), ["generation"] = target.ExpectedGeneration, ["revision"] = target.ExpectedRevision,
        });
        Assert.True(cancelled["succeeded"]!.GetValue<bool>(), cancelled["code"]?.GetValue<string>());
        Assert.Equal("cancelled", await State(f, held));
        Assert.Equal(Operator.Subject, await all.ObserveAsync("job_cancelled", held));

        var resumed = await all.Peer.SendAsync("workerResume", new JsonObject { ["revision"] = paused["revision"]!.GetValue<long>() });
        Assert.True(resumed["succeeded"]!.GetValue<bool>(), resumed["code"]?.GetValue<string>());
        var after = await all.DispatchAsync("after");
        await WaitState(f, after, "succeeded");
        var stats = await all.StatsAsync();
        Assert.False(stats.Invoked.ContainsKey("held"));
        Assert.Equal((1, 1), (stats.Invoked["one"], stats.Invoked["after"]));

        await all.StopAsync();
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_instances WHERE status IN ('draining', 'stopping')"));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-02")]
    public async Task A_producer_only_process_feeds_three_worker_processes()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pmode02"));
        await using var producer = await Role(f, "producer");
        PeerWorkerHandle[] workers = [await StartWorker(f, "worker1"), await StartWorker(f, "worker2"), await StartWorker(f, "worker3")];
        await AssertPresence(f, workers);
        Assert.Null(producer.Services.GetService<WorkerEngine>());

        var jobs = new List<JobId>();
        for (var i = 0; i < 30; i++)
        {
            jobs.Add((await producer.Dispatcher.DispatchAsync(new WorkJob("p" + i))).JobId);
        }

        foreach (var job in jobs)
        {
            await WaitState(f, job, "succeeded");
        }

        // Each job processed once, by exactly one of the worker processes. Shares between them are not asserted
        // (no global fairness across replicas, 09 §7).
        var stats = await Task.WhenAll(workers.Select(w => w.StatsAsync()));
        Assert.Equal(30, stats.Sum(s => s.Completed.Count));
        var keys = stats.SelectMany(s => s.Completed.Keys).ToList();
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.Ordinal).Count());
        Assert.All(stats.SelectMany(s => s.Invoked.Values), n => Assert.Equal(1, n));
        // Exactly one executed attempt per job, and it succeeded. A reservation abandoned or released before Begin (for
        // example a Begin past its resolution budget on a loaded host, then recovered) is not an execution.
        var irregular = await TextAsync(f, """
            SELECT a.job_id || ' ' || string_agg(coalesce(a.outcome, 'open') || '#' || coalesce(a.execution_number::text, '-')
                                                 || '@' || a.worker_instance_id, ', ' ORDER BY a.reserved_at)
            FROM runwick.job_attempts AS a
            WHERE a.job_id IN (
                SELECT job_id FROM runwick.job_attempts GROUP BY job_id
                HAVING count(*) FILTER (WHERE execution_number IS NOT NULL) <> 1
                    OR count(*) FILTER (WHERE outcome = 'succeeded') <> 1)
            GROUP BY a.job_id
            """);
        Assert.True(irregular.Count == 0, "jobs without exactly one executed, succeeded attempt: " + string.Join(" | ", irregular));

        // The producer process also hosts the control services; a worker process sees the pause through its cursor.
        await PauseAsync(f, producer.Services, workers[1]);
        foreach (var w in workers)
        {
            await w.StopAsync();
        }

        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-03")]
    public async Task Worker_processes_on_disjoint_queues_take_only_their_own_lane()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pmode03"));
        await using var toA = await Role(f, "producer-a");
        await using var toB = await Role(f, "producer-b", queue: "lane_b");
        var a = await StartWorker(f, "worker-a");
        var b1 = await StartWorker(f, "worker-b1", queue: "lane_b");
        var b2 = await StartWorker(f, "worker-b2", queue: "lane_b");
        await AssertPresence(f, a, b1, b2);

        var jobs = new List<JobId>();
        for (var i = 0; i < 6; i++)
        {
            jobs.Add((await toA.Dispatcher.DispatchAsync(new WorkJob("a" + i))).JobId);
            jobs.Add((await toB.Dispatcher.DispatchAsync(new WorkJob("b" + i))).JobId);
        }

        foreach (var job in jobs)
        {
            await WaitState(f, job, "succeeded");
        }

        var sa = await a.StatsAsync();
        var sb = new[] { await b1.StatsAsync(), await b2.StatsAsync() };
        Assert.Equal(6, sa.Completed.Count);
        Assert.All(sa.Completed.Keys, k => Assert.StartsWith("a", k, StringComparison.Ordinal));
        Assert.Equal(6, sb.Sum(s => s.Completed.Count));
        Assert.All(sb.SelectMany(s => s.Completed.Keys), k => Assert.StartsWith("b", k, StringComparison.Ordinal));

        // Pausing lane B holds lane B only, in both of its worker processes; A keeps draining its lane.
        var paused = await PauseAsync(f, toB.Services, a, "lane_b");
        var heldB = (await toB.Dispatcher.DispatchAsync(new WorkJob("b-held"))).JobId;
        var freeA = (await toA.Dispatcher.DispatchAsync(new WorkJob("a-free"))).JobId;
        await WaitState(f, freeA, "succeeded");
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal("pending", await State(f, heldB));
        Assert.True((await Queues(toB.Services).ResumeAsync("lane_b", paused.Revision!.Value, Operator)).Succeeded);
        await WaitState(f, heldB, "succeeded");

        foreach (var w in new[] { a, b1, b2 })
        {
            await w.StopAsync();
        }

        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-04")]
    public async Task A_control_only_process_steers_a_worker_process()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pmode04"));
        await using var control = await Role(f, "control");
        var w = await StartWorker(f, "worker");
        Assert.Null(control.Services.GetService<WorkerEngine>());
        await AssertPresence(f, w);

        var paused = await PauseAsync(f, control.Services, w);
        var held = await w.DispatchAsync("held");
        var kept = await w.DispatchAsync("kept");
        var cancelled = await Jobs(control.Services).CancelAsync(await Target(f, held), Operator, "not needed");
        Assert.True(cancelled.Succeeded, cancelled.Code);
        Assert.Equal("cancelled", cancelled.Effect);
        Assert.Equal(Operator.Subject, await w.ObserveAsync("job_cancelled", held));

        Assert.True((await Queues(control.Services).ResumeAsync("default", paused.Revision!.Value, Operator)).Succeeded);
        await WaitState(f, kept, "succeeded");

        // A job dispatched by the control process is processed by the worker process.
        var fromControl = (await control.Dispatcher.DispatchAsync(new WorkJob("from-control"))).JobId;
        await WaitState(f, fromControl, "succeeded");
        Assert.False((await w.StatsAsync()).Invoked.ContainsKey("held"));

        // A running job in the worker process: Cancel records the request (T22); the worker observes it on Renew and
        // cancels the handler token, which the "long" handler honours.
        var running = await w.DispatchAsync("long-1");
        await WaitState(f, running, "running");
        var requested = await Jobs(control.Services).CancelAsync(await Target(f, running), Operator, "stop it");
        Assert.True(requested.Succeeded, requested.Code);
        Assert.Equal("cancel_requested", requested.Effect);
        Assert.Equal(Operator.Subject, await w.ObserveAsync("job_cancel_requested", running));
        await WaitState(f, running, "cancelled");
        Assert.Equal("user_cancelled",
            (await TextAsync(f, "SELECT terminal_reason FROM runwick.jobs WHERE job_id = @j", ("j", running.Value))).Single());
        Assert.Equal(4, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE application = @a", ("a", f.Application)));
        await w.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-05")]
    public async Task Producer_control_and_three_staged_completion_worker_processes_run_together()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pmode05"));
        await f.ExecAsync("CREATE TABLE public.ledger (job_id uuid NOT NULL, attempt_id uuid NOT NULL)");
        await using var producer = await Role(f, "producer");
        await using var control = await Role(f, "control");
        PeerWorkerHandle[] workers =
        [
            .. await Task.WhenAll(Enumerable.Range(1, 3).Select(i =>
                StartWorker(f, "worker" + i, businessConnectionString: f.ClientConnectionString("app" + i, "business")))),
        ];
        await AssertPresence(f, workers);

        var paused = await PauseAsync(f, control.Services, workers[0]);
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
        }

        Assert.Equal(victims.Count, await f.CountAsync(
            "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'job_cancelled' AND actor_id = @actor AND job_id = ANY(@v)",
            ("actor", Operator.Subject), ("v", victims.Select(v => v.Value).ToArray())));

        // A worker process sees the cancellations through its cursor (the last one; each read starts from the beginning).
        Assert.Equal(Operator.Subject, await workers[2].ObserveAsync("job_cancelled", victims[^1]));

        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM public.ledger WHERE job_id = ANY(@v)", ("v", victims.Select(v => v.Value).ToArray())));

        // Every committed stage is settled by its worker process: by observing the staged receipt or, when that lookup
        // stays unresolved under load, by the normal-path Finish that storage rejects (08 §8.2; as the SP-011 MODE-05).
        var expected = all.Count - victims.Count;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        Stats[] stats;
        while (true)
        {
            stats = await Task.WhenAll(workers.Select(w => w.StatsAsync()));
            if (stats.Sum(s => s.StagedObserved + s.FinishesSent) == expected)
            {
                break;
            }

            Assert.True(DateTime.UtcNow < deadline, $"settled {stats.Sum(s => s.StagedObserved + s.FinishesSent)} of {expected}");
            await Task.Delay(100);
        }

        Assert.True(stats.Sum(s => s.StagedObserved) > 0);
        Assert.Equal(expected, stats.Sum(s => s.Completed.Count));
        foreach (var w in workers)
        {
            await w.StopAsync();
        }

        await AssertInvariants(f);
    }
}
