using System.Text.Json.Nodes;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// OBS-08 (18 §10): telemetry is best-effort and process-local. A worker process killed after its Finish committed and
/// before the settlement measurement leaves the transition in durable history and no measurement; the restarted process
/// does not authorize or count the finished job again.
/// </summary>
public sealed class TelemetryPeerTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static long Count(JsonObject counts, string key) => counts[key]?.GetValue<long>() ?? 0;

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-022")]
    [Trait("Case", "OBS-08")]
    public async Task A_peer_killed_after_Finish_commits_keeps_the_transition_and_the_restart_counts_nothing_again()
    {
        await using var f = await DriverFixture.StartAsync(Ns("obs08"));
        var local = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<WorkJob>().Named("test.work").ToQueue("default").WithMaxExecutions(3);
        });

        // Peer A: every Finish reply is held after COMMIT (resends included), so the settlement is never measured there.
        var a = await f.OpenPeer("obs08-a");
        await a.SendAsync("telemetryListen");
        foreach (var operation in new[] { Operations.Finish, Operations.FinishMany })
        {
            await a.SendAsync("armFault", new JsonObject
            {
                ["point"] = "AfterCommitBeforeReply", ["action"] = "Delay", ["operation"] = operation, ["delayMs"] = 600_000, ["times"] = 100,
            });
        }

        var job = (await local.Dispatcher.DispatchAsync(new WorkJob("obs08"))).JobId.Value;
        await a.SendAsync("startWorker", new JsonObject { ["cs"] = f.ClientConnectionString("obs08-a"), ["app"] = f.Application, ["concurrency"] = 1 });
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", job)).Result == 1,
            Long, "Finish committed");
        var seenByA = await a.SendAsync("telemetryRead");
        a.Kill();

        Assert.Equal(1, Count(seenByA, "runwick.execution.authorized"));
        Assert.Equal(0, Count(seenByA, "runwick.settlement.duration|confirmed"));

        // Durable history has the transition; the measurements A had made died with it.
        Assert.Equal(["succeeded"], await TextAsync(f, "SELECT coalesce(outcome, '<open>') FROM runwick.job_attempts WHERE job_id = @j", ("j", job)));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));

        // Peer B restarts the worker on the same store: it never begins the finished job, so it counts nothing for it.
        var b = await f.OpenPeer("obs08-b");
        await b.SendAsync("telemetryListen");
        await b.SendAsync("startWorker", new JsonObject { ["cs"] = f.ClientConnectionString("obs08-b"), ["app"] = f.Application, ["concurrency"] = 1 });
        var probe = (await local.Dispatcher.DispatchAsync(new WorkJob("after-restart"))).JobId.Value;
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", probe)).Result == 1,
            Long, "the restarted worker runs new work");
        var seenByB = await b.SendAsync("telemetryRead");

        // Exactly the new job: one authorization and one confirmed settlement; the killed attempt is not replayed or recounted.
        Assert.Equal(1, Count(seenByB, "runwick.execution.authorized"));
        Assert.Equal(1, Count(seenByB, "runwick.settlement.duration|confirmed"));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", job)));
    }
}
