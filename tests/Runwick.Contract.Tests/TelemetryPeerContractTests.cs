using System.Text.Json.Nodes;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Contract.Tests;

/// <summary>
/// OBS-08 (18 §10) on every driver, ported from Runwick.Postgres.Tests.TelemetryPeerTests: telemetry is best-effort and
/// process-local. A worker process killed after its Finish committed and before the settlement measurement leaves the transition in
/// durable history and no measurement; the restarted process does not authorize or count the finished job again.
/// </summary>
[Trait("Category", "Fault")]
[Trait("Item", "SP-028")]
public sealed class TelemetryPeerContractTests
{
    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static long Count(JsonObject counts, string key) => counts[key]?.GetValue<long>() ?? 0;

    static async Task<StoreSnapshot> ModelAsync(DriverFixture f) => (await f.ReadCommittedSnapshot()).Model;

    static bool Succeeded(DriverFixture f, Guid job) => ModelAsync(f).GetAwaiter().GetResult().Job(job).State == "succeeded";

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "OBS-08")]
    public async Task A_peer_killed_after_Finish_commits_keeps_the_transition_and_the_restart_counts_nothing_again(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("obs08_" + Guid.NewGuid().ToString("N")[..8], kind);
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
        await WorkerHost.WaitUntil(() => Succeeded(f, job), Long, "Finish committed");
        var seenByA = await a.SendAsync("telemetryRead");
        a.Kill();

        Assert.Equal(1, Count(seenByA, "runwick.execution.authorized"));
        Assert.Equal(0, Count(seenByA, "runwick.settlement.duration|confirmed"));

        // Durable history has the transition; the measurements A had made died with it.
        var before = await ModelAsync(f);
        Assert.Equal("succeeded", Assert.Single(before.AttemptsOf(job)).Outcome);
        Assert.Equal(1, before.Job(job).LifetimeExecutions);

        // Peer B restarts the worker on the same store: it never begins the finished job, so it counts nothing for it.
        var b = await f.OpenPeer("obs08-b");
        await b.SendAsync("telemetryListen");
        await b.SendAsync("startWorker", new JsonObject { ["cs"] = f.ClientConnectionString("obs08-b"), ["app"] = f.Application, ["concurrency"] = 1 });
        var probe = (await local.Dispatcher.DispatchAsync(new WorkJob("after-restart"))).JobId.Value;
        await WorkerHost.WaitUntil(() => Succeeded(f, probe), Long, "the restarted worker runs new work");

        // The row turns 'succeeded' at COMMIT, before the Finish reply is back and the settlement is measured: wait for the measurement.
        JsonObject seenByB = [];
        await WorkerHost.WaitUntil(() =>
        {
            seenByB = b.SendAsync("telemetryRead").Result;
            return Count(seenByB, "runwick.settlement.duration|confirmed") >= 1;
        }, Long, "the restarted worker's settlement measurement");

        // Exactly the new job: one authorization and one confirmed settlement; the killed attempt is not replayed or recounted.
        Assert.Equal(1, Count(seenByB, "runwick.execution.authorized"));
        Assert.Equal(1, Count(seenByB, "runwick.settlement.duration|confirmed"));
        var after = await ModelAsync(f);
        Assert.Equal(1, after.Job(job).LifetimeExecutions);
        Assert.Single(after.AttemptsOf(job));
    }
}
