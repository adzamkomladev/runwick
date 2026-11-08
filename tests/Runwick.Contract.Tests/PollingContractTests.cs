using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;
using static Runwick.Contract.Tests.ControlContractTests;

namespace Runwick.Contract.Tests;

/// <summary>
/// driver-contract-suite:OBS-01 on SQLite and Redis (PostgreSQL keeps its LISTEN form in Runwick.Postgres.Tests): with every wake
/// dropped (notifications on receipt and the local wakes), polling alone finds due work, a pause and its resume, and a dead
/// owner's lease.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class PollingContractTests
{
    public static TheoryData<StoreKind> NonPostgres => UpgradeContractTests.NonPostgresKinds;

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    [Theory]
    [MemberData(nameof(NonPostgres))]
    [Trait("Case", "OBS-01")]
    public async Task With_every_wake_dropped_polling_finds_work_controls_and_recovery(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("obs01_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 4, configure: r => r.Queue("other"));
        w.Engine.NotificationsSuppressed = true;
        foreach (var pool in w.Engine.Pools)
        {
            pool.Wake.Suppressed = true;
        }

        await w.StartAsync();

        // The bound is the idle cap this store's worker runs with, plus 20 % jitter, the hint age and one probe and claim (slack 4 s).
        var bound = w.Engine.IdleBackoffCap * 1.2 + WorkerSettings.HintMaxAge + TimeSpan.FromSeconds(4);
        var producer = await f.OpenClient(configure: r =>
        {
            WorkerHost.RegisterWork(r);
            r.Queue("other");
        });

        // Due work from another process.
        var first = (await producer.Dispatcher.DispatchAsync(new WorkJob("polled"))).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("polled"), bound, "discovered by polling");

        // Pause then resume, observed through the probe.
        var revision = await Count(f, "SELECT revision FROM runwick.queue_controls WHERE queue = 'default'");
        var actor = new ControlActor("user:ops", null, "host", "obs01");
        var controls = Controls(producer);
        var paused = await controls.SetQueueStateAsync(producer.Command(new QueueStateRequest("default", true, revision, actor)), TestContext.Current.CancellationToken);
        Assert.Equal(StoreStatus.Applied, paused.Status);
        var held = (await producer.Dispatcher.DispatchAsync(new WorkJob("while-paused"))).JobId.Value;
        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Empty((await f.ReadCommittedSnapshot()).Model.AttemptsOf(held));
        var resumed = await controls.SetQueueStateAsync(producer.Command(new QueueStateRequest("default", false, paused.Value!.Revision, actor)),
            TestContext.Current.CancellationToken);
        Assert.Equal(StoreStatus.Applied, resumed.Status);
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("while-paused"), bound, "resume discovered by polling");

        // Recovery: a reservation whose owner vanished (a 1 s lease on queue "other", which this worker does not subscribe to) is
        // found by the timer-driven recovery loop (05 §13), which never waits on a wake signal.
        var orphan = (await producer.Dispatcher.DispatchAsync(new WorkJob("orphan"), new DispatchOptions { Queue = "other" })).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await producer.ReserveAsync(queue: "other", lease: TimeSpan.FromSeconds(1))).Status);
        await WorkerHost.WaitUntil(
            () => f.ReadCommittedSnapshot().GetAwaiter().GetResult().Model.AttemptsOf(orphan).Count(a => a.Outcome == "abandoned_before_start") == 1,
            Wait, "the dead owner's lease recovered by polling");
        Assert.NotEqual(first, orphan);

        // Every notification that arrived was dropped.
        Assert.Equal(w.Engine.NotificationsReceived, w.Engine.NotificationsDropped);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
