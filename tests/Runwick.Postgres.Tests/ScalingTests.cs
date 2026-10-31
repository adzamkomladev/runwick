using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Postgres.Tests;

/// <summary>
/// SP-014 against the real store (11 §4–§6, 15 §7): the shared demand estimate, SetPoolOverride/1 revisions and
/// replay, history only on change, and a target reduction that cancels nothing.
/// </summary>
[Trait("Item", "SP-014")]
public sealed class ScalingTests
{
    static readonly ControlActor Actor = new("user:ops", null, "host", "scaling");

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-07")]
    [Trait("Case", "CMD-01")]
    public async Task Pool_overrides_are_revisioned_replayable_and_audited()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ovr"));
        var c = await f.OpenClient();
        var instance = Guid.CreateVersion7();
        var ct = TestContext.Current.CancellationToken;

        // A replay is the same command: same ID, body and NotAfter (whole ms).
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1)).ToUnixTimeMilliseconds());
        var create = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor), id, notAfter), ct);
        Assert.Equal(StoreStatus.Applied, create.Status);
        Assert.Equal((3, 1L), (create.Value!.Target, create.Value.Revision));

        var replay = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor), id, notAfter), ct);
        Assert.True(replay.Replayed);
        Assert.Equal(create.Value, replay.Value);

        var same = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 1, Actor)), ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        var stale = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 5, null, 0, Actor)), ct);
        Assert.Equal((StoreStatus.VersionConflict, 1L), (stale.Status, stale.Error!.ActualRevision));

        var expires = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(10);
        var timed = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 5, expires, 1, Actor)), ct);
        Assert.Equal((5, 2L), (timed.Value!.Target, timed.Value.Revision));
        var cleared = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", null, null, 2, Actor)), ct);
        Assert.Equal((null, 3L), (cleared.Value!.Target, cleared.Value.Revision));

        foreach (var (target, exp, field) in new (int?, DateTimeOffset?, string)[]
        {
            (10_001, null, "target"), (-1, null, "target"), (null, expires, "expiresAt"),
            (1, DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1), "expiresAt"), (1, DateTimeOffset.UtcNow + TimeSpan.FromDays(8), "expiresAt"),
        })
        {
            var bad = await c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", target, exp, 3, Actor)), ct);
            Assert.Equal((StoreStatus.InvalidRequest, field), (bad.Status, bad.Error!.Detail));
        }

        // Applied changes only: one event and one audit row each; rejections and the replay write neither.
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'pool_override_set'"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetPoolOverride' AND target_kind = 'pool'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-08")]
    [Trait("Case", "WRK-02")]
    [Trait("Case", "CAP-12")]
    public async Task The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing()
    {
        await using var f = await DriverFixture.StartAsync(Ns("scl"));
        var release = new TaskCompletionSource();
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 1));
        w.Script.Default = (_, _, _) => release.Task;
        foreach (var key in new[] { "a", "b", "c", "d" })
        {
            await w.DispatchAsync(key);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 1, Wait, "one job running");

        var ct = TestContext.Current.CancellationToken;
        var scaling = (IScalingStore)w.Engine.Store;
        var estimate = await scaling.EstimateDemandAsync(f.Application, w.Engine.WorkerInstanceId, ["default"], w.Engine.Contracts, ct);
        Assert.Equal(1, estimate.LiveWorkers);
        Assert.NotEqual(0, estimate.MembershipDigest);
        Assert.False(estimate.Truncated);
        var row = Assert.Single(estimate.Backlog);
        Assert.Equal(("default", "test.work", 3L, 1), (row.Queue, row.Contract.Name, row.Due, row.CapableInstances));
        Assert.Empty(estimate.Overrides);

        // Override to zero: refill stops, the running handler is not cancelled and finishes normally.
        var c = await f.OpenClient();
        var set = await scaling.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(w.Engine.WorkerInstanceId, "main", 0, null, 0, Actor)), ct);
        Assert.Equal(StoreStatus.Applied, set.Status);
        var pool = w.Engine.Pools.Single();
        await WorkerHost.WaitUntil(() => pool.Gate.Target == 0, Wait, "override applied");
        Assert.Contains(pool.LastDecision!.Reason, new[] { "override", "override_active" });
        Assert.Equal(0, (await scaling.EstimateDemandAsync(f.Application, w.Engine.WorkerInstanceId, ["default"], w.Engine.Contracts, ct)).Overrides["main"]);

        release.SetResult();
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 1, Wait, "the running job completes");
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        Assert.Single(w.Script.Invocations);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'cancelled'"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        // History holds the applied change only, never unchanged samples.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.scaling_events WHERE old_target = 1 AND new_target = 0 AND reason = 'override'"));

        // Clearing the override resumes inside [min, max]; the backlog drains.
        var clear = await scaling.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(w.Engine.WorkerInstanceId, "main", null, null, 1, Actor)), ct);
        Assert.Equal(StoreStatus.Applied, clear.Status);
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 4, Wait, "backlog drains after the override is removed");
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.scaling_events"));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-07")]
    public async Task Unchanged_targets_are_never_recorded_and_unknown_demand_is_not_zero()
    {
        await using var f = await DriverFixture.StartAsync(Ns("hist"));
        var c = await f.OpenClient();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => c.Store.RecordScalingEventAsync(f.Application,
            new ScalingEventRecord(Guid.CreateVersion7(), "main", 2, 2, 2, 2, "within_band", null, null), ct));
        Assert.True(await c.Store.RecordScalingEventAsync(f.Application,
            new ScalingEventRecord(Guid.CreateVersion7(), "main", 2, 3, 4, 4, "grow", DateTimeOffset.UtcNow, 7), ct));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.scaling_events"));

        // No live worker and no backlog: a known, empty estimate — distinct from a failed one, which throws.
        var empty = await c.Store.EstimateDemandAsync(f.Application, Guid.CreateVersion7(), ["default"],
            [new JobContract("test.work", 1, Serialization.RunwickJsonProfile.ProfileId, Serialization.RunwickJsonProfile.ProfileVersion)], ct);
        Assert.Equal((0, 0L), (empty.LiveWorkers, empty.MembershipDigest));
        Assert.Empty(empty.Backlog);
    }
}
