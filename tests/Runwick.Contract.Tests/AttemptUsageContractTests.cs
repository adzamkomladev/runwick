using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// USG-01..02 (16 Changes 2026-11-09): what the worker process used during an attempt is stored with the attempt when its
/// Finish applies, on every store, and GetJob returns it; an attempt finished without it reads as not recorded (null).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "USG")]
public sealed class AttemptUsageContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly ManagementCaller Admin = new(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);

    static AttemptResourceUsage Usage(int seed) => new(1_000 + seed, 2_000_000 + seed, 50_000_000, 51_000_000 + seed, 60_000_000 + seed, seed % 3);

    static StoreCommand<FinishRequest> WithUsage(StoreCommand<FinishRequest> cmd, AttemptResourceUsage? usage) =>
        cmd with { Body = cmd.Body with { Usage = usage } };

    static async Task<AttemptResourceUsage?> StoredAsync(TestClient c, Guid job) =>
        Assert.Single((await c.Services.GetRequiredService<IManagementSession>().GetJobAsync(Admin, job, CancellationToken.None))!.Attempts).Usage;

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "USG-01")]
    public async Task Finish_stores_the_usage_with_the_attempt_and_a_replay_keeps_it(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("usg01"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await FinishManyContractTests.RunningAsync(c, new PingJob("a"), new PingJob("b"));

        var with = WithUsage(c.FinishCommand(leases[0], AttemptOutcome.Succeeded), Usage(7));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(with)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(leases[1], AttemptOutcome.Succeeded)).Status);

        Assert.Equal(Usage(7), await StoredAsync(c, leases[0].JobId));
        Assert.Null(await StoredAsync(c, leases[1].JobId)); // finished without usage: not recorded

        var replay = await c.FinishAsync(with);
        Assert.Equal(StoreStatus.Applied, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Equal(Usage(7), await StoredAsync(c, leases[0].JobId));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "USG-02")]
    public async Task FinishMany_stores_each_applied_items_usage_and_a_refused_item_writes_none(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("usg02"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await FinishManyContractTests.RunningAsync(c, new PingJob("a"), new PingJob("b"), new PingJob("c"));
        var stale = leases[1] with { OwnershipToken = Guid.NewGuid() };

        var reply = await c.FinishManyAsync(
        [
            WithUsage(c.FinishCommand(leases[0], AttemptOutcome.Succeeded), Usage(1)),
            WithUsage(c.FinishCommand(stale, AttemptOutcome.Succeeded), Usage(2)),
            WithUsage(c.FinishCommand(leases[2], AttemptOutcome.PermanentFailure, error: new AttemptError("boom", "System.Exception", "m", null)), Usage(3)),
        ]);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal([StoreStatus.Applied, StoreStatus.StaleLease, StoreStatus.Applied], reply.Value!.Outcomes.Select(o => o.Status));

        Assert.Equal(Usage(1), await StoredAsync(c, leases[0].JobId));
        Assert.Null(await StoredAsync(c, leases[1].JobId));
        Assert.Equal(Usage(3), await StoredAsync(c, leases[2].JobId));
    }
}
