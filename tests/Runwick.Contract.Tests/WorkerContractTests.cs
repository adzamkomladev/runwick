using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.ReferenceModel;

namespace Runwick.Contract.Tests;

/// <summary>The real worker engine over each store kind (20 §12): batched claim, renewal and finish included.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class WorkerContractTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RUN-01")]
    public async Task A_worker_runs_dispatched_jobs_to_success_with_one_retry(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("wrk_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 8);
        var failedOnce = 0;
        w.Script.On("flaky", (_, _, _) => Interlocked.Exchange(ref failedOnce, 1) == 0 ? throw new InvalidOperationException("first try") : Task.CompletedTask);
        await w.StartAsync();
        await w.DispatchAsync("flaky");
        for (var i = 0; i < 40; i++)
        {
            await w.DispatchAsync("j" + i);
        }

        try
        {
            await WorkerHost.WaitUntil(() => f.ReadCommittedSnapshot().GetAwaiter().GetResult().Model.Jobs.Count(j => j.State == "succeeded") == 41, Wait, "41 succeeded");
        }
        catch (TimeoutException)
        {
            var s = await f.ReadCommittedSnapshot();
            Assert.Fail($"states: {string.Join(", ", s.Model.Jobs.GroupBy(j => j.State).Select(g => g.Key + "=" + g.Count()))}; "
                + $"events {string.Join(",", w.Events.Select(e => e.Item1).Distinct().Take(30))}; invocations {w.Script.Invocations.Count}; receipts {string.Join(", ", s.Model.Receipts.GroupBy(r => r.Operation).Select(g => g.Key + "=" + g.Count()))}");
        }

        await w.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, w.Script.InvocationsOf("flaky"));
        Assert.Equal(42, w.Script.Invocations.Count);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
