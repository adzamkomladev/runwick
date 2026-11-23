using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// Combined finish and claim (05 §12.7) in the worker: with the option on, every job still settles exactly once with one Finish receipt on every
/// store (stores without the call keep finishing and claiming separately); on Redis and PostgreSQL the call is used, and a refused or lost call
/// falls back to resolving each command by its own ID.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerWideCollection))]
public sealed class FinishAndClaimContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    /// <summary>The stores that offer the combined call, among those this run tests.</summary>
    public static TheoryData<StoreKind> CombinedKinds => [.. CoreContractTests.Kinds.Select(k => k.Data).Where(Combined)];

    static bool Combined(StoreKind kind) => kind is StoreKind.Redis or StoreKind.Postgres;

    static WorkerHost Host(DriverFixture f, int concurrency, Action<BatchStoreDouble>? made = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, options: o => o.UseCombinedFinishClaim(),
            overrides: made is null ? null : s => s.AddSingleton<IRunwickStore>(sp =>
            {
                var store = new BatchStoreDouble(f.Kind == StoreKind.Postgres ? sp.GetRequiredService<PgStore>() : sp.GetRequiredService<RedisStore>());
                made(store);
                return store;
            }));

    static Task DispatchAsync(WorkerHost w, int jobs) =>
        w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);

    static async Task<int> SucceededAsync(DriverFixture f) => (await Model(f)).Jobs.Count(j => j.State == "succeeded");

    static async Task<long> FinishReceipts(DriverFixture f) => (await Model(f)).Receipts.Count(r => r.Operation == "Finish");

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FC-10")]
    public async Task Every_job_settles_once_with_one_finish_receipt(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc10"), kind);
        await using var w = Host(f, 4);
        await DispatchAsync(w, 80);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 80, TimeSpan.FromSeconds(120), "all 80 jobs settled");
        Assert.Equal(80, w.Script.Invocations.Count);
        Assert.Equal(80, await FinishReceipts(f));
        Assert.Empty(await History(f));
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));
        if (Combined(kind))
        {
            Assert.True(w.Engine.Completion.CombinedCalls > 0, "the combined call was never used");
        }
        else
        {
            Assert.Equal(0, w.Engine.Completion.CombinedCalls);
        }
    }

    [Theory]
    [MemberData(nameof(CombinedKinds))]
    [Trait("Case", "FC-11")]
    public async Task A_refused_call_stops_the_option_and_separate_calls_finish_everything(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc11"), kind);
        await using var w = Host(f, 4, s => s.Refused.Add("FinishAndClaim"));
        await DispatchAsync(w, 40);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 40, TimeSpan.FromSeconds(120), "all 40 jobs settled");
        Assert.Equal(40, w.Script.Invocations.Count);
        Assert.Equal(40, await FinishReceipts(f));
        Assert.Equal(1, w.Engine.Completion.CombinedCalls); // one refusal turned the option off for this worker
        Assert.True(w.Engine.Completion.CombinedFallbacks >= 1);
    }

    [Theory]
    [MemberData(nameof(CombinedKinds))]
    [Trait("Case", "FC-12")]
    public async Task A_lost_reply_resolves_each_command_by_its_own_id_and_nothing_runs_twice(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc12"), kind);
        BatchStoreDouble? store = null;
        await using var w = Host(f, 4, s =>
        {
            store = s;
            s.LoseNextFinishAndClaim();
        });
        await DispatchAsync(w, 40);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 40, TimeSpan.FromSeconds(120), "all 40 jobs settled");
        Assert.Equal(40, w.Script.Invocations.Count);
        Assert.Equal(40, await FinishReceipts(f));
        Assert.Equal(1, store!.RepliesLost);
        Assert.Empty(await History(f));
    }
}
