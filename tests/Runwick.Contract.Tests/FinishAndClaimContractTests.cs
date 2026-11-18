using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// Combined finish and claim (05 §12.7) in the worker: with the option on, every job still settles exactly once with one Finish receipt on every
/// store (stores without the call keep finishing and claiming separately); on Redis the call is used, and a refused or lost call falls back to
/// resolving each command by its own ID.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerWideCollection))]
public sealed class FinishAndClaimContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static WorkerHost Host(DriverFixture f, int concurrency, Action<BatchStoreDouble>? made = null, bool buffered = false) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency,
            options: o => (buffered ? o.UseBufferedFinish() : o).UseCombinedFinishClaim(),
            overrides: made is null ? null : s => s.AddSingleton<IRunwickStore>(sp =>
            {
                var store = new BatchStoreDouble(sp.GetRequiredService<RedisStore>());
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
        if (kind == StoreKind.Redis)
        {
            Assert.True(w.Engine.Completion.CombinedCalls > 0, "the combined call was never used");
        }
        else
        {
            Assert.Equal(0, w.Engine.Completion.CombinedCalls);
        }
    }

    [Fact]
    [Trait("Case", "FC-11")]
    public async Task A_refused_call_stops_the_option_and_separate_calls_finish_everything()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc11"), StoreKind.Redis);
        await using var w = Host(f, 4, s => s.Refused.Add("FinishAndClaim"));
        await DispatchAsync(w, 40);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 40, TimeSpan.FromSeconds(120), "all 40 jobs settled");
        Assert.Equal(40, w.Script.Invocations.Count);
        Assert.Equal(40, await FinishReceipts(f));
        Assert.Equal(1, w.Engine.Completion.CombinedCalls); // one refusal turned the option off for this worker
        Assert.True(w.Engine.Completion.CombinedFallbacks >= 1);
    }

    [Fact]
    [Trait("Case", "FC-12")]
    public async Task A_lost_reply_resolves_each_command_by_its_own_id_and_nothing_runs_twice()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc12"), StoreKind.Redis);
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

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FC-13")]
    public async Task With_buffered_finish_the_pool_claim_carries_the_waiting_finishes_and_every_job_settles_once(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc13"), kind);
        await using var w = Host(f, 4, buffered: true);
        await DispatchAsync(w, 80);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 80, TimeSpan.FromSeconds(120), "all 80 jobs settled");
        Assert.Equal(80, w.Script.Invocations.Count);
        Assert.Equal(80, await FinishReceipts(f));
        Assert.Empty(await History(f));
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));
        if (kind == StoreKind.Redis)
        {
            Assert.True(w.Engine.Completion.LoopCarriedCalls > 0, "the pool's claim never carried a finish");
        }
        else
        {
            Assert.Equal(0, w.Engine.Completion.LoopCarriedCalls);
        }
    }

    [Fact]
    [Trait("Case", "FC-14")]
    public async Task A_lost_reply_of_a_carried_claim_resolves_each_command_and_nothing_runs_twice()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc14"), StoreKind.Redis);
        BatchStoreDouble? store = null;
        await using var w = Host(f, 4, s =>
        {
            store = s;
            s.LoseNextFinishAndClaim();
        }, buffered: true);
        await DispatchAsync(w, 40);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 40, TimeSpan.FromSeconds(120), "all 40 jobs settled");
        Assert.Equal(40, w.Script.Invocations.Count);
        Assert.Equal(40, await FinishReceipts(f));
        Assert.Equal(1, store!.RepliesLost);
        Assert.True(w.Engine.Completion.LoopCarriedCalls > 0);
        Assert.Empty(await History(f));
    }

    [Fact]
    [Trait("Case", "FC-15")]
    public async Task A_refused_carried_claim_stops_the_option_and_the_claim_is_sent_alone()
    {
        await using var f = await DriverFixture.StartAsync(Ns("fc15"), StoreKind.Redis);
        await using var w = Host(f, 4, s => s.Refused.Add("FinishAndClaim"), buffered: true);
        await DispatchAsync(w, 40);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 40, TimeSpan.FromSeconds(120), "all 40 jobs settled");
        Assert.Equal(40, w.Script.Invocations.Count);
        Assert.Equal(40, await FinishReceipts(f));
        Assert.Equal(1, w.Engine.Completion.CombinedCalls);
        Assert.Empty(await History(f));
    }
}
