using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Sqlite.Store;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// G1 (buffered finish) on the real stores: every job settles exactly once with one Finish receipt, the buffer stays inside its bound, a
/// graceful stop sends what is buffered, and a worker whose finishes never reached the store leaves its jobs to expire and run again.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-G1")]
[Collection(nameof(RedisServerWideCollection))]
public sealed class BufferedFinishContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    /// <summary>The crash case needs a double in front of the store, which exists for SQLite and Redis.</summary>
    public static TheoryData<StoreKind> DoubleKinds => new() { StoreKind.Sqlite, StoreKind.Redis };

    static WorkerHost Host(DriverFixture f, int concurrency, Action<WorkerOptions>? options, Action<BatchStoreDouble>? made = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, options: options, overrides: made is null ? null :
            s => s.AddSingleton<IRunwickStore>(sp => Wrap(f, sp, made)));

    static BatchStoreDouble Wrap(DriverFixture f, IServiceProvider sp, Action<BatchStoreDouble> made)
    {
        var store = new BatchStoreDouble(f.Kind == StoreKind.Sqlite ? sp.GetRequiredService<SqliteStore>() : sp.GetRequiredService<RedisStore>());
        made(store);
        return store;
    }

    static Task DispatchAsync(WorkerHost w, int jobs) =>
        w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);

    static async Task<int> SucceededAsync(DriverFixture f) => (await Model(f)).Jobs.Count(j => j.State == "succeeded");

    static async Task<long> FinishReceipts(DriverFixture f) => (await Model(f)).Receipts.Count(r => r.Operation == "Finish");

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "G1-01")]
    public async Task Every_job_settles_once_and_the_buffer_empties(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("g1a"), kind);
        await using var w = Host(f, 4, o => o.UseBufferedFinish());
        await DispatchAsync(w, 60);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 60, TimeSpan.FromSeconds(120), "all 60 jobs settled");
        Assert.Equal(60, w.Script.Invocations.Count);
        Assert.Equal(60, await FinishReceipts(f));
        Assert.Equal(0, w.Engine.Completion.Buffered);
        Assert.True(w.Engine.Completion.BufferedHighWater >= 1);
        Assert.Empty(await History(f));
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "G1-02")]
    public async Task A_full_buffer_keeps_slots_and_loses_nothing(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("g1b"), kind);
        await using var w = Host(f, 8, o => o.UseBufferedFinish(maxCount: 2));
        await DispatchAsync(w, 48);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 48, TimeSpan.FromSeconds(120), "all 48 jobs settled");
        Assert.InRange(w.Engine.Completion.BufferedHighWater, 1, 2);
        Assert.Equal(48, await FinishReceipts(f));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "G1-03")]
    public async Task A_graceful_stop_sends_the_buffered_finishes(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("g1c"), kind);

        // A one-second interval would keep the finishes in memory for a second; the stop sends them at once.
        await using var w = Host(f, 16, o => o.UseBufferedFinish(TimeSpan.FromSeconds(1)));
        await DispatchAsync(w, 16);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, TimeSpan.FromSeconds(60), "every handler ran");
        await w.StopAsync();
        Assert.Equal(16, await SucceededAsync(f));
        Assert.Equal(16, await FinishReceipts(f));
        Assert.Equal(0, w.Engine.Completion.Buffered);
    }

    [Theory]
    [MemberData(nameof(DoubleKinds))]
    [Trait("Case", "G1-04")]
    public async Task Finishes_that_never_reached_the_store_leave_the_jobs_to_expire_and_run_again_on_another_worker(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("g1d"), kind);
        var jobs = 8;
        await using (var dead = Host(f, 4, o => o.UseBufferedFinish(), s => s.HoldFinishes = true))
        {
            await DispatchAsync(dead, jobs);
            await dead.StartAsync();

            // Every handler ran (the slots were reused: eight jobs on four slots) and no finish reached the store.
            await WorkerHost.WaitUntil(() => dead.Script.Invocations.Count == jobs, TimeSpan.FromSeconds(60), "every handler ran");
            Assert.Equal(0, await SucceededAsync(f));
            Assert.Equal(0, await FinishReceipts(f));
        }

        // Another worker finds the leases expired, recovers the jobs and runs them again.
        await using var survivor = Host(f, 4, null);
        await survivor.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == jobs, TimeSpan.FromSeconds(180), "every job re-run and settled");
        Assert.Equal(jobs, survivor.Script.Invocations.Count);
        var model = await Model(f);
        Assert.All(model.Jobs, j => Assert.True(model.AttemptsOf(j.JobId).Count() >= 2, "the first attempt was never finished, the second was"));
    }
}
