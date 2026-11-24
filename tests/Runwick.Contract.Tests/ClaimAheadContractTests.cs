using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Sqlite.Store;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// Claim ahead (05 §9.8) on the real stores: a pool claims beyond its concurrency but never runs more handlers than its concurrency, every
/// job runs once, a lost claim reply replays, and the jobs a worker holds waiting are recovered when it dies and never start once fenced.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-CA")]
[Collection(nameof(RedisServerWideCollection))]
public sealed class ClaimAheadContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    /// <summary>The fault cases need a double in front of the store, which exists for SQLite and Redis.</summary>
    public static TheoryData<StoreKind> DoubleKinds => new() { StoreKind.Sqlite, StoreKind.Redis };

    static WorkerHost Host(DriverFixture f, int concurrency, Action<WorkerOptions>? options, Action<BatchStoreDouble>? made = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, options: options, overrides: made is null ? null : (Action<IServiceCollection>)(
            s => s.AddSingleton<IRunwickStore>(sp =>
            {
                var store = new BatchStoreDouble(f.Kind == StoreKind.Sqlite ? sp.GetRequiredService<SqliteStore>() : sp.GetRequiredService<RedisStore>());
                made(store);
                return store;
            })));

    static Task DispatchAsync(WorkerHost w, int jobs) =>
        w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);

    static async Task<int> SucceededAsync(DriverFixture f) => (await Model(f)).Jobs.Count(j => j.State == "succeeded");

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CA-01")]
    public async Task Claims_beyond_concurrency_but_never_runs_more_handlers_than_it(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("caa"), kind);
        await using var w = Host(f, 4, o => o.ClaimAhead = 4);
        int running = 0, maxRunning = 0, maxHeld = 0;
        w.Script.Default = async (_, _, _) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref maxRunning, now);
            await Task.Delay(20, CancellationToken.None);
            Interlocked.Decrement(ref running);
        };
        w.Engine.Pools.Single().Gate.OutstandingChanged += n => InterlockedMax(ref maxHeld, n);
        await DispatchAsync(w, 60);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 60, TimeSpan.FromSeconds(120), "all 60 jobs settled");

        Assert.InRange(maxRunning, 1, 4);
        Assert.InRange(maxHeld, 5, 8);
        Assert.Equal(60, w.Script.Invocations.Count);
        Assert.All(w.Script.Invocations.GroupBy(i => i.JobKey), g => Assert.Single(g));
        Assert.Empty(await History(f));
        await w.StopAsync();
        Assert.Equal(0, w.Engine.Pools.Single().Gate.Outstanding);
    }

    [Theory]
    [MemberData(nameof(DoubleKinds))]
    [Trait("Case", "CA-02")]
    public async Task A_lost_claim_reply_replays_and_every_job_runs_once(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cab"), kind);
        BatchStoreDouble? store = null;
        await using var w = Host(f, 2, o => o.ClaimAhead = 4, s =>
        {
            store = s;
            s.LoseNextReserveAndBegin();
        });
        await DispatchAsync(w, 24);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == 24, TimeSpan.FromSeconds(120), "all 24 jobs settled");

        Assert.Equal(1, store!.RepliesLost);
        Assert.Equal(24, w.Script.Invocations.Count);
        Assert.All(w.Script.Invocations.GroupBy(i => i.JobKey), g => Assert.Single(g));
        var model = await Model(f);
        Assert.All(model.Jobs, j => Assert.Single(model.AttemptsOf(j.JobId)));
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(DoubleKinds))]
    [Trait("Case", "CA-03")]
    public async Task Waiting_jobs_of_a_dead_worker_are_recovered_and_never_start_once_fenced(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cac"), kind);
        BatchStoreDouble? store = null;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var jobs = 6;
        await using var dead = Host(f, 2, o => o.ClaimAhead = 4, s => store = s);
        dead.Script.Default = (_, _, _) => release.Task;
        await DispatchAsync(dead, jobs);
        await dead.StartAsync();

        // Two handlers run and four claimed jobs wait for a run slot; then the worker is cut off from the store, as if it had died.
        var pool = dead.Engine.Pools.Single();
        await WorkerHost.WaitUntil(() => pool.Gate.Outstanding == jobs && dead.Script.Invocations.Count == 2, TimeSpan.FromSeconds(60), "2 running, 4 waiting");
        store!.Frozen = true;
        store.HoldFinishes = true;

        // Another worker finds every lease expired, the waiting ones included, recovers the jobs and runs them.
        await using var survivor = Host(f, 4, null);
        await survivor.StartAsync();
        await WorkerHost.WaitUntil(() => SucceededAsync(f).Result == jobs, TimeSpan.FromSeconds(180), "every job recovered and settled");
        Assert.Equal(jobs, survivor.Script.Invocations.Count);

        // The dead worker comes back: by now every entry of it has lost authority (its renewals never answered). Its running handlers
        // return, and its waiting jobs never start although run slots are now free; nothing it sends changes the settled jobs (fenced).
        store.Frozen = false;
        store.HoldFinishes = false;
        release.SetResult();
        await WorkerHost.WaitUntil(() => pool.Gate.Outstanding == 0, TimeSpan.FromSeconds(60), "the dead worker retired every entry");
        Assert.Equal(2, dead.Script.Invocations.Count);
        Assert.Equal(jobs, await SucceededAsync(f));
        var model = await Model(f);
        Assert.All(model.Jobs, j => Assert.True(model.AttemptsOf(j.JobId).Count() >= 2, "the dead worker's attempt was never finished"));
        Assert.Empty(await History(f));
    }

    static void InterlockedMax(ref int target, int value)
    {
        for (var seen = Volatile.Read(ref target); value > seen; seen = Volatile.Read(ref target))
        {
            if (Interlocked.CompareExchange(ref target, value, seen) == seen)
            {
                return;
            }
        }
    }
}
