using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Sqlite.Store;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// FM-08, FM-09, FM-10 and RAB-10 (PF-06): the worker engine against the batched store operations: it batches queued Finishes into FinishMany, resolves
/// a lost batch reply item by item, and falls back to single operations when the store refuses a batch as UnsupportedVersion. Ported from the
/// PostgreSQL suite. PostgreSQL slows the client's first Finishes at the gate and hides a server function; SQLite holds its writer at the commit point and
/// hides the operation with a trigger that fails as a missing table (what its runner maps to UnsupportedVersion); Redis pauses writes for a second and uses
/// its test seams for a lost reply and a refused operation (it has no store fault points, 22 §12).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
[Collection(nameof(RedisPauseCollection))]
public sealed class WorkerBatchContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    /// <summary>The double in front of the worker's store (SQLite and Redis); PostgreSQL's cases use its real fault points and a real hidden function.</summary>
    sealed class Doubles
    {
        public BatchStoreDouble? Store;
    }

    static WorkerHost Host(DriverFixture f, int concurrency, Doubles? doubles = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, overrides: doubles is null || f.Kind == StoreKind.Postgres ? null :
            s => s.AddSingleton<IRunwickStore>(sp => doubles.Store = new BatchStoreDouble(f.Kind == StoreKind.Sqlite
                ? sp.GetRequiredService<SqliteStore>() : sp.GetRequiredService<RedisStore>())));

    static Task<long> FinishReceipts(DriverFixture f) => Model(f).ContinueWith(m => (long)m.Result.Receipts.Count(r => r.Operation == "Finish"), TaskScheduler.Default);

    /// <summary>Makes the settlement path busy so Finishes queue behind it (PostgreSQL/SQLite arm now; Redis pauses once handlers are running).</summary>
    static void BusyLane(DriverFixture f)
    {
        if (f.Kind == StoreKind.Postgres)
        {
            f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Sleep(TimeSpan.FromMilliseconds(1500)), FaultTarget.Op(Operations.Finish), times: 2);
        }
        else if (f.Kind == StoreKind.Sqlite)
        {
            f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Sleep(TimeSpan.FromMilliseconds(1000)), FaultTarget.Op(Operations.Finish), times: 1);
        }
    }

    static async Task PauseOnceHandlersRunAsync(DriverFixture f, WorkerHost w)
    {
        if (f.Kind == StoreKind.Redis)
        {
            await WorkerHost.WaitUntil(() => w.Script.Invocations.Count >= 2, TimeSpan.FromSeconds(60), "two handlers running");
            var mux = await RedisServer.AdminAsync();
            await mux.GetServer(mux.GetEndPoints()[0]).ExecuteAsync("CLIENT", "PAUSE", 1000, "WRITE");
        }
    }

    static async Task RunJobsAsync(DriverFixture f, WorkerHost w, int jobs, bool start = true)
    {
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);
        if (start)
        {
            await w.StartAsync();
        }

        await PauseOnceHandlersRunAsync(f, w);
        await WorkerHost.WaitUntil(() => Model(f).Result.Jobs.Count(j => j.State == "succeeded") == jobs, TimeSpan.FromSeconds(90), "every job settled");
        Assert.Equal(jobs, w.Script.Invocations.Count);
        Assert.Equal(jobs, await FinishReceipts(f));
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));

        // The held lane makes control waits long, so the pressure gate may stop admission for a while (05 §15); it
        // reopens by itself once the lane is healthy. No admission close is latched.
        await WorkerHost.WaitUntil(() => w.Engine.AdmissionOpen, TimeSpan.FromSeconds(30), "admission open again");
        Assert.DoesNotContain(w.Engine.Health().Diagnostics, d => d.Contains("admission closed", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-08")]
    public async Task Finishes_that_queue_behind_a_busy_lane_are_sent_in_one_call(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm08"), kind);
        await using var w = Host(f, concurrency: 16);
        BusyLane(f);
        await RunJobsAsync(f, w, 40);
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchesApplied) > 0);
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchedItemsSettled) >= 2);
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-09")]
    public async Task A_lost_batch_reply_in_the_worker_resolves_each_finish_by_its_receipt(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm09"), kind);
        var doubles = new Doubles();
        await using var w = Host(f, concurrency: 16, kind == StoreKind.Redis ? doubles : null);
        BusyLane(f);
        // PostgreSQL and SQLite drop the first FinishMany reply after its commit (the fault point). Redis has no store fault points, so its double
        // lets the real store apply the first FinishMany and then loses the reply.
        var drop = kind == StoreKind.Redis ? null : f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.FinishMany));
        doubles.Store?.LoseNextFinishMany();

        await RunJobsAsync(f, w, 24);
        Assert.Equal(1, drop?.Fired ?? doubles.Store!.RepliesLost);

        // The dropped call committed: its items were resolved by Finish/1 replays of their own receipts, not reapplied.
        var model = await Model(f);
        Assert.DoesNotContain(model.Jobs, j => model.AttemptsOf(j.JobId).Count() > 1);
        Assert.True(Interlocked.Read(ref w.Engine.Completion.BatchesSent) > Interlocked.Read(ref w.Engine.Completion.BatchesApplied));
        Assert.Empty(await History(f));
    }

    /// <summary>After the worker started: the store now refuses <paramref name="operation"/> as UnsupportedVersion (PostgreSQL: a missing function; the others: the double).</summary>
    static async Task HideOperationAsync(DriverFixture f, Doubles doubles, string operation)
    {
        if (f.Kind == StoreKind.Postgres)
        {
            await f.ExecAsync(operation == Operations.FinishMany
                ? "ALTER FUNCTION runwick.finish_many_v2 RENAME TO finish_many_v2_hidden"
                : "ALTER FUNCTION runwick.reserve_and_begin_v2 RENAME TO reserve_and_begin_v2_hidden");
        }
        else
        {
            doubles.Store!.Refused.Add(operation);
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FM-10")]
    public async Task A_refused_batch_turns_batching_off_and_every_finish_settles_alone(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("fm10"), kind);
        var doubles = new Doubles();
        await using var w = Host(f, concurrency: 16, doubles);
        await w.StartAsync();

        // After startup's schema check, before any finish: the store now refuses the batched finish.
        await HideOperationAsync(f, doubles, Operations.FinishMany);
        BusyLane(f);
        await RunJobsAsync(f, w, 24, start: false);
        var completion = w.Engine.Completion;
        Assert.True(Interlocked.Read(ref completion.BatchesSent) > 0);
        Assert.Equal(0, Interlocked.Read(ref completion.BatchesApplied));
        Assert.True(w.Engine.Health().Diagnostics.Any(d => d.Contains("FinishMany returned UnsupportedVersion", StringComparison.Ordinal)),
            $"last batch failure: {completion.LastBatchFailure}");
        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-10")]
    public async Task A_non_applied_batch_reply_falls_back_to_reserve_and_begin(StoreKind kind)
    {
        // Any reply other than Applied or a Reserve-like no-effect (here: the operation refused as UnsupportedVersion) turns the pool to
        // Reserve then Begin; admission stays open.
        await using var f = await DriverFixture.StartAsync(Ns("rab10"), kind);
        var doubles = new Doubles();
        await using var w = Host(f, concurrency: 8, doubles);
        await w.StartAsync();

        // After startup's schema check, before any claim: the store now refuses the batched claim.
        await HideOperationAsync(f, doubles, Operations.ReserveAndBegin);
        const int jobs = 12;
        await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, jobs).Select(n => new BatchDispatchItem(new WorkJob("j" + n)))]);
        await WorkerHost.WaitUntil(() => Model(f).Result.Jobs.Count(j => j.State == "succeeded") == jobs, TimeSpan.FromSeconds(90),
            "every job settled through Reserve and Begin");

        var pool = w.Engine.Pools.Single();
        var model = await Model(f);
        Assert.Equal(0, Interlocked.Read(ref pool.BatchClaims));
        Assert.DoesNotContain(model.Receipts, r => r.Operation == "ReserveAndBegin");
        Assert.True(model.Receipts.Count(r => r.Operation == "Begin") >= jobs);
        Assert.True(w.Engine.AdmissionOpen);
        Assert.Contains(w.Engine.Health().Diagnostics, d => d.Contains("ReserveAndBegin returned UnsupportedVersion", StringComparison.Ordinal));
        Assert.Empty(await History(f));
    }
}
