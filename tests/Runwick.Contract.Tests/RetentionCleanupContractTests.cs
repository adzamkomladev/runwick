using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.RetentionContractTests;

namespace Runwick.Contract.Tests;

/// <summary>
/// Cleanup batches under concurrency, failure and drain (17 §8: CLN-01, CLN-03, CLN-04) on SQLite and Redis, ported from
/// Runwick.Postgres.Tests.RetentionTests (PostgreSQL keeps its own forms: row locks, lock_timeout). On SQLite and Redis one writer
/// serializes a batch with live work and with other batches, so there is nothing to skip: a batch never reports <c>skipped_locked</c>
/// (20 §1, 22 §4.2).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class RetentionCleanupContractTests
{
    public static TheoryData<StoreKind> NonPostgres => UpgradeContractTests.NonPostgresKinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static Task<CleanupBatchResult> Batch(TestClient c, int rows) =>
        Retention(c).RunCleanupBatchAsync(c.Application, RecordClasses.JobDetailFailed, new CleanupLimits(rows, 1 << 20, TimeSpan.FromSeconds(5)), CancellationToken.None);

    static async Task<RetentionClassProgress> Progress(TestClient c) =>
        (await Retention(c).GetRetentionAsync(c.Application, Ct)).Value.Classes.Single(p => p.RecordClass == RecordClasses.JobDetailFailed);

    [Theory]
    [MemberData(nameof(NonPostgres))]
    [Trait("Case", "CLN-01")]
    public async Task Cln01_two_processes_share_a_class_and_their_batches_add_up(StoreKind kind)
    {
        var (f, first) = await OpenAsync(kind, "cln01");
        await using var _ = f;
        var jobs = new List<Guid>();
        for (var i = 0; i < 60; i++)
        {
            jobs.Add(await TerminalAsync(first, new PingJob("j" + i)));
        }

        await AgeJobsAsync(f, TimeSpan.FromDays(31));
        var second = await f.OpenClient("local", r => TestJobs.Register(r));

        // Both processes work the same class at once, in small batches: every batch is "done" (never skipped_locked), the rows they
        // delete add up to the 60 with none deleted twice or missed.
        var counted = await Task.WhenAll(new[] { first, second }.Select(c => Task.Run(async () =>
        {
            var rows = 0;
            for (var i = 0; i < 40; i++)
            {
                var r = await Batch(c, 7);
                Assert.Equal("done", r.Status);
                rows += r.Rows;
            }

            return rows;
        }, Ct)));

        Assert.Equal(60, counted.Sum());
        foreach (var job in jobs)
        {
            Assert.Equal(1, await Expired(f, job));
        }

        Assert.Equal(60, (await Progress(first)).TotalRows);
    }

    [Theory]
    [MemberData(nameof(NonPostgres))]
    [Trait("Case", "CLN-03")]
    public async Task Cln03_a_failed_batch_records_its_error_and_the_next_resumes_without_double_counting(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "cln03");
        await using var _ = f;
        var jobs = new List<Guid>();
        for (var i = 0; i < 6; i++)
        {
            jobs.Add(await TerminalAsync(c, new PingJob("j" + i)));
        }

        await AgeJobsAsync(f, TimeSpan.FromDays(31));
        Assert.Equal(3, (await Batch(c, 3)).Rows);
        var expiredBefore = 0L;
        foreach (var job in jobs)
        {
            expiredBefore += await Expired(f, job);
        }

        Assert.Equal(3, expiredBefore);

        // The next batch fails: on SQLite at its commit, which rolls it back whole; on Redis its reply is lost after the script ran
        // (the unknown outcome), so the batch stands and the failure is only recorded.
        var fault = kind == StoreKind.Redis
            ? f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.CleanupBatch))
            : f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Throw(), FaultTarget.Op(Operations.CleanupBatch));
        var failed = await Batch(c, 3);
        Assert.True(fault.Fired >= 1);
        Assert.Equal("failed", failed.Status);
        Assert.NotNull(failed.ErrorCode);
        var progress = await Progress(c);
        Assert.Equal(failed.ErrorCode, progress.LastErrorCode);
        Assert.Equal(1, progress.TodayFailures);
        if (kind == StoreKind.Sqlite)
        {
            // The failed batch rolled back whole.
            var expired = 0L;
            foreach (var job in jobs)
            {
                expired += await Expired(f, job);
            }

            Assert.Equal(3, expired);
        }

        // The class finishes from where it was: every job's detail is expired once, and the total does not double count.
        for (var i = 0; i < 5 && !(await Batch(c, 3)).PassComplete; i++)
        {
        }

        foreach (var job in jobs)
        {
            Assert.Equal(1, await Expired(f, job));
        }

        Assert.Equal(6, (await Progress(c)).TotalRows);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CLN-04")]
    public async Task Cln04_no_batch_starts_once_the_worker_drains(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("cln04_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.CleanupBatches) >= RecordClasses.All.Count, TimeSpan.FromSeconds(60), "one batch per class");
        await w.StopAsync();
        var after = Interlocked.Read(ref w.Engine.CleanupBatches);
        await Task.Delay(1500, Ct);
        Assert.Equal(after, Interlocked.Read(ref w.Engine.CleanupBatches));
        Assert.Equal(0, Interlocked.Read(ref w.Engine.CleanupFailures));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var view = await Retention(c).GetRetentionAsync(c.Application, Ct);
        Assert.Equal(RecordClasses.All.Count, view.Value.Classes.Count(p => p.LastBatchAt is not null));
    }
}
