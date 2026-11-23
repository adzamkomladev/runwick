using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Sqlite.Store;

namespace Runwick.Sqlite.Tests;

/// <summary>
/// <see cref="SqliteOptions.GroupCommit"/> (20 §5.1): queued writes of one call class share one transaction, each under its own
/// savepoint, and every reply follows the shared commit.
/// </summary>
[Trait("Category", "Contract")]
public sealed class SqliteGroupCommitTests
{
    sealed class Setup(DriverFixture fixture, TestClient client, SqExecutor executor, SqRunner runner, IReadOnlyList<LeaseHandle> leases)
    {
        public DriverFixture Fixture => fixture;
        public TestClient Client => client;
        public SqExecutor Executor => executor;
        public SqRunner Runner => runner;
        public IReadOnlyList<LeaseHandle> Leases => leases;

        /// <summary>Holds the writer thread with a plain write until the returned handle is set.</summary>
        public async Task<(Task Held, ManualResetEventSlim Release)> HoldWriter(CancellationToken ct)
        {
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new ManualResetEventSlim();
            var held = executor.WriteAsync(SqLane.Background, _ =>
            {
                entered.SetResult();
                release.Wait(ct);
                return true;
            }, ct);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            return (held, release);
        }

        public async Task WaitQueued(int count, CancellationToken ct)
        {
            for (var i = 0; executor.QueuedWrites < count; i++)
            {
                Assert.True(i < 500, $"expected {count} queued writes, saw {executor.QueuedWrites}");
                await Task.Delay(10, ct);
            }
        }

        public Task<long> JobsIn(string state, params LeaseHandle[] of) => Count(fixture.SqlitePath, state, of);

        public static Task<long> Count(string path, string state, params LeaseHandle[] of) => Task.FromResult(of.Sum(l =>
            SqliteSnapshotReader.Count(path, "SELECT count(*) FROM runwick_jobs WHERE job_id = @id AND state = @state", ("id", l.JobId), ("state", state))));

        public long Receipts(string path, params Guid[] ids) =>
            ids.Sum(id => SqliteSnapshotReader.Count(path, "SELECT count(*) FROM runwick_storage_commands WHERE command_id = @id", ("id", id)));
    }

    static async Task<Setup> Start(string name, int jobs, CancellationToken ct)
    {
        var fixture = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var client = await fixture.OpenClient(configure: r => TestJobs.Register(r));
        client.Services.GetRequiredService<SqliteStoreRegistration>().Options.GroupCommit = true;
        for (var i = 0; i < jobs; i++)
        {
            await client.Dispatcher.DispatchAsync(new PingJob("job-" + i), cancellationToken: ct);
        }

        var claimed = await client.ReserveAndBeginAsync(jobs);
        var leases = claimed.Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle).ToList();
        Assert.Equal(jobs, leases.Count);
        return new Setup(fixture, client, client.Services.GetRequiredService<SqExecutor>(), client.Services.GetRequiredService<SqRunner>(), leases);
    }

    [Fact]
    public async Task A_mixed_group_commits_once_and_a_failed_member_rolls_back_alone()
    {
        var ct = TestContext.Current.CancellationToken;
        var s = await Start("group_mixed", 3, ct);
        await using var _ = s.Fixture;
        var c = s.Client;
        var finish = c.FinishCommand(s.Leases[0], AttemptOutcome.Succeeded);
        var faulted = c.FinishCommand(s.Leases[1], AttemptOutcome.Succeeded);
        var many = c.FinishCommand(s.Leases[2], AttemptOutcome.Succeeded);
        s.Fixture.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), FaultTarget.Command(faulted.CommandId));
        var commits = s.Runner.Commits;

        var (held, release) = await s.HoldWriter(ct);
        Task<StoreReply<FinishedAttempt>> first, failing;
        Task<StoreReply<FinishedMany>> batch;
        try
        {
            first = c.FinishAsync(finish);
            failing = c.FinishAsync(faulted);
            batch = c.FinishManyAsync([many]);
            await s.WaitQueued(3, ct);
        }
        finally
        {
            release.Set();
        }

        await held;
        Assert.Equal(StoreStatus.Applied, (await first).Status);
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => failing);
        Assert.Equal(StoreStatus.Applied, (await batch).Status);
        Assert.Equal(commits + 1, s.Runner.Commits);

        Assert.Equal(2, await s.JobsIn("succeeded", s.Leases[0], s.Leases[2]));
        Assert.Equal(1, await s.JobsIn("running", s.Leases[1]));
        Assert.Equal(0, s.Receipts(s.Fixture.SqlitePath, faulted.CommandId));
        var resent = await c.FinishAsync(faulted);
        Assert.Equal(StoreStatus.Applied, resent.Status);
        Assert.False(resent.Replayed);
        Assert.Empty(await s.Fixture.HistoryViolations());
    }

    [Fact]
    public async Task A_member_cancelled_after_the_group_took_it_is_not_applied_and_the_rest_commit()
    {
        var ct = TestContext.Current.CancellationToken;
        var s = await Start("group_cancel", 3, ct);
        await using var _ = s.Fixture;
        var c = s.Client;
        var head = c.FinishCommand(s.Leases[0], AttemptOutcome.Succeeded);
        var cancelled = c.FinishCommand(s.Leases[1], AttemptOutcome.Succeeded);
        var tail = c.FinishCommand(s.Leases[2], AttemptOutcome.Succeeded);
        var atHead = s.Fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(head.CommandId));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var (held, release) = await s.HoldWriter(ct);
        Task<StoreReply<FinishedAttempt>> first, second, third;
        try
        {
            first = c.Driver.FinishAsync(head, ct);
            second = c.Driver.FinishAsync(cancelled, cancel.Token);
            third = c.Driver.FinishAsync(tail, ct);
            await s.WaitQueued(3, ct);
        }
        finally
        {
            release.Set();
        }

        await held;
        try
        {
            // The group has taken all three off the queue and is inside the head's savepoint.
            await DriverFixture.WaitAtBarrier(atHead);
            Assert.Equal(0, s.Executor.QueuedWrites);
            await cancel.CancelAsync();
            await Assert.ThrowsAsync<StoreNotAppliedException>(() => second);
        }
        finally
        {
            DriverFixture.ReleaseBarrier(atHead);
        }

        Assert.Equal(StoreStatus.Applied, (await first).Status);
        Assert.Equal(StoreStatus.Applied, (await third).Status);
        Assert.Equal(1, await s.JobsIn("running", s.Leases[1]));
        Assert.Equal(0, s.Receipts(s.Fixture.SqlitePath, cancelled.CommandId));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(cancelled)).Status);
    }

    [Fact]
    public async Task A_crash_image_taken_mid_group_has_every_replied_write_and_no_unreplied_one()
    {
        var ct = TestContext.Current.CancellationToken;
        var s = await Start("group_crash", 3, ct);
        await using var _ = s.Fixture;
        var c = s.Client;

        // Replied before the group: must survive the crash.
        var replied = c.FinishCommand(s.Leases[0], AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(replied)).Status);

        var released = c.FinishCommand(s.Leases[1], AttemptOutcome.Succeeded);
        var last = c.FinishCommand(s.Leases[2], AttemptOutcome.Succeeded);
        var atLast = s.Fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(last.CommandId));
        var (held, release) = await s.HoldWriter(ct);
        Task<StoreReply<FinishedAttempt>> first, second;
        try
        {
            first = c.FinishAsync(released);
            second = c.FinishAsync(last);
            await s.WaitQueued(2, ct);
        }
        finally
        {
            release.Set();
        }

        await held;
        var image = Path.Combine(Path.GetTempPath(), "runwick-group-crash-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(image);
        try
        {
            try
            {
                // The first member's savepoint is released into the open transaction; the second is at BeforeCommit. The files on
                // disk now are what a kill -9 would leave: copy them without the -shm, so opening the copy runs WAL recovery.
                await DriverFixture.WaitAtBarrier(atLast);
                Assert.False(first.IsCompleted);
                Assert.False(second.IsCompleted);
                File.Copy(s.Fixture.SqlitePath, Path.Combine(image, "runwick.db"));
                if (File.Exists(s.Fixture.SqlitePath + "-wal"))
                {
                    File.Copy(s.Fixture.SqlitePath + "-wal", Path.Combine(image, "runwick.db-wal"));
                }
            }
            finally
            {
                DriverFixture.ReleaseBarrier(atLast);
            }

            var crashed = Path.Combine(image, "runwick.db");
            Assert.Equal(1, await Setup.Count(crashed, "succeeded", s.Leases[0]));
            Assert.Equal(1, s.Receipts(crashed, replied.CommandId));
            Assert.Equal(2, await Setup.Count(crashed, "running", s.Leases[1], s.Leases[2]));
            Assert.Equal(0, s.Receipts(crashed, released.CommandId, last.CommandId));
            Assert.Equal(0, SqliteSnapshotReader.Count(crashed, "SELECT count(*) FROM runwick_storage_commands WHERE result IS NULL"));
        }
        finally
        {
            Directory.Delete(image, recursive: true);
        }

        Assert.Equal(StoreStatus.Applied, (await first).Status);
        Assert.Equal(StoreStatus.Applied, (await second).Status);
        Assert.Equal(3, await s.JobsIn("succeeded", [.. s.Leases]));
    }

    [Fact]
    public async Task A_renewal_that_arrives_mid_group_runs_before_the_rest_of_the_group()
    {
        var ct = TestContext.Current.CancellationToken;
        var s = await Start("group_priority", 5, ct);
        await using var _ = s.Fixture;
        var c = s.Client;

        // Four settlement writes (the class's slot count); the first and last hold the writer at BeforeCommit.
        var finishes = s.Leases.Take(4).Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var atFirst = s.Fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(finishes[0].CommandId));
        var atLast = s.Fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(finishes[3].CommandId));
        var (held, release) = await s.HoldWriter(ct);
        List<Task<StoreReply<FinishedAttempt>>> calls;
        try
        {
            calls = [.. finishes.Select(f => c.FinishAsync(f))];
            await s.WaitQueued(4, ct);
        }
        finally
        {
            release.Set();
        }

        await held;
        Task<StoreReply<RenewedLease>> renewal;
        try
        {
            await DriverFixture.WaitAtBarrier(atFirst);
            renewal = c.RenewAsync(s.Leases[4]);
            await s.WaitQueued(1, ct);
        }
        finally
        {
            DriverFixture.ReleaseBarrier(atFirst);
        }

        try
        {
            // The group commits the first finish and stops; the renewal goes next, while the rest of the group waits at the
            // last finish's barrier.
            Assert.Equal(StoreStatus.Applied, (await renewal.WaitAsync(TimeSpan.FromSeconds(10), ct)).Status);
            Assert.Equal(StoreStatus.Applied, (await calls[0]).Status);
            await DriverFixture.WaitAtBarrier(atLast);
            Assert.All(calls.Skip(1), call => Assert.False(call.IsCompleted));
        }
        finally
        {
            DriverFixture.ReleaseBarrier(atLast);
        }

        foreach (var call in calls)
        {
            Assert.Equal(StoreStatus.Applied, (await call).Status);
        }

        Assert.Equal(4, await s.JobsIn("succeeded", [.. s.Leases.Take(4)]));
    }
}
