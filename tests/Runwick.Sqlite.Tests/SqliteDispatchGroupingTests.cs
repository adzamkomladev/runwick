using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Sqlite.Store;

namespace Runwick.Sqlite.Tests;

[Trait("Category", "Contract")]
public sealed class SqliteDispatchGroupingTests
{
    static StoreCommand<DispatchRequest> Command(TestClient client, IQueueJob job, DispatchOptions? options = null)
    {
        var preparer = client.Services.GetRequiredService<DispatchPreparer>();
        var prepared = preparer.Prepare(client.Runtime.StoreId, job, options);
        var item = new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) };
        return client.Command(new DispatchRequest(item, client.Runtime.StoreId, prepared.Digests.Command), prepared.OperationId.Value, prepared.NotAfter);
    }

    [Fact]
    public async Task Queued_dispatches_share_a_commit_with_independent_results_and_receipts()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await DriverFixture.StartAsync("dispatch_group_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var client = await fixture.OpenClient(configure: r => TestJobs.Register(r));
        var runner = client.Services.GetRequiredService<SqRunner>();

        var commits = runner.Commits;
        var blocker = Command(client, new PingJob("blocker"));
        var holdWriter = fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(blocker.CommandId));

        var window = new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "group-window", TimeSpan.FromHours(1)) };
        var created = Command(client, new KeyedJob("first", "first"), window);
        var conflict = Command(client, new KeyedJob("other", "other"), window);
        var invalid = Command(client, new PingJob("invalid"));
        invalid = invalid with { Body = invalid.Body with { Item = invalid.Body.Item with { Intent = invalid.Body.Item.Intent with { Queue = "missing" } } } };
        var expired = Command(client, new PingJob("expired")) with { NotAfter = DateTimeOffset.UtcNow.AddSeconds(-1) };
        var faulted = Command(client, new KeyedJob("fault", "fault"));
        var lost = Command(client, new PingJob("lost"));
        var cancelled = Command(client, new PingJob("cancelled"));
        fixture.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), FaultTarget.Command(faulted.CommandId));
        fixture.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(lost.CommandId));
        var holdGroup = fixture.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(lost.CommandId));

        var calls = new List<Task<StoreReply<DispatchResult>>>();
        var blockingCall = client.Driver.DispatchAsync(blocker, ct);
        try
        {
            await DriverFixture.WaitAtBarrier(holdWriter);
            using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var cancelledCall = client.Driver.DispatchAsync(cancelled, cancel.Token);
            cancel.Cancel();
            await Assert.ThrowsAsync<StoreNotAppliedException>(() => cancelledCall);

            // All seven entries reach the writer queue in order while the first command holds the writer.
            foreach (var command in new[] { created, created, conflict, invalid, expired, faulted, lost })
            {
                calls.Add(client.Driver.DispatchAsync(command, ct));
            }
        }
        finally
        {
            DriverFixture.ReleaseBarrier(holdWriter);
        }

        try
        {
            await DriverFixture.WaitAtBarrier(holdGroup);
            Assert.Equal(StoreStatus.Applied, (await blockingCall).Status);
            Assert.All(calls, call => Assert.False(call.IsCompleted));
            Assert.Equal(1, await fixture.CountAsync("SELECT count(*) FROM runwick_jobs"));
        }
        finally
        {
            DriverFixture.ReleaseBarrier(holdGroup);
        }

        var first = await calls[0];
        var replay = await calls[1];
        Assert.Equal(created.Body.Item.Intent.ProposedJobId, first.Value!.JobId);
        Assert.Equal(first.Value, replay.Value);
        Assert.Equal(first.DecidedAt, replay.DecidedAt);
        Assert.True(replay.Replayed);
        Assert.Equal(StoreStatus.UniquenessConflict, (await calls[2]).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await calls[3]).Status);
        Assert.Equal(StoreStatus.CommandExpired, (await calls[4]).Status);
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => calls[5]);
        var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => calls[6]);
        Assert.Equal(lost.CommandId, unknown.CommandId);
        Assert.Equal(commits + 2, runner.Commits);

        var resolved = await client.Driver.DispatchAsync(lost, ct);
        Assert.True(resolved.Replayed);
        Assert.Equal(lost.Body.Item.Intent.ProposedJobId, resolved.Value!.JobId);
        Assert.Equal(3, await fixture.CountAsync("SELECT count(*) FROM runwick_jobs"));
        Assert.Equal(2, await fixture.CountAsync("SELECT count(*) FROM runwick_dispatch_keys"));
        Assert.Equal(0, await fixture.CountAsync("SELECT count(*) FROM runwick_storage_commands WHERE result IS NULL"));
        foreach (var command in new[] { conflict, invalid, expired, faulted, cancelled })
        {
            Assert.Equal(0, await fixture.CountAsync("SELECT count(*) FROM runwick_storage_commands WHERE command_id = @id", ("id", command.CommandId)));
        }
    }

    [Fact]
    public async Task A_failed_shared_commit_rolls_back_both_commands_and_reports_their_own_ids()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var fixture = await DriverFixture.StartAsync("dispatch_group_failure_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var client = await fixture.OpenClient(configure: r => TestJobs.Register(r));
        var executor = client.Services.GetRequiredService<SqExecutor>();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var first = Command(client, new PingJob("first"));
        var second = Command(client, new PingJob("second"));
        using var admin = new SqliteConnection($"Data Source={fixture.SqlitePath};Pooling=False");
        admin.Open();
        var blocker = executor.WriteAsync(SqLane.Producer, _ =>
        {
            entered.SetResult();
            release.Wait(ct);
            return true;
        }, ct);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            SqFile.Exec(admin, $"""
                CREATE TABLE dispatch_test_parent (id INTEGER PRIMARY KEY);
                CREATE TABLE dispatch_test_child (id INTEGER REFERENCES dispatch_test_parent (id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER dispatch_test_commit AFTER INSERT ON runwick_queue_events
                WHEN NEW.command_id = X'{Convert.ToHexString(SqUuid.Bytes(second.CommandId))}'
                BEGIN INSERT INTO dispatch_test_child VALUES (1); END;
                """);
            var firstCall = client.Driver.DispatchAsync(first, ct);
            var secondCall = client.Driver.DispatchAsync(second, ct);
            release.Set();
            Assert.Equal(first.CommandId, (await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => firstCall)).CommandId);
            Assert.Equal(second.CommandId, (await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => secondCall)).CommandId);
        }
        finally
        {
            release.Set();
            await blocker;
        }

        Assert.Equal(0, await fixture.CountAsync("SELECT count(*) FROM runwick_jobs"));
        Assert.Equal(0, await fixture.CountAsync("SELECT count(*) FROM runwick_storage_commands WHERE command_id IN (@first, @second)",
            ("first", first.CommandId), ("second", second.CommandId)));
        SqFile.Exec(admin, "DROP TRIGGER dispatch_test_commit");
        foreach (var command in new[] { first, second })
        {
            var reply = await client.Driver.DispatchAsync(command, ct);
            Assert.False(reply.Replayed);
            Assert.Equal(command.Body.Item.Intent.ProposedJobId, reply.Value!.JobId);
        }
    }
}
