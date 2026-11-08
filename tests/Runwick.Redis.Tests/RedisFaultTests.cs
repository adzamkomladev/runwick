using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// The crash, pause and disk cases of 22 §12 (REDIS-01 kill -9 and AOF parts, REDIS-02, REDIS-03). Each case that needs Docker
/// control starts its own throwaway server (<see cref="RedisNode"/>); the others use the shared test server.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisFaultTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ------------------------------------------------------------------ helpers

    static async Task<TestClient> ClientOnAsync(RedisNode node, string name)
    {
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct);
        return await TestClient.CreateAsync(RedisServer.Prefix + node.Configuration, name + "_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r => TestJobs.Register(r));
    }

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    /// <summary>Retries while the client is still reconnecting after a crash (calls fail fast meanwhile, nothing queued).</summary>
    static async Task<T> Retry<T>(Func<Task<T>> call, int seconds = 30)
    {
        var until = DateTime.UtcNow.AddSeconds(seconds);
        while (true)
        {
            try
            {
                return await call();
            }
            catch (Exception ex) when (DateTime.UtcNow < until && ex is StoreNotAppliedException or StoreUnavailableException or RedisException
                or TimeoutException or StoreOutcomeUnknownException { Code: "commit_outcome_unknown" })
            {
                await Task.Delay(200, Ct);
            }
        }
    }

    static async Task<ConnectionMultiplexer> AdminAsync(RedisNode node) =>
        await ConnectionMultiplexer.ConnectAsync(node.Configuration + ",allowAdmin=true,abortConnect=false,connectTimeout=5000");

    static async Task<HarnessSnapshot> SnapshotAsync(RedisNode node, string app)
    {
        await using var mux = await AdminAsync(node);
        return await RedisSnapshotReader.ReadAsync(mux, app);
    }

    static Task<string> Guard(RedisNode node, string app) => node.CliAsync("hget", RxClient.Meta(app).ToString(), "guard");

    /// <summary>A raw read script call (22 §4.6): the first element is OK, or RECOVERING while an intent is pending.</summary>
    static async Task<string> RawRead(TestClient c)
    {
        var store = Store(c);
        var a = await store.Runner.Client.EvalAsync(RxLane.Background, RxScript.Read("describe"), RxClient.Meta(c.Application),
            RxClient.ReadFrame(store.Runner.StoreId), null, Ct);
        return (string)a[0]!;
    }

    static async Task<(LeaseHandle Lease, Guid JobId)> ReserveBegin(TestClient c, string id, TimeSpan? lease = null)
    {
        await c.Dispatcher.DispatchAsync(new PingJob(id), cancellationToken: Ct);
        var reserved = await Retry(() => c.ReserveAsync(lease: lease));
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var handle = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(handle)).Reply.Status);
        return (handle, handle.JobId);
    }

    // ------------------------------------------------------------------ REDIS-01: kill -9 in every interrupted state

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Kill_9_in_every_interrupted_state_of_Finish_restarts_from_the_AOF_hides_the_partial_state_and_the_resend_finishes()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis01kill");
        var store = Store(c);
        var interrupted = 0;
        var pendingStates = 0;
        var n = 1;
        for (; n <= 60; n++)
        {
            var (lease, _) = await ReserveBegin(c, "p" + n);
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            store.Runner.Client.FailAfter = n;
            StoreReply<FinishedAttempt>? first = null;
            try
            {
                first = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
            }
            catch (StoreOutcomeUnknownException)
            {
                interrupted++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            if (first is not null)
            {
                break; // n is past the last write: the call completed, every earlier prefix was interrupted
            }

            await node.CrashAndRestartAsync();
            if ((await Guard(node, c.Application)).Length > 0)
            {
                pendingStates++;
                Assert.Equal("RECOVERING", await Retry(() => RawRead(c)));
            }

            var again = await Retry(() => c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter));
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal("succeeded", again.Value!.Disposition);
            Assert.True(again.Replayed || n > 0);
            Assert.Equal("", await Guard(node, c.Application));
        }

        Assert.True(n > 5 && n < 60, $"Finish completed after {n} prefixes");
        Assert.Equal(n - 1, interrupted);
        Assert.True(pendingStates >= n - 2, $"only {pendingStates} of {interrupted} restarts found a pending intent");
        var snapshot = await SnapshotAsync(node, c.Application);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.All(snapshot.Model.Jobs, j => Assert.Equal("succeeded", j.State));
        Assert.Equal(n, snapshot.Model.Jobs.Count);
    }

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Acknowledged_dispatches_survive_kill_9_during_AOF_rewrites()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis01aof");
        // ~50 MB of unrelated keys make the rewrite take long enough to be running when the kill arrives.
        await node.CliAsync("eval", "for i=1,400000 do redis.call('SET','pad:'..i,string.rep('x',100)) end", "0");
        var acked = new List<Guid>();
        var killedMidRewrite = 0;
        for (var round = 0; round < 3; round++)
        {
            await node.CliAsync("bgrewriteaof");
            for (var i = 0; i < 25; i++)
            {
                acked.Add((await Retry(() => c.Dispatcher.DispatchAsync(new PingJob($"r{round}-{i}"), cancellationToken: Ct))).JobId.Value);
            }

            if ((await node.CliAsync("info", "persistence")).Contains("aof_rewrite_in_progress:1", StringComparison.Ordinal))
            {
                killedMidRewrite++;
            }

            await node.CrashAndRestartAsync();
            var snapshot = await SnapshotAsync(node, c.Application);
            var present = snapshot.Model.Jobs.Select(j => j.JobId).ToHashSet();
            Assert.All(acked, id => Assert.Contains(id, present));
            Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        }

        Assert.True(killedMidRewrite >= 1, "the rewrite had finished before every kill; raise the padding");
        Assert.Equal(75, acked.Count);
    }

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task A_torn_last_AOF_write_refuses_startup_until_redis_check_aof_fix_and_nothing_acknowledged_is_lost()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis01torn");
        var acked = new List<Guid>();
        for (var i = 0; i < 6; i++)
        {
            acked.Add((await c.Dispatcher.DispatchAsync(new PingJob("t" + i), cancellationToken: Ct)).JobId.Value);
        }

        await node.KillAsync();
        // The server died halfway through appending a command that was never acknowledged.
        var tail = await node.WithDataAsync(
            "f=$(ls /data/appendonlydir/*.incr.aof | tail -1); printf '*4\\r\\n$4\\r\\nHSET\\r\\n$5\\r\\nabc' >> \"$f\"; echo \"$f\"");
        Assert.Contains("incr.aof", tail, StringComparison.Ordinal);

        Assert.Equal(0, (await RedisNode.Docker("start", node.Name)).Code);
        for (var i = 0; i < 100 && await node.IsRunningAsync(); i++)
        {
            await Task.Delay(100, Ct);
        }

        Assert.False(await node.IsRunningAsync(), "a torn AOF must refuse startup (aof-load-truncated no)");
        Assert.Contains("redis-check-aof", await node.LogsAsync(), StringComparison.Ordinal);

        var fix = await node.WithDataAsync("echo y | redis-check-aof --fix /data/appendonlydir/appendonly.aof.manifest 2>&1");
        Assert.Contains("AOF", fix, StringComparison.Ordinal); // startup below proves the repair
        await node.StartAgainAsync();

        var snapshot = await SnapshotAsync(node, c.Application);
        var present = snapshot.Model.Jobs.Select(j => j.JobId).ToHashSet();
        Assert.All(acked, id => Assert.Contains(id, present));
        Assert.Equal(StoreStatus.Applied, (await Retry(() => c.ReserveAsync())).Status);
        Assert.Equal(DispatchDisposition.Created, (await Retry(() => c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct))).Disposition);
    }

    // ------------------------------------------------------------------ REDIS-02

    [Fact]
    [Trait("Case", "REDIS-02")]
    public async Task A_lost_reply_resends_to_the_same_reply_and_a_changed_body_is_a_CommandConflict()
    {
        var f = await DriverFixture.StartAsync("redis02a_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = Store(c);
        var lost = 0;
        for (var n = 1; n <= 60 && lost == 0; n++)
        {
            var (lease, _) = await ReserveBegin(c, "l" + n);
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            store.Runner.Client.FailAfter = n;
            bool threw = false;
            try
            {
                await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
            }
            catch (StoreOutcomeUnknownException)
            {
                threw = true;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            var db = (await RedisServer.AdminAsync()).GetDatabase();
            var published = threw && ((string?)await db.HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "") == "";
            if (!published)
            {
                // Finish the earlier prefix so the next iteration starts clean.
                await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
                continue;
            }

            // Every write happened and only the reply was lost: the resend returns the stored reply, and a changed body conflicts.
            var same = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
            Assert.Equal((StoreStatus.Applied, true, "succeeded"), (same.Status, same.Replayed, same.Value!.Disposition));
            var changed = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, commandId: id, notAfter: notAfter);
            Assert.Equal(StoreStatus.CommandConflict, changed.Status);
            lost++;
        }

        Assert.Equal(1, lost);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Case", "REDIS-02")]
    public async Task An_acknowledged_result_survives_kill_9_and_the_confirmation_marker_survives_a_second_crash()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis02kill");
        var (lease, jobId) = await ReserveBegin(c, "ack");
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var applied = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, applied.Status);
        var receipt = RxClient.KeyPrefix(c.Application) + "rcpt:" + RxClient.Hex(id);
        Assert.Equal("", await node.CliAsync("hget", receipt, "confirmed"));

        await node.CrashAndRestartAsync(); // kill -9 right after the acknowledgement
        var snapshot = await SnapshotAsync(node, c.Application);
        Assert.Equal("succeeded", snapshot.Model.Jobs.Single(j => j.JobId == jobId).State);

        var replay = await Retry(() => c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter));
        Assert.True(replay.Replayed);
        Assert.Equal(applied.Value, replay.Value);
        var marker = await node.CliAsync("hget", receipt, "confirmed");
        Assert.NotEqual("", marker);

        await node.CrashAndRestartAsync(); // the marker was acknowledged under always: it is in the AOF
        Assert.Equal(marker, await node.CliAsync("hget", receipt, "confirmed"));
    }

    [Fact]
    [Trait("Case", "REDIS-02")]
    public async Task A_full_disk_under_always_stops_the_server_before_replying_and_the_resend_applies_once_after_restart()
    {
        var dir = Path.Combine(Path.GetTempPath(), "g4c-diskfull-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var mount = await RedisNode.Run("mount", "-t", "tmpfs", "-o", "size=6m", "tmpfs", dir);
        Assert.True(mount.Code == 0, "disk-full needs root to mount a small tmpfs (reported as not run otherwise): " + mount.Output);
        try
        {
            await RedisNode.Run("chmod", "777", dir);
            await using var node = await RedisNode.StartAsync(dataDir: dir);
            await using var c = await ClientOnAsync(node, "redis02disk");
            var acked = new List<Guid>();
            for (var i = 0; i < 10; i++)
            {
                acked.Add((await c.Dispatcher.DispatchAsync(new PingJob("a" + i), cancellationToken: Ct)).JobId.Value);
            }

            try
            {
                await using var fill = File.Create(Path.Combine(dir, "fill"));
                var block = new byte[64 * 1024];
                while (true)
                {
                    fill.Write(block);
                    fill.Flush();
                }
            }
            catch (IOException)
            {
                // full
            }

            PreparedDispatch? failed = null;
            for (var i = 0; i < 200 && failed is null; i++)
            {
                var p = c.Prepared.Prepare(new PingJob("big" + i + new string('x', 8 * 1024)));
                try
                {
                    acked.Add((await c.Prepared.DispatchAsync(p, Ct)).JobId.Value);
                }
                catch (Exception ex) when (ex is RunwickException or StoreOutcomeUnknownException or StoreNotAppliedException)
                {
                    failed = p;
                }
            }

            Assert.NotNull(failed);
            for (var i = 0; i < 50 && await node.IsRunningAsync(); i++)
            {
                await Task.Delay(100, Ct);
            }

            Assert.False(await node.IsRunningAsync(), "appendfsync always: the server exits instead of acknowledging an unpersisted write");

            File.Delete(Path.Combine(dir, "fill")); // the operator adds capacity
            await node.StartAgainAsync();
            Assert.Equal("always", (await node.CliAsync("config", "get", "appendfsync")).Split('\n')[1]);

            var snapshot = await SnapshotAsync(node, c.Application);
            var present = snapshot.Model.Jobs.Select(j => j.JobId).ToList();
            Assert.All(acked, id => Assert.Contains(id, present));
            var resend = await Retry(() => c.Prepared.DispatchAsync(failed!, Ct));
            Assert.Equal(DispatchDisposition.Created, resend.Disposition);
            var after = await SnapshotAsync(node, c.Application);
            Assert.Equal(acked.Count + 1, after.Model.Jobs.Count);
            Assert.Equal(1, after.Model.Jobs.Count(j => j.JobId == resend.JobId.Value));
            Assert.Empty(SnapshotInvariants.Violations(after.Model));
        }
        finally
        {
            await RedisNode.Run("umount", "-l", dir);
        }
    }

    // ------------------------------------------------------------------ REDIS-03

    [Fact]
    [Trait("Case", "REDIS-03")]
    public async Task A_command_past_NotAfter_is_CommandExpired_and_resolves_as_not_applied()
    {
        var f = await DriverFixture.StartAsync("redis03a_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var id = Guid.CreateVersion7();
        var expired = await c.ReserveAsync(commandId: id, notAfter: DateTimeOffset.UtcNow.AddSeconds(-1));
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, expired.Effect);
        var resolution = await Store(c).ResolveOperationAsync(c.Application, id, DateTimeOffset.UtcNow.AddSeconds(-1), Ct);
        Assert.IsType<ReceiptResolution.NotApplied>(resolution);
        Assert.All((await f.ReadCommittedSnapshot()).Model.Jobs, j => Assert.Equal("pending", j.State));
    }

    [Fact]
    [Trait("Case", "REDIS-03")]
    public async Task A_command_run_by_a_paused_server_after_NotAfter_is_refused_by_server_time_and_so_is_the_resend()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis03pause");
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(2);

        await node.PauseAsync(); // TCP still accepts: the command bytes wait in the socket buffer
        var call = Task.Run(async () =>
        {
            try
            {
                return (StoreReply<ReservedJob>?)await c.ReserveAsync(commandId: id, notAfter: notAfter);
            }
            catch (StoreOutcomeUnknownException)
            {
                return null;
            }
        }, Ct);
        await Task.Delay(TimeSpan.FromSeconds(3), Ct); // past NotAfter by server time
        await node.UnpauseAsync(); // the server now runs the stale bytes
        var reply = await call;
        if (reply is not null)
        {
            Assert.Equal((StoreStatus.CommandExpired, StoreEffect.ObservedNoEffect), (reply.Status, reply.Effect));
        }

        await Task.Delay(300, Ct);
        var resolution = await Retry(() => Store(c).ResolveOperationAsync(c.Application, id, notAfter, Ct));
        Assert.IsType<ReceiptResolution.NotApplied>(resolution);
        var again = await Retry(() => c.ReserveAsync(commandId: id, notAfter: notAfter));
        Assert.Equal(StoreStatus.CommandExpired, again.Status);
        var snapshot = await SnapshotAsync(node, c.Application);
        Assert.All(snapshot.Model.Jobs, j => Assert.Equal("pending", j.State));
    }

    [Fact]
    [Trait("Case", "REDIS-03")]
    public async Task While_the_server_is_down_calls_fail_fast_and_nothing_runs_late_after_the_restart()
    {
        await using var node = await RedisNode.StartAsync();
        await using var c = await ClientOnAsync(node, "redis03down");
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        await node.KillAsync();
        await Task.Delay(500, Ct);

        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(2);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(() => c.ReserveAsync(commandId: id, notAfter: notAfter));
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(3), $"failed after {clock.Elapsed}");

        await node.StartAgainAsync();
        await Task.Delay(TimeSpan.FromSeconds(3), Ct); // a queued command would have been flushed by now
        var snapshot = await SnapshotAsync(node, c.Application);
        Assert.All(snapshot.Model.Jobs, j => Assert.Equal("pending", j.State));
        var resolution = await Retry(() => Store(c).ResolveOperationAsync(c.Application, id, notAfter, Ct));
        Assert.True(resolution is ReceiptResolution.NotApplied or ReceiptResolution.NotObserved, resolution.ToString());
        Assert.Equal(StoreStatus.CommandExpired, (await Retry(() => c.ReserveAsync(commandId: id, notAfter: notAfter))).Status);
    }

    [Fact]
    [Trait("Case", "REDIS-03")]
    public async Task A_Begin_published_after_its_lease_expired_cannot_start_a_handler_and_Recover_moves_the_job_on()
    {
        var f = await DriverFixture.StartAsync("redis03d_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = Store(c);
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var reserved = await c.ReserveAsync(lease: TimeSpan.FromSeconds(1));
        var lease = reserved.Value!.Lease.Handle;

        var beginId = Guid.CreateVersion7();
        var beginNotAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        store.Runner.Client.FailAfter = 1; // the intent is accepted while the lease is valid, but nothing is projected
        try
        {
            await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.BeginAsync(lease, beginId, beginNotAfter));
        }
        finally
        {
            store.Runner.Client.FailAfter = 0;
        }

        await Task.Delay(1300, Ct); // the lease expires while the intent is pending
        var published = await c.BeginAsync(lease, beginId, beginNotAfter); // recovery publishes the historical decision
        Assert.Equal(StoreStatus.Applied, published.Reply.Status);

        // Before user code the worker takes a fresh ownership-checked Renew: the expired lease refuses it, so no handler starts.
        var renew = await c.RenewAsync(lease);
        Assert.NotEqual(StoreStatus.Applied, renew.Status);
        Assert.Equal("lease_expired", renew.Error!.Code);

        var expired = Assert.Single(await c.Driver.ListExpiredLeasesAsync(c.Application, 10, Ct));
        Assert.Equal(StoreStatus.Applied, (await c.RecoverAsync(expired)).Status);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
