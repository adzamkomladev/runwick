using System.Diagnostics;
using Runwick.Redis.Prototype;

namespace Runwick.Redis.Prototype.Tests;

/// <summary>REDIS-02 persistence error / reply loss; REDIS-03 expired commands and reconnect.</summary>
public sealed class Redis02And03Tests(ITestOutputHelper output)
{
    private static StoreCommand Dispatch(string id, int payloadBytes = 1) =>
        StoreCommand.Create("dispatch", TimeSpan.FromSeconds(60), id, "default", new string('x', payloadBytes), "3", "");

    [Fact]
    public async Task REDIS02_lost_reply_is_resolved_by_the_same_command_and_a_changed_body_conflicts()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("reply");
        await store.InitAsync();
        var c = Dispatch("j1");

        // Every write committed and published; only the reply is lost.
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(c, failAfter: 9));
        Assert.Equal("Recorded", await store.ResolveAsync(c, 86_400_000));
        var replay = await store.ExecuteAsync(c);
        Assert.True(replay.Replayed);
        Assert.Equal("j1", replay.Json.GetProperty("jobId").GetString());

        var changed = c with { Args = ["j1", "default", "other", "3", ""] };
        var conflict = await store.ExecuteAsync(changed);
        Assert.Equal(("NOEFFECT", "CommandConflict"), (conflict.Kind, conflict.Status));
        Assert.Equal(1, (await store.ReadSummaryAsync()).Counter("created"));
    }

    [Fact]
    public async Task REDIS02_acknowledged_result_survives_kill_and_replay_is_confirmed_by_a_fresh_fsynced_write()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("confirm");
        await store.InitAsync();
        var c = Dispatch("j1");
        var applied = await store.ExecuteAsync(c);

        await node.CrashAndRestartAsync(); // kill -9 right after the acknowledgement

        var replay = await store.ExecuteAsync(c);
        Assert.True(replay.Replayed);
        Assert.Equal(applied.Result, replay.Result);
        Assert.Equal("CONFIRMED", await store.ConfirmAsync(c.Id));

        await node.CrashAndRestartAsync();
        Assert.True((await node.Db.HashGetAsync($"{store.Prefix}rcpt:{c.Id}", "confirmedAt")).HasValue);
    }

    [Fact]
    public async Task REDIS02_disk_full_stops_Redis_before_any_reply_and_restart_shows_only_whole_commands()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sp018-diskfull-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var mount = await Run("mount", "-t", "tmpfs", "-o", "size=4m", "tmpfs", dir);
        if (mount.Code != 0) Assert.Skip("needs root to mount a small tmpfs: " + mount.Output);
        try
        {
            if (OperatingSystem.IsLinux()) File.SetUnixFileMode(dir, (UnixFileMode)0b111_111_111);
            await using var node = await RedisNode.StartAsync(dataDir: dir);
            var store = node.Store("disk");
            await store.InitAsync();
            var acked = new List<(StoreCommand Cmd, StoreReply Reply)>();
            for (var i = 0; i < 10; i++)
            {
                var c = Dispatch($"j{i}", 2048);
                acked.Add((c, await store.ExecuteAsync(c)));
            }

            // Fill the disk from outside, then keep writing until Redis cannot append.
            try
            {
                await using var fill = File.Create(Path.Combine(dir, "fill"));
                var block = new byte[64 * 1024];
                while (true) fill.Write(block);
            }
            catch (IOException)
            {
                // full
            }

            Exception? failure = null;
            StoreCommand? failed = null;
            for (var i = 10; i < 100 && failure is null; i++)
            {
                var c = Dispatch($"j{i}", 16 * 1024);
                try
                {
                    acked.Add((c, await store.ExecuteAsync(c)));
                }
                catch (Exception e) when (e is StoreOutcomeUnknownException or StoreNotAppliedException)
                {
                    failure = e;
                    failed = c;
                }
            }

            Assert.NotNull(failure);
            await Task.Delay(500);
            Assert.False(await node.IsRunningAsync()); // appendfsync always: Redis exits instead of acknowledging
            var log = (await node.LogsAsync()).Split('\n').Where(l => l.Contains("AOF", StringComparison.Ordinal)).TakeLast(2);
            output.WriteLine($"{acked.Count} acknowledged before the disk filled; client saw {failure!.GetType().Name}; log: {string.Join(" | ", log)}");

            File.Delete(Path.Combine(dir, "fill")); // operator adds capacity
            await node.StartAgainAsync();

            foreach (var (cmd, reply) in acked) Assert.Equal(reply.Result, (await store.ExecuteAsync(cmd)).Result);
            var resolution = await store.ResolveAsync(failed!, 86_400_000);
            Assert.Equal("NotObserved", resolution); // the unacknowledged command left nothing (Redis trimmed the short write)
            var s = await store.ReadSummaryAsync();
            Assert.False(s.Recovering);
            Assert.Equal(acked.Count, s.Counter("created"));
            Assert.Equal(s.Counter("created"), s.Pending);
        }
        finally
        {
            await Run("umount", dir);
        }
    }

    [Fact]
    public async Task REDIS03_command_past_NotAfter_never_applies_and_resolves_as_not_applied()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("expired");
        await store.InitAsync();
        var c = new StoreCommand("dispatch", Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddMilliseconds(-1).ToUnixTimeMilliseconds(), ["j1", "default", "p", "3", ""]);
        var r = await store.ExecuteAsync(c);
        Assert.Equal(("NOEFFECT", "CommandExpired"), (r.Kind, r.Status));
        Assert.Equal("NotApplied", await store.ResolveAsync(c, 86_400_000));
        Assert.Equal(0, (await store.ReadSummaryAsync()).Counter("created"));
    }

    [Fact]
    public async Task REDIS03_command_delivered_late_by_a_frozen_server_is_rejected_server_side()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("late");
        await store.InitAsync();
        await store.ExecuteAsync(Dispatch("j1"));

        await node.PauseAsync(); // TCP still accepts: the command bytes sit in the socket buffer
        var reserve = StoreCommand.Create("reserve", TimeSpan.FromMilliseconds(1500), "w1", "t1", "a1", "30000");
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(reserve));
        await Task.Delay(1000); // past NotAfter
        await node.UnpauseAsync(); // Redis now executes the stale bytes

        await Task.Delay(300);
        Assert.Equal("NotApplied", await store.ResolveAsync(reserve, 600_000));
        var again = await store.ExecuteAsync(reserve); // resubmitting the same command never applies it fresh
        Assert.Equal("CommandExpired", again.Status);
        var s = await store.ReadSummaryAsync();
        Assert.Equal((1, 0), (s.Counter("pending"), s.Counter("reserved")));
    }

    [Fact]
    public async Task REDIS03_no_offline_backlog_while_down_and_no_late_execution_after_reconnect()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("down");
        await store.InitAsync();
        await node.KillAsync();
        await Task.Delay(300);

        var c = Dispatch("j1");
        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(() => store.ExecuteAsync(c));
        output.WriteLine($"failed in {sw.ElapsedMilliseconds} ms while down");
        Assert.True(sw.ElapsedMilliseconds < 2500);

        await node.StartAgainAsync();
        await Task.Delay(1000); // a queued command would have been flushed by now
        Assert.Equal("NotObserved", await store.ResolveAsync(c, 86_400_000));
        Assert.Equal(0, (await store.ReadSummaryAsync()).Counter("created"));
    }

    [Fact]
    public async Task REDIS03_begin_accepted_before_lease_expiry_and_published_after_it_cannot_start_a_handler()
    {
        await using var node = await RedisNode.StartAsync();
        var store = node.Store("lease");
        await store.InitAsync();
        await store.ExecuteAsync(Dispatch("j1"));
        await store.ExecuteAsync(StoreCommand.Create("reserve", TimeSpan.FromSeconds(30), "w1", "t1", "a1", "1000"));
        var begin = StoreCommand.Create("begin", TimeSpan.FromSeconds(30), "j1", "a1", "w1", "t1", "1");
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => store.ExecuteAsync(begin, failAfter: 1)); // intent accepted, lease still valid

        await Task.Delay(1200); // the lease expires while the intent is pending
        var replay = await store.ExecuteAsync(begin); // recovery publishes the historical decision
        Assert.True(replay.Replayed);

        // Before user code the worker takes a fresh ownership-checked Renew: the expired lease refuses it.
        var renew = await store.ExecuteAsync(StoreCommand.Create("renew", TimeSpan.FromSeconds(30), "j1", "a1", "w1", "t1", "1", "30000"));
        Assert.Equal(("StaleLease", "lease_expired"), (renew.Status, renew.Code));
        var recovered = await store.ExecuteAsync(StoreCommand.Create("recover", TimeSpan.FromSeconds(30)));
        Assert.Equal("retrying", recovered.Json.GetProperty("state").GetString());
    }

    private static async Task<(int Code, string Output)> Run(string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var text = await p.StandardOutput.ReadToEndAsync() + await p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();
        return (p.ExitCode, text);
    }
}
