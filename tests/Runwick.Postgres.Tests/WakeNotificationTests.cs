using System.Collections.Concurrent;
using System.Diagnostics;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Worker;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// WAKE-01/02 (07 Changes 2026-11-02, PF-07): the LISTEN/NOTIFY wake hint of 05 §7.2. A notification is sent only by a
/// committed dispatch of work eligible now; the worker's listener wakes its pools and resubscribes after a lost session.
/// OBS-01 (ReadinessTests) shows that dropping every notification loses nothing.
/// </summary>
[Trait("Item", "PF-07")]
public sealed class WakeNotificationTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    sealed class AheadOfDatabaseClock : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UtcNow.AddMinutes(2);
    }

    sealed class Listener : IAsyncDisposable
    {
        readonly NpgsqlConnection _conn;

        public ConcurrentQueue<string> Payloads { get; } = new();

        Listener(NpgsqlConnection conn) => _conn = conn;

        public static async Task<Listener> StartAsync(DriverFixture f)
        {
            var conn = new NpgsqlConnection(f.AdminConnectionString);
            await conn.OpenAsync();
            var l = new Listener(conn);
            conn.Notification += (_, e) => l.Payloads.Enqueue(e.Payload);
            using var cmd = new NpgsqlCommand("LISTEN " + WakeChannels.Work, conn);
            await cmd.ExecuteNonQueryAsync();
            return l;
        }

        /// <summary>Processes notifications for <paramref name="time"/>, or until <paramref name="until"/> holds.</summary>
        public async Task PumpAsync(TimeSpan time, Func<bool>? until = null)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < time && until?.Invoke() != true)
            {
                await _conn.WaitAsync(TimeSpan.FromMilliseconds(200));
            }
        }

        public async ValueTask DisposeAsync() => await _conn.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "WAKE-01")]
    public async Task A_committed_dispatch_notifies_its_queue_and_a_rolled_back_or_queued_behind_delayed_one_does_not()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wake01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r), time: new AheadOfDatabaseClock());
        await using var listener = await Listener.StartAsync(f);
        var prefix = f.Application + "/";

        // Rolled back: the staged rows and the notification vanish together.
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            Assert.Equal(DispatchDisposition.Created, (await Staging(c).StageAsync(c.Prepared.Prepare(new PingJob("rolled-back")), tx)).Disposition);
            await tx.RollbackAsync();
        }

        // The job appears due on the client clock, but is still in the future at the database decision time.
        var delayed = new DispatchOptions { NotBefore = DateTimeOffset.UtcNow.AddMinutes(1) };
        await c.Dispatcher.DispatchAsync(new KeyedJob("later", "future"), delayed);
        var batch = await c.Dispatcher.DispatchManyAsync([
            new BatchDispatchItem(new KeyedJob("reused", "future")),
            new BatchDispatchItem(new PingJob("later-in-batch"), delayed),
        ]);
        Assert.Equal(DispatchDisposition.Reused, batch.Items[0].Receipt!.Disposition);
        Assert.Equal(DispatchDisposition.Created, batch.Items[1].Receipt!.Disposition);
        // The first delayed job is its lane's earliest not-yet-due job and wakes once; the batch's one, due at the same time with a
        // later ID, is queued behind it and does not (2026-11-19, delayed pickup).
        await listener.PumpAsync(TimeSpan.FromSeconds(2));
        const string queue = "default";
        Assert.Equal([prefix + queue], listener.Payloads.Where(p => p.StartsWith(prefix, StringComparison.Ordinal)));

        // Committed, driver-owned: one notification naming the job's queue.
        var prepared = c.Prepared.Prepare(new PingJob("now"));
        var now = await c.Prepared.DispatchAsync(prepared);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND queue = @q", ("j", now.JobId.Value), ("q", queue)));
        await listener.PumpAsync(TimeSpan.FromSeconds(10), () => listener.Payloads.Count(p => p.StartsWith(prefix, StringComparison.Ordinal)) >= 2);
        Assert.Equal([prefix + queue, prefix + queue], listener.Payloads.Where(p => p.StartsWith(prefix, StringComparison.Ordinal)));

        // Replaying the immediate creation keeps its original receipt. The next listener pump must still see one wake.
        Assert.Equal(now.JobId, (await c.Prepared.DispatchAsync(prepared)).JobId);

        // Committed, staged: notified by the caller's COMMIT, not before.
        await using (var conn = await OpenBusinessAsync(f))
        {
            var tx = await conn.BeginTransactionAsync();
            await Staging(c).StageAsync(c.Prepared.Prepare(new PingJob("staged")), tx);
            await listener.PumpAsync(TimeSpan.FromSeconds(1));
            Assert.Equal(2, listener.Payloads.Count(p => p.StartsWith(prefix, StringComparison.Ordinal)));
            await tx.CommitAsync();
            await tx.DisposeAsync();
        }

        await listener.PumpAsync(TimeSpan.FromSeconds(10), () => listener.Payloads.Count(p => p.StartsWith(prefix, StringComparison.Ordinal)) >= 3);
        Assert.Equal(3, listener.Payloads.Count(p => p == prefix + queue));

        await c.Dispatcher.DispatchManyAsync([
            new BatchDispatchItem(new PingJob("first-due")),
            new BatchDispatchItem(new PingJob("second-due")),
            new BatchDispatchItem(new PingJob("future"), delayed),
        ]);
        await listener.PumpAsync(TimeSpan.FromSeconds(10), () => listener.Payloads.Count(p => p == prefix + queue) >= 4);
        await listener.PumpAsync(TimeSpan.FromMilliseconds(500));
        Assert.Equal(4, listener.Payloads.Count(p => p == prefix + queue));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "WAKE-01")]
    public async Task A_dispatch_notifies_when_it_is_due_now_or_the_earliest_delayed_job_of_its_lane()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wake_delayed"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await using var listener = await Listener.StartAsync(f);
        var payload = f.Application + "/default";

        // The first delayed job of an empty lane wakes (an idle worker's last probe could not name it), a later one does not, an
        // earlier one does again, and a due-now one always does. A batch wakes once for its earliest item, and not at all when
        // every item is queued behind an earlier delayed job.
        var now = DateTimeOffset.UtcNow;
        (string[] Keys, double?[] Minutes, int Wakes)[] steps =
        [
            (["first"], [5], 1), (["later"], [10], 1), (["earlier"], [2], 2), (["now"], [null], 3),
            (["behind", "earliest"], [20, 1], 4), (["behind-1", "behind-2"], [30, 40], 4),
        ];
        foreach (var (keys, minutes, wakes) in steps)
        {
            BatchDispatchItem[] items = [.. keys.Zip(minutes, (k, m) => new BatchDispatchItem(new PingJob(k),
                m is { } at ? new DispatchOptions { NotBefore = now.AddMinutes(at) } : null))];
            if (items.Length == 1)
            {
                await c.Dispatcher.DispatchAsync(new PingJob(keys[0]), items[0].Options);
            }
            else
            {
                await c.Dispatcher.DispatchManyAsync(items);
            }

            await listener.PumpAsync(TimeSpan.FromSeconds(10), () => listener.Payloads.Count(p => p == payload) >= wakes);
            await listener.PumpAsync(TimeSpan.FromMilliseconds(500)); // room for a wrongly sent extra notification
            Assert.Equal(wakes, listener.Payloads.Count(p => p == payload));
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "WAKE-01")]
    public async Task A_failed_post_commit_wake_leaves_the_dispatch_and_receipt_durable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var f = await DriverFixture.StartAsync(Ns("wake_failed"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var dispatchContext = new AsyncLocal<string?> { Value = "dispatch caller" };
        var attempted = new TaskCompletionSource<(string[] Payloads, string? Caller)>(TaskCreationOptions.RunContinuationsAsynchronously);
        c.Store.Sources.SendDispatchWakesOverride = (payloads, _) =>
        {
            attempted.TrySetResult((payloads, dispatchContext.Value));
            return Task.FromException(new IOException("Wake send failed."));
        };

        var prepared = c.Prepared.Prepare(new PingJob("committed"));
        var accepted = await c.Prepared.DispatchAsync(prepared, ct);
        Assert.Equal(DispatchDisposition.Created, accepted.Disposition);
        var wake = await attempted.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.Equal([f.Application + "/default"], wake.Payloads);
        Assert.Null(wake.Caller);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j", ("j", accepted.JobId.Value)));
        var resolution = await c.Prepared.ResolveAsync(prepared.OperationId, ct);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal(accepted.JobId, resolution.Receipt!.JobId);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "WAKE-02")]
    public async Task The_worker_listener_wakes_its_pools_and_resubscribes_after_losing_its_session()
    {
        await using var f = await DriverFixture.StartAsync(Ns("wake02"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 4);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.ListenerSubscriptions) >= 1, Wait, "the listener subscribed");
        Assert.True(w.Engine.ListenerHealthy);
        Assert.Equal(WorkerSettings.IdleBackoffCapListening, w.Engine.IdleBackoffCap);
        var producer = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r));

        await producer.Dispatcher.DispatchAsync(new WorkJob("woken"));
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("woken"), Wait, "the first job");
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.NotificationsReceived) >= 1, Wait, "the first notification");

        // Lose the listener session: polling covers the gap and the listener resubscribes after its back-off.
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM (
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE datname = current_database() AND application_name LIKE '%:listen') t
            """));
        // The loss is reported (the pools fall back to the 2 s cap until the resubscription), then the long cap returns.
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.ListenerLosses) >= 1, Wait, "the loss reported");
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.ListenerSubscriptions) >= 2, Wait, "the listener resubscribed");
        await WorkerHost.WaitUntil(() => w.Engine.ListenerHealthy, Wait, "healthy again");
        Assert.Equal(WorkerSettings.IdleBackoffCapListening, w.Engine.IdleBackoffCap);

        var before = Interlocked.Read(ref w.Engine.NotificationsReceived);
        await producer.Dispatcher.DispatchAsync(new WorkJob("after-reconnect"));
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("after-reconnect"), Wait, "the job after the reconnect");
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.NotificationsReceived) > before, Wait, "a notification after the reconnect");
        Assert.Equal(0L, w.Engine.NotificationsDropped);
        Assert.Empty(await f.HistoryViolations());
    }
}
