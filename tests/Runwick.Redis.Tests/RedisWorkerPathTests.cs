using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>Worker-side cost work on Redis (perf plan F): what the readiness probe reports so an idle worker can sleep until the next job is due.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
public sealed class RedisWorkerPathTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task The_probe_names_the_earliest_job_that_is_not_due_yet()
    {
        await using var f = await DriverFixture.StartAsync("rwp_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        LaneKey[] lanes = [new("default", "medium")];

        var empty = await c.Driver.ProbeAsync(c.Application, lanes, c.Contracts, Ct);
        Assert.Null(empty.NextDueAt);

        var later = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(40);
        var earlier = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(20);
        await c.Dispatcher.DispatchAsync(new PingJob("later"), new DispatchOptions { NotBefore = later }, Ct);
        await c.Dispatcher.DispatchAsync(new PingJob("earlier"), new DispatchOptions { NotBefore = earlier }, Ct);

        var delayedOnly = await c.Driver.ProbeAsync(c.Application, lanes, c.Contracts, Ct);
        Assert.Empty(delayedOnly.Lanes);
        Assert.NotNull(delayedOnly.NextDueAt);
        Assert.InRange(Math.Abs((delayedOnly.NextDueAt!.Value - earlier).TotalMilliseconds), 0, 50);

        // A job that is due now is ready at once, and the hint still names the delayed one.
        await c.Dispatcher.DispatchAsync(new PingJob("now"), cancellationToken: Ct);
        var withDue = await c.Driver.ProbeAsync(c.Application, lanes, c.Contracts, Ct);
        Assert.Single(withDue.Lanes);
        Assert.InRange(Math.Abs((withDue.NextDueAt!.Value - earlier).TotalMilliseconds), 0, 50);
    }

    [Fact]
    public async Task A_dispatch_publishes_a_wake_when_it_is_due_now_or_the_earliest_delayed_job_of_its_lane()
    {
        await using var f = await DriverFixture.StartAsync("rww_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = c.Services.GetRequiredService<RedisStore>();
        var woke = new ConcurrentQueue<string>();
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var listening = store.ListenAsync(c.Application, WakeChannels.Work, woke.Enqueue, () => subscribed.TrySetResult(), () => { }, stop.Token);
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        // The first delayed job of an empty lane wakes (an idle worker's last probe could not name it), a later one does not, an
        // earlier one does again, and a due-now one always does.
        var now = DateTimeOffset.UtcNow;
        (string Key, DateTimeOffset? NotBefore, int Wakes)[] steps =
            [("first", now.AddMinutes(5), 1), ("later", now.AddMinutes(10), 1), ("earlier", now.AddMinutes(2), 2), ("now", null, 3)];
        foreach (var (key, notBefore, wakes) in steps)
        {
            await c.Dispatcher.DispatchAsync(new PingJob(key), notBefore is { } at ? new DispatchOptions { NotBefore = at } : null, Ct);
            for (var i = 0; i < 100 && woke.Count < wakes; i++)
            {
                await Task.Delay(50, Ct);
            }

            await Task.Delay(200, Ct); // room for a wrongly published extra hint
            Assert.Equal(Enumerable.Repeat("default", wakes), woke.ToArray());
        }

        // The scheduler channel has no listener: the call returns at once, like a store without notifications.
        await store.ListenAsync(c.Application, WakeChannels.Schedule, _ => { }, () => { }, () => { }, Ct);
        await stop.CancelAsync();
        await listening;
    }
}
