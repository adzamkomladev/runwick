using System.Collections.Concurrent;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// Counts the work wakes an idle worker of the client's application would receive: the store's notification channel
/// (PostgreSQL NOTIFY, Redis PUBLISH) or SQLite's in-process hint (05 §7.2).
/// </summary>
internal sealed class WakeProbe : IAsyncDisposable
{
    readonly CancellationTokenSource _stop = new();
    readonly ConcurrentQueue<string> _queues = new();
    Task _listening = Task.CompletedTask;

    public static async Task<WakeProbe> StartAsync(TestClient c)
    {
        var probe = new WakeProbe();
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        switch (c.Driver)
        {
            case IWakeNotificationSource n:
                probe._listening = n.ListenAsync(c.Application, WakeChannels.Work, probe._queues.Enqueue, () => subscribed.TrySetResult(), () => { },
                    probe._stop.Token);
                break;
            case ILocalWorkWakeSource l:
                // Registered before its first await.
                probe._listening = l.ListenForLocalWorkAsync(c.Application, probe._queues.Enqueue, probe._stop.Token);
                subscribed.TrySetResult();
                break;
            default:
                throw new InvalidOperationException("The store sends no wakes.");
        }

        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(30));
        return probe;
    }

    /// <summary>Waits for <paramref name="count"/> wakes of <paramref name="queue"/>, then leaves room for a wrongly sent extra one.</summary>
    public async Task ExpectAsync(int count, string queue = "default")
    {
        for (var i = 0; i < 100 && Count(queue) < count; i++)
        {
            await Task.Delay(100);
        }

        await Task.Delay(500);
        Assert.Equal(count, Count(queue));
    }

    int Count(string queue) => _queues.Count(q => q == queue);

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        try
        {
            await _listening;
        }
        catch (OperationCanceledException)
        {
        }

        _stop.Dispose();
    }
}
