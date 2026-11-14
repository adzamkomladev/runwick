using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore : ILocalWorkWakeSource
{
    static readonly Lock LocalWakeLock = new();
    static readonly Dictionary<(string Database, string Application), Action<string>> LocalListeners = new();

    public async Task ListenForLocalWorkAsync(string application, Action<string> onWake, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var path = Volatile.Read(ref _databaseFile);
        if (path is null)
        {
            return;
        }

        var key = (path, application);
        lock (LocalWakeLock)
        {
            LocalListeners[key] = LocalListeners.GetValueOrDefault(key) + onWake;
        }

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);
        }
        finally
        {
            lock (LocalWakeLock)
            {
                if (LocalListeners[key] - onWake is { } remaining)
                {
                    LocalListeners[key] = remaining;
                }
                else
                {
                    LocalListeners.Remove(key);
                }
            }
        }
    }

    Action<string>? LocalListenersFor(string application)
    {
        var path = Volatile.Read(ref _databaseFile);
        if (path is null)
        {
            return null;
        }

        lock (LocalWakeLock)
        {
            return LocalListeners.GetValueOrDefault((path, application));
        }
    }

    static void PublishLocalWake(Action<string> listeners, string queue)
    {
        foreach (var listener in Delegate.EnumerateInvocationList(listeners))
        {
            try
            {
                listener(queue);
            }
            catch (Exception)
            {
                // A process-local hint is best effort. The dispatch has already committed and workers keep polling.
            }
        }
    }

    static bool IsDueCreated(DispatchItem item, DispatchResult? result, DateTimeOffset decidedAt) =>
        result?.Disposition == DispatchDisposition.Created &&
        (item.Intent.NotBeforeMs is not { } due || due <= decidedAt.ToUnixTimeMilliseconds());
}
