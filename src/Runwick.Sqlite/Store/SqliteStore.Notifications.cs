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

    /// <summary>
    /// After a committed, non-replayed dispatch: each queue with a created job that is due now, or that is the earliest not-yet-due
    /// job of its lane (an idle worker's last probe could not name it), is woken once. Jobs queued behind an earlier delayed job wake
    /// nothing. The earliest check is one indexed read per lane with a delayed creation, made only when a local worker listens.
    /// </summary>
    async Task WakeLocalAsync(Action<string> listeners, string application, IEnumerable<(DispatchItem Item, DispatchResult? Result)> outcomes,
        DateTimeOffset decidedAt)
    {
        var queues = new HashSet<string>(StringComparer.Ordinal);
        Dictionary<(string Queue, string Priority), (long DueMs, Guid JobId)>? delayed = null;
        foreach (var (item, result) in outcomes)
        {
            if (result?.Disposition != DispatchDisposition.Created)
            {
                continue;
            }

            if (item.Intent.NotBeforeMs is { } due && due > decidedAt.ToUnixTimeMilliseconds())
            {
                var lane = (item.Intent.Queue, item.Intent.Priority);
                if ((delayed ??= []).TryGetValue(lane, out var held) is false || due < held.DueMs)
                {
                    delayed[lane] = (due, result.JobId);
                }
            }
            else if (queues.Add(item.Intent.Queue))
            {
                PublishLocalWake(listeners, item.Intent.Queue);
            }
        }

        if (delayed is null)
        {
            return;
        }

        List<string> earliest;
        try
        {
            earliest = await runner.ReadAsync(conn =>
            {
                var now = Now();
                return delayed.Values.SelectMany(d => Rows(Cmd(conn, application, """
                    SELECT x.queue FROM runwick_jobs AS x
                    WHERE x.application = @application AND x.job_id = @job AND x.state IN ('pending', 'retrying')
                      AND NOT EXISTS (
                          SELECT 1 FROM runwick_jobs AS j
                          WHERE j.application = x.application AND j.queue = x.queue AND j.priority = x.priority
                            AND j.state IN ('pending', 'retrying') AND j.eligible_at > @now
                            AND (j.eligible_at < x.eligible_at OR (j.eligible_at = x.eligible_at AND j.job_id < x.job_id)))
                    """).P("job", d.JobId).P("now", now), r => r.GetString(0))).ToList();
            }, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best effort, like the hint itself: the dispatch has committed and workers keep polling.
            return;
        }

        foreach (var queue in earliest)
        {
            if (queues.Add(queue))
            {
                PublishLocalWake(listeners, queue);
            }
        }
    }
}
