using System.Collections.Concurrent;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    // Client wake (0018_client_wake, perf prototype): a direct Dispatch/1 or DispatchMany/1 no longer notifies inside its
    // transaction, because the notify lock is cluster-wide and is held through the commit flush. After the commit the
    // dispatcher sends the wake itself, at most one notify in flight per application/queue: arrivals during a send fold
    // into one more send after it. A lost wake only delays a pickup to the next poll (05 §7.2).

    readonly ConcurrentDictionary<string, int> _wakeState = new(StringComparer.Ordinal); // 0 idle, 1 sending, 2 sending + more

    /// <summary>Client wakes sent (tests and diagnostics).</summary>
    internal long ClientWakesSent;

    void WakeAfterCommit<T>(StoreReply<T> reply, string application, IEnumerable<DispatchItem> items) where T : class
    {
        if (reply.Status != StoreStatus.Applied || reply.Replayed)
        {
            return;
        }

        // A delayed job is not due, so nothing is woken for it (polling finds it when due), as in the function.
        var nowMs = time.GetUtcNow().ToUnixTimeMilliseconds();
        foreach (var queue in items.Where(i => i.Intent.NotBeforeMs is not { } nb || nb <= nowMs).Select(i => i.Intent.Queue).Distinct(StringComparer.Ordinal))
        {
            var key = application + "/" + queue;
            while (true)
            {
                var state = _wakeState.GetOrAdd(key, 0);
                if (state == 0 && _wakeState.TryUpdate(key, 1, 0))
                {
                    _ = SendWakesAsync(key);
                    break;
                }

                if (state == 2 || (state == 1 && _wakeState.TryUpdate(key, 2, 1)))
                {
                    break;
                }
            }
        }
    }

    async Task SendWakesAsync(string key)
    {
        while (true)
        {
            try
            {
                var cmd = sources.For(PgLane.Producer).CreateCommand("SELECT pg_notify('" + WakeChannels.Work + "', $1)");
                await using (cmd.ConfigureAwait(false))
                {
                    cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = key });
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                Interlocked.Increment(ref ClientWakesSent);
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or InvalidOperationException or ObjectDisposedException)
            {
                // Polling covers a lost wake.
            }

            if (_wakeState.TryUpdate(key, 0, 1))
            {
                return;
            }

            _wakeState[key] = 1; // more dispatches committed during the send: one more wake covers them all
        }
    }
}
