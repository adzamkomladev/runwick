using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgDataSources
{
    static readonly TimeSpan DispatchWakeInterval = TimeSpan.FromMilliseconds(100);
    static readonly TimeSpan DispatchWakeBudget = TimeSpan.FromSeconds(1);

    readonly object _dispatchWakeGate = new();
    readonly HashSet<string> _pendingDispatchWakes = new(StringComparer.Ordinal);

    // Delayed creations since the last send: per lane (application, queue, priority), the one due first.
    readonly Dictionary<(string Application, string Queue, string Priority), (long DueMs, Guid JobId)> _pendingDelayedWakes = new();
    Task? _dispatchWakePump;
    bool _dispatchWakesStopped;

    /// <summary>Instance-local test seam for a failed notification send.</summary>
    internal Func<string[], CancellationToken, Task>? SendDispatchWakesOverride { get; set; }

    internal void WakeDispatch(string application, string queue)
    {
        lock (_dispatchWakeGate)
        {
            if (_dispatchWakesStopped || _pendingDispatchWakes.Count >= PayloadBounds.MaxBatchItems)
            {
                return;
            }

            _pendingDispatchWakes.Add(application + "/" + queue);
            StartPump();
        }
    }

    /// <summary>
    /// A committed delayed creation. Its queue is woken only if, when the pump sends, the job is still pending and is due or is
    /// the earliest not-yet-due job of its lane, which an idle worker's last probe could not name. Jobs queued behind an earlier
    /// delayed job send nothing, so a steady delayed stream costs one indexed check per lane and send, and almost no notifications.
    /// </summary>
    internal void WakeDelayed(string application, string queue, string priority, long dueMs, Guid jobId)
    {
        lock (_dispatchWakeGate)
        {
            var lane = (application, queue, priority);
            var held = _pendingDelayedWakes.TryGetValue(lane, out var h);
            if (_dispatchWakesStopped || (!held && _pendingDelayedWakes.Count >= PayloadBounds.MaxBatchItems))
            {
                return;
            }

            if (!held || dueMs < h.DueMs)
            {
                _pendingDelayedWakes[lane] = (dueMs, jobId);
            }

            StartPump();
        }
    }

    void StartPump()
    {
        if (_dispatchWakePump is null)
        {
            // One pump can deliver hints from several independent dispatches.
            using var flow = ExecutionContext.SuppressFlow();
            _dispatchWakePump = Task.Run(FlushDispatchWakesAsync);
        }
    }

    async Task FlushDispatchWakesAsync()
    {
        while (true)
        {
            string[] payloads;
            (string Application, Guid JobId)[] delayed;
            lock (_dispatchWakeGate)
            {
                if (_dispatchWakesStopped || (_pendingDispatchWakes.Count == 0 && _pendingDelayedWakes.Count == 0))
                {
                    _dispatchWakePump = null;
                    return;
                }

                payloads = [.. _pendingDispatchWakes];
                delayed = [.. _pendingDelayedWakes.Select(d => (d.Key.Application, d.Value.JobId))];
                _pendingDispatchWakes.Clear();
                _pendingDelayedWakes.Clear();
            }

            var slot = Gate(PgLane.Background);
            if (slot.Wait(0))
            {
                try
                {
                    using var budget = new CancellationTokenSource(DispatchWakeBudget);
                    if (SendDispatchWakesOverride is { } send)
                    {
                        await send(payloads, budget.Token).ConfigureAwait(false);
                    }
                    else
                    {
                        await SendDispatchWakesAsync(payloads, delayed, budget.Token).ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (ex is NpgsqlException or TimeoutException or IOException or OperationCanceledException or InvalidOperationException)
                {
                    // Work polling covers a missed wake (05 §7.2).
                }
                finally
                {
                    slot.Release();
                }
            }

            // Keep one pump during the cooldown. New hints for the same queue fold into its next send.
            await Task.Delay(DispatchWakeInterval).ConfigureAwait(false);
        }
    }

    async Task SendDispatchWakesAsync(string[] payloads, (string Application, Guid JobId)[] delayed, CancellationToken ct)
    {
        // A delayed candidate wakes its queue while it is pending and nothing not yet due precedes it in claim order
        // (eligible_at, job_id): one primary-key and one jobs_ready lookup each. PostgreSQL folds identical payloads.
        using var cmd = For(PgLane.Background).CreateCommand($"""
            SELECT pg_catalog.pg_notify('{WakeChannels.Work}', w.payload)
            FROM (
                SELECT q.payload FROM pg_catalog.unnest($1::text[]) AS q(payload)
                UNION ALL
                SELECT x.application || '/' || x.queue
                FROM unnest($2::text[], $3::uuid[]) AS c(application, job_id) -- multi-argument unnest is FROM syntax, not a pg_catalog function
                JOIN runwick.jobs AS x
                  ON x.application = c.application AND x.job_id = c.job_id AND x.state IN ('pending', 'retrying')
                WHERE NOT EXISTS (
                    SELECT 1 FROM runwick.jobs AS j
                    WHERE j.application = x.application AND j.queue = x.queue AND j.priority = x.priority
                      AND j.state IN ('pending', 'retrying')
                      AND j.eligible_at > pg_catalog.now() -- stable, so an index bound that skips a due backlog
                      AND (j.eligible_at, j.job_id) < (x.eligible_at, x.job_id))
            ) AS w
            """);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = payloads });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = delayed.Select(d => d.Application).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid, Value = delayed.Select(d => d.JobId).ToArray() });
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task StopDispatchWakesAsync()
    {
        Task? pump;
        lock (_dispatchWakeGate)
        {
            _dispatchWakesStopped = true;
            _pendingDispatchWakes.Clear();
            _pendingDelayedWakes.Clear();
            pump = _dispatchWakePump;
        }

        if (pump is not null)
        {
            await pump.ConfigureAwait(false);
        }
    }
}
