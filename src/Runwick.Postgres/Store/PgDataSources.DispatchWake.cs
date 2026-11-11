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
            if (_dispatchWakePump is null)
            {
                // One pump can deliver hints from several independent dispatches.
                using var flow = ExecutionContext.SuppressFlow();
                _dispatchWakePump = Task.Run(FlushDispatchWakesAsync);
            }
        }
    }

    async Task FlushDispatchWakesAsync()
    {
        while (true)
        {
            string[] payloads;
            lock (_dispatchWakeGate)
            {
                if (_dispatchWakesStopped || _pendingDispatchWakes.Count == 0)
                {
                    _dispatchWakePump = null;
                    return;
                }

                payloads = [.. _pendingDispatchWakes];
                _pendingDispatchWakes.Clear();
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
                        await SendDispatchWakesAsync(payloads, budget.Token).ConfigureAwait(false);
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

    async Task SendDispatchWakesAsync(string[] payloads, CancellationToken ct)
    {
        using var cmd = For(PgLane.Background).CreateCommand(
            $"SELECT pg_catalog.pg_notify('{WakeChannels.Work}', q.payload) FROM pg_catalog.unnest($1) AS q(payload)");
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = payloads });
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task StopDispatchWakesAsync()
    {
        Task? pump;
        lock (_dispatchWakeGate)
        {
            _dispatchWakesStopped = true;
            _pendingDispatchWakes.Clear();
            pump = _dispatchWakePump;
        }

        if (pump is not null)
        {
            await pump.ConfigureAwait(false);
        }
    }
}
