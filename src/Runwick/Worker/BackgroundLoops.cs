using Microsoft.Extensions.Logging;
using Runwick.Protocol;

namespace Runwick.Worker;

/// <summary>A probed snapshot plus the local monotonic instant it was received.</summary>
internal sealed record ReadyView(ReadySnapshot Snapshot, TimeSpan ReceivedAt)
{
    /// <summary>Lane age = (ObservedAt − OldestEligibleAt) + elapsed since receipt (05 §8.5).</summary>
    public Dictionary<LaneKey, TimeSpan> Ages(TimeSpan now) =>
        Snapshot.Lanes.ToDictionary(l => l.Lane, l => Snapshot.ObservedAt - l.OldestEligibleAt + (now - ReceivedAt));
}

/// <summary>
/// One readiness probe in flight per polling group (05 §7): the cached snapshot is a hint at most 2 s old;
/// it never authorizes a reservation.
/// </summary>
internal sealed class ReadinessCoordinator(WorkerEngine engine, IReadOnlyList<LaneKey> lanes)
{
    readonly Lock _lock = new();
    ReadyView? _current;
    Task<ReadyView>? _inflight;
    TimeSpan _lastProbe = TimeSpan.MinValue;

    public long Probes;

    public bool Healthy { get; private set; } = true;

    public IReadOnlyList<LaneKey> Lanes => lanes;

    /// <summary>The current snapshot when younger than the hint age, otherwise a fresh probe (shared by all pools).</summary>
    public Task<ReadyView> GetAsync(CancellationToken ct)
    {
        lock (_lock)
        {
            var now = engine.Clock.Now;
            if (_current is { } c && now - c.ReceivedAt < WorkerSettings.HintMaxAge)
            {
                return Task.FromResult(c);
            }

            return _inflight ??= ProbeAsync(ct);
        }
    }

    /// <summary>Drops the cached snapshot (reconnect, control change); the next request probes again.</summary>
    public void Invalidate()
    {
        lock (_lock)
        {
            _current = null;
        }
    }

    async Task<ReadyView> ProbeAsync(CancellationToken ct)
    {
        try
        {
            await Task.Yield();
            Interlocked.Increment(ref Probes);
            var snapshot = await engine.CallAsync(engine.BackgroundIo, 0, engine.Options.ControlRequestTimeout,
                t => engine.Store.ProbeAsync(engine.Application, lanes, engine.Contracts, t), ct).ConfigureAwait(false);
            var view = new ReadyView(snapshot, engine.Clock.Now);
            lock (_lock)
            {
                _current = view;
                _lastProbe = view.ReceivedAt;
            }

            Healthy = true;
            return view;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            Healthy = false;
            throw;
        }
        finally
        {
            lock (_lock)
            {
                _inflight = null;
            }
        }
    }
}

/// <summary>Worker presence (05 §14): heartbeat only; never lease authority.</summary>
internal sealed class PresenceLoop(WorkerEngine engine)
{
    long _sequence;

    public string Status { get; private set; } = "starting";

    public bool Failing { get; private set; }

    public async Task PublishAsync(string status, CancellationToken ct)
    {
        Status = status;
        var record = new PresenceRecord(engine.WorkerInstanceId, Interlocked.Increment(ref _sequence), 1, status, Environment.ProcessId,
            Environment.MachineName, typeof(PresenceLoop).Assembly.GetName().Version?.ToString() ?? "0",
            [.. engine.Settings.Pools.SelectMany(p => p.Queues.Select(q => q.Queue)).Distinct()],
            engine.Settings.Pools.ToDictionary(p => p.Name, p => p.Target), engine.Contracts, WorkerSettings.PresenceExpiry);
        try
        {
            await engine.CallAsync(engine.BackgroundIo, 0, engine.Options.ControlRequestTimeout,
                t => engine.Store.PublishPresenceAsync(engine.Application, record, t), ct).ConfigureAwait(false);
            Failing = false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Presence failures degrade observability only.
            Failing = true;
            engine.Logger.LogDebug(ex, "Runwick presence publication failed.");
        }
    }
}

internal sealed partial class WorkerEngine
{
    /// <summary>Heartbeat every 5 s ± 10 % until control stop (05 §14).</summary>
    async Task PresenceLoopAsync()
    {
        await Presence.PublishAsync("ready", ControlStop).ConfigureAwait(false);
        while (!ControlStop.IsCancellationRequested)
        {
            await Clock.Delay(EngineClock.Uniform(WorkerSettings.PresenceHeartbeat * 0.9, WorkerSettings.PresenceHeartbeat * 1.1), ControlStop).ConfigureAwait(false);
            var status = Draining ? "draining" : Health().Readiness == WorkerReadiness.Healthy ? "ready" : "degraded";
            await Presence.PublishAsync(status, ControlStop).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Bounded expired-lease discovery and sequential Recover commands (05 §13). Storage arbitrates races by exact
    /// attempt and token; presence never triggers recovery.
    /// </summary>
    async Task RecoveryLoopAsync()
    {
        var interval = Options.RecoveryScanInterval;
        var jitter = Options.RecoveryScanJitter;
        while (!ControlStop.IsCancellationRequested)
        {
            await Clock.Delay(EngineClock.Uniform(interval - jitter, interval + jitter), ControlStop).ConfigureAwait(false);
            IReadOnlyList<ExpiredLease> expired;
            try
            {
                expired = await CallAsync(BackgroundIo, 1, Options.ControlRequestTimeout,
                    t => Store.ListExpiredLeasesAsync(Application, WorkerSettings.RecoveryBatch, t)).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ControlStop.IsCancellationRequested)
            {
                Logger.LogDebug(ex, "Runwick recovery scan failed.");
                continue;
            }

            foreach (var lease in expired)
            {
                if (ControlStop.IsCancellationRequested)
                {
                    break;
                }

                await RecoverAsync(lease).ConfigureAwait(false);
            }
        }
    }

    async Task RecoverAsync(ExpiredLease lease)
    {
        var command = Command(new RecoverRequest(lease.JobId, lease.AttemptId, lease.OwnershipToken));
        for (var attempt = 0; !ControlStop.IsCancellationRequested; attempt++)
        {
            try
            {
                var reply = await CallAsync(BackgroundIo, 1, Options.ControlRequestTimeout,
                    t => Store.RecoverAsync(command, t)).ConfigureAwait(false);
                if (reply.Status == StoreStatus.Applied)
                {
                    Emit("recovered");
                }

                return;
            }
            catch (Exception ex) when (!ControlStop.IsCancellationRequested)
            {
                // Unknown or not applied: resubmit the same command (the receipt makes it idempotent).
                if (Clock.WallNow > command.NotAfter)
                {
                    return;
                }

                Logger.LogDebug(ex, "Runwick Recover {Job} retry.", lease.JobId);
                await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
            }
        }
    }
}
