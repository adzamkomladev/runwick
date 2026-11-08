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

    /// <summary>Queues the current snapshot reports saturated (probe/2 evidence; empty without one).</summary>
    public HashSet<string> SaturatedQueues()
    {
        lock (_lock)
        {
            return _current is { } c
                ? new HashSet<string>(c.Snapshot.Queues.Where(q => q.Saturated && !q.Paused).Select(q => q.Queue), StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// A wake notification for <paramref name="queue"/> (05 §7.2, PF-07). When the cached snapshot already shows a due lane
    /// of the queue the hint adds nothing and false is returned; otherwise the snapshot is dropped so the woken pools probe
    /// again (still ≥ 50 ms apart, so a notification burst never forms a probe loop).
    /// </summary>
    public bool OnWake(string queue)
    {
        lock (_lock)
        {
            if (_current is { } c && engine.Clock.Now - c.ReceivedAt < WorkerSettings.HintMaxAge
                && c.Snapshot.Lanes.Any(l => string.Equals(l.Lane.Queue, queue, StringComparison.Ordinal)))
            {
                return false;
            }

            _current = null;
            return true;
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

            // ≥ 50 ms between probe starts per group, whatever asked for it (09 §4.1, 06 §1.5): wakes and invalidations
            // coalesce into the next spaced probe and never form a probe loop.
            TimeSpan wait;
            lock (_lock)
            {
                wait = _lastProbe == TimeSpan.MinValue ? TimeSpan.Zero : _lastProbe + WorkerSettings.ProbeSpacing - engine.Clock.Now;
            }

            if (wait > TimeSpan.Zero)
            {
                await engine.Clock.Delay(wait, ct).ConfigureAwait(false);
            }

            lock (_lock)
            {
                _lastProbe = engine.Clock.Now;
            }

            Interlocked.Increment(ref Probes);
            var snapshot = await engine.CallAsync(engine.BackgroundIo, 0, engine.Options.ControlRequestTimeout,
                t => engine.Store.ProbeAsync(engine.Application, lanes, engine.Contracts, t), ct).ConfigureAwait(false);
            var view = new ReadyView(snapshot, engine.Clock.Now);
            lock (_lock)
            {
                _current = view;
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

    public long Publications;

    public long Failures;

    public string Status { get; private set; } = "starting";

    public bool Failing { get; private set; }

    public async Task PublishAsync(string status, CancellationToken ct)
    {
        Status = status;
        var record = new PresenceRecord(engine.WorkerInstanceId, Interlocked.Increment(ref _sequence), 1, status, Environment.ProcessId,
            Environment.MachineName, typeof(PresenceLoop).Assembly.GetName().Version?.ToString() ?? "0",
            [.. engine.Settings.Pools.SelectMany(p => p.Queues.Select(q => q.Queue)).Distinct()],
            engine.Pools.ToDictionary(p => p.Settings.Name, p => p.Gate.Target), engine.Contracts, WorkerSettings.PresenceExpiry);
        try
        {
            await engine.CallAsync(engine.BackgroundIo, 0, engine.Options.ControlRequestTimeout,
                t => engine.Store.PublishPresenceAsync(engine.Application, record, t), ct).ConfigureAwait(false);
            Failing = false;
            Interlocked.Increment(ref Publications);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Presence failures degrade observability only (readiness Degraded, 05 §17); never lease authority.
            Failing = true;
            Interlocked.Increment(ref Failures);
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
        // A new root span per Recover command (18 §2.1); measured after the outcome is known.
        System.Diagnostics.Activity.Current = null;
        using var span = Telemetry.Telemetry.Source.StartActivity("runwick.recover");
        var result = await RecoverOnceAsync(lease).ConfigureAwait(false);
        span?.SetTag("runwick.result", result);
        Telemetry.Telemetry.Recoveries.Add(1, result);
    }

    async Task<string> RecoverOnceAsync(ExpiredLease lease)
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
                    return "recovered";
                }

                return Snake(reply.Status.ToString());
            }
            catch (Exception ex) when (!ControlStop.IsCancellationRequested)
            {
                // Unknown or not applied: resubmit the same command (the receipt makes it idempotent).
                if (Clock.WallNow > command.NotAfter)
                {
                    return "unresolved";
                }

                Logger.LogDebug(ex, "Runwick Recover {Job} retry.", lease.JobId);
                await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
            }
        }

        return "unresolved";
    }
}
