using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Runwick.Protocol;

namespace Runwick.Worker;

/// <summary>
/// Renewal scheduling and the safety watchdog (05 §11). One due item per attempt (replaced on reschedule); renewal
/// I/O uses the renewal lane, earliest safety deadline first. The watchdog is a separate timer that latches loss at
/// <c>now ≥ d</c> regardless of the arbiter or any hung request.
/// </summary>
internal sealed class LeaseCoordinator(WorkerEngine engine) : IDisposable
{
    readonly Lock _lock = new();
    readonly PriorityQueue<(AttemptEntry Entry, long Version), TimeSpan> _due = new();
    readonly Dictionary<Guid, long> _versions = [];
    readonly ConcurrentDictionary<long, TimeSpan> _waiting = new();
    readonly WakeSignal _wake = new();
    long _version;
    long _waitId;
    ITimer? _watchdog;

    public long RenewsSent;

    public void Start()
    {
        _watchdog = engine.Clock.Time.CreateTimer(_ => Watchdog(), null, engine.Options.LeaseWatchdogInterval, engine.Options.LeaseWatchdogInterval);
        engine.Supervise("lease", LoopAsync, critical: true);
    }

    public void Wake() => _wake.Set();

    public void Dispose() => _watchdog?.Dispose();

    /// <summary>The longest current wait of a renewal or settlement item for its I/O class (pressure gate input).</summary>
    public TimeSpan OldestWait(TimeSpan now) => _waiting.IsEmpty ? TimeSpan.Zero : _waiting.Values.Select(v => now - v).Max();

    public IDisposable TrackWait()
    {
        var id = Interlocked.Increment(ref _waitId);
        _waiting[id] = engine.Clock.Now;
        return new WaitToken(this, id);
    }

    sealed class WaitToken(LeaseCoordinator owner, long id) : IDisposable
    {
        public void Dispose() => owner._waiting.TryRemove(id, out _);
    }

    /// <summary>Schedules the next renewal from the send time of the last fresh request (05 §11.2).</summary>
    public void Schedule(AttemptEntry entry, TimeSpan sentAt)
    {
        if (entry.Deadline is not { } d)
        {
            return;
        }

        var j = EngineClock.Uniform(engine.Settings.RenewalIntervalMin, engine.Settings.RenewalIntervalMax);
        var due = WorkerEngine.Min(sentAt + j, d - engine.Settings.RenewalLead);
        Enqueue(entry, due);
    }

    /// <summary>Renew as soon as possible (replayed claim/Begin needs a fresh confirmation).</summary>
    public void ScheduleNow(AttemptEntry entry) => Enqueue(entry, engine.Clock.Now);

    void Enqueue(AttemptEntry entry, TimeSpan due)
    {
        lock (_lock)
        {
            var version = ++_version;
            _versions[entry.AttemptId] = version;
            _due.Enqueue((entry, version), due);
            if (_due.Count > 4 * Math.Max(16, _versions.Count))
            {
                Compact();
            }
        }

        _wake.Set();
    }

    public void Unregister(AttemptEntry entry)
    {
        lock (_lock)
        {
            _versions.Remove(entry.AttemptId);
        }
    }

    /// <summary>Stale heap entries are dropped when they exceed the live count.</summary>
    void Compact()
    {
        var live = new List<((AttemptEntry, long), TimeSpan)>();
        while (_due.TryDequeue(out var item, out var due))
        {
            if (_versions.TryGetValue(item.Entry.AttemptId, out var v) && v == item.Version)
            {
                live.Add((item, due));
            }
        }

        foreach (var (item, due) in live)
        {
            _due.Enqueue(item, due);
        }
    }

    async Task LoopAsync()
    {
        var stop = engine.ControlStop;
        while (!stop.IsCancellationRequested)
        {
            AttemptEntry? ready = null;
            TimeSpan wait;
            lock (_lock)
            {
                wait = TimeSpan.FromSeconds(1);
                while (_due.TryPeek(out var item, out var due))
                {
                    if (!_versions.TryGetValue(item.Entry.AttemptId, out var v) || v != item.Version)
                    {
                        _due.Dequeue();
                        continue;
                    }

                    var now = engine.Clock.Now;
                    if (due <= now)
                    {
                        _due.Dequeue();
                        _versions.Remove(item.Entry.AttemptId);
                        ready = item.Entry;
                    }
                    else
                    {
                        wait = WorkerEngine.Min(wait, due - now);
                    }

                    break;
                }
            }

            if (ready is not null)
            {
                // Bounded: at most one renewal task per attempt; the renewal lane bounds concurrent I/O.
                _ = RenewDueAsync(ready);
                continue;
            }

            await _wake.WaitAsync(engine.Clock, wait, stop).ConfigureAwait(false);
        }
    }

    static bool Renewable(AttemptEntry e) =>
        !e.IsLost && !e.SettlementInFlight && e.Phase is AttemptPhase.Reserved or AttemptPhase.Beginning or AttemptPhase.Running
            or AttemptPhase.CleaningUp or AttemptPhase.Finishing or AttemptPhase.Releasing;

    /// <summary>A due renewal: transient failures retry as new Renew commands while authority remains (05 §11.2).</summary>
    async Task RenewDueAsync(AttemptEntry entry)
    {
        try
        {
            for (var attempt = 0; Renewable(entry) && !engine.ControlStop.IsCancellationRequested; attempt++)
            {
                var result = await RenewOnceAsync(entry).ConfigureAwait(false);
                if (result != RenewResult.Transient)
                {
                    return;
                }

                if (entry.Deadline is { } d && engine.Clock.Now >= d)
                {
                    return;
                }

                await engine.Clock.Delay(engine.Backoff(attempt), engine.ControlStop).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (engine.ControlStop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            engine.Logger.LogError(ex, "Runwick renewal of {Entry} failed unexpectedly.", entry);
        }
    }

    internal enum RenewResult
    {
        Fresh,
        Lost,
        Skipped,
        Transient,
    }

    /// <summary>
    /// One Renew through the arbiter: a new command ID, no receipt (R1). Only a fresh Applied reply advances d; a
    /// reply after a latch is history.
    /// </summary>
    public async Task<RenewResult> RenewOnceAsync(AttemptEntry entry)
    {
        using var slot = await entry.Arbiter.AcquireAsync(ControlPriority.Renew, engine.ControlStop).ConfigureAwait(false);
        if (!Renewable(entry))
        {
            return RenewResult.Skipped;
        }

        var now = engine.Clock.Now;
        var budget = engine.Options.ControlRequestTimeout;
        if (entry.Deadline is { } d0)
        {
            if (now >= d0)
            {
                return RenewResult.Lost;
            }

            budget = WorkerEngine.Min(budget, d0 - now);
        }

        var command = engine.Command(new RenewRequest(entry.Lease, engine.Options.LeaseDuration));
        var priority = entry.Deadline is { } dd ? (int)Math.Min(int.MaxValue, Math.Max(0, dd.TotalMilliseconds)) : 0;
        StoreReply<RenewedLease> reply;
        TimeSpan sentAt;
        try
        {
            using (TrackWait())
            {
                Interlocked.Increment(ref RenewsSent);
                sentAt = engine.Clock.Now;
                reply = await engine.CallAsync(engine.RenewalIo, priority, budget, t => engine.Store.RenewAsync(command, t)).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (!engine.ControlStop.IsCancellationRequested)
        {
            // Unknown or not applied: no receipt resolution; the next Renew is a new command (05 §6 rule 4).
            engine.Logger.LogDebug(ex, "Runwick Renew of {Entry} failed.", entry);
            return RenewResult.Transient;
        }

        switch (reply.Status)
        {
            case StoreStatus.Applied when !reply.Replayed && reply.Value is { } renewed:
                // A reply that arrives at or after the current d is history: loss latches instead (05 §11.2).
                if (entry.IsLost || (entry.Deadline is { } old && engine.Clock.Now >= old))
                {
                    engine.LoseAuthority(entry, LossCause.SafetyDeadline);
                    return RenewResult.Lost;
                }

                entry.AdvanceDeadline(engine.DeadlineFrom(sentAt, renewed.Lease, reply.DecidedAt));
                Interlocked.Increment(ref entry.Renewals);

                Schedule(entry, sentAt);
                if (renewed.CancellationRequested)
                {
                    engine.ObserveDurableCancellation(entry);
                }

                engine.Emit("renewed", entry);
                return RenewResult.Fresh;
            case StoreStatus.StaleLease:
                engine.LoseAuthority(entry, LossCause.StaleLease);
                return RenewResult.Lost;
            case StoreStatus.InvalidState:
                engine.LoseAuthority(entry, LossCause.InvalidState);
                return RenewResult.Lost;
            case StoreStatus.InvariantFault:
                engine.Diagnose($"Renew invariant fault {reply.Error?.Code} for {entry}");
                engine.LoseAuthority(entry, LossCause.InvariantFault);
                return RenewResult.Lost;
            default:
                return RenewResult.Transient;
        }
    }

    /// <summary>Safety watchdog tick (05 §11.4): delayed delivery never extends a deadline.</summary>
    void Watchdog()
    {
        var now = engine.Clock.Now;
        foreach (var entry in engine.Entries)
        {
            if (!entry.IsLost && entry.Phase != AttemptPhase.Retired && entry.Deadline is { } d && now >= d)
            {
                engine.LoseAuthority(entry, LossCause.SafetyDeadline);
            }
        }
    }
}
