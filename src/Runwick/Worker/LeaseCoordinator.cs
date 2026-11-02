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

    /// <summary>
    /// The longest current wait of a renewal or settlement item for its I/O class (pressure gate input). One pass over
    /// a snapshot: the last waiter can leave between an emptiness check and the read, so an empty view is zero wait.
    /// </summary>
    public TimeSpan OldestWait(TimeSpan now)
    {
        var oldest = TimeSpan.Zero;
        foreach (var since in _waiting.Values)
        {
            oldest = now - since > oldest ? now - since : oldest;
        }

        return oldest;
    }

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
            List<AttemptEntry>? wave = null;
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
                        if (engine.Store is ILeaseBatchStore)
                        {
                            wave = CollectWave(ready, now);
                        }
                    }
                    else
                    {
                        wait = WorkerEngine.Min(wait, due - now);
                    }

                    break;
                }
            }

            if (wave is { Count: > 1 })
            {
                // One RenewMany for the wave (PF-06); members whose arbiter is busy take the per-attempt path.
                _ = RenewWaveAsync(wave);
                continue;
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

        return Apply(entry, reply.Status, reply.Error?.Code, reply.Value, reply.Replayed, reply.DecidedAt, sentAt);
    }

    /// <summary>One lease's Renew outcome (05 §11.3), from Renew/1 or from its member of a RenewMany/1 reply.</summary>
    RenewResult Apply(AttemptEntry entry, StoreStatus status, string? code, RenewedLease? value, bool replayed, DateTimeOffset decidedAt, TimeSpan sentAt)
    {
        switch (status)
        {
            case StoreStatus.Applied when !replayed && value is { } renewed:
                // A reply that arrives at or after the current d is history: loss latches instead (05 §11.2).
                if (entry.IsLost || (entry.Deadline is { } old && engine.Clock.Now >= old))
                {
                    engine.LoseAuthority(entry, LossCause.SafetyDeadline);
                    return RenewResult.Lost;
                }

                entry.AdvanceDeadline(engine.DeadlineFrom(sentAt, renewed.Lease, decidedAt));
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
                engine.Diagnose($"Renew invariant fault {code} for {entry}");
                engine.LoseAuthority(entry, LossCause.InvariantFault);
                return RenewResult.Lost;
            default:
                return RenewResult.Transient;
        }
    }

    // ------------------------------------------------------------------ renewal waves (PF-06, 05 Changes 2026-11-02)

    /// <summary>Renewal waves sent as one RenewMany call.</summary>
    public long RenewWavesSent;

    /// <summary>
    /// The wave a due item starts: every live item due within the coalescing window (half the renewal jitter), up to
    /// <see cref="ILeaseBatchStore.MaxLeases"/>. A member renews at most that much early and never later than scheduled,
    /// so every bound of 05 §11.2 still holds. The caller holds the lock.
    /// </summary>
    List<AttemptEntry> CollectWave(AttemptEntry first, TimeSpan now)
    {
        var wave = new List<AttemptEntry> { first };
        var horizon = now + (engine.Options.RenewalJitter / 2);
        while (wave.Count < ILeaseBatchStore.MaxLeases && _due.TryPeek(out var next, out var due) && due <= horizon)
        {
            _due.Dequeue();
            if (_versions.TryGetValue(next.Entry.AttemptId, out var v) && v == next.Version)
            {
                _versions.Remove(next.Entry.AttemptId);
                wave.Add(next.Entry);
            }
        }

        return wave;
    }

    /// <summary>
    /// One RenewMany for a wave. Each member holds its own arbiter slot for the whole call (05 §6 rule 3), taken without
    /// waiting: a member whose arbiter is busy (Begin, Finish or Release in flight) renews alone once it is free. Each
    /// member's outcome is applied exactly as a Renew/1 reply. A failure of the call as a whole, or a member's transient
    /// outcome, falls back to the per-attempt path: a new Renew per attempt, with that path's own backoff.
    /// </summary>
    async Task RenewWaveAsync(List<AttemptEntry> wave)
    {
        var members = new List<(AttemptEntry Entry, PriorityGate.Slot Slot)>();
        var single = new List<AttemptEntry>();
        try
        {
            var jobs = new HashSet<Guid>();
            foreach (var e in wave)
            {
                if (!Renewable(e))
                {
                    continue;
                }

                var slot = jobs.Add(e.Lease.JobId) ? e.Arbiter.TryAcquire() : null;
                if (slot is null)
                {
                    single.Add(e);
                    continue;
                }

                if (!Renewable(e) || (e.Deadline is { } d && engine.Clock.Now >= d))
                {
                    // Skipped, or lost: the watchdog latches an expired deadline.
                    slot.Dispose();
                    continue;
                }

                members.Add((e, slot));
            }

            if (members.Count == 1)
            {
                single.Add(members[0].Entry);
                members[0].Slot.Dispose();
                members.Clear();
            }

            if (members.Count == 0)
            {
                return;
            }

            var now = engine.Clock.Now;
            var budget = engine.Options.ControlRequestTimeout;
            var earliest = members.Select(m => m.Entry.Deadline).OfType<TimeSpan>().DefaultIfEmpty(TimeSpan.MaxValue).Min();
            if (earliest != TimeSpan.MaxValue)
            {
                budget = WorkerEngine.Min(budget, earliest - now);
            }

            var batch = (ILeaseBatchStore)engine.Store;
            var command = engine.Command(new RenewManyRequest([.. members.Select(m => m.Entry.Lease)], engine.Options.LeaseDuration));
            var priority = earliest == TimeSpan.MaxValue ? 0 : (int)Math.Min(int.MaxValue, Math.Max(0, earliest.TotalMilliseconds));
            StoreReply<RenewedLeases> reply;
            TimeSpan sentAt;
            try
            {
                using (TrackWait())
                {
                    Interlocked.Add(ref RenewsSent, members.Count);
                    Interlocked.Increment(ref RenewWavesSent);
                    sentAt = engine.Clock.Now;
                    reply = await engine.CallAsync(engine.RenewalIo, priority, budget, t => batch.RenewManyAsync(command, t)).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (!engine.ControlStop.IsCancellationRequested)
            {
                // Unknown or not applied for every member: each renews alone with a new command (05 §6 rule 4).
                engine.Logger.LogDebug(ex, "Runwick RenewMany of {Count} leases failed.", members.Count);
                single.AddRange(members.Select(m => m.Entry));
                return;
            }

            if (reply.Status != StoreStatus.Applied || reply.Replayed || reply.Value is not { } value || value.Outcomes.Count != members.Count)
            {
                single.AddRange(members.Select(m => m.Entry));
                return;
            }

            for (var i = 0; i < members.Count; i++)
            {
                var o = value.Outcomes[i];
                if (Apply(members[i].Entry, o.Status, o.Code, o.Value, replayed: false, reply.DecidedAt, sentAt) == RenewResult.Transient)
                {
                    single.Add(members[i].Entry);
                }
            }
        }
        catch (OperationCanceledException) when (engine.ControlStop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            engine.Logger.LogError(ex, "Runwick renewal wave failed unexpectedly.");
            single.AddRange(members.Select(m => m.Entry));
        }
        finally
        {
            foreach (var m in members)
            {
                m.Slot.Dispose();
            }

            foreach (var e in single.Distinct())
            {
                _ = RenewDueAsync(e);
            }
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
