using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Worker;

/// <summary>
/// One pool (05 §8): tickets, lane selection, cursors and cooldowns. Selection and claim settlement are serial per
/// pool (at most one Reserve outstanding, including resolution of an uncertain claim); execution is concurrent.
/// </summary>
internal sealed class PoolCoordinator
{
    readonly WorkerEngine _engine;
    readonly Dictionary<LaneKey, CandidateCursor?> _cursors = [];
    readonly HashSet<LaneKey> _excluded = [];
    ReadyView? _excludedFor;
    readonly HashSet<string> _saturated = new(StringComparer.Ordinal);
    readonly Dictionary<string, long> _queueRevisions = new(StringComparer.Ordinal);
    readonly System.Collections.Concurrent.ConcurrentQueue<string> _released = new();
    int _suspensions;
    int _idleStep;
    int _roundReserves;
    bool _claimUnresolved;

    public PoolCoordinator(WorkerEngine engine, PoolSettings settings)
    {
        _engine = engine;
        Settings = settings;
        Gate = new LocalCapacityGate(settings.Target);
        Selector = new LaneSelector(settings);
        Gate.Released += () => Wake.Set();
        Scaler = settings.Auto is { } auto ? new Scaling.PoolScaler(auto.Min, auto.Max) : null;
        Gate.OutstandingChanged += n => Occupancy.Record(_engine.Clock.Now, n);
    }

    /// <summary>The automatic controller of an <c>AutoConcurrency</c> pool; null for a fixed pool (11 §3).</summary>
    public Scaling.PoolScaler? Scaler { get; }

    public Scaling.OccupancyIntegrator Occupancy { get; } = new();

    public Scaling.AdmissionOutcomes Outcomes { get; } = new();

    /// <summary>The last scaler decision, for diagnostics and tests.</summary>
    public Scaling.ScalerDecision? LastDecision { get; set; }

    public PoolSettings Settings { get; }

    public LocalCapacityGate Gate { get; }

    public LaneSelector Selector { get; }

    public WakeSignal Wake { get; } = new();

    /// <summary>The Reserve currently in flight (or being resolved); shutdown waits for it.</summary>
    public Task? ClaimTask { get; private set; }

    public long ReservesSent;

    /// <summary>Fresh claims stop while a Reserve or Begin of this pool is unresolved (05 §15.2).</summary>
    public bool Suspended => Volatile.Read(ref _suspensions) > 0 || _claimUnresolved;

    public bool ClaimUnresolved => _claimUnresolved;

    /// <summary>Saturated-queue Reserves skipped because probe/2 evidence excluded the queue (09 §4.2).</summary>
    public long SaturationSkips;

    public void Suspend() => Interlocked.Increment(ref _suspensions);

    /// <summary>
    /// A local attempt of <paramref name="queue"/> retired: a known release, the one local event that may recheck a
    /// saturated queue before the next probe (capacity-defaults §2). Drained by the opportunity loop, which it wakes.
    /// </summary>
    public void OnAttemptRetired(string queue)
    {
        _released.Enqueue(queue);
        Wake.Set();
    }

    public void Resume()
    {
        Interlocked.Decrement(ref _suspensions);
        Wake.Set();
    }

    /// <summary>The opportunity loop (05 §8.3).</summary>
    public async Task RunAsync()
    {
        var stop = _engine.LoopStop;
        while (!stop.IsCancellationRequested && !_engine.Draining)
        {
            if (!_engine.AdmissionOpen || Suspended)
            {
                await Wake.WaitAsync(_engine.Clock, TimeSpan.FromSeconds(1), stop).ConfigureAwait(false);
                continue;
            }

            var ticket = Gate.TryAcquire();
            if (ticket is null)
            {
                await Wake.WaitAsync(_engine.Clock, TimeSpan.FromSeconds(1), stop).ConfigureAwait(false);
                continue;
            }

            ReadyView view;
            try
            {
                view = await _engine.Readiness.GetAsync(stop).ConfigureAwait(false);
            }
            catch (Exception ex) when (!stop.IsCancellationRequested)
            {
                ticket.Return();
                _engine.Logger.LogDebug(ex, "Runwick readiness probe failed.");
                await IdleAsync(stop).ConfigureAwait(false);
                continue;
            }

            var now = _engine.Clock.Now;
            if (!ReferenceEquals(view, _excludedFor))
            {
                _excluded.Clear();
                _excludedFor = view;
                ApplyQueueEvidence(view.Snapshot);
            }

            // A known release lifts both the probe exclusion and the queue-scope cooldown at once (09 §4.2).
            while (_released.TryDequeue(out var releasedQueue))
            {
                _saturated.Remove(releasedQueue);
                Selector.ReleaseQueue(releasedQueue);
            }

            var ages = view.Ages(now);
            foreach (var lane in _excluded)
            {
                ages.Remove(lane);
            }

            foreach (var lane in ages.Keys.Where(l => _saturated.Contains(l.Queue)).ToList())
            {
                ages.Remove(lane);
                Interlocked.Increment(ref SaturationSkips);
            }

            var choice = Selector.Choose(ages, now);
            if (choice is null)
            {
                ticket.Return();
                if (ages.Count == 0)
                {
                    // Nothing claimable in this hint: the next opportunity probes afresh.
                    _engine.Readiness.Invalidate();
                }

                await IdleAsync(stop).ConfigureAwait(false);
                continue;
            }

            if (await LaneVisitAsync(choice, ticket).ConfigureAwait(false))
            {
                _idleStep = 0;
            }

            if (_roundReserves >= WorkerSettings.ReservesPerRound)
            {
                _roundReserves = 0;
                await Task.Yield();
            }
        }
    }

    /// <summary>
    /// Probe/2 queue evidence (09 §4.2), applied once per snapshot. A saturated queue scope excludes that queue's lanes
    /// for the snapshot's life (no Reserve is sent for them), without touching cooldown steps; unrelated queues are
    /// unaffected. A changed control revision is a relevant control change: the queue's cooldowns reset at once. A
    /// snapshot without queue rows (probe/1 or scripted stores) is no evidence and changes nothing.
    /// </summary>
    void ApplyQueueEvidence(ReadySnapshot snapshot)
    {
        _saturated.Clear();
        foreach (var q in snapshot.Queues)
        {
            if (_queueRevisions.TryGetValue(q.Queue, out var seen) && seen != q.ControlRevision)
            {
                Selector.ResetQueue(q.Queue);
            }

            _queueRevisions[q.Queue] = q.ControlRevision;
            if (q.Saturated && !q.Paused)
            {
                _saturated.Add(q.Queue);
            }
        }
    }

    /// <summary>Idle backoff: 100 ms doubling to 2 s, ±20 %; a wake hint ends it early (05 §7.2).</summary>
    async Task IdleAsync(CancellationToken stop)
    {
        var nominal = TimeSpan.FromTicks(Math.Min(WorkerSettings.IdleBackoffCap.Ticks, WorkerSettings.IdleBackoffBase.Ticks << Math.Min(_idleStep, 10)));
        _idleStep++;
        await Wake.WaitAsync(_engine.Clock, EngineClock.Jitter(nominal), stop).ConfigureAwait(false);
    }

    /// <summary>
    /// Reserve commands for one lane until the visit ends (05 §8.6): ≤ 8 blocked candidates per visit, ≤ 32 Reserves
    /// per round. Returns true for an Applied reservation.
    /// </summary>
    async Task<bool> LaneVisitAsync(LaneChoice choice, CapacityTicket ticket)
    {
        var lane = new LaneKey(choice.Queue, choice.Priority);
        var blocked = 0;
        while (true)
        {
            _roundReserves++;
            var claim = ClaimAsync(ticket, lane, _cursors.GetValueOrDefault(lane));
            ClaimTask = claim;
            var (reply, sentAt) = await claim.ConfigureAwait(false);
            var now = _engine.Clock.Now;
            if (reply is null)
            {
                // Control stop with the claim unresolved: recorded; any reservation is left to recovery.
                _engine.Diagnose($"unresolved claim at control stop in pool {Settings.Name}");
                ticket.Return();
                return false;
            }

            if (reply.Status == StoreStatus.Applied && reply.Value is { } reserved)
            {
                Selector.OnApplied(choice);
                _engine.Admit(this, ticket, reserved, reply.Replayed ? null : _engine.DeadlineFrom(sentAt, reserved.Lease, reply.DecidedAt), sentAt);
                return true;
            }

            Selector.OnNotApplied(choice);
            ticket.Return();
            switch (reply.Status)
            {
                case StoreStatus.NoWork:
                    _cursors[lane] = null;
                    _excluded.Add(lane);
                    return false;
                case StoreStatus.QueuePaused:
                    Selector.CoolQueue(choice.Queue, now);
                    return false;
                case StoreStatus.CapacityBlocked:
                    Outcomes.Record(now, blocked: true);
                    _cursors[lane] = reply.Error?.ResumeAfter;
                    switch (Classify(reply.Error?.BlockedScopeId, lane))
                    {
                        case BlockedScope.Queue:
                            Selector.CoolQueue(choice.Queue, now);
                            return false;
                        case BlockedScope.SingleTypeLane:
                            Selector.CoolLane(choice.Queue, choice.Priority, now);
                            return false;
                    }

                    blocked++;
                    if (blocked >= WorkerSettings.BlockedCandidatesPerLaneVisit)
                    {
                        Selector.CoolLane(choice.Queue, choice.Priority, now);
                        return false;
                    }

                    if (_roundReserves >= WorkerSettings.ReservesPerRound || !_engine.AdmissionOpen || Suspended)
                    {
                        return false;
                    }

                    // Continue on the same lane in a new command with a new ticket.
                    ticket = Gate.TryAcquire()!;
                    if (ticket is null)
                    {
                        return false;
                    }

                    continue;
                case StoreStatus.AdmissionClosed or StoreStatus.UnsupportedVersion:
                    _engine.CloseAdmission($"Reserve returned {reply.Status} ({reply.Error?.Code})");
                    return false;
                case StoreStatus.CommandConflict or StoreStatus.InvariantFault or StoreStatus.InvalidRequest:
                    _engine.CloseAdmission($"invariant fault: Reserve returned {reply.Status} ({reply.Error?.Code})");
                    return false;
                default:
                    // CommandExpired (definitive) or another no-effect result: the visit ends.
                    return false;
            }
        }
    }

    enum BlockedScope
    {
        Queue,
        SingleTypeLane,
        Other,
    }

    BlockedScope Classify(Guid? scopeId, LaneKey lane)
    {
        if (scopeId is not { } id || !_engine.ScopeMap.TryGetValue(id, out var scope))
        {
            return BlockedScope.Other;
        }

        if (scope.Kind == ScopeRef.QueueKind && scope.Key == lane.Queue)
        {
            return BlockedScope.Queue;
        }

        if (scope.Kind == ScopeRef.JobTypeKind)
        {
            // A type ceiling cools the lane only when this lane is configured for that single type.
            var types = _engine.Registry.Compiled.Where(j => j.HandlerType is not null && j.Queue == lane.Queue && j.Priority == lane.Priority)
                .Select(j => j.Name).Distinct().ToList();
            if (types.Count == 1 && types[0] == scope.Key)
            {
                return BlockedScope.SingleTypeLane;
            }
        }

        return BlockedScope.Other;
    }

    /// <summary>
    /// One Reserve command resolved to a reply (05 §9.1–§9.2): an unknown commit resubmits the same command; past the
    /// claim resolution budget the claim is surfaced unresolved and this pool stays suspended. Null at control stop.
    /// </summary>
    async Task<(StoreReply<ReservedJob>? Reply, TimeSpan SentAt)> ClaimAsync(CapacityTicket ticket, LaneKey lane, CandidateCursor? after)
    {
        _ = ticket;
        var command = _engine.Command(new ReserveRequest(_engine.WorkerInstanceId, lane.Queue, lane.Priority, _engine.Contracts, after,
            _engine.Options.LeaseDuration));
        var first = _engine.Clock.Now;
        try
        {
            for (var attempt = 0; !_engine.ControlStop.IsCancellationRequested; attempt++)
            {
                var sentAt = _engine.Clock.Now;
                try
                {
                    Interlocked.Increment(ref ReservesSent);
                    var reply = await _engine.CallAsync(_engine.AdmissionIo, attempt == 0 ? ControlPriority.Begin : ControlPriority.Resolution,
                        _engine.Options.ControlRequestTimeout, t => _engine.Store.TryReserveAsync(command, t)).ConfigureAwait(false);
                    return (reply, sentAt);
                }
                catch (Exception ex) when (!_engine.ControlStop.IsCancellationRequested)
                {
                    _engine.Logger.LogDebug(ex, "Runwick Reserve {Command} unresolved; resubmitting the same command.", command.CommandId);
                    if (!_claimUnresolved && _engine.Clock.Now - first >= _engine.Options.ClaimResolutionBudget)
                    {
                        _claimUnresolved = true;
                        _engine.Diagnose($"unresolved claim {command.CommandId} in pool {Settings.Name}: admission suspended until it resolves");
                    }

                    await _engine.Clock.Delay(_engine.Backoff(attempt), _engine.ControlStop).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (_engine.ControlStop.IsCancellationRequested)
        {
        }
        finally
        {
            if (_claimUnresolved)
            {
                _claimUnresolved = false;
                Wake.Set();
            }
        }

        return (null, TimeSpan.Zero);
    }
}
