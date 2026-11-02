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
    bool _batchOff;

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

    /// <summary>Applied ReserveAndBegin commands (05 §9.6), for diagnostics and tests.</summary>
    public long BatchClaims;

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

    /// <summary>
    /// Idle backoff: 100 ms doubling to the cap, ±20 %; a wake hint ends it early (05 §7.2). The cap is 10 s while the
    /// wake listener is healthy and 2 s otherwise (05 Changes 2026-11-02, idle caps), read at each wait.
    /// </summary>
    async Task IdleAsync(CancellationToken stop)
    {
        var nominal = TimeSpan.FromTicks(Math.Min(_engine.IdleBackoffCap.Ticks, WorkerSettings.IdleBackoffBase.Ticks << Math.Min(_idleStep, 10)));
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
            StoreReply<ReservedJob>? reply = null;
            TimeSpan sentAt = default;

            // More than one free slot: claim them in one ReserveAndBegin command (05 §9.6). The batch settles its own
            // Applied reply; a reply like a single Reserve's (no work, paused, all blocked, expired) continues below as
            // one; any other non-Applied reply falls back to Reserve then Begin for this ticket.
            if (BatchTickets(ticket) is { } tickets)
            {
                var batch = BatchAsync(tickets, lane, _cursors.GetValueOrDefault(lane));
                ClaimTask = batch;
                var (batchReply, batchSentAt) = await batch.ConfigureAwait(false);
                switch (Settle(choice, lane, tickets, batchReply, batchSentAt))
                {
                    case BatchResult.Applied:
                        return true;
                    case BatchResult.Stopped:
                        return false;
                    case BatchResult.AsReserve:
                        reply = StoreReply<ReservedJob>.NoEffect(batchReply!.Status, batchReply.Error ?? new StoreError("no_effect"), batchReply.DecidedAt);
                        sentAt = batchSentAt;
                        break;
                }
            }

            if (reply is null)
            {
                var claim = ClaimAsync(ticket, lane, _cursors.GetValueOrDefault(lane));
                ClaimTask = claim;
                (reply, sentAt) = await claim.ConfigureAwait(false);
            }

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

    enum BatchResult
    {
        /// <summary>At least one job began and no blocked candidate cooled the lane; every ticket is settled.</summary>
        Applied,

        /// <summary>Every ticket is settled and the visit ends (control stop, nothing began, or a cooldown).</summary>
        Stopped,

        /// <summary>A no-effect reply a single Reserve also returns; the first ticket continues as that Reserve's.</summary>
        AsReserve,

        /// <summary>Any other reply: the first ticket claims again with Reserve, then Begin.</summary>
        Fallback,
    }

    /// <summary>
    /// The tickets for one ReserveAndBegin command: the given one plus every other free slot, at most
    /// <see cref="IReserveAndBeginStore.MaxJobs"/>. Null (and nothing more acquired) when the store has no batched
    /// claim, the batch is off for this pool, or there is only one free slot: a single slot keeps Reserve then Begin.
    /// </summary>
    List<CapacityTicket>? BatchTickets(CapacityTicket first)
    {
        if (_batchOff || _engine.Store is not IReserveAndBeginStore)
        {
            return null;
        }

        var tickets = new List<CapacityTicket> { first };
        while (tickets.Count < IReserveAndBeginStore.MaxJobs && Gate.TryAcquire() is { } extra)
        {
            tickets.Add(extra);
        }

        return tickets.Count > 1 ? tickets : null;
    }

    /// <summary>
    /// Settles a ReserveAndBegin reply (05 §9.6). Applied: each begun job moves one ticket into a new entry with its
    /// Begin already decided; every other outcome returns its ticket at once (a rate deferral is counted and recorded as
    /// a blocked admission outcome, a capacity-blocked candidate moves the lane cursor and cools as a blocked Reserve
    /// does). Not Applied: every ticket but the first returns, and the first continues as the reply says.
    /// </summary>
    BatchResult Settle(LaneChoice choice, LaneKey lane, List<CapacityTicket> tickets, StoreReply<ClaimedJobs>? reply, TimeSpan sentAt)
    {
        if (reply is null)
        {
            _engine.Diagnose($"unresolved claim at control stop in pool {Settings.Name}");
            tickets.ForEach(t => t.Return());
            return BatchResult.Stopped;
        }

        if (reply.Status != StoreStatus.Applied || reply.Value is not { } claimed)
        {
            foreach (var extra in tickets.Skip(1))
            {
                extra.Return();
            }

            if (reply.Status is StoreStatus.NoWork or StoreStatus.QueuePaused or StoreStatus.CapacityBlocked or StoreStatus.CommandExpired)
            {
                return BatchResult.AsReserve;
            }

            // Anything else (an unsupported operation, a refused request, a fault): this pool claims with Reserve and
            // Begin from now on, and the single path reports what it finds.
            _batchOff = true;
            _engine.Diagnose($"ReserveAndBegin returned {reply.Status} ({reply.Error?.Code}) in pool {Settings.Name}; claiming with Reserve and Begin");
            return BatchResult.Fallback;
        }

        Interlocked.Increment(ref BatchClaims);
        var now = _engine.Clock.Now;
        var applied = false;
        var cool = false;
        var i = 0;
        foreach (var outcome in claimed.Outcomes)
        {
            var ticket = tickets[i++];
            switch (outcome.Status)
            {
                case StoreStatus.Applied when outcome.Reserved is { } reserved && outcome.Begun is { } begun:
                    applied = true;
                    var deadline = reply.Replayed ? (TimeSpan?)null : _engine.DeadlineFrom(sentAt, reserved.Lease, reply.DecidedAt);
                    _engine.Admit(this, ticket, reserved, deadline, sentAt,
                        new BeginReply(StoreReply<BegunExecution>.Applied(begun, reply.DecidedAt, reply.Replayed), null));
                    break;
                case StoreStatus.RateLimited:
                    // T20 committed for this job alone (10 §4): as a deferred Begin, the ticket returns now.
                    ticket.Return();
                    Interlocked.Increment(ref _engine.RateDeferrals);
                    Outcomes.Record(now, blocked: true);
                    break;
                case StoreStatus.CapacityBlocked:
                    ticket.Return();
                    Outcomes.Record(now, blocked: true);
                    _cursors[lane] = outcome.Error?.ResumeAfter;
                    switch (Classify(outcome.Error?.BlockedScopeId, lane))
                    {
                        case BlockedScope.Queue:
                            Selector.CoolQueue(choice.Queue, now);
                            cool = true;
                            break;
                        case BlockedScope.SingleTypeLane:
                            Selector.CoolLane(choice.Queue, choice.Priority, now);
                            cool = true;
                            break;
                    }

                    break;
                default:
                    // Begin closed the attempt (cancelled before start, budget exhausted): nothing to run.
                    ticket.Return();
                    break;
            }
        }

        // Fewer candidates than slots: the spare tickets return.
        foreach (var spare in tickets.Skip(i))
        {
            spare.Return();
        }

        if (applied)
        {
            Selector.OnApplied(choice);
        }
        else
        {
            Selector.OnNotApplied(choice);
        }

        return applied && !cool ? BatchResult.Applied : BatchResult.Stopped;
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
    Task<(StoreReply<ReservedJob>? Reply, TimeSpan SentAt)> ClaimAsync(CapacityTicket ticket, LaneKey lane, CandidateCursor? after)
    {
        _ = ticket;
        var command = _engine.Command(new ReserveRequest(_engine.WorkerInstanceId, lane.Queue, lane.Priority, _engine.Contracts, after,
            _engine.Options.LeaseDuration));
        return ResolveAsync(command.CommandId, "Reserve", t => _engine.Store.TryReserveAsync(command, t));
    }

    /// <summary>One ReserveAndBegin command for <paramref name="tickets"/>.Count slots, resolved as a Reserve is (05 §9.6).</summary>
    Task<(StoreReply<ClaimedJobs>? Reply, TimeSpan SentAt)> BatchAsync(List<CapacityTicket> tickets, LaneKey lane, CandidateCursor? after)
    {
        var store = (IReserveAndBeginStore)_engine.Store;
        var command = _engine.Command(new ReserveAndBeginRequest(_engine.WorkerInstanceId, lane.Queue, lane.Priority, _engine.Contracts, after,
            _engine.Options.LeaseDuration, tickets.Count));
        return ResolveAsync(command.CommandId, "ReserveAndBegin", t => store.ReserveAndBeginAsync(command, t));
    }

    async Task<(StoreReply<T>? Reply, TimeSpan SentAt)> ResolveAsync<T>(Guid commandId, string operation, Func<CancellationToken, Task<StoreReply<T>>> send)
        where T : class
    {
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
                        _engine.Options.ControlRequestTimeout, send).ConfigureAwait(false);
                    return (reply, sentAt);
                }
                catch (Exception ex) when (!_engine.ControlStop.IsCancellationRequested)
                {
                    _engine.Logger.LogDebug(ex, "Runwick {Operation} {Command} unresolved; resubmitting the same command.", operation, commandId);
                    if (!_claimUnresolved && _engine.Clock.Now - first >= _engine.Options.ClaimResolutionBudget)
                    {
                        _claimUnresolved = true;
                        _engine.Diagnose($"unresolved claim {commandId} in pool {Settings.Name}: admission suspended until it resolves");
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
