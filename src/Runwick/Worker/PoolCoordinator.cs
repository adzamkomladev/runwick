using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Worker;

/// <summary>
/// One pool (05 §8): tickets, lane selection, cursors and cooldowns. Selection and claim settlement are serial per
/// pool; up to <see cref="WorkerOptions.ClaimsInFlight"/> batched claims may wait for their replies at once (05 §9.7), any other
/// claim is the only one outstanding when it is sent. Execution is concurrent.
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
    int _unresolved;
    int _inFlight;
    readonly System.Collections.Concurrent.ConcurrentDictionary<Task, byte> _claims = new();
    bool _batchOff;
    readonly PriorityGate _claimGate = new(1);

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

    /// <summary>The claims currently in flight (or being resolved, or settling); shutdown waits for them.</summary>
    public ICollection<Task> ClaimTasks => _claims.Keys;

    /// <summary>Batched claims sent and not yet settled (05 §9.7).</summary>
    public int ClaimsInFlight => Volatile.Read(ref _inFlight);

    /// <summary>The most claims this pool has had in flight at once (diagnostics and tests).</summary>
    public int ClaimsInFlightHighWater;

    public long ReservesSent;

    /// <summary>Applied ReserveAndBegin commands (05 §9.6), for diagnostics and tests.</summary>
    public long BatchClaims;

    /// <summary>Finish-and-claim attempts the pool declined, by reason (diagnostics): claim in flight, admission closed, no fresh hint, lane shape.</summary>
    public long InlineDeclinedBusy, InlineDeclinedClosed, InlineDeclinedStale, InlineDeclinedLane;

    /// <summary>Fresh claims stop while a Reserve or Begin of this pool is unresolved (05 §15.2).</summary>
    public bool Suspended => Volatile.Read(ref _suspensions) > 0 || ClaimUnresolved;

    public bool ClaimUnresolved => Volatile.Read(ref _unresolved) > 0;

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
            if (!_engine.AdmissionOpen || Suspended || !MayClaim() || Gate.TryAcquire() is not { } ticket)
            {
                if (await Wake.WaitAsync(_engine.Clock, TimeSpan.FromSeconds(1), stop).ConfigureAwait(false))
                {
                    _idleStep = 0;
                }

                continue;
            }

            // The claim lock keeps a finish-and-claim call of the completion lane (05 §12.7) out of the selection and claim below.
            (bool Idle, TimeSpan? Until) next;
            PriorityGate.Slot claimSlot;
            try
            {
                claimSlot = await _claimGate.AcquireAsync(0, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                ticket.Return();
                throw;
            }

            using (claimSlot)
            {
                next = await OpportunityAsync(ticket, stop).ConfigureAwait(false);
            }

            if (next.Idle)
            {
                await IdleAsync(stop, next.Until).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Another claim may be sent: none is in flight, or fewer than the limit are and at least target ÷ limit slots are free, so a claim
    /// sent beside another is still a large one (05 §9.7). Small claims cost the store more per job than the waiting they save.
    /// </summary>
    bool MayClaim()
    {
        var inFlight = ClaimsInFlight;
        var limit = _engine.Options.ClaimsInFlight;
        return inFlight == 0 || (inFlight < limit && Gate.Target - Gate.Outstanding >= Math.Max(1, Gate.Target / limit));
    }

    /// <summary>One opportunity with a ticket in hand: the probe view, the lane choice and the claim. Idle: the pool then waits (until the due time when known).</summary>
    async Task<(bool Idle, TimeSpan? Until)> OpportunityAsync(CapacityTicket ticket, CancellationToken stop)
    {
        ReadyView view;
        try
        {
            view = await _engine.Readiness.GetAsync(stop).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ticket.Return();
            if (stop.IsCancellationRequested)
            {
                throw;
            }

            _engine.Logger.LogDebug(ex, "Runwick readiness probe failed.");
            return (true, null);
        }

        if (!_engine.AdmissionOpen || Suspended)
        {
            ticket.Return();
            return (false, null);
        }

        var now = _engine.Clock.Now;
        var choice = SelectLane(view, now, out var readyLanes);
        if (choice is null)
        {
            ticket.Return();
            TimeSpan? untilDue = null;
            if (readyLanes == 0)
            {
                // Nothing claimable in this hint: the next opportunity probes afresh. A store that can name the next job to
                // fall due lets the pool sleep until then instead of on its idle backoff.
                _engine.Readiness.Invalidate();
                if (view.Snapshot.NextDueAt is { } due)
                {
                    untilDue = due - view.Snapshot.ObservedAt - (now - view.ReceivedAt);
                }
            }

            return (true, untilDue);
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

        return (false, null);
    }

    /// <summary>
    /// The lane the next claim is for, from a probe view: applies the probe exclusions, queue evidence and known releases, then asks
    /// the selector. <paramref name="readyLanes"/> is how many lanes remained claimable before the selector's cooldowns.
    /// </summary>
    LaneChoice? SelectLane(ReadyView view, TimeSpan now, out int readyLanes)
    {
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

        readyLanes = ages.Count;
        return Selector.Choose(ages, now);
    }

    /// <summary>A claim prepared for a finish-and-claim call (05 §12.7); holds the pool's claim lock until disposed.</summary>
    internal sealed class InlineClaim(PoolCoordinator pool, PriorityGate.Slot slot, LaneChoice choice, LaneKey lane, List<CapacityTicket> tickets,
        StoreCommand<ReserveAndBeginRequest> command) : IDisposable
    {

        public LaneChoice Choice => choice;

        public LaneKey Lane => lane;

        /// <summary>The slots of the finishing attempts that the claim may fill, in outcome order.</summary>
        public List<CapacityTicket> Tickets => tickets;

        public StoreCommand<ReserveAndBeginRequest> Command => command;

        public PoolCoordinator Pool => pool;

        public void Dispose() => slot.Dispose();
    }

    /// <summary>
    /// Prepares a claim of <paramref name="tickets"/>.Count jobs to ride on a finish call (05 §12.7), or null when the pool would not
    /// claim right now with exactly this shape: admission closed or draining, a claim in flight (the lock is taken without waiting), no
    /// fresh readiness hint (no probe is started), more than one claimable lane or an aging pick (shares divide admissions, so each
    /// admission is its own selection), or no batched claim in the store.
    /// </summary>
    internal InlineClaim? TryBeginInlineClaim(List<CapacityTicket> tickets)
    {
        if (tickets.Count == 0)
        {
            return null;
        }

        if (ClaimsInFlight > 0 || _claimGate.TryAcquire() is not { } slot)
        {
            Interlocked.Increment(ref InlineDeclinedBusy);
            return null;
        }

        InlineClaim? claim = null;
        try
        {
            if (_batchOff || _engine.Store is not IReserveAndBeginStore || !_engine.AdmissionOpen || _engine.Draining || Suspended)
            {
                Interlocked.Increment(ref InlineDeclinedClosed);
                return null;
            }

            if (!_engine.Readiness.TryGetFresh(out var view))
            {
                Interlocked.Increment(ref InlineDeclinedStale);
                return null;
            }

            var choice = SelectLane(view, _engine.Clock.Now, out _);
            if (choice is null || choice.Aging || choice.EligibleQueues.Count != 1 || choice.EligibleLanes.Count != 1)
            {
                Interlocked.Increment(ref InlineDeclinedLane);
                return null;
            }

            var lane = new LaneKey(choice.Queue, choice.Priority);
            var command = _engine.Command(new ReserveAndBeginRequest(_engine.WorkerInstanceId, lane.Queue, lane.Priority, _engine.Contracts,
                _cursors.GetValueOrDefault(lane), _engine.Options.LeaseDuration, tickets.Count));
            claim = new InlineClaim(this, slot, choice, lane, tickets, command);
            return claim;
        }
        finally
        {
            if (claim is null)
            {
                slot.Dispose();
            }
        }
    }

    /// <summary>The call is still running or being resolved: shutdown waits for it as it does for a Reserve in flight.</summary>
    internal void TrackClaim(Task task)
    {
        if (_claims.TryAdd(task, 0))
        {
            _ = task.ContinueWith(t => _claims.TryRemove(t, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Settles the claim half of a finish-and-claim call. A null reply is an unknown outcome: the same claim command is resubmitted
    /// alone, as any claim is (05 §9.1), so a committed claim replays and an uncommitted one applies. Applied: each begun job takes the
    /// slot of a finishing attempt (the slot moves to the new attempt, so the finisher's retirement no longer frees it). Anything else
    /// leaves the finishing attempts' slots where they are, and the pool's state follows the reply as a claim of the loop's would.
    /// </summary>
    internal async Task SettleInlineAsync(InlineClaim claim, StoreReply<ClaimedJobs>? reply, TimeSpan sentAt)
    {
        if (reply is null)
        {
            var store = (IReserveAndBeginStore)_engine.Store;
            (reply, sentAt) = await ResolveAsync(claim.Command.CommandId, "ReserveAndBegin", t => store.ReserveAndBeginAsync(claim.Command, t)).ConfigureAwait(false);
            if (reply is null)
            {
                return;
            }
        }

        if (reply.Status == StoreStatus.Applied && reply.Value is not null)
        {
            var moved = new List<CapacityTicket>();
            foreach (var t in claim.Tickets)
            {
                if (t.Transfer() is { } slot)
                {
                    moved.Add(slot);
                }
            }

            if (Settle(claim.Choice, claim.Lane, moved, reply, sentAt) == BatchResult.Applied)
            {
                _idleStep = 0;
            }

            Wake.Set();
            return;
        }

        NotApplied(claim.Choice, claim.Lane, reply);
    }

    /// <summary>A claim that took nothing and was not followed by another claim in the same visit: the pool state follows the reply.</summary>
    void NotApplied(LaneChoice choice, LaneKey lane, StoreReply<ClaimedJobs> reply)
    {
        var now = _engine.Clock.Now;
        Selector.OnNotApplied(choice);
        switch (reply.Status)
        {
            case StoreStatus.NoWork:
                _cursors[lane] = null;
                _excluded.Add(lane);
                break;
            case StoreStatus.QueuePaused:
                Selector.CoolQueue(choice.Queue, now);
                break;
            case StoreStatus.CapacityBlocked:
                Outcomes.Record(now, blocked: true);
                _cursors[lane] = reply.Error?.ResumeAfter;
                switch (Classify(reply.Error?.BlockedScopeId, lane))
                {
                    case BlockedScope.Queue:
                        Selector.CoolQueue(choice.Queue, now);
                        break;
                    case BlockedScope.SingleTypeLane:
                        Selector.CoolLane(choice.Queue, choice.Priority, now);
                        break;
                }

                break;
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
    /// wake listener is healthy and 2 s otherwise (05 Changes 2026-11-02, idle caps), read at each wait. A store that names the next
    /// due job (<paramref name="untilDue"/>) ends it at that time if sooner.
    /// </summary>
    async Task IdleAsync(CancellationToken stop, TimeSpan? untilDue = null)
    {
        var nominal = TimeSpan.FromTicks(Math.Min(_engine.IdleBackoffCap.Ticks, WorkerSettings.IdleBackoffBase.Ticks << Math.Min(_idleStep, 10)));
        _idleStep++;
        var wait = EngineClock.Jitter(nominal);
        if (untilDue is { } due && due < wait)
        {
            // Never below the probe spacing, so a lane that is due but not claimable cannot make a probe loop.
            wait = TimeSpan.FromTicks(Math.Max(due.Ticks, WorkerSettings.ProbeSpacing.Ticks));
        }

        if (await Wake.WaitAsync(_engine.Clock, wait, stop).ConfigureAwait(false))
        {
            _idleStep = 0;
            // A cached lane can have been set aside after NoWork while another lane is cooling. A fresh
            // snapshot clears the exclusions before selection; the group's probe spacing still applies.
            _engine.Readiness.Invalidate();
        }
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

            // Claim the free slots in one ReserveAndBegin command (05 §9.6). The batch settles its own
            // Applied reply; a reply like a single Reserve's (no work, paused, all blocked, expired) continues below as
            // one; any other non-Applied reply falls back to Reserve then Begin for this ticket.
            if (BatchTickets(ticket, choice) is { } tickets)
            {
                var batch = BatchAsync(tickets, lane, _cursors.GetValueOrDefault(lane));
                if (_engine.Options.ClaimsInFlight > 1)
                {
                    // Pipelined (05 §9.7): the loop goes on selecting while this claim waits; its reply settles under the claim lock.
                    var n = Interlocked.Increment(ref _inFlight);
                    if (n > ClaimsInFlightHighWater)
                    {
                        ClaimsInFlightHighWater = n;
                    }

                    TrackClaim(SettleLaterAsync(choice, lane, tickets, batch));
                    // Not yet known to be Applied: the idle back-off resets when the reply settles as Applied, as for a claim awaited here.
                    return false;
                }

                TrackClaim(batch);
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
                if (!_engine.AdmissionOpen || Suspended)
                {
                    ticket.Return();
                    return false;
                }

                var claim = ClaimAsync(ticket, lane, _cursors.GetValueOrDefault(lane));
                TrackClaim(claim);
                (reply, sentAt) = await claim.ConfigureAwait(false);
            }

            var now = _engine.Clock.Now;
            if (reply is null)
            {
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

    /// <summary>
    /// Settles a pipelined batched claim once its reply is in (05 §9.7), under the claim lock as a claim of the loop's settles. A
    /// reply that takes nothing returns every ticket and updates the pool as the inline claim's does; the visit does not continue.
    /// </summary>
    async Task SettleLaterAsync(LaneChoice choice, LaneKey lane, List<CapacityTicket> tickets, Task<(StoreReply<ClaimedJobs>? Reply, TimeSpan SentAt)> batch)
    {
        try
        {
            var (reply, sentAt) = await batch.ConfigureAwait(false);
            // No cancellation: the tickets must be settled, and every holder of the lock releases it at control stop.
            using var slot = await _claimGate.AcquireAsync(0, CancellationToken.None).ConfigureAwait(false);
            switch (Settle(choice, lane, tickets, reply, sentAt))
            {
                case BatchResult.Applied:
                    _idleStep = 0;
                    break;
                case BatchResult.AsReserve:
                    tickets[0].Return();
                    NotApplied(choice, lane, reply!);
                    break;
                case BatchResult.Fallback:
                    tickets[0].Return();
                    break;
            }
        }
        catch (Exception ex)
        {
            _engine.Logger.LogError(ex, "Runwick pipelined claim settlement failed in pool {Pool}.", Settings.Name);
            throw;
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            Wake.Set();
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
    /// claim or the batch is off for this pool.
    /// Also null unless the selection had exactly one candidate lane and was not an aging pick: queue and lane shares
    /// divide admissions (05 §8.4), so with several ready lanes every admission is its own selection.
    /// </summary>
    List<CapacityTicket>? BatchTickets(CapacityTicket first, LaneChoice choice)
    {
        if (_batchOff || _engine.Store is not IReserveAndBeginStore || choice.Aging || choice.EligibleQueues.Count != 1
            || choice.EligibleLanes.Count != 1)
        {
            return null;
        }

        var tickets = new List<CapacityTicket> { first };
        while (tickets.Count < IReserveAndBeginStore.MaxJobs && Gate.TryAcquire() is { } extra)
        {
            tickets.Add(extra);
        }

        return tickets;
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
        var applied = 0;
        var cool = false;
        var i = 0;
        foreach (var outcome in claimed.Outcomes)
        {
            var ticket = tickets[i++];
            switch (outcome.Status)
            {
                case StoreStatus.Applied when outcome.Reserved is { } reserved && outcome.Begun is { } begun:
                    applied++;
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

        // One commit per begun job: the lane was the only candidate, so scores stay balanced and the queue's aging
        // count advances once per admission, as n single claims would leave them.
        for (var k = 0; k < applied; k++)
        {
            Selector.OnApplied(choice);
        }

        if (applied == 0)
        {
            Selector.OnNotApplied(choice);
        }

        return applied > 0 && !cool ? BatchResult.Applied : BatchResult.Stopped;
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
        var submitted = false;
        var unresolved = false;
        try
        {
            for (var attempt = 0; !_engine.ControlStop.IsCancellationRequested; attempt++)
            {
                var sentAt = _engine.Clock.Now;
                try
                {
                    Interlocked.Increment(ref ReservesSent);
                    var reply = await _engine.CallAsync<StoreReply<T>?>(_engine.AdmissionIo,
                        submitted ? ControlPriority.Resolution : ControlPriority.Begin, _engine.Options.ControlRequestTimeout, t =>
                        {
                            // Waiting for an I/O slot may have crossed a stop. Resolve commands already submitted
                            // with the same identity even while fresh admission is closed.
                            if (!submitted && (!_engine.AdmissionOpen || Suspended))
                            {
                                return Task.FromResult<StoreReply<T>?>(null);
                            }

                            submitted = true;
                            return send(t)!;
                        }).ConfigureAwait(false);
                    return (reply, sentAt);
                }
                catch (Exception ex) when (!_engine.ControlStop.IsCancellationRequested)
                {
                    _engine.Logger.LogDebug(ex, "Runwick {Operation} {Command} unresolved; resubmitting the same command.", operation, commandId);
                    if (submitted && !unresolved && _engine.Clock.Now - first >= _engine.Options.ClaimResolutionBudget)
                    {
                        unresolved = true;
                        Interlocked.Increment(ref _unresolved);
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
            if (unresolved)
            {
                Interlocked.Decrement(ref _unresolved);
                Wake.Set();
            }
        }

        if (submitted)
        {
            _engine.Diagnose($"unresolved claim at control stop in pool {Settings.Name}");
        }

        return (null, TimeSpan.Zero);
    }
}
