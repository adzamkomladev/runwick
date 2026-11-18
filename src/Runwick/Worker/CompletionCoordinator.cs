using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Telemetry;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Worker;

/// <summary>How a published Finish ended locally.</summary>
internal enum SettlementResult
{
    Applied,
    Stale,
    Unresolved,
}

/// <summary>
/// Finish submission and resolution (05 §12): a bounded ready channel (never drops), workers equal to the settlement
/// lane, the frozen command (same ID, same body) on every resubmission, and the completion resolution budget.
/// <para>
/// Batched finish (05 §12.5, PF-06 batched finish): when the store offers <see cref="IFinishManyStore"/>, a worker that
/// takes a Finish from the channel also takes every other Finish already waiting there (up to
/// <see cref="IFinishManyStore.MaxItems"/>) and sends them as one FinishMany call. Each item is its own frozen Finish/1
/// command; a failed call, a non-Applied reply or an item's transient outcome falls back to resolving that item exactly as
/// a single Finish (same command, same budget).
/// </para>
/// </summary>
internal sealed class CompletionCoordinator(WorkerEngine engine)
{
    // Buffered finish (G1): more Finishes can be waiting than there are slots, so the channel is sized to the buffer bound.
    readonly Channel<AttemptEntry> _ready = Channel.CreateBounded<AttemptEntry>(
        new BoundedChannelOptions(Math.Max(Math.Max(1, engine.Settings.GroupMax), engine.Options.BufferedFinish ? engine.Options.FinishBufferMaxCount : 0))
        { FullMode = BoundedChannelFullMode.Wait });
    readonly bool _buffered = engine.Options.BufferedFinish;
    readonly bool _combined = engine.Options.CombinedFinishClaim;
    bool _combinedOff;
    readonly PriorityGate _collector = new(1);
    readonly Lock _bufferLock = new();
    readonly Queue<AttemptEntry> _bufferedOrder = new();

    /// <summary>The batch a buffered collector is filling (guarded by the buffer lock); a claim of the pool's loop may take it.</summary>
    readonly List<AttemptEntry> _collecting = [];
    long _bufferedBytes;
    int _bufferedCount;
    int _diagnosticResolvers;
    bool _batchOff;

    /// <summary>A buffered Finish older than this stops further slot give-backs until it settles (G1 age bound).</summary>
    static readonly TimeSpan BufferStallAge = TimeSpan.FromSeconds(1);

    /// <summary>Finishes whose slot was given back and that the store has not yet settled (G1).</summary>
    public int Buffered => Volatile.Read(ref _bufferedCount);

    /// <summary>The most buffered Finishes seen at once (G1 diagnostics and tests).</summary>
    public int BufferedHighWater;

    /// <summary>Finishes that kept their slot because a count, byte or age bound was hit (G1).</summary>
    public long BufferFullRefusals;

    /// <summary>Flushes sent because the batch was full, the flush interval passed, or a drain or lease margin forced it (G1).</summary>
    public long FlushesFull, FlushesByAge, FlushesForced;

    public int Pending => _ready.Reader.Count;

    /// <summary>FinishMany calls sent (05 §12.5), for diagnostics and tests; each item also counts in <c>FinishesSent</c>.</summary>
    public long BatchesSent;

    /// <summary>Applied FinishMany replies (every item decided in one call).</summary>
    public long BatchesApplied;

    /// <summary>Items settled from an Applied FinishMany reply without a per-item resolution.</summary>
    public long BatchedItemsSettled;

    /// <summary>Finish-and-claim calls sent (05 §12.7), for diagnostics and tests.</summary>
    public long CombinedCalls;

    /// <summary>Applied finish-and-claim calls whose claim half started at least one job.</summary>
    public long CombinedClaimed;

    /// <summary>Finish-and-claim calls the pool's claim loop sent carrying waiting Finishes, and the Finishes they carried (05 §12.7).</summary>
    public long LoopCarriedCalls, LoopCarriedFinishes;

    /// <summary>Finish-and-claim calls that were refused or failed and left their finishes to be resolved one by one.</summary>
    public long CombinedFallbacks;

    /// <summary>The last FinishMany failure that fell back to per-item resolution (diagnostics and tests).</summary>
    public string? LastBatchFailure { get; private set; }

    public void Start()
    {
        for (var i = 0; i < WorkerSettings.SettlementLane; i++)
        {
            engine.Supervise("completion", WorkerAsync, critical: true);
        }
    }

    /// <summary>Publishes the immutable Finish after cleanup (05 §10.1 step 8); the budget runs from here.</summary>
    public async Task<SettlementResult> SettleAsync(AttemptEntry entry, FinishRequest finish)
    {
        entry.FrozenFinish = finish;
        entry.FinishCommand = engine.Command(finish);
        entry.FinishPublishedAt = engine.Clock.Now;
        if (!entry.TryMove(AttemptPhase.Finishing))
        {
            return SettlementResult.Unresolved;
        }

        if (_buffered)
        {
            GiveBackSlot(entry, finish);
        }

        var started = Stopwatch.GetTimestamp();
        using var span = T.Span("settle " + entry.Reserved.Queue);
        span.Activity?.SetTag("messaging.system", "runwick").SetTag("messaging.operation.type", "settle").SetTag("messaging.operation.name", "finish")
            .SetTag("messaging.destination.name", entry.Reserved.Queue);
        SettlementResult? result = null;
        try
        {
            await _ready.Writer.WriteAsync(entry, engine.ControlStop).ConfigureAwait(false);
            result = await entry.Settled.Task.ConfigureAwait(false);
            return result.Value;
        }
        finally
        {
            Unbuffer(entry);

            // After the decision: the measurement never shapes it (18 §1).
            var name = result switch
            {
                SettlementResult.Applied => "confirmed",
                SettlementResult.Stale => "lost",
                _ => "unresolved",
            };
            T.SettlementDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, name);
            span.Activity?.SetTag("runwick.settlement.result", name).SetTag("runwick.storage.attempts", Volatile.Read(ref entry.FinishAttempts));
            entry.Span?.SetTag("runwick.settlement.result", name);
            if (result != SettlementResult.Applied)
            {
                span.Activity?.SetStatus(ActivityStatusCode.Error);
                entry.Span?.SetStatus(ActivityStatusCode.Error);
            }

            if (name == "unresolved")
            {
                RunwickLog.SettlementUnresolved(engine.Logger, entry.Lease.JobId, entry.AttemptId, WorkerEngine.OutcomeName(finish.Outcome),
                    (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
        }
    }

    async Task WorkerAsync()
    {
        while (await NextBatchAsync().ConfigureAwait(false) is { } taken)
        {
            var batch = _combined ? await TrySettleCombinedAsync(taken).ConfigureAwait(false) : taken;
            if (batch.Count == 0)
            {
                continue;
            }

            if (batch.Count == 1)
            {
                Complete(batch[0], await ResolveAsync(batch[0], detached: false).ConfigureAwait(false));
                continue;
            }

            await SettleBatchAsync(batch).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The next Finishes to send together; null when the engine stops. Default: the first Finish plus whatever already waits, no
    /// delay (05 §12.5). Buffered finish (G1): one worker at a time collects, waiting up to the flush interval for company.
    /// </summary>
    async Task<List<AttemptEntry>?> NextBatchAsync()
    {
        using var turn = _buffered ? await _collector.AcquireAsync(0, engine.ControlStop).ConfigureAwait(false) : null;
        {
            while (await _ready.Reader.WaitToReadAsync(engine.ControlStop).ConfigureAwait(false))
            {
                if (!_ready.Reader.TryRead(out var first))
                {
                    continue;
                }

                // What is already waiting joins this call. A Finish that finds the lane idle is sent at once, alone, unless
                // buffered finish asks it to wait for company.
                var batch = new List<AttemptEntry> { first };
                if (!Volatile.Read(ref _batchOff) && engine.Store is IFinishManyStore)
                {
                    if (_buffered)
                    {
                        batch = await CollectAsync(first).ConfigureAwait(false);
                        if (batch.Count == 0)
                        {
                            // A claim of the pool's loop took every Finish this worker was collecting (05 §12.7).
                            continue;
                        }
                    }
                    else
                    {
                        while (batch.Count < IFinishManyStore.MaxItems && _ready.Reader.TryRead(out var more))
                        {
                            batch.Add(more);
                        }
                    }
                }

                return batch;
            }

            return null;
        }
    }

    /// <summary>
    /// Collects a batch from <paramref name="first"/> until it is full, its oldest Finish has waited the flush interval, the engine
    /// drains, or an attempt is close to its lease safety margin: a partial batch never waits past those (plan §5.1 rule 5). The batch
    /// being filled is shared under the buffer lock, so a claim of the pool's loop can take it (<see cref="TakeWaiting"/>); an emptied
    /// batch returns empty.
    /// </summary>
    async Task<List<AttemptEntry>> CollectAsync(AttemptEntry first)
    {
        lock (_bufferLock)
        {
            _collecting.Add(first);
        }

        while (true)
        {
            TimeSpan wait;
            lock (_bufferLock)
            {
                while (_collecting.Count < IFinishManyStore.MaxItems && _ready.Reader.TryRead(out var more))
                {
                    _collecting.Add(more);
                }

                if (_collecting.Count == 0)
                {
                    return [];
                }

                var now = engine.Clock.Now;
                var flushAt = _collecting[0].FinishPublishedAt!.Value + engine.Options.FinishFlushInterval;
                var full = _collecting.Count >= IFinishManyStore.MaxItems;
                var forced = !full && (engine.Draining || engine.ControlStop.IsCancellationRequested || NearLeaseMargin(_collecting, now));
                if (full || forced || now >= flushAt)
                {
                    Interlocked.Increment(ref full ? ref FlushesFull : ref forced ? ref FlushesForced : ref FlushesByAge);
                    List<AttemptEntry> batch = [.. _collecting];
                    _collecting.Clear();
                    return batch;
                }

                wait = flushAt - now;
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(engine.ControlStop);
            var arrival = _ready.Reader.WaitToReadAsync(cts.Token).AsTask();
            var tick = engine.Clock.Delay(wait, cts.Token);
            await Task.WhenAny(arrival, tick).ConfigureAwait(false);
            await cts.CancelAsync().ConfigureAwait(false);
            foreach (var t in new Task[] { arrival, tick })
            {
                try
                {
                    await t.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Takes up to <paramref name="max"/> Finishes that wait to be sent (05 §12.7): first those a buffered collector is holding, oldest
    /// first, then those still in the ready channel. Each is removed exactly once (under the buffer lock, or by the channel's own read),
    /// so it is sent by the taker alone.
    /// </summary>
    List<AttemptEntry> TakeWaiting(int max)
    {
        var taken = new List<AttemptEntry>();
        lock (_bufferLock)
        {
            var n = Math.Min(max, _collecting.Count);
            taken.AddRange(_collecting.GetRange(0, n));
            _collecting.RemoveRange(0, n);
            while (taken.Count < max && _ready.Reader.TryRead(out var e))
            {
                taken.Add(e);
            }
        }

        return taken;
    }

    bool NearLeaseMargin(List<AttemptEntry> batch, TimeSpan now)
    {
        foreach (var e in batch)
        {
            if (e.Deadline is { } d && d - now <= engine.Settings.RenewalLead)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Buffered finish (G1): frees the local slot as soon as the Finish is published, if the buffer has room. The entry stays
    /// tracked, renewed and fenced until the store settles it; only the capacity ticket is given back. A refused give-back
    /// (count, bytes or age bound) leaves the ticket held until retirement, which is the default behaviour and the refill stop.
    /// </summary>
    void GiveBackSlot(AttemptEntry entry, FinishRequest finish)
    {
        var size = 256 + 2 * ((finish.Error?.Code.Length ?? 0) + (finish.Error?.ExceptionType.Length ?? 0) + (finish.Error?.Message.Length ?? 0)
            + (finish.Error?.StackTrace?.Length ?? 0)) + (finish.Usage is null ? 0 : 64);
        lock (_bufferLock)
        {
            while (_bufferedOrder.TryPeek(out var head) && head.BufferedBytes == 0)
            {
                _bufferedOrder.Dequeue();
            }

            var stalled = _bufferedOrder.TryPeek(out var oldest) && engine.Clock.Now - oldest.FinishPublishedAt!.Value > BufferStallAge;
            if (_bufferedCount >= engine.Options.FinishBufferMaxCount || _bufferedBytes + size > engine.Options.FinishBufferMaxBytes || stalled)
            {
                Interlocked.Increment(ref BufferFullRefusals);
                return;
            }

            _bufferedCount++;
            _bufferedBytes += size;
            entry.BufferedBytes = size;
            _bufferedOrder.Enqueue(entry);
            if (_bufferedCount > BufferedHighWater)
            {
                BufferedHighWater = _bufferedCount;
            }
        }

        entry.Ticket.Release();
    }

    void Unbuffer(AttemptEntry entry)
    {
        if (!_buffered)
        {
            return;
        }

        lock (_bufferLock)
        {
            if (entry.BufferedBytes > 0)
            {
                _bufferedCount--;
                _bufferedBytes -= entry.BufferedBytes;
                entry.BufferedBytes = 0;
            }
        }
    }

    static void Complete(AttemptEntry entry, SettlementResult? result)
    {
        if (result is { } r)
        {
            entry.Settled.TrySetResult(r);
        }
    }

    TimeSpan BudgetEnd(AttemptEntry entry) => entry.FinishPublishedAt!.Value + engine.Options.CompletionResolutionBudget;

    /// <summary>
    /// Finish-and-claim carried by a finisher (05 §12.7). The Finishes of one pool that already wait, plus a claim of that many jobs for
    /// the slots they are about to free, go to the store in one call. Returns the entries this method did not take (all of them when it
    /// declines: not supported, no fresh readiness hint, a claim in flight, a lane shape the loop would not batch), which settle the
    /// ordinary way. Each Finish keeps its own command and receipt and the claim keeps its own, so every failure resolves one command at a
    /// time: a refused or lost call resolves each Finish alone and the claim by resubmitting the same command.
    /// </summary>
    async Task<List<AttemptEntry>> TrySettleCombinedAsync(List<AttemptEntry> taken)
    {
        if (Volatile.Read(ref _combinedOff) || engine.Store is not IFinishAndClaimStore store || engine.Draining)
        {
            return taken;
        }

        var now = engine.Clock.Now;
        var pool = taken[0].Pool;
        var members = taken.Where(e => ReferenceEquals(e.Pool, pool) && now < BudgetEnd(e)).Take(IFinishAndClaimStore.MaxFinishes).ToList();
        var tickets = members.Select(e => e.Ticket).Where(t => !t.IsReleased).ToList();
        if (tickets.Count == 0 || pool.TryBeginInlineClaim(tickets) is not { } inline)
        {
            return taken;
        }

        var rest = taken.Except(members).ToList();
        var done = new TaskCompletionSource();
        pool.TrackClaim(done.Task);
        Combined? call = null;
        try
        {
            call = await SendCombinedAsync(store, members, inline.Command).ConfigureAwait(false);

            // The claim half is settled before any Finish completes, so a finisher's retirement finds its slot already moved.
            if (!call.Refused && !engine.ControlStop.IsCancellationRequested)
            {
                await pool.SettleInlineAsync(inline, call.Claim, call.SentAt).ConfigureAwait(false);
            }
        }
        finally
        {
            inline.Dispose();
            done.TrySetResult();
            call?.CompleteSettled();
        }

        await ResolveAloneAsync(call.Fallback, call.RetryDelay).ConfigureAwait(false);
        return rest;
    }

    /// <summary>
    /// Finish-and-claim carried by the pool's loop (05 §12.7): the loop is about to send <paramref name="claim"/> for its own free slots
    /// and takes the Finishes that wait (in a buffered collector or the ready channel) into the same call. Null when nothing waits or the
    /// call is off, and the loop sends its claim alone. Otherwise the claim's reply as the loop's own claim would read it; a null reply
    /// means the call was refused, lost or not sent, and the loop resolves the same claim command alone (<c>MaybeCommitted</c> when it
    /// was sent and not refused). Taken Finishes complete here: Applied items at once, the rest resolve one by one in the background so
    /// the loop is not held. The claim fills the loop's own free slots, so no slot moves between the two halves.
    /// </summary>
    internal async Task<(StoreReply<ClaimedJobs>? Reply, TimeSpan SentAt, bool MaybeCommitted)?> TryClaimWithFinishesAsync(
        StoreCommand<ReserveAndBeginRequest> claim)
    {
        if (!_combined || Volatile.Read(ref _combinedOff) || engine.Store is not IFinishAndClaimStore store)
        {
            return null;
        }

        var taken = TakeWaiting(IFinishAndClaimStore.MaxFinishes);
        if (taken.Count == 0)
        {
            return null;
        }

        var now = engine.Clock.Now;
        var members = taken.Where(e => now < BudgetEnd(e)).ToList();
        var alone = taken.Except(members).ToList();
        Combined? call = null;
        if (members.Count > 0)
        {
            Interlocked.Increment(ref LoopCarriedCalls);
            Interlocked.Add(ref LoopCarriedFinishes, members.Count);
            try
            {
                call = await SendCombinedAsync(store, members, claim).ConfigureAwait(false);
            }
            finally
            {
                call?.CompleteSettled();
            }

            alone.AddRange(call.Fallback);
        }

        // Not awaited: the loop goes on claiming; each Finish's own SettleAsync waits for its result, and drain waits for that.
        _ = ResolveAloneAsync(alone, call?.RetryDelay ?? false);
        return call is null ? null : (call.Claim, call.SentAt, call.Sent && !call.Refused);
    }

    /// <summary>Resolves each entry alone with its own frozen Finish command (05 §12.4).</summary>
    async Task ResolveAloneAsync(List<AttemptEntry> entries, bool retryDelay)
    {
        if (entries.Count == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(entries.Select(async e => Complete(e, await ResolveAsync(e, detached: false, firstAttempt: retryDelay ? 1 : 0).ConfigureAwait(false))))
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (engine.ControlStop.IsCancellationRequested)
        {
        }
    }

    /// <summary>One finish-and-claim call's result: the claim reply, the Finishes it decided, and those left to resolve alone.</summary>
    sealed class Combined
    {
        public StoreReply<ClaimedJobs>? Claim;
        public TimeSpan SentAt;
        public bool Sent, Refused, RetryDelay;
        public readonly Dictionary<AttemptEntry, SettlementResult> Results = [];
        public readonly List<AttemptEntry> Fallback = [];

        public void CompleteSettled()
        {
            foreach (var (entry, result) in Results)
            {
                entry.Settled.TrySetResult(result);
            }
        }
    }

    /// <summary>
    /// Sends one finish-and-claim call for <paramref name="members"/> and <paramref name="claim"/> (05 §12.7). Every member holds its own
    /// arbiter slot for the call, as in FinishMany. A refused call (UnsupportedVersion, InvalidRequest) turns the call off for this worker.
    /// </summary>
    async Task<Combined> SendCombinedAsync(IFinishAndClaimStore store, List<AttemptEntry> members, StoreCommand<ReserveAndBeginRequest> claim)
    {
        var call = new Combined();
        var slots = new List<PriorityGate.Slot>();
        StoreReply<FinishedAndClaimed>? reply = null;
        try
        {
            // Entries of one call belong to distinct attempts (see SettleBatchAsync), so taking their arbiters in turn cannot deadlock.
            foreach (var e in members)
            {
                slots.Add(await e.Arbiter.AcquireAsync(ControlPriority.Settlement, engine.ControlStop).ConfigureAwait(false));
                e.SettlementInFlight = true;
                e.CommandUnresolved = true;
                Interlocked.Increment(ref e.FinishAttempts);
            }

            var now = engine.Clock.Now;
            var budgetEnd = members.Min(e => BudgetEnd(e));
            if (budgetEnd > now)
            {
                var request = new FinishAndClaimRequest(members.Select(e => e.FinishCommand!).ToList(), claim);
                try
                {
                    using (engine.Lease.TrackWait())
                    {
                        Interlocked.Add(ref engine.FinishesSent, members.Count);
                        Interlocked.Increment(ref CombinedCalls);
                        call.SentAt = engine.Clock.Now;
                        call.Sent = true;
                        reply = await engine.CallAsync(engine.SettlementIo, 0, WorkerEngine.Min(engine.Options.ControlRequestTimeout, budgetEnd - now),
                            t => store.FinishAndClaimAsync(request, t)).ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (!engine.ControlStop.IsCancellationRequested)
                {
                    // Outcome unknown: each Finish resolves alone with its own command, and the claim by resubmitting its command.
                    engine.Logger.LogDebug(ex, "Runwick FinishAndClaim {Command} of {Count} unresolved; resolving each command alone.", claim.CommandId, members.Count);
                    LastBatchFailure = ex.GetType().Name + ": " + ex.Message;
                }
            }
        }
        finally
        {
            slots.ForEach(s => s.Dispose());
        }

        if (reply is { Status: StoreStatus.Applied, Value: { } value })
        {
            call.Claim = value.Claim;
            if (value.Finished is { } finished && finished.Outcomes.Count == members.Count)
            {
                for (var i = 0; i < members.Count; i++)
                {
                    if (Apply(members[i], finished.Outcomes[i]) is { } result)
                    {
                        Interlocked.Increment(ref BatchedItemsSettled);
                        call.Results[members[i]] = result;
                    }
                    else
                    {
                        call.Fallback.Add(members[i]);
                    }
                }
            }
            else
            {
                // A replayed claim carries no finishes: each resolves from its own receipt.
                call.Fallback.AddRange(members);
            }

            if (call.Claim.Status == StoreStatus.Applied && call.Claim.Value is { Outcomes: { } outcomes } && outcomes.Any(o => o.Status == StoreStatus.Applied))
            {
                Interlocked.Increment(ref CombinedClaimed);
            }

            return call;
        }

        Interlocked.Increment(ref CombinedFallbacks);
        call.Fallback.AddRange(members);
        if (reply is null)
        {
            call.RetryDelay = true;
            LastBatchFailure ??= "no reply";
        }
        else
        {
            // A refused call applied nothing: no claim to settle. The operation missing or a refused request stops the combined calls.
            call.Refused = true;
            LastBatchFailure = $"{reply.Status} ({reply.Error?.Code})";
            if (reply.Status is StoreStatus.UnsupportedVersion or StoreStatus.InvalidRequest)
            {
                Volatile.Write(ref _combinedOff, true);
                engine.Diagnose($"FinishAndClaim returned {reply.Status} ({reply.Error?.Code}); finishing and claiming with separate calls");
            }
        }

        return call;
    }

    /// <summary>
    /// One FinishMany call for entries taken together from the channel (05 §12.5). Every member holds its own arbiter slot
    /// for the whole call, as a single Finish does. An entry whose budget already ran out, or a lone member, takes the
    /// single path. Each item's reply is applied exactly as the same Finish/1 reply; anything else resolves per item.
    /// </summary>
    async Task SettleBatchAsync(List<AttemptEntry> batch)
    {
        var now = engine.Clock.Now;
        var members = batch.Where(e => now < BudgetEnd(e)).ToList();
        var alone = batch.Except(members).ToList();
        if (members.Count < 2)
        {
            alone.AddRange(members);
            members.Clear();
        }

        var fallback = new List<AttemptEntry>();
        var retryDelay = false;
        if (members.Count > 0)
        {
            var slots = new List<PriorityGate.Slot>();
            StoreReply<FinishedMany>? reply = null;
            var store = (IFinishManyStore)engine.Store;
            try
            {
                // Entries of one batch belong to distinct attempts, and no other holder of an attempt arbiter waits for a
                // second one (RenewMany takes them without waiting), so taking them in turn cannot deadlock.
                foreach (var e in members)
                {
                    slots.Add(await e.Arbiter.AcquireAsync(ControlPriority.Settlement, engine.ControlStop).ConfigureAwait(false));
                    e.SettlementInFlight = true;
                    e.CommandUnresolved = true;
                    Interlocked.Increment(ref e.FinishAttempts);
                }

                now = engine.Clock.Now;
                var budgetEnd = members.Min(e => BudgetEnd(e));
                var items = members.Select(e => e.FinishCommand!).ToList();
                var command = engine.Command(new FinishManyRequest(items)) with { NotAfter = items.Max(i => i.NotAfter) };
                if (budgetEnd > now)
                {
                    try
                    {
                        using (engine.Lease.TrackWait())
                        {
                            Interlocked.Add(ref engine.FinishesSent, members.Count);
                            Interlocked.Increment(ref BatchesSent);
                            reply = await engine.CallAsync(engine.SettlementIo, 0, WorkerEngine.Min(engine.Options.ControlRequestTimeout, budgetEnd - now),
                                t => store.FinishManyAsync(command, t)).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex) when (!engine.ControlStop.IsCancellationRequested)
                    {
                        // Not applied or outcome unknown: every item resolves alone with its own command (its receipt decides).
                        engine.Logger.LogDebug(ex, "Runwick FinishMany {Command} of {Count} unresolved; resolving each Finish.", command.CommandId, members.Count);
                        LastBatchFailure = ex.GetType().Name + ": " + ex.Message;
                    }
                }
            }
            finally
            {
                slots.ForEach(s => s.Dispose());
            }

            if (reply is { Status: StoreStatus.Applied, Value: { } value } && value.Outcomes.Count == members.Count)
            {
                Interlocked.Increment(ref BatchesApplied);
                for (var i = 0; i < members.Count; i++)
                {
                    if (Apply(members[i], value.Outcomes[i]) is { } result)
                    {
                        Interlocked.Increment(ref BatchedItemsSettled);
                        members[i].Settled.TrySetResult(result);
                    }
                    else
                    {
                        fallback.Add(members[i]);
                    }
                }
            }
            else
            {
                if (reply is { Status: StoreStatus.UnsupportedVersion or StoreStatus.InvalidRequest or StoreStatus.InvariantFault })
                {
                    // The batch itself is refused (the operation missing, a refused request, a fault in one item): stop
                    // batching and let each Finish/1 report its own outcome.
                    if (reply.Status != StoreStatus.InvariantFault)
                    {
                        Volatile.Write(ref _batchOff, true);
                    }

                    LastBatchFailure = $"{reply.Status} ({reply.Error?.Code})";
                    engine.Diagnose($"FinishMany returned {reply.Status} ({reply.Error?.Code}); resolving each Finish alone");
                }
                else
                {
                    LastBatchFailure = reply is null ? LastBatchFailure ?? "no reply" : $"{reply.Status} ({reply.Error?.Code})";
                    retryDelay = true;
                }

                fallback.AddRange(members);
            }
        }

        if (fallback.Count + alone.Count == 0)
        {
            return;
        }

        // Each remaining Finish resolves as today (same command, same budget); the settlement lane bounds them.
        await Task.WhenAll(alone.Select(async e => Complete(e, await ResolveAsync(e, detached: false).ConfigureAwait(false)))
            .Concat(fallback.Select(async e => Complete(e, await ResolveAsync(e, detached: false, firstAttempt: retryDelay ? 1 : 0).ConfigureAwait(false)))))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Sends the frozen Finish until it resolves or the budget expires. On expiry the entry latches
    /// <see cref="LossCause.CompletionUnresolved"/> (unconfirmed outcome) and resolution continues detached for
    /// diagnostics until control stop; the pipeline is never rerun. <paramref name="firstAttempt"/> &gt; 0 continues a
    /// resolution the batch began: it waits that attempt's backoff first and resubmits at resolution priority.
    /// </summary>
    async Task<SettlementResult?> ResolveAsync(AttemptEntry entry, bool detached, int firstAttempt = 0)
    {
        var command = entry.FinishCommand!;
        var budgetEnd = BudgetEnd(entry);
        if (firstAttempt > 0)
        {
            await engine.Clock.Delay(engine.Backoff(firstAttempt - 1), engine.ControlStop).ConfigureAwait(false);
        }

        for (var attempt = firstAttempt; !engine.ControlStop.IsCancellationRequested; attempt++)
        {
            var now = engine.Clock.Now;
            if (!detached && now >= budgetEnd)
            {
                entry.Latch(LossCause.CompletionUnresolved);
                engine.Diagnose($"unconfirmed outcome: Finish {command.CommandId} of {entry} unresolved after {engine.Options.CompletionResolutionBudget}");
                StartDetached(entry);
                return SettlementResult.Unresolved;
            }

            StoreReply<FinishedAttempt>? reply = null;
            using (var slot = await entry.Arbiter.AcquireAsync(attempt == 0 ? ControlPriority.Settlement : ControlPriority.Resolution, engine.ControlStop).ConfigureAwait(false))
            {
                entry.SettlementInFlight = true;
                entry.CommandUnresolved = true;
                Interlocked.Increment(ref entry.FinishAttempts);
                var timeout = detached ? engine.Options.ControlRequestTimeout : WorkerEngine.Min(engine.Options.ControlRequestTimeout, budgetEnd - now);
                try
                {
                    using (engine.Lease.TrackWait())
                    {
                        Interlocked.Increment(ref engine.FinishesSent);
                        reply = await engine.CallAsync(engine.SettlementIo, 0, timeout, t => engine.Store.FinishAsync(command, t)).ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (!engine.ControlStop.IsCancellationRequested)
                {
                    engine.Logger.LogDebug(ex, "Runwick Finish {Command} unresolved; resubmitting the same command.", command.CommandId);
                }
            }

            if (reply is not null && Apply(entry, reply) is { } result)
            {
                return result;
            }

            await engine.Clock.Delay(engine.Backoff(attempt), engine.ControlStop).ConfigureAwait(false);
        }

        return detached ? null : SettlementResult.Unresolved;
    }

    /// <summary>
    /// One Finish/1 reply (05 §12.4), from a single call or a FinishMany item: the local result when it is definitive,
    /// null when the same command must be resubmitted.
    /// </summary>
    SettlementResult? Apply(AttemptEntry entry, StoreReply<FinishedAttempt> reply)
    {
        var command = entry.FinishCommand!;
        switch (reply.Status)
        {
            case StoreStatus.Applied:
                Resolved(entry);
                engine.Emit(reply.Replayed ? "finishReplayed" : "finished", entry);
                return SettlementResult.Applied;
            case StoreStatus.StaleLease or StoreStatus.InvalidState:
                // Definitive: ownership was already gone; the stored outcome is not overwritten.
                Resolved(entry);
                engine.Diagnose($"Finish of {entry} found ownership gone ({reply.Status}); stored outcome kept");
                return SettlementResult.Stale;
            case StoreStatus.CommandExpired:
                // Definitively not applied (no receipt when checked after the deadline); never a new Finish ID.
                Resolved(entry);
                entry.Latch(LossCause.CompletionUnresolved);
                engine.Diagnose($"unknown durability: Finish {command.CommandId} of {entry} expired without a receipt; recovery applies");
                return SettlementResult.Unresolved;
            case StoreStatus.InvalidRequest or StoreStatus.InvariantFault or StoreStatus.CommandConflict:
                Resolved(entry);
                entry.Latch(LossCause.InvariantFault);
                engine.Diagnose($"Finish of {entry} rejected: {reply.Status} {reply.Error?.Code}");
                return SettlementResult.Unresolved;
            default:
                return null;
        }
    }

    static void Resolved(AttemptEntry entry)
    {
        entry.SettlementInFlight = false;
        entry.CommandUnresolved = false;
    }

    void StartDetached(AttemptEntry entry)
    {
        if (Interlocked.Increment(ref _diagnosticResolvers) > engine.Settings.GroupMax)
        {
            Interlocked.Decrement(ref _diagnosticResolvers);
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                if (await ResolveAsync(entry, detached: true).ConfigureAwait(false) is { } late)
                {
                    engine.Diagnose($"Finish of {entry} resolved late as {late} (after the completion budget)");
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                Interlocked.Decrement(ref _diagnosticResolvers);
            }
        });
    }
}
