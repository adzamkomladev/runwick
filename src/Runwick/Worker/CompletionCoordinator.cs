using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Runwick.Protocol;

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
    readonly Channel<AttemptEntry> _ready = Channel.CreateBounded<AttemptEntry>(
        new BoundedChannelOptions(Math.Max(1, engine.Settings.GroupMax)) { FullMode = BoundedChannelFullMode.Wait });
    int _diagnosticResolvers;
    bool _batchOff;

    public int Pending => _ready.Reader.Count;

    /// <summary>FinishMany calls sent (05 §12.5), for diagnostics and tests; each item also counts in <c>FinishesSent</c>.</summary>
    public long BatchesSent;

    /// <summary>Applied FinishMany replies (every item decided in one call).</summary>
    public long BatchesApplied;

    /// <summary>Items settled from an Applied FinishMany reply without a per-item resolution.</summary>
    public long BatchedItemsSettled;

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

        await _ready.Writer.WriteAsync(entry, engine.ControlStop).ConfigureAwait(false);
        return await entry.Settled.Task.ConfigureAwait(false);
    }

    async Task WorkerAsync()
    {
        while (await _ready.Reader.WaitToReadAsync(engine.ControlStop).ConfigureAwait(false))
        {
            if (!_ready.Reader.TryRead(out var first))
            {
                continue;
            }

            // What is already waiting joins this call: no coalescing delay (05 §12.5). A Finish that finds the lane idle is
            // sent at once, alone; Finishes that queued while both lane calls were busy go together.
            var batch = new List<AttemptEntry> { first };
            if (!Volatile.Read(ref _batchOff) && engine.Store is IFinishManyStore)
            {
                while (batch.Count < IFinishManyStore.MaxItems && _ready.Reader.TryRead(out var more))
                {
                    batch.Add(more);
                }
            }

            if (batch.Count == 1)
            {
                Complete(first, await ResolveAsync(first, detached: false).ConfigureAwait(false));
                continue;
            }

            await SettleBatchAsync(batch).ConfigureAwait(false);
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

                    engine.Diagnose($"FinishMany returned {reply.Status} ({reply.Error?.Code}); resolving each Finish alone");
                }
                else
                {
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
