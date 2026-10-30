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
/// </summary>
internal sealed class CompletionCoordinator(WorkerEngine engine)
{
    readonly Channel<AttemptEntry> _ready = Channel.CreateBounded<AttemptEntry>(
        new BoundedChannelOptions(Math.Max(1, engine.Settings.GroupMax)) { FullMode = BoundedChannelFullMode.Wait });
    int _diagnosticResolvers;

    public int Pending => _ready.Reader.Count;

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
        await foreach (var entry in _ready.Reader.ReadAllAsync(engine.ControlStop).ConfigureAwait(false))
        {
            var result = await ResolveAsync(entry, detached: false).ConfigureAwait(false);
            if (result is { } r)
            {
                entry.Settled.TrySetResult(r);
            }
        }
    }

    /// <summary>
    /// Sends the frozen Finish until it resolves or the budget expires. On expiry the entry latches
    /// <see cref="LossCause.CompletionUnresolved"/> (unconfirmed outcome) and resolution continues detached for
    /// diagnostics until control stop; the pipeline is never rerun.
    /// </summary>
    async Task<SettlementResult?> ResolveAsync(AttemptEntry entry, bool detached)
    {
        var command = entry.FinishCommand!;
        var budgetEnd = entry.FinishPublishedAt!.Value + engine.Options.CompletionResolutionBudget;
        for (var attempt = 0; !engine.ControlStop.IsCancellationRequested; attempt++)
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

            switch (reply?.Status)
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
            }

            await engine.Clock.Delay(engine.Backoff(attempt), engine.ControlStop).ConfigureAwait(false);
        }

        return detached ? null : SettlementResult.Unresolved;
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
