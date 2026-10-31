using Microsoft.Extensions.Logging;
using Runwick.Protocol;

namespace Runwick.Worker;

/// <summary>A Finish/1 staged on the caller's transaction (08 §8.1) and a probe telling whether that transaction is still open.</summary>
internal sealed record StagedFinish(StoreCommand<FinishRequest> Command, Func<bool> TransactionOpen);

/// <summary>
/// Transactional completion on the engine path (08 §8, 01 §16): staging admission for the provider's
/// <c>IPostgresJobCompletion</c>, then outcome observation by receipt resolution instead of the engine's own Finish.
/// </summary>
internal sealed partial class WorkerEngine
{
    /// <summary>
    /// Admits one staging for a running pipeline and freezes its Finish command. Renewal pauses: the caller's
    /// transaction holds the job row until it ends (08 §8.1 step 3).
    /// </summary>
    internal StoreCommand<FinishRequest> PrepareStagedCompletion(AttemptEntry entry)
    {
        lock (entry.Gate)
        {
            if (entry.IsLost)
            {
                throw new JobOwnershipLostException($"The attempt no longer owns job {entry.Lease.JobId} ({entry.Loss}); roll the transaction back.");
            }

            if (!entry.PipelineLive || entry.Phase is not AttemptPhase.Running)
            {
                throw new JobCompletionException(RunwickErrorCodes.NoActiveAttempt, "Transactional completion is available only while the attempt's pipeline runs.");
            }

            if (entry.StagingStarted)
            {
                throw new JobCompletionException(RunwickErrorCodes.CompletionAlreadyStaged, "A completion was already staged for this attempt.");
            }

            entry.StagingStarted = true;
            entry.SettlementInFlight = true;
        }

        var elapsed = Clock.Now - (entry.ExecutionStartedAt ?? Clock.Now);
        return Command(new FinishRequest(entry.Lease, AttemptOutcome.Succeeded, elapsed < TimeSpan.Zero ? TimeSpan.Zero : elapsed, null, null));
    }

    /// <summary>The staged Finish was applied inside the caller's transaction (its commit is still the caller's).</summary>
    internal void RecordStagedCompletion(AttemptEntry entry, StoreCommand<FinishRequest> command, Func<bool> transactionOpen)
    {
        entry.Staged = new StagedFinish(command, transactionOpen);
        Emit("completionStaged", entry);
    }

    /// <summary>Staging was rejected. Ownership loss keeps renewal stopped; any other rejection resumes it and allows a retry.</summary>
    internal void AbandonStagedCompletion(AttemptEntry entry, bool ownershipLost)
    {
        lock (entry.Gate)
        {
            if (ownershipLost)
            {
                return;
            }

            entry.StagingStarted = false;
            entry.SettlementInFlight = false;
        }
    }

    /// <summary>
    /// 08 §8.2: settles an attempt whose pipeline staged a completion. Returns without a Finish when the staged
    /// receipt is committed; otherwise publishes the normal-path Finish, which storage closes at most once.
    /// </summary>
    async Task ObserveStagedAsync(AttemptEntry entry, PipelineObservation observation, StagedFinish staged, TimeSpan? timeout)
    {
        var token = observation.Result is { Kind: JobExecutionResultKind.Completed } completed ? completed.Completion : null;
        if (token is not null && (token.AttemptId.Value != entry.AttemptId || token.CommandId != staged.Command.CommandId))
        {
            await SettleNormallyAsync(entry, new FinishRequest(entry.Lease, AttemptOutcome.PermanentFailure, observation.Elapsed, null,
                new AttemptError("pipeline_contract_fault", "", "The returned staged completion does not belong to this attempt.", null))).ConfigureAwait(false);
            return;
        }

        if (!staged.TransactionOpen())
        {
            var resolution = await ResolveStagedAsync(entry, staged.Command).ConfigureAwait(false);
            if (resolution is ReceiptResolution.Recorded { Status: nameof(StoreStatus.Applied) })
            {
                Interlocked.Increment(ref StagedCompletionsObserved);
                if (observation.Exception is { } late)
                {
                    // TXC-06: the commit stands; the exception is a diagnostic only.
                    entry.Diagnostic = "handler threw after a committed staged completion: " + late.GetType().Name;
                    Diagnose($"{entry}: {entry.Diagnostic}");
                }

                entry.SettlementInFlight = false;
                entry.TryMove(AttemptPhase.Finishing);
                entry.Settled.TrySetResult(SettlementResult.Applied);
                Emit("completionCommitted", entry);
                return;
            }
        }

        // Not committed (or still open): exceptions classify normally; a normal return is staged_completion_not_committed.
        var finish = observation.Exception is not null || observation.Misuse is not null
            ? OutcomeClassifier.Classify(entry.Lease, observation, entry.Causes, timeout)
            : new FinishRequest(entry.Lease, AttemptOutcome.PermanentFailure, observation.Elapsed, null,
                new AttemptError(RunwickErrorCodes.StagedCompletionNotCommitted, "",
                    "The handler staged a completion but its transaction did not commit.", null));
        await SettleNormallyAsync(entry, finish).ConfigureAwait(false);
    }

    async Task SettleNormallyAsync(AttemptEntry entry, FinishRequest finish)
    {
        entry.SettlementInFlight = false;
        if (!entry.IsLost)
        {
            await Completion.SettleAsync(entry, finish).ConfigureAwait(false);
        }
    }

    /// <summary>Receipt lookup on the primary (03 §8.6) within the control request timeout; transport failures count as not observed.</summary>
    async Task<ReceiptResolution?> ResolveStagedAsync(AttemptEntry entry, StoreCommand<FinishRequest> command)
    {
        for (var attempt = 0; attempt < 3 && !ControlStop.IsCancellationRequested; attempt++)
        {
            try
            {
                return await CallAsync(SettlementIo, 0, Options.ControlRequestTimeout,
                    t => Store.ResolveOperationAsync(Application, command.CommandId, command.NotAfter, t)).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ControlStop.IsCancellationRequested)
            {
                Logger.LogDebug(ex, "Runwick staged completion {Command} of {Entry} could not be resolved yet.", command.CommandId, entry);
                await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
            }
        }

        return null;
    }
}
