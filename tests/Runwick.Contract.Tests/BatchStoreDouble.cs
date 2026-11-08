using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// A store in front of a real driver store for the worker-fallback cases (FM-09, FM-10, RAB-10): it forwards every call, and for the batch operations it
/// can answer <c>UnsupportedVersion</c> without calling the store (what PostgreSQL's renamed function does), or call the store and then lose the reply
/// (what a dropped reply after commit does). Only the worker's reaction is under test; every effect that does happen is the real driver's.
/// </summary>
internal sealed class BatchStoreDouble(IRunwickStore inner) : IRunwickStore, ILeaseBatchStore, IReserveAndBeginStore, IFinishManyStore, IWakeNotificationSource
{
    int _loseNext;

    /// <summary>Operations answered as <c>UnsupportedVersion</c> without reaching the store.</summary>
    public HashSet<string> Refused { get; } = [];

    /// <summary>Calls of FinishMany whose reply was lost after the store applied them.</summary>
    public int RepliesLost;

    /// <summary>The next FinishMany is applied by the store and its reply replaced by an unknown outcome.</summary>
    public void LoseNextFinishMany() => Interlocked.Exchange(ref _loseNext, 1);

    public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct) =>
        inner is IWakeNotificationSource source ? source.ListenAsync(application, channel, onWake, onSubscribed, onLost, ct) : Task.CompletedTask;

    public Guid StoreId => inner.StoreId;

    public string AcknowledgementProfile => inner.AcknowledgementProfile;

    public Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct) => inner.DescribeAsync(application, ct);

    public Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct) =>
        inner.RegisterApplicationAsync(command, ct);

    public Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct) =>
        inner.RegisterQueuesAsync(command, ct);

    public Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct) =>
        inner.RegisterScopesAsync(command, ct);

    public Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct) => inner.DispatchAsync(command, ct);

    public Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        inner.DispatchManyAsync(command, ct);

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) => inner.ResolveCommandAsync(command, ct);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        inner.ResolveOperationAsync(application, commandId, notAfterBound, ct);

    public Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct) => inner.TryReserveAsync(command, ct);

    public Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct) => inner.BeginExecutionAsync(command, ct);

    public Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct) => inner.RenewAsync(command, ct);

    public Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct) => inner.FinishAsync(command, ct);

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct) => inner.RecoverAsync(command, ct);

    public Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct) =>
        inner.ReleaseReservationAsync(command, ct);

    public Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct) =>
        inner.ProbeAsync(application, lanes, supportedContracts, ct);

    public Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct) =>
        inner.PublishPresenceAsync(application, presence, ct);

    public Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct) =>
        inner.ListExpiredLeasesAsync(application, limit, ct);

    public Task<StoreReply<RenewedLeases>> RenewManyAsync(StoreCommand<RenewManyRequest> command, CancellationToken ct) =>
        ((ILeaseBatchStore)inner).RenewManyAsync(command, ct);

    public Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(StoreCommand<ReserveAndBeginRequest> command, CancellationToken ct) =>
        Refused.Contains(Operations.ReserveAndBegin)
            ? Task.FromResult(StoreReply<ClaimedJobs>.NoEffect(StoreStatus.UnsupportedVersion, new StoreError("operation_unsupported"), DateTimeOffset.UtcNow))
            : ((IReserveAndBeginStore)inner).ReserveAndBeginAsync(command, ct);

    public async Task<StoreReply<FinishedMany>> FinishManyAsync(StoreCommand<FinishManyRequest> command, CancellationToken ct)
    {
        if (Refused.Contains(Operations.FinishMany))
        {
            return StoreReply<FinishedMany>.NoEffect(StoreStatus.UnsupportedVersion, new StoreError("operation_unsupported"), DateTimeOffset.UtcNow);
        }

        var reply = await ((IFinishManyStore)inner).FinishManyAsync(command, ct).ConfigureAwait(false);
        if (Interlocked.Exchange(ref _loseNext, 0) == 1)
        {
            Interlocked.Increment(ref RepliesLost);
            throw new StoreOutcomeUnknownException("commit_outcome_unknown", command.CommandId, Operations.FinishMany);
        }

        return reply;
    }
}
