using System.Collections.Concurrent;
using System.Diagnostics;
using Runwick.Protocol;

namespace Runwick.Workloads;

/// <summary>
/// <see cref="IRunwickStore"/> decorator that records each store call's duration and reply status (07 §14.4). It measures
/// the call as the engine issues it; the engine's local admission wait before the call is recorded separately
/// (<c>controlWait</c>, from the engine's own tracking). Also derives the renewal margin (previous expiry − Renew decision,
/// both store instants) and due-to-Begin (<c>eligible_at</c> from the Reserve cursor → Begin decision).
/// </summary>
internal sealed class TimingStore(IRunwickStore inner, Metrics metrics, bool detailed = false) : IRunwickStore, ILeaseBatchStore, IWakeNotificationSource
{
    // Wake notifications pass straight through (05 §7.2, PF-07); they are not store calls and are not timed.
    public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, CancellationToken ct) =>
        inner is IWakeNotificationSource source ? source.ListenAsync(application, channel, onWake, onSubscribed, ct) : Task.CompletedTask;

    readonly ConcurrentDictionary<Guid, DateTimeOffset> _expiry = new();
    readonly ConcurrentDictionary<Guid, DateTimeOffset> _eligible = new();

    public Guid StoreId => inner.StoreId;

    public string AcknowledgementProfile => inner.AcknowledgementProfile;

    async Task<T> Timed<T>(string op, Func<Task<T>> call, Func<T, string> status)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            var result = await call().ConfigureAwait(false);
            var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            metrics.Observe(op, elapsed);
            metrics.Count($"{op}:{status(result)}");
            if (detailed)
            {
                // G-2 only: latency per reply status (P-06 rate decision latency, Begin Applied vs RateLimited).
                metrics.Observe($"{op}@{status(result)}", elapsed);
            }
            return result;
        }
        catch (Exception ex)
        {
            metrics.Observe(op + ":error", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            metrics.Count($"{op}:{ex.GetType().Name}");
            throw;
        }
    }

    static string S<T>(StoreReply<T> r) where T : class => r.Replayed ? r.Status + ":replayed" : r.Status.ToString();

    public Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct) => inner.DescribeAsync(application, ct);

    public Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct) =>
        inner.RegisterApplicationAsync(command, ct);

    public Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct) =>
        inner.RegisterQueuesAsync(command, ct);

    public Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct) =>
        inner.RegisterScopesAsync(command, ct);

    public Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        Timed("dispatch", () => inner.DispatchAsync(command, ct), S);

    public Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        Timed("dispatchMany", () => inner.DispatchManyAsync(command, ct), S);

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) =>
        Timed("resolve", () => inner.ResolveCommandAsync(command, ct), r => r.GetType().Name);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        Timed("resolve", () => inner.ResolveOperationAsync(application, commandId, notAfterBound, ct), r => r.GetType().Name);

    public async Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct)
    {
        var reply = await Timed("reserve", () => inner.TryReserveAsync(command, ct), S).ConfigureAwait(false);
        if (reply is { Status: StoreStatus.Applied, Value: { } job })
        {
            _expiry[job.Lease.Handle.AttemptId] = job.Lease.ExpiresAt;
            _eligible[job.Lease.Handle.AttemptId] = job.Cursor.EligibleAt;
        }

        return reply;
    }

    public async Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct)
    {
        var reply = await Timed("begin", () => inner.BeginExecutionAsync(command, ct), r => S(r.Reply)).ConfigureAwait(false);
        var attempt = command.Body.Lease.AttemptId;
        if (reply.Reply is { Status: StoreStatus.Applied, Value: { } begun })
        {
            _expiry[attempt] = begun.Lease.ExpiresAt;
            if (!reply.Reply.Replayed && _eligible.TryRemove(attempt, out var eligible))
            {
                metrics.Observe("dueToBegin", (reply.Reply.DecidedAt - eligible).TotalMilliseconds);
            }
        }

        return reply;
    }

    public async Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct)
    {
        var reply = await Timed("renew", () => inner.RenewAsync(command, ct), S).ConfigureAwait(false);
        var attempt = command.Body.Lease.AttemptId;
        if (_expiry.TryGetValue(attempt, out var previous))
        {
            var margin = (previous - reply.DecidedAt).TotalMilliseconds;
            if (reply.Status == StoreStatus.Applied)
            {
                metrics.Observe("renewalMargin", margin);
            }
            else
            {
                metrics.Count("renewalMarginMissed");
            }
        }

        if (reply is { Status: StoreStatus.Applied, Value: { } renewed })
        {
            _expiry[attempt] = renewed.Lease.ExpiresAt;
        }

        return reply;
    }

    /// <summary>RenewMany (PF-06): timed as <c>renewMany</c>; each member's margin is recorded as for Renew.</summary>
    public async Task<StoreReply<RenewedLeases>> RenewManyAsync(StoreCommand<RenewManyRequest> command, CancellationToken ct)
    {
        var batch = inner as ILeaseBatchStore ?? throw new NotSupportedException("the inner store has no RenewMany");
        var reply = await Timed("renewMany", () => batch.RenewManyAsync(command, ct), S).ConfigureAwait(false);
        metrics.Observe("renewManySize", command.Body.Leases.Count);
        if (reply is { Status: StoreStatus.Applied, Value: { } value })
        {
            for (var i = 0; i < value.Outcomes.Count && i < command.Body.Leases.Count; i++)
            {
                var attempt = command.Body.Leases[i].AttemptId;
                if (_expiry.TryGetValue(attempt, out var previous))
                {
                    if (value.Outcomes[i].Status == StoreStatus.Applied)
                    {
                        metrics.Observe("renewalMargin", (previous - reply.DecidedAt).TotalMilliseconds);
                    }
                    else
                    {
                        metrics.Count("renewalMarginMissed");
                    }
                }

                if (value.Outcomes[i].Value is { } renewed)
                {
                    _expiry[attempt] = renewed.Lease.ExpiresAt;
                }
            }
        }

        return reply;
    }

    public async Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct)
    {
        var reply = await Timed("finish", () => inner.FinishAsync(command, ct), S).ConfigureAwait(false);
        if (reply.Status is StoreStatus.Applied or StoreStatus.StaleLease or StoreStatus.InvalidState)
        {
            Forget(command.Body.Lease.AttemptId);
        }

        return reply;
    }

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct) =>
        Timed("recover", () => inner.RecoverAsync(command, ct), S);

    public async Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct)
    {
        var reply = await Timed("release", () => inner.ReleaseReservationAsync(command, ct), S).ConfigureAwait(false);
        Forget(command.Body.Lease.AttemptId);
        return reply;
    }

    public Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct) =>
        Timed("probe", () => inner.ProbeAsync(application, lanes, supportedContracts, ct), r => r.Lanes.Count == 0 ? "empty" : "ready");

    public Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct) =>
        Timed("presence", () => inner.PublishPresenceAsync(application, presence, ct), r => r.Accepted ? "accepted" : "rejected");

    public Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct) =>
        Timed("listExpired", () => inner.ListExpiredLeasesAsync(application, limit, ct), r => r.Count == 0 ? "none" : "found");

    void Forget(Guid attempt)
    {
        _expiry.TryRemove(attempt, out _);
        _eligible.TryRemove(attempt, out _);
    }
}
