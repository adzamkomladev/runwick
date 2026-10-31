using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Outbox;

/// <summary>
/// The relay (14 §4): claim a bounded batch in a short source transaction, persist the transport command, send outside
/// any source transaction, then mark delivered under the lease token. Source rollback publishes nothing; a lost delivered
/// mark is repaired by the destination's delivery mapping, never by a second job.
/// </summary>
internal sealed class OutboxRelay(
    IOutboxSource source,
    RunwickRuntime runtime,
    IPreparedJobDispatcher dispatcher,
    DispatchPreparer preparer,
    IOutboxDestinationStore? destination,
    OutboxRelayOptions options,
    TimeProvider time,
    ILogger<OutboxRelay> logger) : IHostedService, IAsyncDisposable
{
    readonly Guid _owner = Guid.CreateVersion7();
    readonly ConcurrentDictionary<Guid, InFlight> _inFlight = new();
    readonly CancellationTokenSource _claimStop = new();
    readonly CancellationTokenSource _abort = new();
    readonly SemaphoreSlim _slotFreed = new(0);
    Task? _claimLoop;
    Task? _renewLoop;
    volatile bool _renewalUncertain;
    int _disposed;

    // Counters for tests and diagnostics.
    public long Delivered;
    public long Blocked;
    public long RetriesScheduled;
    public long LeaseLosses;
    public long Sends;

    public int InFlightCount => _inFlight.Count;

    sealed class InFlight(OutboxLease lease)
    {
        public OutboxLease Lease { get; } = lease;

        public volatile bool Lost;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var errors = options.Validate();
        if (errors.Count > 0)
        {
            throw new InvalidOperationException("Invalid outbox relay options: " + string.Join(" ", errors));
        }

        if (!options.Enabled)
        {
            return Task.CompletedTask;
        }

        if (destination is null)
        {
            throw new InvalidOperationException("The configured Runwick store does not implement ReceiveOutbox/1 (outbox_destination).");
        }

        _claimLoop = Task.Run(ClaimLoopAsync, CancellationToken.None);
        _renewLoop = Task.Run(RenewLoopAsync, CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <summary>Stops claims first, then observes in-flight sends until <paramref name="cancellationToken"/> (the host deadline).</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _claimStop.CancelAsync().ConfigureAwait(false);
        if (_claimLoop is { } loop)
        {
            await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        try
        {
            while (!_inFlight.IsEmpty)
            {
                await _slotFreed.WaitAsync(TimeSpan.FromMilliseconds(100), cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Host deadline: abandon waiting. Nothing is marked delivered without a destination reply; leases expire.
        }
        finally
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            if (_renewLoop is { } renew)
            {
                await renew.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _claimStop.CancelAsync().ConfigureAwait(false);
        await _abort.CancelAsync().ConfigureAwait(false);
        _claimStop.Dispose();
        _abort.Dispose();
        _slotFreed.Dispose();
    }

    async Task ClaimLoopAsync()
    {
        var stop = _claimStop.Token;
        var failures = 0;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Call(c => runtime.EnsureReadyAsync(c)).ConfigureAwait(false);
                await Call(source.InitializeAsync).ConfigureAwait(false);
                break;
            }
            catch (Exception ex) when (!stop.IsCancellationRequested && ex is not OutOfMemoryException)
            {
                logger.LogWarning(ex, "Runwick outbox relay could not initialize; retrying.");
                await DelayAsync(Backoff(failures++), stop).ConfigureAwait(false);
            }
        }

        failures = 0;
        while (!stop.IsCancellationRequested)
        {
            var free = options.MaxInFlight - _inFlight.Count;
            var claimed = 0;
            if (free > 0 && !_renewalUncertain)
            {
                try
                {
                    var leases = await Call(c => source.ClaimAsync(_owner, free, options.LeaseDuration, c)).ConfigureAwait(false);
                    failures = 0;
                    foreach (var lease in leases)
                    {
                        var entry = new InFlight(lease);
                        if (_inFlight.TryAdd(lease.MessageId, entry))
                        {
                            claimed++;
                            _ = Task.Run(() => DeliverAsync(entry), CancellationToken.None);
                        }
                    }
                }
                catch (Exception ex) when (!stop.IsCancellationRequested && ex is not OutOfMemoryException)
                {
                    logger.LogWarning(ex, "Runwick outbox claim failed.");
                    await DelayAsync(Backoff(failures++), stop).ConfigureAwait(false);
                    continue;
                }
            }

            if (claimed == 0 || _inFlight.Count >= options.MaxInFlight)
            {
                try
                {
                    await _slotFreed.WaitAsync(options.IdlePoll, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    async Task RenewLoopAsync()
    {
        var stop = _abort.Token;
        while (!stop.IsCancellationRequested)
        {
            if (!await DelayAsync(options.RenewalInterval, stop).ConfigureAwait(false))
            {
                return;
            }

            var uncertain = false;
            foreach (var entry in _inFlight.Values)
            {
                if (entry.Lost)
                {
                    continue;
                }

                try
                {
                    if (!await Call(c => source.RenewAsync(entry.Lease, options.LeaseDuration, c)).ConfigureAwait(false))
                    {
                        entry.Lost = true;
                        Interlocked.Increment(ref LeaseLosses);
                    }
                }
                catch (Exception ex) when (!stop.IsCancellationRequested && ex is not OutOfMemoryException)
                {
                    // Renewal uncertainty: stop new claims until a renewal round succeeds (14 §4.3).
                    logger.LogDebug(ex, "Runwick outbox lease renewal failed for {MessageId}.", entry.Lease.MessageId);
                    uncertain = true;
                }
            }

            _renewalUncertain = uncertain;
        }
    }

    async Task DeliverAsync(InFlight entry)
    {
        var lease = entry.Lease;
        try
        {
            PreparedDispatch prepared;
            try
            {
                prepared = dispatcher.ImportPrepared(lease.Envelope);
            }
            catch (DispatchValidationException ex)
            {
                // Malformed intent, destination binding or policy/profile drift: visible, never rewritten (OUTBOX-05).
                await BlockAsync(entry, ex.Code).ConfigureAwait(false);
                return;
            }

            if (prepared.StoreId != lease.DestinationStoreId || !string.Equals(prepared.Application, lease.DestinationApplication, StringComparison.Ordinal))
            {
                await BlockAsync(entry, "destination_binding_mismatch").ConfigureAwait(false);
                return;
            }

            // Contract drift: this host no longer registers the frozen job contract. Visible, never rewritten (14 §5.4).
            if (runtime.Registry.ByName(prepared.Intent.JobName) is not { } job || !job.Routes.ContainsKey(prepared.Intent.PayloadVersion))
            {
                await BlockAsync(entry, "contract_not_registered").ConfigureAwait(false);
                return;
            }

            var identity = new OutboxSourceIdentity(source.SourceStoreId, lease.MessageId);
            var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, lease.AcceptUntil.ToUnixTimeMilliseconds(),
                prepared.Intent, prepared.Digests);
            if (!fingerprint.AsSpan().SequenceEqual(lease.DeliveryFingerprint))
            {
                await BlockAsync(entry, "delivery_fingerprint_mismatch").ConfigureAwait(false);
                return;
            }

            if (time.GetUtcNow() >= lease.AcceptUntil)
            {
                await AfterHorizonAsync(entry, prepared.Application, identity, fingerprint).ConfigureAwait(false);
                return;
            }

            // Reuse the persisted transport command while it is valid; otherwise persist a new one before sending.
            var now = time.GetUtcNow();
            Guid commandId;
            DateTimeOffset notAfter;
            if (lease.TransportCommandId is { } id && lease.TransportNotAfter is { } na && now < na)
            {
                (commandId, notAfter) = (id, na);
            }
            else
            {
                commandId = Guid.CreateVersion7();
                var bound = now + options.TransportLifetime < lease.AcceptUntil ? now + options.TransportLifetime : lease.AcceptUntil;
                notAfter = DateTimeOffset.FromUnixTimeMilliseconds(Fingerprints.FloorEpochMs(bound));
                if (entry.Lost || !await Call(c => source.RecordTransportAsync(lease, commandId, notAfter, c)).ConfigureAwait(false))
                {
                    Interlocked.Increment(ref LeaseLosses);
                    return;
                }
            }

            var command = new StoreCommand<ReceiveOutboxRequest>(prepared.Application, commandId, notAfter,
                new ReceiveOutboxRequest(identity,
                    new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) }, lease.AcceptUntil, fingerprint, prepared.StoreId));
            var reply = await SendAsync(command).ConfigureAwait(false);
            if (reply is null)
            {
                await RetryLaterAsync(entry, "transport_unresolved").ConfigureAwait(false);
                return;
            }

            await HandleAsync(entry, prepared.Application, identity, fingerprint, reply).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A source write failed or the host is stopping: leave the lease to expire; the mapping makes the retry safe.
            logger.LogDebug(ex, "Runwick outbox delivery of {MessageId} did not complete.", lease.MessageId);
        }
        finally
        {
            _inFlight.TryRemove(lease.MessageId, out _);
            _slotFreed.Release();
        }
    }

    /// <summary>Sends one transport command; an unknown outcome resends the same command until its NotAfter (03 §4).</summary>
    async Task<StoreReply<OutboxDelivery>?> SendAsync(StoreCommand<ReceiveOutboxRequest> command)
    {
        for (var attempt = 0; time.GetUtcNow() < command.NotAfter && !_abort.IsCancellationRequested; attempt++)
        {
            try
            {
                Interlocked.Increment(ref Sends);
                return await Call(c => destination!.ReceiveOutboxAsync(command, c)).ConfigureAwait(false);
            }
            catch (StoreOutcomeUnknownException)
            {
                // Resend the same command: the receipt (or the mapping) answers if the first one applied.
            }
            catch (OperationCanceledException) when (!_abort.IsCancellationRequested)
            {
                // Provider call timeout: the request may still be running. A hung task keeps its slot until it returns.
            }
            catch (StoreNotAppliedException)
            {
                return null;
            }

            await DelayAsync(TimeSpan.FromMilliseconds(Math.Min(1000, 100 << Math.Min(attempt, 4))), _abort.Token).ConfigureAwait(false);
        }

        return null;
    }

    async Task HandleAsync(InFlight entry, string application, OutboxSourceIdentity identity, byte[] fingerprint, StoreReply<OutboxDelivery> reply)
    {
        switch (reply.Status)
        {
            case StoreStatus.Applied when reply.Value is { } delivered:
                await MarkDeliveredAsync(entry, delivered).ConfigureAwait(false);
                return;
            case StoreStatus.CommandExpired when reply.Error?.Code == "delivery_horizon_passed":
                await AfterHorizonAsync(entry, application, identity, fingerprint).ConfigureAwait(false);
                return;
            case StoreStatus.CommandExpired:
            case StoreStatus.AdmissionClosed:
                // The transport command expired or admission is paused: retry later under the same delivery identity.
                await RetryLaterAsync(entry, reply.Error?.Code ?? reply.Status.ToString()).ConfigureAwait(false);
                return;
            default:
                // Deterministic rejection (invalid intent, unregistered queue, uniqueness, delivery conflict, version).
                await BlockAsync(entry, reply.Error?.Code ?? reply.Status.ToString()).ConfigureAwait(false);
                return;
        }
    }

    /// <summary>After AcceptUntil nothing new may apply: resolve a retained mapping or block for reconciliation (14 §6.3).</summary>
    async Task AfterHorizonAsync(InFlight entry, string application, OutboxSourceIdentity identity, byte[] fingerprint)
    {
        var lookup = await Call(c => destination!.FindDeliveryAsync(application, identity, fingerprint, c)).ConfigureAwait(false);
        if (lookup.Delivery is { } delivered)
        {
            await MarkDeliveredAsync(entry, delivered).ConfigureAwait(false);
        }
        else
        {
            await BlockAsync(entry, lookup.Conflict ? "delivery_conflict" : "horizon_passed_undelivered").ConfigureAwait(false);
        }
    }

    async Task MarkDeliveredAsync(InFlight entry, OutboxDelivery delivered)
    {
        var marked = await Call(c => source.MarkDeliveredAsync(entry.Lease,
            new OutboxDelivered(delivered.JobId, delivered.Disposition.ToString(), delivered.ConfirmedProfile, delivered.DeliveredAt), c)).ConfigureAwait(false);
        if (marked)
        {
            Interlocked.Increment(ref Delivered);
        }
        else
        {
            // A replacement owner holds the message; the destination mapping already protects the delivery.
            Interlocked.Increment(ref LeaseLosses);
        }
    }

    async Task RetryLaterAsync(InFlight entry, string error)
    {
        if (await Call(c => source.RetryLaterAsync(entry.Lease, Backoff(entry.Lease.Attempts), error, c)).ConfigureAwait(false))
        {
            Interlocked.Increment(ref RetriesScheduled);
        }
    }

    async Task BlockAsync(InFlight entry, string reason)
    {
        logger.LogWarning("Runwick outbox message {MessageId} blocked: {Reason}.", entry.Lease.MessageId, reason);
        if (await Call(c => source.BlockAsync(entry.Lease, reason, c)).ConfigureAwait(false))
        {
            Interlocked.Increment(ref Blocked);
        }
    }

    /// <summary>Every source and destination call is bounded by the provider timeout and the abort token.</summary>
    async Task<T> Call<T>(Func<CancellationToken, Task<T>> call)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
        cts.CancelAfter(options.ProviderCallTimeout);
        return await call(cts.Token).ConfigureAwait(false);
    }

    async Task Call(Func<CancellationToken, Task> call)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
        cts.CancelAfter(options.ProviderCallTimeout);
        await call(cts.Token).ConfigureAwait(false);
    }

    /// <summary>Exponential from RetryInitial to RetryMax with equal jitter (14 §4.3).</summary>
    TimeSpan Backoff(int attempts)
    {
        var nominal = options.RetryInitial.TotalMilliseconds * Math.Pow(2, Math.Clamp(attempts, 0, 30));
        var capped = Math.Min(nominal, options.RetryMax.TotalMilliseconds);
        var half = (long)(capped / 2);
        return TimeSpan.FromMilliseconds(half + RandomNumberGenerator.GetInt32((int)Math.Min(int.MaxValue, half + 1)));
    }

    async Task<bool> DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, time, ct).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return false;
        }
    }
}
