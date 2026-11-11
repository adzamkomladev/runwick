using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Internal;
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
    Task? _listener;
    Task? _cleanupLoop;
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
        _cleanupLoop = Task.Run(CleanupLoopAsync, CancellationToken.None);
        if (source is IOutboxWakeSource wake)
        {
            // Optional (14 Changes 2026-11-02, PF-07): a committed stage ends the idle poll early.
            _listener = Task.Run(() => ListenAsync(wake), CancellationToken.None);
        }

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

        if (_listener is { } listener)
        {
            await listener.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        if (_cleanupLoop is { } cleanup)
        {
            await cleanup.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
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
        if (_listener is { } listener)
        {
            await listener.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        if (_cleanupLoop is { } cleanup)
        {
            await cleanup.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

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
                    Interlocked.Increment(ref Claims);
                    var leases = await Call(c => source.ClaimAsync(_owner, free, options.LeaseDuration, c), "OutboxClaim").ConfigureAwait(false);
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
                // Nothing claimed with a free slot: the idle poll, longer while the wake listener is healthy. A full or
                // renewal-paused relay keeps IdlePoll (a freed slot ends the wait either way).
                var idle = claimed == 0 && free > 0 && !_renewalUncertain ? IdlePollNow : options.IdlePoll;
                try
                {
                    await _slotFreed.WaitAsync(idle, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    /// <summary>Wake notifications received (tests and diagnostics).</summary>
    public long Notifications;

    /// <summary>Claim calls (tests and diagnostics).</summary>
    public long Claims;

    /// <summary>Listener sessions lost or refused after a subscription (tests and diagnostics).</summary>
    public long ListenerLosses;

    /// <summary>
    /// While the wake listener is subscribed, the idle poll is this multiple of <see cref="OutboxRelayOptions.IdlePoll"/>
    /// (1 s to 5 s by default; 14 Changes 2026-11-02, idle caps).
    /// </summary>
    internal const int ListeningIdleFactor = 5;

    volatile bool _listenerHealthy;

    /// <summary>True from a successful (re)subscription until the session is lost, refused or the listener stops.</summary>
    public bool ListenerHealthy => _listenerHealthy;

    /// <summary>The idle poll now: 5 × IdlePoll while the listener is healthy, IdlePoll otherwise.</summary>
    internal TimeSpan IdlePollNow => _listenerHealthy ? options.IdlePoll * ListeningIdleFactor : options.IdlePoll;

    async Task ListenAsync(IOutboxWakeSource wake)
    {
        try
        {
            await wake.ListenAsync(OnNotification, OnSubscribed, OnListenerLost, _claimStop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_claimStop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Runwick outbox wake listener stopped; the relay keeps polling.");
        }
        finally
        {
            OnListenerLost();
        }
    }

    void OnNotification()
    {
        Interlocked.Increment(ref Notifications);
        OnWake();
    }

    /// <summary>(Re)subscribed: stages committed while unsubscribed were not notified, so claim once now.</summary>
    void OnSubscribed()
    {
        _listenerHealthy = true;
        OnWake();
    }

    /// <summary>
    /// Lost, refused or stopped: stages are no longer notified, so the 1 s poll applies again. The current wait ends now,
    /// so no wait begun under the longer poll outlives the session.
    /// </summary>
    void OnListenerLost()
    {
        if (!_listenerHealthy)
        {
            return;
        }

        _listenerHealthy = false;
        Interlocked.Increment(ref ListenerLosses);
        OnWake();
    }

    void OnWake()
    {
        try
        {
            if (_slotFreed.CurrentCount == 0)
            {
                _slotFreed.Release();
            }
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>Delivered source rows deleted by this relay's cleanup loop (tests and diagnostics).</summary>
    public long Purged;

    /// <summary>The 17 §4.4 budgets, fixed for the relay: 100 rows and 1 MiB of intent per batch, 1 s between batches, 60 s idle.</summary>
    internal const int CleanupRows = 100;
    internal const long CleanupBytes = 1 << 20;
    static readonly TimeSpan CleanupPause = TimeSpan.FromSeconds(1), CleanupIdle = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Source cleanup (17 §6): one batch at a time until a batch deletes nothing, then the idle poll. A failed batch backs
    /// off 5 s doubling to 5 min. It stops with claims; several relays on one source skip each other's locked rows.
    /// </summary>
    async Task CleanupLoopAsync()
    {
        var stop = _claimStop.Token;
        var failures = 0;
        while (!stop.IsCancellationRequested)
        {
            TimeSpan wait;
            try
            {
                var rows = await Call(c => source.PurgeDeliveredAsync(CleanupRows, CleanupBytes, c)).ConfigureAwait(false);
                Interlocked.Add(ref Purged, rows);
                failures = 0;
                wait = rows > 0 ? CleanupPause : CleanupIdle;
            }
            catch (Exception ex) when (!stop.IsCancellationRequested && ex is not OutOfMemoryException)
            {
                logger.LogDebug(ex, "Runwick outbox source cleanup failed.");
                wait = TimeSpan.FromTicks(Math.Min(TimeSpan.FromSeconds(5).Ticks << Math.Min(failures++, 6), TimeSpan.FromMinutes(5).Ticks));
            }

            if (!await DelayAsync(wait, stop).ConfigureAwait(false))
            {
                return;
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
                if (entry.Lost || !await Call(c => source.RecordTransportAsync(lease, commandId, notAfter, c), "OutboxRecordTransport").ConfigureAwait(false))
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
                return await Call(c => destination!.ReceiveOutboxAsync(command, c), "ReceiveOutbox").ConfigureAwait(false);
            }
            catch (StoreOutcomeUnknownException)
            {
                // Resend the same command: the receipt (or the mapping) answers if the first one applied.
            }
            catch (OperationCanceledException) when (!_abort.IsCancellationRequested)
            {
                // Provider call timeout: the request may still be running. A hung task keeps its slot until it returns.
            }
            catch (StoreNotAppliedException ex)
            {
                Telemetry.RunwickLog.OutboxDestinationUnavailable(logger, ex.Code);
                return null;
            }

            await DelayAsync(TimeSpan.FromMilliseconds(Math.Min(1000, 100 << Math.Min(attempt, 4))), _abort.Token).ConfigureAwait(false);
        }

        if (!_abort.IsCancellationRequested)
        {
            Telemetry.RunwickLog.OutboxDestinationUnavailable(logger, "transport_unresolved");
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
            new OutboxDelivered(delivered.JobId, delivered.Disposition.ToString(), delivered.ConfirmedProfile, delivered.DeliveredAt), c),
            "OutboxMarkDelivered").ConfigureAwait(false);
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
        Telemetry.RunwickLog.OutboxDeliveryBlocked(logger, entry.Lease.MessageId, reason);
        if (await Call(c => source.BlockAsync(entry.Lease, reason, c)).ConfigureAwait(false))
        {
            Interlocked.Increment(ref Blocked);
        }
    }

    /// <summary>Every source and destination call is bounded by the provider timeout and the abort token.</summary>
    async Task<T> Call<T>(Func<CancellationToken, Task<T>> call, string? operation = null)
    {
        var started = operation is null ? 0 : PhaseTimings.Start();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
        cts.CancelAfter(options.ProviderCallTimeout);
        try
        {
            return await call(cts.Token).ConfigureAwait(false);
        }
        finally
        {
            if (operation is not null)
            {
                PhaseTimings.Lap(started, operation, "relayCall");
            }
        }
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
