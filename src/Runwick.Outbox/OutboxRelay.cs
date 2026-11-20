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

    sealed class InFlight(OutboxLease lease, long claimSentAt)
    {
        public OutboxLease Lease { get; } = lease;

        public long LeaseSentAt = claimSentAt;

        /// <summary>Diagnostics (PhaseTimings): when the claim returned, and when the message took a slot.</summary>
        public long ClaimedAt = PhaseTimings.Start();
        public long StartedAt;
        public volatile bool Lost;

        /// <summary>The slot this message's destination call holds, shared by a group; null while it waits (guarded by the slot lock).</summary>
        public SlotTicket? Slot;

        /// <summary>True once this message stopped counting toward its slot (guarded by the slot lock).</summary>
        public bool SlotDone;
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
            var room = ClaimRoom();
            if (options.ClaimBatchMin > 1 && room > 0 && room < options.ClaimBatchMin && !_inFlight.IsEmpty)
            {
                var batchWaitStarted = PhaseTimings.Start();
                // Let more room open so this claim carries a batch; a message already in flight needs no claim to make progress.
                var started = time.GetTimestamp();
                _waitingForRoom = true;
                while (room < options.ClaimBatchMin && !_inFlight.IsEmpty)
                {
                    // The wait is recomputed once here: a negative timeout would throw and end the claim loop for good.
                    var remaining = options.ClaimBatchWait - time.GetElapsedTime(started);
                    if (remaining <= TimeSpan.Zero)
                    {
                        break;
                    }

                    try
                    {
                        await _slotFreed.WaitAsync(remaining, stop).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stop.IsCancellationRequested)
                    {
                        return;
                    }

                    room = ClaimRoom();
                }

                _waitingForRoom = false;
                PhaseTimings.Lap(batchWaitStarted, "OutboxClaimLoop", "batchWait");
            }

            var claimed = 0;
            if (room > 0 && !_renewalUncertain)
            {
                try
                {
                    Interlocked.Increment(ref Claims);
                    var claimSentAt = time.GetTimestamp();
                    var now = time.GetUtcNow();
                    var bound = DateTimeOffset.FromUnixTimeMilliseconds(Fingerprints.FloorEpochMs(now + options.TransportLifetime));
                    var leases = await Call(c => source.ClaimWithTransportAsync(_owner, room, options.LeaseDuration, now, bound, c),
                        "OutboxClaim").ConfigureAwait(false);
                    failures = 0;
                    var entries = new List<InFlight>(leases.Count);
                    foreach (var lease in leases)
                    {
                        var entry = new InFlight(lease, claimSentAt);
                        if (_inFlight.TryAdd(lease.MessageId, entry))
                        {
                            claimed++;
                            entries.Add(entry);
                        }
                    }

                    Enqueue(entries);
                }
                catch (Exception ex) when (!stop.IsCancellationRequested && ex is not OutOfMemoryException)
                {
                    logger.LogWarning(ex, "Runwick outbox claim failed.");
                    await DelayAsync(Backoff(failures++), stop).ConfigureAwait(false);
                    continue;
                }
            }

            // A full claim with room left claims again at once. Otherwise wait: the source had nothing more (the idle poll, longer
            // while the wake listener is healthy; a notification ends it, and so does a lease ending while messages are in flight, since
            // rows keep arriving while the relay is busy and a source without notifications would otherwise leave them for a whole
            // poll), or there is no room (a lease ending ends it), or renewal is uncertain (IdlePoll).
            var drained = room > 0 && claimed < room && !_renewalUncertain;
            _waitingForRoom = !drained || !_inFlight.IsEmpty;
            if (drained || ClaimRoom() <= 0 || _renewalUncertain)
            {
                var idle = drained ? IdlePollNow : options.IdlePoll;
                var waitStarted = PhaseTimings.Start();
                try
                {
                    await _slotFreed.WaitAsync(idle, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }

                PhaseTimings.Lap(waitStarted, "OutboxClaimLoop", drained ? "idleWait" : "fullWait");
            }

            _waitingForRoom = false;
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
                    var sentAt = time.GetTimestamp();
                    if (await Call(c => source.RenewAsync(entry.Lease, options.LeaseDuration, c)).ConfigureAwait(false))
                    {
                        Volatile.Write(ref entry.LeaseSentAt, sentAt);
                    }
                    else if (_inFlight.ContainsKey(entry.Lease.MessageId))
                    {
                        // Not counted when the message's own mark finished meanwhile (the renewal raced it); that is no loss.
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

    /// <summary>One claimed message with its transport command built and its source lease still valid, ready to send.</summary>
    sealed record Ready(InFlight Entry, string Application, OutboxSourceIdentity Identity, byte[] Fingerprint, StoreCommand<ReceiveOutboxRequest> Command);

    /// <summary>Grouped destination calls sent (tests and diagnostics).</summary>
    public long GroupSends;

    /// <summary>Commands answered by a grouped destination call (tests and diagnostics).</summary>
    public long GroupedCommands;

    /// <summary>Grouped sends that fell back to one send per command (tests and diagnostics).</summary>
    public long GroupFallbacks;

    /// <summary>One relay slot: one destination call, alone or grouped; free again when every message it carries is done with it.</summary>
    internal sealed class SlotTicket(int messages)
    {
        public int Remaining = messages;
    }

    readonly Lock _slots = new();
    readonly Queue<InFlight> _waiting = new();
    int _slotted;
    volatile bool _waitingForRoom;

    /// <summary>Messages one slot carries: <see cref="OutboxRelayOptions.DeliveryGroupSize"/> when the destination groups, otherwise 1.</summary>
    int SlotSize => destination is IOutboxGroupDestinationStore ? options.DeliveryGroupSize : 1;

    /// <summary>Slots (<see cref="OutboxRelayOptions.MaxInFlight"/> destination calls) not held by a delivery.</summary>
    internal int FreeSlots()
    {
        lock (_slots)
        {
            return options.MaxInFlight - _slotted;
        }
    }

    /// <summary>
    /// The most leases the relay holds (14 Changes 2026-11-20): twice what the slots carry, so up to one full round of claimed messages
    /// waits beside the busy slots (messages awaiting their mark included).
    /// </summary>
    internal int LeaseCap => 2 * options.MaxInFlight * SlotSize;

    /// <summary>
    /// How many messages the next claim asks for: whatever keeps the leases at <see cref="LeaseCap"/>, so a freed slot starts its next
    /// message at once instead of waiting for a claim round trip.
    /// </summary>
    internal int ClaimRoom() => LeaseCap - _inFlight.Count;

    /// <summary>Claimed messages wait (leased and renewed, holding no slot) until a slot is free.</summary>
    void Enqueue(List<InFlight> entries)
    {
        lock (_slots)
        {
            foreach (var entry in entries)
            {
                _waiting.Enqueue(entry);
            }
        }

        Pump();
    }

    /// <summary>Starts waiting messages in free slots, up to a group per slot; called after a claim and whenever a slot frees.</summary>
    void Pump()
    {
        if (_abort.IsCancellationRequested)
        {
            return; // past the host deadline: messages still waiting keep their leases until they expire and are reclaimed
        }

        var size = SlotSize;
        List<InFlight[]>? start = null;
        lock (_slots)
        {
            while (_slotted < options.MaxInFlight && _waiting.Count > 0)
            {
                var take = Math.Min(size, _waiting.Count);
                var ticket = new SlotTicket(take);
                var batch = new InFlight[take];
                for (var i = 0; i < take; i++)
                {
                    batch[i] = _waiting.Dequeue();
                    batch[i].Slot = ticket;
                }

                _slotted++;
                (start ??= []).Add(batch);
            }
        }

        foreach (var batch in start ?? [])
        {
            _ = batch.Length == 1
                ? Task.Run(() => DeliverAsync(batch[0]), CancellationToken.None)
                : Task.Run(() => DeliverGroupAsync(batch), CancellationToken.None);
        }
    }

    /// <summary>This message is done with its slot; true when that freed the slot.</summary>
    bool GiveUpSlot(InFlight entry)
    {
        lock (_slots)
        {
            if (entry.Slot is not { } ticket || entry.SlotDone)
            {
                return false;
            }

            entry.SlotDone = true;
            if (--ticket.Remaining > 0)
            {
                return false;
            }

            _slotted--;
            return true;
        }
    }

    void Release(InFlight entry)
    {
        if (_inFlight.TryRemove(entry.Lease.MessageId, out _))
        {
            var held = !entry.SlotDone;
            if (GiveUpSlot(entry) || held)
            {
                PhaseTimings.Lap(entry.StartedAt, "OutboxDelivery", "slotHeld");
            }

            SignalRoom();
            Pump();
        }
    }

    /// <summary>The destination answered: the message stops counting toward its slot while its source mark is written.</summary>
    void FreeSlotForMark(InFlight entry)
    {
        if (options.ReleaseSlotAfterReply && !entry.SlotDone)
        {
            PhaseTimings.Lap(entry.StartedAt, "OutboxDelivery", "slotHeld");
            if (GiveUpSlot(entry))
            {
                Pump();
            }
        }
    }

    /// <summary>Room for a claim opened: wake the claim loop only when it waits for room (or shutdown waits for in-flight work).</summary>
    void SignalRoom()
    {
        if (_waitingForRoom || _claimStop.IsCancellationRequested)
        {
            OnWake();
        }
    }

    async Task DeliverAsync(InFlight entry)
    {
        try
        {
            if (await PrepareAsync(entry).ConfigureAwait(false) is { } ready)
            {
                await SendAndHandleAsync(ready).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A source write failed or the host is stopping: leave the lease to expire; the mapping makes the retry safe.
            logger.LogDebug(ex, "Runwick outbox delivery of {MessageId} did not complete.", entry.Lease.MessageId);
        }
        finally
        {
            Release(entry);
        }
    }

    /// <summary>
    /// D4: prepares every claimed message, sends the plain ones as one destination transaction (each with its own transport
    /// command, receipt and mapping), then settles each message at the source under its own lease token. Anything the group
    /// does not answer is sent alone with the same command, which the receipts make safe.
    /// </summary>
    async Task DeliverGroupAsync(InFlight[] entries)
    {
        var ready = new List<Ready>(entries.Length);
        foreach (var entry in entries)
        {
            try
            {
                if (await PrepareAsync(entry).ConfigureAwait(false) is { } r)
                {
                    ready.Add(r);
                    continue;
                }
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                logger.LogDebug(ex, "Runwick outbox delivery of {MessageId} did not complete.", entry.Lease.MessageId);
            }

            Release(entry);
        }

        var grouped = ready.Where(r => IOutboxGroupDestinationStore.CanGroup(r.Command.Body)).ToList();
        var alone = ready.Where(r => !grouped.Contains(r)).ToList();
        IReadOnlyList<StoreReply<OutboxDelivery>>? replies = null;
        if (grouped.Count >= 2)
        {
            try
            {
                replies = await TrySendGroupAsync(grouped).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                logger.LogDebug(ex, "Runwick outbox group send did not complete.");
            }
        }

        var tasks = new List<Task>(ready.Count);
        for (var i = 0; i < grouped.Count; i++)
        {
            tasks.Add(Settle(grouped[i], replies?[i]));
        }

        tasks.AddRange(alone.Select(r => Settle(r, null)));
        await Task.WhenAll(tasks).ConfigureAwait(false);

        async Task Settle(Ready r, StoreReply<OutboxDelivery>? reply)
        {
            try
            {
                if (reply is null)
                {
                    await SendAndHandleAsync(r).ConfigureAwait(false);
                }
                else
                {
                    await HandleAsync(r.Entry, r.Application, r.Identity, r.Fingerprint, reply).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                logger.LogDebug(ex, "Runwick outbox delivery of {MessageId} did not complete.", r.Entry.Lease.MessageId);
            }
            finally
            {
                Release(r.Entry);
            }
        }
    }

    /// <summary>One grouped destination call, bounded like every provider call; null sends each command alone instead.</summary>
    async Task<IReadOnlyList<StoreReply<OutboxDelivery>>?> TrySendGroupAsync(List<Ready> group)
    {
        var commands = group.Select(r => r.Command).ToArray();
        try
        {
            Interlocked.Increment(ref GroupSends);
            Interlocked.Add(ref Sends, commands.Length);
            var reply = await Call(c => ((IOutboxGroupDestinationStore)destination!).ReceiveOutboxGroupAsync(commands, c), "ReceiveOutboxGroup")
                .ConfigureAwait(false);
            if (reply is { Status: StoreStatus.Applied, Value.Outcomes: { } outcomes } && outcomes.Count == commands.Length)
            {
                Interlocked.Add(ref GroupedCommands, commands.Length);
                return outcomes;
            }
        }
        catch (Exception ex) when (ex is StoreOutcomeUnknownException or StoreNotAppliedException
            || (ex is OperationCanceledException && !_abort.IsCancellationRequested))
        {
            // Unknown or unavailable: the same commands go alone below; a receipt (or the mapping) answers if the group applied.
            logger.LogDebug(ex, "Runwick outbox group of {Count} was not answered; sending each command alone.", commands.Length);
        }

        Interlocked.Increment(ref GroupFallbacks);
        return null;
    }

    /// <summary>The ordinary single send of a prepared command and the source settlement of its reply.</summary>
    async Task SendAndHandleAsync(Ready r)
    {
        var reply = await SendAsync(r.Command).ConfigureAwait(false);
        if (reply is null)
        {
            await RetryLaterAsync(r.Entry, "transport_unresolved").ConfigureAwait(false);
            return;
        }

        await HandleAsync(r.Entry, r.Application, r.Identity, r.Fingerprint, reply).ConfigureAwait(false);
    }

    /// <summary>Everything before the destination send; null when the message was settled (blocked, past its horizon) or lost its lease.</summary>
    async Task<Ready?> PrepareAsync(InFlight entry)
    {
        var lease = entry.Lease;
        var prepareStarted = entry.StartedAt = PhaseTimings.Lap(entry.ClaimedAt, "OutboxDelivery", "claimToStart");
        PreparedDispatch prepared;
        try
        {
            prepared = dispatcher.ImportPrepared(lease.Envelope);
        }
        catch (DispatchValidationException ex)
        {
            // Malformed intent, destination binding or policy/profile drift: visible, never rewritten (OUTBOX-05).
            await BlockAsync(entry, ex.Code).ConfigureAwait(false);
            return null;
        }

        if (prepared.StoreId != lease.DestinationStoreId || !string.Equals(prepared.Application, lease.DestinationApplication, StringComparison.Ordinal))
        {
            await BlockAsync(entry, "destination_binding_mismatch").ConfigureAwait(false);
            return null;
        }

        // Contract drift: this host no longer registers the frozen job contract. Visible, never rewritten (14 §5.4).
        if (runtime.Registry.ByName(prepared.Intent.JobName) is not { } job || !job.Routes.ContainsKey(prepared.Intent.PayloadVersion))
        {
            await BlockAsync(entry, "contract_not_registered").ConfigureAwait(false);
            return null;
        }

        var identity = new OutboxSourceIdentity(source.SourceStoreId, lease.MessageId);
        var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, lease.AcceptUntil.ToUnixTimeMilliseconds(),
            prepared.Intent, prepared.Digests);
        if (!fingerprint.AsSpan().SequenceEqual(lease.DeliveryFingerprint))
        {
            await BlockAsync(entry, "delivery_fingerprint_mismatch").ConfigureAwait(false);
            return null;
        }

        if (time.GetUtcNow() >= lease.AcceptUntil)
        {
            await AfterHorizonAsync(entry, prepared.Application, identity, fingerprint).ConfigureAwait(false);
            return null;
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
                return null;
            }
        }

        // Measure the lease from the claim or renewal's send time. A queued or delayed claim reply cannot
        // authorize a first send after its conservatively measured lease lifetime.
        if (entry.Lost || time.GetElapsedTime(Volatile.Read(ref entry.LeaseSentAt)) >= options.LeaseDuration)
        {
            return null;
        }

        var command = new StoreCommand<ReceiveOutboxRequest>(prepared.Application, commandId, notAfter,
            new ReceiveOutboxRequest(identity,
                new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests) { Rates = preparer.RatesFor(prepared.Intent) }, lease.AcceptUntil, fingerprint, prepared.StoreId));
        PhaseTimings.Lap(prepareStarted, "OutboxDelivery", "prepare");
        return new Ready(entry, prepared.Application, identity, fingerprint, command);
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
        FreeSlotForMark(entry);
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
