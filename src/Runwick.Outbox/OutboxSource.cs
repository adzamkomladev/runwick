using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Outbox;

/// <summary>A staged message claimed by one relay owner (14 §4.1). <see cref="Token"/> guards every later source write.</summary>
internal sealed record OutboxLease(
    Guid MessageId,
    Guid Token,
    Guid DestinationStoreId,
    string DestinationApplication,
    byte[] Envelope,
    byte[] DeliveryFingerprint,
    DateTimeOffset StagedAt,
    DateTimeOffset AcceptUntil,
    int Attempts,
    Guid? TransportCommandId,
    DateTimeOffset? TransportNotAfter);

/// <summary>What a delivered mark records (14 §4.2).</summary>
internal sealed record OutboxDelivered(Guid JobId, string Disposition, string ConfirmedProfile, DateTimeOffset DeliveredAt);

/// <summary>
/// An optional wake channel of a source (14 Changes 2026-11-02, PF-07): signals after a committed stage so the relay
/// claims before its idle poll. A hint only; the relay's polling stays the correctness path.
/// </summary>
internal interface IOutboxWakeSource
{
    /// <summary>
    /// Runs until <paramref name="ct"/>; calls <paramref name="onWake"/> per notification, <paramref name="onSubscribed"/>
    /// after each (re)subscription and <paramref name="onLost"/> when the session is lost or refused, before the reconnect
    /// back-off. The relay polls at its longer idle interval only between the two (14 Changes 2026-11-02, idle caps).
    /// </summary>
    Task ListenAsync(Action onWake, Action onSubscribed, Action onLost, CancellationToken ct);
}

/// <summary>
/// One source database (14 §4). Every method after <see cref="InitializeAsync"/> is a short transaction of its own; a
/// <c>false</c> return means the lease token no longer matches and nothing was written.
/// </summary>
internal interface IOutboxSource
{
    /// <summary>The immutable source UUID; valid after <see cref="InitializeAsync"/>.</summary>
    Guid SourceStoreId { get; }

    /// <summary>Checks the installed source schema version and reads the source UUID. Never creates or regenerates it.</summary>
    Task InitializeAsync(CancellationToken ct);

    Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct);

    /// <summary>
    /// Claims and can persist a new transport identity atomically, using the relay's clock for transport expiry.
    /// Older sources return their usual claim and let the relay record a transport before sending.
    /// </summary>
    Task<IReadOnlyList<OutboxLease>> ClaimWithTransportAsync(Guid owner, int max, TimeSpan leaseDuration, DateTimeOffset transportAsOf,
        DateTimeOffset transportBound, CancellationToken ct) => ClaimAsync(owner, max, leaseDuration, ct);

    Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct);

    /// <summary>Persists the transport command ID and NotAfter before the request is sent (14 §6.2).</summary>
    Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct);

    Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct);

    Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct);

    Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct);

    /// <summary>
    /// Retention (17 §6): deletes one batch of <c>delivered</c> rows with <c>delivered_at</c> older than 7 days and
    /// <c>accept_until</c> more than 24 h past, oldest first, at most <paramref name="maxRows"/> rows and — after the
    /// first row — <paramref name="maxBytes"/> of intent bytes. Locked rows are skipped. Returns the rows deleted.
    /// <c>pending</c>, <c>leased</c> and <c>blocked</c> rows are never deleted.
    /// </summary>
    Task<int> PurgeDeliveredAsync(int maxRows, long maxBytes, CancellationToken ct);
}

/// <summary>The frozen intent a source adapter inserts (14 §3.2).</summary>
internal sealed record OutboxIntent(
    Guid MessageId,
    Guid DestinationStoreId,
    string DestinationApplication,
    byte[] Envelope,
    byte[] BusinessDigest,
    byte[] PolicyDigest,
    byte[] DeliveryFingerprint,
    Guid ProposedJobId,
    DateTimeOffset StagedAt,
    DateTimeOffset AcceptUntil);

/// <summary>
/// The adapter-independent half of Stage: freeze the job exactly as Prepare does, against the configured destination,
/// with no I/O. The adapter supplies the source UUID and source-database time.
/// </summary>
internal sealed class OutboxStager(RunwickRuntime runtime, DispatchPreparer preparer, OutboxDestination destination)
{
    public PreparedDispatch Prepare(IQueueJob job, OutboxStageOptions? options)
    {
        ArgumentNullException.ThrowIfNull(job);
        var storeId = destination.StoreId ?? (runtime.IsReady
            ? runtime.StoreId
            : throw new OutboxStagingException("outbox_destination_unknown",
                "The destination store ID is not configured and Runwick has not described the destination yet."));
        return preparer.Prepare(storeId, job, options?.Dispatch);
    }

    public static TimeSpan Horizon(OutboxStageOptions? options)
    {
        var horizon = options?.AcceptanceHorizon ?? OutboxOperations.DefaultHorizon;
        return horizon > TimeSpan.Zero && horizon <= OutboxOperations.MaxHorizon && horizon.Ticks % TimeSpan.TicksPerMillisecond == 0
            ? horizon
            : throw new OutboxStagingException("outbox_horizon_invalid", "The acceptance horizon must be a whole number of milliseconds in (0, 30 days].");
    }

    /// <summary>Freezes one message at source time <paramref name="stagedAt"/> (ms floor).</summary>
    public static OutboxIntent Freeze(Guid sourceStoreId, Guid messageId, PreparedDispatch prepared, DateTimeOffset stagedAt, TimeSpan horizon)
    {
        var staged = DateTimeOffset.FromUnixTimeMilliseconds(Fingerprints.FloorEpochMs(stagedAt));
        var acceptUntil = staged + horizon;
        var delivery = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, new OutboxSourceIdentity(sourceStoreId, messageId),
            acceptUntil.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
        return new OutboxIntent(messageId, prepared.StoreId, prepared.Application, prepared.Export().ToArray(),
            prepared.Digests.Business, prepared.Digests.Policy, delivery, prepared.Intent.ProposedJobId, staged, acceptUntil);
    }

    /// <summary>Same message ID: the same destination and the same business and policy intent (14 §3.1).</summary>
    public static bool SameIntent(OutboxIntent incoming, Guid destinationStoreId, string destinationApplication, byte[] business, byte[] policy) =>
        incoming.DestinationStoreId == destinationStoreId
        && string.Equals(incoming.DestinationApplication, destinationApplication, StringComparison.Ordinal)
        && incoming.BusinessDigest.AsSpan().SequenceEqual(business)
        && incoming.PolicyDigest.AsSpan().SequenceEqual(policy);

    /// <summary>
    /// Undoes a failed or cancelled Stage on the caller's transaction (14 §2.1 step 5). It runs on every exception,
    /// cancellation included, and never takes the caller's token: a cancelled token must not skip the undo and leave the
    /// staged row inside a transaction the caller may still commit. When the provider already ended the transaction (a
    /// broken connection, or an error SQLite answers with an automatic rollback) the savepoint is gone; that failure is
    /// swallowed so the caller sees the original error unchanged (TX-03).
    /// </summary>
    public static async Task RollBackToSavepointAsync(System.Data.Common.DbTransaction transaction, string savepoint)
    {
        try
        {
            await transaction.RollbackAsync(savepoint, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is System.Data.Common.DbException or InvalidOperationException or ObjectDisposedException)
        {
            // The transaction (and with it the savepoint) no longer exists; nothing staged can commit.
        }
    }

    public static StagedOutboxMessage Result(Guid sourceStoreId, OutboxIntent intent, bool existing) =>
        new(sourceStoreId, intent.MessageId, intent.DestinationStoreId, intent.DestinationApplication, new JobId(intent.ProposedJobId),
            intent.StagedAt, intent.AcceptUntil, existing);
}
