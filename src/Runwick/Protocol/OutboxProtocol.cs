using Runwick.Serialization;

namespace Runwick.Protocol;

/// <summary>Outbox destination extension (14 §5): operation name, bounds and horizon defaults.</summary>
internal static class OutboxOperations
{
    public const string ReceiveOutbox = "ReceiveOutbox";

    /// <summary>A transport command's NotAfter is at most this far ahead, and never beyond AcceptUntil (14 §6).</summary>
    public static readonly TimeSpan TransportCap = TimeSpan.FromSeconds(30);

    public static readonly TimeSpan DefaultHorizon = TimeSpan.FromDays(7);

    public static readonly TimeSpan MaxHorizon = TimeSpan.FromDays(30);

    /// <summary>A delivery mapping is kept at least this long after AcceptUntil (14 §5.2).</summary>
    public static readonly TimeSpan MappingRetention = TimeSpan.FromHours(24);

    /// <summary>The persistence profile a PostgreSQL destination confirms: the commit of the mapping itself.</summary>
    public const string PostgresProfile = "postgres.commit.v1";
}

/// <summary>Immutable source message identity (14 §3).</summary>
internal sealed record OutboxSourceIdentity(Guid SourceStoreId, Guid MessageId);

/// <summary>
/// ReceiveOutbox/1 body: the source identity, the frozen dispatch item, the absolute acceptance cutoff and the
/// delivery fingerprint computed at Stage. <see cref="StoreId"/> is the destination binding.
/// </summary>
internal sealed record ReceiveOutboxRequest(
    OutboxSourceIdentity Source, DispatchItem Item, DateTimeOffset AcceptUntil, byte[] DeliveryFingerprint, Guid StoreId);

/// <summary>The retained delivery mapping; <see cref="FirstDelivery"/> is false when an existing mapping was returned.</summary>
internal sealed record OutboxDelivery(
    Guid JobId, DispatchDisposition Disposition, DuplicateMatch Match, bool PolicyDrift, bool FirstDelivery,
    DateTimeOffset DeliveredAt, DateTimeOffset AcceptUntil, string ConfirmedProfile);

/// <summary>Destination side of the outbox (14 §5). Required of every driver that advertises <c>outbox_destination</c>.</summary>
internal interface IOutboxDestinationStore
{
    Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct);

    /// <summary>
    /// Read-only mapping lookup, used after AcceptUntil when no new command may apply (14 §6.3). Throws
    /// <see cref="StoreNotAppliedException"/> when the store cannot be read.
    /// </summary>
    Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct);
}

/// <summary>
/// A mapping lookup: <see cref="Delivery"/> when a mapping with the same delivery fingerprint is retained;
/// <see cref="Conflict"/> when a mapping exists for the identity with different intent; neither when none is retained.
/// </summary>
internal sealed record OutboxLookup(OutboxDelivery? Delivery, bool Conflict);

/// <summary>Hash domains <c>runwick.outbox.delivery/1</c> and <c>runwick.outbox.command/1</c> (14 §7).</summary>
internal static class OutboxFingerprints
{
    public const string DeliveryDomain = "runwick.outbox.delivery/1";
    public const string CommandDomain = "runwick.outbox.command/1";

    /// <summary>
    /// Covers the destination binding, the source identity, the absolute cutoff and all frozen intent: the business and
    /// policy digests (02 §7), the diagnostic metadata and the proposed job ID. Excludes the transport command.
    /// </summary>
    public static byte[] Delivery(Guid destinationStoreId, string application, OutboxSourceIdentity source, long acceptUntilMs,
        DispatchIntent intent, DispatchDigests digests) =>
        Fingerprints.Digest(DeliveryDomain, CanonicalValue.Object()
            .Add("acceptUntil", acceptUntilMs)
            .Add("application", application)
            .Add("businessDigest", DispatchDigests.Hex(digests.Business))
            .Add("destinationStoreId", Fingerprints.Uuid(destinationStoreId))
            .Add("diagnosticMetadata", Fingerprints.StringMap(intent.DiagnosticMetadata))
            .Add("fingerprintVersion", Fingerprints.Version)
            .Add("messageId", Fingerprints.Uuid(source.MessageId))
            .Add("policyDigest", DispatchDigests.Hex(digests.Policy))
            .Add("proposedJobId", Fingerprints.Uuid(intent.ProposedJobId))
            .Add("sourceStoreId", Fingerprints.Uuid(source.SourceStoreId))
            .Build());

    /// <summary>The ordinary command envelope (02 §8.3) under the outbox command domain; the body is the delivery digest.</summary>
    public static byte[] Command(Guid destinationStoreId, string application, Guid commandId, DateTimeOffset notAfter, byte[] delivery) =>
        Fingerprints.Digest(CommandDomain, Fingerprints.Command(destinationStoreId, application, OutboxOperations.ReceiveOutbox, 1,
            commandId, Fingerprints.FloorEpochMs(notAfter),
            CanonicalValue.Object().Add("deliveryDigest", DispatchDigests.Hex(delivery)).Build()));
}
