namespace Runwick.Outbox;

/// <summary>
/// The result of staging one outbox message on the caller's transaction (14 §2). Nothing is delivered until the
/// caller commits and a relay sends it; <see cref="Existing"/> is true when the same message ID with the same intent
/// was already staged.
/// </summary>
public sealed record StagedOutboxMessage(
    Guid SourceStoreId,
    Guid MessageId,
    Guid DestinationStoreId,
    string DestinationApplication,
    JobId ProposedJobId,
    DateTimeOffset StagedAt,
    DateTimeOffset AcceptUntil,
    bool Existing);

/// <summary>Per-message staging options (14 §2.2).</summary>
public sealed record OutboxStageOptions
{
    /// <summary>A stable caller-provided message ID; a new UUIDv7 when absent. Never reuse an ID for different intent.</summary>
    public Guid? MessageId { get; init; }

    /// <summary>Ordinary dispatch options, frozen at Stage exactly as Prepare freezes them.</summary>
    public DispatchOptions? Dispatch { get; init; }

    /// <summary>Acceptance horizon from source staging time; default 7 days, at most 30 days (14 §6.1).</summary>
    public TimeSpan? AcceptanceHorizon { get; init; }
}

/// <summary>Where staged messages are delivered (14 §2.3). One destination per relay group in M3.</summary>
public sealed record OutboxDestination
{
    /// <summary>
    /// The destination store UUID. When null, Stage uses the store this host's Runwick runtime has already
    /// described, and fails with <c>outbox_destination_unknown</c> before the first handshake.
    /// </summary>
    public Guid? StoreId { get; init; }
}

/// <summary>Relay budgets (14 §4.3, 06 §1.12).</summary>
public sealed class OutboxRelayOptions
{
    public bool Enabled { get; set; } = true;

    public int MaxInFlight { get; set; } = 4;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);

    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ProviderCallTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan IdlePoll { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan RetryInitial { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan RetryMax { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// A free slot sends up to this many waiting plain messages to the destination as one transaction, each keeping its own
    /// transport command, receipt and mapping (14 Changes 2026-11-15, D4; 2026-11-20). 4 is the default since 2026-11-20 (Outbox D);
    /// 1 sends each message alone. Only destinations that implement grouped delivery (PostgreSQL) honor it; others send alone.
    /// </summary>
    public int DeliveryGroupSize { get; set; } = 4;

    /// <summary>
    /// The claim waits until at least this many of the <see cref="MaxInFlight"/> slots are free (or nothing is in flight, or
    /// <see cref="ClaimBatchWait"/> has passed), so one source claim carries several messages instead of one per freed slot
    /// (14 Changes 2026-11-17, D2). 1 (the default) claims as soon as any slot is free.
    /// </summary>
    public int ClaimBatchMin { get; set; } = 1;

    /// <summary>The longest a claim waits for <see cref="ClaimBatchMin"/> slots to free. Ignored when <see cref="ClaimBatchMin"/> is 1.</summary>
    public TimeSpan ClaimBatchWait { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>
    /// When true (the default since 2026-11-20, Outbox D), a message stops counting against <see cref="MaxInFlight"/> once the destination
    /// has answered, so the next send overlaps its source mark. The source lease is still held and renewed until the mark finishes, and
    /// shutdown still waits for it. False keeps a slot until the mark is written (14 Changes 2026-11-17, D2).
    /// </summary>
    public bool ReleaseSlotAfterReply { get; set; } = true;

    /// <summary>
    /// When true, delivered marks that queue behind one another on the source's single control connection are written in one
    /// statement and one commit, each still guarded by its own lease token (14 Changes 2026-11-17, D2). Honored by the
    /// PostgreSQL source; other sources ignore it. True is the default since 2026-11-20 (Outbox D); false writes one statement per mark.
    /// </summary>
    public bool BatchDeliveredMarks { get; set; } = true;

    /// <summary>Transport command lifetime; at most 30 s and never beyond AcceptUntil.</summary>
    public TimeSpan TransportLifetime { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<string> Validate()
    {
        var errors = new List<string>();
        if (MaxInFlight is < 1 or > 64)
        {
            errors.Add("MaxInFlight must be 1-64.");
        }

        if (DeliveryGroupSize is < 1 or > 16)
        {
            errors.Add("DeliveryGroupSize must be 1-16.");
        }

        if (ClaimBatchMin < 1 || ClaimBatchMin > MaxInFlight || ClaimBatchWait < TimeSpan.Zero || ClaimBatchWait > TimeSpan.FromSeconds(1))
        {
            errors.Add("ClaimBatchMin must be 1 to MaxInFlight and ClaimBatchWait 0 to 1 s.");
        }

        if (LeaseDuration < TimeSpan.FromSeconds(5) || RenewalInterval <= TimeSpan.Zero || RenewalInterval * 3 > LeaseDuration)
        {
            errors.Add("LeaseDuration must be at least 5 s and at least three renewal intervals.");
        }

        if (ProviderCallTimeout <= TimeSpan.Zero || ProviderCallTimeout >= RenewalInterval)
        {
            errors.Add("ProviderCallTimeout must be positive and shorter than RenewalInterval.");
        }

        if (IdlePoll <= TimeSpan.Zero || RetryInitial <= TimeSpan.Zero || RetryMax < RetryInitial)
        {
            errors.Add("IdlePoll, RetryInitial and RetryMax must be positive with RetryMax >= RetryInitial.");
        }

        if (TransportLifetime <= TimeSpan.Zero || TransportLifetime > Protocol.OutboxOperations.TransportCap)
        {
            errors.Add("TransportLifetime must be positive and at most 30 s.");
        }

        return errors;
    }
}

/// <summary>The same message ID was staged before with different intent or destination (14 §3).</summary>
public sealed class OutboxMessageConflictException(Guid messageId)
    : RunwickException("outbox_message_conflict", ErrorDisposition.KnownNotApplied,
        $"Outbox message {messageId} was already staged with different intent; message IDs are never reused.")
{
    public Guid MessageId { get; } = messageId;
}

/// <summary>Staging could not write: the transaction, the source schema or the destination binding is unusable.</summary>
public sealed class OutboxStagingException(string code, string message, Exception? innerException = null)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message, innerException);
