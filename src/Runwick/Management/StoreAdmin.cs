namespace Runwick;

/// <summary>One application's admission mode after an operator change (23 §3).</summary>
public sealed record AdmissionChange(string Application, string PreviousMode, string Mode, long Revision, bool Changed);

/// <summary>An application's gate as stored (23 §6).</summary>
public sealed record ApplicationAdmissionState(string Application, string AdmissionMode, long GateRevision, string RequiredCapabilities);

/// <summary>
/// What an operator can read before an upgrade, restore or clone (23 §6). The lease and presence counts are what the store
/// records; <see cref="Quiescence"/> always says they do not prove that any process has stopped. Counts are null where a driver
/// does not report them.
/// </summary>
public sealed record StoreAdminStatus(
    Guid StoreId, long StoreRevision, long BinaryRevision, string MigrationMode, IReadOnlyList<ApplicationAdmissionState> Applications,
    int? OwnedLeases, int? WorkerPresences, int? ExpiredWorkerPresences, string Quiescence);

/// <summary>A clone's new logical store identity (23 §7).</summary>
public sealed record StoreIdentityChange(Guid OldStoreId, Guid NewStoreId);

/// <summary>The fixed rules every driver applies to an operator admission change (23 §3).</summary>
public static class StoreAdminRules
{
    /// <summary>The sentence every status carries (23 §6).</summary>
    public const string QuiescenceNotice =
        "unverified: expired or absent lease and presence rows do not prove that no old process is running; stop processes yourself";

    /// <summary>Null when the change is allowed, else the error code. <c>open</c> to <c>maintenance</c> directly is refused: drain first.</summary>
    public static string? Check(string previous, string mode) =>
        mode is not ("open" or "drain" or "maintenance") ? "invalid_mode"
        : previous == "open" && mode == "maintenance" ? "drain_first"
        : null;
}
