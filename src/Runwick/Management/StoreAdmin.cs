namespace Runwick;

/// <summary>One application's admission mode after an operator change (23 §3).</summary>
public sealed record AdmissionChange(string Application, string PreviousMode, string Mode, long Revision, bool Changed);

/// <summary>One application's capability after an operator activation (10 §6.1, 23 §3.6). <paramref name="Changed"/> is false when it was already active.</summary>
public sealed record CapabilityChange(string Application, string Capability, int Version, long Revision, bool Changed);

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

    /// <summary>The audit <c>actor_scheme</c> of an operator command; the subject is the caller's <c>actor</c> string (08 §2).</summary>
    internal const string OperatorScheme = "operator";

    /// <summary>The operator actor becomes an audit subject: 1 to 256 UTF-8 bytes without control characters (08 §2).</summary>
    internal static void CheckActor(string actor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        if (System.Text.Encoding.UTF8.GetByteCount(actor) > 256 || actor.Any(char.IsControl))
        {
            throw new ArgumentException("The actor must be at most 256 UTF-8 bytes without control characters.", nameof(actor));
        }
    }

    /// <summary>Capabilities an operator can raise; the rest are active from the first registration.</summary>
    public static readonly IReadOnlyList<string> ActivatableCapabilities = ["scheduler", "rate_admission"];

    /// <summary>Null when <paramref name="name"/> can be activated, else <c>invalid_capability</c>.</summary>
    public static string? CheckCapability(string name) => ActivatableCapabilities.Contains(name, StringComparer.Ordinal) ? null : "invalid_capability";

    /// <summary>The binary's version of <paramref name="name"/>.</summary>
    public static int CapabilityVersion(string name) => Hosting.RunwickRuntime.KnownCapabilities[name];

    /// <summary>The required-capabilities JSON with <paramref name="name"/> raised to <paramref name="version"/>, or null when it is already there. Never lowers one.</summary>
    public static string? Raise(string requiredJson, string name, int version)
    {
        var o = System.Text.Json.Nodes.JsonNode.Parse(requiredJson)?.AsObject() ?? [];
        if (o.TryGetPropertyValue(name, out var have) && have!.GetValue<int>() >= version)
        {
            return null;
        }

        o[name] = version;
        return o.ToJsonString();
    }
}
