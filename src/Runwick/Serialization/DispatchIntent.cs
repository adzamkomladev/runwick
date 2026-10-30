namespace Runwick.Serialization;

/// <summary>Stable scope identity used in fingerprints (kind ∈ queue, job-type, resource).</summary>
internal sealed record ScopeRef(string Kind, string Key)
{
    public const string QueueKind = "queue";
    public const string JobTypeKind = "job-type";
    public const string ResourceKind = "resource";

    /// <summary>Deduplicated, sorted by (kind bytes, key bytes) — spec 02 IC-06.</summary>
    public static IReadOnlyList<ScopeRef> Normalize(IEnumerable<ScopeRef> scopes) =>
        [.. scopes.Distinct().OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance)];
}

/// <summary>Effective retry policy snapshot (mirrors <c>RetryPolicySnapshot</c>).</summary>
internal sealed record RetryPolicy(string Backoff, int EvaluatorVersion, string JitterKind, long InitialDelayMs, long MaximumDelayMs)
{
    public static readonly RetryPolicy DefaultExecution = new("exponential", 1, "equal", 5_000, 60_000);
    public static readonly RetryPolicy DefaultUnstarted = new("exponential", 1, "equal", 1_000, 30_000);
}

/// <summary>Effective generation policy frozen at Prepare (spec 02 §7.2).</summary>
internal sealed record ExecutionPolicy(
    int MaxExecutions,
    long? ExecutionTimeoutMs,
    RetryPolicy Retry,
    int MaxConsecutiveUnstartedAbandonments,
    RetryPolicy UnstartedRetry)
{
    public static readonly ExecutionPolicy Default = new(3, null, RetryPolicy.DefaultExecution, 5, RetryPolicy.DefaultUnstarted);
}

/// <summary>
/// The frozen business intent of one dispatch item (spec 02 §7.1) plus its diagnostic metadata,
/// policy and proposed job ID. Holds the original payload bytes, never a CLR object.
/// </summary>
internal sealed record DispatchIntent(
    string JobName,
    int PayloadVersion,
    string SerializerProfileId,
    int SerializerProfileVersion,
    byte[] PayloadUtf8,
    string Queue,
    string Priority,
    long? NotBeforeMs,
    IReadOnlyList<ScopeRef> ConcurrencyScopes,
    IReadOnlyList<ScopeRef> RateScopes,
    string? ActiveKey,
    string? WindowKey,
    long? WindowMs,
    IReadOnlyDictionary<string, string> BusinessMetadata,
    IReadOnlyDictionary<string, string> DiagnosticMetadata,
    ExecutionPolicy Policy,
    Guid ProposedJobId)
{
    public override string ToString() => $"DispatchIntent({JobName} v{PayloadVersion}, {PayloadUtf8.Length} bytes)";
}

/// <summary>The three digests of a dispatch (32 raw bytes each).</summary>
internal sealed record DispatchDigests(byte[] Business, byte[] Policy, byte[] Command)
{
    public static string Hex(byte[] digest) => Convert.ToHexStringLower(digest);
}
