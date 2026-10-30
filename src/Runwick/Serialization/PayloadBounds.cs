namespace Runwick.Serialization;

/// <summary>Structural bounds of the <c>runwick.json</c> v1 profile (spec 02 §4.1).</summary>
internal static class PayloadBounds
{
    public const int MaxPayloadBytes = 1_048_576;
    public const int MaxDepth = 32;
    public const int MaxStringBytes = 262_144;
    public const int MaxPropertyNameBytes = 256;
    public const int MaxPropertiesPerObject = 256;
    public const int MaxArrayElements = 16_384;
    public const int MaxNumberLexemeBytes = 128;
    public const int MaxMetadataBytes = 16_384;
    public const int MaxMetadataEntries = 32;
    public const int MaxEnvelopeBytes = 2_097_152;
    public const int MaxBatchItems = 100;
    public const int MaxCapabilityTuples = 512;
}

/// <summary>
/// Generation-policy bounds shared by registration and import (spec 03 I-6; spec 04
/// <c>job_generations</c> CHECKs; spec 01 I-23).
/// </summary>
internal static class PolicyBounds
{
    public const int MaxExecutions = 100;
    public const int MaxUnstartedAbandonments = 100;
    public const long MaxExecutionTimeoutMs = 2_592_000_000; // 30 days
    public const long MaxRetryDelayMs = 86_400_000;          // 24 hours
    public const long MaxWindowMs = 315_360_000_000;         // 3,650 days (spec 02 IC-15)

    /// <summary>Null when <paramref name="policy"/> satisfies every bound; otherwise the violated rule.</summary>
    public static string? Violation(ExecutionPolicy policy)
    {
        if (policy.MaxExecutions is < 1 or > MaxExecutions)
        {
            return $"maxExecutions must be within 1–{MaxExecutions}.";
        }

        if (policy.MaxConsecutiveUnstartedAbandonments is < 1 or > MaxUnstartedAbandonments)
        {
            return $"maxConsecutiveUnstartedAbandonments must be within 1–{MaxUnstartedAbandonments}.";
        }

        if (policy.ExecutionTimeoutMs is < 1 or > MaxExecutionTimeoutMs)
        {
            return $"executionTimeout must be within 1–{MaxExecutionTimeoutMs} ms.";
        }

        return RetryViolation(policy.Retry, "retry") ?? RetryViolation(policy.UnstartedRetry, "unstartedRetry");
    }

    static string? RetryViolation(RetryPolicy r, string name)
    {
        if (r.Backoff != "exponential" || r.EvaluatorVersion != 1 || r.JitterKind != "equal")
        {
            return $"{name} must use the exponential v1 evaluator with equal jitter.";
        }

        if (r.InitialDelayMs is < 1 or > MaxRetryDelayMs || r.MaximumDelayMs < r.InitialDelayMs || r.MaximumDelayMs > MaxRetryDelayMs)
        {
            return $"{name} delays must satisfy 1 ms ≤ initial ≤ maximum ≤ {MaxRetryDelayMs} ms.";
        }

        return null;
    }
}

/// <summary>Per-container/token bounds the scanner enforces.</summary>
internal readonly record struct ScannerLimits(
    int MaxDepth, int MaxStringBytes, int MaxPropertyNameBytes, int MaxProperties, int MaxArrayElements, int MaxNumberBytes)
{
    /// <summary>The <c>runwick.json</c> v1 payload bounds (spec 02 §4.1).</summary>
    public static readonly ScannerLimits Payload = new(
        PayloadBounds.MaxDepth, PayloadBounds.MaxStringBytes, PayloadBounds.MaxPropertyNameBytes,
        PayloadBounds.MaxPropertiesPerObject, PayloadBounds.MaxArrayElements, PayloadBounds.MaxNumberLexemeBytes);

    /// <summary>
    /// Prepared export envelopes: same grammar, UTF-8 and duplicate rules (spec 02 §9.3 step 2); the
    /// base64 payload string may use the whole envelope budget. Schema rules bound everything else.
    /// </summary>
    public static readonly ScannerLimits Envelope = Payload with { MaxStringBytes = PayloadBounds.MaxEnvelopeBytes };
}

/// <summary>A payload or envelope failed validation. Always known-not-applied.</summary>
internal sealed class PayloadValidationException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}
