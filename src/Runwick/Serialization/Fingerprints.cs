using System.Globalization;

namespace Runwick.Serialization;

/// <summary>Builds the canonical envelopes of spec 02 §7–§8 and hashes them.</summary>
internal static class Fingerprints
{
    public const int Version = 1;
    public const string BusinessDomain = "runwick.dispatch-request.v1";
    public const string PolicyDomain = "runwick.dispatch-policy.v1";
    public const string CommandDomain = "runwick.command.v1";

    public static CanonicalValue Business(string application, DispatchIntent intent) =>
        Business(application, intent, JsonPayloadScanner.ScanPayload(intent.PayloadUtf8));

    public static CanonicalValue Business(string application, DispatchIntent intent, CanonicalValue payload) =>
        CanonicalValue.Object()
            .Add("application", application)
            .Add("businessMetadata", StringMap(intent.BusinessMetadata))
            .Add("concurrencyScopes", Scopes(intent.ConcurrencyScopes))
            .Add("eligibility", Eligibility(intent.NotBeforeMs))
            .Add("fingerprintVersion", Version)
            .Add("jobName", intent.JobName)
            .Add("payload", payload)
            .Add("payloadVersion", intent.PayloadVersion)
            .Add("priority", intent.Priority)
            .Add("queue", intent.Queue)
            .Add("rateScopes", Scopes(intent.RateScopes))
            .Add("serializerProfileId", intent.SerializerProfileId)
            .Add("serializerProfileVersion", intent.SerializerProfileVersion)
            .Add("uniqueness", CanonicalValue.Object()
                .Add("activeKey", intent.ActiveKey)
                .Add("window", intent.WindowMs is { } w ? CanonicalValue.Int(w) : CanonicalValue.Null)
                .Add("windowKey", intent.WindowKey)
                .Build())
            .Build();

    public static CanonicalValue Policy(ExecutionPolicy policy) =>
        CanonicalValue.Object()
            .Add("executionTimeout", policy.ExecutionTimeoutMs is { } t ? CanonicalValue.Int(t) : CanonicalValue.Null)
            .Add("fingerprintVersion", Version)
            .Add("maxConsecutiveUnstartedAbandonments", policy.MaxConsecutiveUnstartedAbandonments)
            .Add("maxExecutions", policy.MaxExecutions)
            .Add("retry", Retry(policy.Retry))
            .Add("unstartedRetry", Retry(policy.UnstartedRetry))
            .Build();

    static CanonicalValue Retry(RetryPolicy r) =>
        CanonicalValue.Object()
            .Add("backoff", r.Backoff)
            .Add("evaluatorVersion", r.EvaluatorVersion)
            .Add("initialDelay", r.InitialDelayMs)
            .Add("jitterKind", r.JitterKind)
            .Add("maximumDelay", r.MaximumDelayMs)
            .Build();

    public static CanonicalValue Eligibility(long? notBeforeMs) => notBeforeMs is { } ms
        ? CanonicalValue.Object().Add("mode", "instant").Add("utcEpochMilliseconds", ms).Build()
        : CanonicalValue.Object().Add("mode", "immediate").Build();

    public static CanonicalValue Scopes(IReadOnlyList<ScopeRef> scopes) =>
        CanonicalValue.Array(ScopeRef.Normalize(scopes).Select(s =>
            CanonicalValue.Object().Add("key", s.Key).Add("kind", s.Kind).Build()));

    public static CanonicalValue StringMap(IReadOnlyDictionary<string, string> map)
    {
        var builder = CanonicalValue.Object();
        foreach (var (k, v) in map)
        {
            builder.Add(k, v);
        }

        return builder.Build();
    }

    /// <summary>The command envelope hashed under <c>runwick.command.v1</c>.</summary>
    public static CanonicalValue Command(Guid storeId, string application, string operation, int operationVersion,
        Guid commandId, long notAfterMs, CanonicalValue body) =>
        CanonicalValue.Object()
            .Add("application", application)
            .Add("body", body)
            .Add("commandId", Uuid(commandId))
            .Add("fingerprintVersion", Version)
            .Add("notAfter", notAfterMs)
            .Add("operation", operation)
            .Add("operationVersion", operationVersion)
            .Add("storeId", Uuid(storeId))
            .Build();

    public static CanonicalValue Uuid(Guid value) => CanonicalValue.Str(value.ToString("D", CultureInfo.InvariantCulture));

    public static byte[] Digest(string domain, CanonicalValue value) => CanonicalEncoder.Digest(domain, value);

    /// <summary>Digests of a <c>Dispatch</c>/1 item and command (spec 02 §8.3); the business and policy values are encoded once.</summary>
    public static DispatchDigests ForDispatch(Guid storeId, string application, Guid commandId, long notAfterMs, DispatchIntent intent)
    {
        var business = new CanonicalEncoded(CanonicalEncoder.Encode(Business(application, intent)));
        var policy = new CanonicalEncoded(CanonicalEncoder.Encode(Policy(intent.Policy)));
        var body = CanonicalValue.Object()
            .Add("business", business)
            .Add("diagnosticMetadata", StringMap(intent.DiagnosticMetadata))
            .Add("policy", policy)
            .Add("proposedJobId", Uuid(intent.ProposedJobId))
            .Build();
        return new DispatchDigests(
            Digest(BusinessDomain, business),
            Digest(PolicyDomain, policy),
            Digest(CommandDomain, Command(storeId, application, "Dispatch", 1, commandId, notAfterMs, body)));
    }

    /// <summary>
    /// Digests for each item plus the <c>DispatchMany</c>/1 command digest. Each item's business value is encoded once
    /// (for its own digest and inside the command), and each distinct policy instance once for the whole batch.
    /// </summary>
    public static (IReadOnlyList<DispatchDigests> Items, byte[] Command) ForBatch(
        Guid storeId, string application, Guid commandId, long notAfterMs, bool continueOnConflict, IReadOnlyList<DispatchIntent> items)
    {
        var digests = new List<DispatchDigests>(items.Count);
        var bodies = new List<CanonicalValue>(items.Count);
        var policies = new Dictionary<ExecutionPolicy, (CanonicalEncoded Value, byte[] Digest)>(ReferenceEqualityComparer.Instance);
        for (var ordinal = 0; ordinal < items.Count; ordinal++)
        {
            var intent = items[ordinal];
            var business = new CanonicalEncoded(CanonicalEncoder.Encode(Business(application, intent)));
            if (!policies.TryGetValue(intent.Policy, out var policy))
            {
                var encoded = new CanonicalEncoded(CanonicalEncoder.Encode(Policy(intent.Policy)));
                policies[intent.Policy] = policy = (encoded, Digest(PolicyDomain, encoded));
            }

            bodies.Add(CanonicalValue.Object()
                .Add("business", business)
                .Add("diagnosticMetadata", StringMap(intent.DiagnosticMetadata))
                .Add("ordinal", ordinal)
                .Add("policy", policy.Value)
                .Add("proposedJobId", Uuid(intent.ProposedJobId))
                .Build());
            digests.Add(new DispatchDigests(Digest(BusinessDomain, business), policy.Digest, []));
        }

        var body = CanonicalValue.Object()
            .Add("continueOnConflict", continueOnConflict)
            .Add("items", CanonicalValue.Array(bodies))
            .Build();
        return (digests, Digest(CommandDomain, Command(storeId, application, "DispatchMany", 1, commandId, notAfterMs, body)));
    }

    /// <summary>Instant → epoch ms, rounding toward +∞ (NotBefore) or −∞ (NotAfter/expiry). Spec 02 §6.</summary>
    public static long CeilEpochMs(DateTimeOffset value) => CeilDiv(value.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks, TimeSpan.TicksPerMillisecond);

    public static long FloorEpochMs(DateTimeOffset value) => FloorDiv(value.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks, TimeSpan.TicksPerMillisecond);

    public static long CeilMs(TimeSpan value) => CeilDiv(value.Ticks, TimeSpan.TicksPerMillisecond);

    /// <summary>Configured durations must be whole positive milliseconds (spec 02 IC-14).</summary>
    public static long WholeMs(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be a positive whole number of milliseconds.");
        }

        return value.Ticks / TimeSpan.TicksPerMillisecond;
    }

    static long FloorDiv(long a, long b) => a >= 0 ? a / b : -((-a + b - 1) / b);

    static long CeilDiv(long a, long b) => -FloorDiv(-a, b);
}
