namespace Runwick;

public enum ConfigurationSource
{
    LibraryDefault,
    ApplicationDefault,
    Attribute,
    Fluent,
}

public readonly record struct EffectiveValue<T>(T Value, ConfigurationSource Source);

public sealed record RetryDelayPolicy(string Evaluator, string Jitter, TimeSpan InitialDelay, TimeSpan MaxDelay);

/// <summary>A declared shared concurrency scope. <see cref="Key"/> is null for per-resource selectors.</summary>
public sealed record ScopeDeclaration(string Kind, string? Key, int Capacity);

public sealed record JobDescriptor(
    string Name,
    Type PayloadType,
    EffectiveValue<int> PayloadVersion,
    IReadOnlyList<int> DecodablePayloadVersions,
    EffectiveValue<string> Queue,
    EffectiveValue<string> Priority,
    EffectiveValue<int> MaxExecutions,
    EffectiveValue<TimeSpan?> ExecutionTimeout,
    EffectiveValue<RetryDelayPolicy> RetryDelay,
    EffectiveValue<int> MaxUnstartedAbandonments,
    EffectiveValue<RetryDelayPolicy> UnstartedRecoveryDelay,
    IReadOnlyList<ScopeDeclaration> Scopes,
    IReadOnlyList<string> Middleware,
    Type? HandlerType);

/// <summary>The validated, effective registration with a per-value source map.</summary>
public interface IRunwickJobRegistry
{
    string Application { get; }

    IReadOnlyList<JobDescriptor> Jobs { get; }

    IReadOnlyList<string> Queues { get; }

    JobDescriptor? Find(string jobName);

    JobDescriptor Get<TJob>() where TJob : class, IQueueJob;
}

public sealed class ProducerOptions
{
    /// <summary>Offset of the frozen NotAfter assigned at Prepare. Default and maximum 5 minutes.</summary>
    public TimeSpan CommandLifetime { get; set; } = TimeSpan.FromMinutes(5);

    public int MaxWaitingOperations { get; set; } = 64;

    public TimeSpan AdmissionWait { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Producer/admin mutations in flight at once; the PostgreSQL store's producer lane and work pool follow it (06 §1.7).
    /// Default 4; at least 1.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = 4;
}
