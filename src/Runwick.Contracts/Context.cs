namespace Runwick;

/// <summary>
/// Read-only facts about the current attempt. Resolvable only inside the attempt's DI scope;
/// inject it into a handler or middleware constructor when needed.
/// </summary>
public interface IJobContext
{
    string Application { get; }

    JobId JobId { get; }

    AttemptId AttemptId { get; }

    /// <summary>1 for the dispatched generation.</summary>
    int Generation { get; }

    /// <summary>Committed Begin number within the generation, from 1.</summary>
    int ExecutionNumber { get; }

    int MaxExecutions { get; }

    string JobName { get; }

    string Queue { get; }

    string Priority { get; }

    /// <summary>Payload version of the stored bytes.</summary>
    int OriginalPayloadVersion { get; }

    /// <summary>Registered current version handed to the handler.</summary>
    int CurrentPayloadVersion { get; }

    string SerializerProfileId { get; }

    int SerializerProfileVersion { get; }

    /// <summary>Storage decision instant of job creation.</summary>
    DateTimeOffset CreatedAt { get; }

    /// <summary>Business metadata supplied at dispatch.</summary>
    IReadOnlyDictionary<string, string> Metadata { get; }

    JobTraceContext Trace { get; }

    /// <summary>One per fencing-enabled permit held. Protects a resource only if it rejects lower values.</summary>
    IReadOnlyList<ResourceFence> Fences { get; }

    /// <summary>The schedule occurrence a scheduled job materializes (13); null for every other job.</summary>
    JobScheduleInfo? Schedule { get; }

    /// <summary>Each locally observed cancellation cause, independently.</summary>
    JobCancellationCause CancellationCauses { get; }

    /// <summary>Per-attempt scratch space; never persisted.</summary>
    IDictionary<object, object?> Items { get; }

    /// <summary>The upgraded payload passed to the handler. <typeparamref name="TJob"/> must be the registered current type.</summary>
    TJob GetPayload<TJob>() where TJob : class, IQueueJob;
}

public readonly record struct ResourceFence(string ScopeKind, string ScopeKey, long Value)
{
    public override string ToString() => $"{ScopeKind}:{Value}";
}

public readonly record struct JobTraceContext(string? TraceParent, string? TraceState);

[Flags]
public enum JobCancellationCause
{
    None = 0,
    UserRequested = 1,
    Timeout = 2,
    Shutdown = 4,
    LeaseLost = 8,
}

public sealed record JobScheduleInfo(
    string ScheduleName,
    DateTimeOffset ScheduledAt,
    DateTimeOffset CoveredFrom,
    DateTimeOffset CoveredUntil);

/// <summary>Thrown by a handler to fail the job permanently without consuming further retries.</summary>
public class PermanentJobFailureException(string code, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public string Code { get; } = code;
}
