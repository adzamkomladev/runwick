namespace Runwick;

/// <summary>The continuation of a middleware pipeline. It may be invoked at most once.</summary>
public delegate Task<JobExecutionResult> JobExecutionDelegate();

/// <summary>Wraps handler execution for one attempt.</summary>
public interface IJobMiddleware
{
    Task<JobExecutionResult> InvokeAsync(
        IJobContext context,
        JobExecutionDelegate next,
        CancellationToken cancellationToken);
}

public enum JobExecutionResultKind
{
    Succeeded,
    Retry,
    Failed,
    Cancelled,
    Completed,
}

/// <summary>
/// The closed outcome of a pipeline. Factories never validate content; the engine validates when it
/// consumes the result so a malformed result cannot masquerade as a retryable exception.
/// </summary>
public sealed class JobExecutionResult
{
    static readonly JobExecutionResult SucceededInstance = new(JobExecutionResultKind.Succeeded);

    JobExecutionResult(JobExecutionResultKind kind) => Kind = kind;

    public JobExecutionResultKind Kind { get; }

    /// <summary>Retry: reason; Failed: code; Cancelled: reason.</summary>
    public string? ReasonCode { get; private init; }

    /// <summary>Failed only.</summary>
    public string? Message { get; private init; }

    /// <summary>Retry only; null means "use the policy delay".</summary>
    public TimeSpan? RetryDelay { get; private init; }

    /// <summary>Completed only (transactional completion, M3).</summary>
    public StagedCompletion? Completion { get; private init; }

    public static JobExecutionResult Succeeded() => SucceededInstance;

    public static JobExecutionResult Retry(string reason, TimeSpan? delay = null)
    {
        ArgumentNullException.ThrowIfNull(reason);
        return new(JobExecutionResultKind.Retry) { ReasonCode = reason, RetryDelay = delay };
    }

    public static JobExecutionResult Failed(string code, string message)
    {
        ArgumentNullException.ThrowIfNull(code);
        ArgumentNullException.ThrowIfNull(message);
        return new(JobExecutionResultKind.Failed) { ReasonCode = code, Message = message };
    }

    public static JobExecutionResult Cancelled(string reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        return new(JobExecutionResultKind.Cancelled) { ReasonCode = reason };
    }

    public static JobExecutionResult Completed(StagedCompletion completion)
    {
        ArgumentNullException.ThrowIfNull(completion);
        return new(JobExecutionResultKind.Completed) { Completion = completion };
    }

    public override string ToString() => ReasonCode is null ? Kind.ToString() : $"{Kind}({ReasonCode})";
}

/// <summary>
/// Token proving a Finish was staged inside a caller's transaction (transactional completion, M3).
/// </summary>
public sealed class StagedCompletion
{
    internal StagedCompletion(JobId jobId, AttemptId attemptId, Guid commandId)
    {
        JobId = jobId;
        AttemptId = attemptId;
        CommandId = commandId;
    }

    public JobId JobId { get; }

    public AttemptId AttemptId { get; }

    public Guid CommandId { get; }
}
