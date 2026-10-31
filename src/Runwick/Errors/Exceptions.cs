namespace Runwick;

public enum ErrorDisposition
{
    /// <summary>Nothing was written by this call (includes known-not-submitted).</summary>
    KnownNotApplied,

    /// <summary>The command may or may not have applied; resolve by operation ID.</summary>
    OutcomeUnknown,
}

public interface IRunwickError
{
    string Code { get; }

    ErrorDisposition Disposition { get; }
}

public abstract class RunwickException : Exception, IRunwickError
{
    protected RunwickException(string code, ErrorDisposition disposition, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
        Disposition = disposition;
    }

    public string Code { get; }

    public ErrorDisposition Disposition { get; }
}

public sealed record ConfigurationError(string Code, string Subject, string Message)
{
    public override string ToString() => $"[{Code}] {Subject}: {Message}";
}

public sealed class RunwickConfigurationException : RunwickException
{
    public RunwickConfigurationException(IReadOnlyList<ConfigurationError> errors)
        : base(RunwickErrorCodes.ConfigurationInvalid, ErrorDisposition.KnownNotApplied,
            "Runwick configuration is invalid:" + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(e => "  " + e)))
    {
        Errors = errors;
    }

    public IReadOnlyList<ConfigurationError> Errors { get; }
}

public sealed class DispatchValidationException(string code, string message, IReadOnlyList<BatchItemFailure>? itemFailures = null)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message)
{
    public IReadOnlyList<BatchItemFailure> ItemFailures { get; } = itemFailures ?? [];
}

public sealed class UniquenessConflictException(string code, string message, JobId? conflictingJobId, IReadOnlyList<BatchItemFailure>? itemFailures = null)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message)
{
    public JobId? ConflictingJobId { get; } = conflictingJobId;

    public IReadOnlyList<BatchItemFailure> ItemFailures { get; } = itemFailures ?? [];
}

public sealed class CommandConflictException(DispatchOperationId operationId)
    : RunwickException(RunwickErrorCodes.CommandConflict, ErrorDisposition.KnownNotApplied,
        $"Operation {operationId} was already recorded with different arguments.")
{
    public DispatchOperationId OperationId { get; } = operationId;
}

public sealed class CommandExpiredException(DispatchOperationId operationId, PreparedDispatch? prepared = null, PreparedBatch? preparedBatch = null)
    : RunwickException(RunwickErrorCodes.CommandExpired, ErrorDisposition.KnownNotApplied,
        $"Operation {operationId} passed its NotAfter deadline without applying; it can never apply.")
{
    public DispatchOperationId OperationId { get; } = operationId;

    public PreparedDispatch? Prepared { get; } = prepared;

    public PreparedBatch? PreparedBatch { get; } = preparedBatch;
}

public sealed class LocalCapacityExceededException(PreparedDispatch? prepared = null, PreparedBatch? preparedBatch = null)
    : RunwickException(RunwickErrorCodes.LocalCapacityExceeded, ErrorDisposition.KnownNotApplied,
        "Local producer admission is full; the command was not submitted.")
{
    public PreparedDispatch? Prepared { get; } = prepared;

    public PreparedBatch? PreparedBatch { get; } = preparedBatch;
}

public sealed class StoreUnavailableException(string message, Exception? innerException = null)
    : RunwickException(RunwickErrorCodes.StoreUnavailable, ErrorDisposition.KnownNotApplied, message, innerException);

public sealed class StoreIncompatibleException(string message, Exception? innerException = null)
    : RunwickException(RunwickErrorCodes.StoreIncompatible, ErrorDisposition.KnownNotApplied, message, innerException);

public sealed class AdmissionClosedException(string message)
    : RunwickException(RunwickErrorCodes.AdmissionClosed, ErrorDisposition.KnownNotApplied, message);

public sealed class DispatchOutcomeUnknownException(
    string code, DispatchOperationId operationId, PreparedDispatch? prepared, PreparedBatch? preparedBatch, Exception? innerException = null)
    : RunwickException(code, ErrorDisposition.OutcomeUnknown,
        $"The outcome of operation {operationId} is unknown; resolve it with ResolveAsync or re-send the same prepared request.", innerException)
{
    public DispatchOperationId OperationId { get; } = operationId;

    public PreparedDispatch? Prepared { get; } = prepared;

    public PreparedBatch? PreparedBatch { get; } = preparedBatch;
}

public sealed class DispatchCanceledAfterSendException(
    DispatchOperationId operationId, PreparedDispatch? prepared, PreparedBatch? preparedBatch, CancellationToken token)
    : OperationCanceledException($"Cancelled after operation {operationId} was sent; its outcome is unknown.", token), IRunwickError
{
    public string Code => RunwickErrorCodes.CancelledAfterSend;

    public ErrorDisposition Disposition => ErrorDisposition.OutcomeUnknown;

    public DispatchOperationId OperationId { get; } = operationId;

    public PreparedDispatch? Prepared { get; } = prepared;

    public PreparedBatch? PreparedBatch { get; } = preparedBatch;
}

public sealed class TransactionValidationException(string code, string message)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);

public sealed class JobOwnershipLostException(string message)
    : RunwickException(RunwickErrorCodes.OwnershipLost, ErrorDisposition.KnownNotApplied, message);

/// <summary>Transactional completion misuse (01 §16): <c>no_active_attempt</c> or <c>completion_already_staged</c>.</summary>
public sealed class JobCompletionException(string code, string message)
    : RunwickException(code, ErrorDisposition.KnownNotApplied, message);
