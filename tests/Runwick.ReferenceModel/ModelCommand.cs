namespace Runwick.ReferenceModel;

public enum ModelOp { Dispatch, Reserve, Begin, Renew, Finish, Recover, Release, RequestCancel, BeginDeferred }

public enum ModelOutcome { Succeeded, RetryableFailure, PermanentFailure, UserCancelled, Interrupted, TimedOut }

/// <summary>One committed command as observed by a test (decided time is store time).</summary>
public sealed record ModelCommand(
    ModelOp Op,
    Guid JobId,
    DateTimeOffset DecidedAt,
    Guid? AttemptId = null,
    Guid? Token = null,
    ModelOutcome? Outcome = null,
    TimeSpan? Lease = null)
{
    public override string ToString() => $"{Op}({JobId:N}@{DecidedAt:HH:mm:ss.fff}{(Outcome is { } o ? "," + o : "")})";
}
