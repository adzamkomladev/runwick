namespace Runwick.ReferenceModel;

public sealed record ModelResult(bool Applied, string Code, JobModel Job);

/// <summary>
/// The model's transition table. A rejection never changes the job. Store-side checks like
/// "lease unexpired" use the command's decided time, which is fresh store time after locks.
/// </summary>
public static class TransitionRules
{
    static readonly HashSet<(string From, string To)> Legal =
    [
        (States.Pending, States.Reserved),
        (States.Retrying, States.Reserved),
        (States.Reserved, States.Running),
        (States.Reserved, States.Pending),      // release/recover with started_count = 0
        (States.Reserved, States.Retrying),     // release/recover with started_count > 0
        (States.Reserved, States.Failed),       // abandonment guard exhausted
        (States.Reserved, States.Cancelled),    // durable cancellation before start
        (States.Running, States.Succeeded),
        (States.Running, States.Retrying),
        (States.Running, States.Failed),
        (States.Running, States.Cancelled),
        (States.Pending, States.Cancelled),
        (States.Retrying, States.Cancelled),
    ];

    public static bool IsLegalEdge(string from, string to) => Legal.Contains((from, to));

    public static ModelResult Apply(JobModel job, ModelCommand cmd)
    {
        var now = cmd.DecidedAt;
        ModelResult Reject(string code) => new(false, code, job);
        ModelResult Ok(JobModel next) => new(true, "applied", next);
        bool OwnsLive() => job.AttemptId == cmd.AttemptId && job.Token == cmd.Token
                           && job.LeaseExpiresAt is { } e && e > now;

        switch (cmd.Op)
        {
            case ModelOp.Reserve:
                if (!States.Waiting.Contains(job.State))
                {
                    return Reject("no_work");
                }

                return Ok(job with
                {
                    State = States.Reserved,
                    AttemptId = cmd.AttemptId ?? throw new ArgumentException("attempt id"),
                    Token = cmd.Token ?? throw new ArgumentException("token"),
                    LeaseExpiresAt = now + (cmd.Lease ?? TimeSpan.FromSeconds(60)),
                    Reservations = job.Reservations + 1,
                });

            case ModelOp.Begin:
                if (!OwnsLive() || job.State is not States.Reserved)
                {
                    return Reject("stale_lease");
                }

                if (job.CancelRequested)
                {
                    return Ok(Release(job) with { State = States.Cancelled });
                }

                if (job.StartedCount >= job.MaxExecutions)
                {
                    return Reject("budget_exhausted");
                }

                return Ok(job with
                {
                    State = States.Running,
                    StartedCount = job.StartedCount + 1,
                    LifetimeExecutions = job.LifetimeExecutions + 1,
                    ConsecutiveUnstarted = 0,
                });

            case ModelOp.BeginDeferred:
                // T20 (10 §5): a committed rate deferral closes the attempt and returns the job to its waiting state.
                // Cancellation and an exhausted budget are decided before any charge, so neither can defer; nothing is
                // charged (no execution, abandonment or unstarted-guard count).
                if (!OwnsLive() || job.State is not States.Reserved)
                {
                    return Reject("stale_lease");
                }

                if (job.CancelRequested || job.StartedCount >= job.MaxExecutions)
                {
                    return Reject("invalid_state");
                }

                return Ok(Release(job) with { State = WaitingState(job) });

            case ModelOp.Renew:
                if (!OwnsLive() || !States.OwnedStates.Contains(job.State))
                {
                    return Reject("stale_lease");
                }

                return Ok(job with { LeaseExpiresAt = now + (cmd.Lease ?? TimeSpan.FromSeconds(60)) });

            case ModelOp.Finish:
                if (!OwnsLive() || job.State is not States.Running)
                {
                    return Reject("stale_lease");
                }

                return Ok(Release(job) with { State = FinishState(job, cmd.Outcome ?? ModelOutcome.Succeeded) });

            case ModelOp.Release:
                if (!OwnsLive() || job.State is not States.Reserved)
                {
                    return Reject(job.State is States.Running ? "invalid_state" : "stale_lease");
                }

                return Ok(Release(job) with { State = job.CancelRequested ? States.Cancelled : WaitingState(job) });

            case ModelOp.Recover:
                if (job.AttemptId != cmd.AttemptId || job.Token != cmd.Token
                    || job.LeaseExpiresAt is not { } expires || expires > now
                    || !States.OwnedStates.Contains(job.State))
                {
                    return Reject("stale_lease");
                }

                if (job.State is States.Reserved)
                {
                    var abandoned = job.ConsecutiveUnstarted + 1;
                    var next = Release(job) with { ConsecutiveUnstarted = abandoned };
                    if (job.CancelRequested)
                    {
                        return Ok(next with { State = States.Cancelled });
                    }

                    return Ok(next with { State = abandoned >= job.MaxUnstarted ? States.Failed : WaitingState(job) });
                }

                // Running crash: the execution was already counted at Begin.
                return Ok(Release(job) with
                {
                    State = job.CancelRequested ? States.Cancelled
                        : job.StartedCount >= job.MaxExecutions ? States.Failed
                        : States.Retrying,
                });

            case ModelOp.RequestCancel:
                if (States.Terminal.Contains(job.State))
                {
                    return Reject("invalid_state");
                }

                return States.Waiting.Contains(job.State)
                    ? Ok(job with { State = States.Cancelled })
                    : Ok(job with { CancelRequested = true });

            default:
                return Reject("unsupported");
        }
    }

    static string WaitingState(JobModel job) => job.StartedCount == 0 ? States.Pending : States.Retrying;

    static JobModel Release(JobModel job) => job with { AttemptId = null, Token = null, LeaseExpiresAt = null };

    static string FinishState(JobModel job, ModelOutcome outcome)
    {
        if (outcome is ModelOutcome.Succeeded)
        {
            return States.Succeeded;
        }

        if (job.CancelRequested || outcome is ModelOutcome.UserCancelled)
        {
            return States.Cancelled;
        }

        if (outcome is ModelOutcome.PermanentFailure)
        {
            return States.Failed;
        }

        return job.StartedCount >= job.MaxExecutions ? States.Failed : States.Retrying;
    }
}
