using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Worker;

/// <summary>Local attempt phases (05 §5.2); separate from durable job states.</summary>
internal enum AttemptPhase
{
    PendingClaim,
    Reserved,
    Beginning,
    Running,
    CleaningUp,
    Finishing,
    Releasing,
    OwnershipLost,
    Retired,
}

/// <summary>Why an attempt latched <see cref="AttemptPhase.OwnershipLost"/> (05 §5.2).</summary>
internal enum LossCause
{
    StaleLease,
    InvalidState,
    SafetyDeadline,
    GraceExpired,
    CleanupExpired,
    BeginUnresolved,
    CompletionUnresolved,
    ReleaseUnconfirmed,
    ShutdownCutoff,
    InvariantFault,
}

/// <summary>
/// One reserved attempt (05 §5.1). State changes take the short <see cref="Gate"/> lock only; no lock is held
/// across database I/O or user code. Once loss is latched the entry never leaves OwnershipLost except to Retired.
/// </summary>
internal sealed class AttemptEntry : IDisposable
{
    readonly Lock _lock = new();
    readonly CancellationTokenSource _execution = new();
    readonly TaskCompletionSource _retired = new(TaskCreationOptions.RunContinuationsAsynchronously);
    TimeSpan? _deadline;
    int _invoked;

    public AttemptEntry(PoolCoordinator pool, CapacityTicket ticket, ReservedJob reserved, CompiledJob? job, TimeSpan? deadline)
    {
        Pool = pool;
        Ticket = ticket;
        Reserved = reserved;
        Job = job;
        _deadline = deadline;
        NeedsFreshRenew = deadline is null;
        ExecutionToken = _execution.Token;
    }

    public Lock Gate => _lock;

    public PoolCoordinator Pool { get; }

    /// <summary>When the Reserve that created this entry was sent: the start of the full ticket-holding duration.</summary>
    public TimeSpan AdmittedAt { get; init; }

    public CapacityTicket Ticket { get; }

    public ReservedJob Reserved { get; }

    public LeaseHandle Lease => Reserved.Lease.Handle;

    /// <summary>
    /// The Begin decided in the same command as the reservation (ReserveAndBegin/1, 05 §9.6): the lifecycle settles it
    /// instead of sending Begin. Null for a Reserve-created entry, and once consumed.
    /// </summary>
    public (BeginReply Reply, TimeSpan SentAt)? PreBegun { get; set; }

    public Guid AttemptId => Lease.AttemptId;

    /// <summary>The registration that advertised the tuple; null is a configuration invariant fault (05 §10.1).</summary>
    public CompiledJob? Job { get; }

    public PriorityGate Arbiter { get; } = new(1);

    public AttemptPhase Phase { get; private set; } = AttemptPhase.Reserved;

    public LossCause? Loss { get; private set; }

    public bool IsLost => Loss is not null;

    /// <summary>Set by a replayed Reserve/Begin: a fresh Renew must confirm authority before invocation.</summary>
    public bool NeedsFreshRenew { get; set; }

    /// <summary>Local authority deadline d (05 §11.1); null until a fresh reply establishes it.</summary>
    public TimeSpan? Deadline
    {
        get
        {
            lock (_lock)
            {
                return _deadline;
            }
        }
    }

    public JobCancellationCause Causes { get; private set; }

    public TimeSpan? FirstCancellationAt { get; private set; }

    public bool DurableCancellationObserved { get; set; }

    /// <summary>Begin's committed result (execution number) once applied.</summary>
    public BegunExecution? Begun { get; set; }

    public TimeSpan? ExecutionStartedAt { get; set; }

    /// <summary>True while user work (pipeline or its joined tasks) is alive.</summary>
    public bool PipelineLive { get; set; }

    /// <summary>True while scope disposal runs.</summary>
    public bool CleanupLive { get; set; }

    /// <summary>A Finish/Begin/Release command was submitted and is not yet resolved.</summary>
    public bool CommandUnresolved { get; set; }

    /// <summary>True between Finish submission and its resolution: no Renew may be issued (05 §6 rule 5).</summary>
    public bool SettlementInFlight { get; set; }

    /// <summary>A transactional completion was admitted for staging (08 §8.1); at most one per attempt.</summary>
    public bool StagingStarted { get; set; }

    /// <summary>The Finish staged on the caller's transaction, once applied there.</summary>
    public StagedFinish? Staged { get; set; }

    public FinishRequest? FrozenFinish { get; set; }

    /// <summary>The frozen Finish command: one ID and body for every resubmission (05 §12.2).</summary>
    public StoreCommand<FinishRequest>? FinishCommand { get; set; }

    public TaskCompletionSource<SettlementResult> Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Coalesced wake for the attempt's supervisor (causes, loss, shutdown boundaries).</summary>
    public WakeSignal Changed { get; } = new();

    public TimeSpan? FinishPublishedAt { get; set; }

    public string? Diagnostic { get; set; }

    /// <summary>The attempt's <c>process</c> span (18 §2.1); null when nothing listens.</summary>
    public System.Diagnostics.Activity? Span { get; set; }

    /// <summary>The <c>runwick.begin</c> result code (18 §2.1).</summary>
    public string? BeginResult { get; set; }

    /// <summary>Finish submissions of this attempt (<c>runwick.storage.attempts</c>).</summary>
    public int FinishAttempts;

    public int Renewals;

    /// <summary>Captured once so it stays readable after the entry is disposed.</summary>
    public CancellationToken ExecutionToken { get; }

    public Task Retired => _retired.Task;

    /// <summary>The runner's task, tracked separately from cancellation (05 §10.4).</summary>
    public Task? RunnerTask { get; set; }

    /// <summary>Moves to <paramref name="next"/> unless loss is latched or the entry retired; returns whether it moved.</summary>
    public bool TryMove(AttemptPhase next)
    {
        lock (_lock)
        {
            if (Loss is not null || Phase == AttemptPhase.Retired)
            {
                return false;
            }

            Phase = next;
            return true;
        }
    }

    /// <summary>Advances d from a fresh reply; never after a latch, never backwards.</summary>
    public void AdvanceDeadline(TimeSpan d)
    {
        lock (_lock)
        {
            if (Loss is null && (_deadline is null || d > _deadline))
            {
                _deadline = d;
            }

            NeedsFreshRenew = false;
        }
    }

    /// <summary>Latches loss once; returns true for the first latch.</summary>
    public bool Latch(LossCause cause)
    {
        lock (_lock)
        {
            if (Loss is not null || Phase == AttemptPhase.Retired)
            {
                return false;
            }

            Loss = cause;
            Phase = AttemptPhase.OwnershipLost;
            return true;
        }
    }

    /// <summary>Records one cancellation cause; returns true when it is the first User/Timeout/Shutdown cause (grace starts).</summary>
    public bool AddCause(JobCancellationCause cause, TimeSpan now)
    {
        lock (_lock)
        {
            var first = FirstCancellationAt is null && cause is JobCancellationCause.UserRequested or JobCancellationCause.Timeout or JobCancellationCause.Shutdown;
            Causes |= cause;
            if (first)
            {
                FirstCancellationAt = now;
            }

            return first;
        }
    }

    /// <summary>Requests execution cancellation off the caller's thread; callback faults are observed (05 §10.4).</summary>
    public Task CancelExecutionAsync()
    {
        if (_execution.IsCancellationRequested)
        {
            return Task.CompletedTask;
        }

        return Task.Run(async () =>
        {
            try
            {
                await _execution.CancelAsync().ConfigureAwait(false);
            }
            catch (AggregateException ex)
            {
                Diagnostic = "cancellation callback failed: " + ex.InnerException?.Message;
            }
            catch (ObjectDisposedException)
            {
                // Retired meanwhile: nothing is left to cancel.
            }
        });
    }

    /// <summary>Latches invocation once (05 §9.5 step 2).</summary>
    public bool TryLatchInvocation() => Interlocked.Exchange(ref _invoked, 1) == 0;

    public bool Invoked => Volatile.Read(ref _invoked) == 1;

    /// <summary>Retires once: releases the ticket exactly once.</summary>
    public bool Retire()
    {
        lock (_lock)
        {
            if (Phase == AttemptPhase.Retired)
            {
                return false;
            }

            Phase = AttemptPhase.Retired;
        }

        Ticket.Release();
        Pool.OnAttemptRetired(Reserved.Queue);
        _retired.TrySetResult();
        return true;
    }

    /// <summary>Called once the entry has retired and left the engine's set.</summary>
    public void Dispose() => _execution.Dispose();

    public override string ToString() => $"{Lease.JobId}/{AttemptId} {Phase}{(Loss is { } l ? $"({l})" : "")}";
}
