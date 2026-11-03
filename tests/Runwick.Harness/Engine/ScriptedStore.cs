using System.Collections.Concurrent;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Harness.Engine;

/// <summary>One call into the scripted store, as the script sees it.</summary>
internal sealed record StoreCall(string Operation, Guid CommandId, object Body, int Submission, CancellationToken Token)
{
    public Guid? JobId => Body switch
    {
        BeginRequest b => b.Lease.JobId,
        RenewRequest r => r.Lease.JobId,
        FinishRequest f => f.Lease.JobId,
        ReleaseRequest r => r.Lease.JobId,
        RecoverRequest r => r.JobId,
        _ => null,
    };
}

/// <summary>What the script does with a call before the store applies it.</summary>
internal enum CallEffect
{
    Proceed,

    /// <summary>Not applied; the client sees a definite transport failure.</summary>
    FailNotApplied,

    /// <summary>Not applied; the call never returns until its token is cancelled.</summary>
    Hang,
}

/// <summary>
/// An in-memory store implementing the internal store interfaces for engine tests (07 §2.8). It keeps receipts by
/// command ID (so resubmissions replay), leases, executions and finishes. The script decides each call's fate
/// before (<see cref="Before"/>) and after (<see cref="DropReply"/>) the effect.
/// </summary>
internal class ScriptedStore(TimeProvider time) : IRunwickStore, IWakeNotificationSource
{
    readonly Lock _lock = new();
    readonly Dictionary<Guid, object> _receipts = [];
    readonly Dictionary<Guid, int> _submissions = [];
    readonly List<Job> _jobs = [];

    internal sealed class Job(Guid id, DispatchIntent intent)
    {
        public Guid Id { get; } = id;
        public DispatchIntent Intent { get; } = intent;
        public string State { get; set; } = JobStates.Pending;
        public int Executions { get; set; }
        public long Reservations { get; set; }
        public LeaseHandle? Lease { get; set; }
        public DateTimeOffset ExpiresAt { get; set; }
        public DateTimeOffset CreatedAt { get; init; }
        public bool CancellationRequested { get; set; }
        public List<FinishRequest> Finishes { get; } = [];
        public List<string> History { get; } = [];
    }

    public Guid StoreId { get; } = Guid.NewGuid();

    public string AcknowledgementProfile => "scripted";

    /// <summary>Every call, in arrival order.</summary>
    public ConcurrentQueue<StoreCall> Calls { get; } = new();

    /// <summary>The queue of every reservation, in store admission order (Reserve and ReserveAndBegin alike).</summary>
    public ConcurrentQueue<string> Admissions { get; } = new();

    /// <summary>Decides each call before it is applied; null proceeds.</summary>
    public Func<StoreCall, Task<CallEffect>>? Before { get; set; }

    /// <summary>Replaces a reply after the effect (for example a <c>Replayed</c> flag the client must ignore).</summary>
    public Func<StoreCall, object, object>? Rewrite { get; set; }

    /// <summary>After the effect: true drops the reply (outcome unknown to the client).</summary>
    public Func<StoreCall, bool>? DropReply { get; set; }

    public IReadOnlyList<Job> Jobs
    {
        get
        {
            lock (_lock)
            {
                return [.. _jobs];
            }
        }
    }

    public Job JobOf(Guid id)
    {
        lock (_lock)
        {
            return _jobs.Single(j => j.Id == id);
        }
    }

    public int Count(string operation) => Calls.Count(c => c.Operation == operation);

    public Guid Add(DispatchIntent intent)
    {
        lock (_lock)
        {
            var job = new Job(Guid.CreateVersion7(), intent) { CreatedAt = time.GetUtcNow() };
            _jobs.Add(job);
            return job.Id;
        }
    }

    public void RequestCancellation(Guid jobId)
    {
        lock (_lock)
        {
            _jobs.Single(j => j.Id == jobId).CancellationRequested = true;
        }
    }

    /// <summary>Takes a lease away as if it expired and another owner recovered it.</summary>
    public void Steal(Guid jobId)
    {
        lock (_lock)
        {
            var job = _jobs.Single(j => j.Id == jobId);
            job.Lease = job.Lease! with { OwnershipToken = Guid.NewGuid(), AttemptId = Guid.NewGuid() };
            job.History.Add("stolen");
        }
    }

    private protected async Task<T> Run<T>(string operation, Guid commandId, object body, Func<DateTimeOffset, T> apply, CancellationToken ct) where T : class
    {
        int submission;
        lock (_lock)
        {
            submission = _submissions[commandId] = _submissions.GetValueOrDefault(commandId) + 1;
        }

        var call = new StoreCall(operation, commandId, body, submission, ct);
        Calls.Enqueue(call);
        await Task.Yield();
        switch (Before is null ? CallEffect.Proceed : await Before(call).ConfigureAwait(false))
        {
            case CallEffect.FailNotApplied:
                throw new StoreNotAppliedException("scripted_failure");
            case CallEffect.Hang:
                await Task.Delay(Timeout.InfiniteTimeSpan, time, ct).ConfigureAwait(false);
                break;
        }

        ct.ThrowIfCancellationRequested();
        T reply;
        lock (_lock)
        {
            if (_receipts.TryGetValue(commandId, out var stored))
            {
                reply = Replay((T)stored);
            }
            else
            {
                reply = apply(time.GetUtcNow());
                if (Receipted(operation) && IsApplied(reply))
                {
                    _receipts[commandId] = reply;
                }
            }
        }

        if (Rewrite is not null)
        {
            reply = (T)Rewrite(call, reply);
        }

        if (DropReply?.Invoke(call) == true)
        {
            throw new StoreOutcomeUnknownException("scripted_drop", commandId, operation);
        }

        return reply;
    }

    static bool Receipted(string operation) => operation != Operations.Renew;

    static bool IsApplied(object reply) => reply switch
    {
        BeginReply b => b.Reply.Effect == StoreEffect.Applied,
        StoreReply<ReservedJob> r => r.Effect == StoreEffect.Applied,
        StoreReply<FinishedAttempt> r => r.Effect == StoreEffect.Applied,
        StoreReply<ReleasedReservation> r => r.Effect == StoreEffect.Applied,
        StoreReply<ClaimedJobs> r => r.Effect == StoreEffect.Applied,
        _ => false,
    };

    static T Replay<T>(T stored) => stored switch
    {
        BeginReply b => (T)(object)(b with { Reply = b.Reply with { Replayed = true } }),
        StoreReply<ReservedJob> r => (T)(object)(r with { Replayed = true }),
        StoreReply<FinishedAttempt> r => (T)(object)(r with { Replayed = true }),
        StoreReply<ReleasedReservation> r => (T)(object)(r with { Replayed = true }),
        StoreReply<ClaimedJobs> r => (T)(object)(r with { Replayed = true }),
        _ => stored,
    };

    static StoreReply<T> Fail<T>(StoreStatus status, string code, DateTimeOffset now) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code), now);

    Job? Owned(LeaseHandle lease) => _jobs.SingleOrDefault(j => j.Id == lease.JobId && j.Lease == lease);

    JobPosition Position(Job j) => new(j.Id, 1, j.Reservations, j.State);

    // ---- handshake

    public Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct) =>
        Task.FromResult(new StoreDescriptor(StoreId, 1, 1, "scripted", 1, Array.Empty<byte>(), null, time.GetUtcNow()));

    public Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct) =>
        Task.FromResult(StoreReply<ApplicationSeedResult>.Applied(
            new ApplicationSeedResult(true, new ApplicationGate(1, 1, new Dictionary<string, int>(), "open")), time.GetUtcNow()));

    public Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct) =>
        Task.FromResult(StoreReply<QueueSeedResult>.Applied(
            new QueueSeedResult([.. command.Body.Queues.Select(q => new QueueSeed(q, true, false, 1))]), time.GetUtcNow()));

    public Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct) =>
        Task.FromResult(StoreReply<ScopeSeedResult>.Applied(new ScopeSeedResult([.. command.Body.Scopes.Select(s =>
            new ScopeSeed(Guid.NewGuid(), s.Kind, s.Key, true, s.Capacity, s.Capacity, 1, false))]), time.GetUtcNow()));

    // ---- dispatch (tests add jobs with Add)

    public Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        var id = Add(command.Body.Item.Intent);
        return Task.FromResult(StoreReply<DispatchResult>.Applied(new DispatchResult(id, DispatchDisposition.Created, DuplicateMatch.None, false), time.GetUtcNow()));
    }

    public Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) => throw new NotSupportedException();

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        throw new NotSupportedException();

    // ---- reservation

    public Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct) =>
        Run(Operations.Reserve, command.CommandId, command.Body, now =>
        {
            var r = command.Body;
            return ReserveNext(r.WorkerInstanceId, r.Queue, r.Priority, r.SupportedContracts, r.LeaseDuration, now) is { } reserved
                ? StoreReply<ReservedJob>.Applied(reserved, now)
                : Fail<ReservedJob>(StoreStatus.NoWork, "no_work", now);
        }, ct);

    /// <summary>Reserves the lane's next claimable job (caller holds the lock); null when the lane is empty.</summary>
    private protected ReservedJob? ReserveNext(Guid worker, string queue, string priority, IReadOnlyList<JobContract> contracts, TimeSpan leaseDuration,
        DateTimeOffset now)
    {
        var job = _jobs.FirstOrDefault(j => j.State is JobStates.Pending or JobStates.Retrying && j.Intent.Queue == queue
            && j.Intent.Priority == priority && contracts.Any(c => c.Name == j.Intent.JobName && c.PayloadVersion == j.Intent.PayloadVersion));
        if (job is null)
        {
            return null;
        }

        job.State = JobStates.Reserved;
        job.Reservations++;
        job.Lease = new LeaseHandle(job.Id, Guid.CreateVersion7(), worker, Guid.NewGuid(), 1);
        job.ExpiresAt = now + leaseDuration;
        job.History.Add("reserved");
        Admissions.Enqueue(queue);
        var i = job.Intent;
        return new ReservedJob(Position(job), new LeaseGrant(job.Lease, job.ExpiresAt),
            new JobContract(i.JobName, i.PayloadVersion, i.SerializerProfileId, i.SerializerProfileVersion), new CandidateCursor(job.CreatedAt, job.Id),
            job.Reservations, i.PayloadUtf8, i.BusinessMetadata, i.DiagnosticMetadata, i.Queue, i.Priority, i.Policy, [], job.CreatedAt);
    }

    public Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct) =>
        Run(Operations.Begin, command.CommandId, command.Body, now => BeginOwned(command.Body.Lease, now), ct);

    /// <summary>Begins the reserved attempt <paramref name="lease"/> owns (caller holds the lock).</summary>
    private protected BeginReply BeginOwned(LeaseHandle lease, DateTimeOffset now)
    {
        var job = Owned(lease);
        if (job is null || job.ExpiresAt <= now)
        {
            return new BeginReply(Fail<BegunExecution>(StoreStatus.StaleLease, "stale_lease", now), null);
        }

        if (job.State != JobStates.Reserved)
        {
            return new BeginReply(Fail<BegunExecution>(StoreStatus.InvalidState, "not_reserved", now), null);
        }

        job.State = JobStates.Running;
        job.Executions++;
        job.History.Add("begun");
        return new BeginReply(StoreReply<BegunExecution>.Applied(new BegunExecution(Position(job), new LeaseGrant(job.Lease!, job.ExpiresAt),
            job.Executions, job.Executions, now), now), null);
    }

    public Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct) =>
        Run(Operations.Renew, command.CommandId, command.Body, now =>
        {
            var job = Owned(command.Body.Lease);
            if (job is null || job.ExpiresAt <= now)
            {
                return Fail<RenewedLease>(StoreStatus.StaleLease, "stale_lease", now);
            }

            job.ExpiresAt = now + command.Body.LeaseDuration;
            job.History.Add("renewed");
            return StoreReply<RenewedLease>.Applied(new RenewedLease(new LeaseGrant(job.Lease!, job.ExpiresAt), job.CancellationRequested), now);
        }, ct);

    // ---- state

    public Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct) =>
        Run(Operations.Finish, command.CommandId, command.Body, now =>
        {
            var job = Owned(command.Body.Lease);
            if (job is null || job.ExpiresAt <= now)
            {
                return Fail<FinishedAttempt>(StoreStatus.StaleLease, "stale_lease", now);
            }

            if (job.State != JobStates.Running)
            {
                return Fail<FinishedAttempt>(StoreStatus.InvalidState, "not_running", now);
            }

            job.Finishes.Add(command.Body);
            job.State = command.Body.Outcome == AttemptOutcome.Succeeded ? JobStates.Succeeded : JobStates.Failed;
            job.Lease = null;
            job.History.Add("finished:" + command.Body.Outcome);
            return StoreReply<FinishedAttempt>.Applied(new FinishedAttempt(Position(job), command.Body.Outcome.ToString(), job.State, null, null), now);
        }, ct);

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct) =>
        Run(Operations.Recover, command.CommandId, command.Body, now =>
        {
            var job = _jobs.SingleOrDefault(j => j.Id == command.Body.JobId && j.Lease?.AttemptId == command.Body.ExpectedAttemptId);
            if (job is null || job.ExpiresAt > now)
            {
                return Fail<FinishedAttempt>(StoreStatus.StaleLease, "stale_lease", now);
            }

            var outcome = job.State == JobStates.Running ? "abandoned_running" : "abandoned_before_start";
            job.State = JobStates.Retrying;
            job.Lease = null;
            job.History.Add("recovered:" + outcome);
            return StoreReply<FinishedAttempt>.Applied(new FinishedAttempt(Position(job), outcome, JobStates.Retrying, null, now), now);
        }, ct);

    public Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct) =>
        Run(Operations.ReleaseReservation, command.CommandId, command.Body, now =>
        {
            var job = Owned(command.Body.Lease);
            if (job is null || job.ExpiresAt <= now)
            {
                return Fail<ReleasedReservation>(StoreStatus.StaleLease, "stale_lease", now);
            }

            if (job.State != JobStates.Reserved)
            {
                return Fail<ReleasedReservation>(StoreStatus.InvalidState, "not_reserved", now);
            }

            job.State = JobStates.Pending;
            job.Lease = null;
            job.History.Add("released");
            return StoreReply<ReleasedReservation>.Applied(new ReleasedReservation(Position(job), "released", now), now);
        }, ct);

    // ---- wake notifications (05 §7.2)

    /// <summary>
    /// The scripted listener. Null (the default): <see cref="ListenAsync"/> returns at once, never subscribing, exactly
    /// like a store without notifications. Set it before start to script subscriptions, losses and notifications.
    /// </summary>
    public ScriptedListener? Listener { get; set; }

    public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct) =>
        Listener is { } listener && channel == WakeChannels.Work ? listener.RunAsync(onWake, onSubscribed, onLost, ct) : Task.CompletedTask;


    public Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct)
    {
        lock (_lock)
        {
            var now = time.GetUtcNow();
            var ready = lanes.Select(l => (l, _jobs.Where(j => j.State is JobStates.Pending or JobStates.Retrying && j.Intent.Queue == l.Queue && j.Intent.Priority == l.Priority)
                    .Select(j => (DateTimeOffset?)j.CreatedAt).Min()))
                .Where(x => x.Item2 is not null).Select(x => new ReadyLane(x.l, x.Item2!.Value)).ToList();
            return Task.FromResult(new ReadySnapshot(now, ready));
        }
    }

    public Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct) =>
        Task.FromResult(new PresenceAck(time.GetUtcNow() + presence.Expiry, true));

    public Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct)
    {
        lock (_lock)
        {
            var now = time.GetUtcNow();
            IReadOnlyList<ExpiredLease> expired = [.. _jobs.Where(j => j.Lease is not null && j.ExpiresAt <= now && j.State is JobStates.Reserved or JobStates.Running)
                .Take(limit).Select(j => new ExpiredLease(j.Id, j.Lease!.AttemptId, j.Lease.OwnershipToken, j.State, j.ExpiresAt))];
            return Task.FromResult(expired);
        }
    }
}

/// <summary>
/// The scripted store with the batched claim (ReserveAndBegin/1, 05 §9.6): up to MaxJobs of the lane's next jobs are
/// reserved and begun in one call, each exactly as Reserve then Begin would leave it. An empty lane is NoWork.
/// </summary>
internal sealed class ScriptedBatchStore(TimeProvider time) : ScriptedStore(time), IReserveAndBeginStore
{
    public Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(StoreCommand<ReserveAndBeginRequest> command, CancellationToken ct) =>
        Run(Operations.ReserveAndBegin, command.CommandId, command.Body, now =>
        {
            var r = command.Body;
            var outcomes = new List<ClaimOutcome>();
            while (outcomes.Count < r.MaxJobs && ReserveNext(r.WorkerInstanceId, r.Queue, r.Priority, r.SupportedContracts, r.LeaseDuration, now) is { } reserved)
            {
                var begun = BeginOwned(reserved.Lease.Handle, now).Reply.Value!;
                outcomes.Add(new ClaimOutcome(reserved.Position.JobId, StoreStatus.Applied, null, reserved, begun, null, null));
            }

            return outcomes.Count > 0
                ? StoreReply<ClaimedJobs>.Applied(new ClaimedJobs(outcomes), now)
                : StoreReply<ClaimedJobs>.NoEffect(StoreStatus.NoWork, new StoreError("no_work"), now);
        }, ct);
}

/// <summary>A scripted wake listener session: the test decides when it subscribes, loses its session and notifies.</summary>
internal sealed class ScriptedListener
{
    Action<string>? _onWake;
    Action? _onSubscribed;
    Action? _onLost;
    readonly TaskCompletionSource _running = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes once the engine's listener task has started.</summary>
    public Task Running => _running.Task;

    internal async Task RunAsync(Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct)
    {
        _onWake = onWake;
        _onSubscribed = onSubscribed;
        _onLost = onLost;
        _running.TrySetResult();
        await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
    }

    public void Subscribe() => (_onSubscribed ?? throw new InvalidOperationException("listener not running"))();

    public void Lose() => (_onLost ?? throw new InvalidOperationException("listener not running"))();

    public void Notify(string queue) => (_onWake ?? throw new InvalidOperationException("listener not running"))(queue);
}
