using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Scheduling;

/// <summary>
/// The scheduler role (13 §4.1): seeds declared schedules at start, then polls due schedules, plans each under the
/// evaluator slot and sends one MaterializeOccurrences command at a time. It holds no lease; the store's locked
/// revisions pick one winner among replicas. All I/O is on the background lane.
/// </summary>
internal sealed class SchedulerService : IRunwickComponent, IDisposable
{
    static readonly TimeSpan RequestBudget = TimeSpan.FromSeconds(5);
    static readonly TimeSpan ResolutionBudget = TimeSpan.FromSeconds(30);

    readonly ScheduleDeclarations _declarations;
    readonly RunwickRuntime _runtime;
    readonly DispatchPreparer _preparer;
    readonly IScheduleStore? _store;
    readonly TimeProvider _time;
    readonly ILogger _logger;
    readonly EvaluatorSlot _slot = new();
    readonly TzdbArtifact _artifact;
    readonly CancellationTokenSource _stop = new();
    TaskCompletionSource _wake = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly Dictionary<Guid, string> _localDiagnostics = [];
    Task? _loop;
    double _tokens;
    long _refilledAt;

    public SchedulerService(ScheduleDeclarations declarations, IServiceProvider services)
    {
        _declarations = declarations;
        _runtime = services.GetRequiredService<RunwickRuntime>();
        _preparer = services.GetRequiredService<DispatchPreparer>();
        _store = services.GetService<IScheduleStore>();
        _time = services.GetRequiredService<TimeProvider>();
        _logger = services.GetService<ILogger<SchedulerService>>() ?? (ILogger)NullLogger.Instance;
        _artifact = TzdbArtifact.Pinned;
        _tokens = declarations.Options.Burst;
        _refilledAt = _time.GetTimestamp();
        Health = new RoleTracker(_time);
    }

    /// <summary>Scheduler readiness (18 §7): each pass's due read and cursor writes.</summary>
    internal RoleTracker Health { get; }

    internal TimeSpan PollInterval => _declarations.Options.PollInterval;

    internal TimeSpan MaxIdle => _declarations.Options.IdlePollInterval * ListeningIdleFactor;

    /// <summary>Local diagnostics by schedule ID (SCHED-11): profiles this process cannot evaluate. Never written to the store.</summary>
    internal IReadOnlyDictionary<Guid, string> LocalDiagnostics
    {
        get
        {
            lock (_localDiagnostics)
            {
                return new Dictionary<Guid, string>(_localDiagnostics);
            }
        }
    }

    /// <summary>Completed materializations (tests and diagnostics).</summary>
    internal long Materializations;

    /// <summary>Schedule-channel notifications received (13 Changes 2026-11-02, PF-07).</summary>
    internal long Notifications;

    /// <summary>Due reads sent by the loop (tests and diagnostics).</summary>
    internal long Passes;

    /// <summary>Listener sessions lost or refused after a subscription (tests and diagnostics).</summary>
    internal long ListenerLosses;

    /// <summary>
    /// While the schedule listener is subscribed, the idle back-off may grow to this multiple of
    /// <see cref="SchedulerOptions.IdlePollInterval"/> (5 s to 15 s by default; 13 Changes 2026-11-02, idle caps).
    /// </summary>
    internal const int ListeningIdleFactor = 3;

    volatile bool _listenerHealthy;

    /// <summary>True from a successful (re)subscription until the session is lost, refused or the listener stops.</summary>
    internal bool ListenerHealthy => _listenerHealthy;

    /// <summary>The idle back-off cap now: 3 × IdlePollInterval while the listener is healthy, IdlePollInterval otherwise.</summary>
    internal TimeSpan IdleCap => _listenerHealthy ? _declarations.Options.IdlePollInterval * ListeningIdleFactor : _declarations.Options.IdlePollInterval;

    // Local instant at which each schedule this process last materialized becomes due again (its new scan_after_at,
    // carried over from store time through the plan's cutoff). An idle wait never runs past the earliest of them.
    readonly Dictionary<Guid, DateTimeOffset> _nextScan = [];

    string Application => _runtime.Registry.Application;

    public async Task StartAsync(CancellationToken ct)
    {
        var store = _store ?? throw new StoreIncompatibleException("The configured store does not support schedules (13).");
        await _runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        foreach (var declaration in _declarations.Schedules)
        {
            await SeedAsync(store, declaration, ct).ConfigureAwait(false);
        }

        _loop = Task.Run(() => LoopAsync(store, _stop.Token), CancellationToken.None);
        if (store is IWakeNotificationSource notifications)
        {
            // Optional (PF-07): a schedule edit ends the idle back-off early; polling stays the correctness path.
            _listener = Task.Run(() => ListenAsync(notifications, _stop.Token), CancellationToken.None);
        }
    }

    Task? _listener;

    async Task ListenAsync(IWakeNotificationSource notifications, CancellationToken ct)
    {
        try
        {
            await notifications.ListenAsync(Application, WakeChannels.Schedule, _ =>
            {
                Interlocked.Increment(ref Notifications);
                Signal();
            }, OnSubscribed, OnListenerLost, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Schedule notification listener stopped; the scheduler keeps polling.");
        }
        finally
        {
            OnListenerLost();
        }
    }

    /// <summary>(Re)subscribed: edits made while unsubscribed were not notified, so pass once now.</summary>
    void OnSubscribed()
    {
        _listenerHealthy = true;
        Signal();
    }

    /// <summary>
    /// Lost, refused or stopped: edits are no longer notified, so the 5 s cap applies again. The current idle wait ends
    /// now (one pass, then the back-off from PollInterval), so no wait begun under the longer cap outlives the session.
    /// </summary>
    void OnListenerLost()
    {
        if (!_listenerHealthy)
        {
            return;
        }

        _listenerHealthy = false;
        Interlocked.Increment(ref ListenerLosses);
        Signal();
    }

    /// <summary>Time until the earliest known next due instant, or null when none is known (passed ones are dropped).</summary>
    internal TimeSpan? UntilKnownDue()
    {
        lock (_nextScan)
        {
            var now = _time.GetUtcNow();
            DateTimeOffset? earliest = null;
            foreach (var (id, at) in _nextScan.ToList())
            {
                if (at <= now)
                {
                    _nextScan.Remove(id);
                }
                else if (earliest is null || at < earliest)
                {
                    earliest = at;
                }
            }

            return earliest - now;
        }
    }

    /// <summary>Records a materialized schedule's next scan instant, mapped from store time to local time through the cutoff.</summary>
    internal void NoteNextScan(Guid scheduleId, DateTimeOffset scanAfterAt, DateTimeOffset cutoff)
    {
        lock (_nextScan)
        {
            _nextScan[scheduleId] = _time.GetUtcNow() + (scanAfterAt - cutoff);
        }
    }

    void Signal() => Volatile.Read(ref _wake).TrySetResult();

    /// <summary>Waits out the idle delay; true when a notification ended it early (the signal is then consumed).</summary>
    async Task<bool> IdleAsync(TimeSpan delay, CancellationToken ct)
    {
        var wake = Volatile.Read(ref _wake);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var timer = Task.Delay(delay, _time, cts.Token);
        if (await Task.WhenAny(wake.Task, timer).ConfigureAwait(false) == wake.Task)
        {
            await cts.CancelAsync().ConfigureAwait(false);
            Interlocked.CompareExchange(ref _wake, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), wake);
            return true;
        }

        await timer.ConfigureAwait(false);
        return false;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is { } loop)
        {
            await loop.WaitAsync(ct).ConfigureAwait(false);
        }

        if (_listener is { } listener)
        {
            await listener.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        _stop.Dispose();
        _slot.Dispose();

    }

    // ------------------------------------------------------------------ registration seed (13 §6.1)

    async Task SeedAsync(IScheduleStore store, ScheduleDeclaration declaration, CancellationToken ct)
    {
        var definition = ScheduleCompiler.Compile(declaration, _preparer, _artifact);

        // The cutoff is store time; the first cron occurrence is strictly after it (no backfill).
        var cutoff = await store.StoreNowAsync(ct).ConfigureAwait(false);
        var next = await _slot.RunAsync(() => ScheduleRecurrence.For(definition, _artifact, _time)(cutoff), ct).ConfigureAwait(false);
        var firstDue = definition.Kind == ScheduleOperations.OneTime ? definition.DueAt : next?.At;
        var command = new StoreCommand<RegisterScheduleRequest>(Application, Guid.CreateVersion7(), _time.GetUtcNow() + ScheduleOperations.RegisterLifetime,
            new RegisterScheduleRequest(declaration.Key, definition, cutoff, firstDue));
        var reply = await SendAsync(() => store.RegisterScheduleAsync(command, ct), command.CommandId, ct).ConfigureAwait(false);
        if (reply.Status == StoreStatus.UnsupportedVersion)
        {
            throw new StoreIncompatibleException(
                $"Schedule '{declaration.Key}' cannot be registered: {reply.Error?.Code}. Activate capability 'scheduler' once per application: run "
                + $"'runwick store activate-capability --name scheduler --application {Application} --provider postgres|sqlite --connection-env VAR' "
                + "(Redis: RunwickRedisMigrations.ActivateCapabilityAsync), then restart.");
        }

        if (reply.Value is { Outcome: "drift" })
        {
            // Startup never overwrites (13 §6.1): the stored definition stays authoritative until an explicit edit.
            _logger.LogWarning("Schedule {Key} differs from its stored definition revision {Revision}; the stored definition stays in effect.",
                declaration.Key, reply.Value.DefinitionRevision);
        }
        else if (reply.Status != StoreStatus.Applied)
        {
            throw new StoreNotAppliedException(reply.Error?.Code ?? reply.Status.ToString());
        }
    }

    // ------------------------------------------------------------------ materialization loop (13 §4)

    async Task LoopAsync(IScheduleStore store, CancellationToken ct)
    {
        var options = _declarations.Options;
        var idle = options.PollInterval;
        while (!ct.IsCancellationRequested)
        {
            var worked = false;
            try
            {
                Interlocked.Increment(ref Passes);
                var due = await store.ReadDueSchedulesAsync(Application, ScheduleBounds.DueReadLimit, ct).ConfigureAwait(false);
                foreach (var schedule in due.Schedules)
                {
                    worked |= await MaterializeOneAsync(store, schedule, due.ObservedAt, ct).ConfigureAwait(false);
                }

                Health.Ok();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is StoreUnavailableException or StoreNotAppliedException or StoreOutcomeUnknownException or TimeoutException)
            {
                Health.Failed(ex);
                _logger.LogDebug(ex, "Scheduler pass failed; retrying after back-off.");
                Telemetry.RunwickLog.StoreUnavailable(_logger, "scheduler", ex is RunwickException r ? r.Code : ex.GetType().Name);
            }

            // Idle back-off to IdleCap (5 s, or 15 s while the schedule listener is healthy), never past the next due
            // instant this process knows of: occurrences become due by time, which no notification announces.
            idle = worked ? options.PollInterval : TimeSpan.FromTicks(Math.Min(idle.Ticks * 2, IdleCap.Ticks));
            var jitter = 1 + ((Random.Shared.NextDouble() * 0.2) - 0.1);
            var wait = TimeSpan.FromTicks((long)(idle.Ticks * jitter));
            if (UntilKnownDue() is { } untilDue && untilDue < wait)
            {
                wait = untilDue;
            }

            try
            {
                if (await IdleAsync(wait, ct).ConfigureAwait(false))
                {
                    idle = options.PollInterval;
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>Plans and sends one schedule's materialization. Returns true when the store applied a change.</summary>
    internal async Task<bool> MaterializeOneAsync(IScheduleStore store, ScheduleSnapshot schedule, DateTimeOffset cutoff, CancellationToken ct)
    {
        MaterializeRequest? plan;
        try
        {
            plan = await _slot.RunAsync(() =>
                SchedulePlanner.Plan(schedule, cutoff, ScheduleRecurrence.For(schedule.Definition, _artifact, _time), Guid.CreateVersion7), ct)
                .ConfigureAwait(false);
        }
        catch (ScheduleEvaluationException ex) when (ex.Class == EvaluationErrorClass.Deterministic)
        {
            await RecordFailureAsync(store, schedule, ex.WireName, ex.Message, ct).ConfigureAwait(false);
            return true;
        }
        catch (ScheduleEvaluationException ex)
        {
            // Local incompatibility, pressure or transient: a local diagnostic only, never needs_review (SCHED-11).
            lock (_localDiagnostics)
            {
                _localDiagnostics[schedule.ScheduleId] = ex.WireName;
            }

            Telemetry.RunwickLog.ScheduleMaterializationFailed(_logger, schedule.Key, ex.WireName);
            return false;
        }

        if (plan is null)
        {
            return false;
        }

        await TakeAllowanceAsync(plan.Occurrences.Count, ct).ConfigureAwait(false);
        var command = new StoreCommand<MaterializeRequest>(Application, Guid.CreateVersion7(), _time.GetUtcNow() + ScheduleOperations.SchedulerLifetime, plan);
        System.Diagnostics.Activity.Current = null;
        using var span = Telemetry.Telemetry.Span("runwick.schedule.materialize");
        var reply = await SendAsync(() => store.MaterializeOccurrencesAsync(command, ct), command.CommandId, ct).ConfigureAwait(false);
        span.Activity?.SetTag("runwick.schedule.result", Worker.WorkerEngine.Snake(reply.Status.ToString())).SetTag("runwick.schedule.occurrences", plan.Occurrences.Count);
        if (reply is { Status: StoreStatus.Applied, Value: { } materialized })
        {
            var now = _time.GetUtcNow();
            foreach (var due in plan.Occurrences)
            {
                Telemetry.Telemetry.ScheduleMaterializationDelay.Record(Math.Max(0, (now - due.ScheduledAt).TotalSeconds));
            }

            foreach (var occurrence in materialized.Occurrences)
            {
                Telemetry.Telemetry.ScheduleOccurrences.Add(1, occurrence.Disposition);
            }

            if (plan.AuditRanges.Count > 0)
            {
                Telemetry.Telemetry.ScheduleOccurrences.Add(plan.AuditRanges.Count, "skipped");
                Telemetry.RunwickLog.ScheduleMisfireEpisode(_logger, schedule.Key, schedule.Definition.MisfirePolicy, plan.AuditRanges.Count);
            }
        }

        switch (reply.Status)
        {
            case StoreStatus.Applied:
                Interlocked.Increment(ref Materializations);
                NoteNextScan(plan.ScheduleId, plan.After.ScanAfterAt, cutoff);
                return true;
            case StoreStatus.InvalidRequest or StoreStatus.UniquenessConflict when reply.Error?.Code is "queue_not_registered" or "uniqueness_conflict"
                or "uniqueness_composition_conflict" or "window_fingerprint_mismatch":
                await RecordFailureAsync(store, schedule, reply.Error!.Code, reply.Error.Detail, ct).ConfigureAwait(false);
                return true;
            default:
                // Stale plans, a paused or reviewed row, a backward clock or gated capabilities: replan on the next pass.
                _logger.LogDebug("Schedule {Key} plan not applied: {Reply}.", schedule.Key, reply);
                return false;
        }
    }

    async Task RecordFailureAsync(IScheduleStore store, ScheduleSnapshot schedule, string code, string? detail, CancellationToken ct)
    {
        var actor = new ControlActor("svc:scheduler", "Runwick scheduler", "runwick", null);
        var trimmed = detail is { Length: > 512 } ? detail[..512] : detail;
        var command = new StoreCommand<EvaluationFailureRequest>(Application, Guid.CreateVersion7(), _time.GetUtcNow() + ScheduleOperations.SchedulerLifetime,
            new EvaluationFailureRequest(schedule.ScheduleId, schedule.DefinitionRevision, code, trimmed, actor));
        var reply = await SendAsync(() => store.RecordEvaluationFailureAsync(command, ct), command.CommandId, ct).ConfigureAwait(false);
        if (reply.Status == StoreStatus.Applied)
        {
            Telemetry.RunwickLog.ScheduleNeedsReview(_logger, schedule.Key, code);
        }
    }

    /// <summary>
    /// Sends one command; an unknown outcome re-sends the same command (same ID and body, so a committed effect replays)
    /// until the resolution budget ends. A new command is never minted for the same plan (13 §4.1).
    /// </summary>
    async Task<StoreReply<T>> SendAsync<T>(Func<Task<StoreReply<T>>> send, Guid commandId, CancellationToken ct) where T : class
    {
        var deadline = _time.GetUtcNow() + ResolutionBudget;
        var delay = TimeSpan.FromMilliseconds(100);
        while (true)
        {
            try
            {
                using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
                budget.CancelAfter(RequestBudget);
                return await send().WaitAsync(budget.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is StoreOutcomeUnknownException or StoreUnavailableException or TimeoutException
                || (ex is OperationCanceledException && !ct.IsCancellationRequested))
            {
                if (_time.GetUtcNow() >= deadline)
                {
                    throw new StoreOutcomeUnknownException("resolution_budget_exhausted", commandId, typeof(T).Name, ex);
                }

                await Task.Delay(delay, _time, ct).ConfigureAwait(false);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(2).Ticks));
            }
        }
    }

    /// <summary>Local allowance: a token bucket of <c>JobsPerSecond</c> with <c>Burst</c> capacity (13 §10).</summary>
    async Task TakeAllowanceAsync(int jobs, CancellationToken ct)
    {
        var options = _declarations.Options;
        while (true)
        {
            var now = _time.GetTimestamp();
            _tokens = Math.Min(options.Burst, _tokens + (_time.GetElapsedTime(_refilledAt, now).TotalSeconds * options.JobsPerSecond));
            _refilledAt = now;
            if (_tokens >= Math.Min(jobs, options.Burst))
            {
                _tokens -= Math.Min(jobs, options.Burst);
                return;
            }

            var wait = TimeSpan.FromSeconds((Math.Min(jobs, options.Burst) - _tokens) / options.JobsPerSecond);
            await Task.Delay(wait, _time, ct).ConfigureAwait(false);
        }
    }
}
