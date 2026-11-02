using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Hosting;
using Runwick.Internal;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Worker;

/// <summary>Readiness as the health check reports it (05 §17).</summary>
internal enum WorkerReadiness
{
    Healthy,
    Degraded,
    Unhealthy,
}

internal sealed record WorkerHealth(bool Live, WorkerReadiness Readiness, string Status, IReadOnlyList<string> Diagnostics);

/// <summary>Optional engine hooks for deterministic tests (barriers on runner start); null in production.</summary>
internal sealed class EngineHooks
{
    public Func<AttemptEntry, Task>? BeforeRunnerStart { get; set; }

    public Action<string, AttemptEntry?>? Event { get; set; }
}

/// <summary>
/// WorkerHostedService + WorkerSupervisor (05 §2): validates, provisions, starts every coordinator under
/// supervision, owns admission state, the pressure gates and the staged shutdown (§16).
/// </summary>
internal sealed partial class WorkerEngine : IRunwickComponent, IAsyncDisposable
{
    readonly RunwickRuntime _runtime;
    readonly IServiceScopeFactory _scopes;
    readonly IHostApplicationLifetime? _lifetime;
    readonly List<Task> _tasks = [];
    readonly CancellationTokenSource _loops = new();
    readonly CancellationTokenSource _controlStop = new();
    readonly Lock _lock = new();
    readonly ConcurrentDictionary<Guid, AttemptEntry> _entries = new();
    readonly ConcurrentQueue<string> _diagnostics = new();
    CancellationTokenRegistration _stoppingRegistration;
    TimeSpan? _t0;
    bool _admissionClosed;
    string? _closedReason;
    bool _pressureStopped;
    int _healthyChecks;
    long _progressTick;
    TimeSpan _lastTick;
    bool _started;
    bool _stopped;

    public WorkerEngine(RunwickRuntime runtime, WorkerSettings settings, IServiceScopeFactory scopes, TimeProvider time,
        ILogger<WorkerEngine> logger, IHostApplicationLifetime? lifetime = null)
    {
        _runtime = runtime;
        Settings = settings;
        _scopes = scopes;
        _lifetime = lifetime;
        Logger = logger;
        Clock = new EngineClock(time);
        Registry = runtime.Registry;
        Contracts = [.. Registry.Capabilities.Select(c => new JobContract(c.Name, c.PayloadVersion, c.SerializerProfileId, c.SerializerProfileVersion))];
        Pipelines = Registry.Compiled.Where(j => j.HandlerType is not null).ToDictionary(j => j.Name, JobPipeline.Create, StringComparer.Ordinal);
        Pools = [.. settings.Pools.Select(p => new PoolCoordinator(this, p))];
        Lease = new LeaseCoordinator(this);
        Completion = new CompletionCoordinator(this);
        AdmissionIo = new PriorityGate(4);
        RenewalIo = new PriorityGate(4);
        SettlementIo = new PriorityGate(WorkerSettings.SettlementLane);
        BackgroundIo = new PriorityGate(2);
        Readiness = new ReadinessCoordinator(this, [.. Pools.SelectMany(p => p.Selector.Lanes).Distinct()]);
    }

    public WorkerSettings Settings { get; }

    public WorkerOptions Options => Settings.Options;

    public EngineClock Clock { get; }

    public ILogger Logger { get; }

    public JobRegistry Registry { get; }

    public IRunwickStore Store => _runtime.Store;

    public string Application => Registry.Application;

    public Guid WorkerInstanceId { get; } = Guid.CreateVersion7();

    public IReadOnlyList<JobContract> Contracts { get; }

    public IReadOnlyDictionary<string, JobPipeline> Pipelines { get; }

    public IReadOnlyList<PoolCoordinator> Pools { get; }

    public LeaseCoordinator Lease { get; }

    public CompletionCoordinator Completion { get; }

    public ReadinessCoordinator Readiness { get; }

    public PriorityGate AdmissionIo { get; }

    public PriorityGate RenewalIo { get; }

    public PriorityGate SettlementIo { get; }

    public PriorityGate BackgroundIo { get; }

    public IServiceScopeFactory ScopeFactory => _scopes;

    public EngineHooks Hooks { get; } = new();

    /// <summary>Scope ID → (kind, key) from startup provisioning (05 §8.7).</summary>
    public IReadOnlyDictionary<Guid, ScopeSeed> ScopeMap { get; private set; } = new Dictionary<Guid, ScopeSeed>();

    public IReadOnlyCollection<AttemptEntry> Entries => (IReadOnlyCollection<AttemptEntry>)_entries.Values;

    /// <summary>Cancelled at ControlStopCutoff or by the host token (05 §16 step 6); never the host token itself.</summary>
    public CancellationToken ControlStop => _controlStop.Token;

    public CancellationToken LoopStop => _loops.Token;

    public long Invocations;

    public long ScopesCreated;

    public long FinishesSent;

    /// <summary>Staged completions observed committed by receipt resolution (08 §8.2); no engine Finish was sent for them.</summary>
    public long StagedCompletionsObserved;

    /// <summary>Begins that storage answered with a committed rate deferral (T20, 10 §5).</summary>
    public long RateDeferrals;

    /// <summary>T0: first locally observed stop signal.</summary>
    public TimeSpan? StopSignalAt
    {
        get
        {
            lock (_lock)
            {
                return _t0;
            }
        }
    }

    public bool Draining => StopSignalAt is not null;

    public TimeSpan DrainCutoff => StopSignalAt!.Value + Options.ShutdownDrain;

    public TimeSpan CancellationCutoff => DrainCutoff + Options.ShutdownCancellation;

    public TimeSpan ControlStopCutoff => CancellationCutoff + Options.ShutdownFinalization;

    public bool PastDrainCutoff => StopSignalAt is not null && Clock.Now >= DrainCutoff;

    /// <summary>Fresh Reserve admission: open, not draining, not pressure-stopped.</summary>
    public bool AdmissionOpen
    {
        get
        {
            lock (_lock)
            {
                return _started && !_admissionClosed && !_pressureStopped && _t0 is null;
            }
        }
    }

    public bool PressureStopped
    {
        get
        {
            lock (_lock)
            {
                return _pressureStopped;
            }
        }
    }

    public string? ClosedReason
    {
        get
        {
            lock (_lock)
            {
                return _closedReason;
            }
        }
    }

    public void Emit(string name, AttemptEntry? entry = null)
    {
        Hooks.Event?.Invoke(name, entry);
        if (entry is not null)
        {
            Logger.LogDebug("{Event} {Entry}", name, entry);
        }
    }

    public void Diagnose(string message)
    {
        _diagnostics.Enqueue(message);
        while (_diagnostics.Count > 64 && _diagnostics.TryDequeue(out _))
        {
        }

        Logger.LogWarning("Runwick worker: {Diagnostic}", message);
    }

    /// <summary>Critical fault / incompatibility (05 §2 rule 3, §15.2): admission closes; renewal and settlement continue.</summary>
    public void CloseAdmission(string reason)
    {
        lock (_lock)
        {
            if (_admissionClosed)
            {
                return;
            }

            _admissionClosed = true;
            _closedReason = reason;
        }

        Diagnose("admission closed: " + reason);
        WakePools();
    }

    public void WakePools()
    {
        foreach (var pool in Pools)
        {
            pool.Wake.Set();
        }
    }

    // ------------------------------------------------------------------ wake notifications (05 §7.2, PF-07)

    /// <summary>Wake notifications received for a queue this worker serves.</summary>
    public long NotificationsReceived;

    /// <summary>Notifications dropped while <see cref="NotificationsSuppressed"/> was set.</summary>
    public long NotificationsDropped;

    /// <summary>Listener (re)subscriptions.</summary>
    public long ListenerSubscriptions;

    /// <summary>
    /// Drops every wake notification (harness hook for driver-contract-suite:OBS-01): the worker then finds work, control
    /// changes and recovery only by polling, which stays the correctness path.
    /// </summary>
    public bool NotificationsSuppressed { get; set; }

    void OnWakeNotification(string queue)
    {
        var pools = Pools.Where(p => p.Settings.Queues.Any(q => string.Equals(q.Queue, queue, StringComparison.Ordinal))).ToList();
        if (pools.Count == 0)
        {
            return;
        }

        Interlocked.Increment(ref NotificationsReceived);
        if (NotificationsSuppressed)
        {
            Interlocked.Increment(ref NotificationsDropped);
            return;
        }

        if (Readiness.OnWake(queue))
        {
            foreach (var pool in pools)
            {
                pool.Wake.Set();
            }
        }
    }

    /// <summary>(Re)subscribed: anything committed while unsubscribed was not notified, so look once now.</summary>
    void OnListenerSubscribed()
    {
        Interlocked.Increment(ref ListenerSubscriptions);
        if (NotificationsSuppressed)
        {
            return;
        }

        Readiness.Invalidate();
        WakePools();
    }

    public void Track(AttemptEntry entry) => _entries[entry.AttemptId] = entry;

    public void Untrack(AttemptEntry entry) => _entries.TryRemove(entry.AttemptId, out _);

    /// <summary>Starts a supervised task; a fault in a critical task closes admission (05 §2 rules 2–3).</summary>
    public void Supervise(string name, Func<Task> body, bool critical)
    {
        var task = Task.Run(async () =>
        {
            try
            {
                await body().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_loops.IsCancellationRequested || _controlStop.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Runwick worker task {Task} faulted.", name);
                if (critical)
                {
                    CloseAdmission($"critical task {name} faulted: {ex.GetType().Name}");
                }
            }
        });
        lock (_lock)
        {
            _tasks.Add(task);
        }
    }

    // ------------------------------------------------------------------ startup (05 §3)

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Steps 1–2 (validation, registry) ran when the settings and registry were built; step 3: compatibility.
        var unsupported = _runtime.UnsupportedCapabilities();
        if (unsupported.Count > 0)
        {
            throw new RunwickConfigurationException([new ConfigurationError(
                unsupported.Contains("rate_admission") ? RunwickErrorCodes.RatePoliciesUnsupported : RunwickErrorCodes.UnsupportedCapability,
                Application, $"V-24: the application requires capabilities this worker does not support: {string.Join(", ", unsupported)}. Upgrade this worker before the application activates them.")]);
        }

        // Step 4 ran in the runtime handshake (seed-if-absent); keep its scope map for blocked-scope classification.
        ScopeMap = _runtime.Scopes.ToDictionary(s => s.ScopeId);

        // Step 5: presence before admission (05 §14). A few bounded retries; a worker whose presence still fails starts
        // degraded (09 §3.2) because presence is observational and never gates ownership.
        for (var attempt = 0; ; attempt++)
        {
            await Presence.PublishAsync("starting", cancellationToken).ConfigureAwait(false);
            if (!Presence.Failing || attempt + 1 >= WorkerSettings.FirstPresenceAttempts)
            {
                break;
            }

            await Clock.Delay(Backoff(attempt), cancellationToken).ConfigureAwait(false);
        }

        if (Presence.Failing)
        {
            Diagnose("presence publication failing at startup; admission opens with readiness degraded");
        }

        // Step 6.
        Lease.Start();
        Completion.Start();
        Supervise("recovery", RecoveryLoopAsync, critical: false);
        Supervise("presence", PresenceLoopAsync, critical: false);
        Supervise("scaler", ScalerLoopAsync, critical: false);
        Supervise("supervisor", SupervisorLoopAsync, critical: true);
        foreach (var pool in Pools)
        {
            Supervise("pool:" + pool.Settings.Name, pool.RunAsync, critical: true);
        }

        // Optional wake channel (05 §7.2, PF-07): a hint only; losing it costs latency, never correctness.
        if (Store is IWakeNotificationSource notifications)
        {
            Supervise("listener", () => notifications.ListenAsync(Application, WakeChannels.Work, OnWakeNotification, OnListenerSubscribed, LoopStop),
                critical: false);
        }

        if (_lifetime is not null)
        {
            _stoppingRegistration = _lifetime.ApplicationStopping.Register(() => SignalStop());
        }

        lock (_lock)
        {
            _started = true;
        }

        WakePools();
        Emit("started");
    }

    // ------------------------------------------------------------------ supervisor tick, pressure gates (05 §15)

    PresenceLoop? _presence;

    public PresenceLoop Presence => _presence ??= new PresenceLoop(this);

    async Task SupervisorLoopAsync()
    {
        _lastTick = Clock.Now;
        while (!_loops.IsCancellationRequested)
        {
            await Clock.Delay(TimeSpan.FromSeconds(1), _loops.Token).ConfigureAwait(false);
            var now = Clock.Now;
            var lag = now - _lastTick - TimeSpan.FromSeconds(1);
            _lastTick = now;
            Interlocked.Increment(ref _progressTick);
            EvaluatePressure(now, lag);
        }
    }

    /// <summary>One 1 s pressure check (05 §15.2): stop immediately, resume after three consecutive healthy checks.</summary>
    internal void EvaluatePressure(TimeSpan now, TimeSpan lag)
    {
        var owned = _entries.Values.Where(e => !e.IsLost && e.Phase != AttemptPhase.Retired && e.Deadline is not null).ToList();
        var minMargin = owned.Count == 0 ? TimeSpan.MaxValue : owned.Min(e => e.Deadline!.Value - now);
        var oldestFinish = _entries.Values.Where(e => e.FinishPublishedAt is not null && e.Phase == AttemptPhase.Finishing)
            .Select(e => now - e.FinishPublishedAt!.Value).DefaultIfEmpty(TimeSpan.Zero).Max();
        var controlWait = Lease.OldestWait(now);
        var arbiterBlocked = _entries.Values.Any(e => e.Arbiter.Held > 0
            && System.Diagnostics.Stopwatch.GetElapsedTime(e.Arbiter.HeldSinceTimestamp) > Options.ControlRequestTimeout + Options.SchedulerAllowance);

        var stop = minMargin < Settings.StopMargin || oldestFinish >= TimeSpan.FromSeconds(5) || controlWait >= TimeSpan.FromSeconds(1)
            || arbiterBlocked || lag > Options.SchedulerAllowance;
        var healthy = minMargin >= Settings.ResumeMargin && oldestFinish <= TimeSpan.FromSeconds(1) && controlWait <= TimeSpan.FromMilliseconds(250)
            && !arbiterBlocked && lag <= Options.SchedulerAllowance && !_entries.Values.Any(e => e.CommandUnresolved && e.IsLost);
        string? transition = null;
        lock (_lock)
        {
            if (stop)
            {
                if (!_pressureStopped)
                {
                    Logger.LogWarning("Runwick worker pressure stop: margin {Margin}, oldest finish {Finish}, control wait {Wait}, lag {Lag}.",
                        minMargin, oldestFinish, controlWait, lag);
                    transition = "pressureStop:" + (minMargin < Settings.StopMargin ? "margin" : oldestFinish >= TimeSpan.FromSeconds(5) ? "finish"
                        : controlWait >= TimeSpan.FromSeconds(1) ? "controlWait" : arbiterBlocked ? "arbiter" : "lag");
                }

                _pressureStopped = true;
                _healthyChecks = 0;
            }
            else if (_pressureStopped)
            {
                _healthyChecks = healthy ? _healthyChecks + 1 : 0;
                if (_healthyChecks >= 3)
                {
                    _pressureStopped = false;
                    _healthyChecks = 0;
                    transition = "pressureResume";
                }
            }
        }

        if (transition is not null)
        {
            Emit(transition);
        }

        // Only the resume transition wakes the pools: a wake on every healthy tick would cut the idle backoff
        // (100 ms → 2 s) to the 1 s tick and double idle probes.
        if (transition == "pressureResume")
        {
            WakePools();
        }
    }

    public WorkerHealth Health()
    {
        var diagnostics = _diagnostics.ToList();
        var live = _stopped || Clock.Now - _lastTick <= 10 * Options.LeaseWatchdogInterval + TimeSpan.FromSeconds(1);
        string status;
        WorkerReadiness readiness;
        lock (_lock)
        {
            if (!_started || _t0 is not null || _admissionClosed)
            {
                (readiness, status) = (WorkerReadiness.Unhealthy, !_started ? "starting" : _t0 is not null ? "draining" : "closed");
            }
            else if (_pressureStopped || Pools.All(p => p.Suspended))
            {
                (readiness, status) = (WorkerReadiness.Unhealthy, "pressure");
            }
            else if (Pools.Any(p => p.Suspended) || Presence.Failing
                || _entries.Values.Any(e => e.Loss is LossCause.CleanupExpired or LossCause.GraceExpired))
            {
                (readiness, status) = (WorkerReadiness.Degraded, "degraded");
            }
            else
            {
                (readiness, status) = (WorkerReadiness.Healthy, "ready");
            }
        }

        return new WorkerHealth(live, readiness, status, diagnostics);
    }

    // ------------------------------------------------------------------ staged shutdown (05 §16)

    /// <summary>Records T0 once (ApplicationStopping or StopAsync entry, whichever is first) and closes admission atomically.</summary>
    public void SignalStop()
    {
        lock (_lock)
        {
            if (_t0 is not null)
            {
                return;
            }

            _t0 = Clock.Now;
        }

        foreach (var pool in Pools)
        {
            pool.Gate.Close();
        }

        Emit("draining");
        WakePools();
        Lease.Wake();
        foreach (var entry in _entries.Values)
        {
            entry.Changed.Set();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        SignalStop();
        using var hostRegistration = cancellationToken.Register(() => _controlStop.Cancel());
        var drainingPresence = Presence.PublishAsync("draining", CancellationToken.None);
        try
        {
            // Wait for every attempt to retire, bounded by the absolute ControlStopCutoff (or the host token).
            while (!_controlStop.IsCancellationRequested)
            {
                var remaining = ControlStopCutoff - Clock.Now;
                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                var live = _entries.Values.Where(e => e.Phase != AttemptPhase.Retired).Select(e => e.Retired).ToList();
                var claims = Pools.Select(p => p.ClaimTask).Where(t => t is { IsCompleted: false }).Cast<Task>().ToList();
                if (live.Count == 0 && claims.Count == 0)
                {
                    break;
                }

                using var step = CancellationTokenSource.CreateLinkedTokenSource(_controlStop.Token);
                var delay = Clock.Delay(Min(remaining, TimeSpan.FromMilliseconds(50)), step.Token);
                await Task.WhenAny([.. live, .. claims, delay]).ConfigureAwait(false);
                await step.CancelAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            // The draining publication runs under control stop, so it gets its 2 s bound before the stop cancels it.
            try
            {
                await drainingPresence.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
            {
                Diagnose("the draining presence publication did not finish within 2 s");
            }

            // Step 6: stop renewal, recovery and presence; cancel remaining I/O; report unresolved work.
            await _controlStop.CancelAsync().ConfigureAwait(false);
            await _loops.CancelAsync().ConfigureAwait(false);
            foreach (var entry in _entries.Values.Where(e => e.Phase != AttemptPhase.Retired))
            {
                entry.Latch(LossCause.ShutdownCutoff);
                Diagnose($"unresolved at shutdown: {entry}");
            }

            Task[] tasks;
            lock (_lock)
            {
                tasks = [.. _tasks];
                _stopped = true;
            }

            // Coordinator loops end promptly; user code is never awaited here.
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            try
            {
                await Task.WhenAll(tasks).WaitAsync(bound.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Diagnose("some worker loops did not stop within 2 s of control stop");
            }

            Emit("stopped");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stoppingRegistration.DisposeAsync().ConfigureAwait(false);
        if (!_loops.IsCancellationRequested)
        {
            await _loops.CancelAsync().ConfigureAwait(false);
        }

        if (!_controlStop.IsCancellationRequested)
        {
            await _controlStop.CancelAsync().ConfigureAwait(false);
        }

        Lease.Dispose();
        _loops.Dispose();
        _controlStop.Dispose();
    }

    public static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    // ------------------------------------------------------------------ bounded control I/O (05 §18)

    /// <summary>
    /// One control request: waits for its I/O class and runs the call, all within <paramref name="timeout"/>; the class
    /// slot is held until the provider task ends. Control stop cancels it.
    /// </summary>
    public async Task<T> CallAsync<T>(PriorityGate io, int priority, TimeSpan timeout, Func<CancellationToken, Task<T>> call, CancellationToken extra = default)
    {
        using var deadline = Clock.After(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token, _controlStop.Token, extra);
        var lap = PhaseTimings.Start();
        using var slot = await io.AcquireAsync(priority, linked.Token).ConfigureAwait(false);
        PhaseTimings.Lap(lap, io == RenewalIo ? "Renew" : io == SettlementIo ? "Finish" : "Admission", "engineLane");
        return await call(linked.Token).ConfigureAwait(false);
    }

    public StoreCommand<T> Command<T>(T body) => new(Application, Guid.CreateVersion7(), Clock.NotAfter(Options.OwnershipCommandLifetime), body);

    /// <summary>Lease grant → d = s + (ExpiresAt − DecidedAt) − safety (05 §11.1).</summary>
    public TimeSpan DeadlineFrom(TimeSpan sentAt, LeaseGrant grant, DateTimeOffset decidedAt) =>
        sentAt + (grant.ExpiresAt - decidedAt) - Options.LeaseSafetyMargin;

    public TimeSpan Backoff(int attempt) => EngineClock.ControlBackoff(attempt, Options.ControlRetryInitialBackoff, Options.ControlRetryMaxBackoff);
}
