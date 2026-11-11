using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runwick.Hosting;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Worker;

namespace Runwick;

public enum RunwickHealthStatus
{
    Healthy,
    Degraded,
    Unhealthy,
}

/// <summary>One role's cached health (18 §7). <see cref="Reason"/> is a bounded code, never free text.</summary>
public sealed record RunwickRoleHealth(string Role, RunwickHealthStatus Status, string Reason, DateTimeOffset ObservedAt);

/// <summary>Cached liveness and per-role readiness (18 §7). Never waits on the store.</summary>
public interface IRunwickHealth
{
    RunwickRoleHealth Liveness();

    IReadOnlyList<RunwickRoleHealth> Readiness();
}

/// <summary>
/// Store reachability of one role, fed by its own calls and refreshers. Failing for less than the window is Degraded,
/// longer is Unhealthy; no success for <c>staleAfter</c> is Degraded <c>stale</c> (18 §7).
/// </summary>
internal sealed class RoleTracker(TimeProvider time)
{
    readonly Lock _lock = new();
    DateTimeOffset? _lastOk;
    DateTimeOffset? _failingSince;
    bool _incompatible;

    public void Ok()
    {
        lock (_lock)
        {
            (_lastOk, _failingSince, _incompatible) = (time.GetUtcNow(), null, false);
        }
    }

    public void Failed(Exception? ex = null)
    {
        lock (_lock)
        {
            if (ex is StoreIncompatibleException)
            {
                _incompatible = true;
            }

            _failingSince ??= time.GetUtcNow();
        }
    }

    public (RunwickHealthStatus, string) Evaluate(TimeSpan window, TimeSpan staleAfter)
    {
        var now = time.GetUtcNow();
        lock (_lock)
        {
            if (_incompatible)
            {
                return (RunwickHealthStatus.Unhealthy, "incompatible");
            }

            if (_failingSince is { } since)
            {
                return now - since >= window ? (RunwickHealthStatus.Unhealthy, "store_unavailable") : (RunwickHealthStatus.Degraded, "store_unavailable");
            }

            return _lastOk is { } ok && now - ok < staleAfter ? (RunwickHealthStatus.Healthy, "ok") : (RunwickHealthStatus.Degraded, "stale");
        }
    }
}

/// <summary>Marks a host that serves the management API (registered by <c>AddRunwickManagement</c>).</summary>
internal sealed class ManagementRoleMarker;

/// <summary>
/// The core health snapshot (18 §7) plus its 10 s refreshers on the background class: the producer compatibility
/// refresh and, on a management host, a store read. Checks only read cached state.
/// </summary>
internal sealed class RunwickHealthService(IServiceProvider services, RunwickRuntime runtime, TimeProvider time) : IRunwickHealth, IRunwickComponent, IDisposable
{
    public static readonly TimeSpan RefreshPeriod = TimeSpan.FromSeconds(10);
    static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);
    static readonly TimeSpan Window = TimeSpan.FromSeconds(30);

    readonly CancellationTokenSource _stop = new();
    readonly WorkerEngine? _worker = services.GetService<WorkerEngine>();
    readonly SchedulerService? _scheduler = services.GetService<SchedulerService>();
    readonly bool _management = services.GetService<ManagementRoleMarker>() is not null;
    readonly RoleTracker _managementReads = new(time);
    readonly ILogger _logger = services.GetService<ILogger<RunwickHealthService>>() ?? (ILogger)Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    Task? _loop;
    readonly Lock _descriptorLock = new();
    readonly StoreObservationHistory _history = new();
    (StoreDescriptor Descriptor, DateTimeOffset At, long Timestamp)? _lastDescriptor;
    string? _descriptorFailure;
    (Guid StoreId, object Value, DateTimeOffset At, long Timestamp)? _lastDiagnostics;
    bool _diagnosticsFailed;
    TimeSpan _diagnosticsRetryDelay;
    long _diagnosticsAttemptedAt;

    internal (StoreDescriptor? Descriptor, DateTimeOffset? LastSuccessfulAt, TimeSpan? Age, string? Failure) StoreObservation()
    {
        lock (_descriptorLock)
        {
            return _lastDescriptor is { } last
                ? (last.Descriptor, last.At, time.GetElapsedTime(last.Timestamp), _descriptorFailure)
                : (null, null, null, _descriptorFailure);
        }
    }

    internal (T? Value, DateTimeOffset? SampledAt, bool Fresh) Diagnostics<T>(Guid? storeId) where T : class
    {
        lock (_descriptorLock)
        {
            if (_lastDiagnostics is not { } last || last.StoreId != storeId || last.Value is not T value)
            {
                return (null, null, false);
            }

            var age = time.GetElapsedTime(last.Timestamp);
            return age < TimeSpan.FromMinutes(5) ? (value, last.At, !_diagnosticsFailed && age < 3 * RefreshPeriod) : (null, null, false);
        }
    }

    internal (IReadOnlyList<StoreCheckObservation> Checks, StoreTrend? Trend) StoreHistory(Guid? trendStoreId)
    {
        lock (_descriptorLock)
        {
            return _history.Snapshot(trendStoreId);
        }
    }

    async Task RefreshDiagnosticsAsync<T>(Func<CancellationToken, Task<T>> observe, Guid storeId, string role) where T : class
    {
        if (_diagnosticsRetryDelay > TimeSpan.Zero && time.GetElapsedTime(_diagnosticsAttemptedAt) < _diagnosticsRetryDelay)
        {
            return;
        }

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        budget.CancelAfter(Budget);
        _diagnosticsAttemptedAt = time.GetTimestamp();
        try
        {
            var value = await observe(budget.Token).ConfigureAwait(false);
            budget.Token.ThrowIfCancellationRequested();
            lock (_descriptorLock)
            {
                var at = time.GetUtcNow();
                _lastDiagnostics = (storeId, value, at, time.GetTimestamp());
                _diagnosticsFailed = false;
                switch (value)
                {
                    case PostgresStoreDiagnostics postgres:
                        _history.RecordSample(storeId, at, StoreTrendMetric.PostgresLockWaits, postgres.LockWaits.Scope,
                            postgres.LockWaits.Status == StoreSnapshotStatus.Available ? postgres.LockWaits.Value?.SessionsWaiting : null);
                        break;
                    case RedisStoreDiagnostics redis:
                        _history.RecordSample(storeId, at, StoreTrendMetric.RedisMemoryUsedBytes, redis.Memory.Scope,
                            redis.Memory.Status == StoreSnapshotStatus.Available ? redis.Memory.Value?.UsedBytes : null);
                        break;
                }
            }

            _diagnosticsRetryDelay = TimeSpan.Zero;
        }
        catch (Exception ex)
        {
            if (!_stop.IsCancellationRequested)
            {
                lock (_descriptorLock)
                {
                    _diagnosticsFailed = true;
                    _history.RecordGap(time.GetUtcNow());
                }

                _diagnosticsRetryDelay = TimeSpan.FromSeconds(Math.Clamp(_diagnosticsRetryDelay.TotalSeconds * 2, 20, 60));
                Telemetry.RunwickLog.StoreUnavailable(_logger, role, ex.GetType().Name);
            }
        }
    }

    async Task<StoreDescriptor> ObserveDescriptorAsync(IRunwickStore store, CancellationToken ct)
    {
        try
        {
            var descriptor = await store.DescribeAsync(runtime.Registry.Application, ct).ConfigureAwait(false);
            if (_management && store is ISqliteStoreDiagnosticsSource sqlite)
            {
                descriptor = descriptor with { SqliteDiagnostics = sqlite.ObserveDiagnostics() };
            }

            ct.ThrowIfCancellationRequested();
            lock (_descriptorLock)
            {
                var at = time.GetUtcNow();
                _lastDescriptor = (descriptor, at, time.GetTimestamp());
                _descriptorFailure = null;
                if (_management)
                {
                    _history.SelectStore(descriptor.StoreId);
                    _history.RecordCheck(at, StoreCheckStatus.Reachable);
                    if (descriptor.SqliteDiagnostics?.WalFile is { } wal)
                    {
                        _history.RecordSample(descriptor.StoreId, at, StoreTrendMetric.SqliteWalBytes, wal.Scope,
                            wal.Status == StoreSnapshotStatus.Available ? wal.Value?.Bytes : null);
                    }
                }
            }

            return descriptor;
        }
        catch (Exception ex) when (!_stop.IsCancellationRequested)
        {
            lock (_descriptorLock)
            {
                var incompatible = ex is StoreIncompatibleException;
                _descriptorFailure = incompatible ? "incompatible" : "store_unavailable";
                if (_management)
                {
                    var at = time.GetUtcNow();
                    _history.RecordCheck(at, incompatible ? StoreCheckStatus.Incompatible : StoreCheckStatus.Unavailable);
                    _history.RecordGap(at);
                }
            }

            throw;
        }
    }

    public RunwickRoleHealth Liveness()
    {
        var live = _worker?.Health().Live ?? true;
        return new("live", live ? RunwickHealthStatus.Healthy : RunwickHealthStatus.Unhealthy, live ? "ok" : "tick_stalled", time.GetUtcNow());
    }

    public IReadOnlyList<RunwickRoleHealth> Readiness()
    {
        var now = time.GetUtcNow();
        var stale = 3 * RefreshPeriod;
        var list = new List<RunwickRoleHealth>();
        var (ps, pr) = runtime.Producer.Evaluate(Window, stale);
        list.Add(new("producer", ps, pr, now));
        if (_worker is not null)
        {
            var h = _worker.Health();
            var status = h.Readiness switch
            {
                WorkerReadiness.Healthy => RunwickHealthStatus.Healthy,
                WorkerReadiness.Degraded => RunwickHealthStatus.Degraded,
                _ => RunwickHealthStatus.Unhealthy,
            };
            list.Add(new("worker", status, h.Status, now));
        }

        if (_scheduler is not null)
        {
            var (ss, sr) = _scheduler.Health.Evaluate(3 * _scheduler.PollInterval, 3 * _scheduler.MaxIdle);
            list.Add(new("scheduler", ss, sr, now));
        }

        if (_management)
        {
            var (ms, mr) = _managementReads.Evaluate(Window, stale);
            list.Add(new("management", ms, mr, now));
        }

        return list;
    }

    public Task StartAsync(CancellationToken ct)
    {
        _loop = Task.Run(LoopAsync, CancellationToken.None);
        return Task.CompletedTask;
    }

    async Task LoopAsync()
    {
        var store = runtime.Store;
        var reads = services.GetService<IManagementQueryStore>();
        for (var tick = 0; !_stop.IsCancellationRequested; tick++)
        {
            await Refresh(runtime.Producer, "producer", ct => ObserveDescriptorAsync(store, ct)).ConfigureAwait(false);
            if (_management && reads is not null)
            {
                await Refresh(_managementReads, "management", ct => reads.ListQueuesAsync(runtime.Registry.Application, null, null, 1, ct)).ConfigureAwait(false);
            }

            if (_management && StoreObservation() is ({ } descriptor, _, _, null))
            {
                if (store is IPostgresStoreDiagnosticsSource postgres)
                {
                    await RefreshDiagnosticsAsync(postgres.ObserveDiagnosticsAsync, descriptor.StoreId, "postgres_diagnostics").ConfigureAwait(false);
                }
                else if (store is IRedisStoreDiagnosticsSource redis)
                {
                    await RefreshDiagnosticsAsync(redis.ObserveDiagnosticsAsync, descriptor.StoreId, "redis_diagnostics").ConfigureAwait(false);
                }
            }

            if (tick % 6 == 5)
            {
                Telemetry.RunwickLog.Limiter.Flush(_logger);
            }

            try
            {
                await Task.Delay(RefreshPeriod, time, _stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    async Task Refresh<TResult>(RoleTracker tracker, string role, Func<CancellationToken, Task<TResult>> read)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        budget.CancelAfter(Budget);
        try
        {
            await read(budget.Token).ConfigureAwait(false);
            tracker.Ok();
        }
        catch (Exception ex)
        {
            // A provider can report cancellation of an in-flight read as a store error during shutdown.
            if (!_stop.IsCancellationRequested)
            {
                tracker.Failed(ex);
                Telemetry.RunwickLog.StoreUnavailable(_logger, role, ex is RunwickException r ? r.Code : ex.GetType().Name);
            }
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is { } loop)
        {
            await loop.ConfigureAwait(false);
        }
    }

    public void Dispose() => _stop.Dispose();
}
