using System.Reflection;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Registration;

namespace Runwick;

/// <summary>Configures one Runwick application. Created by <c>AddRunwick</c>.</summary>
public sealed class RunwickBuilder
{
    internal RunwickBuilder(IServiceCollection services) => Services = services;

    public IServiceCollection Services { get; }

    internal LayerValue<string> ApplicationName { get; } = new();
    internal Dictionary<Type, JobDeclaration> Jobs { get; } = [];
    internal List<(Type Job, Type Handler)> Handlers { get; } = [];
    internal PolicyLayer Defaults { get; } = new();
    internal Dictionary<string, LayerValue<int>> QueueDeclarations { get; } = new(StringComparer.Ordinal);
    internal List<Type> GlobalMiddleware { get; } = [];
    internal Dictionary<string, List<Type>> MiddlewareAliases { get; } = new(StringComparer.Ordinal);
    internal List<IStoreProviderRegistration> StoreProviders { get; } = [];
    internal List<WorkerBuilder> Workers { get; } = [];
    internal List<Action<ProducerOptions>> ProducerConfiguration { get; } = [];
    internal List<IJsonTypeInfoResolver> JsonResolvers { get; } = [];
    internal List<ConfigurationError> EarlyErrors { get; } = [];
    internal RunwickTelemetryOptions TelemetryOptions { get; } = new();

    /// <summary>Telemetry options (18 §1); nothing is exported unless the host adds a listener or exporter.</summary>
    public RunwickBuilder Telemetry(Action<RunwickTelemetryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(TelemetryOptions);
        return this;
    }

    public RunwickBuilder Application(string name)
    {
        ApplicationName.Set(name);
        return this;
    }

    public JobBuilder<TJob> AddJob<TJob>() where TJob : class, IQueueJob => new(Declare(typeof(TJob), register: true));

    public JobBuilder<TJob> ConfigureJob<TJob>() where TJob : class, IQueueJob => new(Declare(typeof(TJob), register: false));

    public RunwickBuilder AddHandler<TJob, THandler>()
        where TJob : class, IQueueJob
        where THandler : class, IJobHandler<TJob>
    {
        BindHandler(typeof(TJob), typeof(THandler));
        return this;
    }

    public RunwickBuilder AddJobsFromAssemblyContaining<TMarker>()
    {
        foreach (var type in ScanTypes(typeof(TMarker).Assembly))
        {
            if (typeof(IQueueJob).IsAssignableFrom(type) && type.GetCustomAttribute<JobNameAttribute>(inherit: false) is not null)
            {
                Declare(type, register: true);
            }
        }

        return this;
    }

    public RunwickBuilder AddHandlersFromAssemblyContaining<TMarker>()
    {
        foreach (var type in ScanTypes(typeof(TMarker).Assembly))
        {
            foreach (var contract in type.GetInterfaces()
                         .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IJobHandler<>))
                         .OrderBy(i => i.GenericTypeArguments[0].FullName, StringComparer.Ordinal))
            {
                BindHandler(contract.GenericTypeArguments[0], type);
            }
        }

        return this;
    }

    public RunwickBuilder JobDefaults(Action<JobDefaultsBuilder> configure)
    {
        configure(new JobDefaultsBuilder(Defaults));
        return this;
    }

    public RunwickBuilder Queue(string name, Action<QueueBuilder>? configure = null)
    {
        if (!QueueDeclarations.TryGetValue(name, out var capacity))
        {
            QueueDeclarations[name] = capacity = new LayerValue<int>();
        }

        configure?.Invoke(new QueueBuilder(capacity));
        return this;
    }

    public RunwickBuilder UseMiddleware<TMiddleware>() where TMiddleware : class, IJobMiddleware
    {
        GlobalMiddleware.Add(typeof(TMiddleware));
        return this;
    }

    public RunwickBuilder AddMiddleware<TMiddleware>(string alias) where TMiddleware : class, IJobMiddleware
    {
        if (!MiddlewareAliases.TryGetValue(alias, out var types))
        {
            MiddlewareAliases[alias] = types = [];
        }

        if (!types.Contains(typeof(TMiddleware)))
        {
            types.Add(typeof(TMiddleware));
        }

        return this;
    }

    public RunwickBuilder AddJsonTypeInfoResolver(IJsonTypeInfoResolver resolver)
    {
        JsonResolvers.Add(resolver);
        return this;
    }

    public RunwickBuilder Producer(Action<ProducerOptions> configure)
    {
        ProducerConfiguration.Add(configure);
        return this;
    }

    public RunwickBuilder AddWorker(Action<WorkerBuilder> configure)
    {
        var worker = new WorkerBuilder();
        configure(worker);
        Workers.Add(worker);
        return this;
    }

    internal void AddStoreProvider(IStoreProviderRegistration provider) => StoreProviders.Add(provider);

    JobDeclaration Declare(Type type, bool register)
    {
        if (!Jobs.TryGetValue(type, out var declaration))
        {
            Jobs[type] = declaration = new JobDeclaration(type);
        }

        declaration.Registered |= register;
        return declaration;
    }

    void BindHandler(Type job, Type handler)
    {
        Declare(job, register: true);
        if (!Handlers.Contains((job, handler)))
        {
            Handlers.Add((job, handler));
        }
    }

    static IEnumerable<Type> ScanTypes(Assembly assembly)
    {
        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            types = [.. ex.Types.Where(t => t is not null)!];
        }

        return types
            .Where(t => t is { IsClass: true, IsAbstract: false, IsGenericTypeDefinition: false })
            .OrderBy(t => t.FullName, StringComparer.Ordinal);
    }
}

/// <summary>Fluent configuration of one job contract (the fluent precedence layer).</summary>
public sealed class JobBuilder<TJob> where TJob : class, IQueueJob
{
    readonly JobDeclaration _d;

    internal JobBuilder(JobDeclaration declaration) => _d = declaration;

    public JobBuilder<TJob> Named(string name) => Do(() => _d.Name.Set(name));

    public JobBuilder<TJob> AtVersion(int payloadVersion) => Do(() => _d.Version.Set(payloadVersion));

    public JobBuilder<TJob> ToQueue(string queue) => Do(() => _d.Fluent.Queue.Set(queue));

    public JobBuilder<TJob> WithPriority(string priority) => Do(() => _d.Fluent.Priority.Set(priority));

    public JobBuilder<TJob> WithMaxExecutions(int maxExecutions) => Do(() => _d.Fluent.MaxExecutions.Set(maxExecutions));

    public JobBuilder<TJob> WithExecutionTimeout(TimeSpan timeout) => Do(() => _d.Fluent.ExecutionTimeout.Set(timeout));

    public JobBuilder<TJob> WithoutExecutionTimeout() => Do(() => _d.Fluent.ExecutionTimeout.Set(null));

    public JobBuilder<TJob> WithRetryDelay(TimeSpan initialDelay, TimeSpan maxDelay) => Do(() => _d.Fluent.RetryDelay.Set((initialDelay, maxDelay)));

    public JobBuilder<TJob> WithMaxUnstartedAbandonments(int maxConsecutive) => Do(() => _d.Fluent.MaxUnstartedAbandonments.Set(maxConsecutive));

    public JobBuilder<TJob> WithUnstartedRecoveryDelay(TimeSpan initialDelay, TimeSpan maxDelay) => Do(() => _d.Fluent.UnstartedDelay.Set((initialDelay, maxDelay)));

    public JobBuilder<TJob> Upgrade<TFrom, TTo>(int fromVersion, int toVersion, Func<TFrom, TTo> upgrade)
        where TFrom : class
        where TTo : class
    {
        ArgumentNullException.ThrowIfNull(upgrade);
        _d.Upgrades.Add(new UpgradeStep(fromVersion, toVersion, typeof(TFrom), typeof(TTo), o => upgrade((TFrom)o)));
        return this;
    }

    public JobBuilder<TJob> UseMiddleware<TMiddleware>() where TMiddleware : class, IJobMiddleware =>
        Do(() => _d.Middleware.Add(new MiddlewareRef(null, typeof(TMiddleware))));

    public JobBuilder<TJob> UseMiddleware(string alias) => Do(() => _d.Middleware.Add(new MiddlewareRef(alias, null)));

    public JobBuilder<TJob> MaxConcurrent(int capacity) => Do(() => _d.JobTypeCapacity.Set(capacity));

    public JobBuilder<TJob> PerResource(Func<TJob, string> resourceKey, int capacity)
    {
        ArgumentNullException.ThrowIfNull(resourceKey);
        _d.Resources.Add(new ResourceSelector(o => resourceKey((TJob)o), capacity));
        return this;
    }

    /// <summary>
    /// At most <paramref name="maxExecutions"/> Begin authorizations of this job type in any rolling <paramref name="window"/>,
    /// across every worker (10 §2). Requires the application capability <c>rate_admission</c>.
    /// </summary>
    public JobBuilder<TJob> RollingWindow(int maxExecutions, TimeSpan window) =>
        Do(() => _d.Rates.Add(new RateSelector(Serialization.ScopeRef.JobTypeKind, null, maxExecutions, window)));

    /// <summary>As <see cref="RollingWindow(int, TimeSpan)"/>, one window per resource key the selector returns (10 §2).</summary>
    public JobBuilder<TJob> RollingWindowPerResource(Func<TJob, string> resourceKey, int maxExecutions, TimeSpan window)
    {
        ArgumentNullException.ThrowIfNull(resourceKey);
        _d.Rates.Add(new RateSelector(Serialization.ScopeRef.ResourceKind, o => resourceKey((TJob)o), maxExecutions, window));
        return this;
    }

    public JobBuilder<TJob> UniqueWhileActive(Func<TJob, string> activeKey)
    {
        ArgumentNullException.ThrowIfNull(activeKey);
        _d.ActiveSelector.Set(o => activeKey((TJob)o));
        return this;
    }

    public JobBuilder<TJob> UniqueWithinWindow(Func<TJob, string> windowKey, TimeSpan window)
    {
        ArgumentNullException.ThrowIfNull(windowKey);
        _d.WindowSelector.Set((o => windowKey((TJob)o), window));
        return this;
    }

    JobBuilder<TJob> Do(Action action)
    {
        action();
        return this;
    }
}

/// <summary>Application job defaults (the second precedence layer).</summary>
public sealed class JobDefaultsBuilder
{
    readonly PolicyLayer _layer;

    internal JobDefaultsBuilder(PolicyLayer layer) => _layer = layer;

    public JobDefaultsBuilder ToQueue(string queue) => Do(() => _layer.Queue.Set(queue));

    public JobDefaultsBuilder WithPriority(string priority) => Do(() => _layer.Priority.Set(priority));

    public JobDefaultsBuilder WithMaxExecutions(int maxExecutions) => Do(() => _layer.MaxExecutions.Set(maxExecutions));

    public JobDefaultsBuilder WithExecutionTimeout(TimeSpan timeout) => Do(() => _layer.ExecutionTimeout.Set(timeout));

    public JobDefaultsBuilder WithoutExecutionTimeout() => Do(() => _layer.ExecutionTimeout.Set(null));

    public JobDefaultsBuilder WithRetryDelay(TimeSpan initialDelay, TimeSpan maxDelay) => Do(() => _layer.RetryDelay.Set((initialDelay, maxDelay)));

    public JobDefaultsBuilder WithMaxUnstartedAbandonments(int maxConsecutive) => Do(() => _layer.MaxUnstartedAbandonments.Set(maxConsecutive));

    public JobDefaultsBuilder WithUnstartedRecoveryDelay(TimeSpan initialDelay, TimeSpan maxDelay) => Do(() => _layer.UnstartedDelay.Set((initialDelay, maxDelay)));

    JobDefaultsBuilder Do(Action action)
    {
        action();
        return this;
    }
}

public sealed class QueueBuilder
{
    readonly LayerValue<int> _capacity;

    internal QueueBuilder(LayerValue<int> capacity) => _capacity = capacity;

    public QueueBuilder MaxConcurrent(int capacity)
    {
        _capacity.Set(capacity);
        return this;
    }
}

public enum LaneSelectionMode
{
    Weighted,
    StrictWithAging,
    Strict,
}

public sealed class WorkerBuilder
{
    internal List<PoolBuilder> Pools { get; } = [];
    internal int? MaxOutstanding { get; private set; }
    internal List<Action<WorkerOptions>> OptionActions { get; } = [];

    public WorkerBuilder Pool(string name, Action<PoolBuilder> configure)
    {
        var pool = new PoolBuilder(name);
        configure(pool);
        Pools.Add(pool);
        return this;
    }

    public WorkerBuilder MaxOutstandingAttempts(int maximum)
    {
        MaxOutstanding = maximum;
        return this;
    }

    public WorkerBuilder Configure(Action<WorkerOptions> configure)
    {
        OptionActions.Add(configure);
        return this;
    }

    internal List<Action<RetentionOptions>> RetentionActions { get; } = [];

    /// <summary>The background cleanup loop of this worker process (17 §4.4).</summary>
    public WorkerBuilder Retention(Action<RetentionOptions> configure)
    {
        RetentionActions.Add(configure);
        return this;
    }
}

/// <summary>
/// Cleanup budgets (17 §4.4). One batch at a time per process; a class waits <see cref="BatchPause"/> between its batches
/// and <see cref="IdlePoll"/> after a pass that found nothing. A failed batch backs off 5 s, doubling to 5 min.
/// </summary>
public sealed class RetentionOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>Root rows per batch, 1 – 1,000.</summary>
    public int BatchRows { get; set; } = 100;

    /// <summary>Decoded detail bytes per batch, 64 KiB – 16 MiB.</summary>
    public int BatchDetailBytes { get; set; } = 1024 * 1024;

    /// <summary>Database time a batch aims at, 50 ms – 2 s; the statement timeout is 4 × target, at least 2 s.</summary>
    public TimeSpan BatchTarget { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>0 – 60 s.</summary>
    public TimeSpan BatchPause { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>1 s – 1 h.</summary>
    public TimeSpan IdlePoll { get; set; } = TimeSpan.FromSeconds(60);
}

public sealed class PoolBuilder
{
    internal PoolBuilder(string name) => Name = name;

    internal string Name { get; }
    internal List<(string Queue, int Weight)> Subscriptions { get; } = [];
    internal int? Fixed { get; private set; }
    internal (int Min, int Max)? Auto { get; private set; }
    internal LaneSelectionMode Mode { get; private set; } = LaneSelectionMode.Weighted;
    internal (int High, int Medium, int Low) Weights { get; private set; } = (6, 3, 1);

    public PoolBuilder Queues(params string[] queues)
    {
        foreach (var q in queues)
        {
            Subscriptions.Add((q, 1));
        }

        return this;
    }

    public PoolBuilder Queue(string queue, int weight)
    {
        Subscriptions.Add((queue, weight));
        return this;
    }

    public PoolBuilder FixedConcurrency(int concurrency)
    {
        Fixed = concurrency;
        return this;
    }

    public PoolBuilder AutoConcurrency(int min, int max)
    {
        Auto = (min, max);
        return this;
    }

    public PoolBuilder LaneSelection(LaneSelectionMode mode)
    {
        Mode = mode;
        return this;
    }

    public PoolBuilder LaneWeights(int high, int medium, int low)
    {
        Weights = (high, medium, low);
        return this;
    }
}

/// <summary>Local worker timing policy. Defaults and inequalities: docs/spec/06-defaults.md.</summary>
public sealed class WorkerOptions
{
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan LeaseSafetyMargin { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromSeconds(15);
    /// <summary>Renewal is drawn from [interval − jitter, interval + jitter].</summary>
    public TimeSpan RenewalJitter { get; set; } = TimeSpan.FromSeconds(3);
    public TimeSpan LeaseWatchdogInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan SchedulerAllowance { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ControlRequestTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ControlRetryInitialBackoff { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan ControlRetryMaxBackoff { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan MinimumInvocationMargin { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan ClaimResolutionBudget { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan BeginResolutionBudget { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan CompletionResolutionBudget { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan CancellationGrace { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan CleanupBudget { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan OwnershipCommandLifetime { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan RecoveryScanInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan RecoveryScanJitter { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan ShutdownBudget { get; set; } = TimeSpan.FromSeconds(25);
    public TimeSpan ShutdownDrain { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan ShutdownCancellation { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ShutdownFinalization { get; set; } = TimeSpan.FromSeconds(5);
}
