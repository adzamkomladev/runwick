using System.Reflection;
using System.Text.Json.Serialization;
using Runwick.Execution;
using Runwick.Serialization;

namespace Runwick.Registration;

/// <summary>Validates a <see cref="RunwickBuilder"/> and compiles the immutable registry (spec 01 §5–§8).</summary>
internal sealed class RegistryCompiler(RunwickBuilder builder, bool isWorker)
{
    const int MaxScopesPerJob = 8;
    readonly List<ConfigurationError> _errors = [.. builder.EarlyErrors];

    public IReadOnlyList<ConfigurationError> Errors => _errors;

    void Error(string code, string subject, string message) => _errors.Add(new ConfigurationError(code, subject, message));

    public JobRegistry? Compile()
    {
        var application = CompileApplication();
        if (builder.StoreProviders.Count == 0)
        {
            Error(RunwickErrorCodes.MissingStore, "store", "No storage provider configured; call UsePostgres(...).");
        }
        else if (builder.StoreProviders.Count > 1)
        {
            Error(RunwickErrorCodes.DuplicateStore, "store", "More than one storage provider configured.");
        }

        var queues = CompileQueues();
        var aliases = CompileAliases();
        CheckDefaultsLayer();
        foreach (var t in builder.GlobalMiddleware.GroupBy(t => t).Where(g => g.Count() > 1))
        {
            Error(RunwickErrorCodes.DuplicateMiddleware, t.Key.Name, "Global middleware registered twice.");
        }

        var jobs = new List<CompiledJob>();
        foreach (var declaration in builder.Jobs.Values.OrderBy(d => d.Type.FullName, StringComparer.Ordinal))
        {
            if (!declaration.Registered)
            {
                Error(RunwickErrorCodes.JobNotRegistered, declaration.Type.Name,
                    "ConfigureJob<T>() was called but the contract is never registered (AddJob, AddHandler or assembly scan).");
                continue;
            }

            if (CompileJob(declaration, queues, aliases) is { } job)
            {
                jobs.Add(job);
            }
        }

        foreach (var group in jobs.GroupBy(j => j.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            Error(RunwickErrorCodes.DuplicateJobName, group.Key,
                $"Job name is used by {string.Join(", ", group.Select(j => j.PayloadType.Name))}.");
        }

        if (_errors.Count > 0 || application is null)
        {
            return null;
        }

        var registry = new JobRegistry(application, jobs, queues, builder.GlobalMiddleware);
        if (isWorker && registry.Capabilities.Count > PayloadBounds.MaxCapabilityTuples)
        {
            Error(RunwickErrorCodes.CapabilitySetTooLarge, "worker",
                $"{registry.Capabilities.Count} decoding tuples exceed the {PayloadBounds.MaxCapabilityTuples}-tuple bound.");
            return null;
        }

        return registry;
    }

    void CheckDefaultsLayer()
    {
        var d = builder.Defaults;
        foreach (var (setting, conflict) in new[]
        {
            ("Queue", d.Queue.HasConflict), ("Priority", d.Priority.HasConflict), ("MaxExecutions", d.MaxExecutions.HasConflict),
            ("ExecutionTimeout", d.ExecutionTimeout.HasConflict), ("RetryDelay", d.RetryDelay.HasConflict),
            ("MaxUnstartedAbandonments", d.MaxUnstartedAbandonments.HasConflict), ("UnstartedRecoveryDelay", d.UnstartedDelay.HasConflict),
        })
        {
            if (conflict)
            {
                Error(RunwickErrorCodes.SameLayerConflict, "JobDefaults", $"JobDefaults set {setting} to different values.");
            }
        }
    }

    string? CompileApplication()
    {
        var name = builder.ApplicationName;
        if (!name.IsSet)
        {
            Error(RunwickErrorCodes.MissingApplication, "application", "Call Application(name); there is no default application.");
            return null;
        }

        if (name.HasConflict)
        {
            Error(RunwickErrorCodes.SameLayerConflict, "application", "Application(...) was called with different names.");
        }

        if (!Identifiers.IsName(name.Value))
        {
            Error(RunwickErrorCodes.InvalidApplicationName, name.Value, "Application names are 1–128 chars of [A-Za-z0-9._-], starting with a letter or digit.");
            return null;
        }

        return name.Value;
    }

    Dictionary<string, int?> CompileQueues()
    {
        var queues = new Dictionary<string, int?>(StringComparer.Ordinal);
        foreach (var (name, capacity) in builder.QueueDeclarations)
        {
            if (!Identifiers.IsName(name))
            {
                Error(RunwickErrorCodes.InvalidQueueName, name, "Invalid queue name.");
                continue;
            }

            if (capacity.HasConflict)
            {
                Error(RunwickErrorCodes.SameLayerConflict, name, "Queue MaxConcurrent declared with different capacities.");
            }

            if (capacity.IsSet && capacity.Value < 1)
            {
                Error(RunwickErrorCodes.InvalidScope, name, "Queue MaxConcurrent must be at least 1.");
            }

            queues[name] = capacity.IsSet ? capacity.Value : null;
        }

        return queues;
    }

    Dictionary<string, Type> CompileAliases()
    {
        var aliases = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var (alias, types) in builder.MiddlewareAliases)
        {
            if (!Identifiers.IsName(alias))
            {
                Error(RunwickErrorCodes.InvalidMiddlewareAlias, alias, "Invalid middleware alias.");
                continue;
            }

            if (types.Count > 1)
            {
                Error(RunwickErrorCodes.MiddlewareAliasConflict, alias, $"Alias bound to {string.Join(", ", types.Select(t => t.Name))}.");
                continue;
            }

            aliases[alias] = types[0];
        }

        return aliases;
    }

    CompiledJob? CompileJob(JobDeclaration d, Dictionary<string, int?> queues, Dictionary<string, Type> aliases)
    {
        var type = d.Type;
        var subject = type.Name;
        var before = _errors.Count;

        if (!typeof(IQueueJob).IsAssignableFrom(type) || type.IsAbstract)
        {
            Error(RunwickErrorCodes.InvalidPolicy, subject, "Job contracts are concrete classes implementing IQueueJob.");
            return null;
        }

        // Identity: attribute and fluent declarations must agree.
        var nameAttr = type.GetCustomAttribute<JobNameAttribute>(inherit: false)?.Name;
        var versionAttr = type.GetCustomAttribute<PayloadVersionAttribute>(inherit: false)?.Version;
        if (d.Name.HasConflict || (nameAttr is not null && d.Name.IsSet && nameAttr != d.Name.Value))
        {
            Error(RunwickErrorCodes.IdentityConflict, subject, "[JobName] and Named(...) disagree.");
        }

        var name = d.Name.IsSet ? d.Name.Value : nameAttr;
        if (name is null)
        {
            Error(RunwickErrorCodes.MissingJobName, subject, "Every job contract needs [JobName] or .Named(...); CLR names are never used.");
            return null;
        }

        subject = name;
        if (!Identifiers.IsJobName(name))
        {
            Error(RunwickErrorCodes.InvalidJobName, name, "Job names are 1–200 chars of [A-Za-z0-9._-], starting with a letter or digit.");
        }

        if (d.Version.HasConflict || (versionAttr is not null && d.Version.IsSet && versionAttr != d.Version.Value))
        {
            Error(RunwickErrorCodes.IdentityConflict, name, "[PayloadVersion] and AtVersion(...) disagree.");
        }

        var version = d.Version.IsSet ? d.Version.Value : versionAttr ?? 1;
        var versionSource = d.Version.IsSet ? ConfigurationSource.Fluent : versionAttr is not null ? ConfigurationSource.Attribute : ConfigurationSource.LibraryDefault;
        if (version < 1)
        {
            Error(RunwickErrorCodes.InvalidPayloadVersion, name, "Payload versions start at 1.");
        }

        ValidateSerializerMetadata(type, name);
        var routes = CompileUpgrades(d, name, version);

        // Policy precedence: library → application defaults → attributes → fluent.
        var queue = Resolve(name, "Queue", Identifiers.DefaultQueue, builder.Defaults.Queue,
            Attr(type.GetCustomAttribute<QueueAttribute>(inherit: false)?.Name), d.Fluent.Queue);
        var priority = Resolve(name, "Priority", Identifiers.DefaultPriority, builder.Defaults.Priority,
            Attr(type.GetCustomAttribute<PriorityAttribute>(inherit: false)?.Priority), d.Fluent.Priority);
        var maxExecutions = Resolve(name, "MaxExecutions", 3, builder.Defaults.MaxExecutions,
            Attr(type.GetCustomAttribute<MaxExecutionsAttribute>(inherit: false)?.MaxExecutions), d.Fluent.MaxExecutions);
        var timeout = Resolve<TimeSpan?>(name, "ExecutionTimeout", null, builder.Defaults.ExecutionTimeout, default, d.Fluent.ExecutionTimeout);
        var retry = Resolve(name, "RetryDelay", (TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60)), builder.Defaults.RetryDelay, default, d.Fluent.RetryDelay);
        var unstarted = Resolve(name, "MaxUnstartedAbandonments", 5, builder.Defaults.MaxUnstartedAbandonments, default, d.Fluent.MaxUnstartedAbandonments);
        var unstartedDelay = Resolve(name, "UnstartedRecoveryDelay", (TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)), builder.Defaults.UnstartedDelay, default, d.Fluent.UnstartedDelay);

        if (!Identifiers.IsName(queue.Value))
        {
            Error(RunwickErrorCodes.InvalidQueueName, name, $"Queue '{queue.Value}' is not a valid queue name.");
        }
        else
        {
            queues.TryAdd(queue.Value, null);
        }

        if (!Identifiers.IsPriority(priority.Value))
        {
            Error(RunwickErrorCodes.InvalidPriority, name, $"Priority '{priority.Value}' must be low, medium or high.");
        }

        if (maxExecutions.Value is < 1 or > PolicyBounds.MaxExecutions)
        {
            Error(RunwickErrorCodes.InvalidPolicy, name, $"MaxExecutions must be within 1–{PolicyBounds.MaxExecutions}.");
        }

        if (unstarted.Value is < 1 or > PolicyBounds.MaxUnstartedAbandonments)
        {
            Error(RunwickErrorCodes.InvalidPolicy, name,
                $"MaxUnstartedAbandonments must be within 1–{PolicyBounds.MaxUnstartedAbandonments}; the guard cannot be disabled.");
        }

        long? timeoutMs = null;
        if (timeout.Value is { } t)
        {
            timeoutMs = WholeMs(name, "ExecutionTimeout", t, PolicyBounds.MaxExecutionTimeoutMs);
        }

        var retryPolicy = DelayPolicy(name, "RetryDelay", retry.Value);
        var unstartedPolicy = DelayPolicy(name, "UnstartedRecoveryDelay", unstartedDelay.Value);

        // Scopes.
        if (d.JobTypeCapacity.HasConflict)
        {
            Error(RunwickErrorCodes.SameLayerConflict, name, "MaxConcurrent declared with different capacities.");
        }

        int? jobTypeCapacity = d.JobTypeCapacity.IsSet ? d.JobTypeCapacity.Value : null;
        if (jobTypeCapacity is < 1)
        {
            Error(RunwickErrorCodes.InvalidScope, name, "MaxConcurrent must be at least 1.");
        }

        foreach (var r in d.Resources.Where(r => r.Capacity < 1))
        {
            Error(RunwickErrorCodes.InvalidScope, name, $"PerResource capacity {r.Capacity} must be at least 1.");
        }

        var scopeCount = (queues.GetValueOrDefault(queue.Value) is null ? 0 : 1) + (jobTypeCapacity is null ? 0 : 1) + d.Resources.Count;
        if (scopeCount > MaxScopesPerJob)
        {
            Error(RunwickErrorCodes.TooManyScopes, name, $"{scopeCount} concurrency scopes exceed {MaxScopesPerJob}.");
        }

        // Rate scopes (10 §2).
        foreach (var r in d.Rates)
        {
            if (r.Limit is < RateBounds.MinLimit or > RateBounds.MaxLimit
                || r.Window.Ticks % TimeSpan.TicksPerMillisecond != 0 || r.WindowMs is < RateBounds.MinWindowMs or > RateBounds.MaxWindowMs)
            {
                Error(RunwickErrorCodes.InvalidRatePolicy, name,
                    $"RollingWindow needs 1–100,000 executions and a whole-millisecond window of 1 s–24 h (got {r.Limit} per {r.Window}).");
            }
        }

        if (d.Rates.Count(r => r.Select is null) > 1)
        {
            Error(RunwickErrorCodes.SameLayerConflict, name, "RollingWindow declared more than once for the job type.");
        }

        if (d.Rates.Count > RateBounds.MaxScopesPerJob)
        {
            Error(RunwickErrorCodes.TooManyRateScopes, name, $"{d.Rates.Count} rate scopes exceed {RateBounds.MaxScopesPerJob}.");
        }

        // Uniqueness selectors.
        if (d.ActiveSelector.HasConflict || d.WindowSelector.HasConflict)
        {
            Error(RunwickErrorCodes.SameLayerConflict, name, "Uniqueness selectors declared twice.");
        }

        CompiledWindow? window = null;
        if (d.WindowSelector.IsSet)
        {
            var w = d.WindowSelector.Value.Window;
            if (w <= TimeSpan.Zero || w.Ticks % TimeSpan.TicksPerMillisecond != 0 || w.Ticks / TimeSpan.TicksPerMillisecond > PolicyBounds.MaxWindowMs)
            {
                Error(RunwickErrorCodes.InvalidUniqueness, name, "UniqueWithinWindow needs a positive whole-millisecond window of at most 3,650 days.");
            }

            window = new CompiledWindow(d.WindowSelector.Value.Select, w);
        }

        // Handler.
        var handlers = builder.Handlers.Where(h => h.Job == type).Select(h => h.Handler).Distinct().ToList();
        if (handlers.Count > 1)
        {
            Error(RunwickErrorCodes.DuplicateHandler, name, $"Handlers {string.Join(", ", handlers.Select(h => h.Name))} are bound to one job name.");
        }

        var middleware = CompileMiddleware(type, name, d, aliases);

        if (_errors.Count > before)
        {
            return null;
        }

        var policy = new ExecutionPolicy(maxExecutions.Value, timeoutMs, retryPolicy, unstarted.Value, unstartedPolicy);
        var scopes = new List<ScopeDeclaration>();
        if (queues.GetValueOrDefault(queue.Value) is { } qc)
        {
            scopes.Add(new ScopeDeclaration(ScopeRef.QueueKind, queue.Value, qc));
        }

        if (jobTypeCapacity is { } jc)
        {
            scopes.Add(new ScopeDeclaration(ScopeRef.JobTypeKind, name, jc));
        }

        scopes.AddRange(d.Resources.Select(r => new ScopeDeclaration(ScopeRef.ResourceKind, null, r.Capacity)));

        var descriptor = new JobDescriptor(
            name, type, new EffectiveValue<int>(version, versionSource), [.. routes.Keys.Order()],
            queue, priority, maxExecutions, timeout,
            new EffectiveValue<RetryDelayPolicy>(new RetryDelayPolicy(RetryEvaluator.Name, RetryEvaluator.EqualJitter, retry.Value.Item1, retry.Value.Item2), retry.Source),
            unstarted,
            new EffectiveValue<RetryDelayPolicy>(new RetryDelayPolicy(RetryEvaluator.Name, RetryEvaluator.EqualJitter, unstartedDelay.Value.Item1, unstartedDelay.Value.Item2), unstartedDelay.Source),
            scopes, [.. middleware.Select(m => m.Name)], handlers.FirstOrDefault());

        return new CompiledJob(name, type, version, routes, queue.Value, priority.Value, policy, jobTypeCapacity,
            d.Resources, d.ActiveSelector.IsSet ? d.ActiveSelector.Value : null, window,
            middleware, handlers.FirstOrDefault(), descriptor)
        {
            Rates = [.. d.Rates],
        };
    }

    static (bool Set, T Value) Attr<T>(T? value) where T : class => (value is not null, value!);

    static (bool Set, T Value) Attr<T>(T? value) where T : struct => (value.HasValue, value.GetValueOrDefault());

    EffectiveValue<T> Resolve<T>(string job, string setting, T library, LayerValue<T> defaults, (bool Set, T Value) attribute, LayerValue<T> fluent)
    {
        if (fluent.HasConflict)
        {
            Error(RunwickErrorCodes.SameLayerConflict, job, $"Fluent configuration set {setting} to different values.");
        }

        if (fluent.IsSet)
        {
            return new(fluent.Value, ConfigurationSource.Fluent);
        }

        if (attribute.Set)
        {
            return new(attribute.Value, ConfigurationSource.Attribute);
        }

        return defaults.IsSet ? new(defaults.Value, ConfigurationSource.ApplicationDefault) : new(library, ConfigurationSource.LibraryDefault);
    }

    long WholeMs(string job, string setting, TimeSpan value, long maximumMs)
    {
        if (value <= TimeSpan.Zero || value.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            Error(RunwickErrorCodes.InvalidPolicy, job, $"{setting} must be a positive whole number of milliseconds.");
            return 1;
        }

        var ms = value.Ticks / TimeSpan.TicksPerMillisecond;
        if (ms > maximumMs)
        {
            Error(RunwickErrorCodes.InvalidPolicy, job, $"{setting} must be at most {maximumMs} ms.");
        }

        return ms;
    }

    RetryPolicy DelayPolicy(string job, string setting, (TimeSpan Initial, TimeSpan Max) value)
    {
        var initial = WholeMs(job, setting + ".initial", value.Initial, PolicyBounds.MaxRetryDelayMs);
        var max = WholeMs(job, setting + ".max", value.Max, PolicyBounds.MaxRetryDelayMs);
        if (initial > max)
        {
            Error(RunwickErrorCodes.InvalidPolicy, job, $"{setting}: initial delay exceeds the maximum.");
        }

        return new RetryPolicy("exponential", RetryEvaluator.Version, RetryEvaluator.EqualJitter, initial, max);
    }

    Dictionary<int, DecodeRoute> CompileUpgrades(JobDeclaration d, string name, int current)
    {
        var routes = new Dictionary<int, DecodeRoute> { [current] = new DecodeRoute(current, d.Type, []) };
        var byFrom = new Dictionary<int, UpgradeStep>();
        foreach (var step in d.Upgrades)
        {
            if (step.ToVersion != step.FromVersion + 1)
            {
                Error(RunwickErrorCodes.UpgradeNotAdjacent, name, $"Upgrade {step.FromVersion}→{step.ToVersion} is not adjacent.");
                continue;
            }

            if (step.FromVersion < 1 || step.ToVersion > current)
            {
                Error(step.FromVersion >= current ? RunwickErrorCodes.UpgradeCycle : RunwickErrorCodes.UpgradeGap, name,
                    $"Upgrade {step.FromVersion}→{step.ToVersion} does not lead to current version {current}.");
                continue;
            }

            if (!byFrom.TryAdd(step.FromVersion, step))
            {
                Error(RunwickErrorCodes.UpgradeDuplicateRoute, name, $"Two upgrades start at version {step.FromVersion}.");
            }
        }

        if (byFrom.Count == 0)
        {
            return routes;
        }

        var lowest = byFrom.Keys.Min();
        for (var v = lowest; v < current; v++)
        {
            if (!byFrom.ContainsKey(v))
            {
                Error(RunwickErrorCodes.UpgradeGap, name, $"No upgrade from version {v} to {v + 1}.");
                return routes;
            }
        }

        // Chain types: final TTo is the current type; each TFrom equals the previous TTo; no type used twice.
        var typeByVersion = new Dictionary<int, Type> { [current] = d.Type };
        for (var v = current - 1; v >= lowest; v--)
        {
            var step = byFrom[v];
            if (step.ToType != typeByVersion[v + 1])
            {
                Error(RunwickErrorCodes.UpgradeTypeConflict, name, $"Upgrade {v}→{v + 1} produces {step.ToType.Name}, expected {typeByVersion[v + 1].Name}.");
                return routes;
            }

            typeByVersion[v] = step.FromType;
        }

        if (typeByVersion.Values.Distinct().Count() != typeByVersion.Count)
        {
            Error(RunwickErrorCodes.UpgradeTypeConflict, name, "One CLR type is used for two payload versions.");
            return routes;
        }

        for (var v = lowest; v < current; v++)
        {
            var steps = Enumerable.Range(v, current - v).Select(x => byFrom[x]).ToList();
            routes[v] = new DecodeRoute(v, typeByVersion[v], steps);
        }

        return routes;
    }

    List<Type> CompileMiddleware(Type type, string name, JobDeclaration d, Dictionary<string, Type> aliases)
    {
        var chain = new List<Type>(builder.GlobalMiddleware);
        var attributes = type.GetCustomAttributes<UseJobMiddlewareAttribute>(inherit: false)
            .OrderBy(a => a.Order).ThenBy(a => a.Alias, StringComparer.Ordinal).ToList();
        var refs = attributes.Select(a => new MiddlewareRef(a.Alias, null)).Concat(d.Middleware).ToList();
        var seenAliases = new HashSet<string>(StringComparer.Ordinal);
        foreach (var r in refs)
        {
            if (r.Alias is { } alias)
            {
                if (!Identifiers.IsName(alias))
                {
                    Error(RunwickErrorCodes.InvalidMiddlewareAlias, name, $"Invalid middleware alias '{alias}'.");
                    continue;
                }

                if (!seenAliases.Add(alias))
                {
                    Error(RunwickErrorCodes.DuplicateMiddleware, name, $"Middleware alias '{alias}' appears twice in the effective chain.");
                    continue;
                }

                if (aliases.TryGetValue(alias, out var bound))
                {
                    chain.Add(bound);
                }
                else if (isWorker)
                {
                    Error(RunwickErrorCodes.UnknownMiddlewareAlias, name,
                        $"Job '{name}' uses middleware alias '{alias}', but no AddMiddleware<T>(\"{alias}\") call registers it.");
                }
            }
            else if (r.Type is { } t)
            {
                chain.Add(t);
            }
        }

        foreach (var duplicate in chain.GroupBy(t => t).Where(g => g.Count() > 1))
        {
            Error(RunwickErrorCodes.DuplicateMiddleware, name, $"Middleware {duplicate.Key.Name} appears twice in the effective chain.");
        }

        return chain;
    }

    /// <summary>Rejects member metadata that relaxes the runwick.json v1 profile (spec 02 §3.2).</summary>
    void ValidateSerializerMetadata(Type root, string name)
    {
        var seen = new HashSet<Type>();
        var pending = new Stack<Type>([root]);
        while (pending.Count > 0)
        {
            var type = pending.Pop();
            if (type == typeof(object))
            {
                Error(RunwickErrorCodes.InvalidPolicy, name, "A member or collection element is typed object; use JsonElement or a concrete type.");
                continue;
            }

            if (!seen.Add(type) || type.IsPrimitive || type == typeof(string))
            {
                continue;
            }

            if (type.IsArray)
            {
                pending.Push(type.GetElementType()!);
                continue;
            }

            // Framework types (collections, Nullable<T>, JsonElement, Guid, …): only their type arguments are user data.
            if (type.Namespace?.StartsWith("System", StringComparison.Ordinal) == true)
            {
                foreach (var argument in type.IsGenericType ? type.GenericTypeArguments : [])
                {
                    pending.Push(argument);
                }

                continue;
            }

            if (type.IsEnum && type.GetCustomAttribute<FlagsAttribute>() is not null)
            {
                Error(RunwickErrorCodes.InvalidPolicy, name, $"[Flags] enum {type.Name} is not supported by runwick.json v1.");
            }

            if (type.GetCustomAttribute<JsonNumberHandlingAttribute>() is { Handling: not JsonNumberHandling.Strict }
                || type.GetCustomAttribute<JsonUnmappedMemberHandlingAttribute>() is { UnmappedMemberHandling: JsonUnmappedMemberHandling.Skip })
            {
                Error(RunwickErrorCodes.InvalidPolicy, name, $"{type.Name} relaxes the runwick.json v1 profile.");
            }

            // Custom converters are executable configuration that Runwick has not registered (spec 02 §3.2).
            if (type.GetCustomAttribute<JsonConverterAttribute>() is not null)
            {
                Error(RunwickErrorCodes.InvalidPolicy, name, $"{type.Name} carries [JsonConverter]; runwick.json v1 accepts no custom converters.");
            }

            foreach (var argument in type.IsGenericType ? type.GenericTypeArguments : [])
            {
                pending.Push(argument);
            }

            foreach (var derived in type.GetCustomAttributes<JsonDerivedTypeAttribute>(inherit: false))
            {
                pending.Push(derived.DerivedType);
            }

            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.GetIndexParameters().Length > 0 || property.GetCustomAttribute<JsonIgnoreAttribute>() is { Condition: JsonIgnoreCondition.Always })
                {
                    continue;
                }

                if (property.GetCustomAttribute<JsonExtensionDataAttribute>() is not null
                    || property.GetCustomAttribute<JsonNumberHandlingAttribute>() is { Handling: not JsonNumberHandling.Strict })
                {
                    Error(RunwickErrorCodes.InvalidPolicy, name, $"{type.Name}.{property.Name} relaxes the runwick.json v1 profile.");
                }

                if (property.GetCustomAttribute<JsonConverterAttribute>() is not null)
                {
                    Error(RunwickErrorCodes.InvalidPolicy, name, $"{type.Name}.{property.Name} carries [JsonConverter]; runwick.json v1 accepts no custom converters.");
                }

                pending.Push(property.PropertyType);
            }
        }
    }
}
