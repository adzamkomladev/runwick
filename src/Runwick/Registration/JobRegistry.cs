using Runwick.Serialization;

namespace Runwick.Registration;

/// <summary>How to decode stored bytes of one payload version and upgrade them to the current type.</summary>
internal sealed record DecodeRoute(int Version, Type DtoType, IReadOnlyList<UpgradeStep> Steps)
{
    public object Upgrade(object dto)
    {
        foreach (var step in Steps)
        {
            dto = step.Apply(dto) ?? throw new InvalidOperationException($"Upgrade {step.FromVersion}→{step.ToVersion} returned null.");
        }

        return dto;
    }
}

internal sealed record CompiledWindow(Func<object, string> Select, TimeSpan Window);

/// <summary>A validated job contract with its fully resolved effective policy.</summary>
internal sealed record CompiledJob(
    string Name,
    Type PayloadType,
    int CurrentVersion,
    IReadOnlyDictionary<int, DecodeRoute> Routes,
    string Queue,
    string Priority,
    ExecutionPolicy Policy,
    int? JobTypeCapacity,
    IReadOnlyList<ResourceSelector> Resources,
    Func<object, string>? ActiveSelector,
    CompiledWindow? WindowSelector,
    IReadOnlyList<Type> MiddlewareChain,
    Type? HandlerType,
    JobDescriptor Descriptor)
{
    /// <summary>Rolling-window rate scopes (10 §2); empty for jobs without rate policy.</summary>
    public IReadOnlyList<RateSelector> Rates { get; init; } = [];
}

/// <summary>Rate policy validation bounds (06 §1.11, capacity-defaults §3).</summary>
internal static class RateBounds
{
    public const int MinLimit = 1;
    public const int MaxLimit = 100_000;
    public const long MinWindowMs = 1_000;
    public const long MaxWindowMs = 86_400_000;
    public const int MaxScopesPerJob = 8;
}

/// <summary>Immutable compiled registry: built once, validated, never mutated.</summary>
internal sealed class JobRegistry : IRunwickJobRegistry
{
    readonly Dictionary<string, CompiledJob> _byName;
    readonly Dictionary<Type, CompiledJob> _byType;

    public JobRegistry(
        string application,
        IReadOnlyList<CompiledJob> jobs,
        IReadOnlyDictionary<string, int?> queues,
        IReadOnlyList<Type> globalMiddleware)
    {
        Application = application;
        Compiled = [.. jobs.OrderBy(j => j.Name, StringComparer.Ordinal)];
        _byName = Compiled.ToDictionary(j => j.Name, StringComparer.Ordinal);
        _byType = Compiled.ToDictionary(j => j.PayloadType);
        QueueCapacities = queues;
        GlobalMiddleware = globalMiddleware;
        Jobs = [.. Compiled.Select(j => j.Descriptor)];
        Queues = [.. queues.Keys.OrderBy(q => q, StringComparer.Ordinal)];
        Capabilities = [.. Compiled
            .Where(j => j.HandlerType is not null)
            .SelectMany(j => j.Routes.Keys.Select(v => new JobContractKey(j.Name, v, RunwickJsonProfile.ProfileId, RunwickJsonProfile.ProfileVersion)))
            .OrderBy(c => c.Name, Internal.Utf8Order.Instance).ThenBy(c => c.PayloadVersion)];
    }

    public string Application { get; }

    public IReadOnlyList<CompiledJob> Compiled { get; }

    public IReadOnlyList<JobDescriptor> Jobs { get; }

    public IReadOnlyList<string> Queues { get; }

    /// <summary>Registered queues and their declared shared ceiling (null = no queue scope).</summary>
    public IReadOnlyDictionary<string, int?> QueueCapacities { get; }

    public IReadOnlyList<Type> GlobalMiddleware { get; }

    /// <summary>Decoding tuples this registry can execute (jobs with a bound handler).</summary>
    public IReadOnlyList<JobContractKey> Capabilities { get; }

    public JobDescriptor? Find(string jobName) => _byName.TryGetValue(jobName, out var job) ? job.Descriptor : null;

    public JobDescriptor Get<TJob>() where TJob : class, IQueueJob =>
        _byType.TryGetValue(typeof(TJob), out var job)
            ? job.Descriptor
            : throw new InvalidOperationException($"{typeof(TJob).Name} is not a registered job contract.");

    public CompiledJob? ByName(string name) => _byName.GetValueOrDefault(name);

    public CompiledJob? ByType(Type type) => _byType.GetValueOrDefault(type);
}

/// <summary>Decoding capability tuple (name, payload version, profile ID, profile version).</summary>
internal sealed record JobContractKey(string Name, int PayloadVersion, string SerializerProfileId, int SerializerProfileVersion);
