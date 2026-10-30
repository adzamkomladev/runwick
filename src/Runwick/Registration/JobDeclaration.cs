namespace Runwick.Registration;

internal sealed record UpgradeStep(int FromVersion, int ToVersion, Type FromType, Type ToType, Func<object, object> Apply);

internal sealed record MiddlewareRef(string? Alias, Type? Type);

internal sealed record ResourceSelector(Func<object, string> Select, int Capacity);

/// <summary>Everything declared about one CLR job type before validation.</summary>
internal sealed class JobDeclaration(Type type)
{
    public Type Type { get; } = type;
    public bool Registered { get; set; }
    public LayerValue<string> Name { get; } = new();
    public LayerValue<int> Version { get; } = new();
    public PolicyLayer Fluent { get; } = new();
    public List<UpgradeStep> Upgrades { get; } = [];
    public List<MiddlewareRef> Middleware { get; } = [];
    public LayerValue<int> JobTypeCapacity { get; } = new();
    public List<ResourceSelector> Resources { get; } = [];
    public LayerValue<Func<object, string>> ActiveSelector { get; } = new();
    public LayerValue<(Func<object, string> Select, TimeSpan Window)> WindowSelector { get; } = new();
}

/// <summary>Implemented by a storage provider (<c>UsePostgres</c>) to register its services.</summary>
internal interface IStoreProviderRegistration
{
    string Name { get; }

    void Register(Microsoft.Extensions.DependencyInjection.IServiceCollection services);
}
