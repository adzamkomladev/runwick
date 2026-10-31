using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Hosting;

/// <summary>
/// Process-wide Runwick state shared by producers and workers: the compiled registry, the store and the
/// one-time handshake (Describe → RegisterApplication → RegisterQueue → RegisterScope).
/// </summary>
internal sealed class RunwickRuntime(JobRegistry registry, IRunwickStore store, TimeProvider time, ILogger<RunwickRuntime> logger) : IDisposable
{
    /// <summary>Capabilities this binary knows (spec 03 §10.3; M3 additions per item, 08 §1).</summary>
    public static readonly IReadOnlyDictionary<string, int> KnownCapabilities = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["execution"] = 1,
        ["concurrency_permits"] = 1,
        ["dispatch_uniqueness"] = 1,
        ["controls"] = 1,
        ["transactional_completion"] = 1, ["rate_admission"] = 1, ["outbox_destination"] = 1,
        ["scheduler"] = 1,
    };

    readonly SemaphoreSlim _gate = new(1, 1);
    StoreDescriptor? _descriptor;

    public void Dispose() => _gate.Dispose();

    public JobRegistry Registry => registry;

    public IRunwickStore Store => store;

    public TimeProvider Time => time;

    public bool IsReady => _descriptor is not null;

    public Guid StoreId => _descriptor?.StoreId
        ?? throw new InvalidOperationException("Runwick has not completed its store handshake; start the host (or await a dispatch) first.");

    public IReadOnlyList<ScopeSeed> Scopes { get; private set; } = [];

    public ApplicationGate? Gate { get; private set; }

    public async Task<StoreDescriptor> EnsureReadyAsync(CancellationToken ct)
    {
        if (_descriptor is { } ready)
        {
            return ready;
        }

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _descriptor ??= await HandshakeAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    async Task<StoreDescriptor> HandshakeAsync(CancellationToken ct)
    {
        var app = registry.Application;
        var descriptor = await store.DescribeAsync(app, ct).ConfigureAwait(false);
        var seeded = await Applied(store.RegisterApplicationAsync(Command(Operations.RegisterApplication, new RegisterApplicationRequest()), ct)).ConfigureAwait(false);
        Gate = seeded.Gate;
        var queues = registry.QueueCapacities.Keys.Order(StringComparer.Ordinal).ToList();
        if (queues.Count > 0)
        {
            await Applied(store.RegisterQueuesAsync(Command(Operations.RegisterQueue, new RegisterQueuesRequest(queues)), ct)).ConfigureAwait(false);
        }

        var scopes = registry.QueueCapacities.Where(q => q.Value is not null)
            .Select(q => new ScopeRequirement(Serialization.ScopeRef.QueueKind, q.Key, q.Value!.Value))
            .Concat(registry.Compiled.Where(j => j.JobTypeCapacity is not null)
                .Select(j => new ScopeRequirement(Serialization.ScopeRef.JobTypeKind, j.Name, j.JobTypeCapacity!.Value)))
            .ToList();
        if (scopes.Count > 0)
        {
            var result = await Applied(store.RegisterScopesAsync(Command(Operations.RegisterScope, new RegisterScopesRequest(scopes)), ct)).ConfigureAwait(false);
            Scopes = result.Scopes;
            foreach (var drift in result.Scopes.Where(s => s.StoredCapacity != s.ConfiguredCapacity))
            {
                logger.LogWarning("Scope {Kind}:{Key} has stored capacity {Stored}; configuration says {Configured}. The stored value stays authoritative.",
                    drift.Kind, drift.Key, drift.StoredCapacity, drift.ConfiguredCapacity);
            }
        }

        return descriptor;
    }

    StoreCommand<T> Command<T>(string operation, T body) =>
        new(registry.Application, Guid.CreateVersion7(), time.GetUtcNow() + TimeSpan.FromMinutes(5) is var na
            ? DateTimeOffset.FromUnixTimeMilliseconds(na.ToUnixTimeMilliseconds()) : default, body);

    static async Task<T> Applied<T>(Task<StoreReply<T>> call) where T : class
    {
        var reply = await call.ConfigureAwait(false);
        return reply.Status == StoreStatus.Applied && reply.Value is not null
            ? reply.Value
            : throw new StoreIncompatibleException($"Store handshake failed: {reply.Status} {reply.Error?.Code} {reply.Error?.Detail}");
    }

    /// <summary>Unknown required capabilities (e.g. <c>rate_admission</c>) make the application unsupported by this binary.</summary>
    public IReadOnlyList<string> UnsupportedCapabilities() =>
        Gate is null
            ? []
            : [.. Gate.RequiredCapabilities.Where(c => !KnownCapabilities.TryGetValue(c.Key, out var v) || v < c.Value).Select(c => c.Key)];
}
