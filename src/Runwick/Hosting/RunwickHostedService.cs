using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runwick.Registration;

namespace Runwick.Hosting;

/// <summary>Starts optional runtime components (the worker engine) after the handshake.</summary>
internal interface IRunwickComponent
{
    Task StartAsync(CancellationToken ct);

    Task StopAsync(CancellationToken ct);
}

/// <summary>Validates configuration and completes the store handshake at host start.</summary>
internal sealed class RunwickHostedService(IServiceProvider services, IEnumerable<IRunwickComponent> components) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        services.GetRequiredService<JobRegistry>();
        await services.GetRequiredService<RunwickRuntime>().EnsureReadyAsync(cancellationToken).ConfigureAwait(false);
        foreach (var component in components)
        {
            await component.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var component in components.Reverse())
        {
            await component.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
