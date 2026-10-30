using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Postgres;

internal static partial class PostgresServices
{
    static partial void RegisterStore(IServiceCollection services)
    {
        services.TryAddSingleton<IFaultInjector, NullFaultInjector>();
        services.AddSingleton(sp =>
        {
            var registration = sp.GetRequiredService<PostgresStoreRegistration>();
            var application = sp.GetRequiredService<JobRegistry>().Application;
            return new PgDataSources(registration.ConnectionString, "runwick:" + application);
        });
        services.AddSingleton(sp => new PgRunner(
            sp.GetRequiredService<PgDataSources>(), sp.GetRequiredService<IFaultInjector>(), sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton(sp => new PgStore(
            sp.GetRequiredService<PgDataSources>(), sp.GetRequiredService<PgRunner>(), sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IRunwickStore>(sp => sp.GetRequiredService<PgStore>());
    }
}
