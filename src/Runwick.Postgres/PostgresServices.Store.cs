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
        services.AddSingleton<IJobControlStore>(sp => sp.GetRequiredService<PgStore>());
        services.AddSingleton<IRateControlStore>(sp => sp.GetRequiredService<PgStore>());
        services.AddSingleton<IReadinessDiagnostics>(sp => sp.GetRequiredService<PgStore>());
        services.AddSingleton<IOutboxDestinationStore>(sp => sp.GetRequiredService<PgStore>());
        services.AddSingleton<IScheduleStore>(sp => sp.GetRequiredService<PgStore>());
        services.AddSingleton<IScalingStore>(sp => sp.GetRequiredService<PgStore>());
        services.AddScoped<IPostgresJobCompletion, PostgresJobCompletion>();
        services.AddSingleton<IPostgresJobDispatcher, PostgresJobDispatcher>();
    }
}
