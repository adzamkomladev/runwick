using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Postgres;

/// <summary>Driver service registration; filled in by SP-005 (store, runner, migrator).</summary>
internal static partial class PostgresServices
{
    public static void Register(IServiceCollection services) => RegisterStore(services);

    static partial void RegisterStore(IServiceCollection services);
}
