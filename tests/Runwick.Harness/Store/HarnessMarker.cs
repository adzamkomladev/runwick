using System.Runtime.CompilerServices;

namespace Runwick.Harness.Store;

/// <summary>
/// Marks every process that loads the harness as a test binary (PF-05): its stores may run against the fault-hooks
/// template, which refuses production binaries with <c>schema_incompatible</c> / <c>fault_hooks</c>.
/// </summary>
internal static class HarnessMarker
{
#pragma warning disable CA2255 // Intended: every test process that loads the harness is marked before any store opens.
    [ModuleInitializer]
    internal static void Initialize()
    {
        Runwick.Postgres.Store.PgRunner.HarnessMarker = true;
        Runwick.Sqlite.Store.SqRunner.HarnessMarker = true;
        Runwick.Redis.Store.RxClient.HarnessMarker = true;
    }
#pragma warning restore CA2255
}
