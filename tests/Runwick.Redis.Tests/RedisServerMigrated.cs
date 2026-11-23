using Runwick.Harness.Store;

[assembly: AssemblyFixture(typeof(Runwick.Redis.Tests.RedisServerMigrated))]

namespace Runwick.Redis.Tests;

/// <summary>
/// Migrates the shared test server once before any test class starts. On a brand-new empty server, classes that write rw:* keys
/// directly (diagnostics, admin reads) could otherwise run before the first fixture migrated, and every later migration then
/// refused with schema_not_empty.
/// </summary>
public sealed class RedisServerMigrated : IAsyncLifetime
{
    public async ValueTask InitializeAsync() => await RedisServer.EnsureMigratedAsync();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
