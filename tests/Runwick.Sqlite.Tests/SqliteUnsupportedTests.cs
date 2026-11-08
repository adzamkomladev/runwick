using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Sqlite.Store;

namespace Runwick.Sqlite.Tests;

/// <summary>
/// Declared-unsupported SQLite topologies fail with an explicit code (20 §2.3, §13: one local file on one host).
/// Both cases carry the <c>Unsupported</c> trait so the conformance report can tell them from ordinary passes.
/// </summary>
[Trait("Item", "SP-028")]
[Trait("Unsupported", "sqlite")]
[Collection(nameof(SqliteSeams))]
public sealed class SqliteUnsupportedTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    [Trait("Case", "UNS-SQ-01")]
    public async Task A_store_file_on_a_network_filesystem_fails_startup_with_topology_unsupported()
    {
        await using var f = await DriverFixture.StartAsync("uns_sq_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var dir = Path.GetFullPath(Path.GetDirectoryName(f.SqlitePath)!);
        // Only this fixture's directory is a pretend NFS mount; every other path keeps resolving to the local root.
        var mountInfo = $"22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n40 22 0:35 / {dir} rw,relatime shared:20 - nfs4 server:/export rw,vers=4.2\n";
        SqFile.MountInfoOverride = () => mountInfo;
        try
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => f.OpenClient(configure: r => TestJobs.Register(r)));
            Assert.Contains("topology_unsupported", ex.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            SqFile.MountInfoOverride = null;
        }
    }

    [Theory]
    [Trait("Case", "UNS-SQ-02")]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source=x.db;Mode=Memory")]
    [InlineData("Data Source=x.db;Cache=Shared")]
    public async Task In_memory_and_shared_cache_stores_are_refused_at_startup_with_invalid_policy(string cs)
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("uns");
            TestJobs.Register(r);
            r.UseSqlite(cs);
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains("invalid_policy", ex.ToString(), StringComparison.Ordinal);
    }
}
