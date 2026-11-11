using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

public sealed class StoreSnapshotTests
{
    [Fact]
    public async Task An_outage_preserves_only_a_bounded_last_known_snapshot()
    {
        await using var f = await DriverFixture.StartAsync(Ns("storehealth"), StoreProfile.DirectPrimaryViaProxy);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var h = await DashHost.StartAsync(f, services: b => b.Services.AddSingleton<TimeProvider>(clock), api: true);
        var viewer = h.Browser(Users.Viewer);
        var scoped = h.Browser(Users.Scoped);
        var admin = h.Browser(Users.Admin);

        Assert.Equal(HttpStatusCode.Forbidden, (await scoped.LoadAsync("/runwick/store")).Response.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await scoped.VisitAsync("/api/v1/store")).StatusCode);

        var ready = await WaitForSnapshotAsync(viewer, "Reachable");
        Assert.Equal("PermissionDenied", ready.GetProperty("trendStatus").GetString());
        Assert.False(ready.TryGetProperty("trend", out _));
        var check = Assert.Single(ready.GetProperty("recentChecks").EnumerateArray());
        Assert.Equal("HostCompatibilityCheck", check.GetProperty("source").GetString());
        Assert.Equal("Reachable", check.GetProperty("status").GetString());
        Assert.True(check.GetProperty("observedAt").GetDateTimeOffset() <= ready.GetProperty("lastSuccessfulAt").GetDateTimeOffset());
        var id = ready.GetProperty("identity").GetProperty("value").GetProperty("storeId").GetGuid();
        Assert.Equal("postgres", ready.GetProperty("provider").GetString());
        Assert.Equal("Available", ready.GetProperty("admission").GetProperty("status").GetString());
        var version = ready.GetProperty("serverVersion");
        Assert.Equal("Available", version.GetProperty("status").GetString());
        Assert.Equal("ProviderNode", version.GetProperty("scope").GetString());
        Assert.True(Version.TryParse(version.GetProperty("value").GetString(), out _));
        var denied = ready.GetProperty("postgres").GetProperty("databaseSize");
        Assert.Equal("PermissionDenied", denied.GetProperty("status").GetString());
        Assert.False(denied.TryGetProperty("value", out _));
        Assert.False(ready.GetProperty("postgres").TryGetProperty("sampledAt", out _));

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        JsonElement stats;
        do
        {
            stats = (await SnapshotAsync(admin)).GetProperty("postgres");
            if (stats.GetProperty("databaseSize").GetProperty("status").GetString() == "Available") break;
            await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token);
            clock.Advance(TimeSpan.FromSeconds(10));
        }
        while (true);
        var sampledAt = stats.GetProperty("sampledAt").GetDateTimeOffset();
        var size = stats.GetProperty("databaseSize").GetProperty("value").GetProperty("bytes").GetInt64();
        Assert.True(size > 0);
        Assert.True(stats.GetProperty("sessions").GetProperty("value").GetProperty("connectedSessions").GetInt32() > 0);
        Assert.True(stats.GetProperty("jobsTable").GetProperty("value").GetProperty("totalBytes").GetInt64() > 0);
        Assert.Equal("ProviderNode", stats.GetProperty("wal").GetProperty("scope").GetString());
        Assert.True(stats.GetProperty("wal").GetProperty("value").GetProperty("bytesGenerated").GetDecimal() >= 0);
        Assert.False(stats.GetProperty("recovery").GetProperty("value").GetProperty("inRecovery").GetBoolean());

        h.Grants[Users.Admin] = [.. Users.All.Where(g => g.Permission != ManagementPermissions.StoreDiagnosticsRead)];
        var revokedStats = (await SnapshotAsync(admin)).GetProperty("postgres");
        var revoked = revokedStats.GetProperty("databaseSize");
        Assert.Equal("PermissionDenied", revoked.GetProperty("status").GetString());
        Assert.False(revoked.TryGetProperty("value", out _));
        Assert.False(revokedStats.TryGetProperty("sampledAt", out _));
        h.Grants[Users.Admin] = Users.All;
        var lastSuccessfulAt = (await SnapshotAsync(admin)).GetProperty("lastSuccessfulAt").GetString();

        f.Proxy!.DropEverything = true;
        var failed = await WaitForSnapshotAsync(admin, "Unavailable", clock);
        Assert.Equal(["Reachable", "Unavailable"],
            failed.GetProperty("recentChecks").EnumerateArray().Select(c => c.GetProperty("status").GetString()));
        Assert.Equal("PostgresLockWaits", failed.GetProperty("trend").GetProperty("metric").GetString());
        Assert.False(failed.GetProperty("trend").GetProperty("samples").EnumerateArray().Last().TryGetProperty("value", out _));
        Assert.Equal("Stale", failed.GetProperty("identity").GetProperty("status").GetString());
        Assert.Equal(id, failed.GetProperty("identity").GetProperty("value").GetProperty("storeId").GetGuid());
        Assert.Equal("Stale", failed.GetProperty("serverVersion").GetProperty("status").GetString());
        Assert.Equal(version.GetProperty("value").GetString(), failed.GetProperty("serverVersion").GetProperty("value").GetString());
        var staleStats = failed.GetProperty("postgres").GetProperty("databaseSize");
        Assert.Equal("Stale", staleStats.GetProperty("status").GetString());
        Assert.Equal(size, staleStats.GetProperty("value").GetProperty("bytes").GetInt64());
        Assert.Equal(sampledAt, failed.GetProperty("postgres").GetProperty("sampledAt").GetDateTimeOffset());
        Assert.Equal(lastSuccessfulAt, failed.GetProperty("lastSuccessfulAt").GetString());
        var (response, body, page) = await viewer.LoadAsync("/runwick/store");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Store", page.GetProperty("component").GetString());
        Assert.Equal("Stale", page.Props().GetProperty("snapshot").GetProperty("identity").GetProperty("status").GetString());
        Assert.DoesNotContain("Password", body, StringComparison.OrdinalIgnoreCase);

        clock.Advance(TimeSpan.FromMinutes(5));
        var expired = await SnapshotAsync(admin);
        Assert.Equal("Unavailable", expired.GetProperty("identity").GetProperty("status").GetString());
        Assert.False(expired.GetProperty("identity").TryGetProperty("value", out _));
        Assert.Equal("Unavailable", expired.GetProperty("serverVersion").GetProperty("status").GetString());
        Assert.False(expired.GetProperty("serverVersion").TryGetProperty("value", out _));
        var expiredStats = expired.GetProperty("postgres").GetProperty("databaseSize");
        Assert.Equal("Unavailable", expiredStats.GetProperty("status").GetString());
        Assert.False(expiredStats.TryGetProperty("value", out _));
        Assert.False(expired.GetProperty("postgres").TryGetProperty("sampledAt", out _));
        Assert.Equal(failed.GetProperty("lastSuccessfulAt").GetString(), expired.GetProperty("lastSuccessfulAt").GetString());
        Assert.False(expired.TryGetProperty("trend", out _));

        f.Proxy.DropEverything = false;
        var recovered = await WaitForSnapshotAsync(admin, "Reachable", clock);
        Assert.Equal(["Reachable", "Unavailable", "Reachable"],
            recovered.GetProperty("recentChecks").EnumerateArray().Select(c => c.GetProperty("status").GetString()));
    }

    [Fact]
    public async Task Sqlite_reports_observed_profile_verification_and_unsupported_topology()
    {
        await using var f = await DriverFixture.StartAsync(Ns("storeprofile"), StoreKind.Sqlite);
        await using var h = await DashHost.StartAsync(f, api: true);
        var snapshot = await WaitForSnapshotAsync(h.Browser(Users.Viewer), "Reachable");

        Assert.Equal("sqlite", snapshot.GetProperty("provider").GetString());
        var profile = snapshot.GetProperty("persistence").GetProperty("value");
        Assert.Equal("Available", profile.GetProperty("verification").GetString());
        var version = snapshot.GetProperty("serverVersion");
        Assert.Equal("Available", version.GetProperty("status").GetString());
        Assert.Equal("Process", version.GetProperty("scope").GetString());
        Assert.True(Version.TryParse(version.GetProperty("value").GetString(), out _));
        Assert.Equal("Unsupported", snapshot.GetProperty("topology").GetProperty("status").GetString());
        Assert.Equal("Unavailable", snapshot.GetProperty("recovery").GetProperty("status").GetString());

        var denied = snapshot.GetProperty("sqlite");
        foreach (var name in new[] { "databaseFile", "walFile", "writer" })
        {
            var fact = denied.GetProperty(name);
            Assert.Equal("PermissionDenied", fact.GetProperty("status").GetString());
            Assert.False(fact.TryGetProperty("value", out _));
        }

        var admin = h.Browser(Users.Admin);
        var adminSnapshot = await SnapshotAsync(admin);
        var trend = adminSnapshot.GetProperty("trend");
        Assert.Equal("SqliteWalBytes", trend.GetProperty("metric").GetString());
        Assert.Equal("Store", trend.GetProperty("scope").GetString());
        Assert.True(trend.GetProperty("samples").EnumerateArray().Last().GetProperty("value").GetInt64() >= 0);
        var details = adminSnapshot.GetProperty("sqlite");
        Assert.True(details.GetProperty("databaseFile").GetProperty("value").GetProperty("bytes").GetInt64() > 0);
        Assert.True(details.GetProperty("walFile").GetProperty("value").GetProperty("bytes").GetInt64() >= 0);
        var writer = details.GetProperty("writer");
        Assert.Equal("Process", writer.GetProperty("scope").GetString());
        Assert.True(writer.GetProperty("value").GetProperty("busyConflicts").GetInt64() >= 0);

        h.Grants[Users.Admin] = [.. Users.All.Where(g => g.Permission != ManagementPermissions.StoreDiagnosticsRead)];
        var revokedSnapshot = await SnapshotAsync(admin);
        Assert.Equal("PermissionDenied", revokedSnapshot.GetProperty("trendStatus").GetString());
        Assert.False(revokedSnapshot.TryGetProperty("trend", out _));
        var revoked = revokedSnapshot.GetProperty("sqlite").GetProperty("databaseFile");
        Assert.Equal("PermissionDenied", revoked.GetProperty("status").GetString());
        Assert.False(revoked.TryGetProperty("value", out _));
    }

    [Fact]
    public async Task Redis_diagnostics_are_sampled_and_require_an_explicit_grant()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RUNWICK_TEST_REDIS")))
        {
            Assert.Skip("Set RUNWICK_TEST_REDIS to exercise the Redis Store snapshot.");
        }

        await using var f = await DriverFixture.StartAsync(Ns("storeredis"), StoreKind.Redis);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var h = await DashHost.StartAsync(f, services: b => b.Services.AddSingleton<TimeProvider>(clock), api: true);
        var admin = h.Browser(Users.Admin);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        JsonElement stats;
        while (true)
        {
            stats = (await SnapshotAsync(admin)).GetProperty("redis");
            if (stats.TryGetProperty("sampledAt", out _)) break;
            await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token);
            clock.Advance(TimeSpan.FromSeconds(10));
        }

        var memory = stats.GetProperty("memory");
        Assert.Equal("ProviderNode", memory.GetProperty("scope").GetString());
        Assert.True(memory.GetProperty("value").GetProperty("usedBytes").GetInt64() > 0);
        Assert.True(stats.GetProperty("activity").GetProperty("value").GetProperty("connectedClients").GetInt64() > 0);
        Assert.True(stats.GetProperty("aof").GetProperty("value").GetProperty("enabled").GetBoolean());
        Assert.Equal("Primary", stats.GetProperty("replication").GetProperty("value").GetProperty("role").GetString());

        var trend = (await SnapshotAsync(admin)).GetProperty("trend");
        Assert.Equal("RedisMemoryUsedBytes", trend.GetProperty("metric").GetString());
        Assert.Equal("ProviderNode", trend.GetProperty("scope").GetString());
        Assert.Contains(trend.GetProperty("samples").EnumerateArray(),
            sample => sample.TryGetProperty("value", out var value) && value.GetInt64() > 0);

        h.Grants[Users.Admin] = [.. Users.All.Where(g => g.Permission != ManagementPermissions.StoreDiagnosticsRead)];
        var revoked = (await SnapshotAsync(admin)).GetProperty("redis");
        Assert.False(revoked.TryGetProperty("sampledAt", out _));
        foreach (var name in new[] { "memory", "activity", "aof", "replication" })
        {
            Assert.Equal("PermissionDenied", revoked.GetProperty(name).GetProperty("status").GetString());
            Assert.False(revoked.GetProperty(name).TryGetProperty("value", out _));
        }
    }

    static async Task<JsonElement> WaitForSnapshotAsync(Browser browser, string connection, FakeTimeProvider? clock = null)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (true)
        {
            var value = await SnapshotAsync(browser);
            if (value.GetProperty("connection").GetProperty("status").GetString() == connection)
            {
                return value;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token);
            clock?.Advance(TimeSpan.FromSeconds(10));
        }
    }

    static async Task<JsonElement> SnapshotAsync(Browser browser)
    {
        using var response = await browser.VisitAsync("/api/v1/store");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        return json.RootElement.Clone();
    }
}
