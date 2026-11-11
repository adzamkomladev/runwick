using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Runwick.Harness.Store;
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

        Assert.Equal(HttpStatusCode.Forbidden, (await scoped.LoadAsync("/runwick/store")).Response.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await scoped.VisitAsync("/api/v1/store")).StatusCode);

        var ready = await WaitForSnapshotAsync(viewer, "Reachable");
        var id = ready.GetProperty("identity").GetProperty("value").GetProperty("storeId").GetGuid();
        Assert.Equal("postgres", ready.GetProperty("provider").GetString());
        Assert.Equal("Available", ready.GetProperty("admission").GetProperty("status").GetString());
        Assert.Equal("Unavailable", ready.GetProperty("serverVersion").GetProperty("status").GetString());

        f.Proxy!.DropEverything = true;
        var failed = await WaitForSnapshotAsync(viewer, "Unavailable", clock);
        Assert.Equal("Stale", failed.GetProperty("identity").GetProperty("status").GetString());
        Assert.Equal(id, failed.GetProperty("identity").GetProperty("value").GetProperty("storeId").GetGuid());
        Assert.Equal(ready.GetProperty("lastSuccessfulAt").GetString(), failed.GetProperty("lastSuccessfulAt").GetString());
        var (response, body, page) = await viewer.LoadAsync("/runwick/store");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Store", page.GetProperty("component").GetString());
        Assert.Equal("Stale", page.Props().GetProperty("snapshot").GetProperty("identity").GetProperty("status").GetString());
        Assert.DoesNotContain("Password", body, StringComparison.OrdinalIgnoreCase);

        clock.Advance(TimeSpan.FromMinutes(5));
        var expired = await SnapshotAsync(viewer);
        Assert.Equal("Unavailable", expired.GetProperty("identity").GetProperty("status").GetString());
        Assert.False(expired.GetProperty("identity").TryGetProperty("value", out _));
        Assert.Equal(failed.GetProperty("lastSuccessfulAt").GetString(), expired.GetProperty("lastSuccessfulAt").GetString());
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
        Assert.Equal("Unsupported", snapshot.GetProperty("topology").GetProperty("status").GetString());
        Assert.Equal("Unavailable", snapshot.GetProperty("recovery").GetProperty("status").GetString());
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
