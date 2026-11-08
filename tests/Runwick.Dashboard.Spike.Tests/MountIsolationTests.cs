using System.Net;
using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using static Runwick.Dashboard.Spike.Tests.Support;

namespace Runwick.Dashboard.Spike.Tests;

/// <summary>DASH-01 two hosts and two mounts; DASH-02 a host already using Inertia.Net 1.5.3.</summary>
[Trait("Item", "SP-023")]
public sealed class MountIsolationTests
{
    [Fact]
    [Trait("Case", "DASH-01")]
    public async Task Two_mounts_in_one_host_keep_principals_props_and_tokens_apart()
    {
        await using var app = await StartAsync(
            b =>
            {
                b.Services.AddBasic("basic-a");
                b.Services.AddBasicOnly("basic-b", "bob");
                b.Services.AddDashboard("ops-a", "basic-a", Data("alpha"));
                b.Services.AddDashboard("ops-b", "basic-b", Data("bravo"));
            },
            a =>
            {
                a.UseRunwickDashboardSpike("/ops-a", "ops-a");
                a.UseRunwickDashboardSpike("/ops-b", "ops-b");
            });

        var alice = app.Browser("alice");
        var (pageA, _, propsA) = await alice.LoadAsync("/ops-a");
        Assert.Equal(HttpStatusCode.OK, pageA.StatusCode);
        Assert.Equal("ops-a", propsA.GetProperty("props").GetProperty("mount").GetString());
        Assert.Equal("alpha", propsA.GetProperty("props").GetProperty("jobs")[0].GetProperty("name").GetString());
        Assert.Contains("path=/ops-a", pageA.Header("Set-Cookie"), StringComparison.OrdinalIgnoreCase);

        // alice exists only in mount A's scheme.
        Assert.Equal(HttpStatusCode.Unauthorized, (await alice.SendAsync(HttpMethod.Get, "/ops-b", inertia: false)).StatusCode);

        var bob = app.Browser("bob");
        var (_, _, propsB) = await bob.LoadAsync("/ops-b");
        Assert.Equal("bravo", propsB.GetProperty("props").GetProperty("jobs")[0].GetProperty("name").GetString());
        Assert.DoesNotContain("alpha", propsB.GetRawText(), StringComparison.Ordinal);

        // A's token and cookie, replayed against B by an operator of B, are refused: different cookie name and protector purpose.
        bob.Csrf = alice.Csrf;
        foreach (var (name, value) in alice.Cookies)
        {
            await bob.SendAsync(HttpMethod.Get, "/ops-b", inertia: false, configure: r => r.Headers.Add("Cookie", $"{name}={value}"));
        }

        Assert.Equal(HttpStatusCode.Forbidden, (await bob.PostAsync("/ops-b/jobs/1/retry", revision: 1)).StatusCode);
    }

    [Fact]
    [Trait("Case", "DASH-01")]
    public async Task Two_hosts_in_one_process_with_the_same_mount_do_not_share_props_or_tokens()
    {
        await using var first = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data("first")); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        await using var second = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data("second")); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));

        var one = first.Browser("alice");
        var (_, _, page) = await one.LoadAsync("/runwick");
        Assert.Equal("first", page.GetProperty("props").GetProperty("jobs")[0].GetProperty("name").GetString());

        var two = second.Browser("alice");
        var (_, _, other) = await two.LoadAsync("/runwick");
        Assert.Equal("second", other.GetProperty("props").GetProperty("jobs")[0].GetProperty("name").GetString());

        // Same mount id and cookie name, but each host has its own Data Protection keys.
        two.Csrf = one.Csrf;
        Assert.Equal(HttpStatusCode.Forbidden, (await two.PostAsync("/runwick/jobs/1/retry", revision: 1)).StatusCode);
    }

    [Fact]
    [Trait("Case", "DASH-01")]
    public async Task Each_mount_is_its_own_Inertia_Net_named_mount_and_leaves_the_default_alone()
    {
        var services = new ServiceCollection();
        services.AddInertia(o => o.Version = "host-1");
        services.AddBasic("basic");
        services.AddDashboard("ops-a", "basic", Data());
        services.AddDashboard("ops-b", "basic", Data(), assets: Rebuilt());
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<InertiaOptions>>();
        var (a, b) = (monitor.Get("runwick-ops-a"), monitor.Get("runwick-ops-b"));
        Assert.Equal("host-1", provider.GetRequiredService<IOptions<InertiaOptions>>().Value.Version);
        Assert.Equal(DashboardAssets.Embedded.Version, a.Version);
        Assert.NotEqual(a.Version, b.Version);
        Assert.Equal(".Inertia.State.runwick-ops-a", a.State.CookieName);
        Assert.Equal(".Inertia.State.runwick-ops-b", b.State.CookieName);
        Assert.Same(provider.GetRequiredKeyedService<DashboardMount>("ops-a"), provider.GetRequiredKeyedService<IInertiaRootView>("runwick-ops-a"));
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_already_using_Inertia_keeps_its_routes_unchanged()
    {
        using var root = new HostRoot();
        var without = await HostPagesAsync(root, withDashboard: false);
        var with = await HostPagesAsync(root, withDashboard: true);
        Assert.Equal(without, with);
        Assert.Contains("host-secret-user", with[1], StringComparison.Ordinal); // the host's shared prop still reaches host pages
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task Dashboard_pages_in_an_Inertia_host_carry_no_host_shared_props_version_or_root_view()
    {
        using var root = new HostRoot();
        await using var app = await InertiaHostAsync(root, withDashboard: true, dashboardFirst: true);
        var browser = app.Browser("alice");
        var (response, body, page) = await browser.LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain("host-secret-user", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>host</title>", body, StringComparison.Ordinal);
        Assert.NotEqual("host-1", page.GetProperty("version").GetString());

        var visit = await browser.VisitAsync("/runwick/jobs/1");
        Assert.Equal(HttpStatusCode.OK, visit.StatusCode);
        Assert.DoesNotContain("hostUser", await visit.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task Host_UseInertia_before_the_mount_answers_dashboard_visits_with_its_own_version()
    {
        // The documented ordering rule: the host's global middleware version-checks every Inertia GET it sees, so placed first
        // it sends a dashboard client into a reload loop. This test pins that failure mode.
        using var root = new HostRoot();
        await using var app = await InertiaHostAsync(root, withDashboard: true, dashboardFirst: false);
        var browser = app.Browser("alice");
        await browser.LoadAsync("/runwick");
        var visit = await browser.VisitAsync("/runwick/jobs/1");
        Assert.Equal(HttpStatusCode.Conflict, visit.StatusCode);
        Assert.Equal("host-1", visit.Header("X-Inertia-Version"));
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_JSON_naming_policy_reaches_named_mounts_but_not_the_dashboards_props()
    {
        // Remaining gap in 1.7.0: a named mount serializes props with the host's HttpJsonOptions. The plain "evidence" mount shows
        // it; the dashboard passes JsonElements written with its own options, so its prop names stay as the client expects.
        await using var app = await StartAsync(
            b =>
            {
                b.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseUpper);
                b.Services.AddInertia("evidence", null);
                b.Services.AddBasic("basic");
                b.Services.AddDashboard("default", "basic", Data());
            },
            a =>
            {
                a.UseRunwickDashboardSpike("/runwick", "default");
                a.Map("/evidence", e =>
                {
                    e.UseInertia("evidence");
                    e.Run(c => Inertia.Net.Inertia.Render("Job", new InertiaProps { ["job"] = new SpikeJob("1", "n", "s", 1, 0) }).ExecuteAsync(c));
                });
            });
        var browser = app.Browser("alice");
        var evidence = await Browser.JsonAsync(await browser.VisitAsync("/evidence"));
        Assert.Equal("n", evidence.GetProperty("props").GetProperty("job").GetProperty("NAME").GetString());

        var (_, _, page) = await browser.LoadAsync("/runwick/jobs/1");
        Assert.Equal("alpha", page.GetProperty("props").GetProperty("job").GetProperty("name").GetString());
        Assert.Equal(1, page.GetProperty("props").GetProperty("job").GetProperty("revision").GetInt64());
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_catch_all_fallback_does_not_capture_the_mount()
    {
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data()); }, a =>
        {
            a.UseRunwickDashboardSpike("/runwick", "default");
            a.MapFallback(() => Results.Text("host-spa"));
        });
        var browser = app.Browser("alice");
        Assert.Equal("host-spa", await (await browser.SendAsync(HttpMethod.Get, "/anything", inertia: false)).Content.ReadAsStringAsync());
        var (response, _, page) = await browser.LoadAsync("/runwick/jobs/1");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Job", page.GetProperty("component").GetString());
    }

    private static async Task<string[]> HostPagesAsync(HostRoot root, bool withDashboard)
    {
        await using var app = await InertiaHostAsync(root, withDashboard, dashboardFirst: true);
        var browser = app.Browser();
        var html = await browser.SendAsync(HttpMethod.Get, "/", inertia: false);
        browser.Version = "host-1";
        var json = await browser.VisitAsync("/");
        var stale = await browser.VisitAsync("/", r => r.Headers.Remove("X-Inertia-Version"));
        string Describe(HttpResponseMessage r) => $"{(int)r.StatusCode} {r.Content.Headers.ContentType} vary={r.Header("Vary")} loc={r.Header("X-Inertia-Location")}";
        return [Describe(html), await html.Content.ReadAsStringAsync(), Describe(json), await json.Content.ReadAsStringAsync(), Describe(stale)];
    }

    private static Task<WebApplication> InertiaHostAsync(HostRoot root, bool withDashboard, bool dashboardFirst, Action<WebApplication>? extra = null) =>
        StartAsync(
            b =>
            {
                b.Services.AddInertia(o =>
                {
                    o.Version = "host-1";
                    o.Share("hostUser", _ => "host-secret-user");
                });
                if (withDashboard)
                {
                    b.Services.AddBasic("basic");
                    b.Services.AddDashboard("default", "basic", Data());
                }
            },
            a =>
            {
                if (withDashboard && dashboardFirst)
                {
                    a.UseRunwickDashboardSpike("/runwick", "default");
                }

                a.UseInertia();
                if (withDashboard && !dashboardFirst)
                {
                    a.UseRunwickDashboardSpike("/runwick", "default");
                }

                a.MapGet("/", () => Inertia.Net.Inertia.Render("Home", new InertiaProps { ["greeting"] = "hello from the host" }));
                extra?.Invoke(a);
            },
            root.Path);

    /// <summary>A content root holding the host's own Inertia root view.</summary>
    private sealed class HostRoot : IDisposable
    {
        public HostRoot()
        {
            Path = Directory.CreateTempSubdirectory("sp023-host-").FullName;
            File.WriteAllText(System.IO.Path.Combine(Path, "app.html"), "<!DOCTYPE html><html><head><title>host</title></head><body>@inertia</body></html>");
        }

        public string Path { get; }

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }

    private static DashboardAssets Rebuilt()
    {
        var old = DashboardAssets.Embedded;
        var files = old.Files.ToDictionary();
        files[old.Entry] = [.. old.Files[old.Entry], .. "\n// v2"u8.ToArray()];
        return new DashboardAssets(files);
    }
}
