using System.Net;
using System.Security.Claims;
using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>DASH-01 two mounts and two apps; DASH-02 a host already using Inertia.Net; MGMT-07 mounts and the API side by side.</summary>
[Trait("Item", "SP-024")]
public sealed class MountIsolationTests
{
    /// <summary>Mount <c>a</c>: Basic only, admin has every permission. Mount <c>b</c>: Basic and Cookies, admin is an Operator. Rebuilt assets on b.</summary>
    static void TwoMounts(WebApplicationBuilder b)
    {
        b.Services.AddRunwickDashboard("a", o => o.AuthenticationSchemes(RunwickBasicDefaults.Scheme)
            .MapGrants(p => p.FindFirstValue(ClaimTypes.NameIdentifier) == Users.Admin ? Users.All : []).Title = "Alpha");
        b.Services.AddRunwickDashboard("b", o =>
        {
            o.AuthenticationSchemes(RunwickBasicDefaults.Scheme, "Cookies")
                .MapGrants(p => p.FindFirstValue(ClaimTypes.NameIdentifier) switch { Users.Admin => ManagementRoles.Operator, Users.Viewer => ManagementRoles.Viewer, _ => [] })
                .Title = "Bravo";
            o.Assets = Rebuild(DashboardAssets.Embedded);
        });
    }

    [Fact]
    [Trait("Case", "DASH-01")]
    public async Task Two_mounts_in_one_app_keep_principals_props_and_tokens_apart()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash01"));
        await using var h = await DashHost.StartAsync(f, services: TwoMounts, map: false, before: a =>
        {
            a.MapRunwickDashboard("/ops/a", "a");
            a.MapRunwickDashboard("/ops/b", "b");
        });
        var job = await h.DispatchAsync(new PingJob("x"));

        var admin = h.Browser(Users.Admin);
        var (ra, bodyA, pageA) = await admin.LoadAsync("/ops/a");
        Assert.Equal(HttpStatusCode.OK, ra.StatusCode);
        Assert.Contains("<title>Alpha</title>", bodyA, StringComparison.Ordinal);
        Assert.Contains("path=/ops/a", ra.Header("Set-Cookie"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("rwk.a.csrf", ra.Header("Set-Cookie"), StringComparison.Ordinal);
        var tokenA = admin.Csrf!;
        var cookiesA = admin.Cookies.ToDictionary();

        var adminB = h.Browser(Users.Admin);
        var (rb, bodyB, pageB) = await adminB.LoadAsync("/ops/b");
        Assert.Equal(HttpStatusCode.OK, rb.StatusCode);
        Assert.Contains("<title>Bravo</title>", bodyB, StringComparison.Ordinal);
        Assert.NotEqual(pageA.GetProperty("version").GetString(), pageB.GetProperty("version").GetString());
        Assert.True(pageA.Props().GetProperty("can").GetProperty("retentionManage").GetBoolean());
        Assert.False(pageB.Props().GetProperty("can").GetProperty("retentionManage").GetBoolean());
        Assert.DoesNotContain("Alpha", pageB.GetRawText(), StringComparison.Ordinal);
        Assert.DoesNotContain("Bravo", pageA.GetRawText(), StringComparison.Ordinal);
        Assert.Contains(Rebuild(DashboardAssets.Embedded).Entry, bodyB, StringComparison.Ordinal);
        Assert.DoesNotContain(Rebuild(DashboardAssets.Embedded).Entry, bodyA, StringComparison.Ordinal);

        // A principal of B only (a cookie viewer) is challenged by A; a Basic viewer with no grant on A is forbidden.
        var cookieViewer = h.Browser();
        await cookieViewer.SendAsync(HttpMethod.Get, "/login/viewer", inertia: false);
        Assert.Equal(HttpStatusCode.OK, (await cookieViewer.LoadAsync("/ops/b")).Response.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await cookieViewer.SendAsync(HttpMethod.Get, "/ops/a", inertia: false)).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await h.Browser(Users.Viewer).SendAsync(HttpMethod.Get, "/ops/a", inertia: false)).StatusCode);

        // A's cookie and token sent to B by the same principal: 403, no audit row.
        foreach (var (name, value) in cookiesA)
        {
            adminB.Cookies[name] = value;
        }

        adminB.Csrf = tokenA;
        var position = new { expectedGeneration = 1, expectedRevision = (await SecurityTests.JobAsync(admin, job, "/ops/a")).GetProperty("revision").GetInt64() };
        Assert.Equal(HttpStatusCode.Forbidden, (await adminB.PostAsync($"/ops/b/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);
        Assert.Equal(0, await h.AuditsAsync());

        var monitor = h.App.Services.GetRequiredService<IOptionsMonitor<InertiaOptions>>();
        Assert.Equal(".Inertia.State.runwick-a", monitor.Get("runwick-a").State.CookieName);
        Assert.Equal(".Inertia.State.runwick-b", monitor.Get("runwick-b").State.CookieName);
        Assert.Equal(DashboardAssets.Embedded.Version, monitor.Get("runwick-a").Version);
        Assert.Same(h.App.Services.GetRequiredKeyedService<DashboardMount>("a"), h.App.Services.GetRequiredKeyedService<IInertiaRootView>("runwick-a"));
        Assert.Same(h.App.Services.GetRequiredKeyedService<DashboardMount>("b"), h.App.Services.GetRequiredKeyedService<IInertiaRootView>("runwick-b"));
    }

    [Fact]
    [Trait("Case", "DASH-01")]
    public async Task Two_apps_in_one_process_at_the_same_path_do_not_share_props_or_tokens()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash01p"));
        await using var g = await DriverFixture.StartAsync(Ns("dash01q"));
        await using var first = await DashHost.StartAsync(f, mount: o => o.Title = "First");
        await using var second = await DashHost.StartAsync(g, mount: o => o.Title = "Second");
        var job = await first.DispatchAsync(new PingJob("x"));

        var one = first.Browser(Users.Admin);
        var (_, body1, _) = await one.LoadAsync("/runwick");
        var two = second.Browser(Users.Admin);
        var (_, body2, _) = await two.LoadAsync("/runwick");
        Assert.Contains("<title>First</title>", body1, StringComparison.Ordinal);
        Assert.Contains("<title>Second</title>", body2, StringComparison.Ordinal);

        // Same mount id, cookie names and (on this machine) Data Protection key ring; the token of one app is refused by the other.
        var position = SecurityTests.Position(await SecurityTests.JobAsync(one, job));
        foreach (var (name, value) in one.Cookies)
        {
            two.Cookies[name] = value;
        }

        two.Csrf = one.Csrf;
        Assert.Equal(HttpStatusCode.Forbidden, (await two.PostAsync($"/runwick/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);
        Assert.Equal(0, await first.AuditsAsync());
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_already_using_Inertia_keeps_its_responses_byte_identical()
    {
        using var root = new HostRoot();
        await using var f = await DriverFixture.StartAsync(Ns("dash02"));
        var without = await HostPagesAsync(f, root, dashboard: false);
        var with = await HostPagesAsync(f, root, dashboard: true);
        Assert.Equal(without, with);
        Assert.Contains("host-secret-user", with[1], StringComparison.Ordinal); // the host's shared prop still reaches host pages
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task Dashboard_pages_in_an_Inertia_host_carry_no_host_shared_props_version_or_root_view()
    {
        using var root = new HostRoot();
        await using var f = await DriverFixture.StartAsync(Ns("dash02b"));
        await using var h = await InertiaHostAsync(f, root, dashboard: true, dashboardFirst: true);
        var job = await h.DispatchAsync(new PingJob("x"));
        var browser = h.Browser(Users.Admin);
        var (response, body, page) = await browser.LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain("host-secret-user", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<title>host</title>", body, StringComparison.Ordinal);
        Assert.NotEqual("host-1", page.GetProperty("version").GetString());
        var visit = await browser.VisitAsync($"/runwick/jobs/{job}");
        Assert.Equal(HttpStatusCode.OK, visit.StatusCode);
        Assert.DoesNotContain("hostUser", await visit.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task Host_UseInertia_before_the_mount_answers_dashboard_visits_with_its_own_version()
    {
        // The documented order rule (21 §2, not enforced): the host's global middleware version-checks every Inertia GET it sees,
        // so placed first it sends a dashboard client into a reload loop. This test pins that failure mode.
        using var root = new HostRoot();
        await using var f = await DriverFixture.StartAsync(Ns("dash02o"));
        await using var h = await InertiaHostAsync(f, root, dashboard: true, dashboardFirst: false);
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync("/runwick");
        var visit = await browser.VisitAsync("/runwick/queues");
        Assert.Equal(HttpStatusCode.Conflict, visit.StatusCode);
        Assert.Equal("host-1", visit.Header("X-Inertia-Version"));
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_JSON_naming_policy_does_not_change_dashboard_prop_names()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash02j"));
        await using var h = await DashHost.StartAsync(f, services: b =>
            b.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseUpper));
        var job = await h.DispatchAsync(new PingJob("x"));
        var (_, _, page) = await h.Browser(Users.Admin).LoadAsync($"/runwick/jobs/{job}");
        var detail = page.Props().GetProperty("job").GetProperty("job");
        Assert.Equal(job, detail.GetProperty("jobId").GetGuid());
        Assert.Equal(1, detail.GetProperty("generation").GetInt32());
        Assert.True(page.Props().TryGetProperty("can", out _));
    }

    [Fact]
    [Trait("Case", "DASH-02")]
    public async Task A_host_catch_all_fallback_does_not_capture_the_mount()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash02f"));
        await using var h = await DashHost.StartAsync(f, routes: a => a.MapFallback(() => Results.Text("host-spa")));
        var browser = h.Browser(Users.Admin);
        Assert.Equal("host-spa", await (await browser.SendAsync(HttpMethod.Get, "/anything", inertia: false)).Content.ReadAsStringAsync());
        var (response, _, page) = await browser.LoadAsync("/runwick/queues");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Queues", page.GetProperty("component").GetString());
        Assert.Equal(HttpStatusCode.NotFound, (await browser.VisitAsync("/runwick/nope")).StatusCode);
    }

    [Fact]
    [Trait("Case", "MGMT-07")]
    public async Task Two_mounts_and_the_API_with_different_grants_answer_parallel_browsers_apart()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt07"));
        await using var h = await DashHost.StartAsync(f, api: true,
            services: b => b.Services.AddRunwickDashboard("view", o => o.MapGrants(_ => ManagementRoles.Viewer).Title = "Viewing"),
            before: a => a.MapRunwickDashboard("/ops/view", "view"),
            routes: a => a.MapGet("/ops/other", () => "sibling"));
        var job = await h.DispatchAsync(new PingJob("x"));

        async Task<(string Where, bool Cancel, string Title)> OneAsync(int i)
        {
            var browser = h.Browser(Users.Admin);
            if (i % 3 == 2)
            {
                var api = await browser.SendAsync(HttpMethod.Get, "/api/v1/jobs", inertia: false);
                Assert.Equal(HttpStatusCode.OK, api.StatusCode);
                var json = await Browser.JsonAsync(api);
                Assert.False(json.TryGetProperty("can", out _));
                Assert.Contains(json.GetProperty("items").EnumerateArray(), j => j.GetProperty("jobId").GetGuid() == job);
                return ("api", true, "");
            }

            var path = i % 3 == 0 ? "/runwick" : "/ops/view";
            var (_, body, page) = await browser.LoadAsync(path);
            return (path, page.Props().GetProperty("can").GetProperty("cancel").GetBoolean(),
                body.Contains("<title>Viewing</title>", StringComparison.Ordinal) ? "Viewing" : "Runwick");
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 30).Select(OneAsync));
        Assert.All(results.Where(r => r.Where == "/runwick"), r => Assert.Equal((true, "Runwick"), (r.Cancel, r.Title)));
        Assert.All(results.Where(r => r.Where == "/ops/view"), r => Assert.Equal((false, "Viewing"), (r.Cancel, r.Title)));

        // The viewer mount refuses the control the default mount allows the same principal.
        var viewer = h.Browser(Users.Admin);
        await viewer.LoadAsync("/ops/view");
        var position = SecurityTests.Position(await SecurityTests.JobAsync(viewer, job, "/ops/view"));
        Assert.Equal(HttpStatusCode.Forbidden, (await viewer.PostAsync($"/ops/view/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);
        Assert.Equal(0, await h.AuditsAsync());
        Assert.Equal("sibling", await (await viewer.SendAsync(HttpMethod.Get, "/ops/other", inertia: false)).Content.ReadAsStringAsync());
    }

    static async Task<string[]> HostPagesAsync(DriverFixture f, HostRoot root, bool dashboard)
    {
        await using var h = await InertiaHostAsync(f, root, dashboard, dashboardFirst: true);
        var browser = h.Browser();
        var html = await browser.SendAsync(HttpMethod.Get, "/", inertia: false);
        browser.Version = "host-1";
        var json = await browser.VisitAsync("/");
        browser.Version = "old";
        var stale = await browser.VisitAsync("/");
        static string Describe(HttpResponseMessage r) =>
            $"{(int)r.StatusCode} {r.Content.Headers.ContentType} vary={r.Header("Vary")} loc={r.Header("X-Inertia-Location")} cc={r.Header("Cache-Control")}";
        return [Describe(html), await html.Content.ReadAsStringAsync(), Describe(json), await json.Content.ReadAsStringAsync(), Describe(stale),
            await stale.Content.ReadAsStringAsync()];
    }

    static Task<DashHost> InertiaHostAsync(DriverFixture f, HostRoot root, bool dashboard, bool dashboardFirst) =>
        DashHost.StartAsync(f, contentRoot: root.Path, dashboard: dashboard, map: false,
            services: b => b.Services.AddInertia(o =>
            {
                o.Version = "host-1";
                o.Share("hostUser", _ => "host-secret-user");
            }),
            before: a =>
            {
                if (dashboard && dashboardFirst)
                {
                    a.MapRunwickDashboard("/runwick");
                }

                a.UseInertia();
                if (dashboard && !dashboardFirst)
                {
                    a.MapRunwickDashboard("/runwick");
                }

                a.MapGet("/", () => Inertia.Net.Inertia.Render("Home", new InertiaProps { ["greeting"] = "hello from the host" }));
            });

    /// <summary>A content root holding the host's own Inertia root view.</summary>
    sealed class HostRoot : IDisposable
    {
        public HostRoot()
        {
            Path = Directory.CreateTempSubdirectory("sp024-host-").FullName;
            File.WriteAllText(System.IO.Path.Combine(Path, "app.html"), "<!DOCTYPE html><html><head><title>host</title></head><body>@inertia</body></html>");
        }

        public string Path { get; }

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
