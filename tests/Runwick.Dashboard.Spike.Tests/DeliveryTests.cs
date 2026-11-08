using System.Net;
using Inertia.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using static Runwick.Dashboard.Spike.Tests.Support;

namespace Runwick.Dashboard.Spike.Tests;

/// <summary>DASH-05 path base, proxies and redirects; DASH-06 asset upgrades and ambiguous mutations; DASH-07 standalone/embedded.</summary>
[Trait("Item", "SP-023")]
public sealed class DeliveryTests
{
    [Fact]
    [Trait("Case", "DASH-05")]
    public async Task PathBase_reaches_asset_urls_page_urls_cookie_paths_and_redirects()
    {
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data()); }, a =>
        {
            a.UsePathBase("/ops");
            a.UseRunwickDashboardSpike("/runwick", "default");
        });
        var browser = app.Browser("alice");
        var (response, body, page) = await browser.LoadAsync("/ops/runwick/jobs/1?tab=a%20b");
        Assert.Contains($"src=\"/ops/runwick/{DashboardAssets.Embedded.Entry}\"", body, StringComparison.Ordinal);
        Assert.Equal("/ops/runwick/jobs/1?tab=a%20b", page.GetProperty("url").GetString());
        Assert.Equal("/ops/runwick", page.GetProperty("props").GetProperty("base").GetString());
        Assert.Contains("path=/ops/runwick", response.Header("Set-Cookie"), StringComparison.OrdinalIgnoreCase);

        var retry = await browser.PostAsync("/ops/runwick/jobs/1/retry", 1);
        Assert.Equal(HttpStatusCode.SeeOther, retry.StatusCode);
        Assert.Equal("/ops/runwick/jobs/1", retry.Headers.Location!.OriginalString);
    }

    [Theory]
    [Trait("Case", "DASH-05")]
    [InlineData("10.0.0.1", "/edge/runwick")]
    [InlineData("203.0.113.9", "/runwick")]
    public async Task Only_a_trusted_proxy_prefix_is_applied(string remote, string expectedBase)
    {
        await using var app = await StartAsync(
            b =>
            {
                b.Services.AddBasic("basic");
                b.Services.AddDashboard("default", "basic", Data());
                b.Services.Configure<ForwardedHeadersOptions>(o =>
                {
                    o.ForwardedHeaders = ForwardedHeaders.XForwardedPrefix | ForwardedHeaders.XForwardedProto;
                    o.KnownProxies.Add(IPAddress.Parse("10.0.0.1"));
                });
            },
            a =>
            {
                a.UseForwardedHeaders();
                a.UseRunwickDashboardSpike("/runwick", "default");
            });

        var context = await app.GetTestServer().SendAsync(c =>
        {
            c.Connection.RemoteIpAddress = IPAddress.Parse(remote);
            c.Request.Path = "/runwick";
            c.Request.Headers["X-Forwarded-Prefix"] = "/edge";
            c.Request.Headers.Authorization = "Basic " + Convert.ToBase64String("alice:pw-alice"u8.ToArray());
        }, TestContext.Current.CancellationToken);
        var body = await new StreamReader(context.Response.Body).ReadToEndAsync(TestContext.Current.CancellationToken);
        Assert.Equal(200, context.Response.StatusCode);
        var page = System.Text.Json.JsonDocument.Parse(PageScript().Match(body).Groups[1].Value).RootElement;
        Assert.Equal(expectedBase, page.GetProperty("props").GetProperty("base").GetString());
    }

    [Fact]
    [Trait("Case", "DASH-05")]
    public async Task Redirects_stay_inside_the_mount_whatever_the_id()
    {
        var data = new InMemoryDashboardSpikeData().Add("\\\\evil.example", "odd", "p"); // "\\evil.example": browsers read /\\host as //host
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", data); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        var browser = app.Browser("alice");
        await browser.LoadAsync("/runwick");
        var retry = await browser.PostAsync("/runwick/jobs/%5C%5Cevil.example/retry", 1);
        Assert.Equal(HttpStatusCode.SeeOther, retry.StatusCode);
        Assert.Equal("/runwick/jobs/%5C%5Cevil.example", retry.Headers.Location!.OriginalString);
    }

    [Fact]
    [Trait("Case", "DASH-06")]
    public async Task A_new_build_sends_old_clients_a_full_reload_and_drops_old_assets()
    {
        var old = DashboardAssets.Embedded;
        var rebuilt = Rebuild(old);
        Assert.NotEqual(old.Version, rebuilt.Version);
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data(), assets: rebuilt); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));

        var browser = app.Browser("alice");
        browser.Version = old.Version; // a tab loaded before the deploy
        var visit = await browser.VisitAsync("/runwick/jobs/1?x=1");
        Assert.Equal(HttpStatusCode.Conflict, visit.StatusCode);
        Assert.Equal("http://localhost/runwick/jobs/1?x=1", visit.Header("X-Inertia-Location")); // the adapter sends the absolute URL
        Assert.Empty(await visit.Content.ReadAsByteArrayAsync());

        Assert.Equal(HttpStatusCode.NotFound, (await browser.SendAsync(HttpMethod.Get, "/runwick/" + old.Entry, inertia: false)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await browser.SendAsync(HttpMethod.Get, "/runwick/" + rebuilt.Entry, inertia: false)).StatusCode);
        var (_, body, page) = await browser.LoadAsync("/runwick");
        Assert.Equal(rebuilt.Version, page.GetProperty("version").GetString());
        Assert.Contains(rebuilt.Entry, body, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-06")]
    public async Task A_mutation_sent_with_a_stale_version_runs_once_and_is_not_replayed_by_the_reload()
    {
        var data = Data();
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", data, assets: Rebuild(DashboardAssets.Embedded)); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        var browser = app.Browser("alice");
        await browser.LoadAsync("/runwick/jobs/1");
        browser.Version = DashboardAssets.Embedded.Version;

        var post = await browser.PostAsync("/runwick/jobs/1/retry", 1, "cmd-stale");
        Assert.Equal(HttpStatusCode.SeeOther, post.StatusCode);
        var follow = await browser.VisitAsync(post.Headers.Location!.OriginalString);
        Assert.Equal(HttpStatusCode.Conflict, follow.StatusCode); // the client reloads with a GET; nothing re-sends the POST
        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync("/runwick/jobs/1/retry", 1, "cmd-stale")).StatusCode); // same command id
        Assert.Equal(1, data.Applied);
    }

    [Theory]
    [Trait("Case", "DASH-06")]
    [InlineData(RetryOutcome.Unknown, "unknown")]
    [InlineData(RetryOutcome.Conflict, "conflict")]
    public async Task Ambiguous_or_conflicting_mutations_answer_with_the_page_and_keep_the_command_id(RetryOutcome forced, string status)
    {
        var data = Data();
        data.ForcedOutcome = forced;
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", data); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        var browser = app.Browser("alice");
        await browser.LoadAsync("/runwick/jobs/1");
        var response = await browser.PostAsync("/runwick/jobs/1/retry", 1, "cmd-42");

        // Not a redirect (which would look completed) and not a 409 (which is the asset-version signal).
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("true", response.Header("X-Inertia"));
        Assert.Null(response.Header("X-Inertia-Location"));
        var outcome = (await Browser.JsonAsync(response)).GetProperty("props").GetProperty("outcome");
        Assert.Equal(status, outcome.GetProperty("status").GetString());
        Assert.Equal("cmd-42", outcome.GetProperty("commandId").GetString());
        Assert.Equal(0, data.Applied);
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    public async Task Standalone_host_serves_the_dashboard_with_no_workers_or_hosted_services()
    {
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", Data()); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        Assert.Equal(HttpStatusCode.OK, (await app.Browser("alice").LoadAsync("/runwick")).Response.StatusCode);
        Assert.DoesNotContain(app.Services.GetServices<IHostedService>(), s => s.GetType().Assembly.GetName().Name!.StartsWith("Runwick", StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    public async Task Embedding_adds_no_startup_filter_and_only_idle_framework_hosted_services()
    {
        var builder = WebApplication.CreateSlimBuilder();
        var before = builder.Services.ToList();
        builder.Services.AddBasic("basic");
        builder.Services.AddDashboard("default", "basic", Data());
        var added = builder.Services.Except(before).ToList();
        Assert.DoesNotContain(added, d => d.ServiceType == typeof(IStartupFilter));

        // Inertia.Net's own (refresh queue; SSR and TypeScript writers that idle without configuration) and Data Protection's key ring.
        await using var app = builder.Build();
        var hosted = app.Services.GetServices<IHostedService>().Select(s => s.GetType().Namespace!).ToList();
        Assert.All(hosted, ns => Assert.True(ns.StartsWith("Inertia.Net", StringComparison.Ordinal)
            || ns.StartsWith("Microsoft.AspNetCore.DataProtection", StringComparison.Ordinal)
            || ns.StartsWith("Microsoft.AspNetCore.Hosting", StringComparison.Ordinal), ns)); // the web host itself
        Assert.Null(app.Services.GetRequiredService<IOptions<InertiaOptions>>().Value.Version); // the host's default mount untouched
    }

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData("Bad Id", true)]
    [InlineData("default", false)]
    public void Invalid_or_unauthorized_mounts_are_refused_at_registration(string mountId, bool withPolicies) =>
        Assert.ThrowsAny<Exception>(() => new ServiceCollection().AddRunwickDashboardSpike(mountId, o =>
        {
            o.Data = Data();
            o.ViewPolicy = withPolicies ? new AuthorizationPolicyBuilder("basic").RequireAuthenticatedUser().Build() : null;
            o.OperatePolicy = o.ViewPolicy;
        }));

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData("/", "default")]
    [InlineData("/Runwick", "default")]
    [InlineData("/runwick/", "default")]
    [InlineData("/runwick", "unregistered")]
    public void Invalid_paths_and_unknown_mounts_are_refused_when_mapping(string path, string mountId)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddBasic("basic");
        builder.Services.AddDashboard("default", "basic", Data());
        var app = builder.Build();
        Assert.ThrowsAny<Exception>(() => app.UseRunwickDashboardSpike(path, mountId));
    }

    /// <summary>The same files with the entry changed, as a rebuild produces: new hash, new file name, new version.</summary>
    private static DashboardAssets Rebuild(DashboardAssets old)
    {
        var entry = old.Entry.Replace(".js", "-v2.js", StringComparison.Ordinal);
        var files = old.Files.Where(f => f.Key != old.Entry && f.Key != "manifest.json").ToDictionary();
        files[entry] = [.. old.Files[old.Entry], .. "\n// v2"u8.ToArray()];
        files["manifest.json"] = System.Text.Encoding.UTF8.GetBytes(System.Text.Encoding.UTF8.GetString(old.Files["manifest.json"]).Replace(old.Entry, entry, StringComparison.Ordinal));
        return new DashboardAssets(files);
    }
}
