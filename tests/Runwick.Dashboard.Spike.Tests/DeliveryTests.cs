using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); }, a =>
        {
            a.UsePathBase("/ops");
            a.UseRunwickDashboardSpike("/runwick", o => o.Use("default", "basic", Data()));
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
                b.Services.AddRunwickDashboardSpike();
                b.Services.AddBasic("basic");
                b.Services.Configure<ForwardedHeadersOptions>(o =>
                {
                    o.ForwardedHeaders = ForwardedHeaders.XForwardedPrefix | ForwardedHeaders.XForwardedProto;
                    o.KnownProxies.Add(IPAddress.Parse("10.0.0.1"));
                });
            },
            a =>
            {
                a.UseForwardedHeaders();
                a.UseRunwickDashboardSpike("/runwick", o => o.Use("default", "basic", Data()));
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
        Assert.Contains($"\"base\":\"{expectedBase}\"", body, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-05")]
    public async Task Redirects_stay_inside_the_mount_whatever_the_id()
    {
        var data = new InMemoryDashboardSpikeData().Add("\\\\evil.example", "odd", "p"); // "\\evil.example": browsers read /\\host as //host
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); },
            a => a.UseRunwickDashboardSpike("/runwick", o => o.Use("default", "basic", data)));
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
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); },
            a => a.UseRunwickDashboardSpike("/runwick", o => { o.Use("default", "basic", Data()); o.Assets = rebuilt; }));

        var browser = app.Browser("alice");
        browser.Version = old.Version; // a tab loaded before the deploy
        var visit = await browser.VisitAsync("/runwick/jobs/1?x=1");
        Assert.Equal(HttpStatusCode.Conflict, visit.StatusCode);
        Assert.Equal("/runwick/jobs/1?x=1", visit.Header("X-Inertia-Location"));
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
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); },
            a => a.UseRunwickDashboardSpike("/runwick", o => { o.Use("default", "basic", data); o.Assets = Rebuild(DashboardAssets.Embedded); }));
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
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); },
            a => a.UseRunwickDashboardSpike("/runwick", o => o.Use("default", "basic", data)));
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
        await using var app = await StartAsync(b => { b.Services.AddRunwickDashboardSpike(); b.Services.AddBasic("basic"); },
            a => a.UseRunwickDashboardSpike("/runwick", o => o.Use("default", "basic", Data())));
        Assert.Equal(HttpStatusCode.OK, (await app.Browser("alice").LoadAsync("/runwick")).Response.StatusCode);
        Assert.DoesNotContain(app.Services.GetServices<IHostedService>(), s => s.GetType().Assembly.GetName().Name!.StartsWith("Runwick", StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    public void Embedding_adds_no_global_behaviour()
    {
        var builder = WebApplication.CreateSlimBuilder();
        var before = builder.Services.ToList();
        builder.Services.AddRunwickDashboardSpike();
        var descriptors = builder.Services.Except(before).ToList();
        var added = descriptors.Select(d => d.ServiceType).ToList();

        // A host without Data Protection gets its key-ring hosted service; that is the only hosted service added.
        Assert.All(descriptors.Where(d => d.ServiceType == typeof(IHostedService)), d =>
            Assert.StartsWith("Microsoft.AspNetCore.DataProtection", d.ImplementationType!.Namespace, StringComparison.Ordinal));
        added.RemoveAll(t => t == typeof(IHostedService));
        Assert.All(added, type => Assert.True(
            type.Namespace!.StartsWith("Microsoft.AspNetCore.DataProtection", StringComparison.Ordinal)
            || type.Namespace.StartsWith("Microsoft.AspNetCore.Authorization", StringComparison.Ordinal)
            || type.Namespace.StartsWith("Microsoft.AspNetCore.Cryptography", StringComparison.Ordinal)
            || type.Namespace.StartsWith("Microsoft.Extensions.Options", StringComparison.Ordinal),
            $"unexpected global registration {type}"));
        Assert.DoesNotContain(added, t => t == typeof(IStartupFilter));
    }

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData("/", "default", true)]
    [InlineData("/Runwick", "default", true)]
    [InlineData("/runwick/", "default", true)]
    [InlineData("/runwick", "Bad Id", true)]
    [InlineData("/runwick", "default", false)]
    public void Invalid_or_unauthorized_mounts_are_refused_at_startup(string path, string mountId, bool withPolicies)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddRunwickDashboardSpike();
        var app = builder.Build();
        Assert.ThrowsAny<Exception>(() => app.UseRunwickDashboardSpike(path, o =>
        {
            o.Use(mountId, "basic", Data());
            if (!withPolicies)
            {
                o.ViewPolicy = null;
            }
        }));
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
