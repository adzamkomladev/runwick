using System.Net;
using System.Reflection;
using System.Text.Json;
using Inertia.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>DASH-05 path base and proxies, DASH-06 upgrades and ambiguous controls, DASH-07 standalone and refusals, MGMT-08, MGMT-12.</summary>
[Trait("Item", "SP-024")]
public sealed class DeliveryTests
{
    [Fact]
    [Trait("Case", "DASH-05")]
    [Trait("Case", "MGMT-08")]
    public async Task PathBase_reaches_asset_urls_page_urls_cookie_paths_and_redirects()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash05"));
        await using var h = await DashHost.StartAsync(f, before: a => a.UsePathBase("/ops"));
        var job = await h.DispatchAsync(new PingJob("x"));
        var browser = h.Browser(Users.Admin);
        var (response, body, page) = await browser.LoadAsync($"/ops/runwick/jobs/{job}?tab=a%20b");
        Assert.Contains($"src=\"/ops/runwick/{DashboardAssets.Embedded.Entry}\"", body, StringComparison.Ordinal);
        Assert.Equal($"/ops/runwick/jobs/{job}?tab=a%20b", page.GetProperty("url").GetString());
        Assert.Equal("/ops/runwick", page.Props().GetProperty("base").GetString());
        Assert.Contains("path=/ops/runwick", response.Header("Set-Cookie"), StringComparison.OrdinalIgnoreCase);

        var cancel = await browser.PostAsync($"/ops/runwick/jobs/{job}/cancel", SecurityTests.Position(page.Props().GetProperty("job").GetProperty("job")),
            Guid.NewGuid());
        Assert.True(cancel.StatusCode == HttpStatusCode.SeeOther, await cancel.Content.ReadAsStringAsync());
        Assert.Equal($"/ops/runwick/jobs/{job}", cancel.Headers.Location!.OriginalString);
        var queueRevision = (await browser.VisitJsonAsync("/ops/runwick/queues")).Props().GetProperty("queues").GetProperty("items").EnumerateArray()
            .Single(q => q.GetProperty("name").GetString() == "default").GetProperty("revision").GetInt64();
        var queue = await browser.PostAsync("/ops/runwick/queues/default/pause", new { expectedRevision = queueRevision }, Guid.NewGuid());
        Assert.True(queue.StatusCode == HttpStatusCode.SeeOther, await queue.Content.ReadAsStringAsync());
        Assert.Equal("/ops/runwick/queues", queue.Headers.Location!.OriginalString);

        // A control that is refused renders the resource's page; its URL is the resource's, with the prefix.
        var stale = await browser.PostAsync("/ops/runwick/queues/default/resume", new { expectedRevision = 99 }, Guid.NewGuid());
        Assert.Equal(HttpStatusCode.OK, stale.StatusCode);
        Assert.Equal("/ops/runwick/queues", (await Browser.JsonAsync(stale)).GetProperty("url").GetString());
    }

    [Theory]
    [Trait("Case", "DASH-05")]
    [Trait("Case", "MGMT-08")]
    [InlineData("10.0.0.1", "/edge/runwick", 200)]
    [InlineData("203.0.113.9", "/runwick", 401)]
    public async Task Only_a_known_proxy_prefix_and_scheme_are_applied(string remote, string expectedBase, int status)
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash05p"));
        await using var h = await DashHost.StartAsync(f,
            services: b => b.Services.Configure<ForwardedHeadersOptions>(o =>
            {
                o.ForwardedHeaders = ForwardedHeaders.XForwardedPrefix | ForwardedHeaders.XForwardedProto;
                o.KnownProxies.Add(IPAddress.Parse("10.0.0.1"));
            }),
            before: a => a.UseForwardedHeaders(),
            mount: o => o.AuthenticationSchemes(RunwickBasicDefaults.Scheme));

        // Plain HTTP at the server: Basic is accepted only when a known proxy says the client used HTTPS.
        var context = await h.App.GetTestServer().SendAsync(c =>
        {
            c.Connection.RemoteIpAddress = IPAddress.Parse(remote);
            c.Request.Scheme = "http";
            c.Request.Path = "/runwick";
            c.Request.Headers["X-Forwarded-Prefix"] = "/edge";
            c.Request.Headers["X-Forwarded-Proto"] = "https";
            c.Request.Headers.Authorization = "Basic " + Convert.ToBase64String("admin:pw-admin"u8.ToArray());
        }, TestContext.Current.CancellationToken);
        var body = await new StreamReader(context.Response.Body).ReadToEndAsync(TestContext.Current.CancellationToken);
        Assert.Equal(status, context.Response.StatusCode);
        if (status == 200)
        {
            var page = JsonDocument.Parse(PageScript().Match(body).Groups[1].Value).RootElement;
            Assert.Equal(expectedBase, page.Props().GetProperty("base").GetString());
            Assert.Contains($"src=\"{expectedBase}/", body, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("data-page", body, StringComparison.Ordinal);
        }
    }

    [Fact]
    [Trait("Case", "MGMT-08")]
    public async Task Basic_over_plain_HTTP_is_refused_on_every_path()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt08"));
        await using var h = await DashHost.StartAsync(f, mount: o => o.AuthenticationSchemes(RunwickBasicDefaults.Scheme));
        var job = await h.DispatchAsync(new PingJob("secret-payload"));
        var plain = h.Browser(Users.Admin, https: false);
        foreach (var (method, path, inertia) in new[]
                 {
                     (HttpMethod.Get, "/runwick", false), (HttpMethod.Get, $"/runwick/jobs/{job}", true), (HttpMethod.Post, $"/runwick/jobs/{job}/cancel", true),
                     (HttpMethod.Post, "/runwick/jobs:cancel", true), (HttpMethod.Post, $"/runwick/jobs/{job}/payload:reveal", false),
                 })
        {
            var response = await plain.SendAsync(method, path, inertia, "{}");
            Assert.True(response.StatusCode == HttpStatusCode.Unauthorized, $"{method} {path}: {response.StatusCode}");
            Assert.DoesNotContain(job.ToString(), await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        }

        Assert.Equal(HttpStatusCode.OK, (await h.Browser(Users.Admin).LoadAsync("/runwick")).Response.StatusCode);
    }

    [Fact]
    [Trait("Case", "DASH-05")]
    public async Task Redirects_stay_inside_the_mount_whatever_the_id()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash05r"));
        await using var h = await DashHost.StartAsync(f);
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync("/runwick");

        // IDs are GUIDs and names follow the store's identifier grammar, so "\\evil.example" never reaches a Location header:
        // it is not a route (404) or not a known queue (404/400), and nothing redirects off the mount.
        foreach (var path in new[] { "/runwick/jobs/%5C%5Cevil.example/cancel", "/runwick/queues/%5C%5Cevil.example/pause", "/runwick/schedules/%5C%5Cevil.example/trigger" })
        {
            var response = await browser.PostAsync(path, new { expectedRevision = 0, expectedGeneration = 1 }, Guid.NewGuid());
            // 404/400, or 200 with a not_found outcome for a schedule key: never a redirect.
            Assert.True(response.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.BadRequest or HttpStatusCode.OK, $"{path}: {response.StatusCode}");
            Assert.Null(response.Headers.Location);
        }

        // Every 303 the dashboard writes is a path under the mount.
        var job = await h.DispatchAsync(new PingJob("x"));
        var cancel = await browser.PostAsync($"/runwick/jobs/{job}/cancel", SecurityTests.Position(await SecurityTests.JobAsync(browser, job)), Guid.NewGuid());
        Assert.StartsWith("/runwick/", cancel.Headers.Location!.OriginalString, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-06")]
    public async Task A_new_build_sends_old_clients_a_full_reload_and_drops_old_assets()
    {
        var old = DashboardAssets.Embedded;
        var rebuilt = Rebuild(old);
        Assert.NotEqual(old.Version, rebuilt.Version);
        await using var f = await DriverFixture.StartAsync(Ns("dash06"));
        await using var h = await DashHost.StartAsync(f, mount: o => o.Assets = rebuilt);
        var browser = h.Browser(Users.Admin);
        browser.Version = old.Version; // a tab loaded before the deploy
        var visit = await browser.VisitAsync("/runwick/queues?x=1");
        Assert.Equal(HttpStatusCode.Conflict, visit.StatusCode);
        Assert.Equal("https://localhost/runwick/queues?x=1", visit.Header("X-Inertia-Location")); // absolute
        Assert.Empty(await visit.Content.ReadAsByteArrayAsync());

        Assert.Equal(HttpStatusCode.NotFound, (await browser.SendAsync(HttpMethod.Get, "/runwick/" + old.Entry, inertia: false)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await browser.SendAsync(HttpMethod.Get, "/runwick/" + rebuilt.Entry, inertia: false)).StatusCode);
        var (_, body, page) = await browser.LoadAsync("/runwick/queues");
        Assert.Equal(rebuilt.Version, page.GetProperty("version").GetString());
        Assert.Contains(rebuilt.Entry, body, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-06")]
    public async Task A_retry_sent_by_an_old_client_runs_once_and_is_not_replayed_by_the_reload()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash06r"));
        await using var h = await DashHost.StartAsync(f, mount: o => o.Assets = Rebuild(DashboardAssets.Embedded));
        var job = await h.DispatchAsync(new PingJob("x"));
        await f.FailNextAsync("boom", "m");
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync($"/runwick/jobs/{job}");
        var position = SecurityTests.Position(await SecurityTests.JobAsync(browser, job));
        browser.Version = DashboardAssets.Embedded.Version; // the old build

        var id = Guid.NewGuid();
        var post = await browser.PostAsync($"/runwick/jobs/{job}/retry", position, id);
        Assert.Equal(HttpStatusCode.SeeOther, post.StatusCode);
        var follow = await browser.VisitAsync(post.Headers.Location!.OriginalString);
        Assert.Equal(HttpStatusCode.Conflict, follow.StatusCode); // the client reloads with a GET; nothing resends the POST
        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync($"/runwick/jobs/{job}/retry", position, id)).StatusCode); // same command ID
        Assert.Equal(1, await h.AuditsAsync(id));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE job_id = @j", ("j", job)));
    }

    [Fact]
    [Trait("Case", "DASH-06")]
    public async Task Stale_revision_and_outcome_unknown_answer_200_with_the_outcome_and_the_command_id()
    {
        var unknown = new UnknownOutcome();
        await using var f = await DriverFixture.StartAsync(Ns("dash06u"));
        await using var h = await DashHost.StartAsync(f, services: b => unknown.Install(b.Services));
        var job = await h.DispatchAsync(new PingJob("x"));
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync($"/runwick/jobs/{job}");
        var detail = await SecurityTests.JobAsync(browser, job);

        var staleId = Guid.NewGuid();
        var stale = await browser.PostAsync($"/runwick/jobs/{job}/cancel",
            new { expectedGeneration = 1, expectedRevision = detail.GetProperty("revision").GetInt64() + 5 }, staleId);
        AssertOutcome(stale, "stale_revision", staleId, $"/runwick/jobs/{job}");
        Assert.Equal(detail.GetProperty("revision").GetInt64(), (await Browser.JsonAsync(stale)).Props().GetProperty("outcome").GetProperty("actualRevision").GetInt64());

        var unknownId = Guid.NewGuid();
        unknown.CommandId = unknownId;
        var ambiguous = await browser.PostAsync($"/runwick/jobs/{job}/cancel", SecurityTests.Position(detail), unknownId);
        AssertOutcome(ambiguous, "outcome_unknown", unknownId, $"/runwick/jobs/{job}");

        // The client's "Try again" resends the same ID and body; once the session answers, it is one transition and one audit row.
        unknown.CommandId = null;
        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync($"/runwick/jobs/{job}/cancel", SecurityTests.Position(detail), unknownId)).StatusCode);
        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync($"/runwick/jobs/{job}/cancel", SecurityTests.Position(detail), unknownId)).StatusCode);
        Assert.Equal(1, await h.AuditsAsync(unknownId));
        Assert.Equal(1, await h.AuditsAsync());
    }

    internal static void AssertOutcome(HttpResponseMessage response, string code, Guid commandId, string url)
    {
        // Not a redirect (which would look completed) and not a 409 (the asset-version signal).
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("true", response.Header("X-Inertia"));
        Assert.Null(response.Header("X-Inertia-Location"));
        var page = Browser.JsonAsync(response).GetAwaiter().GetResult();
        var outcome = page.Props().GetProperty("outcome");
        Assert.Equal(code, outcome.GetProperty("code").GetString());
        Assert.Equal(commandId, outcome.GetProperty("commandId").GetGuid());
        Assert.Equal(url, page.GetProperty("url").GetString());
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    [Trait("Case", "MGMT-12")]
    public async Task Standalone_host_serves_every_page_and_control_like_the_API()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash07"));
        await using var h = await DashHost.StartAsync(f, api: true);
        var a = await h.DispatchAsync(new PingJob("a"));
        var b = await h.DispatchAsync(new PingJob("b"));
        var browser = h.Browser(Users.Admin);
        foreach (var page in new[] { "/runwick", "/runwick/jobs", $"/runwick/jobs/{a}", "/runwick/queues", "/runwick/workers", "/runwick/schedules",
                     "/runwick/scaling", "/runwick/settings", "/runwick/retention", "/runwick/audit" })
        {
            Assert.True((await browser.LoadAsync(page)).Response.StatusCode == HttpStatusCode.OK, page);
        }

        // The same reads: the Jobs page and GET /v1/jobs list the same jobs in the same order.
        var dashJobs = (await browser.VisitJsonAsync("/runwick/jobs")).Props().GetProperty("jobs").GetProperty("items").EnumerateArray()
            .Select(j => j.GetProperty("jobId").GetGuid()).ToList();
        var apiJobs = (await Browser.JsonAsync(await browser.SendAsync(HttpMethod.Get, "/api/v1/jobs", inertia: false))).GetProperty("items").EnumerateArray()
            .Select(j => j.GetProperty("jobId").GetGuid()).ToList();
        Assert.Equal(apiJobs, dashJobs);
        Assert.Equal(2, dashJobs.Count);

        // The same controls: cancel one job from each; the audit rows match apart from the target and command.
        var positionA = SecurityTests.Position(await SecurityTests.JobAsync(browser, a));
        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync($"/runwick/jobs/{a}/cancel", positionA, Guid.NewGuid())).StatusCode);
        var pb = await SecurityTests.JobAsync(browser, b);
        var antiforgery = await Browser.JsonAsync(await browser.SendAsync(HttpMethod.Get, "/api/v1/antiforgery", inertia: false));
        var api = await browser.SendAsync(HttpMethod.Post, $"/api/v1/jobs/{b}/cancel", inertia: false, JsonSerializer.Serialize(new
        {
            commandId = Guid.NewGuid(), expectedGeneration = pb.GetProperty("generation").GetInt32(), expectedRevision = pb.GetProperty("revision").GetInt64(),
        }), r => r.Headers.Add(antiforgery.GetProperty("headerName").GetString()!, antiforgery.GetProperty("token").GetString()));
        Assert.Equal(HttpStatusCode.OK, api.StatusCode);
        Assert.Equal("cancelled", (await SecurityTests.JobAsync(browser, a)).GetProperty("state").GetString());
        Assert.Equal("cancelled", (await SecurityTests.JobAsync(browser, b)).GetProperty("state").GetString());
        Assert.Equal(1, await f.CountAsync("SELECT count(DISTINCT (operation, actor_subject, actor_scheme)) FROM runwick.management_audit"));
        Assert.Equal(2, await h.AuditsAsync());

        // Standalone: no worker presence, and every hosted service comes from the web host, Inertia.Net, Data Protection or Runwick's own
        // host service (registered by AddRunwick; it starts no worker here). The dashboard assembly adds none.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.worker_instances"));
        var hosted = h.App.Services.GetServices<IHostedService>().Select(s => s.GetType()).ToList();
        Assert.All(hosted, t => Assert.True(t.Namespace!.StartsWith("Inertia.Net", StringComparison.Ordinal)
            || t.Namespace.StartsWith("Microsoft.AspNetCore.DataProtection", StringComparison.Ordinal)
            || t.Namespace.StartsWith("Microsoft.AspNetCore.Hosting", StringComparison.Ordinal)
            || t.Namespace.StartsWith("Microsoft.Extensions.Hosting", StringComparison.Ordinal)
            || t.FullName == "Runwick.Hosting.RunwickHostedService", t.FullName));
        Assert.DoesNotContain(hosted, t => t.Assembly == typeof(DashboardOptions).Assembly);
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    public async Task Embedding_adds_no_startup_filter_and_leaves_the_host_defaults_alone()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddRunwickManagement(m => m.AuthenticationSchemes("Cookies").MapGrants(_ => ManagementRoles.Viewer));
        var before = builder.Services.ToList();
        builder.Services.AddRunwickDashboard("default", _ => { });
        var added = builder.Services.Except(before).ToList();
        Assert.DoesNotContain(added, d => d.ServiceType == typeof(IStartupFilter));
        Assert.DoesNotContain(added, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType?.Assembly == typeof(DashboardOptions).Assembly);
        await using var app = builder.Build();
        Assert.Null(app.Services.GetRequiredService<IOptions<InertiaOptions>>().Value.Version); // the host's default mount untouched
        Assert.DoesNotContain(added, d => d.ServiceType == typeof(IConfigureOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>)
            || d.ServiceType == typeof(IConfigureOptions<Microsoft.AspNetCore.Antiforgery.AntiforgeryOptions>));
    }

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData("Bad Id")]
    [InlineData("")]
    [InlineData("a-very-long-mount-id-that-is-over-32")]
    public void Invalid_mount_ids_are_refused_at_registration(string mountId) =>
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddRunwickDashboard(mountId, _ => { }));

    [Fact]
    [Trait("Case", "DASH-07")]
    public void A_mount_id_registered_twice_is_refused() =>
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddRunwickDashboard("x", _ => { }).AddRunwickDashboard("x", _ => { }));

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData("/", "default", typeof(ArgumentException))]
    [InlineData("/Runwick", "default", typeof(ArgumentException))]
    [InlineData("/runwick/", "default", typeof(ArgumentException))]
    [InlineData("runwick", "default", typeof(ArgumentException))]
    [InlineData("/runwick", "unregistered", typeof(InvalidOperationException))]
    public void Invalid_paths_and_unknown_mounts_are_refused_when_mapping(string path, string mountId, Type exception)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddRunwickManagement(m => m.AuthenticationSchemes("Cookies").MapGrants(_ => ManagementRoles.Viewer));
        builder.Services.AddRunwickDashboard("default", _ => { });
        var app = builder.Build();
        Assert.Throws(exception, () => app.MapRunwickDashboard(path, mountId));
    }

    [Theory]
    [Trait("Case", "DASH-07")]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void Without_management_schemes_or_grants_mapping_is_refused(bool management, bool schemes, bool grants)
    {
        var builder = WebApplication.CreateSlimBuilder();
        if (management)
        {
            builder.Services.AddRunwickManagement(m =>
            {
                if (schemes)
                {
                    m.AuthenticationSchemes("Cookies");
                }

                if (grants)
                {
                    m.MapGrants(_ => ManagementRoles.Viewer);
                }
            });
        }

        builder.Services.AddRunwickDashboard("default", _ => { });
        var app = builder.Build();
        Assert.Throws<InvalidOperationException>(() => app.MapRunwickDashboard("/runwick"));
    }

    [Fact]
    [Trait("Case", "DASH-07")]
    public async Task A_host_route_under_the_mount_stops_the_app_and_the_mount_never_serves()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash07c"));
        var logs = new LogSink();
        await using var h = await DashHost.StartAsync(f, services: b => b.Logging.AddProvider(logs),
            routes: a => a.MapGet("/runwick/health", () => "shadowed"));
        var lifetime = h.App.Services.GetRequiredService<IHostApplicationLifetime>();
        Assert.True(lifetime.ApplicationStopping.IsCancellationRequested);
        Assert.Contains(logs.Lines, l => l.Contains("/runwick/health", StringComparison.Ordinal) && l.Contains("dashboard mount", StringComparison.Ordinal));
        var response = await h.Browser(Users.Admin).SendAsync(HttpMethod.Get, "/runwick", inertia: false);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("starting", (await Browser.JsonAsync(response)).GetProperty("code").GetString());
    }

    /// <summary>
    /// Wraps the registered <see cref="IManagementSession"/> so that a chosen command ID raises <see cref="ControlOutcomeUnknownException"/>,
    /// as the session does once a command's NotAfter passes with no answer from the store.
    /// </summary>
    internal sealed class UnknownOutcome
    {
        public Guid? CommandId { get; set; }

        public void Install(IServiceCollection services)
        {
            var inner = services.Last(d => d.ServiceType == typeof(IManagementSession));
            services.Remove(inner);
            services.AddSingleton(sp =>
            {
                var session = (IManagementSession)inner.ImplementationFactory!(sp);
                var proxy = DispatchProxy.Create<IManagementSession, Proxy>();
                ((Proxy)(object)proxy).Init(session, this);
                return proxy;
            });
        }

#pragma warning disable CA1852 // DispatchProxy derives from it at run time
        public class Proxy : DispatchProxy
#pragma warning restore CA1852
        {
            IManagementSession _inner = null!;
            UnknownOutcome _owner = null!;

            internal void Init(IManagementSession inner, UnknownOutcome owner) => (_inner, _owner) = (inner, owner);

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                if (_owner.CommandId is { } id && args!.Any(a => a is Guid g && g == id || a is JobControlRequest r && r.CommandId == id))
                {
                    var tcs = typeof(Task).GetMethod(nameof(Task.FromException), 1, [typeof(Exception)])!
                        .MakeGenericMethod(targetMethod!.ReturnType.GetGenericArguments()[0]);
                    return tcs.Invoke(null, [new ControlOutcomeUnknownException(id)]);
                }

                try
                {
                    return targetMethod!.Invoke(_inner, args);
                }
                catch (TargetInvocationException ex) when (ex.InnerException is not null)
                {
                    throw ex.InnerException;
                }
            }
        }
    }
}
