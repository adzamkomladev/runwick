using System.Net;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-01, 02, 07, 08: authentication, antiforgery, host isolation and forwarded headers (16 §5).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-020")]
public sealed class AuthenticationTests
{
    static readonly Guid Id = Guid.NewGuid();

    /// <summary>Every route of 16 §5 with a syntactically valid target.</summary>
    static readonly (HttpMethod Method, string Path)[] Routes =
    [
        (HttpMethod.Get, "antiforgery"), (HttpMethod.Get, "jobs"), (HttpMethod.Get, $"jobs/{Id}"),
        (HttpMethod.Post, $"jobs/{Id}/payload:reveal"), (HttpMethod.Post, $"attempts/{Id}/error:reveal"),
        (HttpMethod.Post, $"jobs/{Id}/cancel"), (HttpMethod.Post, $"jobs/{Id}/retry"), (HttpMethod.Post, $"jobs/{Id}/rerun"), (HttpMethod.Post, "jobs:cancel"),
        (HttpMethod.Get, "queues"), (HttpMethod.Post, "queues/default/pause"), (HttpMethod.Post, "queues/default/resume"),
        (HttpMethod.Get, "workers"), (HttpMethod.Get, "scopes"), (HttpMethod.Post, "scopes/queue/default/capacity"),
        (HttpMethod.Get, "schedules"), (HttpMethod.Post, "schedules/s/pause"), (HttpMethod.Post, "schedules/s/resume"),
        (HttpMethod.Post, "schedules/s/trigger"), (HttpMethod.Post, "schedules/s/revalidate"),
        (HttpMethod.Get, "audit"), (HttpMethod.Get, "settings"),
    ];

    static HttpRequestMessage Req(MgmtHost h, HttpMethod method, string path, string scheme = "https") =>
        new(method, $"{scheme}://localhost{h.Base}{path}")
        {
            Content = method == HttpMethod.Post ? new StringContent("{}", System.Text.Encoding.UTF8, "application/json") : null,
        };

    [Fact]
    [Trait("Case", "MGMT-01")]
    public async Task Every_route_needs_one_of_the_listed_schemes_and_the_host_is_unchanged()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt01"));
        await using var h = await MgmtHost.StartAsync(f, browser: true);
        Users.Seed(h);
        await h.DispatchAsync(new PingJob("secret-payload"));

        foreach (var (method, path) in Routes)
        {
            // No credentials; an unlisted scheme; a bad Basic password; a good Basic password over HTTP.
            var none = Req(h, method, path);
            var wrong = Req(h, method, path);
            wrong.Headers.Add("X-Test-User", Users.Admin);
            wrong.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "token");
            var bad = Req(h, method, path);
            bad.Headers.Authorization = MgmtHost.Basic("mallory", "wrong");
            var plain = Req(h, method, path, "http");
            plain.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
            foreach (var request in new[] { none, wrong, bad, plain })
            {
                var (status, body, response) = await h.SendAsync(request);
                Assert.True(status == HttpStatusCode.Unauthorized, $"{method} {path}: {status}");
                Assert.Equal("unauthenticated", body!["code"]!.GetValue<string>());
                Assert.Equal(2, body.AsObject().Count);
                Assert.Null(response.Headers.Location);
                Assert.Equal("Basic", response.Headers.WwwAuthenticate.Single().Scheme);
            }
        }

        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));

        // Failures are limited per source and user: mallory, after 22 failures, is refused even with the right password.
        var limited = Req(h, HttpMethod.Get, "settings");
        limited.Headers.Authorization = MgmtHost.Basic("mallory", "pw-mallory");
        Assert.Equal(HttpStatusCode.Unauthorized, (await h.SendAsync(limited)).Status);

        // The good password over HTTPS works; the host's default cookie scheme still redirects its own routes.
        var ok = Req(h, HttpMethod.Get, "settings");
        ok.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
        Assert.Equal(HttpStatusCode.OK, (await h.SendAsync(ok)).Status);
        var hello = await h.Client.GetAsync(new Uri("https://localhost/hello"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.Redirect, hello.StatusCode);
        Assert.Contains("/login-page", hello.Headers.Location!.ToString(), StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.OK, (await h.Client.GetAsync(new Uri("https://localhost/public"), TestContext.Current.CancellationToken)).StatusCode);
    }

    static async Task<string> LoginAsync(MgmtHost h, string user)
    {
        var r = await h.Client.GetAsync(new Uri($"https://localhost/login/{user}"), TestContext.Current.CancellationToken);
        return string.Join("; ", r.Headers.GetValues("Set-Cookie").Select(c => c.Split(';')[0]));
    }

    /// <summary>The antiforgery request token and the cookies it pairs with, for an authenticated caller.</summary>
    static async Task<(string Token, string Cookies)> AntiforgeryAsync(MgmtHost h, Action<HttpRequestMessage> authenticate, string cookies = "")
    {
        var req = Req(h, HttpMethod.Get, "antiforgery");
        authenticate(req);
        if (cookies.Length > 0)
        {
            req.Headers.Add("Cookie", cookies);
        }

        var (status, body, response) = await h.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, status);
        var set = response.Headers.GetValues("Set-Cookie").Select(c => c.Split(';')[0]);
        return (body!["token"]!.GetValue<string>(), string.Join("; ", cookies.Length > 0 ? set.Prepend(cookies) : set));
    }

    [Fact]
    [Trait("Case", "MGMT-02")]
    public async Task Browser_mutations_need_the_antiforgery_token_and_nothing_skips_it()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt02"));
        await using var h = await MgmtHost.StartAsync(f, browser: true);
        Users.Seed(h);
        var cookie = await LoginAsync(h, Users.Admin);
        var (cookieToken, cookieJar) = await AntiforgeryAsync(h, _ => { }, cookie);
        var basic = MgmtHost.Basic(Users.Admin, "pw-admin");
        var (basicToken, basicJar) = await AntiforgeryAsync(h, r => r.Headers.Authorization = basic);

        async Task<HttpStatusCode> PauseAsync(Action<HttpRequestMessage> authenticate, string? token, bool inertia = false)
        {
            var req = Req(h, HttpMethod.Post, "queues/default/pause");
            req.Content = new StringContent($$"""{"commandId":"{{Guid.NewGuid()}}","expectedRevision":1}""", System.Text.Encoding.UTF8, "application/json");
            authenticate(req);
            if (token is not null)
            {
                req.Headers.Add("X-Runwick-Antiforgery", token);
            }

            if (inertia)
            {
                req.Headers.Add("X-Inertia", "true");
            }

            return (await h.SendAsync(req)).Status;
        }

        void Cookie(HttpRequestMessage r) => r.Headers.Add("Cookie", cookieJar);
        void Basic(HttpRequestMessage r)
        {
            r.Headers.Authorization = basic;
            r.Headers.Add("Cookie", basicJar);
        }

        foreach (var auth in new Action<HttpRequestMessage>[] { Cookie, Basic })
        {
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, null));
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, "not-a-token"));
            Assert.Equal(HttpStatusCode.BadRequest, await PauseAsync(auth, null, inertia: true));
        }

        // Nothing changed: no audit, still revision 1 and not paused.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_controls WHERE queue = 'default' AND NOT paused AND revision = 1"));

        // With the token the same request applies (cookie), and a token issued to the Basic caller works for Basic.
        Assert.Equal(HttpStatusCode.OK, await PauseAsync(Cookie, cookieToken));
        Assert.Equal(HttpStatusCode.Conflict, await PauseAsync(Basic, basicToken)); // stale revision: reached the store

        // A mutation path is never GET.
        var get = Req(h, HttpMethod.Get, "queues/default/pause");
        Cookie(get);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, (await h.SendAsync(get)).Status);

        // A machine-client group with a cookie or Basic scheme fails at startup.
        await using var machine = await DriverFixture.StartAsync(Ns("mgmt02m"));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => MgmtHost.StartAsync(machine, browser: true, machineGroup: true));
        Assert.Contains("ForMachineClients", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "MGMT-07")]
    public async Task Two_hosts_in_one_process_keep_their_own_prefixes_grants_and_results()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt07"));
        await using var a = await MgmtHost.StartAsync(f, prefix: "/ops/a", routes: app => app.MapGet("/ops/sibling", () => "mvc"));
        await using var b = await MgmtHost.StartAsync(f, prefix: "/ops/b", identity: "mgmtb");
        a.Grants["u1"] = ManagementRoles.Operator;
        a.Grants["u2"] = ManagementRoles.Viewer;
        b.Grants["u1"] = [];
        var jobs = await Task.WhenAll(Enumerable.Range(0, 4).Select(i => a.DispatchAsync(new PingJob("j" + i))));

        // Two callers in parallel on host A: the operator's cancels apply, the viewer's are refused.
        var results = await Task.WhenAll(jobs.Select((job, i) => a.PostAsync($"jobs/{job}/cancel", i % 2 == 0 ? "u1" : "u2",
            new { commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 })));
        Assert.Equal([HttpStatusCode.OK, HttpStatusCode.Forbidden, HttpStatusCode.OK, HttpStatusCode.Forbidden], results.Select(r => r.Status));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'u1'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'u2'"));

        // Host B maps the same user to nothing; host A's prefix is not B's; the sibling route is the host's own.
        Assert.Equal(HttpStatusCode.Forbidden, (await b.GetAsync("jobs", "u1")).Status);
        Assert.Equal(HttpStatusCode.OK, (await a.GetAsync("jobs", "u1")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await a.GetAsync("/ops/b/v1/jobs", "u1")).Status);
        var sibling = await a.Client.GetAsync(new Uri("http://localhost/ops/sibling"), TestContext.Current.CancellationToken);
        Assert.Equal("mvc", await sibling.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    [Trait("Case", "MGMT-08")]
    public async Task Forwarded_proto_from_a_known_proxy_enables_basic_and_path_base_works_without_redirects()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt08"));
        await using var h = await MgmtHost.StartAsync(f, browser: true, middleware: app =>
        {
            app.Use((ctx, next) =>
            {
                ctx.Connection.RemoteIpAddress = IPAddress.Parse(ctx.Request.Headers["X-Test-Remote"].ToString() is { Length: > 0 } ip ? ip : "10.0.0.9");
                return next(ctx);
            });
            var forwarded = new ForwardedHeadersOptions { ForwardedHeaders = ForwardedHeaders.XForwardedProto };
            forwarded.KnownIPNetworks.Clear();
            forwarded.KnownProxies.Clear();
            forwarded.KnownProxies.Add(IPAddress.Parse("10.0.0.1"));
            app.UseForwardedHeaders(forwarded);
            app.UsePathBase("/base");
        });
        Users.Seed(h);

        async Task<HttpStatusCode> SettingsAsync(string remote, string path = "/base/runwick/api/v1/settings")
        {
            var req = new HttpRequestMessage(HttpMethod.Get, "http://localhost" + path);
            req.Headers.Authorization = MgmtHost.Basic(Users.Admin, "pw-admin");
            req.Headers.Add("X-Forwarded-Proto", "https");
            req.Headers.Add("X-Test-Remote", remote);
            var (status, _, response) = await h.SendAsync(req);
            Assert.Null(response.Headers.Location);
            return status;
        }

        Assert.Equal(HttpStatusCode.OK, await SettingsAsync("10.0.0.1"));
        Assert.Equal(HttpStatusCode.Unauthorized, await SettingsAsync("10.0.0.2"));
        Assert.Equal(HttpStatusCode.OK, await SettingsAsync("10.0.0.1", "/runwick/api/v1/settings"));

        // A cookie-only caller without a cookie gets 401, never the cookie scheme's login redirect.
        var anonymous = await h.Client.GetAsync(new Uri("https://localhost/base/runwick/api/v1/jobs"), TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
        Assert.Null(anonymous.Headers.Location);
    }
}
