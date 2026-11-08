using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using static Runwick.Dashboard.Spike.Tests.Support;

namespace Runwick.Dashboard.Spike.Tests;

/// <summary>DASH-03 authentication, CSRF and denied requests; DASH-04 hostile strings, history and caching.</summary>
[Trait("Item", "SP-023")]
public sealed class SecurityTests
{
    private static Task<WebApplication> BasicHostAsync(InMemoryDashboardSpikeData? data = null, string? title = null) =>
        StartAsync(b => { b.Services.AddBasic("basic"); b.Services.AddDashboard("default", "basic", data ?? Data(), title); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Basic_challenges_before_version_negotiation_and_checks_the_password()
    {
        await using var app = await BasicHostAsync();
        var anonymous = app.Browser();
        anonymous.Version = "stale";
        var challenged = await anonymous.VisitAsync("/runwick");
        Assert.Equal(HttpStatusCode.Unauthorized, challenged.StatusCode); // not the asset 409: authentication runs first
        Assert.StartsWith("Basic realm=", challenged.Header("WWW-Authenticate"), StringComparison.Ordinal);
        Assert.Null(challenged.Header("X-Inertia-Location"));

        var wrong = app.Browser("alice");
        var bad = await wrong.SendAsync(HttpMethod.Get, "/runwick", inertia: false, configure: r =>
            r.Headers.Authorization = new("Basic", Convert.ToBase64String("alice:nope"u8.ToArray())));
        Assert.Equal(HttpStatusCode.Unauthorized, bad.StatusCode);

        Assert.Equal(HttpStatusCode.OK, (await app.Browser("victor").LoadAsync("/runwick")).Response.StatusCode);
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Host_cookie_and_OIDC_style_challenges_become_full_visits_for_Inertia_requests()
    {
        await using var app = await StartAsync(
            b =>
            {
                b.Services.AddDashboard("cookie", "host-cookie", Data());
                b.Services.AddDashboard("oidc", "oidc-cookie", Data());
                b.Services.AddAuthentication()
                    .AddCookie("host-cookie", o => o.LoginPath = "/login")
                    .AddCookie("oidc-cookie", o => o.ForwardChallenge = "oidc") // the usual cookie + OpenIdConnect pairing
                    .AddScheme<AuthenticationSchemeOptions, ExternalIdpHandler>("oidc", null);
            },
            a =>
            {
                a.UseRunwickDashboardSpike("/cookie", "cookie");
                a.UseRunwickDashboardSpike("/oidc", "oidc");
                a.MapGet("/login", async (HttpContext c, string scheme, string user, string permission) =>
                {
                    var identity = new ClaimsIdentity([new(ClaimTypes.Name, user), new("runwick:permission", permission)], scheme);
                    await c.SignInAsync(scheme, new ClaimsPrincipal(identity));
                    return Results.Ok();
                });
            });

        var browser = app.Browser();
        var html = await browser.SendAsync(HttpMethod.Get, "/cookie", inertia: false);
        Assert.Equal(HttpStatusCode.Redirect, html.StatusCode);
        Assert.StartsWith("http://localhost/login", html.Headers.Location!.ToString(), StringComparison.Ordinal);

        // XHR cannot follow a login or identity-provider redirect, so the client is told to visit it in full.
        var xhr = await browser.VisitAsync("/cookie");
        Assert.Equal(HttpStatusCode.Conflict, xhr.StatusCode);
        Assert.StartsWith("http://localhost/login", xhr.Header("X-Inertia-Location"), StringComparison.Ordinal);
        var idp = await browser.VisitAsync("/oidc/jobs/1");
        Assert.Equal(HttpStatusCode.Conflict, idp.StatusCode);
        Assert.StartsWith("https://idp.example/authorize", idp.Header("X-Inertia-Location"), StringComparison.Ordinal);

        // Signed in by the host (as after an OIDC callback), the same capability checks apply to both mounts.
        await browser.SendAsync(HttpMethod.Get, "/login?scheme=host-cookie&user=carol&permission=view", inertia: false);
        await browser.SendAsync(HttpMethod.Get, "/login?scheme=oidc-cookie&user=carol&permission=view", inertia: false);
        Assert.Equal(HttpStatusCode.OK, (await browser.LoadAsync("/cookie")).Response.StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await browser.LoadAsync("/oidc")).Response.StatusCode);
        // carol may only view: the cookie scheme's forbid goes to its access-denied page (a full visit for Inertia requests).
        var reveal = await browser.SendAsync(HttpMethod.Get, "/oidc/jobs/1/payload", inertia: false);
        Assert.Equal(HttpStatusCode.Redirect, reveal.StatusCode);
        Assert.Contains("AccessDenied", reveal.Headers.Location!.ToString(), StringComparison.Ordinal);
        var retry = await browser.PostAsync("/oidc/jobs/1/retry", revision: 1);
        Assert.Equal(HttpStatusCode.Conflict, retry.StatusCode);
        Assert.Contains("AccessDenied", retry.Header("X-Inertia-Location"), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Mutations_need_this_mounts_token_for_this_principal_and_never_run_on_GET()
    {
        var data = Data();
        await using var app = await StartAsync(b => { b.Services.AddBasic("basic", "mallory"); b.Services.AddDashboard("default", "basic", data); },
            a => a.UseRunwickDashboardSpike("/runwick", "default"));
        var alice = app.Browser("alice");
        await alice.LoadAsync("/runwick");
        var token = alice.Csrf;

        alice.Csrf = null;
        Assert.Equal(HttpStatusCode.Forbidden, (await alice.PostAsync("/runwick/jobs/1/retry", 1, "c1")).StatusCode);
        alice.Csrf = "forged";
        Assert.Equal(HttpStatusCode.Forbidden, (await alice.PostAsync("/runwick/jobs/1/retry", 1, "c2")).StatusCode);

        // A forged X-Inertia plus valid credentials is no bypass; neither is the right token with another user's cookie.
        alice.Csrf = token;
        alice.User = "mallory";
        Assert.Equal(HttpStatusCode.Forbidden, (await alice.PostAsync("/runwick/jobs/1/retry", 1, "c3")).StatusCode);
        alice.User = "alice";

        Assert.Equal(HttpStatusCode.MethodNotAllowed, (await alice.SendAsync(HttpMethod.Get, "/runwick/jobs/1/retry", inertia: true)).StatusCode);
        Assert.Equal(0, data.Applied);

        var victor = app.Browser("victor");
        await victor.LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.Forbidden, (await victor.PostAsync("/runwick/jobs/1/retry", 1, "c4")).StatusCode); // view only
        Assert.Equal(HttpStatusCode.SeeOther, (await alice.PostAsync("/runwick/jobs/1/retry", 1, "c5")).StatusCode);
        Assert.Equal(1, data.Applied);
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Denied_partial_reloads_and_reveals_are_refused()
    {
        await using var app = await BasicHostAsync();
        var victor = app.Browser("victor");
        await victor.LoadAsync("/runwick/jobs/1");
        var partial = await victor.VisitAsync("/runwick/jobs/1", Partial("Job", "audit"));
        Assert.Equal(HttpStatusCode.Forbidden, partial.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await victor.SendAsync(HttpMethod.Get, "/runwick/jobs/1/payload", inertia: false)).StatusCode);

        var alice = app.Browser("alice");
        await alice.LoadAsync("/runwick/jobs/1");
        var allowed = await Browser.JsonAsync(await alice.VisitAsync("/runwick/jobs/1", Partial("Job", "audit")));
        var names = allowed.GetProperty("props").EnumerateObject().Select(p => p.Name).ToList();
        Assert.Contains("audit", names);
        Assert.DoesNotContain("job", names); // only the requested prop (plus the adapter's always-sent "errors")
        var reveal = await alice.SendAsync(HttpMethod.Get, "/runwick/jobs/1/payload", inertia: false);
        Assert.Equal("secret-payload-alpha", (await Browser.JsonAsync(reveal)).GetProperty("payload").GetString());
        Assert.Equal("no-store, private", reveal.Header("Cache-Control"));
    }

    [Fact]
    [Trait("Case", "DASH-04")]
    public async Task Hostile_strings_are_encoded_in_the_initial_HTML_and_round_trip_as_data()
    {
        await using var app = await BasicHostAsync(Data(Hostile), title: "<b>\"t\"</b>");
        var (response, body, page) = await app.Browser("alice").LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain("<script>alert", body, StringComparison.Ordinal);
        Assert.DoesNotContain("<img", body, StringComparison.Ordinal);
        Assert.DoesNotContain("\u2028", body, StringComparison.Ordinal);
        Assert.Contains("<title>&lt;b&gt;&quot;t&quot;&lt;/b&gt;</title>", body, StringComparison.Ordinal);
        Assert.Equal(Hostile, page.GetProperty("props").GetProperty("jobs")[0].GetProperty("name").GetString());
        Assert.Equal(2, body.Split("</script>").Length - 1); // only the entry script and the page script close
    }

    [Fact]
    [Trait("Case", "DASH-04")]
    public async Task Pages_are_private_no_store_with_a_strict_CSP_and_carry_no_payload_or_token()
    {
        await using var app = await BasicHostAsync();
        var browser = app.Browser("alice");
        var (html, body, page) = await browser.LoadAsync("/runwick/jobs/1");
        Assert.Equal("no-store, private", html.Header("Cache-Control"));
        Assert.Equal("X-Inertia", html.Header("Vary"));
        Assert.Equal("nosniff", html.Header("X-Content-Type-Options"));
        Assert.Equal(
            "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
            html.Header("Content-Security-Policy"));
        Assert.DoesNotContain(" style=", body, StringComparison.Ordinal);
        Assert.True(page.GetProperty("encryptHistory").GetBoolean());

        // Page JSON (what history stores) has neither the payload nor the CSRF token.
        var pageJson = page.GetRawText();
        Assert.DoesNotContain("secret-payload", pageJson, StringComparison.Ordinal);
        Assert.DoesNotContain(browser.Csrf!, pageJson, StringComparison.Ordinal);
        var visit = await browser.VisitAsync("/runwick/jobs/1");
        Assert.Equal("no-store, private", visit.Header("Cache-Control"));
        Assert.Equal("true", visit.Header("X-Inertia"));
        Assert.DoesNotContain("secret-payload", await visit.Content.ReadAsStringAsync(), StringComparison.Ordinal);

        var denied = await app.Browser().SendAsync(HttpMethod.Get, "/runwick", inertia: false);
        Assert.Equal("no-store, private", denied.Header("Cache-Control"));

        var asset = await browser.SendAsync(HttpMethod.Get, "/runwick/" + DashboardAssets.Embedded.Entry, inertia: false);
        Assert.Equal(HttpStatusCode.OK, asset.StatusCode);
        Assert.Equal("public, max-age=31536000, immutable", asset.Header("Cache-Control"));
    }

    internal static Action<HttpRequestMessage> Partial(string component, string only) => r =>
    {
        r.Headers.Add("X-Inertia-Partial-Component", component);
        r.Headers.Add("X-Inertia-Partial-Data", only);
    };

    /// <summary>Stands in for OpenIdConnect: never authenticates by itself and challenges by redirecting to another origin.</summary>
    private sealed class ExternalIdpHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync() => Task.FromResult(AuthenticateResult.NoResult());

        protected override Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            Response.Redirect("https://idp.example/authorize?client_id=runwick");
            return Task.CompletedTask;
        }
    }
}
