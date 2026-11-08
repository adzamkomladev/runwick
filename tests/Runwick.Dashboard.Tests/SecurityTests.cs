using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>DASH-03 login and refused requests, DASH-04 hostile text, MGMT-01/02/03/06 over every dashboard path.</summary>
[Trait("Item", "SP-024")]
public sealed class SecurityTests
{
    internal static object Position(JsonElement job) => new
    {
        expectedGeneration = job.GetProperty("generation").GetInt32(),
        expectedRevision = job.GetProperty("revision").GetInt64(),
    };

    internal static async Task<JsonElement> JobAsync(Browser b, Guid job, string mount = "/runwick") =>
        (await b.VisitJsonAsync($"{mount}/jobs/{job}")).Props().GetProperty("job").GetProperty("job");

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Basic_challenges_before_version_negotiation_and_checks_the_password()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash03b"));
        await using var h = await DashHost.StartAsync(f, mount: o => o.AuthenticationSchemes(Runwick.AspNetCore.RunwickBasicDefaults.Scheme));
        var anonymous = h.Browser();
        anonymous.Version = "stale";
        var challenged = await anonymous.VisitAsync("/runwick");
        Assert.Equal(HttpStatusCode.Unauthorized, challenged.StatusCode); // not the asset 409: authentication runs first
        Assert.StartsWith("Basic realm=", challenged.Header("WWW-Authenticate"), StringComparison.Ordinal);
        Assert.Null(challenged.Header("X-Inertia-Location"));
        Assert.Equal("no-store, private", challenged.Header("Cache-Control"));

        var wrong = await h.Browser(Users.Viewer).SendAsync(HttpMethod.Get, "/runwick", inertia: false, configure: r =>
            r.Headers.Authorization = new("Basic", Convert.ToBase64String("viewer:nope"u8.ToArray())));
        Assert.Equal(HttpStatusCode.Unauthorized, wrong.StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await h.Browser(Users.Viewer).LoadAsync("/runwick")).Response.StatusCode);

        // An authenticated principal with no grant at all is forbidden, not challenged.
        Assert.Equal(HttpStatusCode.Forbidden, (await h.Browser(Users.Nobody).LoadAsync("/runwick")).Response.StatusCode);
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Host_cookie_and_OIDC_style_challenges_become_full_visits_for_Inertia_requests()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash03c"));
        await using var h = await DashHost.StartAsync(f,
            services: b =>
            {
                b.Services.AddAuthentication()
                    .AddCookie("oidc-cookie", o => o.ForwardChallenge = "oidc") // the usual cookie + OpenIdConnect pairing
                    .AddScheme<AuthenticationSchemeOptions, ExternalIdpHandler>("oidc", null);
                b.Services.AddRunwickDashboard("oidc", o => o.AuthenticationSchemes("oidc-cookie"));
                b.Services.AddRunwickDashboard("cookie", o => o.AuthenticationSchemes("Cookies"));
            },
            before: a =>
            {
                a.MapRunwickDashboard("/cookie", "cookie");
                a.MapRunwickDashboard("/oidc", "oidc");
            },
            routes: a => a.MapGet("/login-oidc/{user}", async (Microsoft.AspNetCore.Http.HttpContext c, string user) =>
                await c.SignInAsync("oidc-cookie", DashHost.Principal(user, "oidc-cookie"))));
        var job = await h.DispatchAsync(new PingJob("x"));

        var browser = h.Browser();
        var html = await browser.SendAsync(HttpMethod.Get, "/cookie", inertia: false);
        Assert.Equal(HttpStatusCode.Redirect, html.StatusCode);
        Assert.StartsWith("https://localhost/login", html.Headers.Location!.ToString(), StringComparison.Ordinal);

        // XHR cannot follow a login or identity-provider redirect, so the client is told to visit it in full.
        var xhr = await browser.VisitAsync("/cookie");
        Assert.Equal(HttpStatusCode.Conflict, xhr.StatusCode);
        Assert.StartsWith("https://localhost/login", xhr.Header("X-Inertia-Location"), StringComparison.Ordinal);
        Assert.Empty(await xhr.Content.ReadAsStringAsync());
        var idp = await browser.VisitAsync($"/oidc/jobs/{job}");
        Assert.Equal(HttpStatusCode.Conflict, idp.StatusCode);
        Assert.StartsWith("https://idp.example/authorize", idp.Header("X-Inertia-Location"), StringComparison.Ordinal);

        // Signed in by the host (as after an OIDC callback): the same permission checks apply on both mounts.
        await browser.SendAsync(HttpMethod.Get, "/login/viewer", inertia: false);
        await browser.SendAsync(HttpMethod.Get, "/login-oidc/viewer", inertia: false);
        Assert.Equal(HttpStatusCode.OK, (await browser.LoadAsync("/cookie")).Response.StatusCode);
        var (oidc, _, _) = await browser.LoadAsync("/oidc");
        Assert.Equal(HttpStatusCode.OK, oidc.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await browser.RevealAsync($"/oidc/jobs/{job}/payload:reveal")).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await browser.PostAsync($"/oidc/jobs/{job}/cancel", Position(await JobAsync(browser, job)), Guid.NewGuid()))
            .StatusCode);

        // A principal of the cookie scheme with no grant: forbid goes to the access-denied page, a full visit for Inertia.
        await browser.SendAsync(HttpMethod.Get, "/login/nobody", inertia: false);
        var denied = await browser.VisitAsync("/cookie");
        Assert.Equal(HttpStatusCode.Conflict, denied.StatusCode);
        Assert.Contains("AccessDenied", denied.Header("X-Inertia-Location"), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    [Trait("Case", "MGMT-02")]
    public async Task Every_POST_needs_this_mounts_token_for_this_principal_and_never_runs_on_GET()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash03t"));
        await using var h = await DashHost.StartAsync(f,
            services: b => b.Services.AddRunwickDashboard("other", _ => { }),
            before: a => a.MapRunwickDashboard("/other", "other"));
        var job = await h.DispatchAsync(new PingJob("x"));
        var admin = h.Browser(Users.Admin);
        await admin.LoadAsync("/runwick");
        var token = admin.Csrf!;
        var position = Position(await JobAsync(admin, job));
        var other = h.Browser(Users.Admin);
        await other.LoadAsync("/other");

        string[] routes =
        [
            $"/runwick/jobs/{job}/cancel", $"/runwick/jobs/{job}/retry", $"/runwick/jobs/{job}/rerun", "/runwick/jobs:cancel",
            "/runwick/queues/default/pause", "/runwick/queues/default/resume", "/runwick/scopes/job-type/test.capped/capacity",
            "/runwick/schedules/nightly/pause", "/runwick/schedules/nightly/resume", "/runwick/schedules/nightly/trigger",
            "/runwick/schedules/nightly/revalidate", "/runwick/retention/policy", $"/runwick/jobs/{job}/payload:reveal",
        ];
        foreach (var route in routes)
        {
            // Missing, forged, another mount's, and another user's token: 403 before any session call.
            foreach (var (csrf, cookies, user) in new (string?, Browser, string)[]
                     {
                         (null, admin, Users.Admin), ("forged", admin, Users.Admin), (other.Csrf, other, Users.Admin), (token, admin, Users.Operator),
                     })
            {
                var b = h.Browser(user);
                foreach (var c in cookies.Cookies)
                {
                    b.Cookies[c.Key] = c.Value;
                }

                b.Csrf = csrf;
                var response = await b.PostAsync(route, new { items = Array.Empty<object>(), expectedRevision = 1 }, Guid.NewGuid());
                Assert.True(response.StatusCode == HttpStatusCode.Forbidden, $"{route} csrf={csrf?[..Math.Min(8, csrf.Length)]} user={user}: {response.StatusCode}");
            }

            Assert.Equal(HttpStatusCode.MethodNotAllowed, (await admin.SendAsync(HttpMethod.Get, route, inertia: true)).StatusCode);
            Assert.Equal("POST", (await admin.SendAsync(HttpMethod.Get, route, inertia: true)).Header("Allow"));
        }

        // A cookie principal: the same.
        var cookie = h.Browser();
        await cookie.SendAsync(HttpMethod.Get, "/login/admin", inertia: false);
        await cookie.LoadAsync("/runwick");
        var cookieToken = cookie.Csrf;
        cookie.Csrf = null;
        Assert.Equal(HttpStatusCode.Forbidden, (await cookie.PostAsync($"/runwick/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);
        cookie.Csrf = token; // the Basic principal's token: bound to another scheme
        Assert.Equal(HttpStatusCode.Forbidden, (await cookie.PostAsync($"/runwick/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);

        Assert.Equal(0, await h.AuditsAsync());
        Assert.Equal(position.ToString(), Position(await JobAsync(admin, job)).ToString());

        // With the right token both principals act: the token check is the only thing that changed.
        cookie.Csrf = cookieToken;
        Assert.Equal(HttpStatusCode.SeeOther, (await cookie.PostAsync($"/runwick/jobs/{job}/cancel", position, Guid.NewGuid())).StatusCode);
        Assert.Equal(1, await h.AuditsAsync());
    }

    [Fact]
    [Trait("Case", "DASH-03")]
    public async Task Denied_partial_reloads_and_reveals_are_refused()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dash03p"));
        await using var h = await DashHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("secret-payload"));
        var viewer = h.Browser(Users.Viewer);
        await viewer.LoadAsync($"/runwick/jobs/{job}");
        Assert.Equal(HttpStatusCode.Forbidden, (await viewer.VisitAsync($"/runwick/jobs/{job}", Partial("Job", "audit"))).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await viewer.VisitAsync($"/runwick/jobs/{job}", Partial("Job", "nosuchprop"))).StatusCode);
        var reveal = await viewer.RevealAsync($"/runwick/jobs/{job}/payload:reveal");
        Assert.Equal(HttpStatusCode.Forbidden, reveal.StatusCode);
        Assert.DoesNotContain("secret-payload", await reveal.Content.ReadAsStringAsync(), StringComparison.Ordinal);

        var admin = h.Browser(Users.Admin);
        await admin.LoadAsync($"/runwick/jobs/{job}");
        var allowed = await admin.VisitJsonAsync($"/runwick/jobs/{job}", Partial("Job", "audit"));
        var names = allowed.Props().EnumerateObject().Select(p => p.Name).ToList();
        Assert.Contains("audit", names);
        Assert.DoesNotContain("job", names); // only the requested prop (plus the adapter's errors)
        var payload = await admin.RevealAsync($"/runwick/jobs/{job}/payload:reveal");
        Assert.Equal("secret-payload", (await Browser.JsonAsync(payload)).GetProperty("payload").GetProperty("id").GetString());
        Assert.Equal("no-store, private", payload.Header("Cache-Control"));
    }

    /// <summary>The store kinds this run covers (RUNWICK_TEST_STORES narrows it, as for the contract suite).</summary>
    public static TheoryData<StoreKind> Kinds =>
        [.. Enum.GetValues<StoreKind>().Where(k => Environment.GetEnvironmentVariable("RUNWICK_TEST_STORES") is not { Length: > 0 } only
            || only.Split(',').Contains(k.ToString(), StringComparer.OrdinalIgnoreCase))];

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "DASH-04")]
    [Trait("Case", "MGMT-06")]
    public async Task Hostile_strings_are_text_everywhere_and_nothing_sensitive_reaches_props_or_logs(StoreKind kind)
    {
        var logs = new LogSink();
        await using var f = await DriverFixture.StartAsync(Ns("dash04"), kind);
        await using var h = await DashHost.StartAsync(f, services: b =>
        {
            b.Logging.AddProvider(logs);
            b.Logging.SetMinimumLevel(LogLevel.Information);
        }, mount: o => o.Title = "<b>\"t\"</b>");
        const string Short = "</script><img src=x onerror=alert(3)>\u2028"; // scaling text is at most 64 bytes
        var job = await h.DispatchAsync(new PingJob(DashHost.Hostile));
        await h.DispatchAsync(new PingJob("b"));
        var attempt = await f.FailNextAsync(DashHost.Hostile, "error-text-secret");
        await f.ScalingEventAsync(DateTimeOffset.UtcNow.AddMinutes(-1), pool: Short, reason: Short);

        // The actor's display name, a control reason and an error code carry the hostile text; job names and queues cannot
        // (the store's identifier grammar rejects them).
        var browser = h.Browser();
        await browser.SendAsync(HttpMethod.Get, "/login/admin?display=" + Uri.EscapeDataString(DashHost.Hostile), inertia: false);
        var (_, _, first) = await browser.LoadAsync("/runwick");
        var other = first.Props().GetProperty("jobs").GetProperty("items").EnumerateArray().Single(j => j.GetProperty("jobId").GetGuid() != job);
        var cancel = await browser.PostAsync($"/runwick/jobs/{other.GetProperty("jobId")}/cancel",
            new { expectedGeneration = other.GetProperty("generation").GetInt32(), expectedRevision = other.GetProperty("revision").GetInt64(), reason = DashHost.Hostile },
            Guid.NewGuid());
        Assert.Equal(HttpStatusCode.SeeOther, cancel.StatusCode);

        foreach (var path in new[] { "/runwick", $"/runwick/jobs/{job}", "/runwick/audit", "/runwick/scaling" })
        {
            var (response, body, page) = await browser.LoadAsync(path);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.DoesNotContain("<script>alert", body, StringComparison.Ordinal);
            Assert.DoesNotContain("<img", body, StringComparison.Ordinal);
            Assert.DoesNotContain("\u2028", body, StringComparison.Ordinal);
            Assert.Contains("<title>&lt;b&gt;&quot;t&quot;&lt;/b&gt;</title>", body, StringComparison.Ordinal);
            Assert.Equal(2, body.Split("</script>").Length - 1); // only the entry script and the page script close
            Assert.Equal(DashHost.Hostile, page.Props().GetProperty("user").GetString());
            Assert.True(page.GetProperty("encryptHistory").GetBoolean());
            Assert.Equal("no-store, private", response.Header("Cache-Control"));
            Assert.Equal("X-Inertia", response.Header("Vary"));
            Assert.Equal("nosniff", response.Header("X-Content-Type-Options"));
            Assert.Equal(
                "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
                response.Header("Content-Security-Policy"));
            Assert.DoesNotContain(" style=", body, StringComparison.Ordinal);
            AssertNothingSensitive(page.GetRawText(), browser.Csrf!);
        }

        // Data round-trips unchanged, in the initial HTML and in partial reloads (what a poll sends).
        var detail = (await browser.LoadAsync($"/runwick/jobs/{job}")).Page.Props().GetProperty("job");
        Assert.Equal(DashHost.Hostile, detail.GetProperty("attempts")[0].GetProperty("errorCode").GetString());
        var poll = await browser.VisitJsonAsync($"/runwick/jobs/{job}", Partial("Job", "job"));
        Assert.Equal(DashHost.Hostile, poll.Props().GetProperty("job").GetProperty("attempts")[0].GetProperty("errorCode").GetString());
        var audit = await browser.VisitJsonAsync("/runwick/audit", Partial("Audit", "audit"));
        Assert.Equal(DashHost.Hostile, audit.Props().GetProperty("audit").GetProperty("items")[0].GetProperty("reason").GetString());
        Assert.Equal(DashHost.Hostile, audit.Props().GetProperty("audit").GetProperty("items")[0].GetProperty("actorDisplay").GetString());
        var events = await browser.VisitJsonAsync("/runwick/scaling", Partial("Scaling", "events"));
        Assert.Equal(Short, events.Props().GetProperty("events").GetProperty("items")[0].GetProperty("pool").GetString());
        Assert.Equal(Short, events.Props().GetProperty("events").GetProperty("items")[0].GetProperty("reason").GetString());
        var jobs = await browser.VisitJsonAsync("/runwick", Partial("Jobs", "jobs"));
        AssertNothingSensitive(jobs.GetRawText(), browser.Csrf!);
        AssertNothingSensitive(poll.GetRawText(), browser.Csrf!);

        // Reveals: plain JSON, no-store, the stored value as data.
        var payload = await browser.RevealAsync($"/runwick/jobs/{job}/payload:reveal");
        Assert.Equal("no-store, private", payload.Header("Cache-Control"));
        Assert.Equal(DashHost.Hostile, (await Browser.JsonAsync(payload)).GetProperty("payload").GetProperty("id").GetString());
        var error = await browser.RevealAsync($"/runwick/attempts/{attempt}/error:reveal");
        Assert.Contains("error-text-secret", await error.Content.ReadAsStringAsync(), StringComparison.Ordinal);

        // Assets are immutable; a 401 is no-store.
        var asset = await browser.SendAsync(HttpMethod.Get, "/runwick/" + DashboardAssets.Embedded.Entry, inertia: false);
        Assert.Equal("public, max-age=31536000, immutable", asset.Header("Cache-Control"));
        Assert.Equal("text/javascript; charset=utf-8", asset.Content.Headers.ContentType!.ToString());
        Assert.Equal(HttpStatusCode.NotFound, (await browser.SendAsync(HttpMethod.Get, "/runwick/manifest.json", inertia: false)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await browser.SendAsync(HttpMethod.Get, "/runwick/assets/nope.js", inertia: false)).StatusCode);

        // Logs at Information or above carry no payload, error text or token.
        Assert.NotEmpty(logs.Lines);
        Assert.DoesNotContain(logs.Lines, l => l.Contains(DashHost.Hostile[..20], StringComparison.Ordinal)
            || l.Contains("error-text-secret", StringComparison.Ordinal) || l.Contains(browser.Csrf!, StringComparison.Ordinal));
    }

    static void AssertNothingSensitive(string json, string csrf)
    {
        Assert.DoesNotContain("error-text-secret", json, StringComparison.Ordinal);
        Assert.DoesNotContain(csrf, json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"payload\"", json, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "MGMT-01")]
    public async Task Without_valid_credentials_no_path_returns_data_and_the_host_is_unchanged()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt01"));
        await using var h = await DashHost.StartAsync(f, routes: a =>
        {
            a.MapGet("/hello", () => "hello").RequireAuthorization();
            a.MapGet("/public", () => "public");
        });
        var job = await h.DispatchAsync(new PingJob("secret-payload"));
        var paths = new (HttpMethod Method, string Path, bool Inertia, Action<HttpRequestMessage>? Partial)[]
        {
            (HttpMethod.Get, "/runwick", false, null), (HttpMethod.Get, $"/runwick/jobs/{job}", true, null),
            (HttpMethod.Get, $"/runwick/jobs/{job}", true, Partial("Job", "job")), (HttpMethod.Post, $"/runwick/jobs/{job}/cancel", true, null),
            (HttpMethod.Post, "/runwick/jobs:cancel", true, null), (HttpMethod.Post, $"/runwick/jobs/{job}/payload:reveal", false, null),
        };
        var credentials = new Action<HttpRequestMessage>?[]
        {
            null,
            r => r.Headers.Authorization = new("Bearer", "abc"),
            r => r.Headers.Authorization = new("Basic", Convert.ToBase64String("admin:wrong"u8.ToArray())),
        };
        foreach (var credential in credentials)
        {
            foreach (var (method, path, inertia, partial) in paths)
            {
                var response = await h.Browser().SendAsync(method, path, inertia, "{}", r =>
                {
                    credential?.Invoke(r);
                    partial?.Invoke(r);
                });
                var body = await response.Content.ReadAsStringAsync();
                // Basic answers 401; the host cookie scheme redirects to its login page (409 + X-Inertia-Location for Inertia requests).
                Assert.True(response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Redirect or HttpStatusCode.Conflict, $"{method} {path}: {response.StatusCode}");
                if (response.StatusCode != HttpStatusCode.Unauthorized)
                {
                    Assert.Contains("/login", (response.Headers.Location?.ToString() ?? response.Header("X-Inertia-Location"))!, StringComparison.Ordinal);
                }

                Assert.DoesNotContain(job.ToString(), body, StringComparison.Ordinal);
                Assert.DoesNotContain("secret-payload", body, StringComparison.Ordinal);
            }
        }

        Assert.Equal(HttpStatusCode.OK, (await h.Browser().SendAsync(HttpMethod.Get, "/runwick/" + DashboardAssets.Embedded.Entry, inertia: false)).StatusCode);
        var hello = await h.Browser().SendAsync(HttpMethod.Get, "/hello", inertia: false);
        Assert.Equal(HttpStatusCode.Redirect, hello.StatusCode); // the host's default scheme (Cookies) still challenges its routes
        Assert.Equal("public", await (await h.Browser().SendAsync(HttpMethod.Get, "/public", inertia: false)).Content.ReadAsStringAsync());
        Assert.Equal("Cookies", (await h.App.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetDefaultAuthenticateSchemeAsync())!.Name);
    }

    [Fact]
    [Trait("Case", "MGMT-03")]
    public async Task A_queue_scoped_caller_sees_only_its_queues_on_every_path()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt03"));
        await using var h = await DashHost.StartAsync(f);
        var mine = await h.DispatchAsync(new PingJob("mine"));
        await h.DispatchAsync(new PingJob("mine-2"));
        var hidden = await h.DispatchAsync(new OtherJob("hidden"));
        var hiddenAttempt = Guid.NewGuid();
        var scoped = h.Browser(Users.Scoped);
        await scoped.LoadAsync("/runwick");

        Assert.Equal(HttpStatusCode.NotFound, (await scoped.LoadAsync($"/runwick/jobs/{hidden}")).Response.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await scoped.VisitAsync($"/runwick/jobs/{hidden}")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await scoped.VisitAsync($"/runwick/jobs/{hidden}", Partial("Job", "job"))).StatusCode);
        foreach (var op in new[] { "cancel", "retry", "rerun" })
        {
            var r = await scoped.PostAsync($"/runwick/jobs/{hidden}/{op}", new { expectedGeneration = 1, expectedRevision = 1 }, Guid.NewGuid());
            Assert.Equal(HttpStatusCode.NotFound, r.StatusCode);
        }

        Assert.Equal(HttpStatusCode.NotFound, (await scoped.RevealAsync($"/runwick/jobs/{hidden}/payload:reveal")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await scoped.RevealAsync($"/runwick/attempts/{hiddenAttempt}/error:reveal")).StatusCode);
        var bulk = await scoped.PostAsync("/runwick/jobs:cancel",
            new { items = new[] { new { jobId = hidden, commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 } } });
        Assert.Equal("not_found", (await Browser.JsonAsync(bulk)).Props().GetProperty("outcome").GetProperty("items")[0].GetProperty("status").GetString());

        // Lists and their next pages hold only the allowed queue.
        var queues = new List<string>();
        var url = "/runwick/jobs?limit=1";
        while (true)
        {
            var page = await scoped.VisitJsonAsync(url);
            var jobs = page.Props().GetProperty("jobs");
            queues.AddRange(jobs.GetProperty("items").EnumerateArray().Select(j => j.GetProperty("queue").GetString()!));
            if (jobs.GetProperty("next").GetString() is not { } next)
            {
                break;
            }

            url = "/runwick/jobs?limit=1&cursor=" + next;
        }

        Assert.Equal(["default", "default"], queues);
        var queueList = (await scoped.VisitJsonAsync("/runwick/queues")).Props().GetProperty("queues").GetProperty("items");
        Assert.Equal(["default"], queueList.EnumerateArray().Select(q => q.GetProperty("name").GetString()));

        // A tampered cursor renders the first page with a notice.
        var first = (await scoped.VisitJsonAsync("/runwick/jobs?limit=1")).Props().GetProperty("jobs").GetProperty("next").GetString()!;
        var tampered = await scoped.VisitJsonAsync("/runwick/jobs?limit=1&cursor=" + first[..^4] + "AAAA");
        Assert.Equal("invalid_cursor", tampered.Props().GetProperty("notice").GetString());
        Assert.Single(tampered.Props().GetProperty("jobs").GetProperty("items").EnumerateArray());

        // An actor field in a POST body is ignored: the audit row names the authenticated caller.
        var position = Position(await JobAsync(scoped, mine));
        var cancel = await scoped.PostAsync($"/runwick/jobs/{mine}/cancel",
            new { expectedGeneration = 1, expectedRevision = ((dynamic)position).expectedRevision, actor = "admin", actorSubject = "admin", grants = "all" }, Guid.NewGuid());
        Assert.Equal(HttpStatusCode.SeeOther, cancel.StatusCode);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'scoped'"));
    }

    /// <summary>Stands in for OpenIdConnect: never authenticates by itself and challenges by redirecting to another origin.</summary>
    sealed class ExternalIdpHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
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

/// <summary>Collects formatted log lines (with their arguments) at every level the host lets through.</summary>
internal sealed class LogSink : ILoggerProvider
{
    public System.Collections.Concurrent.ConcurrentQueue<string> Lines { get; } = new();

    public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

    public void Dispose()
    {
    }

    sealed class Logger(LogSink sink, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                var args = state is IEnumerable<KeyValuePair<string, object?>> pairs ? string.Join("|", pairs.Select(p => p.Value)) : "";
                sink.Lines.Enqueue($"{category} {formatter(state, exception)} {args} {exception}");
            }
        }
    }
}
