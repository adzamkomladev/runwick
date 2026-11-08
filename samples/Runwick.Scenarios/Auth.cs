using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick;
using Runwick.AspNetCore;
using Runwick.Management;

/// <summary>Accepts "Authorization: Bearer token-for-NAME". A stand-in for the host's real bearer scheme (JWT or similar); any non-cookie, non-Basic scheme works.</summary>
sealed class BearerHandler(IOptionsMonitor<AuthenticationSchemeOptions> o, ILoggerFactory l, UrlEncoder e) : AuthenticationHandler<AuthenticationSchemeOptions>(o, l, e)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var h = Request.Headers.Authorization.ToString();
        if (!h.StartsWith("Bearer token-for-", StringComparison.Ordinal))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var user = h["Bearer token-for-".Length..];
        var principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, user)], Scheme.Name));
        return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(principal, Scheme.Name)));
    }
}

/// <summary>SCN-16 to SCN-18: who may call the management API and the dashboard (spec 16 §5, spec 21 §3.2).</summary>
static class Auth
{
    static readonly IReadOnlyDictionary<string, IReadOnlyList<ManagementGrant>> Grants = new Dictionary<string, IReadOnlyList<ManagementGrant>>
    {
        ["viewer"] = ManagementRoles.Viewer,
        ["operator"] = ManagementRoles.Operator,
        // A queue-scoped grant for a queue that does not exist: sees nothing of "work".
        ["scoped"] = [new ManagementGrant(ManagementPermissions.QueuesRead, new HashSet<string>(StringComparer.Ordinal) { "other" })],
    };

    static async Task<(WebApplication App, string Url, Scratch Store)> StartAsync(bool basic)
    {
        var store = await Scratch.CreateAsync(StoreKind.Pg, "auth");
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseUrls("http://127.0.0.1:0");
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        var o = new NodeOptions(StoreKind.Pg, store.Conn, store.App, store.Sink(), "api", Role.Producer);
        b.Services.AddRunwick(r => NodeHost.Configure(r, o));
        b.Services.AddSingleton(new NodeName("api", store.Sink()));
        b.Services.AddAuthorization();
        var auth = b.Services.AddAuthentication()
            .AddRunwickBasic(x => x.Verifier = (user, pw, _) => ValueTask.FromResult(Grants.ContainsKey(user) && pw == "pw-" + user))
            .AddScheme<AuthenticationSchemeOptions, BearerHandler>("Bearer", null);
        _ = auth;
        b.Services.AddRunwickManagement(m =>
        {
            m.AuthenticationSchemes(basic ? [RunwickBasicDefaults.Scheme] : ["Bearer"]);
            m.MapGrants(p => Grants.GetValueOrDefault(p.Identity?.Name ?? "", []));
        });
        var app = b.Build();
        app.UseForwardedHeaders(new ForwardedHeadersOptions { ForwardedHeaders = ForwardedHeaders.XForwardedProto }); // loopback is a known proxy by default
        var endpoints = app.MapRunwickManagement("/runwick/api");
        if (!basic)
        {
            endpoints.ForMachineClients();
        }

        await app.StartAsync();
        return (app, app.Urls.Single(), store);
    }

    static HttpRequestMessage Req(string url, HttpMethod m, string path, string? authorization = null, bool https = false, string? body = null, string? token = null, string? cookie = null)
    {
        var r = new HttpRequestMessage(m, $"{url}/runwick/api/v1/{path}");
        if (authorization is not null)
        {
            r.Headers.Authorization = AuthenticationHeaderValue.Parse(authorization);
        }

        if (https)
        {
            r.Headers.Add("X-Forwarded-Proto", "https");
        }

        if (token is not null)
        {
            r.Headers.Add("X-Runwick-Antiforgery", token);
        }

        if (cookie is not null)
        {
            r.Headers.Add("Cookie", cookie);
        }

        if (body is not null)
        {
            r.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        return r;
    }

    static string BasicHeader(string user, string pw) => "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{pw}"));

    static async Task<HttpStatusCode> StatusAsync(HttpClient c, HttpRequestMessage r)
    {
        using var resp = await c.SendAsync(r);
        return resp.StatusCode;
    }

    static string Cookies(HttpResponseMessage r) =>
        r.Headers.TryGetValues("Set-Cookie", out var v) ? string.Join("; ", v.Select(x => x.Split(';')[0])) : "";

    static void Expect(string what, HttpStatusCode actual, HttpStatusCode expected)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException($"{what}: expected {(int)expected}, got {(int)actual}");
        }
    }

    static string Pause(long revision) => $$"""{"commandId":"{{Guid.NewGuid()}}","expectedRevision":{{revision}}}""";

    static async Task<long> QueueRevisionAsync(HttpClient c, string url, string authorization, bool https)
    {
        using var resp = await c.SendAsync(Req(url, HttpMethod.Get, "queues", authorization, https));
        var items = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["items"]!.AsArray();
        return items.Single(q => q!["name"]!.GetValue<string>() == "work")!["revision"]!.GetValue<long>();
    }

    public static async Task<string> BasicAsync()
    {
        var (app, url, store) = await StartAsync(basic: true);
        await using var _ = store;
        using var handler = new HttpClientHandler { UseCookies = false };
        using var c = new HttpClient(handler);
        try
        {
            Expect("no credentials", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", https: true)), HttpStatusCode.Unauthorized);
            Expect("wrong password", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", BasicHeader("operator", "nope"), true)), HttpStatusCode.Unauthorized);
            Expect("right password over plain HTTP", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", BasicHeader("operator", "pw-operator"))), HttpStatusCode.Unauthorized);
            var op = BasicHeader("operator", "pw-operator");
            Expect("right password over HTTPS (forwarded)", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", op, true)), HttpStatusCode.OK);
            var revision = await QueueRevisionAsync(c, url, op, true);

            Expect("mutation without antiforgery token", await StatusAsync(c, Req(url, HttpMethod.Post, "queues/work/pause", op, true, Pause(revision))), HttpStatusCode.BadRequest);

            using var tokenResp = await c.SendAsync(Req(url, HttpMethod.Get, "antiforgery", op, true));
            var token = JsonNode.Parse(await tokenResp.Content.ReadAsStringAsync())!["token"]!.GetValue<string>();
            var cookie = Cookies(tokenResp);
            Expect("operator mutation with token", await StatusAsync(c, Req(url, HttpMethod.Post, "queues/work/pause", op, true, Pause(revision), token, cookie)), HttpStatusCode.OK);

            var viewer = BasicHeader("viewer", "pw-viewer");
            using var vToken = await c.SendAsync(Req(url, HttpMethod.Get, "antiforgery", viewer, true));
            var vt = JsonNode.Parse(await vToken.Content.ReadAsStringAsync())!["token"]!.GetValue<string>();
            var vc = Cookies(vToken);
            Expect("viewer mutation (token ok, no permission)", await StatusAsync(c, Req(url, HttpMethod.Post, "queues/work/resume", viewer, true, Pause(revision + 1), vt, vc)), HttpStatusCode.Forbidden);
            return "401 without/with wrong/plain-HTTP credentials; 200 over HTTPS; 400 without antiforgery token; 200 with it; viewer 403";
        }
        finally
        {
            await app.StopAsync();
        }
    }

    public static async Task<string> BearerAsync()
    {
        var (app, url, store) = await StartAsync(basic: false);
        await using var _ = store;
        using var handler = new HttpClientHandler { UseCookies = false };
        using var c = new HttpClient(handler);
        try
        {
            string Bearer(string who) => "Bearer token-for-" + who;
            Expect("no token", await StatusAsync(c, Req(url, HttpMethod.Get, "queues")), HttpStatusCode.Unauthorized);
            Expect("Basic on a bearer-only API", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", BasicHeader("operator", "pw-operator"), true)), HttpStatusCode.Unauthorized);
            Expect("viewer read", await StatusAsync(c, Req(url, HttpMethod.Get, "queues", Bearer("viewer"))), HttpStatusCode.OK);
            var revision = await QueueRevisionAsync(c, url, Bearer("viewer"), false);
            Expect("viewer pause", await StatusAsync(c, Req(url, HttpMethod.Post, "queues/work/pause", Bearer("viewer"), false, Pause(revision))), HttpStatusCode.Forbidden);
            Expect("operator pause, no antiforgery needed", await StatusAsync(c, Req(url, HttpMethod.Post, "queues/work/pause", Bearer("operator"), false, Pause(revision))), HttpStatusCode.OK);

            using var scoped = await c.SendAsync(Req(url, HttpMethod.Get, "queues", Bearer("scoped")));
            var seen = JsonNode.Parse(await scoped.Content.ReadAsStringAsync())!["items"]!.AsArray().Count;
            if (scoped.StatusCode != HttpStatusCode.OK || seen != 0)
            {
                throw new InvalidOperationException($"queue-scoped grant: status {(int)scoped.StatusCode}, saw {seen} queues (expected 0)");
            }

            return "401 without token and with the wrong scheme; viewer reads 200 and is 403 on pause; operator pause 200 without antiforgery; a grant scoped to another queue sees 0 queues";
        }
        finally
        {
            await app.StopAsync();
        }
    }

    public static async Task<string> DashboardAsync()
    {
        var dll = FindDashboard();
        var conn = await Scratch.NewPgDatabaseAsync("dash");
        var port = Random.Shared.Next(20000, 40000);
        try
        {
            await using var host = Child.Start("dashboard", [dll], new Dictionary<string, string> { ["RUNWICK_DASH_PG"] = conn, ["RUNWICK_DASH_PORT"] = port.ToString() }, program: false);
            await Wait.UntilAsync(() => host.Output.Contains("ready", StringComparison.Ordinal) || host.HasExited, 60, "the dashboard host to start");
            if (host.HasExited)
            {
                throw new InvalidOperationException("dashboard host exited: " + host.Output);
            }

            using var handler = new HttpClientHandler { AllowAutoRedirect = false, UseCookies = false };
            using var c = new HttpClient(handler);
            var page = $"http://127.0.0.1:{port}/ops/runwick";
            using var anon = await c.GetAsync(page);
            if ((int)anon.StatusCode is not (302 or 401) || !(anon.Headers.Location?.ToString().Contains("login", StringComparison.Ordinal) ?? anon.StatusCode == HttpStatusCode.Unauthorized))
            {
                throw new InvalidOperationException($"signed-out page: {(int)anon.StatusCode} {anon.Headers.Location}");
            }

            using var login = await c.GetAsync($"http://127.0.0.1:{port}/ops/login/admin");
            var cookie = string.Join("; ", login.Headers.GetValues("Set-Cookie").Select(x => x.Split(';')[0]));
            using var req = new HttpRequestMessage(HttpMethod.Get, page);
            req.Headers.Add("Cookie", cookie);
            using var signedIn = await c.SendAsync(req);
            var html = await signedIn.Content.ReadAsStringAsync();
            Expect("signed-in dashboard page", signedIn.StatusCode, HttpStatusCode.OK);
            if (!html.Contains("<html", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("signed-in page is not HTML");
            }

            return $"signed out: {(int)anon.StatusCode} to the login page; admin cookie: 200, {html.Length} bytes of HTML";
        }
        finally
        {
            await Scratch.DropPgDatabaseAsync(conn);
        }
    }

    static string FindDashboard()
    {
        if (Environment.GetEnvironmentVariable("RUNWICK_DASH_DLL") is { Length: > 0 } env)
        {
            return env;
        }

        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
        {
            var dir = Path.Combine(d.FullName, "samples", "Runwick.Dashboard.Host", "bin");
            if (Directory.Exists(dir))
            {
                return Directory.GetFiles(dir, "Runwick.Dashboard.Host.dll", SearchOption.AllDirectories).OrderByDescending(File.GetLastWriteTimeUtc).First();
            }
        }

        throw new FileNotFoundException("Runwick.Dashboard.Host.dll not found; build the solution or set RUNWICK_DASH_DLL.");
    }
}
