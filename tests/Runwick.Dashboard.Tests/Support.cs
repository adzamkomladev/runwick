using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runwick.AspNetCore;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Dashboard.Tests;

public sealed record OtherJob(string Id) : IQueueJob;

internal static class Users
{
    public const string Admin = "admin", Administrator = "administrator", Operator = "operator", Viewer = "viewer", Scoped = "scoped", Nobody = "nobody";

    /// <summary>Every permission, every queue.</summary>
    public static IReadOnlyList<ManagementGrant> All { get; } = [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))];

    /// <summary>Every queue-scopable permission for queue <c>default</c> only.</summary>
    public static IReadOnlyList<ManagementGrant> DefaultQueueOnly { get; } =
        [.. ManagementPermissions.QueueScoped.Select(p => new ManagementGrant(p, new HashSet<string>(StringComparer.Ordinal) { "default" }))];

    public static ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> Seeded() => new(StringComparer.Ordinal)
    {
        [Admin] = All, [Administrator] = ManagementRoles.Administrator, [Operator] = ManagementRoles.Operator, [Viewer] = ManagementRoles.Viewer,
        [Scoped] = DefaultQueueOnly, [Nobody] = [],
    };
}

/// <summary>
/// A TestServer app with AddRunwick (the fixture's store), Cookies + Runwick Basic, AddRunwickManagement and the dashboard at
/// <c>/runwick</c>; optionally the JSON API at <c>/api</c>. Requests go over https://localhost (Basic refuses plain HTTP).
/// </summary>
internal sealed class DashHost : IAsyncDisposable
{
    public const string Hostile = "</script><script>alert(1)</script>\"'&<img src=x onerror=alert(2)>\u2028end";

    DashHost(WebApplication app, ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> grants, DriverFixture? f)
    {
        App = app;
        Grants = grants;
        Fixture = f;
    }

    public WebApplication App { get; }

    public DriverFixture? Fixture { get; }

    /// <summary>User name → grants, read on every request (a removed grant applies at once).</summary>
    public ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> Grants { get; }

    public static ClaimsPrincipal Principal(string user, string scheme, string? display = null) =>
        new(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, display ?? user)], scheme));

    public static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherJob>().Named("test.other").ToQueue("other");
    }

    public static async Task<DashHost> StartAsync(DriverFixture? f, Action<WebApplicationBuilder>? services = null, Action<WebApplication>? before = null,
        Action<WebApplication>? routes = null, bool api = false, Action<ManagementOptions>? limits = null, Action<DashboardOptions>? mount = null,
        string path = "/runwick", bool map = true, string? contentRoot = null, bool dashboard = true)
    {
        var grants = Users.Seeded();
        var b = WebApplication.CreateSlimBuilder(new WebApplicationOptions { EnvironmentName = "Production", ContentRootPath = contentRoot ?? AppContext.BaseDirectory });
        b.WebHost.UseTestServer();
        b.Logging.ClearProviders();
        if (f is not null)
        {
            b.Services.AddSingleton<IFaultInjector>(f.Faults);
            b.Services.AddRunwick(r =>
            {
                f.UseStore(r.Application(f.Application), "dash");
                Jobs(r);
            });
        }

        b.Services.AddAuthentication("Cookies")
            .AddCookie("Cookies", o => o.LoginPath = "/login")
            .AddRunwickBasic(o => o.Verifier = (user, password, _) => ValueTask.FromResult(password == "pw-" + user));
        b.Services.AddRunwickManagement(m =>
        {
            m.AuthenticationSchemes(RunwickBasicDefaults.Scheme, "Cookies");
            m.MapGrants(p => grants.TryGetValue(p.FindFirstValue(ClaimTypes.NameIdentifier)!, out var g) ? g : []);
            // Rates are exercised by MGMT-10 only.
            m.Configure(limits ?? (o => (o.ReadsPerMinute, o.MutationsPerMinute, o.RevealsPerMinute) = (100_000, 100_000, 100_000)));
        });
        if (dashboard)
        {
            b.Services.AddRunwickDashboard("default", mount ?? (_ => { }));
        }

        services?.Invoke(b);
        var app = b.Build();
        before?.Invoke(app);
        if (map && dashboard)
        {
            app.MapRunwickDashboard(path);
        }

        app.MapGet("/login/{user}", async (HttpContext ctx, string user, string? display) =>
        {
            await ctx.SignInAsync("Cookies", Principal(user, "Cookies", display));
            return Results.Ok();
        });
        routes?.Invoke(app);
        if (api)
        {
            app.MapRunwickManagement("/api");
        }

        await app.StartAsync(TestContext.Current.CancellationToken);
        return new DashHost(app, grants, f);
    }

    public Browser Browser(string? user = null, bool https = true)
    {
        var client = App.GetTestClient();
        client.BaseAddress = new Uri(https ? "https://localhost/" : "http://localhost/");
        return new Browser(client, user);
    }

    public IJobDispatcher Dispatcher => App.Services.GetRequiredService<IJobDispatcher>();

    public async Task<Guid> DispatchAsync<T>(T job) where T : class, IQueueJob =>
        (await Dispatcher.DispatchAsync(job, cancellationToken: TestContext.Current.CancellationToken)).JobId.Value;

    public Task<long> AuditsAsync(Guid? commandId = null) => commandId is { } id
        ? Fixture!.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", id))
        : Fixture!.CountAsync("SELECT count(*) FROM runwick.management_audit");

    public async ValueTask DisposeAsync()
    {
        await App.StopAsync();
        await App.DisposeAsync();
    }
}

internal static partial class Support
{
    public static string? Header(this HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) || response.Content.Headers.TryGetValues(name, out values)
            ? string.Join(", ", values)
            : null;

    [GeneratedRegex("<meta name=\"runwick-csrf\" content=\"([^\"]*)\">")]
    public static partial Regex CsrfMeta();

    [GeneratedRegex("<script data-page=\"app\" type=\"application/json\">(.*?)</script><div id=\"app\">", RegexOptions.Singleline)]
    public static partial Regex PageScript();

    public static JsonElement Props(this JsonElement page) => page.GetProperty("props");

    public static Action<HttpRequestMessage> Partial(string component, string only) => r =>
    {
        r.Headers.Add("X-Inertia-Partial-Component", component);
        r.Headers.Add("X-Inertia-Partial-Data", only);
    };

    /// <summary>Runs the next job of queue <c>default</c> on a store-level client and fails it permanently.</summary>
    public static async Task<Guid> FailNextAsync(this DriverFixture f, string code, string message)
    {
        var c = await f.OpenClient("local", DashHost.Jobs);
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var finished = await c.FinishAsync(lease, AttemptOutcome.PermanentFailure, error: new AttemptError(code, "System.Exception", message, null));
        Assert.Equal(StoreStatus.Applied, finished.Status);
        return lease.AttemptId;
    }

    /// <summary>Inserts a scaling event as the autoscaler would (11 §6).</summary>
    public static Task ScalingEventAsync(this DriverFixture f, DateTimeOffset at, string pool = "default", string reason = "scale_up", Guid? instance = null,
        Guid? eventId = null) =>
        f.Kind != StoreKind.Postgres ? f.RecordScalingEventAsync(pool, reason, instance) : f.ExecAsync("""
            INSERT INTO runwick.scaling_events (application, event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason, snapshot_at, created_at)
            VALUES (@a, @e, @i, @p, 1, 2, 2, NULL, @r, NULL, @t)
            """, ("a", f.Application), ("e", eventId ?? Guid.CreateVersion7()), ("i", instance ?? Guid.Empty), ("p", pool), ("r", reason), ("t", at));

    /// <summary>Reserves and begins the next job of queue <c>default</c>: the job is now owned by a worker.</summary>
    public static async Task<(TestClient Client, LeaseHandle Lease)> RunNextAsync(this DriverFixture f)
    {
        var c = await f.OpenClient("local", DashHost.Jobs);
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        return (c, lease);
    }

    /// <summary>The same files with the entry changed, as a rebuild produces: new hash, new file name, new version.</summary>
    public static DashboardAssets Rebuild(DashboardAssets old)
    {
        var entry = old.Entry.Replace(".js", "-v2.js", StringComparison.Ordinal);
        var files = old.Files.Where(f => f.Key != old.Entry && f.Key != "manifest.json").ToDictionary();
        files[entry] = [.. old.Files[old.Entry], .. "\n// v2"u8.ToArray()];
        files["manifest.json"] = Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(old.Files["manifest.json"]).Replace(old.Entry, entry, StringComparison.Ordinal));
        return new DashboardAssets(files);
    }

    public static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];
}

/// <summary>Sends requests the way the browser and the Inertia client do, keeping cookies.</summary>
internal sealed class Browser(HttpClient client, string? user)
{
    readonly Dictionary<string, string> _cookies = new(StringComparer.Ordinal);

    public HttpClient Client { get; } = client;

    public string? User { get; set; } = user;

    public string? Csrf { get; set; }

    public string? Version { get; set; }

    public Dictionary<string, string> Cookies => _cookies;

    /// <summary>A full page load: reads the token and the version from the HTML.</summary>
    public async Task<(HttpResponseMessage Response, string Body, JsonElement Page)> LoadAsync(string url)
    {
        var response = await SendAsync(HttpMethod.Get, url, inertia: false);
        var body = await response.Content.ReadAsStringAsync();
        var page = default(JsonElement);
        if (Support.PageScript().Match(body) is { Success: true } script)
        {
            page = JsonDocument.Parse(script.Groups[1].Value).RootElement.Clone();
            Version = page.GetProperty("version").GetString();
        }

        if (Support.CsrfMeta().Match(body) is { Success: true } meta)
        {
            Csrf = WebUtility.HtmlDecode(meta.Groups[1].Value);
        }

        return (response, body, page);
    }

    /// <summary>An Inertia visit (XHR).</summary>
    public Task<HttpResponseMessage> VisitAsync(string url, Action<HttpRequestMessage>? configure = null) =>
        SendAsync(HttpMethod.Get, url, inertia: true, configure: configure);

    public async Task<JsonElement> VisitJsonAsync(string url, Action<HttpRequestMessage>? configure = null)
    {
        var response = await VisitAsync(url, configure);
        Assert.True(response.StatusCode == HttpStatusCode.OK, $"{url}: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
        return await JsonAsync(response);
    }

    /// <summary>An Inertia control post with this browser's token and a command ID.</summary>
    public Task<HttpResponseMessage> PostAsync(string url, object body, Guid? commandId = null, Action<HttpRequestMessage>? configure = null) =>
        SendAsync(HttpMethod.Post, url, inertia: true, body: body as string ?? JsonSerializer.Serialize(body, JsonSerializerOptions.Web), configure: request =>
        {
            if (commandId is { } id)
            {
                request.Headers.Add("X-Runwick-Command-Id", id.ToString());
            }

            if (Csrf is not null)
            {
                request.Headers.Add("X-Runwick-Csrf", Csrf);
            }

            configure?.Invoke(request);
        });

    /// <summary>A reveal: plain fetch, no Inertia header.</summary>
    public Task<HttpResponseMessage> RevealAsync(string url) =>
        SendAsync(HttpMethod.Post, url, inertia: false, body: "{}", configure: r =>
        {
            if (Csrf is not null)
            {
                r.Headers.Add("X-Runwick-Csrf", Csrf);
            }
        });

    public async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, bool inertia, string? body = null, Action<HttpRequestMessage>? configure = null)
    {
        using var request = new HttpRequestMessage(method, url);
        if (User is not null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User}:pw-{User}")));
        }

        if (inertia)
        {
            request.Headers.Add("X-Inertia", "true");
            if (Version is not null)
            {
                request.Headers.Add("X-Inertia-Version", Version);
            }
        }

        if (_cookies.Count > 0)
        {
            request.Headers.Add("Cookie", string.Join("; ", _cookies.Select(c => $"{c.Key}={c.Value}")));
        }

        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        configure?.Invoke(request);
        var response = await Client.SendAsync(request, TestContext.Current.CancellationToken);
        if (response.Headers.TryGetValues("Set-Cookie", out var cookies))
        {
            foreach (var cookie in cookies)
            {
                var pair = cookie.Split(';')[0].Split('=', 2);
                _cookies[pair[0]] = pair[1];
            }
        }

        return response;
    }

    public static async Task<JsonElement> JsonAsync(HttpResponseMessage response) =>
        JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.Clone();
}
