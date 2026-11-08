using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runwick.AspNetCore;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.AspNetCore.Tests;

public sealed record OtherJob(string Id) : IQueueJob;

/// <summary>Authenticates <c>X-Test-User: name</c>: a machine-client (non-cookie, non-Basic) scheme for tests.</summary>
internal sealed class TestHeaderHandler(IOptionsMonitor<AuthenticationSchemeOptions> o, ILoggerFactory l, UrlEncoder e)
    : AuthenticationHandler<AuthenticationSchemeOptions>(o, l, e)
{
    public const string Name = "Test";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync() =>
        Task.FromResult(Request.Headers["X-Test-User"].ToString() is { Length: > 0 } user
            ? AuthenticateResult.Success(new AuthenticationTicket(MgmtHost.Principal(user, Scheme.Name), Scheme.Name))
            : AuthenticateResult.NoResult());
}

/// <summary>A TestServer host with AddRunwick (the fixture's store), AddRunwickManagement and MapRunwickManagement.</summary>
internal sealed class MgmtHost : IAsyncDisposable
{
    public const string Prefix = "/runwick/api";

    MgmtHost(WebApplication app, ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> grants, string prefix)
    {
        App = app;
        Grants = grants;
        Base = prefix + "/v1/";
        Client = app.GetTestClient();
    }

    public WebApplication App { get; }

    public HttpClient Client { get; }

    public string Base { get; }

    /// <summary>User name → grants; read on every request (a removed grant applies at once).</summary>
    public ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>> Grants { get; }

    public IJobDispatcher Dispatcher => App.Services.GetRequiredService<IJobDispatcher>();

    public static ClaimsPrincipal Principal(string user, string scheme) =>
        new(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, user)], scheme));

    public static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherJob>().Named("test.other").ToQueue("other");
    }

    /// <summary>
    /// <paramref name="browser"/>: Cookies + Basic with antiforgery; otherwise the <c>Test</c> header scheme on a
    /// <c>ForMachineClients</c> group.
    /// </summary>
    public static async Task<MgmtHost> StartAsync(DriverFixture f, bool browser = false, string prefix = Prefix, Action<ManagementOptions>? options = null,
        Action<WebApplication>? middleware = null, Action<WebApplication>? routes = null, string identity = "mgmt", bool? machineGroup = null)
    {
        var grants = new ConcurrentDictionary<string, IReadOnlyList<ManagementGrant>>(StringComparer.Ordinal);
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseTestServer();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddSingleton<IFaultInjector>(f.Faults);
        b.Services.AddRunwick(r =>
        {
            f.UseStore(r.Application(f.Application), identity);
            Jobs(r);
        });
        b.Services.AddAuthorization();
        b.Services.AddAuthentication("Cookies")
            .AddCookie("Cookies", o => o.LoginPath = "/login-page")
            .AddRunwickBasic(o => o.Verifier = (user, password, _) => ValueTask.FromResult(password == "pw-" + user))
            .AddScheme<AuthenticationSchemeOptions, TestHeaderHandler>(TestHeaderHandler.Name, null);
        b.Services.AddRunwickManagement(m =>
        {
            m.AuthenticationSchemes(browser ? ["Cookies", RunwickBasicDefaults.Scheme] : [TestHeaderHandler.Name]);
            m.MapGrants(p => grants.TryGetValue(p.Identity!.Name!, out var g) ? g : []);
            // Rates are exercised by MGMT-10 only; other cases run with room to spare.
            m.Configure(options ?? (o => (o.ReadsPerMinute, o.MutationsPerMinute, o.RevealsPerMinute) = (100_000, 100_000, 100_000)));
        });
        var app = b.Build();
        middleware?.Invoke(app);
        app.MapGet("/login/{user}", async (HttpContext ctx, string user) =>
        {
            await ctx.SignInAsync("Cookies", Principal(user, "Cookies"));
            return Results.Ok();
        });
        app.MapGet("/hello", () => "hello").RequireAuthorization();
        app.MapGet("/public", () => "public");
        routes?.Invoke(app);
        var group = app.MapRunwickManagement(prefix);
        if (machineGroup ?? !browser)
        {
            group.ForMachineClients();
        }

        await app.StartAsync();
        return new MgmtHost(app, grants, prefix);
    }

    public HttpRequestMessage Request(HttpMethod method, string path, string? user = null, object? body = null)
    {
        var req = new HttpRequestMessage(method, path.StartsWith('/') ? path : Base + path);
        if (user is not null)
        {
            req.Headers.Add("X-Test-User", user);
        }

        if (body is not null)
        {
            req.Content = new StringContent(body is string s ? s : System.Text.Json.JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        }

        return req;
    }

    public async Task<(HttpStatusCode Status, JsonNode? Body, HttpResponseMessage Response)> SendAsync(HttpRequestMessage request)
    {
        var response = await Client.SendAsync(request, TestContext.Current.CancellationToken);
        var text = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        return (response.StatusCode, text.Length == 0 || response.Content.Headers.ContentType?.MediaType != "application/json" ? null : JsonNode.Parse(text), response);
    }

    public Task<(HttpStatusCode Status, JsonNode? Body, HttpResponseMessage Response)> GetAsync(string path, string? user) =>
        SendAsync(Request(HttpMethod.Get, path, user));

    public Task<(HttpStatusCode Status, JsonNode? Body, HttpResponseMessage Response)> PostAsync(string path, string? user, object? body = null) =>
        SendAsync(Request(HttpMethod.Post, path, user, body ?? new { }));

    public static AuthenticationHeaderValue Basic(string user, string password) =>
        new("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + password)));

    public async Task<Guid> DispatchAsync<T>(T job) where T : class, IQueueJob =>
        (await Dispatcher.DispatchAsync(job, cancellationToken: TestContext.Current.CancellationToken)).JobId.Value;

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await App.StopAsync();
        await App.DisposeAsync();
    }
}

internal static class Users
{
    public const string Admin = "admin", Scoped = "scoped", Viewer = "viewer", Nobody = "nobody";

    /// <summary>Every permission, every queue.</summary>
    public static IReadOnlyList<ManagementGrant> All { get; } = [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))];

    /// <summary>Every queue-scopable permission for queue <c>default</c> only.</summary>
    public static IReadOnlyList<ManagementGrant> DefaultQueueOnly { get; } =
        [.. ManagementPermissions.QueueScoped.Select(p => new ManagementGrant(p, new HashSet<string>(StringComparer.Ordinal) { "default" }))];

    public static void Seed(MgmtHost h)
    {
        h.Grants[Admin] = All;
        h.Grants[Scoped] = DefaultQueueOnly;
        h.Grants[Viewer] = ManagementRoles.Viewer;
    }
}
