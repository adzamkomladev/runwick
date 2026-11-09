// Usage: Runwick.Dashboard.Host [--build v2]
//   RUNWICK_DASH_PG    PostgreSQL connection string (required); the schema is migrated at start
//   RUNWICK_DASH_PORT  port on 127.0.0.1 (default 5791)
//   RUNWICK_DASH_DEMO  "1" adds a steady stream of mixed jobs (imports, emails, thumbnails; some slow, some failing) so the
//                      Overview charts have data to show. Off by default: the browser flows never set it.
// A host with its own Inertia.Net page at /ops/ and the Runwick dashboard at /ops/runwick, mounted before the host's UseInertia().
// Users sign in with a cookie at /ops/login (admin, operator, viewer). "--build v2" serves a rebuilt client, standing in for a deploy.
// The data is seeded on every start into a fresh application name, including a job whose payload and error code are hostile text.
using System.Security.Claims;
using Inertia.Net;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Runwick;
using Runwick.Dashboard;
using Runwick.Management;
using Runwick.Postgres.Migrations;

const string Hostile = "</script><script>alert(1)</script>\"'&<img src=x onerror=alert(2)>\u2028end";
var conn = Environment.GetEnvironmentVariable("RUNWICK_DASH_PG") ?? throw new InvalidOperationException("Set RUNWICK_DASH_PG.");
var port = Environment.GetEnvironmentVariable("RUNWICK_DASH_PORT") ?? "5791";
var demo = Environment.GetEnvironmentVariable("RUNWICK_DASH_DEMO") == "1";
await RunwickPostgresMigrations.MigrateAsync(conn);

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
builder.Services.AddInertia(o =>
{
    o.Version = "host-1";
    o.Share("hostUser", _ => "host-secret-user");
});
builder.Services.AddRunwick(r =>
{
    r.Application("dashhost_" + Guid.NewGuid().ToString("N")[..8]).UsePostgres(conn);
    r.AddJob<Report>().Named("sample.report").ToQueue("reports");
    r.AddJob<Flaky>().Named("sample.flaky").ToQueue("default");
    r.AddHandler<Report, ReportHandler>();
    r.AddHandler<Flaky, FlakyHandler>();
    r.AddWorker(w =>
    {
        w.Pool("main", p => p.Queue("default", 1).FixedConcurrency(2)); // "reports" has no worker: its jobs stay pending
        if (demo)
        {
            w.Pool("demo", p => p.Queue("imports", 1).Queue("emails", 1).Queue("media", 1).FixedConcurrency(3));
        }
    });
    if (demo)
    {
        r.AddJob<Import>().Named("demo.import").ToQueue("imports");
        r.AddJob<Email>().Named("demo.email").ToQueue("emails");
        r.AddJob<Thumbnail>().Named("demo.thumbnail").ToQueue("media");
        r.AddHandler<Import, DemoHandler>();
        r.AddHandler<Email, DemoHandler>();
        r.AddHandler<Thumbnail, DemoHandler>();
    }
});
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(o => o.LoginPath = "/login");
// The presets leave out payload, error and audit reads (16 §1); the sample's staff get them explicitly.
IReadOnlyList<ManagementGrant> WithDisclosure(IReadOnlyList<ManagementGrant> preset) => [.. preset,
    new(ManagementPermissions.JobsReadPayload), new(ManagementPermissions.JobsReadError), new(ManagementPermissions.AuditRead)];
var grants = new Dictionary<string, IReadOnlyList<ManagementGrant>>(StringComparer.Ordinal)
{
    ["admin"] = WithDisclosure(ManagementRoles.Administrator), ["operator"] = WithDisclosure(ManagementRoles.Operator), ["viewer"] = ManagementRoles.Viewer,
};
builder.Services.AddRunwickManagement(m => m
    .AuthenticationSchemes(CookieAuthenticationDefaults.AuthenticationScheme)
    .MapGrants(p => grants.GetValueOrDefault(p.FindFirstValue(ClaimTypes.NameIdentifier) ?? "", [])));
builder.Services.AddRunwickDashboard("default", d =>
{
    if (args.Contains("v2"))
    {
        d.Assets = SampleBuild.Rebuilt(DashboardAssets.Embedded); // internal hook, visible to this sample only
    }
});

var app = builder.Build();
app.UsePathBase("/ops");
app.UseAuthentication(); // after UsePathBase, so login redirects carry /ops
app.MapRunwickDashboard("/runwick"); // before UseInertia(): the host's version check never sees dashboard requests
app.UseInertia();
app.MapGet("/", () => Inertia.Net.Inertia.Render("Home", new InertiaProps { ["greeting"] = "host page" }));
app.MapGet("/healthz", () => "ok");
app.MapGet("/login", (string? ReturnUrl) => Results.Content($"""
    <!DOCTYPE html><html><head><title>Sign in</title></head><body><h1>Sign in</h1>
    <ul><li><a href="login/admin?returnUrl={Uri.EscapeDataString(ReturnUrl ?? "")}">admin</a></li>
    <li><a href="login/operator?returnUrl={Uri.EscapeDataString(ReturnUrl ?? "")}">operator</a></li>
    <li><a href="login/viewer?returnUrl={Uri.EscapeDataString(ReturnUrl ?? "")}">viewer</a></li></ul></body></html>
    """, "text/html"));
app.MapGet("/login/{user}", async (HttpContext ctx, string user, string? returnUrl) =>
{
    var display = user == "operator" ? Hostile : user; // the operator's display name is hostile text too
    await ctx.SignInAsync(new ClaimsPrincipal(new ClaimsIdentity(
        [new Claim(ClaimTypes.NameIdentifier, user), new Claim(ClaimTypes.Name, display)], CookieAuthenticationDefaults.AuthenticationScheme)));
    return Results.LocalRedirect(string.IsNullOrEmpty(returnUrl) || !returnUrl.StartsWith("/ops/", StringComparison.Ordinal) ? "/ops/runwick" : returnUrl);
});
app.MapGet("/logout", async (HttpContext ctx) =>
{
    await ctx.SignOutAsync();
    return Results.LocalRedirect("/ops/");
});

await app.StartAsync();
var dispatcher = app.Services.GetRequiredService<IJobDispatcher>();
var later = new DispatchOptions { NotBefore = DateTimeOffset.UtcNow.AddDays(1) };
for (var i = 0; i < 3; i++)
{
    await dispatcher.DispatchAsync(new Report($"ops-{i}@example.com"), later);
}

await dispatcher.DispatchAsync(new Report(Hostile), later); // newest, so first in the list: the reveal rate limit allows few tries
await dispatcher.DispatchAsync(new Flaky("fails with a hostile error code"));
if (demo)
{
    _ = Task.Run(async () =>
    {
        // Bursty arrivals, a little faster than the three demo slots drain, so pending work builds and clears.
        for (var n = 0; !app.Lifetime.ApplicationStopping.IsCancellationRequested; n++)
        {
            IQueueJob job = (n % 5) switch { 0 or 1 => new Email(n), 2 or 3 => new Thumbnail(n), _ => new Import(n) };
            try { await dispatcher.DispatchAsync(job); } catch (Exception ex) { Console.WriteLine($"demo dispatch: {ex.Message}"); }
            await Task.Delay(Random.Shared.Next(n % 40 < 20 ? 150 : 600, n % 40 < 20 ? 500 : 1500));
        }
    });
}

Console.WriteLine($"ready http://127.0.0.1:{port}/ops/runwick");
await app.WaitForShutdownAsync();

sealed record Report(string To) : IQueueJob;

sealed record Flaky(string Note) : IQueueJob;

sealed class ReportHandler : IJobHandler<Report>
{
    public Task HandleAsync(Report job, CancellationToken cancellationToken) => Task.CompletedTask;
}

sealed class FlakyHandler : IJobHandler<Flaky>
{
    public Task HandleAsync(Flaky job, CancellationToken cancellationToken) =>
        throw new PermanentJobFailureException("</script><img src=x onerror=alert(4)>\u2028", "error-text-secret");
}

sealed record Import(int N) : IQueueJob;

sealed record Email(int N) : IQueueJob;

sealed record Thumbnail(int N) : IQueueJob;

/// <summary>Demo work: 50 ms to 1.5 s, and about one in twelve fails (imports more often).</summary>
sealed class DemoHandler : IJobHandler<Import>, IJobHandler<Email>, IJobHandler<Thumbnail>
{
    public Task HandleAsync(Import job, CancellationToken ct) => RunAsync(job.N, 5, ct);

    public Task HandleAsync(Email job, CancellationToken ct) => RunAsync(job.N, 14, ct);

    public Task HandleAsync(Thumbnail job, CancellationToken ct) => RunAsync(job.N, 20, ct);

    static async Task RunAsync(int n, int failEvery, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(50, 1500), ct);
        if (n % failEvery == 3)
        {
            throw new PermanentJobFailureException("demo.upstream_unavailable", "The demo upstream refused the call.");
        }
    }
}

static class SampleBuild
{
    /// <summary>The embedded build with its entry renamed and changed, as a deploy of a new client produces.</summary>
    public static DashboardAssets Rebuilt(DashboardAssets old)
    {
        var entry = old.Entry.Replace(".js", "-v2.js", StringComparison.Ordinal);
        var files = old.Files.Where(f => f.Key != old.Entry && f.Key != "manifest.json").ToDictionary();
        files[entry] = [.. old.Files[old.Entry], .. "\n// v2"u8.ToArray()];
        files["manifest.json"] = System.Text.Encoding.UTF8.GetBytes(
            System.Text.Encoding.UTF8.GetString(old.Files["manifest.json"]).Replace(old.Entry, entry, StringComparison.Ordinal));
        return new DashboardAssets(files);
    }
}
