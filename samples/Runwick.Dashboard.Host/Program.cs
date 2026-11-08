// Usage: Runwick.Dashboard.Host [--build v2]
//   RUNWICK_DASH_PG    PostgreSQL connection string (required); the schema is migrated at start
//   RUNWICK_DASH_PORT  port on 127.0.0.1 (default 5791)
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
    r.AddWorker(w => w.Pool("main", p => p.Queue("default", 1).FixedConcurrency(2))); // "reports" has no worker: its jobs stay pending
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
