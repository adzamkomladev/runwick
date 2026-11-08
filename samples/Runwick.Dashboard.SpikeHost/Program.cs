// Usage: Runwick.Dashboard.SpikeHost [--build v2]   (listens on ASPNETCORE_URLS)
// The host serves its own Inertia.Net page at /ops/ and mounts the dashboard at /ops/runwick (PathBase /ops).
// "--build v2" serves a rebuilt client (new hashes, new version), standing in for a deploy.
using Inertia.Net;
using Microsoft.AspNetCore.Authorization;
using Runwick.Dashboard.Spike;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.Services.AddInertia(o =>
{
    o.Version = "host-1";
    o.Share("hostUser", _ => "host-secret-user");
});
builder.Services.AddRunwickDashboardSpike();
builder.Services.AddAuthentication().AddScheme<SpikeBasicOptions, SpikeBasicHandler>("runwick-basic", o =>
{
    o.Users["alice"] = ("pw-alice", ["view", "operate"]);
    o.Users["victor"] = ("pw-victor", ["view"]);
});

var app = builder.Build();
app.UsePathBase("/ops");
var data = new InMemoryDashboardSpikeData()
    .Add("1", "</script><script>alert(1)</script>\"'&<img src=x onerror=alert(2)>", "card=4111-1111")
    .Add("2", "nightly-report", "{\"to\":\"ops@example.com\"}");
app.UseRunwickDashboardSpike("/runwick", o =>
{
    o.MountId = "ops";
    o.Title = "Runwick (spike)";
    o.Data = data;
    o.ViewPolicy = new AuthorizationPolicyBuilder("runwick-basic").RequireClaim("runwick:permission", "view", "operate").Build();
    o.OperatePolicy = new AuthorizationPolicyBuilder("runwick-basic").RequireClaim("runwick:permission", "operate").Build();
    o.Assets = args.Contains("v2") ? Rebuilt(DashboardAssets.Embedded) : DashboardAssets.Embedded;
});
app.UseInertia(); // after the mount: the host's version check and state store never see dashboard requests
app.MapGet("/", () => Inertia.Net.Inertia.Render("Home", new InertiaProps { ["greeting"] = "host page" }));
app.MapGet("/healthz", () => "ok");
app.Run();

static DashboardAssets Rebuilt(DashboardAssets old)
{
    var entry = old.Entry.Replace(".js", "-v2.js", StringComparison.Ordinal);
    var files = old.Files.Where(f => f.Key != old.Entry && f.Key != "manifest.json").ToDictionary();
    files[entry] = [.. old.Files[old.Entry], .. "\n// v2"u8.ToArray()];
    files["manifest.json"] = System.Text.Encoding.UTF8.GetBytes(System.Text.Encoding.UTF8.GetString(old.Files["manifest.json"]).Replace(old.Entry, entry, StringComparison.Ordinal));
    return new DashboardAssets(files);
}
