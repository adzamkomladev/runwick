using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>DSH-01 stale-build guard, DSH-03 nav and <c>can</c> per preset, DSH-04 package contents and graph (21 §8.3).</summary>
[Trait("Item", "SP-024")]
public sealed class BuildAndPermissionTests
{
    static string Repo
    {
        get
        {
            for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            {
                if (File.Exists(Path.Combine(dir.FullName, "Runwick.slnx")))
                {
                    return dir.FullName;
                }
            }

            throw new InvalidOperationException("Repository root not found.");
        }
    }

    static string Project => Path.Combine(Repo, "src", "Runwick.Dashboard");

    [Fact]
    [Trait("Case", "DSH-01")]
    public void Committed_build_matches_client_sources()
    {
        var client = Path.Combine(Project, "ClientApp");
        var files = Directory.EnumerateFiles(client, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(client, f).Replace('\\', '/'))
            .Where(p => !p.StartsWith("node_modules/", StringComparison.Ordinal) && !p.StartsWith("e2e/", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToList();
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var path in files)
        {
            hash.AppendData(Encoding.UTF8.GetBytes(path));
            hash.AppendData([0]);
            hash.AppendData(File.ReadAllBytes(Path.Combine(client, path)));
            hash.AppendData([0]);
        }

        var committed = File.ReadAllText(Path.Combine(Project, "dist", "source.sha256")).Trim();
        Assert.True(committed == Convert.ToHexStringLower(hash.GetHashAndReset()),
            "ClientApp changed without a rebuild: run `npm ci && npm run build` in src/Runwick.Dashboard/ClientApp and commit dist/.");

        // The embedded build is the committed one, and the client sources stay free of HTML injection and eval (21 §7.1).
        Assert.Equal(committed, Encoding.UTF8.GetString(DashboardAssets.Embedded.Files["source.sha256"]).Trim());
        foreach (var path in files.Where(p => p.EndsWith(".jsx", StringComparison.Ordinal) || p.EndsWith(".js", StringComparison.Ordinal)))
        {
            var text = File.ReadAllText(Path.Combine(client, path));
            foreach (var banned in new[] { "dangerouslySetInnerHTML", "eval(", "new Function", "innerHTML", "DOMParser" })
            {
                Assert.False(text.Contains(banned, StringComparison.Ordinal), $"{path} contains {banned}");
            }
        }
    }

    public static TheoryData<string> Presets => [Users.Viewer, Users.Operator, Users.Administrator];

    [Theory]
    [Trait("Case", "DSH-03")]
    [MemberData(nameof(Presets))]
    public async Task Nav_and_can_map_match_the_preset_permissions(string user)
    {
        await using var f = await DriverFixture.StartAsync(Ns("dsh03"));
        await using var h = await DashHost.StartAsync(f);
        var grants = h.Grants[user];
        bool Has(string p) => grants.Any(g => g.Permission == p);
        var browser = h.Browser(user);
        var (response, _, page) = await browser.LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var can = page.Props().GetProperty("can");
        var expected = new Dictionary<string, string>
        {
            ["overview"] = ManagementPermissions.JobsRead, ["runs"] = ManagementPermissions.JobsRead, ["jobs"] = ManagementPermissions.JobsRead, ["pools"] = ManagementPermissions.CapacityRead, ["queues"] = ManagementPermissions.QueuesRead, ["workers"] = ManagementPermissions.CapacityRead,
            ["schedules"] = ManagementPermissions.SchedulesRead, ["scaling"] = ManagementPermissions.CapacityRead,
            ["settings"] = ManagementPermissions.SettingsRead, ["retention"] = ManagementPermissions.SettingsRead, ["audit"] = ManagementPermissions.AuditRead,
            ["cancel"] = ManagementPermissions.JobsCancel, ["retry"] = ManagementPermissions.JobsRetry, ["rerun"] = ManagementPermissions.JobsRerun,
            ["revealPayload"] = ManagementPermissions.JobsReadPayload, ["revealError"] = ManagementPermissions.JobsReadError,
            ["queueControl"] = ManagementPermissions.QueuesControl, ["capacityEdit"] = ManagementPermissions.CapacityEdit,
            ["scheduleControl"] = ManagementPermissions.SchedulesControl, ["scheduleTrigger"] = ManagementPermissions.SchedulesTrigger,
            ["scheduleEdit"] = ManagementPermissions.SchedulesEdit, ["retentionManage"] = ManagementPermissions.RetentionManage, ["dispatch"] = ManagementPermissions.JobsDispatch,
        };
        Assert.Equal(expected.Keys.Order(), can.EnumerateObject().Select(p => p.Name).Order());
        foreach (var (name, permission) in expected)
        {
            Assert.True(Has(permission) == can.GetProperty(name).GetBoolean(), $"{user}: can.{name}");
        }

        // Each page the nav would list is 200; each it would hide is 403. The server checks; the map is a hint.
        foreach (var (name, route) in new[] { ("overview", "/runwick/overview"), ("runs", "/runwick/runs"), ("jobs", "/runwick/jobs"), ("pools", "/runwick/pools"), ("queues", "/runwick/queues"), ("workers", "/runwick/workers"),
                     ("schedules", "/runwick/schedules"), ("scaling", "/runwick/scaling"), ("settings", "/runwick/settings"),
                     ("retention", "/runwick/retention"), ("audit", "/runwick/audit") })
        {
            var visit = await browser.VisitAsync(route);
            Assert.True((can.GetProperty(name).GetBoolean() ? HttpStatusCode.OK : HttpStatusCode.Forbidden) == visit.StatusCode,
                $"{user} {route}: {(int)visit.StatusCode} {await visit.Content.ReadAsStringAsync()}");
        }
    }

    [Fact]
    [Trait("Case", "DSH-04")]
    public async Task The_package_holds_the_build_and_no_client_sources()
    {
        var output = Directory.CreateTempSubdirectory("sp024-pack-").FullName;
        try
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet", ["pack", Path.Combine(Project, "Runwick.Dashboard.csproj"), "-o", output, "--nologo", "-v", "q"])
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            using var process = Process.Start(start)!;
            var log = await process.StandardOutput.ReadToEndAsync(TestContext.Current.CancellationToken);
            await process.WaitForExitAsync(TestContext.Current.CancellationToken);
            Assert.True(process.ExitCode == 0, log);

            using var package = ZipFile.OpenRead(Directory.GetFiles(output, "Runwick.Dashboard.*.nupkg").Single());
            var names = package.Entries.Select(e => e.FullName).ToList();
            Assert.DoesNotContain(names, n => n.Contains("ClientApp", StringComparison.OrdinalIgnoreCase) || n.Contains("node_modules", StringComparison.Ordinal));
            var dll = package.GetEntry("lib/net10.0/Runwick.Dashboard.dll")!;
            using var stream = new MemoryStream();
            await using (var entry = dll.Open())
            {
                await entry.CopyToAsync(stream, TestContext.Current.CancellationToken);
            }

            var assembly = System.Runtime.Loader.AssemblyLoadContext.Default.LoadFromStream(new MemoryStream(stream.ToArray()));
            var resources = assembly.GetManifestResourceNames();
            Assert.Contains("runwick-dashboard/manifest.json", resources);
            Assert.Contains("runwick-dashboard/" + DashboardAssets.Embedded.Entry, resources);
            Assert.Contains(resources, r => r.EndsWith(".css", StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(output, recursive: true);
        }

        var graph = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Repo, "eng", "package-graph.json"))).RootElement.GetProperty("packages");
        var dashboard = graph.GetProperty("Runwick.Dashboard");
        Assert.True(dashboard.GetProperty("exists").GetBoolean());
        Assert.Equal(["Runwick.AspNetCore"], dashboard.GetProperty("allowedProjectRefs").EnumerateArray().Select(e => e.GetString()));
        Assert.Equal(["Inertia.Net"], dashboard.GetProperty("allowedPackages").EnumerateArray().Select(e => e.GetString()));
        Assert.False(graph.TryGetProperty("Runwick.Dashboard.Spike", out _));
    }
}
