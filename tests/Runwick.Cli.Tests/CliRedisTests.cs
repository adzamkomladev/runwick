using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Runwick.Cli.Tests;

/// <summary>
/// CLI-08, Redis part: <c>migrate</c>, <c>doctor</c> and <c>store status</c> with <c>--provider redis</c>, each test against its own
/// throwaway <c>redis:8.2-alpine</c> container (name <c>runwick-redis-cliredis-*</c>, removed afterwards), with no management server.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-032")]
public sealed class CliRedisTests
{
    /// <summary>redis-aof-always-single-primary-v1 (22 §2.3), as the Redis test harness starts it.</summary>
    static readonly string[] Profile =
    [
        "--appendonly", "yes", "--appendfsync", "always", "--no-appendfsync-on-rewrite", "no",
        "--maxmemory-policy", "noeviction", "--aof-load-truncated", "no", "--save", "",
    ];

    static async Task<CliRun> RunAsync(string configuration, params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var env = new CliEnvironment
        {
            Out = output,
            Error = error,
            Variables = new Dictionary<string, string> { ["RDCS"] = configuration },
            ConfigPath = Path.Combine(Path.GetTempPath(), "rw-cli-no-config-" + Guid.NewGuid().ToString("N") + ".json"),
            PromptSecret = () => throw new InvalidOperationException("The CLI prompted."),
        };
        var run = new CliRun(await RunwickCli.RunAsync(["--agent", .. args, "--provider", "redis", "--connection-env", "RDCS"], env), output.ToString(), error.ToString());
        Assert.Empty(run.Error);
        Assert.DoesNotContain(configuration, run.Out, StringComparison.Ordinal);
        Schemas.AssertValidEnvelope(run.Out.Trim());
        return run;
    }

    static void Fails(CliRun r, string code, int exit) =>
        Assert.True((r.Code, r.Exit) == (code, exit), $"expected {code}/{exit}, got exit {r.Exit}\n{r.Out}");

    static Dictionary<string, (string Status, string? Detail)> Checks(CliRun r) =>
        r.Result!["checks"]!.AsArray().ToDictionary(c => c!["name"]!.GetValue<string>(), c => (c!["status"]!.GetValue<string>(), c["detail"]?.GetValue<string>()));

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task Migrate_applies_once_and_doctor_reports_the_layout()
    {
        await using var redis = await Container.StartAsync(Profile);

        var before = await RunAsync(redis.Configuration, "doctor");
        Assert.Equal(0, before.Exit);
        Assert.Equal(("warn", "store not initialized: run migrate"), Checks(before)["store.migration"]);

        var first = await RunAsync(redis.Configuration, "migrate");
        Assert.Equal(0, first.Exit);
        Assert.Equal(1, first.Result!["layoutRevision"]!.GetValue<long>());
        Assert.Equal(3, first.Result["appliedPhases"]!.AsArray().Count);
        var again = await RunAsync(redis.Configuration, "migrate");
        Assert.Equal(0, again.Exit);
        Assert.Empty(again.Result!["appliedPhases"]!.AsArray());
        var storeId = first.Result["storeId"]!.GetValue<Guid>();
        Assert.Equal(storeId, again.Result["storeId"]!.GetValue<Guid>());

        var doctor = await RunAsync(redis.Configuration, "doctor");
        Assert.Equal(0, doctor.Exit);
        var checks = Checks(doctor);
        Assert.Equal(("pass", "revision 1"), checks["store.migration"]);
        Assert.Equal("pass", checks["store.settings"].Status);
        Assert.Contains("appendfsync=always", checks["store.settings"].Detail, StringComparison.Ordinal);
        Assert.Equal("unverified", checks["api.reachable"].Status);
        Assert.Equal(1, doctor.Result!["cliSchemaRevision"]!.GetValue<long>());

        var status = await RunAsync(redis.Configuration, "store", "status");
        Assert.Equal(0, status.Exit);
        Assert.Equal(storeId, status.Result!["storeId"]!.GetValue<Guid>());
        Assert.Equal("normal", status.Result["migrationMode"]!.GetValue<string>());
        Fails(await RunAsync(redis.Configuration, "store", "admission", "--mode", "drain", "--application", "nobody"), "application_not_registered", 1);

        await redis.CliAsync("HSET", "rw:store", "digest", "00");
        var changed = await RunAsync(redis.Configuration, "doctor");
        Fails(changed, "check_failed", 1);
        Assert.Contains("schema_validation_failed", Checks(changed)["store.migration"].Detail, StringComparison.Ordinal);

        await redis.CliAsync("HSET", "rw:store", "layout", "2");
        var ahead = await RunAsync(redis.Configuration, "doctor");
        Fails(ahead, "check_failed", 1);
        Assert.Contains("CLI older than store", Checks(ahead)["store.migration"].Detail, StringComparison.Ordinal);
        Fails(await RunAsync(redis.Configuration, "migrate"), "migration_unknown", 1);
    }

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task A_server_outside_the_profile_fails_doctor_and_migrate()
    {
        string[] aofOff = [.. Profile];
        aofOff[1] = "no";
        await using var redis = await Container.StartAsync(aofOff);

        var doctor = await RunAsync(redis.Configuration, "doctor");
        Fails(doctor, "check_failed", 1);
        var settings = Checks(doctor)["store.settings"];
        Assert.Equal("fail", settings.Status);
        Assert.StartsWith("store_profile_mismatch: appendonly=no", settings.Detail, StringComparison.Ordinal);

        var migrate = await RunAsync(redis.Configuration, "migrate");
        Fails(migrate, "store_profile_mismatch", 1);
        Assert.False(migrate.Envelope["error"]!["retryable"]!.GetValue<bool>());
        Assert.Equal("0", await redis.CliAsync("EXISTS", "rw:store"));
    }

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task An_unreachable_server_is_store_unavailable()
    {
        var nowhere = "127.0.0.1:" + Container.FreePort() + ",connectTimeout=1000";

        var migrate = await RunAsync(nowhere, "migrate");
        Fails(migrate, "store_unavailable", 4);
        Assert.True(migrate.Envelope["error"]!["retryable"]!.GetValue<bool>());

        var doctor = await RunAsync(nowhere, "doctor");
        Fails(doctor, "check_failed", 1);
        Assert.Equal(("fail", "store_unavailable"), Checks(doctor)["store.migration"]);
        Assert.Equal("unverified", Checks(doctor)["store.settings"].Status);

        Fails(await RunAsync("a:1,b:2", "migrate"), "topology_unsupported", 1);
    }

    /// <summary>A Redis container this test owns; only containers created here are ever touched.</summary>
    sealed class Container : IAsyncDisposable
    {
        const string Image = "redis:8.2-alpine";

        readonly string _name = "runwick-redis-cliredis-" + Guid.NewGuid().ToString("N")[..8];

        public string Configuration { get; private set; } = "";

        public static async Task<Container> StartAsync(string[] args)
        {
            var c = new Container();
            var port = FreePort();
            c.Configuration = "127.0.0.1:" + port;
            var run = await Docker(["run", "-d", "--name", c._name, "-p", $"127.0.0.1:{port}:6379", Image, "redis-server", .. args]);
            if (run.Code != 0)
            {
                await c.DisposeAsync();
                throw new InvalidOperationException("docker run failed: " + run.Output);
            }

            var sw = Stopwatch.StartNew();
            while (!(await c.CliAsync("ping")).Contains("PONG", StringComparison.Ordinal))
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(30))
                {
                    await c.DisposeAsync();
                    throw new TimeoutException("Redis did not become ready.");
                }

                await Task.Delay(100);
            }

            return c;
        }

        public async Task<string> CliAsync(params string[] command) => (await Docker(["exec", _name, "redis-cli", .. command])).Output.Trim();

        public async ValueTask DisposeAsync() => await Docker("rm", "-f", "-v", _name);

        public static int FreePort()
        {
            using var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            return ((IPEndPoint)l.LocalEndpoint).Port;
        }

        static async Task<(int Code, string Output)> Docker(params string[] args)
        {
            var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var a in args)
            {
                psi.ArgumentList.Add(a);
            }

            using var p = Process.Start(psi)!;
            var stdout = p.StandardOutput.ReadToEndAsync();
            var stderr = p.StandardError.ReadToEndAsync();
            await p.WaitForExitAsync();
            return (p.ExitCode, await stdout + await stderr);
        }
    }
}
