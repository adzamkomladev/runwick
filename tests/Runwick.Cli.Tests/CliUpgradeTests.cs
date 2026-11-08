using Runwick.Harness.Jobs;
using Runwick.Harness.Store;

namespace Runwick.Cli.Tests;

/// <summary>The <c>store status</c>, <c>store admission</c> and <c>store reidentify</c> commands (23 §3.5) on PostgreSQL and SQLite files, with no server.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-026")]
public sealed class CliUpgradeTests
{
    public static TheoryData<StoreKind> Kinds => [StoreKind.Postgres, StoreKind.Sqlite];

    static async Task<CliRun> RunAsync(string connection, params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var env = new CliEnvironment
        {
            Out = output,
            Error = error,
            Variables = new Dictionary<string, string> { ["STORECS"] = connection },
            ConfigPath = Path.Combine(Path.GetTempPath(), "no-runwick-cli-config-" + Guid.NewGuid().ToString("N") + ".json"),
            PromptSecret = () => throw new InvalidOperationException("The CLI prompted."),
        };
        var run = new CliRun(await RunwickCli.RunAsync(["--agent", .. args], env), output.ToString(), error.ToString());
        Assert.Empty(run.Error);
        Schemas.AssertValidEnvelope(run.Out.Trim());
        return run;
    }

    static string[] Store(StoreKind kind, params string[] command) =>
        [.. command, "--provider", kind == StoreKind.Postgres ? "postgres" : "sqlite", "--connection-env", "STORECS"];

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CLI-UPG")]
    public async Task Status_admission_and_reidentify_run_the_upgrade_runbook_steps(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("cliupg_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var cs = f.AdminConnectionString;
        var app = f.Application;

        var status = await RunAsync(cs, Store(kind, "store", "status"));
        Assert.Equal(0, status.Exit);
        Assert.Equal("open", status.Result!["applications"]![0]!["admissionMode"]!.GetValue<string>());
        Assert.StartsWith("unverified", status.Result["quiescence"]!.GetValue<string>(), StringComparison.Ordinal);
        var storeId = status.Result["storeId"]!.GetValue<Guid>();

        Fails(await RunAsync(cs, [.. Store(kind, "store", "admission"), "--mode", "maintenance"]), "drain_first", 1);
        var drain = await RunAsync(cs, [.. Store(kind, "store", "admission"), "--mode", "drain", "--application", app]);
        Assert.Equal(0, drain.Exit);
        Assert.True(drain.Result!["changes"]![0]!["changed"]!.GetValue<bool>());
        Fails(await RunAsync(cs, [.. Store(kind, "store", "admission"), "--mode", "drain", "--application", "nobody"]), "application_not_registered", 1);

        Fails(await RunAsync(cs, Store(kind, "store", "reidentify")), "usage_error", 2);
        Fails(await RunAsync(cs, [.. Store(kind, "store", "reidentify"), "--confirm"]), "store_not_quiescent", 1);
        Assert.Equal(0, (await RunAsync(cs, [.. Store(kind, "store", "admission"), "--mode", "maintenance"])).Exit);
        var changed = await RunAsync(cs, [.. Store(kind, "store", "reidentify"), "--confirm"]);
        Assert.Equal(0, changed.Exit);
        Assert.Equal(storeId, changed.Result!["oldStoreId"]!.GetValue<Guid>());

        Assert.Equal(0, (await RunAsync(cs, [.. Store(kind, "store", "admission"), "--mode", "open"])).Exit);
        var after = await RunAsync(cs, Store(kind, "store", "status"));
        Assert.Equal(changed.Result["newStoreId"]!.GetValue<Guid>(), after.Result!["storeId"]!.GetValue<Guid>());
        Assert.Equal("open", after.Result["applications"]![0]!["admissionMode"]!.GetValue<string>());

        Fails(await RunAsync("Host=127.0.0.1;Port=1;Timeout=1", Store(StoreKind.Postgres, "store", "status")), "store_unavailable", 4);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CLI-UPG")]
    public async Task Activate_capability_raises_scheduler_once_and_refuses_bad_names(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("clicap_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var cs = f.AdminConnectionString;

        var first = await RunAsync(cs, [.. Store(kind, "store", "activate-capability"), "--name", "scheduler", "--application", f.Application]);
        Assert.Equal(0, first.Exit);
        Assert.True(first.Result!["changes"]![0]!["changed"]!.GetValue<bool>());
        var again = await RunAsync(cs, [.. Store(kind, "store", "activate-capability"), "--name", "scheduler"]);
        Assert.False(again.Result!["changes"]![0]!["changed"]!.GetValue<bool>());
        var status = await RunAsync(cs, Store(kind, "store", "status"));
        Assert.Contains("scheduler", status.Result!["applications"]![0]!["requiredCapabilities"]!.GetValue<string>(), StringComparison.Ordinal);

        Fails(await RunAsync(cs, [.. Store(kind, "store", "activate-capability"), "--name", "execution"]), "usage_error", 2);
        Fails(await RunAsync(cs, [.. Store(kind, "store", "activate-capability"), "--name", "scheduler", "--application", "nobody"]), "application_not_registered", 1);
    }

    static void Fails(CliRun r, string code, int exit) =>
        Assert.True((r.Code, r.Exit) == (code, exit), $"expected {code}/{exit}, got {r.Code}/{r.Exit}\n{r.Out}");
}
