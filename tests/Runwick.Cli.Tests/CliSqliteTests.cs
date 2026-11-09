using Microsoft.Data.Sqlite;

namespace Runwick.Cli.Tests;

/// <summary>CLI-08, SQLite half: <c>migrate</c> and <c>doctor --provider sqlite</c> on real database files, with no server.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-032")]
public sealed class CliSqliteTests : IDisposable
{
    readonly string _dir = Directory.CreateTempSubdirectory("rw-cli-sq-").FullName;

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(_dir, recursive: true);
    }

    Dictionary<string, string> Db(string file) => new() { ["SQCS"] = "Data Source=" + Path.Combine(_dir, file) };

    async Task<CliRun> RunAsync(Dictionary<string, string> variables, params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var env = new CliEnvironment
        {
            Out = output,
            Error = error,
            Variables = variables,
            ConfigPath = Path.Combine(_dir, "no-config.json"),
            PromptSecret = () => throw new InvalidOperationException("The CLI prompted."),
        };
        var run = new CliRun(await RunwickCli.RunAsync(["--agent", .. args], env), output.ToString(), error.ToString());
        Assert.Empty(run.Error);
        Schemas.AssertValidEnvelope(run.Out.Trim());
        return run;
    }

    static void Fails(CliRun r, string code, int exit) =>
        Assert.True((r.Code, r.Exit) == (code, exit), $"expected {code}/{exit}, got exit {r.Exit}\n{r.Out}");

    static Dictionary<string, (string Status, string? Detail)> Checks(CliRun r) =>
        r.Result!["checks"]!.AsArray().ToDictionary(c => c!["name"]!.GetValue<string>(), c => (c!["status"]!.GetValue<string>(), c["detail"]?.GetValue<string>()));

    Task<CliRun> MigrateAsync(Dictionary<string, string> v) => RunAsync(v, "migrate", "--provider", "sqlite", "--connection-env", "SQCS");

    Task<CliRun> DoctorAsync(Dictionary<string, string> v) => RunAsync(v, "doctor", "--provider", "sqlite", "--connection-env", "SQCS");

    static void Exec(Dictionary<string, string> v, string sql)
    {
        using var conn = new SqliteConnection(v["SQCS"] + ";Pooling=False");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task Migrate_applies_once_and_doctor_reports_the_revisions()
    {
        var v = Db("store.db");

        // doctor never creates the file
        var missing = await DoctorAsync(v);
        Fails(missing, "check_failed", 1);
        Assert.Equal(("fail", "store_unavailable"), Checks(missing)["store.migration"]);
        Assert.False(File.Exists(Path.Combine(_dir, "store.db")));

        var first = await MigrateAsync(v);
        Assert.Equal(0, first.Exit);
        Assert.Equal(2, first.Result!["schemaRevision"]!.GetValue<long>());
        Assert.Equal(6, first.Result["appliedPhases"]!.AsArray().Count);
        var again = await MigrateAsync(v);
        Assert.Equal(0, again.Exit);
        Assert.Empty(again.Result!["appliedPhases"]!.AsArray());
        Assert.Equal(first.Result["storeId"]!.GetValue<Guid>(), again.Result["storeId"]!.GetValue<Guid>());

        var doctor = await DoctorAsync(v);
        Assert.Equal(0, doctor.Exit);
        var checks = Checks(doctor);
        Assert.Equal(("pass", "revision 2"), checks["store.migration"]);
        Assert.Contains("journal_mode=wal", checks["store.settings"].Detail, StringComparison.Ordinal);
        Assert.NotEqual("fail", checks["store.settings"].Status);
        Assert.Equal("unverified", checks["api.reachable"].Status);
        Assert.Equal(2, doctor.Result!["cliSchemaRevision"]!.GetValue<long>());

        Exec(v, "UPDATE runwick_store_metadata SET schema_revision = schema_revision + 1 WHERE singleton_id = 1");
        var ahead = await DoctorAsync(v);
        Fails(ahead, "check_failed", 1);
        Assert.Equal("fail", Checks(ahead)["store.migration"].Status);
        Assert.Contains("CLI older than store", Checks(ahead)["store.migration"].Detail, StringComparison.Ordinal);

        Exec(v, "UPDATE runwick_store_metadata SET schema_revision = 2 WHERE singleton_id = 1");
        Exec(v, "CREATE INDEX runwick_extra ON runwick_jobs (job_name)");
        var changed = await DoctorAsync(v);
        Fails(changed, "check_failed", 1);
        Assert.Contains("schema_incompatible", Checks(changed)["store.migration"].Detail, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task Doctor_on_an_empty_file_says_run_migrate()
    {
        var v = Db("empty.db");
        Exec(v, "CREATE TABLE app_orders (id INTEGER PRIMARY KEY)");
        var doctor = await DoctorAsync(v);
        Assert.Equal(0, doctor.Exit);
        Assert.Equal(("warn", "store not initialized: run migrate"), Checks(doctor)["store.migration"]);
    }

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task Migration_error_codes_map_to_exit_codes()
    {
        // Provider codes pass through unchanged: rejections exit 1, an unreachable store exits 4 and is retryable.
        var memory = await MigrateAsync(new() { ["SQCS"] = "Data Source=:memory:" });
        Fails(memory, "invalid_policy", 1);
        Assert.False(memory.Envelope["error"]!["retryable"]!.GetValue<bool>());

        var foreign = Db("foreign.db");
        Exec(foreign, "CREATE TABLE runwick_something (id INTEGER PRIMARY KEY)");
        Fails(await MigrateAsync(foreign), "schema_not_empty", 1);

        var unreachable = await MigrateAsync(new() { ["SQCS"] = "Data Source=" + Path.Combine(_dir, "no-such-dir", "x.db") });
        Fails(unreachable, "store_unavailable", 4);
        Assert.True(unreachable.Envelope["error"]!["retryable"]!.GetValue<bool>());

        Fails(await MigrateAsync([]), "credential_missing", 5);
        Fails(await RunAsync([], "migrate", "--provider", "sqlite"), "usage_error", 2);
    }
}
