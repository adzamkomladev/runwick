using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Cli.Tests;

/// <summary>19 §9: CLI-01 … CLI-10, the CLI in-process against a management host on the real PostgreSQL store.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-032")]
public sealed class CliTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static void Ok(CliRun r) => Assert.True(r.Exit == 0, $"exit {r.Exit}\n{r.Out}\n{r.Error}");

    static void Fails(CliRun r, string code, int exit)
    {
        Assert.True((r.Code, r.Exit) == (code, exit), $"expected {code}/{exit}, got exit {r.Exit}\n{r.Out}\n{r.Error}");
        Assert.False(r.Envelope["ok"]!.GetValue<bool>());
    }

    static Guid CommandId(CliRun r) => r.Envelope["commandId"]!.GetValue<Guid>();

    static async Task<(string G, string R)> PositionAsync(CliHost h, Guid job)
    {
        var r = await h.AgentAsync("jobs", "get", job.ToString());
        Ok(r);
        var j = r.Result!["job"]!;
        return (j["generation"]!.ToJsonString(), j["revision"]!.ToJsonString());
    }

    static async Task<string[]> CancelArgs(CliHost h, Guid job, string action = "cancel")
    {
        var (g, r) = await PositionAsync(h, job);
        return ["jobs", action, job.ToString(), "--expected-generation", g, "--expected-revision", r];
    }

    static Task<long> Audits(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", commandId));

    /// <summary>Removes the read instant: two reads of the same state differ only there.</summary>
    static JsonNode? Strip(JsonNode? n)
    {
        if (n is JsonObject o)
        {
            o.Remove("observedAt");
            foreach (var (_, v) in o)
            {
                Strip(v);
            }
        }
        else if (n is JsonArray a)
        {
            foreach (var v in a)
            {
                Strip(v);
            }
        }

        return n;
    }

    static IEnumerable<string> Ids(JsonNode? n) => n switch
    {
        JsonObject o => o.SelectMany(kv => Ids(kv.Value)),
        JsonArray a => a.SelectMany(Ids),
        JsonValue v when v.TryGetValue<string>(out var s) && Guid.TryParse(s, out _) => [s],
        _ => [],
    };

    // ------------------------------------------------------------------ CLI-01, CLI-04

    [Theory]
    [Trait("Case", "CLI-01")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_read_and_mutation_works_through_the_cli_with_basic_and_with_bearer(bool bearer)
    {
        await using var f = await DriverFixture.StartAsync(Ns(bearer ? "cli01b" : "cli01"));
        await using var h = await CliHost.StartAsync(f, bearer);
        await f.RegisterScheduleAsync("nightly");
        var done = await h.DispatchAsync(new PingJob("done"));
        await f.FinishNextAsync(AttemptOutcome.Succeeded);
        await h.DispatchAsync(new PingJob("failed"));
        var attempt = await f.FinishNextAsync(AttemptOutcome.PermanentFailure, "error-text");
        var job = await h.DispatchAsync(new PingJob("payload-text"));

        // CLI-04: one model behind the three modes.
        foreach (var read in new string[][]
        {
            ["jobs", "list"], ["jobs", "get", job.ToString()], ["queues", "list"], ["workers", "list", "--include-expired"], ["capacity", "list"],
            ["schedules", "list"], ["audit", "list"], ["settings"],
        })
        {
            var agent = await h.AgentAsync(read);
            Ok(agent);
            var json = await h.RunAsync(["--output", "json", .. read]);
            Ok(json);
            Assert.True(JsonNode.DeepEquals(Strip(agent.Result!.DeepClone()), Strip(JsonNode.Parse(json.Out))), string.Join(' ', read));
            var human = await h.RunAsync(read);
            Ok(human);
            foreach (var id in Ids(agent.Result))
            {
                Assert.True(human.Out.Contains(id, StringComparison.Ordinal), $"{string.Join(' ', read)}: {id} missing from\n{human.Out}\n{agent.Result.ToJsonString()}");
            }
        }

        var mutations = new List<CliRun>();
        async Task<CliRun> Mutate(params string[] args)
        {
            var r = await h.AgentAsync(args);
            Ok(r);
            mutations.Add(r);
            return r;
        }

        Assert.Equal("cancelled", (await Mutate([.. await CancelArgs(h, job), "--reason", "parity"])).Result!["state"]!.GetValue<string>());

        // Human mode reads the expected position itself and says so on stderr.
        var retryId = Guid.NewGuid();
        var retried = await h.RunAsync(["jobs", "retry", job.ToString(), "--command-id", retryId.ToString()]);
        Ok(retried);
        Assert.Contains("note: --expected-generation", retried.Error, StringComparison.Ordinal);
        Assert.Equal(1, await Audits(f, retryId));
        await Mutate(await CancelArgs(h, done, "rerun"));

        var queue = (await h.AgentAsync("queues", "list")).Result!["items"]!.AsArray().Single(q => q!["name"]!.GetValue<string>() == "default")!;
        var paused = await Mutate("queues", "pause", "default", "--expected-revision", queue["revision"]!.ToJsonString());
        await Mutate("queues", "resume", "default", "--expected-revision", paused.Result!["revision"]!.ToJsonString());

        var scope = (await h.AgentAsync("capacity", "list", "--kind", "job-type")).Result!["items"]!.AsArray()
            .Single(s => s!["key"]!.GetValue<string>() == "test.capped")!;
        Assert.Equal(2, (await Mutate("capacity", "set", "job-type", "test.capped", "2", "--expected-revision", scope["policyRevision"]!.ToJsonString()))
            .Result!["capacity"]!.GetValue<int>());

        var schedule = (await h.AgentAsync("schedules", "list")).Result!["items"]![0]!;
        var sp = await Mutate("schedules", "pause", "nightly", "--expected-revision", schedule["controlRevision"]!.ToJsonString());
        await Mutate("schedules", "resume", "nightly", "--expected-revision", sp.Result!["controlRevision"]!.ToJsonString(), "--policy", "runOnce");
        var trigger = await Mutate("schedules", "trigger", "nightly", "--expected-revision", schedule["definitionRevision"]!.ToJsonString());
        Assert.NotNull(trigger.Result!["jobId"]);

        // Revalidate on a schedule that needs no review: the server's refusal passes through unchanged.
        var revalidate = await h.AgentAsync("schedules", "revalidate", "nightly", "--expected-revision", schedule["definitionRevision"]!.ToJsonString());
        Assert.Equal(1, revalidate.Exit);

        var bulk = await h.DispatchAsync(new PingJob("bulk"));
        var (bg, br) = await PositionAsync(h, bulk);
        var many = await h.RunAsync(["--agent", "jobs", "cancel-many", "--file", "-"], stdin: $"{{\"jobId\":\"{bulk}\",\"expectedGeneration\":{bg},\"expectedRevision\":{br}}}\n");
        Ok(many);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE target_id = @j AND operation = 'Cancel'", ("j", bulk.ToString())));

        var dispatched = await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-"], stdin: "{\"id\":\"cli\"}");
        Ok(dispatched);
        mutations.Add(dispatched);

        var payload = await h.AgentAsync("jobs", "reveal-payload", job.ToString(), "--reason", "support");
        Ok(payload);
        Assert.Equal("payload-text", payload.Result!["payload"]!["id"]!.GetValue<string>());
        var error = await h.AgentAsync("jobs", "reveal-error", attempt.ToString(), "--reason", "support");
        Ok(error);
        Assert.Contains("error-text", error.Result!.ToJsonString(), StringComparison.Ordinal);

        // Same store effect as the session called directly: one audit row per command, written under the caller's name.
        foreach (var m in mutations)
        {
            Assert.Equal(1, await Audits(f, CommandId(m)));
        }

        var events = await h.AgentAsync("events", "tail");
        Ok(events);
        Assert.Equal("end", events.Envelope["stream"]!.GetValue<string>());
        Assert.Contains(events.Lines, l => l["stream"]?.GetValue<string>() == "item");
    }

    // ------------------------------------------------------------------ CLI-02

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "CLI-02")]
    public async Task Command_ids_replay_conflict_and_resolve_lost_replies()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli02"));
        await using var h = await CliHost.StartAsync(f);

        // Twice with one ID: the second replays; another body under it conflicts.
        var job = await h.DispatchAsync(new PingJob("a"));
        var id = Guid.NewGuid();
        string[] cancel = [.. await CancelArgs(h, job), "--command-id", id.ToString()];
        var first = await h.AgentAsync(cancel);
        var second = await h.AgentAsync(cancel);
        Ok(first);
        Ok(second);
        Assert.Equal((false, true), (first.Result!["replayed"]!.GetValue<bool>(), second.Result!["replayed"]!.GetValue<bool>()));
        Assert.Equal(1, await Audits(f, id));
        Fails(await h.AgentAsync([.. cancel[..^3], "999", "--command-id", id.ToString()]), "command_conflict", 1);

        // The server drops its reply after commit: the session resends and the receipt answers.
        var dropped = await h.DispatchAsync(new PingJob("b"));
        var dropId = Guid.NewGuid();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(dropId));
        Ok(await h.AgentAsync([.. await CancelArgs(h, dropped), "--command-id", dropId.ToString()]));
        Assert.Equal(1, drop.Fired);
        Assert.Equal(1, await Audits(f, dropId));

        // The reply is lost on the way to the CLI: it resends the same command, which replays.
        var lost = await h.DispatchAsync(new PingJob("c"));
        var lostArgs = await CancelArgs(h, lost);
        h.Fault = x => x.Lose = 1;
        var resolved = await h.AgentAsync(lostArgs);
        Ok(resolved);
        Assert.True(resolved.Result!["replayed"]!.GetValue<bool>());
        Assert.Equal(1, await Audits(f, CommandId(resolved)));

        // No resend answers: outcome_unknown (exit 3); a manual resend with the same ID replays.
        var unknown = await h.DispatchAsync(new PingJob("d"));
        var unknownArgs = await CancelArgs(h, unknown);
        h.Fault = x => x.Lose = 10;
        var gaveUp = await h.AgentAsync(unknownArgs);
        Fails(gaveUp, "outcome_unknown", 3);
        Assert.True(gaveUp.Envelope["error"]!["retryable"]!.GetValue<bool>());
        var unknownId = CommandId(gaveUp);
        Assert.Equal(4, h.LastFault!.Paths.Count(p => p.Contains("/cancel", StringComparison.Ordinal)));
        h.Fault = null;
        var manual = await h.AgentAsync([.. unknownArgs, "--command-id", unknownId.ToString()]);
        Ok(manual);
        Assert.True(manual.Result!["replayed"]!.GetValue<bool>());
        Assert.Equal(1, await Audits(f, unknownId));

        // cancel-many resent with one ID replays every item.
        var j1 = await h.DispatchAsync(new PingJob("e"));
        var j2 = await h.DispatchAsync(new PingJob("f"));
        var lines = new StringBuilder();
        foreach (var j in new[] { j1, j2 })
        {
            var (g, r) = await PositionAsync(h, j);
            lines.Append(CultureInfo.InvariantCulture, $"{{\"jobId\":\"{j}\",\"expectedGeneration\":{g},\"expectedRevision\":{r}}}\n");
        }

        var bulkId = Guid.NewGuid().ToString();
        var b1 = await h.RunAsync(["--agent", "jobs", "cancel-many", "--file", "-", "--command-id", bulkId], stdin: lines.ToString());
        var b2 = await h.RunAsync(["--agent", "jobs", "cancel-many", "--file", "-", "--command-id", bulkId], stdin: lines.ToString());
        Ok(b1);
        Ok(b2);
        Assert.All(b2.Result!["items"]!.AsArray(), i => Assert.True(i!["result"]!["replayed"]!.GetValue<bool>()));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'Cancel' AND target_id IN (@a, @b)",
            ("a", j1.ToString()), ("b", j2.ToString())));
    }

    // ------------------------------------------------------------------ CLI-03

    [Fact]
    [Trait("Case", "CLI-03")]
    public async Task Committed_schemas_equal_the_generated_ones_and_describe_covers_every_command()
    {
        if (Environment.GetEnvironmentVariable("RUNWICK_WRITE_SCHEMAS") == "1")
        {
            var indented = new JsonSerializerOptions { WriteIndented = true };
            await File.WriteAllTextAsync(Schemas.EnvelopePath, RunwickCli.EnvelopeSchema().ToJsonString(indented) + "\n", Ct);
            await File.WriteAllTextAsync(Schemas.DescribePath, RunwickCli.DescribeSchema().ToJsonString(indented) + "\n", Ct);
        }

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(await File.ReadAllTextAsync(Schemas.EnvelopePath, Ct)), RunwickCli.EnvelopeSchema()),
            "schemas/runwick-cli-agent-envelope-v1.json is stale: rerun this test with RUNWICK_WRITE_SCHEMAS=1.");
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(await File.ReadAllTextAsync(Schemas.DescribePath, Ct)), RunwickCli.DescribeSchema()),
            "schemas/runwick-cli-describe-v1.json is stale: rerun this test with RUNWICK_WRITE_SCHEMAS=1.");

        await using var f = await DriverFixture.StartAsync(Ns("cli03"));
        await using var h = await CliHost.StartAsync(f);
        var describe = await h.AgentAsync("describe");
        Ok(describe);
        using (var doc = JsonDocument.Parse(describe.Result!.ToJsonString()))
        {
            Assert.True(JsonSchema.FromText(await File.ReadAllTextAsync(Schemas.DescribePath, Ct)).Evaluate(doc.RootElement).IsValid);
        }

        var commands = describe.Result!["commands"]!.AsArray();
        Assert.Equal(CliCommands.All.Select(c => c.Name), commands.Select(c => c!["name"]!.GetValue<string>()));
        foreach (var c in commands)
        {
            List<string> argv = [.. c!["name"]!.GetValue<string>().Split(' ')];
            argv.AddRange(c["arguments"]!.AsArray().Select(a => Sample(a!["type"]!.GetValue<string>(), null)));
            foreach (var o in c["options"]!.AsArray().Where(o => o!["required"]!.GetValue<bool>() || o["requiredInAgentMode"]!.GetValue<bool>()))
            {
                argv.Add(o!["name"]!.GetValue<string>());
                if (o["type"]!.GetValue<string>() != "flag")
                {
                    argv.Add(Sample(o["type"]!.GetValue<string>(), o["allowed"]?.AsArray()[0]!.GetValue<string>()));
                }
            }

            var parsed = Record.Exception(() => CliParser.Parse(CliCommands.All, ["--agent", .. argv]));
            Assert.True(parsed is null, string.Join(' ', argv) + ": " + (parsed as CliFailure)?.Error.Message);
        }
    }

    static string Sample(string type, string? allowed) => allowed ?? type switch
    {
        "uuid" => Guid.NewGuid().ToString(),
        "integer" => "1",
        "instant" => "2026-01-01T00:00:00Z",
        "duration" => "1s",
        _ => "x",
    };

    // ------------------------------------------------------------------ CLI-05

    [Fact]
    [Trait("Case", "CLI-05")]
    public async Task Secrets_and_payloads_never_leak()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli05"));
        await using var h = await CliHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("marker-5e2"));
        string[] secrets = ["pw-admin", Convert.ToBase64String(Encoding.UTF8.GetBytes("admin:pw-admin"))];

        var outputs = new List<CliRun>
        {
            await h.RunAsync(["--verbose", "jobs", "list"]),
            await h.RunAsync(["--verbose", "jobs", "get", job.ToString()]),
            await h.RunAsync(["--verbose", "jobs", "cancel", job.ToString()]),
            await h.RunAsync(["--verbose", "audit", "list"]),
            await h.RunAsync(["--output", "json", "--secret-stdin", "events", "tail"], stdin: "pw-admin\n", variables: new()),
            await h.AgentAsync("context", "show"),
            await h.AgentAsync("context", "list"),
            await h.AgentAsync("jobs", "get", job.ToString()),
            await h.AgentAsync("events", "tail"),
        };
        Assert.Contains("[redacted]", outputs[0].Error, StringComparison.Ordinal);
        Assert.All(outputs, Ok);
        var all = string.Concat(outputs.Select(o => o.Out + o.Error)) + await File.ReadAllTextAsync(h.ConfigPath, Ct);
        foreach (var s in secrets.Append("marker-5e2"))
        {
            Assert.DoesNotContain(s, all, StringComparison.Ordinal);
        }

        // Only reveal-payload shows the payload.
        Assert.Contains("marker-5e2", (await h.AgentAsync("jobs", "reveal-payload", job.ToString())).Out, StringComparison.Ordinal);

        // No option takes a secret or a connection string as its value.
        var options = CliCommands.All.SelectMany(c => c.Options).Select(o => o.Name).Concat(["--context", "--url", "--output", "--timeout"]).ToList();
        Assert.DoesNotContain(options, o => o.Contains("password", StringComparison.Ordinal) || o.Contains("token", StringComparison.Ordinal)
            || ((o.Contains("secret", StringComparison.Ordinal) || o.Contains("connection", StringComparison.Ordinal)) && !o.EndsWith("-env", StringComparison.Ordinal)
                && !o.EndsWith("-stdin", StringComparison.Ordinal)));
    }

    // ------------------------------------------------------------------ CLI-06

    [Fact]
    [Trait("Case", "CLI-06")]
    public async Task Each_error_code_has_its_exit_code_retryable_flag_and_hint()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli06"));
        await using var h = await CliHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("a"));

        // Agent mode, no secret, a terminal on stdin: credential_missing at once, no prompt (the prompt throws).
        Fails(await h.RunAsync(["--agent", "jobs", "list"], variables: [], stdinIsTerminal: true), "credential_missing", 5);
        Fails(await h.RunAsync(["--agent", "jobs", "list"], variables: new() { ["RW_SECRET_ADMIN"] = "wrong" }), "unauthenticated", 5);

        // An agent mutation without the expected position is a usage error whose next action is the read.
        var missing = await h.AgentAsync("jobs", "cancel", job.ToString());
        Fails(missing, "usage_error", 2);
        Assert.Contains(missing.Envelope["nextActions"]!.AsArray(), a => a!["argv"]!.AsArray().Select(x => x!.GetValue<string>()).SequenceEqual(
            ["runwick", "--agent", "jobs", "get", job.ToString()]));

        Fails(await h.AgentAsync("--url", "http://example.com/runwick/api", "jobs", "list"), "insecure_url", 2);
        Fails(await h.AgentAsync("--context", "nope", "jobs", "list"), "context_not_found", 2);
        Fails(await h.AgentAsync("jobs", "get", Guid.NewGuid().ToString()), "not_found", 1);
        Fails(await h.AgentAsync("jobs", "list", "--cursor", "garbage"), "invalid_cursor", 1);
        var stale = await h.AgentAsync("jobs", "cancel", job.ToString(), "--expected-generation", "1", "--expected-revision", "99");
        Fails(stale, "stale_revision", 1);
        Assert.NotNull(stale.Envelope["error"]!["actualRevision"]);

        await h.SaveContextAsync(CliHost.Viewer);
        var forbidden = await h.AgentAsync("jobs", "cancel", job.ToString(), "--expected-generation", "1", "--expected-revision", "1");
        Fails(forbidden, "forbidden", 5);
        Assert.Contains(ManagementPermissions.JobsCancel, forbidden.Envelope["error"]!["hint"]!.GetValue<string>(), StringComparison.Ordinal);
        await h.SaveContextAsync(CliHost.Admin);

        h.Fault = x => x.Refuse = true;
        var refused = await h.AgentAsync("jobs", "list");
        Fails(refused, "connection_failed", 4);
        Assert.True(refused.Envelope["error"]!["retryable"]!.GetValue<bool>());
        h.Fault = x => x.Answer = _ => new HttpResponseMessage(HttpStatusCode.NotFound);
        Fails(await h.AgentAsync("jobs", "list"), "endpoint_not_supported", 4);
        h.Fault = x => x.Answer = _ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{oops", Encoding.UTF8, "application/json") };
        Fails(await h.AgentAsync("jobs", "list"), "response_invalid", 4);

        // Every server code, forced through the fault handler, keeps the table's exit code, retryable flag and hint.
        foreach (var (code, (exit, retryable, hint)) in CliErrors.Table.Where(e => e.Key != "forbidden"))
        {
            h.Fault = x => x.Answer = r => r.RequestUri!.AbsolutePath.EndsWith("/jobs", StringComparison.Ordinal)
                ? new HttpResponseMessage(retryable ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(new JsonObject { ["code"] = code, ["message"] = "forced" }.ToJsonString(), Encoding.UTF8, "application/json"),
                }
                : null;
            var run = await h.AgentAsync("jobs", "list");
            Fails(run, code, exit);
            Assert.Equal((retryable, hint), (run.Envelope["error"]!["retryable"]!.GetValue<bool>(), run.Envelope["error"]!["hint"]!.GetValue<string>()));
        }
    }

    // ------------------------------------------------------------------ CLI-07

    [Fact]
    [Trait("Case", "CLI-07")]
    public async Task Streams_deliver_every_event_once_and_end_cleanly()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli07"));
        await using var h = await CliHost.StartAsync(f);
        var seen = new List<long>();
        async Task<string> Tail(string? after)
        {
            var r = await h.AgentAsync(["events", "tail", "--limit", "2", .. after is null ? Array.Empty<string>() : ["--after", after]]);
            Ok(r);
            seen.AddRange(r.Lines.Where(l => l["stream"]!.GetValue<string>() == "item").Select(l => l["result"]!["eventId"]!.GetValue<long>()));
            return r.Result!["cursor"]!.GetValue<string>();
        }

        var a = await h.DispatchAsync(new PingJob("a"));
        await h.DispatchAsync(new PingJob("b"));
        Ok(await h.AgentAsync(await CancelArgs(h, a)));
        var cursor = await Tail(null);
        await h.DispatchAsync(new PingJob("c"));
        cursor = await Tail(cursor);
        await Tail(cursor);
        Assert.Equal(seen.Distinct().Count(), seen.Count);
        Assert.Equal(await f.CountAsync("SELECT count(*) FROM runwick.queue_events"), seen.Count);

        // actorId only with Audit.Read.
        var admin = await h.AgentAsync("events", "tail");
        Assert.Contains(admin.Lines, l => l["result"]?["actorId"]?.GetValue<string>() == "admin");
        await h.SaveContextAsync(CliHost.Viewer);
        var viewer = await h.AgentAsync("events", "tail");
        Assert.All(viewer.Lines.Where(l => l["stream"]!.GetValue<string>() == "item"), l => Assert.Null(l["result"]!["actorId"]));
        await h.SaveContextAsync(CliHost.Scoped);
        Fails(await h.AgentAsync("events", "tail"), "forbidden", 5);
        await h.SaveContextAsync(CliHost.Admin);

        // jobs tail ends at the terminal state.
        var tailed = await h.DispatchAsync(new PingJob("t"));
        var tail = h.AgentAsync("jobs", "tail", tailed.ToString(), "--interval", "250ms");
        await Task.Delay(500, Ct);
        Ok(await h.AgentAsync(await CancelArgs(h, tailed)));
        var ended = await tail.WaitAsync(TimeSpan.FromSeconds(20), Ct);
        Ok(ended);
        Assert.Equal(["pending", "cancelled"], ended.Lines.Where(l => l["stream"]!.GetValue<string>() == "item").Select(l => l["result"]!["state"]!.GetValue<string>()));

        // Ctrl-C ends a follow with an end line carrying the cursor.
        using var interrupt = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        var follow = await h.RunAsync(["--agent", "events", "tail", "--follow", "--interval", "250ms"], interrupt: interrupt.Token);
        Ok(follow);
        Assert.Equal("end", follow.Envelope["stream"]!.GetValue<string>());
        Assert.NotNull(follow.Result!["cursor"]);
    }

    // ------------------------------------------------------------------ CLI-08

    [Fact]
    [Trait("Case", "CLI-08")]
    public async Task Migrate_applies_once_and_doctor_reports_the_revisions()
    {
        var server = await StoreServer.SharedAsync();
        var name = await server.CreateEmptyDatabaseAsync(Ns("cli08"));
        try
        {
            await using var f = await DriverFixture.StartAsync(Ns("cli08h"));
            await using var h = await CliHost.StartAsync(f);
            var variables = h.Variables();
            variables["PGCS"] = server.ConnectionStringFor(name, "rw:cli", pooling: false);

            var first = await h.RunAsync(["--agent", "migrate", "--provider", "postgres", "--connection-env", "PGCS"], variables: variables);
            Ok(first);
            Assert.Equal(PgMigrator.CurrentRevision, first.Result!["schemaRevision"]!.GetValue<long>());
            Assert.NotEmpty(first.Result["appliedPhases"]!.AsArray());
            var again = await h.RunAsync(["--agent", "migrate", "--provider", "postgres", "--connection-env", "PGCS"], variables: variables);
            Ok(again);
            Assert.Empty(again.Result!["appliedPhases"]!.AsArray());

            var doctor = await h.RunAsync(["--agent", "doctor", "--connection-env", "PGCS"], variables: variables);
            Ok(doctor);
            var checks = doctor.Result!["checks"]!.AsArray().ToDictionary(c => c!["name"]!.GetValue<string>(), c => c!["status"]!.GetValue<string>());
            Assert.Equal("pass", checks["store.migration"]);
            Assert.Equal("pass", checks["api.auth"]);
            Assert.Equal(PgMigrator.CurrentRevision, doctor.Result["cliSchemaRevision"]!.GetValue<long>());

            await StoreServer.ExecAsync(variables["PGCS"], "UPDATE runwick.store_metadata SET schema_revision = schema_revision + 1 WHERE singleton_id = 1");
            var ahead = await h.RunAsync(["--agent", "doctor", "--connection-env", "PGCS"], variables: variables);
            Fails(ahead, "check_failed", 1);
            Assert.Contains(ahead.Result!["checks"]!.AsArray(), c => c!["name"]!.GetValue<string>() == "store.migration" && c["status"]!.GetValue<string>() == "fail");
        }
        finally
        {
            await server.DropDatabaseAsync(name);
        }
    }

    // ------------------------------------------------------------------ CLI-09

    [Fact]
    [Trait("Case", "CLI-09")]
    public async Task A_limit_above_the_page_size_pages_through_the_api()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli09"));
        await using var h = await CliHost.StartAsync(f);
        for (var i = 0; i < 460; i++)
        {
            await h.DispatchAsync(new PingJob("p" + i.ToString(CultureInfo.InvariantCulture)));
        }

        h.Fault = _ => { };
        var page = await h.AgentAsync("jobs", "list", "--limit", "450");
        Ok(page);
        Assert.Equal(450, page.Result!["items"]!.AsArray().Count);
        Assert.Equal(450, page.Result["items"]!.AsArray().Select(i => i!["jobId"]!.GetValue<Guid>()).Distinct().Count());
        Assert.NotNull(page.Result["next"]);
        Assert.Equal(3, h.LastFault!.Paths.Count(p => p.Contains("/v1/jobs?", StringComparison.Ordinal)));

        var rest = await h.AgentAsync("jobs", "list", "--cursor", page.Result["next"]!.GetValue<string>());
        Ok(rest);
        Assert.Equal(10, rest.Result!["items"]!.AsArray().Count);
        Fails(await h.AgentAsync("jobs", "list", "--cursor", page.Result["next"]!.GetValue<string>()[..^4] + "AAAA"), "invalid_cursor", 1);
    }

    // ------------------------------------------------------------------ CLI-10

    [Fact]
    [Trait("Case", "CLI-10")]
    public async Task Dispatch_creates_one_audited_job_and_refuses_what_does_not_bind()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cli10"));
        await using var h = await CliHost.StartAsync(f);
        var file = Path.Combine(Path.GetDirectoryName(h.ConfigPath)!, "payload.json");
        await File.WriteAllTextAsync(file, "{\"id\":\"from-file\"}", Ct);

        // From a file, then the same command ID again: replayed, still one job and one audit row.
        var id = Guid.NewGuid().ToString();
        var created = await h.AgentAsync("dispatch", "test.ping", "--payload", "@" + file, "--command-id", id, "--reason", "ticket");
        Ok(created);
        Assert.Equal(("created", false), (created.Result!["disposition"]!.GetValue<string>(), created.Result["replayed"]!.GetValue<bool>()));
        var replay = await h.AgentAsync("dispatch", "test.ping", "--payload", "@" + file, "--command-id", id, "--reason", "ticket");
        Ok(replay);
        Assert.True(replay.Result!["replayed"]!.GetValue<bool>());
        Assert.Equal(created.Result["jobId"]!.GetValue<Guid>(), replay.Result["jobId"]!.GetValue<Guid>());
        Assert.Equal((1L, 1L), (await f.CountAsync("SELECT count(*) FROM runwick.jobs"), await Audits(f, Guid.Parse(id))));

        // From stdin, with the reply lost once: the automatic resend replays; one job.
        h.Fault = x => x.Lose = 1;
        var stdin = await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-"], stdin: "{\"id\":\"from-stdin\"}");
        h.Fault = null;
        Ok(stdin);
        Assert.True(stdin.Result!["replayed"]!.GetValue<bool>());
        Assert.Equal((2L, 1L), (await f.CountAsync("SELECT count(*) FROM runwick.jobs"), await Audits(f, CommandId(stdin))));

        // The server drops its reply: resolved by its own resend.
        var dropId = Guid.NewGuid();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(dropId));
        Ok(await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-", "--command-id", dropId.ToString()], stdin: "{\"id\":\"dropped\"}"));
        Assert.Equal(1, drop.Fired);
        Assert.Equal((3L, 1L), (await f.CountAsync("SELECT count(*) FROM runwick.jobs"), await Audits(f, dropId)));

        Fails(await h.RunAsync(["--agent", "dispatch", "no.such", "--payload", "-"], stdin: "{}"), "job_not_registered", 1);
        Fails(await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-", "--payload-version", "9"], stdin: "{\"id\":\"v\"}"),
            "payload_version_unsupported", 1);

        // A payload that does not bind: the path is named, its text appears nowhere.
        foreach (var args in new[] { new[] { "--agent" }, ["--output", "json"], [] })
        {
            var bad = await h.RunAsync([.. args, "dispatch", "test.ping", "--payload", "-"], stdin: "{\"id\":{\"secret\":\"marker-a91\"}}");
            Assert.Equal(1, bad.Exit);
            Assert.Contains("payload_invalid", bad.Out + bad.Error, StringComparison.Ordinal);
            Assert.DoesNotContain("marker-a91", bad.Out + bad.Error, StringComparison.Ordinal);
        }

        var big = "{\"id\":\"" + new string('x', (1024 * 1024) + 1 - 9) + "\"}";
        Assert.Equal((1024 * 1024) + 1, Encoding.UTF8.GetByteCount(big));
        Fails(await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-"], stdin: big), "payload_too_large", 1);

        // Jobs.Dispatch is in no preset.
        h.Grants["preset"] = ManagementRoles.Administrator;
        await h.SaveContextAsync("preset");
        Fails(await h.RunAsync(["--agent", "dispatch", "test.ping", "--payload", "-"], stdin: "{\"id\":\"p\"}"), "forbidden", 5);
        await h.SaveContextAsync(CliHost.Admin);

        // A uniqueness-bound job dispatched twice under different IDs: reused, with an audit row each.
        var k1 = await h.RunAsync(["--agent", "dispatch", "test.keyed", "--payload", "-"], stdin: "{\"id\":\"k1\",\"key\":\"same\"}");
        var k2 = await h.RunAsync(["--agent", "dispatch", "test.keyed", "--payload", "-"], stdin: "{\"id\":\"k2\",\"key\":\"same\"}");
        Ok(k1);
        Ok(k2);
        Assert.Equal(("created", "reused"), (k1.Result!["disposition"]!.GetValue<string>(), k2.Result!["disposition"]!.GetValue<string>()));
        Assert.Equal((1L, 1L), (await Audits(f, CommandId(k1)), await Audits(f, CommandId(k2))));
    }
}
