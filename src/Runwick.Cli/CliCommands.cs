using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Npgsql;
using Runwick.Management;
using Runwick.Postgres.Migrations;
using Runwick.Sqlite.Migrations;
using Runwick.Sqlite.Store;
using V = Runwick.Cli.CliValueType;

namespace Runwick.Cli;

// API mutation bodies as the server writes them (16 §5), for describe's result schemas.
internal sealed record JobControlBody(Guid JobId, string Effect, int Generation, long Revision, string State, bool Replayed, Guid CommandId);

internal sealed record BulkCancelBody(IReadOnlyList<BulkCancelResult> Items, Guid CommandId);

internal sealed record QueueControlBody(string Queue, bool Paused, long Revision, bool Replayed, Guid CommandId);

internal sealed record CapacityControlBody(string ScopeKind, string ScopeKey, int Capacity, long PolicyRevision, int LivePermits, bool Replayed, Guid CommandId);

internal sealed record ScheduleControlBody(string Key, bool Paused, bool NeedsReview, long DefinitionRevision, long ControlRevision, Guid? JobId, bool Replayed,
    Guid CommandId);

internal sealed record ContextInfo(string Name, string Url, string Auth, string? User, string? SecretEnv, bool Current, bool SecretSet);

internal sealed record ContextList(IReadOnlyList<ContextInfo> Items);

internal sealed record LoginResult(bool Ok, string Url, string Auth);

internal sealed record VersionResult(string Version, string ApiVersion);

internal sealed record EventsEnd(string Cursor);

internal sealed record DoctorCheck(string Name, string Status, string? Detail);

internal sealed record DoctorResult(string CliVersion, long CliSchemaRevision, IReadOnlyList<DoctorCheck> Checks);

/// <summary>Every command of the tool (19 §2), each one declaration.</summary>
internal static class CliCommands
{
    const string Prefix = "runwick.cli.bulk.v1";

    static CliArgument Arg(string name, V type, string description) => new(name, type, description);

    static CliOption Opt(string name, V type, string description, bool required = false, bool agent = false, string[]? allowed = null, string? def = null) =>
        new(name, type, required, agent, allowed, def, description);

    static HumanColumn[] Cols(params string[] properties) => [.. properties.Select(p => new HumanColumn(p, p))];

    static readonly CliOption CommandIdOption = Opt("--command-id", V.Uuid, "Command ID (UUID); generated as UUIDv7 when absent. Resend the same ID to resolve an unknown outcome.");
    static readonly CliOption ReasonOption = Opt("--reason", V.String, "Reason recorded in the audit row (at most 512 UTF-8 bytes).");
    static readonly CliOption LimitOption = Opt("--limit", V.Integer, "Items wanted (1-10,000; default 50 human, 200 json/agent).");
    static readonly CliOption CursorOption = Opt("--cursor", V.String, "Continue from a list cursor (valid 15 minutes).");
    static readonly CliOption ExpectedRevision = Opt("--expected-revision", V.Integer, "Revision last observed; read from the server when absent (not with --agent).", agent: true);
    static readonly CliOption ConnectionEnv = Opt("--connection-env", V.String, "Name of the environment variable holding the connection string.");
    static readonly CliOption ConnectionStdin = Opt("--connection-stdin", V.Flag, "Read the connection string from the first line of stdin.");
    static readonly CliOption Provider = Opt("--provider", V.String, "Store provider.", allowed: ["postgres", "sqlite"], def: "postgres");

    static CliCommand Read(string name, string summary, string permission, Type result, Func<CliInvocation, string> path, CliArgument[]? args = null,
        CliOption[]? options = null, HumanColumn[]? columns = null, bool list = false) =>
        new(name, summary, args ?? [], [.. options ?? [], .. list ? new[] { LimitOption, CursorOption } : []], false, false, permission, result, columns ?? [],
            (inv, ct) => list ? ListAsync(inv, path(inv), ct) : GetAsync(inv, path(inv), ct));

    static CliCommand Mutation(string name, string summary, string permission, Type result, CliArgument[] args, CliOption[] options,
        Func<CliInvocation, CancellationToken, Task<CommandOutcome>> run, HumanColumn[]? columns = null) =>
        new(name, summary, args, [.. options, CommandIdOption, ReasonOption], true, false, permission, result, columns ?? [], run);

    static CliCommand Local(string name, string summary, Type result, Func<CliInvocation, CancellationToken, Task<CommandOutcome>> run,
        CliArgument[]? args = null, CliOption[]? options = null, string? permission = null) =>
        new(name, summary, args ?? [], options ?? [], false, false, permission, result, [], run);

    public static IReadOnlyList<CliCommand> All { get; } =
    [
        Local("context add", "Save a context: server URL, login method, user and the name of the secret's environment variable.", typeof(ContextInfo), ContextAdd,
            [Arg("name", V.String, "Context name.")],
            [Opt("--url", V.String, "Management API prefix (https://, or http:// to loopback).", required: true),
             Opt("--auth", V.String, "Login method.", required: true, allowed: ["basic", "bearer"]),
             Opt("--user", V.String, "User name (basic)."), Opt("--secret-env", V.String, "Environment variable that holds the password or token.")]),
        Local("context use", "Make a context current.", typeof(ContextInfo), ContextUse, [Arg("name", V.String, "Context name.")]),
        Local("context list", "List saved contexts.", typeof(ContextList), (inv, _) => Task.FromResult(CommandOutcome.Ok(Contexts(inv, null)))),
        Local("context show", "Show the current context (never a secret value).", typeof(ContextInfo), ContextShow),
        Local("context remove", "Remove a saved context.", typeof(ContextInfo), ContextRemove, [Arg("name", V.String, "Context name.")]),
        Local("login", "Check the current context's credentials; stores nothing.", typeof(LoginResult), Login, permission: "authenticated"),

        Read("jobs list", "List jobs, newest first.", ManagementPermissions.JobsRead, typeof(ManagementPage<JobSummary>),
            inv => "jobs" + Q(inv, ("queue", "--queue"), ("state", "--state"), ("jobName", "--job-name"), ("from", "--from"), ("to", "--to")),
            options: [Opt("--queue", V.String, "Queue."), Opt("--state", V.String, "State.", allowed: ["pending", "reserved", "running", "retrying", "succeeded", "failed", "cancelled"]),
                Opt("--job-name", V.String, "Job name."), Opt("--from", V.Instant, "Created from (default: 7 days before --to)."), Opt("--to", V.Instant, "Created until (default: now).")],
            columns: Cols("jobId", "jobName", "queue", "state", "generation", "revision", "createdAt"), list: true),
        Read("jobs get", "Show a job with its generations and attempts (no payload).", ManagementPermissions.JobsRead, typeof(JobDetail),
            inv => "jobs/" + inv.Arg("id"), [Arg("id", V.Uuid, "Job ID.")]),
        new("jobs tail", "Follow a job until it ends: one item per state or revision change.", [Arg("id", V.Uuid, "Job ID.")],
            [Opt("--interval", V.Duration, "Poll interval.", def: "2s")], false, true, ManagementPermissions.JobsRead, typeof(JobSummary), [], JobsTail),
        JobControl("cancel", ManagementPermissions.JobsCancel),
        JobControl("retry", ManagementPermissions.JobsRetry),
        JobControl("rerun", ManagementPermissions.JobsRerun),
        Mutation("jobs cancel-many", "Cancel up to 100 jobs listed as JSON Lines {jobId, expectedGeneration, expectedRevision}.", ManagementPermissions.JobsCancel,
            typeof(BulkCancelBody), [], [Opt("--file", V.Path, "JSON Lines file, or - for stdin.", required: true)], CancelMany, Cols("jobId", "status", "code")),
        Mutation("jobs reveal-payload", "Show a job's payload; the server writes an audit row.", ManagementPermissions.JobsReadPayload, typeof(RevealedPayload),
            [Arg("id", V.Uuid, "Job ID.")], [], (inv, ct) => RevealAsync(inv, $"jobs/{inv.Arg("id")}/payload:reveal", ct)),
        Mutation("jobs reveal-error", "Show an attempt's error; the server writes an audit row.", ManagementPermissions.JobsReadError, typeof(RevealedError),
            [Arg("attempt-id", V.Uuid, "Attempt ID.")], [], (inv, ct) => RevealAsync(inv, $"attempts/{inv.Arg("attempt-id")}/error:reveal", ct)),
        Mutation("dispatch", "Create a job of a registered job type from a JSON payload (file or stdin); audited.", ManagementPermissions.JobsDispatch,
            typeof(ManagementDispatchResult), [Arg("job-name", V.String, "Registered job name.")],
            [Opt("--payload", V.String, "@file or - (stdin); never inline.", required: true), Opt("--payload-version", V.Integer, "Must equal the job's current version."),
             Opt("--queue", V.String, "Queue override."), Opt("--priority", V.String, "Priority override.", allowed: ["low", "medium", "high"]),
             Opt("--not-before", V.Instant, "Earliest start.")], DispatchAsync),

        Read("queues list", "List queues.", ManagementPermissions.QueuesRead, typeof(ManagementPage<QueueSummary>), _ => "queues",
            columns: Cols("name", "paused", "revision"), list: true),
        QueueControl("pause"),
        QueueControl("resume"),
        Read("workers list", "List worker instances.", ManagementPermissions.CapacityRead, typeof(ManagementPage<WorkerSummary>),
            inv => "workers" + (inv.Flag("--include-expired") ? "?includeExpired=true" : ""), options: [Opt("--include-expired", V.Flag, "Include expired instances.")],
            columns: Cols("instanceId", "hostName", "status", "heartbeatAt"), list: true),
        Read("capacity list", "List concurrency scopes.", ManagementPermissions.CapacityRead, typeof(ManagementPage<ScopeSummary>),
            inv => "scopes" + Q(inv, ("kind", "--kind")), options: [Opt("--kind", V.String, "Scope kind.", allowed: ["queue", "job-type", "resource"])],
            columns: Cols("kind", "key", "capacity", "policyRevision", "livePermits"), list: true),
        Mutation("capacity set", "Set a scope's capacity.", ManagementPermissions.CapacityEdit, typeof(CapacityControlBody),
            [Arg("kind", V.String, "Scope kind."), Arg("key", V.String, "Scope key."), Arg("capacity", V.Integer, "New capacity.")],
            [Opt("--expected-revision", V.Integer, "Policy revision last observed; read when absent (not with --agent).", agent: true)], CapacitySet),

        Read("schedules list", "List schedules.", ManagementPermissions.SchedulesRead, typeof(ManagementPage<ScheduleSummary>), _ => "schedules",
            columns: Cols("key", "paused", "needsReview", "definitionRevision", "controlRevision", "nextDueAt"), list: true),
        ScheduleControl("pause", ManagementPermissions.SchedulesControl, control: true),
        ScheduleControl("resume", ManagementPermissions.SchedulesControl, control: true),
        ScheduleControl("trigger", ManagementPermissions.SchedulesTrigger, control: false),
        ScheduleControl("revalidate", ManagementPermissions.SchedulesEdit, control: false),

        new("events tail", "Read job events after a cursor; --follow keeps polling.", [],
            [Opt("--after", V.String, "Position cursor <txId>.<eventId>."), Opt("--follow", V.Flag, "Keep polling after the last event."),
             Opt("--interval", V.Duration, "Poll interval (250ms-60s).", def: "2s"), Opt("--limit", V.Integer, "Events per request (1-1,000).", def: "100")],
            false, true, ManagementPermissions.JobsRead, typeof(ManagementEvent), [], EventsTail),
        Read("audit list", "List audit rows, newest first.", ManagementPermissions.AuditRead, typeof(ManagementPage<AuditEntry>),
            inv => "audit" + Q(inv, ("targetKind", "--target-kind"), ("targetId", "--target-id"), ("from", "--from"), ("to", "--to")),
            options: [Opt("--target-kind", V.String, "Target kind."), Opt("--target-id", V.String, "Target ID."), Opt("--from", V.Instant, "From."), Opt("--to", V.Instant, "To.")],
            columns: Cols("auditId", "commandId", "operation", "targetKind", "targetId", "actorSubject", "occurredAt"), list: true),
        Read("settings", "Show the management host's settings (allowlisted).", ManagementPermissions.SettingsRead, typeof(ManagementSettings), _ => "settings"),

        Local("migrate", "Apply this CLI's store migrations.", typeof(PostgresMigrationResult), MigrateAsync,
            options: [Opt("--provider", V.String, "Store provider.", required: true, allowed: ["postgres", "sqlite"]), ConnectionEnv, ConnectionStdin],
            permission: "migration role"),
        Local("store status", "Show the store's revision, mode, application gates and the lease and presence rows (never proof that processes stopped).", typeof(StoreAdminStatus), StoreStatusAsync,
            options: [Provider, ConnectionEnv, ConnectionStdin], permission: "migration role"),
        Local("store admission", "Set application admission: drain blocks new work, maintenance blocks everything but status reads; open reopens.", typeof(AdmissionResult), StoreAdmissionAsync,
            options: [Opt("--mode", V.String, "Target admission mode.", required: true, allowed: ["open", "drain", "maintenance"]),
                      Opt("--application", V.String, "Application name; every registered application when absent."), Provider, ConnectionEnv, ConnectionStdin],
            permission: "migration role"),
        Local("store activate-capability", "Raise capability scheduler or rate_admission for an application (never lowers one); schedules and rate scopes are refused until then.", typeof(CapabilityResult), StoreActivateCapabilityAsync,
            options: [Opt("--name", V.String, "Capability to activate.", required: true, allowed: [.. StoreAdminRules.ActivatableCapabilities]),
                      Opt("--application", V.String, "Application name; every registered application when absent."), Provider, ConnectionEnv, ConnectionStdin],
            permission: "migration role"),
        Local("store reidentify", "Give a restored or copied store a new logical identity (offline: every application in maintenance, no live leases).", typeof(StoreIdentityChange), StoreReidentifyAsync,
            options: [Opt("--confirm", V.Flag, "Required: old prepared envelopes and in-flight commands stop resolving.", required: true), Provider, ConnectionEnv, ConnectionStdin],
            permission: "migration role"),
        Local("doctor", "Check the server, credentials, clock and (with a connection) the store.", typeof(DoctorResult), DoctorAsync,
            options: [Provider, ConnectionEnv, ConnectionStdin], permission: "authenticated"),
        Local("version", "Print the CLI version.", typeof(VersionResult),
            (_, _) => Task.FromResult(CommandOutcome.Ok(JsonSerializer.SerializeToNode(new VersionResult(RunwickCli.Version, "v1"), RunwickCli.Json)))),
        Local("describe", "Machine-readable description of every command (use with --agent).", typeof(RunwickCli.DescribeDocument),
            (_, _) => Task.FromResult(CommandOutcome.Ok(RunwickCli.Describe()))),
    ];

    // ------------------------------------------------------------------ helpers

    /// <summary>A query string from options that are set.</summary>
    static string Q(CliInvocation inv, params (string Name, string Option)[] map) => Query([.. map.Select(m => (m.Name, inv.Opt(m.Option)))]);

    static string Query(params (string Name, string? Value)[] pairs)
    {
        var set = pairs.Where(p => p.Value is not null).Select(p => p.Name + "=" + Uri.EscapeDataString(p.Value!)).ToList();
        return set.Count == 0 ? "" : "?" + string.Join('&', set);
    }

    static async Task<CommandOutcome> GetAsync(CliInvocation inv, string path, CancellationToken ct) =>
        CommandOutcome.Ok(await inv.Api().ReadAsync(path, inv.Command.Permission, ct));

    /// <summary>Pages of up to 200 until <c>--limit</c> items or the end (19 §5.2); the last page's cursor is returned, nothing is cut.</summary>
    static async Task<CommandOutcome> ListAsync(CliInvocation inv, string path, CancellationToken ct)
    {
        var wanted = inv.Long("--limit") ?? (inv.Agent || inv.Globals.Output == "json" ? 200 : 50);
        if (wanted is < 1 or > 10_000)
        {
            throw CliErrors.Usage("--limit must be 1-10,000.");
        }

        var items = new JsonArray();
        var cursor = inv.Opt("--cursor");
        JsonNode? observedAt = null;
        do
        {
            var page = await inv.Api().ReadAsync(path + (path.Contains('?', StringComparison.Ordinal) ? "&" : "?")
                + "limit=" + Math.Min(200, wanted - items.Count).ToString(CultureInfo.InvariantCulture)
                + (cursor is null ? "" : "&cursor=" + Uri.EscapeDataString(cursor)), inv.Command.Permission, ct);
            foreach (var item in page["items"]!.AsArray())
            {
                items.Add(item?.DeepClone());
            }

            observedAt = page["observedAt"]?.DeepClone();
            cursor = page["next"]?.GetValue<string>();
        }
        while (cursor is not null && items.Count < wanted);
        return CommandOutcome.Ok(new JsonObject { ["items"] = items, ["observedAt"] = observedAt, ["next"] = cursor });
    }

    static Guid CommandId(CliInvocation inv) =>
        (inv.CommandId = inv.Opt("--command-id") is { } s ? Guid.Parse(s) : Guid.CreateVersion7()).Value;

    /// <summary>Sends a mutation with its command ID and reason; the result is the API body plus <c>commandId</c>.</summary>
    static async Task<CommandOutcome> MutateAsync(CliInvocation inv, string path, JsonObject body, CancellationToken ct)
    {
        var id = inv.CommandId ?? CommandId(inv);
        body["commandId"] = id.ToString("D");
        if (inv.Opt("--reason") is { } reason)
        {
            body["reason"] = reason;
        }

        var r = await inv.Api().MutateAsync(path, body, ct);
        if (r.Status is < 200 or >= 300)
        {
            return CommandOutcome.Fail(ApiClient.ErrorOf(r, inv.Command.Permission), id);
        }

        var result = r.Body as JsonObject ?? new JsonObject();
        result["commandId"] ??= id.ToString("D");
        return CommandOutcome.Ok(result, id);
    }

    /// <summary>
    /// An expected revision from its option or, outside agent mode, read from the server first (19 §5.3); the revision
    /// used is reported on stderr.
    /// </summary>
    static async Task<long> ExpectedAsync(CliInvocation inv, string option, Func<Task<long>> read)
    {
        if (inv.Long(option) is { } given)
        {
            return given;
        }

        if (inv.Agent)
        {
            inv.MissingRevision = true;
            throw CliErrors.Usage($"{option} is required with --agent; read the target first.");
        }

        var value = await read();
        inv.Env.Error.WriteLine($"note: {option} {value.ToString(CultureInfo.InvariantCulture)} (read from the server)");
        return value;
    }

    /// <summary>Pages a list until an item matches.</summary>
    static async Task<JsonNode> FindAsync(CliInvocation inv, string path, string permission, Func<JsonNode, bool> match, CancellationToken ct)
    {
        string? cursor = null;
        do
        {
            var page = await inv.Api().ReadAsync(path + (path.Contains('?', StringComparison.Ordinal) ? "&" : "?") + "limit=200"
                + (cursor is null ? "" : "&cursor=" + Uri.EscapeDataString(cursor)), permission, ct);
            if (page["items"]!.AsArray().FirstOrDefault(i => i is not null && match(i)) is { } found)
            {
                return found;
            }

            cursor = page["next"]?.GetValue<string>();
        }
        while (cursor is not null);
        throw new CliFailure(CliErrors.Of("not_found", "Not found."));
    }

    static string Str(JsonNode? n) => n?.GetValue<string>() ?? "";

    // ------------------------------------------------------------------ contexts

    static Task<CommandOutcome> ContextAdd(CliInvocation inv, CancellationToken _)
    {
        var url = inv.Opt("--url")!;
        RunwickCli.CheckUrl(url);
        if (inv.Opt("--auth") == "basic" && inv.Opt("--user") is null)
        {
            throw CliErrors.Usage("--auth basic needs --user.");
        }

        var config = CliConfig.Load(inv.Env);
        var name = inv.Arg("name");
        config.Contexts[name] = new CliContext(url, inv.Opt("--auth")!, inv.Opt("--user"), inv.Opt("--secret-env"));
        config.Current ??= name;
        config.Save(inv.Env);
        return Task.FromResult(CommandOutcome.Ok(Contexts(inv, name)));
    }

    static Task<CommandOutcome> ContextUse(CliInvocation inv, CancellationToken _)
    {
        var config = CliConfig.Load(inv.Env);
        var name = inv.Arg("name");
        if (!config.Contexts.ContainsKey(name))
        {
            throw new CliFailure(CliErrors.Of("context_not_found", $"No context named '{name}'."));
        }

        config.Current = name;
        config.Save(inv.Env);
        return Task.FromResult(CommandOutcome.Ok(Contexts(inv, name)));
    }

    static Task<CommandOutcome> ContextShow(CliInvocation inv, CancellationToken _)
    {
        var name = inv.Globals.Context ?? CliConfig.Load(inv.Env).Current ?? throw new CliFailure(CliErrors.Of("context_not_found", "No context is selected."));
        return Task.FromResult(CommandOutcome.Ok(Contexts(inv, name)));
    }

    static Task<CommandOutcome> ContextRemove(CliInvocation inv, CancellationToken _)
    {
        var config = CliConfig.Load(inv.Env);
        var name = inv.Arg("name");
        var removed = Contexts(inv, name);
        if (!config.Contexts.Remove(name))
        {
            throw new CliFailure(CliErrors.Of("context_not_found", $"No context named '{name}'."));
        }

        if (config.Current == name)
        {
            config.Current = null;
        }

        config.Save(inv.Env);
        return Task.FromResult(CommandOutcome.Ok(removed));
    }

    /// <summary>One context (<paramref name="name"/>) or the list; reports whether the secret variable is set, never its value.</summary>
    static JsonNode? Contexts(CliInvocation inv, string? name)
    {
        var config = CliConfig.Load(inv.Env);
        var infos = config.Contexts.Select(c => new ContextInfo(c.Key, c.Value.Url, c.Value.Auth, c.Value.User, c.Value.SecretEnv, c.Key == config.Current,
            c.Value.SecretEnv is { } v && inv.Env.Variable(v) is not null)).ToList();
        if (name is null)
        {
            return JsonSerializer.SerializeToNode(new ContextList(infos), RunwickCli.Json);
        }

        return infos.FirstOrDefault(i => i.Name == name) is { } one
            ? JsonSerializer.SerializeToNode(one, RunwickCli.Json)
            : throw new CliFailure(CliErrors.Of("context_not_found", $"No context named '{name}'."));
    }

    static async Task<CommandOutcome> Login(CliInvocation inv, CancellationToken ct)
    {
        var api = inv.Api();
        await api.ReadAsync("antiforgery", null, ct);
        return CommandOutcome.Ok(JsonSerializer.SerializeToNode(new LoginResult(true, api.BaseUrl, api.Auth), RunwickCli.Json));
    }

    // ------------------------------------------------------------------ jobs

    static CliCommand JobControl(string action, string permission) =>
        Mutation("jobs " + action, action switch
        {
            "cancel" => "Cancel a job (a running job gets a cancel request).",
            "retry" => "Retry a failed or cancelled job as a new generation.",
            _ => "Run a succeeded job again as a new generation.",
        }, permission, typeof(JobControlBody), [Arg("id", V.Uuid, "Job ID.")],
        [Opt("--expected-generation", V.Integer, "Generation last observed; read when absent (not with --agent).", agent: true), ExpectedRevision],
        async (inv, ct) =>
        {
            CommandId(inv);
            var id = inv.Arg("id");
            JsonNode? job = null;
            async Task<JsonNode> ReadJob() => job ??= (await inv.Api().ReadAsync("jobs/" + id, ManagementPermissions.JobsRead, ct))["job"]!;
            var generation = await ExpectedAsync(inv, "--expected-generation", async () => (await ReadJob())["generation"]!.GetValue<long>());
            var revision = await ExpectedAsync(inv, "--expected-revision", async () => (await ReadJob())["revision"]!.GetValue<long>());
            return await MutateAsync(inv, $"jobs/{id}/{action}", new JsonObject { ["expectedGeneration"] = generation, ["expectedRevision"] = revision }, ct);
        });

    static async Task<CommandOutcome> JobsTail(CliInvocation inv, CancellationToken ct)
    {
        var interval = inv.Duration("--interval") ?? TimeSpan.FromSeconds(2);
        string? seen = null;
        JsonNode job;
        while (true)
        {
            job = (await inv.Api().ReadAsync("jobs/" + inv.Arg("id"), ManagementPermissions.JobsRead, ct))["job"]!;
            var state = Str(job["state"]);
            var key = state + "/" + job["revision"]!.ToJsonString();
            if (key != seen)
            {
                seen = key;
                await inv.Emit(job.DeepClone());
            }

            if (state is "succeeded" or "failed" or "cancelled")
            {
                return CommandOutcome.Ok(job);
            }

            try
            {
                await Task.Delay(interval, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return CommandOutcome.Ok(job);
            }
        }
    }

    static async Task<CommandOutcome> CancelMany(CliInvocation inv, CancellationToken ct)
    {
        var id = CommandId(inv);
        var file = inv.Opt("--file")!;
        var text = file == "-" ? await inv.Env.In.ReadToEndAsync(ct) : await File.ReadAllTextAsync(file, ct);
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length is 0 or > 100)
        {
            throw CliErrors.Usage("The file must hold 1-100 JSON lines.");
        }

        var items = new JsonArray();
        foreach (var line in lines)
        {
            Guid job;
            JsonNode? o;
            try
            {
                o = JsonNode.Parse(line);
                job = Guid.Parse(Str(o?["jobId"]), CultureInfo.InvariantCulture);
                items.Add(new JsonObject
                {
                    ["jobId"] = job.ToString("D"), ["commandId"] = BulkItemId(id, job).ToString("D"),
                    ["expectedGeneration"] = o!["expectedGeneration"]!.GetValue<int>(), ["expectedRevision"] = o["expectedRevision"]!.GetValue<long>(),
                });
            }
            catch (Exception ex) when (ex is JsonException or FormatException or InvalidOperationException or NullReferenceException)
            {
                throw CliErrors.Usage("Each line must be {\"jobId\", \"expectedGeneration\", \"expectedRevision\"}.");
            }
        }

        return await MutateAsync(inv, "jobs:cancel", new JsonObject { ["items"] = items }, ct);
    }

    /// <summary>Bulk item command ID (19 §5.4): SHA-256(prefix ‖ 0x00 ‖ command ID ‖ job ID, big-endian), version 8, RFC 4122 variant.</summary>
    internal static Guid BulkItemId(Guid commandId, Guid jobId)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData([.. Encoding.ASCII.GetBytes(Prefix), 0, .. commandId.ToByteArray(bigEndian: true), .. jobId.ToByteArray(bigEndian: true)], hash);
        hash[6] = (byte)((hash[6] & 0x0F) | 0x80);
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);
        return new Guid(hash[..16], bigEndian: true);
    }

    static Task<CommandOutcome> RevealAsync(CliInvocation inv, string path, CancellationToken ct)
    {
        CommandId(inv);
        var body = new JsonObject();
        if (inv.Opt("--reason") is { } reason)
        {
            body["reason"] = reason;
        }

        // A reveal carries no command ID on the wire (16 §3); the generated one only labels this run.
        return SendRevealAsync(inv, path, body, ct);
    }

    static async Task<CommandOutcome> SendRevealAsync(CliInvocation inv, string path, JsonObject body, CancellationToken ct)
    {
        var r = await inv.Api().MutateAsync(path, body, ct);
        return r.Status is >= 200 and < 300 ? CommandOutcome.Ok(r.Body, inv.CommandId) : CommandOutcome.Fail(ApiClient.ErrorOf(r, inv.Command.Permission), inv.CommandId);
    }

    static async Task<CommandOutcome> DispatchAsync(CliInvocation inv, CancellationToken ct)
    {
        CommandId(inv);
        var source = inv.Opt("--payload")!;
        var text = source == "-" ? await inv.Env.In.ReadToEndAsync(ct)
            : source.StartsWith('@') ? await File.ReadAllTextAsync(source[1..], ct)
            : throw CliErrors.Usage("--payload takes @file or - (stdin); a payload is never given inline.");
        if (Encoding.UTF8.GetByteCount(text) > 1_048_576)
        {
            return CommandOutcome.Fail(CliErrors.Of("payload_too_large", "The payload exceeds 1 MiB."), inv.CommandId);
        }

        JsonNode? payload;
        try
        {
            payload = JsonNode.Parse(text);
        }
        catch (JsonException)
        {
            throw CliErrors.Usage("The payload is not valid JSON.");
        }

        var body = new JsonObject { ["jobName"] = inv.Arg("job-name"), ["payload"] = payload };
        if (inv.Long("--payload-version") is { } version)
        {
            body["payloadVersion"] = version;
        }

        foreach (var (field, option) in new[] { ("queue", "--queue"), ("priority", "--priority"), ("notBefore", "--not-before") })
        {
            if (inv.Opt(option) is { } value)
            {
                body[field] = value;
            }
        }

        return await MutateAsync(inv, "jobs:dispatch", body, ct);
    }

    // ------------------------------------------------------------------ queues, capacity, schedules

    static CliCommand QueueControl(string action) =>
        Mutation("queues " + action, action == "pause" ? "Pause a queue (stops new reservations)." : "Resume a paused queue.", ManagementPermissions.QueuesControl,
            typeof(QueueControlBody), [Arg("name", V.String, "Queue name.")], [ExpectedRevision],
            async (inv, ct) =>
            {
                CommandId(inv);
                var name = inv.Arg("name");
                var revision = await ExpectedAsync(inv, "--expected-revision", async () =>
                    (await FindAsync(inv, "queues", ManagementPermissions.QueuesRead, q => Str(q["name"]) == name, ct))["revision"]!.GetValue<long>());
                return await MutateAsync(inv, $"queues/{Uri.EscapeDataString(name)}/{action}", new JsonObject { ["expectedRevision"] = revision }, ct);
            });

    static async Task<CommandOutcome> CapacitySet(CliInvocation inv, CancellationToken ct)
    {
        CommandId(inv);
        var (kind, key) = (inv.Arg("kind"), inv.Arg("key"));
        var revision = await ExpectedAsync(inv, "--expected-revision", async () =>
            (await FindAsync(inv, "scopes?kind=" + Uri.EscapeDataString(kind), ManagementPermissions.CapacityRead, s => Str(s["key"]) == key, ct))["policyRevision"]!
                .GetValue<long>());
        return await MutateAsync(inv, $"scopes/{Uri.EscapeDataString(kind)}/{Uri.EscapeDataString(key)}/capacity", new JsonObject
        {
            ["capacity"] = int.Parse(inv.Arg("capacity"), CultureInfo.InvariantCulture), ["expectedPolicyRevision"] = revision,
        }, ct);
    }

    /// <summary>Pause/resume use the control revision; trigger/revalidate the definition revision (16 §4).</summary>
    static CliCommand ScheduleControl(string action, string permission, bool control) =>
        Mutation("schedules " + action, action switch
        {
            "pause" => "Pause a schedule.",
            "resume" => "Resume a schedule (--policy skipPaused|runOnce|catchUp).",
            "trigger" => "Run a schedule now; the command ID is the trigger ID.",
            _ => "Revalidate a schedule that needs review.",
        }, permission, typeof(ScheduleControlBody), [Arg("key", V.String, "Schedule key.")],
        [Opt("--expected-revision", V.Integer, (control ? "Control" : "Definition") + " revision last observed; read when absent (not with --agent).", agent: true),
         .. action == "resume" ? new[] { Opt("--policy", V.String, "Missed runs while paused.", allowed: ["skipPaused", "runOnce", "catchUp"], def: "skipPaused") } : []],
        async (inv, ct) =>
        {
            CommandId(inv);
            var key = inv.Arg("key");
            var revision = await ExpectedAsync(inv, "--expected-revision", async () =>
                (await FindAsync(inv, "schedules", ManagementPermissions.SchedulesRead, s => Str(s["key"]) == key, ct))[control ? "controlRevision" : "definitionRevision"]!
                    .GetValue<long>());
            var body = new JsonObject { ["expectedRevision"] = revision };
            if (inv.Opt("--policy") is { } policy)
            {
                body["policy"] = policy;
            }

            return await MutateAsync(inv, $"schedules/{Uri.EscapeDataString(key)}/{action}", body, ct);
        });

    // ------------------------------------------------------------------ events

    static async Task<CommandOutcome> EventsTail(CliInvocation inv, CancellationToken ct)
    {
        var interval = inv.Duration("--interval") ?? TimeSpan.FromSeconds(2);
        var limit = inv.Long("--limit") ?? 100;
        if (interval < TimeSpan.FromMilliseconds(250) || interval > TimeSpan.FromSeconds(60) || limit is < 1 or > 1000)
        {
            throw CliErrors.Usage("--interval must be 250ms-60s and --limit 1-1,000.");
        }

        var after = inv.Opt("--after");
        try
        {
            while (true)
            {
                var page = await inv.Api().ReadAsync("events" + Query(("after", after), ("limit", limit.ToString(CultureInfo.InvariantCulture))),
                    ManagementPermissions.JobsRead, ct);
                var items = page["items"]!.AsArray();
                foreach (var item in items)
                {
                    await inv.Emit(item!.DeepClone());
                }

                after = Str(page["next"]);
                if (items.Count > 0)
                {
                    continue;
                }

                if (!inv.Flag("--follow") && page["partial"]?.GetValue<bool>() != true)
                {
                    break;
                }

                await Task.Delay(interval, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Ctrl-C ends the stream cleanly; the end line carries the cursor to resume from.
        }

        return CommandOutcome.Ok(JsonSerializer.SerializeToNode(new EventsEnd(after ?? "0.0"), RunwickCli.Json));
    }

    // ------------------------------------------------------------------ store commands: migrate, doctor

    static async Task<string?> ConnectionAsync(CliInvocation inv)
    {
        if (inv.Opt("--connection-env") is { } variable)
        {
            return inv.Env.Variable(variable) ?? throw new CliFailure(CliErrors.Of("credential_missing", $"Environment variable {variable} is not set."));
        }

        return inv.Flag("--connection-stdin")
            ? await inv.Env.In.ReadLineAsync() ?? throw new CliFailure(CliErrors.Of("credential_missing", "stdin holds no connection string."))
            : null;
    }

    static bool IsSqlite(CliInvocation inv) => inv.Opt("--provider") == "sqlite";

    static async Task<CommandOutcome> MigrateAsync(CliInvocation inv, CancellationToken ct)
    {
        var cs = await ConnectionAsync(inv) ?? throw CliErrors.Usage("migrate needs --connection-env or --connection-stdin.");
        try
        {
            return CommandOutcome.Ok(IsSqlite(inv)
                ? JsonSerializer.SerializeToNode(await RunwickSqliteMigrations.MigrateAsync(cs, ct), RunwickCli.Json)
                : JsonSerializer.SerializeToNode(await RunwickPostgresMigrations.MigrateAsync(cs, ct), RunwickCli.Json));
        }
        catch (PostgresMigrationException ex)
        {
            return MigrationFailed(ex.Code, ex.Message);
        }
        catch (SqliteMigrationException ex)
        {
            return MigrationFailed(ex.Code, ex.Message);
        }
    }

    static async Task<CommandOutcome> StoreAsync<T>(CliInvocation inv, Func<string, bool, Task<T>> run)
    {
        var cs = await ConnectionAsync(inv) ?? throw CliErrors.Usage(inv.Command.Name + " needs --connection-env or --connection-stdin.");
        try
        {
            return CommandOutcome.Ok(JsonSerializer.SerializeToNode(await run(cs, IsSqlite(inv)), RunwickCli.Json));
        }
        catch (PostgresMigrationException ex)
        {
            return MigrationFailed(ex.Code, ex.Message);
        }
        catch (SqliteMigrationException ex)
        {
            return MigrationFailed(ex.Code, ex.Message);
        }
    }

    static Task<CommandOutcome> StoreStatusAsync(CliInvocation inv, CancellationToken ct) =>
        StoreAsync(inv, (cs, sqlite) => sqlite ? RunwickSqliteMigrations.GetStatusAsync(cs, ct) : RunwickPostgresMigrations.GetStatusAsync(cs, ct));

    static Task<CommandOutcome> StoreAdmissionAsync(CliInvocation inv, CancellationToken ct)
    {
        var mode = inv.Opt("--mode")!;
        var application = inv.Opt("--application");
        return StoreAsync(inv, async (cs, sqlite) => new AdmissionResult(sqlite
            ? await RunwickSqliteMigrations.SetAdmissionModeAsync(cs, application, mode, "cli", ct)
            : await RunwickPostgresMigrations.SetAdmissionModeAsync(cs, application, mode, "cli", ct)));
    }

    static Task<CommandOutcome> StoreActivateCapabilityAsync(CliInvocation inv, CancellationToken ct)
    {
        var name = inv.Opt("--name")!;
        var application = inv.Opt("--application");
        return StoreAsync(inv, async (cs, sqlite) => new CapabilityResult(sqlite
            ? await RunwickSqliteMigrations.ActivateCapabilityAsync(cs, application, name, "cli", ct)
            : await RunwickPostgresMigrations.ActivateCapabilityAsync(cs, application, name, "cli", ct)));
    }

    internal sealed record CapabilityResult(IReadOnlyList<CapabilityChange> Changes);

    internal sealed record AdmissionResult(IReadOnlyList<AdmissionChange> Changes);

    static Task<CommandOutcome> StoreReidentifyAsync(CliInvocation inv, CancellationToken ct) =>
        StoreAsync(inv, (cs, sqlite) => sqlite ? RunwickSqliteMigrations.ReassignStoreIdAsync(cs, ct) : RunwickPostgresMigrations.ReassignStoreIdAsync(cs, ct));

    /// <summary>Provider codes pass through unchanged; never the inner exception (it may name the server or the file).</summary>
    static CommandOutcome MigrationFailed(string code, string message)
    {
        var unavailable = code is "store_unavailable" or "migration_lock_unavailable" or "migration_lock_timeout" or "migration_connection_lost";
        return CommandOutcome.Fail(new CliError(code, message, unavailable, unavailable ? "Retry when the store is reachable and no other migration runs." : "See the message.",
            null, unavailable ? CliErrors.Unavailable : CliErrors.Rejected));
    }

    static async Task<CommandOutcome> DoctorAsync(CliInvocation inv, CancellationToken ct)
    {
        var checks = new List<DoctorCheck>();
        var connection = await ConnectionAsync(inv);
        ApiClient? api = null;
        try
        {
            api = inv.Api();
        }
        catch (CliFailure f) when (connection is not null)
        {
            checks.Add(new DoctorCheck("api.reachable", "unverified", f.Error.Code));
        }

        if (api is not null)
        {
            await ApiChecksAsync(api, checks, ct);
        }

        if (connection is not null && IsSqlite(inv))
        {
            SqliteStoreChecks(connection, checks);
        }
        else if (connection is not null)
        {
            await StoreChecksAsync(connection, checks, ct);
        }

        var cli = IsSqlite(inv) ? SqliteMigrator.CurrentRevision : PgMigrator.CurrentRevision;
        var result = JsonSerializer.SerializeToNode(new DoctorResult(RunwickCli.Version, cli, checks), RunwickCli.Json);
        var failed = checks.Where(c => c.Status == "fail").Select(c => c.Name).ToList();
        return failed.Count == 0 ? CommandOutcome.Ok(result)
            : new CommandOutcome(result, CliErrors.Of("check_failed", "Failed: " + string.Join(", ", failed) + "."), null, new Dictionary<string, JsonNode>(), []);
    }

    static async Task ApiChecksAsync(ApiClient api, List<DoctorCheck> checks, CancellationToken ct)
    {
        ApiResponse auth;
        try
        {
            auth = await api.GetAsync("antiforgery", ct);
        }
        catch (CliFailure f)
        {
            checks.Add(new DoctorCheck("api.reachable", "fail", f.Error.Code));
            return;
        }

        checks.Add(new DoctorCheck("api.reachable", "pass", null));
        checks.Add(new DoctorCheck("api.auth", auth.Status == 200 ? "pass" : "fail", auth.Status == 200 ? null : ApiClient.ErrorOf(auth, null).Code));
        checks.Add(auth.Date is { } date
            ? new DoctorCheck("api.clock", (DateTimeOffset.UtcNow - date).Duration() > TimeSpan.FromSeconds(5) ? "warn" : "pass",
                $"{(DateTimeOffset.UtcNow - date).TotalSeconds.ToString("0.0", CultureInfo.InvariantCulture)} s")
            : new DoctorCheck("api.clock", "unverified", "no Date header"));

        var settings = await api.GetAsync("settings", ct);
        checks.Add(settings.Status switch
        {
            200 => new DoctorCheck("api.version", "pass", "v1"),
            403 or 401 => new DoctorCheck("api.version", "unverified", "needs Settings.Read"),
            _ => new DoctorCheck("api.version", "fail", ApiClient.ErrorOf(settings, null).Code),
        });
        foreach (var (name, field) in new[] { ("store.kind", "storeKind"), ("store.schemaRevision", "schemaRevision"), ("store.profile", "persistenceProfile") })
        {
            checks.Add(settings.Status == 200 && settings.Body?[field] is { } v
                ? new DoctorCheck(name, "pass", v is JsonValue j && j.TryGetValue<string>(out var s) ? s : v.ToJsonString())
                : new DoctorCheck(name, "unverified", "needs Settings.Read"));
        }
    }

    /// <summary>Store revision vs this CLI's, and the server settings (never reported as passed when unreadable).</summary>
    static async Task StoreChecksAsync(string connectionString, List<DoctorCheck> checks, CancellationToken ct)
    {
        try
        {
            var conn = new NpgsqlConnection(connectionString);
            await using (conn)
            {
                await conn.OpenAsync(ct);
                using (var settings = new NpgsqlCommand("""
                    SELECT current_setting('server_version_num')::int, pg_encoding_to_char(d.encoding), current_setting('synchronous_commit')
                    FROM pg_database AS d WHERE d.datname = current_database()
                    """, conn))
                {
                    var r = await settings.ExecuteReaderAsync(ct);
                    await using (r)
                    {
                        await r.ReadAsync(ct);
                        var (version, encoding, sync) = (r.GetInt32(0), r.GetString(1), r.GetString(2));
                        checks.Add(new DoctorCheck("store.settings", version >= 160000 && encoding == "UTF8" && sync != "off" ? "pass" : "fail",
                            $"server_version_num={version.ToString(CultureInfo.InvariantCulture)} encoding={encoding} synchronous_commit={sync}"));
                    }
                }

                using var meta = new NpgsqlCommand("""
                    SELECT schema_revision, migration_mode FROM runwick.store_metadata WHERE singleton_id = 1
                    """, conn);
                long revision;
                string mode;
                try
                {
                    var r = await meta.ExecuteReaderAsync(ct);
                    await using (r)
                    {
                        if (!await r.ReadAsync(ct))
                        {
                            checks.Add(new DoctorCheck("store.migration", "warn", "store not initialized: run migrate"));
                            return;
                        }

                        (revision, mode) = (r.GetInt64(0), r.GetString(1));
                    }
                }
                catch (PostgresException ex) when (ex.SqlState is "42P01" or "3F000")
                {
                    checks.Add(new DoctorCheck("store.migration", "warn", "store not initialized: run migrate"));
                    return;
                }

                checks.Add(MigrationCheck(revision, mode, PgMigrator.CurrentRevision));
            }
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException or ArgumentException)
        {
            checks.Add(new DoctorCheck("store.settings", "unverified", "could not connect"));
            checks.Add(new DoctorCheck("store.migration", "fail", "store_unavailable"));
        }
    }

    static DoctorCheck MigrationCheck(long revision, string mode, long cli) =>
        mode != "normal" ? new DoctorCheck("store.migration", "fail", $"migration_mode={mode}")
        : revision == cli ? new DoctorCheck("store.migration", "pass", $"revision {revision.ToString(CultureInfo.InvariantCulture)}")
        : revision < cli ? new DoctorCheck("store.migration", "warn", $"store revision {revision.ToString(CultureInfo.InvariantCulture)} < CLI {cli.ToString(CultureInfo.InvariantCulture)}: run migrate")
        : new DoctorCheck("store.migration", "fail", $"CLI older than store (store {revision.ToString(CultureInfo.InvariantCulture)}, CLI {cli.ToString(CultureInfo.InvariantCulture)})");

    /// <summary>
    /// SQLite store checks, using the store's own startup and Describe checks (20 §2, §6): engine version, journal_mode and
    /// topology, then revision, migration mode and schema objects. Opens the file without creating it and changes nothing.
    /// </summary>
    static void SqliteStoreChecks(string connectionString, List<DoctorCheck> checks)
    {
        try
        {
            var cs = SqFile.Normalize(connectionString, SqliteOpenMode.ReadWrite);
            var topology = SqFile.CheckTopology(SqFile.FilePath(cs));
            using var conn = new SqliteConnection(cs);
            conn.Open();
            var version = SqFile.CheckEngine(conn).Version;

            // journal_mode is the file's own setting; synchronous and foreign_keys are per connection and the store sets them.
            var journal = SqFile.Scalar(conn, "PRAGMA journal_mode");
            var initialized = SqFile.Scalar(conn, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'runwick_store_metadata'") == "1";
            var wal = string.Equals(journal, "wal", StringComparison.OrdinalIgnoreCase);
            checks.Add(new DoctorCheck("store.settings", !wal ? (initialized ? "fail" : "warn") : topology == "local" ? "pass" : "unverified",
                $"sqlite_version={version} journal_mode={journal} topology={topology}"));

            using var meta = conn.CreateCommand();
            meta.CommandText = "SELECT schema_revision, migration_mode FROM runwick_store_metadata WHERE singleton_id = 1";
            long revision;
            string mode;
            using (var r = initialized ? meta.ExecuteReader() : null)
            {
                if (r is null || !r.Read())
                {
                    checks.Add(new DoctorCheck("store.migration", "warn", "store not initialized: run migrate"));
                    return;
                }

                (revision, mode) = (r.GetInt64(0), r.GetString(1));
            }

            checks.Add(revision == SqliteMigrator.CurrentRevision && mode == "normal" && !SqliteMigrator.SchemaMatches(conn)
                ? new DoctorCheck("store.migration", "fail", "schema objects differ from this CLI's: schema_incompatible")
                : MigrationCheck(revision, mode, SqliteMigrator.CurrentRevision));
        }
        catch (SqProfileException ex)
        {
            checks.Add(new DoctorCheck("store.settings", "fail", ex.Code));
            checks.Add(new DoctorCheck("store.migration", "fail", ex.Code));
        }
        catch (SqliteException)
        {
            checks.Add(new DoctorCheck("store.settings", "unverified", "could not open"));
            checks.Add(new DoctorCheck("store.migration", "fail", "store_unavailable"));
        }
    }
}
