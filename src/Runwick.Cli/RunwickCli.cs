using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;
using System.Text.Json.Serialization;

namespace Runwick.Cli;

/// <summary>The <c>runwick</c> tool in-process (19 §1): parse, run one command, render it in the chosen mode, return the exit code.</summary>
public static class RunwickCli
{
    internal static readonly string Version = typeof(RunwickCli).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        .Split('+')[0] ?? "0.0.0";

    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        RespectNullableAnnotations = true,
        TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver(),
    };

    static readonly JsonSerializerOptions Indented = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static async Task<int> RunAsync(string[] args, CliEnvironment env)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(env);
        var globals = CliParser.Globals(args, out var rest, out _);
        var command = CliParser.Find(CliCommands.All, rest, out _);
        ParsedCommand? parsed = null;
        CliInvocation? invocation = null;
        ApiClient? api = null;
        CommandOutcome outcome;
        try
        {
            if (command is null && globals.Help)
            {
                env.Out.WriteLine(Help(null));
                return 0;
            }

            parsed = CliParser.Parse(CliCommands.All, args);
            (command, globals) = (parsed.Command, parsed.Globals);
            if (globals.Help)
            {
                env.Out.WriteLine(Help(command));
                return 0;
            }

            invocation = new CliInvocation
            {
                Command = command,
                Arguments = parsed.Arguments,
                Options = parsed.Options,
                Env = env,
                Globals = globals,
                Api = () => api ??= Connect(env, globals),
                Emit = item => Item(env, globals, command, item),
            };
            outcome = await command.Run(invocation, env.Interrupt);
        }
        catch (CliFailure f)
        {
            outcome = CommandOutcome.Fail(f.Error);
        }
        catch (OperationCanceledException) when (env.Interrupt.IsCancellationRequested)
        {
            outcome = CommandOutcome.Ok(null);
        }
#pragma warning disable CA1031 // The process boundary: every failure becomes internal_error with the exception type only (19 §8).
        catch (Exception ex)
#pragma warning restore CA1031
        {
            if (globals.Verbose)
            {
                env.Error.WriteLine(ex.ToString());
            }

            outcome = CommandOutcome.Fail(CliErrors.Of("internal_error", $"Unexpected {ex.GetType().Name}."));
        }
        finally
        {
            api?.Dispose();
        }

        outcome = outcome with
        {
            CommandId = outcome.CommandId ?? invocation?.CommandId,
            Revisions = Revisions(outcome.Result),
        };
        outcome = outcome with { NextActions = [.. outcome.NextActions, .. NextActions(parsed, globals, invocation, outcome)] };
        Render(env, globals, command, outcome);
        return outcome.Error?.ExitCode ?? CliErrors.Ok;
    }

    // ------------------------------------------------------------------ connection (19 §3)

    static ApiClient Connect(CliEnvironment env, GlobalOptions g)
    {
        var config = CliConfig.Load(env);
        var name = g.Context ?? config.Current;
        CliContext context;
        if (name is not null)
        {
            context = config.Contexts.TryGetValue(name, out var c) ? c : throw new CliFailure(CliErrors.Of("context_not_found", $"No context named '{name}'."));
        }
        else
        {
            context = g.Url is not null ? new CliContext(g.Url, "bearer", null, null)
                : throw new CliFailure(CliErrors.Of("context_not_found", "No context is selected and no --url was given."));
        }

        var url = g.Url ?? context.Url;
        CheckUrl(url);
        var secret = (g.SecretStdin ? env.In.ReadLine() : null)
            ?? (context.SecretEnv is { } variable ? env.Variable(variable) : null)
            ?? env.Variable("RUNWICK_SECRET")
            ?? (!g.Agent && g.Output == "human" && env.StdinIsTerminal ? env.PromptSecret?.Invoke() : null);
        if (string.IsNullOrEmpty(secret))
        {
            throw new CliFailure(CliErrors.Of("credential_missing", "No secret: set the context's secret variable or RUNWICK_SECRET, or pass --secret-stdin."));
        }

        return new ApiClient(env, url, context.Auth, context.User, secret, g.TimeoutSeconds, g.Verbose);
    }

    /// <summary>https, or http to a loopback host only.</summary>
    internal static void CheckUrl(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || !(uri.Scheme == Uri.UriSchemeHttps || (uri.Scheme == Uri.UriSchemeHttp && uri.IsLoopback)))
        {
            throw new CliFailure(CliErrors.Of("insecure_url", "The URL must be https:// (or http:// to a loopback host)."));
        }
    }

    static (string? Context, string? Url) Identity(CliEnvironment env, GlobalOptions g)
    {
        try
        {
            var config = CliConfig.Load(env);
            var name = g.Context ?? config.Current;
            return (name, g.Url ?? (name is not null && config.Contexts.TryGetValue(name, out var c) ? c.Url : null));
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            return (g.Context, g.Url);
        }
    }

    // ------------------------------------------------------------------ envelope parts (19 §5.3)

    /// <summary>Every revisioned target in a result, keyed by its full identity.</summary>
    internal static Dictionary<string, JsonNode> Revisions(JsonNode? result)
    {
        var map = new Dictionary<string, JsonNode>(StringComparer.Ordinal);
        Visit(result);
        return map;

        void Visit(JsonNode? node)
        {
            switch (node)
            {
                case JsonArray a:
                    foreach (var x in a)
                    {
                        Visit(x);
                    }

                    break;
                case JsonObject o:
                    if (S(o, "jobId") is { } job && o["generation"] is JsonValue g && o["revision"] is JsonValue r)
                    {
                        map["job:" + job] = new JsonObject { ["generation"] = g.DeepClone(), ["revision"] = r.DeepClone() };
                    }
                    else if ((S(o, "queue") ?? S(o, "name")) is { } queue && o["paused"] is not null && o["revision"] is JsonValue qr && o["controlRevision"] is null)
                    {
                        map["queue:" + queue] = qr.DeepClone();
                    }
                    else if ((S(o, "kind") ?? S(o, "scopeKind")) is { } kind && (S(o, "key") ?? S(o, "scopeKey")) is { } key && o["policyRevision"] is JsonValue p)
                    {
                        map[$"scope:{kind}/{key}"] = p.DeepClone();
                    }
                    else if (S(o, "key") is { } schedule && o["definitionRevision"] is JsonValue d && o["controlRevision"] is JsonValue c)
                    {
                        map["schedule:" + schedule] = new JsonObject { ["definition"] = d.DeepClone(), ["control"] = c.DeepClone() };
                    }

                    foreach (var (_, v) in o)
                    {
                        Visit(v);
                    }

                    break;
            }
        }
    }

    static string? S(JsonObject o, string name) => o[name] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

    static List<NextAction> NextActions(ParsedCommand? p, GlobalOptions g, CliInvocation? inv, CommandOutcome o)
    {
        var next = new List<NextAction>();
        if (p is null)
        {
            return next;
        }

        List<string> Argv(IEnumerable<string> tail) =>
            ["runwick", "--agent", .. g.Context is { } c ? new[] { "--context", c } : [], .. g.Url is { } u ? new[] { "--url", u } : [], .. tail];

        var words = p.Command.Name.Split(' ');
        var id = p.Arguments.TryGetValue("id", out var jobId) ? jobId : null;
        NextAction? read = words[0] switch
        {
            "jobs" when id is not null => new NextAction("jobs get", Argv(["jobs", "get", id]), "Read the current generation and revision."),
            "queues" => new NextAction("queues list", Argv(["queues", "list"]), "Read the queue's current revision."),
            "capacity" => new NextAction("capacity list", Argv(["capacity", "list"]), "Read the scope's current policy revision."),
            "schedules" => new NextAction("schedules list", Argv(["schedules", "list"]), "Read the schedule's current revisions."),
            _ => null,
        };

        if (o.Error is { } e)
        {
            if (e.Code is "outcome_unknown" or "store_unavailable" && p.Command.Mutation && o.CommandId is { } cmd)
            {
                next.Add(new NextAction(p.Command.Name, Argv(With(p.CommandArgv, "--command-id", cmd.ToString("D"))), "Resend the same command; the server replays it if it applied."));
            }
            else if (e.Code is "stale_revision" or "stale_generation" && read is not null)
            {
                next.Add(read);
            }
            else if (e.Code is "credential_missing" or "unauthenticated")
            {
                next.Add(new NextAction("login", Argv(["login"]), "Check the context's credentials."));
            }
            else if (e.Code == "usage_error" && inv?.MissingRevision == true && read is not null)
            {
                next.Add(read);
            }

            return next;
        }

        if (p.Command.Name == "jobs get" && o.Result?["job"] is JsonObject job)
        {
            var action = S(job, "state") switch
            {
                "failed" or "cancelled" => "retry",
                "succeeded" => "rerun",
                _ => null,
            };
            if (action is not null)
            {
                next.Add(new NextAction("jobs " + action, Argv(["jobs", action, S(job, "jobId")!, "--expected-generation", job["generation"]!.ToJsonString(),
                    "--expected-revision", job["revision"]!.ToJsonString()]), action == "retry" ? "Retry the job." : "Run the succeeded job again."));
            }
        }

        if (p.Command.Name == "events tail" && o.Result?["cursor"] is JsonValue cursor)
        {
            next.Add(new NextAction("events tail", Argv(["events", "tail", "--after", cursor.GetValue<string>()]), "Continue from the last cursor."));
        }
        else if (o.Result?["next"] is JsonValue more && more.TryGetValue<string>(out var nextCursor))
        {
            next.Add(new NextAction(p.Command.Name, Argv(With(p.CommandArgv, "--cursor", nextCursor)), "Read the next page."));
        }

        return next;
    }

    /// <summary>The argv with one option set (replacing an earlier value).</summary>
    static List<string> With(IReadOnlyList<string> argv, string option, string value)
    {
        var list = new List<string>();
        for (var i = 0; i < argv.Count; i++)
        {
            if (argv[i] == option)
            {
                i++;
            }
            else if (!argv[i].StartsWith(option + "=", StringComparison.Ordinal))
            {
                list.Add(argv[i]);
            }
        }

        return [.. list, option, value];
    }

    // ------------------------------------------------------------------ rendering (19 §5.1, §5.3)

    internal sealed record EnvelopeError(string Code, string Message, bool Retryable, string Hint, long? ActualRevision);

    internal sealed record CliInfo(string Version, string? Context, string? Url);

    internal sealed record AgentEnvelope(
        int SchemaVersion, string Command, bool Ok, JsonNode? Result, EnvelopeError? Error, Guid? CommandId,
        IReadOnlyDictionary<string, JsonNode> Revisions, IReadOnlyList<NextAction> NextActions, CliInfo Cli,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Stream = null);

    static AgentEnvelope Envelope(CliEnvironment env, GlobalOptions g, string command, CommandOutcome o, string? stream)
    {
        var (context, url) = Identity(env, g);
        return new AgentEnvelope(1, command, o.Error is null, o.Result,
            o.Error is { } e ? new EnvelopeError(e.Code, e.Message, e.Retryable, e.Hint, e.ActualRevision) : null,
            o.CommandId, o.Revisions, o.NextActions, new CliInfo(Version, context, url), stream);
    }

    static Task Item(CliEnvironment env, GlobalOptions g, CliCommand command, JsonNode item)
    {
        if (g.Agent)
        {
            var o = CommandOutcome.Ok(item) with { Revisions = Revisions(item) };
            env.Out.WriteLine(JsonSerializer.Serialize(Envelope(env, g, command.Name, o, "item"), Json));
        }
        else if (g.Output == "json")
        {
            env.Out.WriteLine(item.ToJsonString());
        }
        else
        {
            env.Out.WriteLine(string.Join("  ", Flatten(item).Select(kv => kv.Key + "=" + kv.Value)));
        }

        return env.Out.FlushAsync();
    }

    static void Render(CliEnvironment env, GlobalOptions g, CliCommand? command, CommandOutcome o)
    {
        var name = command?.Name ?? "unknown";
        if (g.Agent)
        {
            env.Out.WriteLine(JsonSerializer.Serialize(Envelope(env, g, name, o, command?.Streaming == true ? "end" : null), Json));
            return;
        }

        if (o.Error is { } e)
        {
            if (g.Output == "json")
            {
                var body = new JsonObject { ["code"] = e.Code, ["message"] = e.Message };
                if (o.CommandId is { } id)
                {
                    body["commandId"] = id.ToString("D");
                }

                if (e.ActualRevision is { } actual)
                {
                    body["actualRevision"] = actual;
                }

                env.Out.WriteLine(body.ToJsonString(Indented));
            }
            else
            {
                env.Error.WriteLine($"error {e.Code}: {e.Message}");
                env.Error.WriteLine($"hint: {e.Hint}");
            }

            return;
        }

        if (command?.Streaming == true || o.Result is null)
        {
            return;
        }

        if (g.Output == "json")
        {
            env.Out.WriteLine(o.Result.ToJsonString(Indented));
            return;
        }

        if (o.Result["items"] is JsonArray items)
        {
            Table(env.Out, command!.Columns, items);
            foreach (var (k, v) in Flatten(o.Result).Where(kv => !kv.Key.StartsWith("items", StringComparison.Ordinal)))
            {
                env.Out.WriteLine($"{k}: {v}");
            }
        }
        else
        {
            foreach (var (k, v) in Flatten(o.Result))
            {
                env.Out.WriteLine($"{k}: {v}");
            }
        }

        if (o.Result["replayed"] is JsonValue r && r.TryGetValue<bool>(out var replayed) && replayed)
        {
            env.Out.WriteLine("replayed (original result returned)");
        }
    }

    static void Table(TextWriter output, IReadOnlyList<HumanColumn> columns, JsonArray items)
    {
        if (columns.Count == 0)
        {
            columns = items.FirstOrDefault() is JsonObject first
                ? [.. first.Where(kv => kv.Value is JsonValue or null).Select(kv => new HumanColumn(kv.Key, kv.Key))]
                : [];
        }

        var rows = items.Select(i => columns.Select(c => i?[c.Property] is { } v ? Scalar(v) : "").ToArray()).ToList();
        var widths = columns.Select((c, i) => rows.Select(r => r[i].Length).Prepend(c.Header.Length).Max()).ToArray();
        output.WriteLine(string.Join("  ", columns.Select((c, i) => c.Header.PadRight(widths[i]))).TrimEnd());
        foreach (var row in rows)
        {
            output.WriteLine(string.Join("  ", row.Select((v, i) => v.PadRight(widths[i]))).TrimEnd());
        }
    }

    static string Scalar(JsonNode v) => v is JsonValue j && j.TryGetValue<string>(out var s) ? s : v.ToJsonString();

    /// <summary>Scalars as <c>path: value</c>; a nested object or array contributes its members under a dotted path.</summary>
    static IEnumerable<KeyValuePair<string, string>> Flatten(JsonNode? node, string prefix = "")
    {
        switch (node)
        {
            case JsonObject o:
                foreach (var (k, v) in o)
                {
                    foreach (var x in Flatten(v, prefix.Length == 0 ? k : prefix + "." + k))
                    {
                        yield return x;
                    }
                }

                break;
            case JsonArray a:
                for (var i = 0; i < a.Count; i++)
                {
                    foreach (var x in Flatten(a[i], $"{prefix}[{i}]"))
                    {
                        yield return x;
                    }
                }

                break;
            case null:
                break;
            default:
                yield return KeyValuePair.Create(prefix, Scalar(node));
                break;
        }
    }

    static string Help(CliCommand? command)
    {
        if (command is null)
        {
            return "runwick <command> [options]   (runwick --agent describe for the machine-readable list)\n\n"
                + string.Join('\n', CliCommands.All.Select(c => $"  {c.Name,-24} {c.Summary}"))
                + "\n\nGlobal: --context <name> --url <url> --output human|json --agent --timeout <s> --verbose --secret-stdin";
        }

        return $"runwick {command.Name} {string.Join(' ', command.Arguments.Select(a => $"<{a.Name}>"))}\n{command.Summary}\n"
            + (command.Permission is { } p ? $"Permission: {p}\n" : "")
            + string.Join('\n', command.Options.Select(o =>
                $"  {o.Name}{(o.Type == CliValueType.Flag ? "" : " <" + o.Type.ToString().ToLowerInvariant() + ">")}{(o.Required ? " (required)" : o.RequiredInAgentMode ? " (required with --agent)" : "")}  {o.Description}"));
    }

    // ------------------------------------------------------------------ describe (19 §5.5)

    internal sealed record DescribeArgument(string Name, string Type, string Description);

    internal sealed record DescribeOption(string Name, string Type, bool Required, bool RequiredInAgentMode, IReadOnlyList<string>? Allowed, string? Default,
        string Description);

    internal sealed record DescribeCommand(string Name, string Summary, bool Mutation, bool Streaming, string? Permission,
        IReadOnlyList<DescribeArgument> Arguments, IReadOnlyList<DescribeOption> Options, JsonNode ResultSchema);

    internal sealed record DescribeError(string Code, bool Retryable, string Hint);

    internal sealed record DescribeDocument(string Name, string Version, string ApiVersion, JsonNode EnvelopeSchema,
        IReadOnlyDictionary<string, string> ExitCodes, IReadOnlyList<DescribeError> ErrorCodes, IReadOnlyList<DescribeCommand> Commands);

    /// <summary>A JSON Schema 2020-12 document for a type, as the serializer writes it.</summary>
    internal static JsonNode SchemaOf(Type type)
    {
        var schema = JsonSchemaExporter.GetJsonSchemaAsNode(Json, type, new JsonSchemaExporterOptions { TreatNullObliviousAsNonNullable = true });
        if (schema is JsonObject o)
        {
            o.Insert(0, "$schema", "https://json-schema.org/draft/2020-12/schema");
        }

        return schema;
    }

    internal static JsonNode EnvelopeSchema() => SchemaOf(typeof(AgentEnvelope));

    internal static JsonNode DescribeSchema() => SchemaOf(typeof(DescribeDocument));

    internal static JsonNode Describe() => JsonSerializer.SerializeToNode(new DescribeDocument("runwick", Version, "v1", EnvelopeSchema(), CliErrors.ExitCodes,
        [.. CliErrors.Table.Select(e => new DescribeError(e.Key, e.Value.Retryable, e.Value.Hint))],
        [.. CliCommands.All.Select(c => new DescribeCommand(c.Name, c.Summary, c.Mutation, c.Streaming, c.Permission,
            [.. c.Arguments.Select(a => new DescribeArgument(a.Name, TypeName(a.Type), a.Description))],
            [.. c.Options.Select(o => new DescribeOption(o.Name, TypeName(o.Type), o.Required, o.RequiredInAgentMode, o.Allowed, o.Default, o.Description))],
            SchemaOf(c.ResultType)))]), Json)!;

    static string TypeName(CliValueType t) => t.ToString().ToLowerInvariant();
}
