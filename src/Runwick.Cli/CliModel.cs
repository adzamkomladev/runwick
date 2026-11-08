using System.Text.Json.Nodes;

namespace Runwick.Cli;

// The command model (19 §4): one declaration per command feeds the parser, help, describe, the HTTP call and the renderers.

internal enum CliValueType
{
    String,
    Uuid,
    Integer,
    Instant,
    Duration,
    Path,
    Flag,
}

internal sealed record CliArgument(string Name, CliValueType Type, string Description);

internal sealed record CliOption(
    string Name, CliValueType Type, bool Required, bool RequiredInAgentMode, IReadOnlyList<string>? Allowed, string? Default, string Description);

/// <summary>A human table column: header and the item property it shows.</summary>
internal sealed record HumanColumn(string Header, string Property);

internal sealed record CliCommand(
    string Name,
    string Summary,
    IReadOnlyList<CliArgument> Arguments,
    IReadOnlyList<CliOption> Options,
    bool Mutation,
    bool Streaming,
    string? Permission,
    Type ResultType,
    IReadOnlyList<HumanColumn> Columns,
    Func<CliInvocation, CancellationToken, Task<CommandOutcome>> Run);

internal sealed record CommandOutcome(
    JsonNode? Result,
    CliError? Error,
    Guid? CommandId,
    IReadOnlyDictionary<string, JsonNode> Revisions,
    IReadOnlyList<NextAction> NextActions)
{
    public static CommandOutcome Ok(JsonNode? result, Guid? commandId = null) => new(result, null, commandId, new Dictionary<string, JsonNode>(), []);

    public static CommandOutcome Fail(CliError error, Guid? commandId = null, IReadOnlyList<NextAction>? next = null) =>
        new(null, error, commandId, new Dictionary<string, JsonNode>(), next ?? []);
}

internal sealed record CliError(string Code, string Message, bool Retryable, string Hint, long? ActualRevision, int ExitCode);

internal sealed record NextAction(string Command, IReadOnlyList<string> Argv, string Reason);

/// <summary>A failure that ends the command with <see cref="Error"/>.</summary>
internal sealed class CliFailure(CliError error) : Exception(error.Message)
{
    public CliError Error { get; } = error;
}

/// <summary>Exit codes (19 §5.5) and the error table (19 §5.6): one source for errors, hints and describe.</summary>
internal static class CliErrors
{
    public const int Ok = 0, Rejected = 1, UsageError = 2, Unknown = 3, Unavailable = 4, Auth = 5;

    public static IReadOnlyDictionary<string, string> ExitCodes { get; } = new Dictionary<string, string>
    {
        ["0"] = "ok", ["1"] = "rejected", ["2"] = "usage_error", ["3"] = "outcome_unknown", ["4"] = "unavailable", ["5"] = "unauthenticated_or_forbidden",
    };

    /// <summary>code → (exit code, retryable, hint).</summary>
    public static IReadOnlyDictionary<string, (int Exit, bool Retryable, string Hint)> Table { get; } = new Dictionary<string, (int, bool, string)>
    {
        ["usage_error"] = (UsageError, false, "Run 'runwick <command> --help' or 'runwick --agent describe'."),
        ["context_not_found"] = (UsageError, false, "Add a context with 'runwick context add' or pass --url."),
        ["insecure_url"] = (UsageError, false, "Use an https:// URL (http:// only to a loopback host)."),
        ["credential_missing"] = (Auth, false, "Set the context's secret variable or pass --secret-stdin."),
        ["connection_failed"] = (Unavailable, true, "The server is unreachable; nothing was sent."),
        ["tls_error"] = (Unavailable, true, "The TLS connection failed; nothing was sent."),
        ["response_invalid"] = (Unavailable, false, "The CLI and server versions may differ."),
        ["endpoint_not_supported"] = (Unavailable, false, "The server is older than the CLI."),
        ["unauthenticated"] = (Auth, false, "Check the context's credentials ('runwick login')."),
        ["forbidden"] = (Auth, false, "Ask for the permission this command needs."),
        ["not_found"] = (Rejected, false, "Check the ID; jobs outside your queues are reported as not found."),
        ["detail_expired"] = (Rejected, false, "Retention removed this detail."),
        ["invalid_request"] = (Rejected, false, "Fix the request and retry."),
        ["invalid_cursor"] = (Rejected, false, "Cursors expire after 15 minutes; start the list again."),
        ["antiforgery_invalid"] = (Rejected, false, "Retry; a bearer context needs a group mapped with ForMachineClients()."),
        ["payload_too_large"] = (Rejected, false, "Payloads are at most 1 MiB."),
        ["unsupported_media_type"] = (Rejected, false, "The CLI and server versions may differ."),
        ["stale_revision"] = (Rejected, false, "Read the target again and retry with its current revision."),
        ["stale_generation"] = (Rejected, false, "Read the job again and retry with its current generation and revision."),
        ["invalid_state"] = (Rejected, false, "The target's state does not allow this; read it again."),
        ["unique_conflict"] = (Rejected, false, "A live uniqueness binding conflicts; read the existing job."),
        ["command_conflict"] = (Rejected, false, "This command ID was used with another body; use a new --command-id."),
        ["cancel_already_requested"] = (Rejected, false, "Cancellation is already requested; wait for the job to end."),
        ["no_change"] = (Rejected, false, "The target is already in that state."),
        ["job_not_registered"] = (Rejected, false, "Register the job type in the management host (AddJob<T>())."),
        ["payload_invalid"] = (Rejected, false, "Fix the payload so it binds to the job type."),
        ["payload_version_unsupported"] = (Rejected, false, "Send the job's current payload version (or omit --payload-version)."),
        ["admission_closed"] = (Unavailable, true, "Retry after the application reopens."),
        ["rate_limited"] = (Unavailable, true, "Wait for Retry-After and retry."),
        ["busy"] = (Unavailable, true, "Retry shortly."),
        ["query_budget_exceeded"] = (Unavailable, true, "Narrow the filters or retry."),
        ["store_unavailable"] = (Unavailable, true, "Retry; mutations resend with the same --command-id."),
        ["outcome_unknown"] = (Unknown, true, "Resend the same command with the same --command-id."),
        ["internal_error"] = (Unavailable, false, "Run again with --verbose for details."),
        ["check_failed"] = (Rejected, false, "See the failed checks."),
    };

    public static CliError Of(string code, string message, long? actualRevision = null, string? hint = null)
    {
        // Codes the table does not list (provider migration codes, new server codes) are rejections, not retryable.
        var (exit, retryable, h) = Table.TryGetValue(code, out var t) ? t : (Rejected, false, "See the message.");
        return new CliError(code, message, retryable, hint ?? h, actualRevision, exit);
    }

    public static CliFailure Usage(string message) => new(Of("usage_error", message));
}

/// <summary>Everything a command run sees: its parsed values, the API client and the output mode.</summary>
internal sealed class CliInvocation
{
    public required CliCommand Command { get; init; }

    public required IReadOnlyDictionary<string, string> Arguments { get; init; }

    /// <summary>Option name (with dashes) → raw value (<c>"true"</c> for a flag).</summary>
    public required IReadOnlyDictionary<string, string> Options { get; init; }

    public required CliEnvironment Env { get; init; }

    public required GlobalOptions Globals { get; init; }

    public required Func<ApiClient> Api { get; init; }

    /// <summary>Streaming commands write each item here as it arrives.</summary>
    public required Func<JsonNode, Task> Emit { get; init; }

    public bool Agent => Globals.Agent;

    /// <summary>A mutation's command ID, set before anything can fail so every envelope of a mutation carries it.</summary>
    public Guid? CommandId { get; set; }

    /// <summary>Agent mode refused a missing expected revision: the next action reads the target.</summary>
    public bool MissingRevision { get; set; }

    public string Arg(string name) => Arguments[name];

    public string? Opt(string name) => Options.TryGetValue(name, out var v) ? v : null;

    public bool Flag(string name) => Options.ContainsKey(name);

    public long? Long(string name) => Opt(name) is { } v ? long.Parse(v, System.Globalization.CultureInfo.InvariantCulture) : null;

    public TimeSpan? Duration(string name) => Opt(name) is { } v ? CliParser.ParseDuration(v) : null;
}

internal sealed record GlobalOptions(
    string? Context, string? Url, string Output, bool Agent, int TimeoutSeconds, bool Verbose, bool SecretStdin, bool Help);
