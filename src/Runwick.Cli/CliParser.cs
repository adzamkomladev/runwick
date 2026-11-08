using System.Globalization;

namespace Runwick.Cli;

internal sealed record ParsedCommand(
    CliCommand Command, IReadOnlyDictionary<string, string> Arguments, IReadOnlyDictionary<string, string> Options, GlobalOptions Globals,
    IReadOnlyList<string> CommandArgv);

/// <summary>
/// POSIX-style long options over the command model (19 §4): <c>--name value</c> or <c>--name=value</c>, arguments in
/// declared order, no abbreviations. Global options may appear before or after the command words.
/// </summary>
internal static class CliParser
{
    static readonly string[] GlobalWithValue = ["--context", "--url", "--output", "--timeout"];
    static readonly string[] GlobalFlags = ["--agent", "--verbose", "--secret-stdin", "--help"];

    /// <summary>Globals only; never throws, so even a usage error can be rendered in the requested mode.</summary>
    public static GlobalOptions Globals(IReadOnlyList<string> args, out List<string> rest, out string? problem)
    {
        rest = [];
        problem = null;
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Count; i++)
        {
            var (name, inline) = Split(args[i]);
            if (GlobalWithValue.Contains(name))
            {
                var value = inline ?? (i + 1 < args.Count ? args[++i] : null);
                if (value is null || !values.TryAdd(name, value))
                {
                    problem ??= $"Option {name} needs one value.";
                }
            }
            else if (GlobalFlags.Contains(name) && inline is null)
            {
                if (!values.TryAdd(name, "true"))
                {
                    problem ??= $"Option {name} is repeated.";
                }
            }
            else
            {
                rest.Add(args[i]);
            }
        }

        var agent = values.ContainsKey("--agent");
        var output = values.GetValueOrDefault("--output", "human");
        if (agent && values.ContainsKey("--output"))
        {
            problem ??= "--agent and --output are exclusive.";
        }

        if (output is not ("human" or "json"))
        {
            problem ??= "--output must be human or json.";
            output = "human";
        }

        var timeout = 30;
        if (values.TryGetValue("--timeout", out var t) && (!int.TryParse(t, NumberStyles.None, CultureInfo.InvariantCulture, out timeout) || timeout is < 1 or > 300))
        {
            problem ??= "--timeout must be 1-300 seconds.";
            timeout = 30;
        }

        return new GlobalOptions(values.GetValueOrDefault("--context"), values.GetValueOrDefault("--url"), output, agent, timeout,
            values.ContainsKey("--verbose") && !agent, values.ContainsKey("--secret-stdin"), values.ContainsKey("--help"));
    }

    static (string Name, string? Inline) Split(string token) =>
        token.StartsWith("--", StringComparison.Ordinal) && token.IndexOf('=', StringComparison.Ordinal) is var eq and > 0
            ? (token[..eq], token[(eq + 1)..])
            : (token, null);

    /// <summary>Finds the longest command name the leading words spell.</summary>
    public static CliCommand? Find(IReadOnlyList<CliCommand> commands, IReadOnlyList<string> rest, out int words)
    {
        for (words = Math.Min(2, rest.Count); words > 0; words--)
        {
            var name = string.Join(' ', rest.Take(words));
            if (commands.FirstOrDefault(c => c.Name == name) is { } c)
            {
                return c;
            }
        }

        return null;
    }

    public static ParsedCommand Parse(IReadOnlyList<CliCommand> commands, IReadOnlyList<string> args)
    {
        var globals = Globals(args, out var rest, out var problem);
        if (problem is not null)
        {
            throw CliErrors.Usage(problem);
        }

        var command = Find(commands, rest, out var words) ?? throw CliErrors.Usage(rest.Count == 0 ? "No command given." : $"Unknown command '{string.Join(' ', rest.Take(2))}'.");
        var arguments = new Dictionary<string, string>(StringComparer.Ordinal);
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        if (globals.Url is { } url && command.Options.Any(o => o.Name == "--url"))
        {
            // context add takes --url as its own option (the URL to save), not as a connection override.
            options["--url"] = url;
            globals = globals with { Url = null };
        }

        var positional = 0;
        for (var i = words; i < rest.Count; i++)
        {
            var token = rest[i];
            if (token.StartsWith("--", StringComparison.Ordinal))
            {
                var (name, inline) = Split(token);
                var option = command.Options.FirstOrDefault(o => o.Name == name) ?? throw CliErrors.Usage($"Unknown option {name} for '{command.Name}'.");
                var value = option.Type == CliValueType.Flag
                    ? inline is null ? "true" : throw CliErrors.Usage($"{name} takes no value.")
                    : inline ?? (i + 1 < rest.Count ? rest[++i] : throw CliErrors.Usage($"{name} needs a value."));
                Check(name, option.Type, option.Allowed, value);
                if (!options.TryAdd(name, value))
                {
                    throw CliErrors.Usage($"Option {name} is repeated.");
                }
            }
            else
            {
                var argument = positional < command.Arguments.Count ? command.Arguments[positional++] : throw CliErrors.Usage($"Unexpected argument '{token}'.");
                Check(argument.Name, argument.Type, null, token);
                arguments[argument.Name] = token;
            }
        }

        if (positional < command.Arguments.Count)
        {
            throw CliErrors.Usage($"Missing argument <{command.Arguments[positional].Name}>.");
        }

        if (command.Options.FirstOrDefault(o => o.Required && !options.ContainsKey(o.Name)) is { } missing)
        {
            throw CliErrors.Usage($"Missing option {missing.Name}.");
        }

        return new ParsedCommand(command, arguments, options, globals, rest);
    }

    static void Check(string name, CliValueType type, IReadOnlyList<string>? allowed, string value)
    {
        var ok = type switch
        {
            CliValueType.Uuid => Guid.TryParseExact(value, "D", out _),
            CliValueType.Integer => long.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
            CliValueType.Instant => DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out _),
            CliValueType.Duration => TryDuration(value, out _),
            _ => value.Length > 0,
        };
        if (!ok || (allowed is not null && !allowed.Contains(value)))
        {
            throw CliErrors.Usage(allowed is null ? $"Value for {name} is not a valid {type.ToString().ToLowerInvariant()}." : $"{name} must be one of {string.Join(", ", allowed)}.");
        }
    }

    public static TimeSpan ParseDuration(string value) => TryDuration(value, out var d) ? d : throw CliErrors.Usage($"'{value}' is not a duration.");

    /// <summary><c>250ms</c>, <c>2s</c>, <c>1m</c>.</summary>
    static bool TryDuration(string value, out TimeSpan duration)
    {
        duration = default;
        var (number, unit) = value.EndsWith("ms", StringComparison.Ordinal) ? (value[..^2], 1d)
            : value.EndsWith('s') ? (value[..^1], 1000d)
            : value.EndsWith('m') ? (value[..^1], 60_000d)
            : (value, double.NaN);
        if (double.IsNaN(unit) || !double.TryParse(number, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var n))
        {
            return false;
        }

        duration = TimeSpan.FromMilliseconds(n * unit);
        return true;
    }
}
