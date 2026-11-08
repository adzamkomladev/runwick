using System.Text.Json;
using System.Text.Json.Serialization;

namespace Runwick.Cli;

/// <summary>The process around one CLI run; tests build their own (19 §1).</summary>
public sealed class CliEnvironment
{
    public required TextWriter Out { get; init; }

    public required TextWriter Error { get; init; }

    public TextReader In { get; init; } = TextReader.Null;

    public IReadOnlyDictionary<string, string> Variables { get; init; } = new Dictionary<string, string>();

    public bool StdoutIsTerminal { get; init; }

    public bool StdinIsTerminal { get; init; }

    /// <summary>The config file; null means <c>$RUNWICK_CONFIG</c>, else the user's application-data folder.</summary>
    public string? ConfigPath { get; init; }

    /// <summary>Replaces the HTTP stack (tests: a TestServer handler). Null uses the system stack and trust store.</summary>
    public HttpMessageHandler? Handler { get; init; }

    /// <summary>Ctrl-C: streaming commands end with an <c>end</c> line.</summary>
    public CancellationToken Interrupt { get; init; }

    /// <summary>Reads a secret without echo; only used in human mode with a terminal on stdin.</summary>
    public Func<string?>? PromptSecret { get; init; }

    /// <summary>Waits between automatic resends of a mutation whose reply was lost (19 §5.4).</summary>
    internal IReadOnlyList<TimeSpan> ResendDelays { get; init; } = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    internal string? Variable(string name) => Variables.TryGetValue(name, out var v) && v.Length > 0 ? v : null;

    /// <summary>The real console process.</summary>
    public static CliEnvironment Console(CancellationToken interrupt) => new()
    {
        Out = System.Console.Out,
        Error = System.Console.Error,
        In = System.Console.In,
        Variables = Environment.GetEnvironmentVariables().Cast<System.Collections.DictionaryEntry>()
            .ToDictionary(e => (string)e.Key, e => (string?)e.Value ?? "", StringComparer.Ordinal),
        StdoutIsTerminal = !System.Console.IsOutputRedirected,
        StdinIsTerminal = !System.Console.IsInputRedirected,
        Interrupt = interrupt,
        PromptSecret = ReadHidden,
    };

    static string? ReadHidden()
    {
        System.Console.Error.Write("secret: ");
        var chars = new List<char>();
        for (var key = System.Console.ReadKey(intercept: true); key.Key != ConsoleKey.Enter; key = System.Console.ReadKey(intercept: true))
        {
            if (key.Key == ConsoleKey.Backspace && chars.Count > 0)
            {
                chars.RemoveAt(chars.Count - 1);
            }
            else if (!char.IsControl(key.KeyChar))
            {
                chars.Add(key.KeyChar);
            }
        }

        System.Console.Error.WriteLine();
        return new string([.. chars]);
    }
}

/// <summary>A saved context (19 §3). Never holds a secret: only the name of the variable that does.</summary>
internal sealed record CliContext(string Url, string Auth, string? User, string? SecretEnv);

internal sealed class CliConfig
{
    public int Version { get; set; } = 1;

    public string? Current { get; set; }

    public Dictionary<string, CliContext> Contexts { get; set; } = new(StringComparer.Ordinal);

    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string PathFor(CliEnvironment env) =>
        env.ConfigPath ?? env.Variable("RUNWICK_CONFIG")
        ?? System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "runwick", "config.json");

    public static CliConfig Load(CliEnvironment env)
    {
        var path = PathFor(env);
        return File.Exists(path) ? JsonSerializer.Deserialize<CliConfig>(File.ReadAllText(path), Json) ?? new CliConfig() : new CliConfig();
    }

    public void Save(CliEnvironment env)
    {
        var path = PathFor(env);
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(path)!);
        var options = new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        using var stream = new FileStream(path, options);
        JsonSerializer.Serialize(stream, this, Json);
    }
}
