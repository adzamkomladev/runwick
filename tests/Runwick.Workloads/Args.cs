using System.Globalization;
using System.Text.Json;

namespace Runwick.Workloads;

/// <summary><c>--name value</c> pairs and bare <c>--flag</c>s after the mode word.</summary>
internal sealed class Args
{
    readonly Dictionary<string, string?> _values = new(StringComparer.Ordinal);

    public Args(IReadOnlyList<string> args)
    {
        for (var i = 0; i < args.Count; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
            {
                Positional.Add(args[i]);
                continue;
            }

            var name = args[i][2..];
            _values[name] = i + 1 < args.Count && !args[i + 1].StartsWith("--", StringComparison.Ordinal) ? args[++i] : null;
        }
    }

    public List<string> Positional { get; } = [];

    public bool Has(string name) => _values.ContainsKey(name);

    public string Str(string name) => _values.TryGetValue(name, out var v) && v is not null ? v : throw new ArgumentException($"--{name} is required");

    public string? OptStr(string name) => _values.TryGetValue(name, out var v) ? v : null;

    public int Int(string name, int fallback) => OptInt(name) ?? fallback;

    public int? OptInt(string name) => OptStr(name) is { } v ? int.Parse(v, CultureInfo.InvariantCulture) : null;

    public double Double(string name, double fallback) =>
        OptStr(name) is { } v ? double.Parse(v, CultureInfo.InvariantCulture) : fallback;
}

internal static class Json
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}
