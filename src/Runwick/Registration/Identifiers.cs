using System.Text;
using System.Text.RegularExpressions;

namespace Runwick.Registration;

/// <summary>Identity grammar of spec 01 §2.</summary>
internal static partial class Identifiers
{
    public const string DefaultQueue = "default";
    public const string DefaultPriority = "medium";

    public static readonly IReadOnlyList<string> Priorities = ["high", "medium", "low"];

    [GeneratedRegex(@"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z", RegexOptions.CultureInvariant)]
    private static partial Regex Name128();

    [GeneratedRegex(@"\A[A-Za-z0-9][A-Za-z0-9._-]{0,199}\z", RegexOptions.CultureInvariant)]
    private static partial Regex Name200();

    [GeneratedRegex(@"\A[a-z_][a-z0-9_]{0,62}\z", RegexOptions.CultureInvariant)]
    private static partial Regex Schema();

    [GeneratedRegex(@"\A00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}\z", RegexOptions.CultureInvariant)]
    private static partial Regex TraceParent();

    public static bool IsName(string? value) => value is not null && Name128().IsMatch(value);

    public static bool IsJobName(string? value) => value is not null && Name200().IsMatch(value);

    public static bool IsSchema(string? value) => value is not null && Schema().IsMatch(value);

    public static bool IsPriority(string? value) => value is "low" or "medium" or "high";

    /// <summary>1–512 UTF-8 bytes, valid Unicode, no Cc controls (spec 01 §2.3).</summary>
    public static bool IsKey(string? value) =>
        !string.IsNullOrEmpty(value) && IsCleanText(value) && Encoding.UTF8.GetByteCount(value) <= 512;

    /// <summary>Metadata values: valid Unicode without control characters (length bounded by the total).</summary>
    public static bool IsMetadataValue(string? value) => value is not null && IsCleanText(value);

    static bool IsCleanText(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= value.Length || !char.IsLowSurrogate(value[i + 1]))
                {
                    return false;
                }

                i++;
                continue;
            }

            if (char.IsLowSurrogate(c) || c <= '\u001F' || c is >= '\u007F' and <= '\u009F')
            {
                return false;
            }
        }

        return true;
    }

    public static bool IsTraceParent(string? value) =>
        value is not null && TraceParent().IsMatch(value)
        && value.AsSpan(3, 32).ContainsAnyExcept('0') && value.AsSpan(36, 16).ContainsAnyExcept('0');

    public static bool IsTraceState(string? value) =>
        value is not null && value.Length <= 512 && value.All(c => c is >= ' ' and <= '~');
}
