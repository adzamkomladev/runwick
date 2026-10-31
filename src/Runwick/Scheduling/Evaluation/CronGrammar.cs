using System.Globalization;
using System.Numerics;

namespace Runwick.Scheduling.Evaluation;

/// <summary>The versioned dialect IDs of spec 12 §2. Five-field is the default; there is no field-count guessing.</summary>
internal static class CronDialect
{
    public const string Cron5 = "runwick.cron5.v1";
    public const string Cron6 = "runwick.cron6.v1";

    /// <summary>One-time schedules (spec 12 §5.3): one UTC instant, no cron fields and no zone artifact.</summary>
    public const string Instant = "runwick.instant.v1";

    public const string Default = Cron5;
}

/// <summary>
/// A cron expression reduced to one bitset per field (spec 12 §3). Bit <c>v</c> set means value <c>v</c> matches.
/// Day of week uses 0–6 with Sunday = 0. <see cref="RawText"/> is kept for display only and is never hashed.
/// </summary>
internal sealed record NormalizedCron(
    string Dialect,
    string RawText,
    ulong Seconds,
    ulong Minutes,
    ulong Hours,
    ulong DaysOfMonth,
    ulong Months,
    ulong DaysOfWeek)
{
    public const ulong AllSeconds = (1UL << 60) - 1;
    public const ulong AllMinutes = (1UL << 60) - 1;
    public const ulong AllHours = (1UL << 24) - 1;
    public const ulong AllDaysOfMonth = ((1UL << 32) - 1) & ~1UL;
    public const ulong AllMonths = ((1UL << 13) - 1) & ~1UL;
    public const ulong AllDaysOfWeek = (1UL << 7) - 1;

    // Days per month 1..12 with February at 29: a day-of-month branch is feasible if some selected
    // month has some selected day (spec 12 §3.3).
    static readonly int[] MaxDays = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    public bool DayOfMonthRestricted => DaysOfMonth != AllDaysOfMonth;

    public bool DayOfWeekRestricted => DaysOfWeek != AllDaysOfWeek;

    public bool DayOfMonthFeasible
    {
        get
        {
            for (var m = 1; m <= 12; m++)
            {
                var monthDays = ((1UL << (MaxDays[m] + 1)) - 1) & ~1UL;
                if ((Months & (1UL << m)) != 0 && (DaysOfMonth & monthDays) != 0)
                {
                    return true;
                }
            }

            return false;
        }
    }

    public static IEnumerable<int> Values(ulong set)
    {
        while (set != 0)
        {
            var v = BitOperations.TrailingZeroCount(set);
            yield return v;
            set &= set - 1;
        }
    }
}

/// <summary>
/// Parser and normalizer for <c>runwick.cron5.v1</c> / <c>runwick.cron6.v1</c> (spec 12 §2–§3). Everything outside the
/// grammar is rejected here, before Cronos sees the text; Cronos only ever receives the normalized rendering.
/// </summary>
internal static class CronGrammar
{
    public const int MaxLength = 256;
    public const int MaxListTerms = 64;

    sealed record Field(string Name, int Min, int Max, int StarMax, int Width, string[]? Names, bool SevenIsSunday = false);

    static readonly Field Second = new("second", 0, 59, 59, 60, null);
    static readonly Field Minute = new("minute", 0, 59, 59, 60, null);
    static readonly Field Hour = new("hour", 0, 23, 23, 24, null);
    static readonly Field DayOfMonth = new("day-of-month", 1, 31, 31, 31, null);

    static readonly Field Month = new(
        "month", 1, 12, 12, 12, ["", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]);

    // Ranges expand in the input's 0–7 domain before 7 maps to Sunday (0); '*' means 0–6.
    static readonly Field DayOfWeek = new(
        "day-of-week", 0, 7, 6, 7, ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"], SevenIsSunday: true);

    public static NormalizedCron Parse(string? text, string dialect = CronDialect.Default)
    {
        var expected = dialect switch
        {
            CronDialect.Cron5 => 5,
            CronDialect.Cron6 => 6,
            _ => throw EvaluationErrors.Grammar("Unknown cron dialect."),
        };

        if (text is null)
        {
            throw EvaluationErrors.Grammar("A cron expression is required.");
        }

        if (text.Length > MaxLength)
        {
            throw EvaluationErrors.Grammar($"A cron expression is at most {MaxLength} characters.");
        }

        foreach (var c in text)
        {
            if (c is not (' ' or '\t') && (c < '!' || c > '~'))
            {
                throw EvaluationErrors.Grammar(
                    string.Create(CultureInfo.InvariantCulture, $"Character U+{(int)c:X4} is not allowed; fields are separated by ASCII spaces or tabs."));
            }
        }

        var fields = text.Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
        if (fields.Length != expected)
        {
            throw EvaluationErrors.Grammar(
                string.Create(CultureInfo.InvariantCulture, $"Dialect {dialect} takes exactly {expected} fields; found {fields.Length}."));
        }

        var terms = 0;
        foreach (var f in fields)
        {
            terms += f.Count(c => c == ',') + 1;
        }

        if (terms > MaxListTerms)
        {
            throw EvaluationErrors.Grammar($"A cron expression has at most {MaxListTerms} list terms.");
        }

        var i = expected - 5;
        var cron = new NormalizedCron(
            dialect,
            text,
            Seconds: expected == 6 ? ParseField(fields[0], Second) : 1UL,
            Minutes: ParseField(fields[i], Minute),
            Hours: ParseField(fields[i + 1], Hour),
            DaysOfMonth: ParseField(fields[i + 2], DayOfMonth),
            Months: ParseField(fields[i + 3], Month),
            DaysOfWeek: ParseField(fields[i + 4], DayOfWeek));

        // An impossible day-of-month branch is fatal only when no day-of-week alternative exists (§3.3).
        if (cron.DayOfMonthRestricted && !cron.DayOfWeekRestricted && !cron.DayOfMonthFeasible)
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.InvalidCalendar,
                "No selected month has any selected day of month, and there is no day-of-week alternative.");
        }

        return cron;
    }

    static ulong ParseField(string text, Field field)
    {
        var set = 0UL;
        foreach (var term in text.Split(','))
        {
            if (term.Length == 0)
            {
                throw EvaluationErrors.Grammar($"The {field.Name} field has an empty list member.");
            }

            set |= ParseTerm(term, field);
        }

        return set;
    }

    static ulong ParseTerm(string term, Field field)
    {
        var slash = term.Split('/');
        if (slash.Length > 2)
        {
            throw Token(field, term);
        }

        var range = slash[0];
        int low, high;
        if (range == "*")
        {
            (low, high) = (field.Min, field.StarMax);
        }
        else if (range.Contains('-', StringComparison.Ordinal))
        {
            var ends = range.Split('-');
            if (ends.Length != 2 || ends[0].Length == 0 || ends[1].Length == 0)
            {
                throw Token(field, term);
            }

            (low, high) = (Value(ends[0], field), Value(ends[1], field));
            if (low > high)
            {
                throw EvaluationErrors.Grammar($"The {field.Name} range '{term}' is reversed; wrapping ranges are not supported.");
            }
        }
        else if (slash.Length == 2)
        {
            throw EvaluationErrors.Grammar($"The {field.Name} step '{term}' needs '*' or a range before '/'.");
        }
        else
        {
            low = high = Value(range, field);
        }

        var step = 1;
        if (slash.Length == 2)
        {
            if (!TryNumber(slash[1], out step) || step < 1 || step > field.Width)
            {
                throw EvaluationErrors.Grammar(
                    string.Create(CultureInfo.InvariantCulture, $"The {field.Name} step in '{term}' must be 1–{field.Width}."));
            }
        }

        var set = 0UL;
        for (var v = low; v <= high; v += step)
        {
            set |= 1UL << (field.SevenIsSunday && v == 7 ? 0 : v);
        }

        return set;
    }

    static int Value(string text, Field field)
    {
        if (TryNumber(text, out var n))
        {
            if (n < field.Min || n > field.Max)
            {
                throw EvaluationErrors.Grammar(
                    string.Create(CultureInfo.InvariantCulture, $"The {field.Name} value {n} is outside {field.Min}–{field.Max}."));
            }

            return n;
        }

        if (field.Names is { } names && text.Length == 3)
        {
            var index = Array.FindIndex(names, s => string.Equals(s, text, StringComparison.OrdinalIgnoreCase));
            if (index >= 0)
            {
                return index;
            }
        }

        throw Token(field, text);
    }

    // One or two ASCII digits: every domain value and step fits, and nothing longer can overflow.
    static bool TryNumber(string text, out int value)
    {
        value = 0;
        if (text.Length is 0 or > 2)
        {
            return false;
        }

        foreach (var c in text)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }

            value = value * 10 + (c - '0');
        }

        return true;
    }

    static ScheduleEvaluationException Token(Field field, string token) =>
        EvaluationErrors.Grammar($"'{token}' is not supported in the {field.Name} field.");
}
