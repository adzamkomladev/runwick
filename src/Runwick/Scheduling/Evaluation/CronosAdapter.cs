using System.Globalization;
using Cronos;
using NodaTime;

namespace Runwick.Scheduling.Evaluation;

/// <summary>
/// The private nominal-calendar adapter of spec 12 §4. Cronos 0.13.0 ANDs restricted day fields and maps zones its own
/// way, so it only ever sees (a) the normalized rendering of the fields, split into at most two streams to obtain OR,
/// and (b) its UTC-only path. A <see cref="DateTimeKind.Utc"/> value here encodes nominal local wall-clock coordinates;
/// it is never an instant.
/// </summary>
internal sealed class CronosAdapter
{
    // Cronos cannot search past DateTime.MaxValue; near it an out-of-range failure means "no candidate".
    const int LastSearchableYear = 9990;

    readonly CronExpression[] _streams;

    CronosAdapter(string[] texts, CronFormat format)
    {
        StreamTexts = texts;
        _streams = [.. texts.Select(t => CronExpression.Parse(t, format))];
    }

    /// <summary>The Cronos expressions evaluated, one per OR branch (diagnostics and tests).</summary>
    public IReadOnlyList<string> StreamTexts { get; }

    public static CronosAdapter Compile(NormalizedCron cron)
    {
        var dom = Render(cron.DaysOfMonth, NormalizedCron.AllDaysOfMonth);
        var dow = Render(cron.DaysOfWeek, NormalizedCron.AllDaysOfWeek);

        // With at most one day field restricted, Cronos's AND with '*' is exactly "the restricted field controls".
        // With both restricted, OR is the union of a day-of-month stream and a day-of-week stream; a day-of-month
        // branch with no possible date is dropped (the grammar accepted it only because day-of-week remains).
        List<(string Dom, string Dow)> branches = [];
        if (cron.DayOfMonthRestricted && cron.DayOfWeekRestricted)
        {
            if (cron.DayOfMonthFeasible)
            {
                branches.Add((dom, "*"));
            }

            branches.Add(("*", dow));
        }
        else
        {
            branches.Add((dom, dow));
        }

        var six = cron.Dialect == CronDialect.Cron6;
        var prefix = six ? Render(cron.Seconds, NormalizedCron.AllSeconds) + " " : "";
        var time = $"{Render(cron.Minutes, NormalizedCron.AllMinutes)} {Render(cron.Hours, NormalizedCron.AllHours)}";
        var month = Render(cron.Months, NormalizedCron.AllMonths);
        try
        {
            return new CronosAdapter(
                [.. branches.Select(b => $"{prefix}{time} {b.Dom} {month} {b.Dow}")],
                six ? CronFormat.IncludeSeconds : CronFormat.Standard);
        }
        catch (CronFormatException ex)
        {
            // The rendering is generated from validated sets; Cronos refusing it is a provider fault, not user input.
            throw new ScheduleEvaluationException(EvaluationErrorCode.ProviderFailure, "Cronos rejected a normalized expression.", ex);
        }
    }

    /// <summary>
    /// The earliest nominal local time at or after (<paramref name="inclusive"/>) / strictly after <paramref name="from"/>
    /// matched by any stream, or null if none exists before Cronos's range ends. One budget step per stream.
    /// </summary>
    public LocalDateTime? Next(LocalDateTime from, bool inclusive, EvaluationBudget budget)
    {
        var encoded = DateTime.SpecifyKind(from.ToDateTimeUnspecified(), DateTimeKind.Utc);
        DateTime? best = null;
        foreach (var stream in _streams)
        {
            budget.Step();
            DateTime? next;
            try
            {
                next = stream.GetNextOccurrence(encoded, inclusive);
            }
            catch (ArgumentOutOfRangeException) when (from.Year >= LastSearchableYear)
            {
                next = null;
            }
            catch (Exception ex)
            {
                throw new ScheduleEvaluationException(EvaluationErrorCode.ProviderFailure, "Cronos failed to evaluate.", ex);
            }

            if (next is { } n && (best is null || n < best))
            {
                best = n;
            }
        }

        return best is { } b ? LocalDateTime.FromDateTime(b) : null;
    }

    static string Render(ulong set, ulong all) =>
        set == all ? "*" : string.Join(',', NormalizedCron.Values(set).Select(v => v.ToString(CultureInfo.InvariantCulture)));
}
