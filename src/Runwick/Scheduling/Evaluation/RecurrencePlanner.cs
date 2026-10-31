using NodaTime;
using NodaTime.TimeZones;

namespace Runwick.Scheduling.Evaluation;

/// <summary>Planner bounds (spec 12 §8; 06 schedule evaluator rows).</summary>
internal sealed record PlannerLimits(int HorizonYears, int HorizonDays, int MaxSteps, TimeSpan Budget)
{
    public const int PreviewCount = 10;

    /// <summary>Materialization plans: 8 years + 2 days per lookup, 4,096 steps, 250 ms cooperative budget.</summary>
    public static readonly PlannerLimits Plan = new(8, 2, 4_096, TimeSpan.FromMilliseconds(250));

    /// <summary>Registration, edit and preview: the same search bounds with a 1 s budget.</summary>
    public static readonly PlannerLimits Registration = new(8, 2, 4_096, TimeSpan.FromSeconds(1));
}

/// <summary>
/// The cooperative step and time budget of one plan (spec 12 §8.2). Checked between library calls only: a synchronous
/// Cronos or NodaTime call is never preempted.
/// </summary>
internal sealed class EvaluationBudget
{
    readonly TimeProvider _time;
    readonly long _started;
    readonly int _maxSteps;
    readonly TimeSpan _budget;

    public EvaluationBudget(TimeProvider time, PlannerLimits limits)
    {
        _time = time;
        _started = time.GetTimestamp();
        _maxSteps = limits.MaxSteps;
        _budget = limits.Budget;
    }

    public int Steps { get; private set; }

    /// <summary>Accounts one adapter candidate or transition step, then checks the deadline.</summary>
    public void Step()
    {
        if (++Steps > _maxSteps)
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.StepLimitExceeded, $"The plan needed more than {_maxSteps} evaluator steps.");
        }

        CheckDeadline();
    }

    public void CheckDeadline()
    {
        if (_time.GetElapsedTime(_started) >= _budget)
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.BudgetElapsed, $"The {_budget.TotalMilliseconds} ms evaluation budget elapsed.");
        }
    }
}

/// <summary>How an occurrence relates to the local clock (spec 12 §7).</summary>
internal enum OccurrenceKind
{
    /// <summary>The matched local time maps to exactly one instant.</summary>
    Exact,

    /// <summary>The matched local time is repeated; this is its earlier instant.</summary>
    FoldEarlier,

    /// <summary>The matched local time is repeated; this is its later instant.</summary>
    FoldLater,

    /// <summary>One or more matches fell in a skipped local range; this is the transition's first valid instant.</summary>
    GapShifted,
}

/// <summary>
/// One planned occurrence. <see cref="Local"/> is the wall-clock time at <see cref="At"/>; for
/// <see cref="OccurrenceKind.GapShifted"/> the skipped local range is [<see cref="SkippedFrom"/>, <see cref="SkippedTo"/>)
/// and <see cref="Coalesced"/> says a normal match at the same instant was merged into it.
/// </summary>
internal sealed record Occurrence(
    Instant At,
    LocalDateTime Local,
    Offset Offset,
    OccurrenceKind Kind,
    LocalDateTime? SkippedFrom = null,
    LocalDateTime? SkippedTo = null,
    bool Coalesced = false)
{
    public long UtcEpochMilliseconds => At.ToUnixTimeMilliseconds();
}

/// <summary>
/// The bounded recurrence planner (spec 12 §7–§8): enumerates constant-offset zone intervals in UTC order, runs the
/// nominal-calendar adapter inside each, applies the gap and fold policies, and emits strictly increasing instants
/// strictly after the cursor. Pure apart from the clock used for the cooperative budget.
/// </summary>
internal sealed class RecurrencePlanner(TimeProvider time)
{
    /// <summary>The instants the planner accepts as cursors and searches; local coordinates stay inside DateTime.</summary>
    public static readonly Instant SupportedStart = Instant.FromUtc(1, 1, 2, 0, 0);

    public static readonly Instant SupportedEnd = Instant.FromUtc(9999, 12, 30, 0, 0);

    /// <summary>
    /// The first <paramref name="count"/> occurrences strictly after <paramref name="afterUtcEpochMilliseconds"/>, each
    /// lookup searching at most the limits' horizon, all lookups sharing one step and time budget.
    /// </summary>
    public IReadOnlyList<Occurrence> Plan(CronSchedule schedule, long afterUtcEpochMilliseconds, int count, PlannerLimits limits)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        var budget = new EvaluationBudget(time, limits);
        var cursor = FromMilliseconds(afterUtcEpochMilliseconds);
        var result = new List<Occurrence>(Math.Min(count, 64));
        while (result.Count < count)
        {
            var next = Next(schedule, cursor, limits, budget);
            result.Add(next);
            cursor = next.At;
        }

        budget.CheckDeadline();
        return result;
    }

    /// <summary>The definition preview (spec 12 §9): the next 10 occurrences under the registration budget.</summary>
    public IReadOnlyList<PreviewItem> Preview(CronSchedule schedule, long afterUtcEpochMilliseconds) =>
        [.. Plan(schedule, afterUtcEpochMilliseconds, PlannerLimits.PreviewCount, PlannerLimits.Registration).Select(PreviewItem.From)];

    internal static Occurrence Next(CronSchedule schedule, Instant cursor, PlannerLimits limits, EvaluationBudget budget)
    {
        if (cursor < SupportedStart || cursor >= SupportedEnd)
        {
            throw new ScheduleEvaluationException(EvaluationErrorCode.RangeOverflow, "The cursor is outside the supported date range.");
        }

        var (end, clipped) = Horizon(cursor, limits);
        var zone = schedule.Zone.Zone;
        ZoneInterval? previous = null;
        foreach (var interval in zone.GetZoneIntervals(new Interval(cursor, end)))
        {
            budget.Step();
            var start = interval.HasStart && interval.Start > cursor ? interval.Start : cursor;
            var stop = interval.HasEnd && interval.End < end ? interval.End : end;
            var gapShift = schedule.Gap == GapPolicy.Shift && start > cursor && start < stop && previous is not null
                ? ShiftedGap(schedule, previous, interval, budget)
                : null;
            if (gapShift is not null)
            {
                return gapShift;
            }

            if (start < stop && InInterval(schedule, interval, cursor, start, stop, budget) is { } occurrence)
            {
                return occurrence;
            }

            previous = interval;
        }

        throw clipped
            ? new ScheduleEvaluationException(EvaluationErrorCode.RangeOverflow, "No occurrence before the end of the supported date range.")
            : new ScheduleEvaluationException(
                EvaluationErrorCode.NoMatchWithinHorizon,
                $"No occurrence within {limits.HorizonYears} years and {limits.HorizonDays} days of the cursor.");
    }

    // Gap shift (§7.2): if any match lies in the skipped local range, emit the transition's first valid instant once,
    // merged with a normal match at that instant. Two adapter queries, however many local times were skipped.
    static Occurrence? ShiftedGap(CronSchedule schedule, ZoneInterval previous, ZoneInterval interval, EvaluationBudget budget)
    {
        if (interval.WallOffset <= previous.WallOffset)
        {
            return null;
        }

        var transition = interval.Start;
        var skippedFrom = transition.WithOffset(previous.WallOffset).LocalDateTime;
        var skippedTo = transition.WithOffset(interval.WallOffset).LocalDateTime;
        if (schedule.Adapter.Next(skippedFrom, inclusive: true, budget) is not { } match || match >= skippedTo)
        {
            return null;
        }

        var coalesced = schedule.Adapter.Next(skippedTo, inclusive: true, budget) == skippedTo;
        return new Occurrence(transition, skippedTo, interval.WallOffset, OccurrenceKind.GapShifted, skippedFrom, skippedTo, coalesced);
    }

    // Normal matches inside one constant-offset interval, clipped to [start, stop) and strictly after the cursor (§7.1, §7.3).
    static Occurrence? InInterval(
        CronSchedule schedule, ZoneInterval interval, Instant cursor, Instant start, Instant stop, EvaluationBudget budget)
    {
        var offset = interval.WallOffset;
        var zone = schedule.Zone.Zone;
        var local = start.WithOffset(offset).LocalDateTime;
        var inclusive = start > cursor;
        while (schedule.Adapter.Next(local, inclusive, budget) is { } match)
        {
            var at = match.WithOffset(offset).ToInstant();
            if (at >= stop)
            {
                return null;
            }

            if (at <= cursor)
            {
                (local, inclusive) = (match, false);
                continue;
            }

            budget.Step();
            var mapping = zone.MapLocal(match);
            if (mapping.Count < 2)
            {
                return new Occurrence(at, match, offset, OccurrenceKind.Exact);
            }

            var earlier = mapping.First().ToInstant() == at;
            if (schedule.Fold == FoldPolicy.Both || (schedule.Fold == FoldPolicy.Earlier) == earlier)
            {
                return new Occurrence(at, match, offset, earlier ? OccurrenceKind.FoldEarlier : OccurrenceKind.FoldLater);
            }

            if (earlier)
            {
                // Policy later: the rest of this interval is the earlier side of the fold.
                return null;
            }

            // Policy earlier, in the later interval: jump past the repeated local range instead of scanning it.
            (local, inclusive) = (mapping.EarlyInterval.End.WithOffset(mapping.EarlyInterval.WallOffset).LocalDateTime, true);
        }

        return null;
    }

    static (Instant End, bool Clipped) Horizon(Instant cursor, PlannerLimits limits)
    {
        Instant end;
        try
        {
            end = cursor.InUtc().LocalDateTime.PlusYears(limits.HorizonYears).PlusDays(limits.HorizonDays).InUtc().ToInstant();
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or OverflowException)
        {
            return (SupportedEnd, true);
        }

        return end > SupportedEnd ? (SupportedEnd, true) : (end, false);
    }

    static Instant FromMilliseconds(long ms)
    {
        try
        {
            return Instant.FromUnixTimeMilliseconds(ms);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new ScheduleEvaluationException(EvaluationErrorCode.RangeOverflow, "The cursor is outside the supported date range.", ex);
        }
    }
}
