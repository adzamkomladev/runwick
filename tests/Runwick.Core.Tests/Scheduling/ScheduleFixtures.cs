using System.Globalization;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>Shared helpers for the SP-015 evaluator tests. Every case runs under the pinned artifact (spec 12 §6).</summary>
internal static class ScheduleFixtures
{
    public static CronSchedule Cron(
        string text,
        string? zone = null,
        GapPolicy gap = GapPolicy.Skip,
        FoldPolicy fold = FoldPolicy.Earlier,
        string dialect = CronDialect.Cron5) =>
        ScheduleDefinition.Compile(text, dialect, zone, TzdbArtifact.Pinned, gap, fold);

    public static long Ms(string isoUtc) =>
        DateTimeOffset.Parse(isoUtc, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUnixTimeMilliseconds();

    public static string Iso(long ms) =>
        DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    /// <summary>A planner whose budget clock never advances, so wall-clock load cannot turn a vector into BudgetElapsed.</summary>
    public static RecurrencePlanner Planner() => new(new FrozenTime());

    public static string[] Plan(CronSchedule schedule, string afterIsoUtc, int count, PlannerLimits? limits = null) =>
        [.. Planner().Plan(schedule, Ms(afterIsoUtc), count, limits ?? PlannerLimits.Plan).Select(o => Iso(o.UtcEpochMilliseconds))];

    public static IReadOnlyList<Occurrence> Occurrences(CronSchedule schedule, string afterIsoUtc, int count) =>
        Planner().Plan(schedule, Ms(afterIsoUtc), count, PlannerLimits.Plan);

    public static ScheduleEvaluationException Fails(Action action) => Assert.Throws<ScheduleEvaluationException>(action);

    // Theory data stays public-typed: the policy enums are internal.
    public static FoldPolicy FoldOf(string name) => Enum.Parse<FoldPolicy>(name, ignoreCase: true);

    public static GapPolicy GapOf(string name) => Enum.Parse<GapPolicy>(name, ignoreCase: true);

    public static string Values(ulong set) => string.Join(",", NormalizedCron.Values(set));

    internal sealed class FrozenTime : TimeProvider
    {
        public override long GetTimestamp() => 0;
    }

    /// <summary>Each timestamp read advances the clock by <paramref name="stepMilliseconds"/>.</summary>
    internal sealed class SteppingTime(long stepMilliseconds) : TimeProvider
    {
        long _now;

        public override long TimestampFrequency => 1_000;

        public override long GetTimestamp()
        {
            var now = _now;
            _now += stepMilliseconds;
            return now;
        }
    }
}
