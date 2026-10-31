using NodaTime;
using Runwick.Scheduling.Evaluation;
using static Runwick.Core.Tests.Scheduling.ScheduleFixtures;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// SCHED-02 … SCHED-08 (spec 12 §4, §7, §12) under the pinned TZDB 2026f artifact: explicit OR day matching, leap-day
/// search, gap skip/shift, fold earlier/later/both, a cursor inside a fold, gap coalescing, and transitions that are
/// not one hour, not at 02:00, or not whole minutes.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
public sealed class RecurrencePlannerTests
{
    const string Berlin = "Europe/Berlin";

    [Fact]
    [Trait("Case", "SCHED-02")]
    public void Restricted_day_fields_match_either_day_of_month_or_day_of_week()
    {
        var cron = Cron("0 9 1 * MON");

        Assert.Equal(["2026-07-01T09:00:00Z", "2026-07-06T09:00:00Z"], Plan(cron, "2026-06-30T23:59:59Z", 2));
        Assert.Equal(
            ["2026-07-01T09:00:00Z", "2026-07-06T09:00:00Z", "2026-07-13T09:00:00Z", "2026-07-20T09:00:00Z",
             "2026-07-27T09:00:00Z", "2026-08-01T09:00:00Z", "2026-08-03T09:00:00Z"],
            Plan(cron, "2026-06-30T23:59:59Z", 7));
    }

    [Fact]
    [Trait("Case", "SCHED-02")]
    public void A_monday_that_is_also_the_first_emits_once()
    {
        // 2026-06-01 is a Monday.
        Assert.Equal(["2026-06-01T09:00:00Z", "2026-06-08T09:00:00Z"], Plan(Cron("0 9 1 * MON"), "2026-05-31T23:59:59Z", 2));
    }

    [Fact]
    [Trait("Case", "SCHED-03")]
    public void February_29_skips_2100_and_finds_2104_after_2096()
    {
        var cron = Cron("0 0 29 2 *");

        Assert.Equal(["2104-02-29T00:00:00Z"], Plan(cron, "2096-02-29T00:00:00Z", 1));
        Assert.Equal(["2096-02-29T00:00:00Z", "2104-02-29T00:00:00Z"], Plan(cron, "2095-01-01T00:00:00Z", 2));
    }

    [Fact]
    [Trait("Case", "SCHED-04")]
    public void Berlin_spring_gap_skip_emits_nothing_that_day()
    {
        Assert.Equal(
            ["2026-03-30T00:30:00Z"],
            Plan(Cron("30 2 * * *", Berlin), "2026-03-28T01:30:00Z", 1));
    }

    [Fact]
    [Trait("Case", "SCHED-04")]
    public void Berlin_spring_gap_shift_emits_the_first_valid_instant_not_0330()
    {
        var o = Occurrences(Cron("30 2 * * *", Berlin, gap: GapPolicy.Shift), "2026-03-28T01:30:00Z", 2);

        Assert.Equal(["2026-03-29T01:00:00Z", "2026-03-30T00:30:00Z"], o.Select(x => Iso(x.UtcEpochMilliseconds)));
        Assert.Equal(OccurrenceKind.GapShifted, o[0].Kind);
        Assert.Equal(new LocalDateTime(2026, 3, 29, 3, 0), o[0].Local);
        Assert.Equal(Offset.FromHours(2), o[0].Offset);
        Assert.Equal(new LocalDateTime(2026, 3, 29, 2, 0), o[0].SkippedFrom);
        Assert.Equal(new LocalDateTime(2026, 3, 29, 3, 0), o[0].SkippedTo);
        Assert.False(o[0].Coalesced);
        Assert.Equal(OccurrenceKind.Exact, o[1].Kind);
    }

    [Theory]
    [Trait("Case", "SCHED-05")]
    [InlineData("earlier", new[] { "2026-10-25T00:30:00Z", "2026-10-26T01:30:00Z" })]
    [InlineData("later", new[] { "2026-10-25T01:30:00Z", "2026-10-26T01:30:00Z" })]
    [InlineData("both", new[] { "2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z", "2026-10-26T01:30:00Z" })]
    public void Berlin_autumn_fold_follows_the_policy_in_utc_order(string fold, string[] expected) =>
        Assert.Equal(expected, Plan(Cron("30 2 * * *", Berlin, fold: FoldOf(fold)), "2026-10-24T12:00:00Z", expected.Length));

    [Fact]
    [Trait("Case", "SCHED-05")]
    public void Fold_occurrences_carry_their_side_of_the_fold()
    {
        var o = Occurrences(Cron("30 2 * * *", Berlin, fold: FoldPolicy.Both), "2026-10-24T12:00:00Z", 3);

        Assert.Equal([OccurrenceKind.FoldEarlier, OccurrenceKind.FoldLater, OccurrenceKind.Exact], o.Select(x => x.Kind));
        Assert.Equal(o[0].Local, o[1].Local);
        Assert.Equal([Offset.FromHours(2), Offset.FromHours(1)], o.Take(2).Select(x => x.Offset));
    }

    [Fact]
    [Trait("Case", "SCHED-06")]
    public void Every_minute_over_the_repeated_hour_with_both_yields_120_ordered_instants()
    {
        var o = Occurrences(Cron("* 2 * * *", Berlin, fold: FoldPolicy.Both), "2026-10-24T23:59:59Z", 120);
        var start = Ms("2026-10-25T00:00:00Z");

        Assert.Equal(Enumerable.Range(0, 120).Select(i => start + i * 60_000L), o.Select(x => x.UtcEpochMilliseconds));
        Assert.All(o.Take(60), x => Assert.Equal(OccurrenceKind.FoldEarlier, x.Kind));
        Assert.All(o.Skip(60), x => Assert.Equal(OccurrenceKind.FoldLater, x.Kind));
        Assert.Equal(o[0].Local, o[60].Local);
        Assert.Equal(o[59].Local, o[119].Local);
    }

    [Fact]
    [Trait("Case", "SCHED-06")]
    public void A_cursor_inside_the_fold_finds_a_later_match_with_an_earlier_local_label()
    {
        // 00:59Z is 02:59 CEST; 01:00Z is 02:00 CET — the local clock moved backward.
        Assert.Equal(["2026-10-25T01:00:00Z"], Plan(Cron("* 2 * * *", Berlin, fold: FoldPolicy.Both), "2026-10-25T00:59:00Z", 1));
        Assert.Equal(["2026-10-25T01:00:00Z"], Plan(Cron("* 2 * * *", Berlin, fold: FoldPolicy.Later), "2026-10-24T23:59:59Z", 1));

        // Earlier: the later side is excluded wholesale; the next match is the following night.
        Assert.Equal(["2026-10-26T01:00:00Z"], Plan(Cron("* 2 * * *", Berlin), "2026-10-25T00:59:00Z", 1));
    }

    [Fact]
    [Trait("Case", "SCHED-06")]
    public void An_excluded_fold_side_is_jumped_not_scanned()
    {
        var schedule = Cron("* 2 * * *", Berlin);
        var budget = new EvaluationBudget(new FrozenTime(), PlannerLimits.Plan);

        var next = RecurrencePlanner.Next(schedule, Instant.FromUnixTimeMilliseconds(Ms("2026-10-25T00:59:00Z")), PlannerLimits.Plan, budget);

        Assert.Equal("2026-10-26T01:00:00Z", Iso(next.UtcEpochMilliseconds));
        Assert.InRange(budget.Steps, 1, 12);
    }

    [Fact]
    [Trait("Case", "SCHED-07")]
    public void A_gap_shift_colliding_with_a_normal_boundary_match_yields_one_occurrence()
    {
        var o = Occurrences(Cron("*/30 2-3 * * *", Berlin, gap: GapPolicy.Shift), "2026-03-28T23:00:00Z", 2);

        Assert.Equal(["2026-03-29T01:00:00Z", "2026-03-29T01:30:00Z"], o.Select(x => Iso(x.UtcEpochMilliseconds)));
        Assert.Equal(OccurrenceKind.GapShifted, o[0].Kind);
        Assert.True(o[0].Coalesced);
        Assert.Equal(new LocalDateTime(2026, 3, 29, 3, 30), o[1].Local);
    }

    [Fact]
    [Trait("Case", "SCHED-07")]
    public void Many_gap_matches_are_one_bounded_query()
    {
        var schedule = Cron("* 2 * * *", Berlin, gap: GapPolicy.Shift);
        var budget = new EvaluationBudget(new FrozenTime(), PlannerLimits.Plan);

        var first = RecurrencePlanner.Next(schedule, Instant.FromUnixTimeMilliseconds(Ms("2026-03-28T23:00:00Z")), PlannerLimits.Plan, budget);
        var second = RecurrencePlanner.Next(schedule, first.At, PlannerLimits.Plan, budget);

        Assert.Equal("2026-03-29T01:00:00Z", Iso(first.UtcEpochMilliseconds));
        Assert.False(first.Coalesced);
        Assert.Equal("2026-03-30T00:00:00Z", Iso(second.UtcEpochMilliseconds));
        Assert.InRange(budget.Steps, 1, 16);
    }

    [Theory]
    [Trait("Case", "SCHED-08")]
    [InlineData("earlier", new[] { "2026-04-04T14:45:00Z" })]
    [InlineData("later", new[] { "2026-04-04T15:15:00Z" })]
    [InlineData("both", new[] { "2026-04-04T14:45:00Z", "2026-04-04T15:15:00Z" })]
    public void Lord_Howe_half_hour_fold(string fold, string[] expected) =>
        Assert.Equal(expected, Plan(Cron("45 1 * * *", "Australia/Lord_Howe", fold: FoldOf(fold)), "2026-04-04T00:00:00Z", expected.Length));

    [Theory]
    [Trait("Case", "SCHED-08")]
    [InlineData("skip", new[] { "2026-10-04T15:15:00Z" })]
    [InlineData("shift", new[] { "2026-10-03T15:30:00Z", "2026-10-04T15:15:00Z" })]
    public void Lord_Howe_half_hour_gap(string gap, string[] expected) =>
        Assert.Equal(expected, Plan(Cron("15 2 * * *", "Australia/Lord_Howe", gap: GapOf(gap)), "2026-10-03T00:00:00Z", expected.Length));

    [Theory]
    [Trait("Case", "SCHED-08")]
    [InlineData("skip", new[] { "2011-12-29T22:00:00Z", "2011-12-30T22:00:00Z" })]
    [InlineData("shift", new[] { "2011-12-29T22:00:00Z", "2011-12-30T10:00:00Z", "2011-12-30T22:00:00Z" })]
    public void Apia_skipped_2011_12_30_entirely(string gap, string[] expected) =>
        Assert.Equal(expected, Plan(Cron("0 12 * * *", "Pacific/Apia", gap: GapOf(gap)), "2011-12-29T00:00:00Z", expected.Length));

    [Fact]
    [Trait("Case", "SCHED-08")]
    public void Apia_shift_reports_a_skipped_whole_day()
    {
        var o = Occurrences(Cron("0 12 * * *", "Pacific/Apia", gap: GapPolicy.Shift), "2011-12-29T23:00:00Z", 1);

        Assert.Equal(new LocalDateTime(2011, 12, 30, 0, 0), o[0].SkippedFrom);
        Assert.Equal(new LocalDateTime(2011, 12, 31, 0, 0), o[0].SkippedTo);
        Assert.Equal(Offset.FromHours(14), o[0].Offset);
    }

    [Theory]
    [Trait("Case", "SCHED-08")]
    [InlineData("skip", new[] { "2018-11-03T03:00:00Z", "2018-11-05T02:00:00Z" })]
    [InlineData("shift", new[] { "2018-11-03T03:00:00Z", "2018-11-04T03:00:00Z", "2018-11-05T02:00:00Z" })]
    public void Sao_Paulo_midnight_gap(string gap, string[] expected) =>
        Assert.Equal(expected, Plan(Cron("0 0 * * *", "America/Sao_Paulo", gap: GapOf(gap)), "2018-11-03T00:00:00Z", expected.Length));

    [Fact]
    [Trait("Case", "SCHED-08")]
    public void Sao_Paulo_midnight_fold_keeps_half_open_boundaries()
    {
        // 2019-02-17T02:00Z: 00:00 (−02) became 23:00 on the 16th (−03).
        Assert.Equal(
            ["2019-02-17T01:30:00Z", "2019-02-17T02:30:00Z"],
            Plan(Cron("30 23 * * *", "America/Sao_Paulo", fold: FoldPolicy.Both), "2019-02-16T12:00:00Z", 2));
        Assert.Equal(["2019-02-17T03:00:00Z"], Plan(Cron("0 0 * * *", "America/Sao_Paulo"), "2019-02-16T12:00:00Z", 1));
    }

    [Fact]
    [Trait("Case", "SCHED-08")]
    public void Second_valued_offsets_are_not_rounded()
    {
        // Africa/Monrovia kept −00:44:30 until 1972-01-07, whose local midnight to 00:44:30 was skipped.
        var o = Occurrences(Cron("0 12 * * *", "Africa/Monrovia"), "1971-06-01T00:00:00Z", 1);

        Assert.Equal(Ms("1971-06-01T12:44:30Z"), o[0].UtcEpochMilliseconds);
        Assert.Equal(Offset.FromSeconds(-2_670), o[0].Offset);
        Assert.Equal(["1972-01-08T00:30:00Z"], Plan(Cron("30 0 * * *", "Africa/Monrovia"), "1972-01-06T12:00:00Z", 1));
        Assert.Equal(
            ["1972-01-07T00:44:30Z", "1972-01-08T00:30:00Z"],
            Plan(Cron("30 0 * * *", "Africa/Monrovia", gap: GapPolicy.Shift), "1972-01-06T12:00:00Z", 2));
    }

    [Fact]
    [Trait("Case", "SCHED-08")]
    public void Output_is_strictly_increasing_across_transitions()
    {
        foreach (var (fold, gap, after) in
                 from f in new[] { FoldPolicy.Earlier, FoldPolicy.Later, FoldPolicy.Both }
                 from g in new[] { GapPolicy.Skip, GapPolicy.Shift }
                 from a in new[] { "2026-03-27T00:00:00Z", "2026-10-23T00:00:00Z" }
                 select (f, g, a))
        {
            var o = Occurrences(Cron("*/15 * * * *", Berlin, gap, fold), after, 400);
            var ms = o.Select(x => x.UtcEpochMilliseconds).ToArray();
            Assert.All(Enumerable.Range(1, ms.Length - 1), i => Assert.True(ms[i] > ms[i - 1]));
            Assert.All(o, x => Assert.Equal(0L, x.UtcEpochMilliseconds % 1_000));
        }
    }

    [Fact]
    [Trait("Case", "SCHED-08")]
    public void Six_field_matches_have_second_resolution()
    {
        Assert.Equal(
            ["2026-03-29T00:59:59Z", "2026-03-29T01:00:00Z", "2026-03-29T01:00:01Z"],
            Plan(Cron("* * * * * *", Berlin, gap: GapPolicy.Shift, dialect: CronDialect.Cron6), "2026-03-29T00:59:58Z", 3));
    }
}
