using Runwick.Scheduling.Evaluation;
using static Runwick.Core.Tests.Scheduling.ScheduleFixtures;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// SCHED-01 and the grammar half of SCHED-03 (spec 12 §2–§4, §12): field formats, aliases, Sunday 7, steps and
/// normalized sets; unsupported syntax rejected by the Runwick grammar before Cronos; one fingerprint per meaning;
/// impossible calendars rejected unless an OR branch survives.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
public sealed class CronGrammarTests
{
    [Fact]
    [Trait("Case", "SCHED-01")]
    public void Five_field_values_ranges_steps_and_names_normalize_to_sets()
    {
        var cron = CronGrammar.Parse("10-50/20 */6 1,15 JAN-mar 5-7");

        Assert.Equal(CronDialect.Cron5, cron.Dialect);
        Assert.Equal("0", Values(cron.Seconds));
        Assert.Equal("10,30,50", Values(cron.Minutes));
        Assert.Equal("0,6,12,18", Values(cron.Hours));
        Assert.Equal("1,15", Values(cron.DaysOfMonth));
        Assert.Equal("1,2,3", Values(cron.Months));
        Assert.Equal("0,5,6", Values(cron.DaysOfWeek));
        Assert.Equal("10-50/20 */6 1,15 JAN-mar 5-7", cron.RawText);
    }

    [Fact]
    [Trait("Case", "SCHED-01")]
    public void Six_field_dialect_is_explicit_and_adds_seconds()
    {
        var cron = CronGrammar.Parse("*/20 0 9 * * *", CronDialect.Cron6);

        Assert.Equal("0,20,40", Values(cron.Seconds));
        Assert.Equal("0", Values(cron.Minutes));
        Assert.Equal("9", Values(cron.Hours));
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse("*/20 0 9 * * *")).Code);
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse("0 9 * * *", CronDialect.Cron6)).Code);
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse("0 9 * * *", "runwick.cron7.v1")).Code);
    }

    [Theory]
    [Trait("Case", "SCHED-01")]
    [InlineData("0 0 * * 7", "0")]
    [InlineData("0 0 * * SUN", "0")]
    [InlineData("0 0 * * sun,7,0", "0")]
    [InlineData("0 0 * * 6-7", "0,6")]
    [InlineData("0 0 * * 1-7/2", "0,1,3,5")]
    [InlineData("0 0 * * */2", "0,2,4,6")]
    [InlineData("0 0 * * */7", "0")]
    [InlineData("0 0 * * MON-FRI", "1,2,3,4,5")]
    public void Day_of_week_expands_in_0_to_7_before_seven_becomes_sunday(string text, string expected) =>
        Assert.Equal(expected, Values(CronGrammar.Parse(text).DaysOfWeek));

    [Theory]
    [Trait("Case", "SCHED-01")]
    [InlineData("*/1 * * * *")]
    [InlineData("0-59 0-23 1-31 1-12 0-6")]
    [InlineData("* * * JAN-DEC 0-7")]
    [InlineData("* * */1 * SUN-SAT")]
    public void A_field_selecting_its_whole_domain_is_unrestricted(string text)
    {
        var cron = CronGrammar.Parse(text);

        Assert.Equal(NormalizedCron.AllMinutes, cron.Minutes);
        Assert.False(cron.DayOfMonthRestricted);
        Assert.False(cron.DayOfWeekRestricted);
        Assert.Equal(["* * * * *"], CronosAdapter.Compile(cron).StreamTexts);
    }

    [Theory]
    [Trait("Case", "SCHED-01")]
    [InlineData("0 9 * * MON-FRI", "0 09 * * 1-5")]
    [InlineData("0 9 * * MON-FRI", "0\t9  *  *  1,2,3,4,5")]
    [InlineData("0 9 * * MON-FRI", "  0 9 * * mon-fri  ")]
    [InlineData("0 9 * * MON-FRI", "0 9 * * 5,4,3,2,1,1")]
    [InlineData("*/15 * * * *", "0,15,30,45 * * * *")]
    [InlineData("*/15 * * * *", "0-59/15 * * * *")]
    [InlineData("0 0 * * 0", "0 0 * * 7")]
    [InlineData("0 0 * * *", "0 0 * * 0-7")]
    [InlineData("0 0 1 JAN *", "0 0 1 1 *")]
    public void Equivalent_text_has_one_fingerprint(string a, string b) =>
        Assert.Equal(ScheduleDefinition.Fingerprint(Cron(a)), ScheduleDefinition.Fingerprint(Cron(b)));

    [Theory]
    [Trait("Case", "SCHED-01")]
    [InlineData("0 0 L * *")]
    [InlineData("0 0 ? * *")]
    [InlineData("0 0 * * ?")]
    [InlineData("0 0 15W * *")]
    [InlineData("0 0 LW * *")]
    [InlineData("0 0 * * 1#2")]
    [InlineData("0 0 * * 5L")]
    [InlineData("H 0 * * *")]
    [InlineData("H(0-29) 0 * * *")]
    [InlineData("R 0 * * *")]
    [InlineData("@daily")]
    [InlineData("@every 5m")]
    [InlineData("0 0 * * * 2026")]
    [InlineData("TZ=UTC 0 0 * *")]
    [InlineData("CRON_TZ=Europe/Berlin 0 9 * *")]
    [InlineData("5/15 * * * *")]
    [InlineData("50-10 * * * *")]
    [InlineData("0 0 * * FRI-MON")]
    [InlineData("0 0 * NOV-FEB *")]
    [InlineData("1,,2 * * * *")]
    [InlineData(",1 * * * *")]
    [InlineData("1, * * * *")]
    [InlineData("60 * * * *")]
    [InlineData("0 24 * * *")]
    [InlineData("0 0 0 * *")]
    [InlineData("0 0 32 * *")]
    [InlineData("0 0 * 0 *")]
    [InlineData("0 0 * 13 *")]
    [InlineData("0 0 * * 8")]
    [InlineData("*/0 * * * *")]
    [InlineData("*/61 * * * *")]
    [InlineData("0 */25 * * *")]
    [InlineData("0 0 1-31/32 * *")]
    [InlineData("0 0 * * */8")]
    [InlineData("-1 * * * *")]
    [InlineData("1- * * * *")]
    [InlineData("1-2-3 * * * *")]
    [InlineData("1/2/3 * * * *")]
    [InlineData("007 * * * *")]
    [InlineData("+5 * * * *")]
    [InlineData("MON 0 * * *")]
    [InlineData("0 JAN * * *")]
    [InlineData("0 0 * * MONDAY")]
    [InlineData("0 0 * JANUARY *")]
    [InlineData("0 0 * * *\n")]
    [InlineData("0 0 * * *\r")]
    [InlineData("0\v0 * * *")]
    [InlineData("0 0 * * *\0")]
    [InlineData("0 0 * * MÖN")]
    [InlineData("")]
    [InlineData("   ")]
    public void Unsupported_syntax_is_rejected_by_the_grammar(string text)
    {
        var error = Fails(() => CronGrammar.Parse(text));

        Assert.Equal(EvaluationErrorCode.InvalidGrammar, error.Code);
        Assert.Equal(EvaluationErrorClass.Invalid, error.Class);
    }

    [Fact]
    [Trait("Case", "SCHED-01")]
    public void Leap_second_sixty_is_rejected_in_the_seconds_field()
    {
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse("60 * * * * *", CronDialect.Cron6)).Code);
        Assert.Equal("59", Values(CronGrammar.Parse("59 * * * * *", CronDialect.Cron6).Seconds));
    }

    [Fact]
    [Trait("Case", "SCHED-01")]
    public void Cronos_receives_only_the_normalized_rendering()
    {
        Assert.Equal(["0 9 * * 1,2,3,4,5"], Cron("0 09 * * mon-fri").Adapter.StreamTexts);
        Assert.Equal(["0,15,30,45 * * * *"], Cron("*/15 * * * *").Adapter.StreamTexts);
        Assert.Equal(["0,30 0 9 * * *"], Cron("*/30 0 9 * * *", dialect: CronDialect.Cron6).Adapter.StreamTexts);
    }

    [Fact]
    [Trait("Case", "SCHED-02")]
    public void Both_day_fields_restricted_compile_to_an_or_of_two_streams()
    {
        Assert.Equal(["0 9 1 * *", "0 9 * * 1"], Cron("0 9 1 * MON").Adapter.StreamTexts);
        Assert.Equal(["0 9 1 * *"], Cron("0 9 1 * *").Adapter.StreamTexts);
        Assert.Equal(["0 9 * * 1"], Cron("0 9 * * MON").Adapter.StreamTexts);
    }

    [Theory]
    [Trait("Case", "SCHED-03")]
    [InlineData("0 0 30 2 *")]
    [InlineData("0 0 30,31 FEB *")]
    [InlineData("0 0 31 4,6,9,11 *")]
    [InlineData("0 0 31 2,4 *")]
    public void An_impossible_calendar_without_a_weekday_alternative_is_rejected(string text)
    {
        var error = Fails(() => CronGrammar.Parse(text));

        Assert.Equal(EvaluationErrorCode.InvalidCalendar, error.Code);
        Assert.Equal(EvaluationErrorClass.Invalid, error.Class);
    }

    [Fact]
    [Trait("Case", "SCHED-03")]
    public void An_impossible_day_of_month_branch_does_not_invalidate_a_weekday_branch()
    {
        var cron = Cron("0 0 30 2 MON");

        Assert.Equal(["0 0 * 2 1"], cron.Adapter.StreamTexts);
        Assert.Equal(["2027-02-01T00:00:00Z", "2027-02-08T00:00:00Z"], Plan(cron, "2026-12-31T00:00:00Z", 2));
    }

    [Theory]
    [Trait("Case", "SCHED-03")]
    [InlineData("0 0 29 2 *")]
    [InlineData("0 0 31 1-12 *")]
    [InlineData("0 0 30 2,3 *")]
    public void A_calendar_possible_in_some_selected_month_is_accepted(string text) =>
        Assert.True(CronGrammar.Parse(text).DayOfMonthFeasible);
}
