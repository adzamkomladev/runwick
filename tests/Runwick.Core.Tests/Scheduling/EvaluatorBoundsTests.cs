using NodaTime;
using Runwick.Scheduling.Evaluation;
using static Runwick.Core.Tests.Scheduling.ScheduleFixtures;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// SCHED-14 (spec 12 §8, §10): input byte and term bounds, the per-plan step bound, the per-lookup horizon, the
/// cooperative time budget, range overflow, and one evaluator task per group, across invalid, sparse and dense
/// expressions. Plus the preview of spec 12 §9.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
[Trait("Case", "SCHED-14")]
public sealed class EvaluatorBoundsTests
{
    [Fact]
    public void Input_is_bounded_to_256_characters()
    {
        var padded = "0" + new string(' ', CronGrammar.MaxLength - 8) + "0 * * *";

        Assert.Equal(CronGrammar.MaxLength, padded.Length);
        Assert.Equal("0", Values(CronGrammar.Parse(padded).Hours));
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse(padded + " ")).Code);
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse(new string('1', 100_000))).Code);
    }

    [Fact]
    public void Input_is_bounded_to_64_list_terms()
    {
        var sixty = string.Join(',', Enumerable.Range(0, 60));

        Assert.Equal(60, NormalizedCron.Values(CronGrammar.Parse($"{sixty} 0 * * *").Minutes).Count());
        Assert.Equal(EvaluationErrorCode.InvalidGrammar, Fails(() => CronGrammar.Parse($"{sixty} 0,1 * * *")).Code);
    }

    [Fact]
    public void The_plan_step_bound_is_deterministic_and_reviewable()
    {
        var dense = Cron("* * * * *");

        Assert.Equal(1_000, Plan(dense, "2026-01-01T00:00:00Z", 1_000).Length);
        var error = Fails(() => Plan(dense, "2026-01-01T00:00:00Z", PlannerLimits.Plan.MaxSteps));

        Assert.Equal(EvaluationErrorCode.StepLimitExceeded, error.Code);
        Assert.Equal(EvaluationErrorClass.Deterministic, error.Class);
    }

    [Fact]
    public void A_sparse_expression_is_a_few_steps_not_a_scan()
    {
        var budget = new EvaluationBudget(new FrozenTime(), PlannerLimits.Plan);

        var next = RecurrencePlanner.Next(
            Cron("0 0 29 2 *"), Instant.FromUtc(2096, 2, 29, 0, 0), PlannerLimits.Plan, budget);

        Assert.Equal("2104-02-29T00:00:00Z", Iso(next.UtcEpochMilliseconds));
        Assert.InRange(budget.Steps, 1, 4);
    }

    [Fact]
    public void A_dense_six_field_expression_stays_inside_the_bounds()
    {
        var budget = new EvaluationBudget(new FrozenTime(), PlannerLimits.Plan);
        var schedule = Cron("* * * * * *", "Europe/Berlin", dialect: CronDialect.Cron6);
        var cursor = Instant.FromUtc(2026, 10, 25, 0, 59, 55);

        for (var i = 0; i < 10; i++)
        {
            cursor = RecurrencePlanner.Next(schedule, cursor, PlannerLimits.Plan, budget).At;
        }

        // Four seconds of 02:59:5x CEST, then the repeated 02:00–02:59 CET hour is jumped (earlier policy).
        Assert.Equal(Instant.FromUtc(2026, 10, 25, 2, 0, 5), cursor);
        Assert.InRange(budget.Steps, 10, 48);
    }

    [Fact]
    public void Each_lookup_searches_at_most_the_horizon()
    {
        var shortHorizon = PlannerLimits.Plan with { HorizonYears = 7 };

        var error = Fails(() => Plan(Cron("0 0 29 2 *"), "2096-02-29T00:00:00Z", 1, shortHorizon));

        Assert.Equal(EvaluationErrorCode.NoMatchWithinHorizon, error.Code);
        Assert.Equal(EvaluationErrorClass.Deterministic, error.Class);
    }

    [Fact]
    public void An_elapsed_budget_is_local_pressure_not_invalidity()
    {
        var planner = new RecurrencePlanner(new SteppingTime(100));

        var error = Fails(() => planner.Plan(Cron("* * * * *"), Ms("2026-01-01T00:00:00Z"), 10, PlannerLimits.Plan));

        Assert.Equal(EvaluationErrorCode.BudgetElapsed, error.Code);
        Assert.Equal(EvaluationErrorClass.LocalPressure, error.Class);
    }

    [Fact]
    public void Dates_past_the_supported_range_overflow_instead_of_wrapping()
    {
        var leap = Cron("0 0 29 2 *");

        Assert.Equal(EvaluationErrorCode.RangeOverflow, Fails(() => Plan(leap, "9996-03-01T00:00:00Z", 1)).Code);
        Assert.Equal(
            EvaluationErrorCode.RangeOverflow,
            Fails(() => Planner().Plan(leap, long.MaxValue, 1, PlannerLimits.Plan)).Code);
        Assert.Equal(EvaluationErrorClass.Deterministic, EvaluationErrors.Classify(EvaluationErrorCode.RangeOverflow));
    }

    [Fact]
    public async Task One_evaluator_task_per_group_and_no_replacement_on_cancellation()
    {
        using var slot = new EvaluatorSlot();
        using var release = new ManualResetEventSlim();
        using var entered = new ManualResetEventSlim();
        var running = 0;
        var maxRunning = 0;

        int Work()
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref maxRunning, now);
            entered.Set();
            release.Wait(TestContext.Current.CancellationToken);
            Interlocked.Decrement(ref running);
            return now;
        }

        using var callerGivesUp = new CancellationTokenSource();
        var first = Task.Run(() => slot.RunAsync(Work, callerGivesUp.Token).AsTask(), TestContext.Current.CancellationToken);
        entered.Wait(TestContext.Current.CancellationToken);

        // The first caller abandons its budget; the slot stays held because the work has not returned.
        await callerGivesUp.CancelAsync();
        using var waiting = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => slot.RunAsync(Work, waiting.Token).AsTask());

        var queued = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() => slot.RunAsync(Work, CancellationToken.None).AsTask(), TestContext.Current.CancellationToken))
            .ToArray();
        release.Set();

        Assert.Equal(1, await first);
        Assert.All(await Task.WhenAll(queued), n => Assert.Equal(1, n));
        Assert.Equal(1, maxRunning);
    }

    [Fact]
    public void Preview_lists_the_next_ten_with_labels()
    {
        var preview = Planner().Preview(Cron("30 2 * * *", "Europe/Berlin", GapPolicy.Shift), Ms("2026-03-28T00:00:00Z"));

        Assert.Equal(PlannerLimits.PreviewCount, preview.Count);
        Assert.Equal(["exact", "gap-shifted", "exact"], preview.Take(3).Select(p => p.Label));
        Assert.Equal("2026-03-29T01:00:00Z", preview[1].Utc);
        Assert.Equal("2026-03-29T03:00:00", preview[1].Local);
        Assert.Equal("2026-03-29T02:00:00/2026-03-29T03:00:00", preview[1].Skipped);
        Assert.Null(preview[0].Skipped);
        Assert.Equal(Ms("2026-03-28T01:30:00Z"), preview[0].UtcEpochMilliseconds);
    }

    static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while (value > (seen = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }
}
