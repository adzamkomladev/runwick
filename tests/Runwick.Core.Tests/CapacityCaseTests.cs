using Runwick.Harness.Engine;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Core.Tests;

/// <summary>CAP-01 (Pure part) and CAP-03 (07 §11): pool bounds, group limits and the lane selector over fake time: SP-009.</summary>
public sealed class CapacityCaseTests
{
    static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    // ------------------------------------------------------------------ CAP-01

    static (WorkerSettings? Settings, List<ConfigurationError> Errors) Validate(Action<WorkerBuilder> pools)
    {
        var builder = new WorkerBuilder();
        pools(builder);
        try
        {
            return (WorkerSettings.Validate(builder, ["default", "other"]), []);
        }
        catch (RunwickConfigurationException ex)
        {
            return (null, [.. ex.Errors]);
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-01")]
    public void Pool_bounds_and_group_limits_are_validated_at_startup()
    {
        // Maxima summing to 17 exceed the group maximum 16.
        var (_, over) = Validate(w => w
            .Pool("a", p => p.Queues("default").FixedConcurrency(8))
            .Pool("b", p => p.Queues("other").FixedConcurrency(8))
            .Pool("c", p => p.Queues("default").FixedConcurrency(1)));
        Assert.Contains(over, e => e.Code == RunwickErrorCodes.PoolBudgetExceeded);

        // FixedConcurrency(8) × 2 is exactly the group maximum; FixedConcurrency(n) sets target = maximum = n.
        var (two, none) = Validate(w => w
            .Pool("a", p => p.Queues("default").FixedConcurrency(8))
            .Pool("b", p => p.Queues("other").FixedConcurrency(8)));
        Assert.Empty(none);
        Assert.Equal([8, 8], two!.Pools.Select(p => p.Target));
        Assert.Equal(16, two.GroupMax);

        // A pool with no mode is fixed with target and maximum 4.
        var (defaults, _) = Validate(w => w.Pool("p", p => p.Queues("default")));
        Assert.Equal(WorkerSettings.DefaultPoolTarget, Assert.Single(defaults!.Pools).Target);
        Assert.Equal(4, defaults.Pools[0].Target);

        // V-15 (06): n ≥ 1; AutoConcurrency needs 0 ≤ min ≤ max and max ≥ 1 (SP-014), and its maximum counts toward the group sum.
        var (_, zero) = Validate(w => w.Pool("p", p => p.Queues("default").FixedConcurrency(0)));
        Assert.Contains(zero, e => e.Code == RunwickErrorCodes.InvalidPool && e.Message.StartsWith("V-15", StringComparison.Ordinal));
        var (_, inverted) = Validate(w => w.Pool("p", p => p.Queues("default").AutoConcurrency(5, 3)));
        Assert.Contains(inverted, e => e.Code == RunwickErrorCodes.InvalidPool && e.Message.StartsWith("V-15", StringComparison.Ordinal));
        var (_, autoOver) = Validate(w => w.Pool("p", p => p.Queues("default").AutoConcurrency(1, 17)));
        Assert.Contains(autoOver, e => e.Code == RunwickErrorCodes.PoolBudgetExceeded);
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-01")]
    public async Task Hung_handlers_never_take_more_than_the_fixed_maximum()
    {
        await using var e = EngineFixture.Create(concurrency: 16);
        e.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromHours(1), e.Time, CancellationToken.None);
        for (var i = 0; i < 24; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.Invocations.Count == 16, Long, "16 hung invocations");
        var maxEntries = 0;
        for (var t = 0; t < 120; t++)
        {
            await e.AdvanceAsync(TimeSpan.FromSeconds(1));
            maxEntries = Math.Max(maxEntries, e.Engine.Entries.Count);
        }

        Assert.Equal(16, maxEntries);
        Assert.Equal(16, e.Script.Invocations.Count);
        Assert.Equal(8, e.Store.Jobs.Count(j => j.State == JobStates.Pending));
    }

    // ------------------------------------------------------------------ CAP-10 (pressure gate input)

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-10")]
    public async Task Control_wait_is_read_safely_while_waiters_come_and_go()
    {
        // The supervisor tick reads the oldest control wait while renewals enter and leave their lanes; the last
        // waiter leaving mid-read once faulted the critical supervisor (G-1 run 2026-10-31, G1-M3).
        await using var e = EngineFixture.Create();
        var lease = e.Engine.Lease;
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var churn = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                lease.TrackWait().Dispose();
            }
        });
        var reads = 0L;
        while (!stop.IsCancellationRequested)
        {
            Assert.True(lease.OldestWait(e.Engine.Clock.Now) >= TimeSpan.Zero);
            reads++;
        }

        await churn;
        Assert.True(reads > 1000, $"{reads} reads");
        Assert.Equal(TimeSpan.Zero, lease.OldestWait(e.Engine.Clock.Now));
    }

    // ------------------------------------------------------------------ CAP-03

    static LaneSelector Selector(LaneSelectionMode mode) => new(new PoolSettings("p", [("default", 1)], 4, mode, (6, 3, 1)));

    static Dictionary<LaneKey, TimeSpan> Ready(TimeSpan? high = null, TimeSpan? medium = null, TimeSpan? low = null)
    {
        var ready = new Dictionary<LaneKey, TimeSpan>();
        foreach (var (p, age) in new[] { ("high", high), ("medium", medium), ("low", low) })
        {
            if (age is { } a)
            {
                ready[new LaneKey("default", p)] = a;
            }
        }

        return ready;
    }

    static LaneChoice Admit(LaneSelector s, Dictionary<LaneKey, TimeSpan> ready)
    {
        var c = s.Choose(ready, TimeSpan.Zero)!;
        s.OnApplied(c);
        return c;
    }

    static long[] Scores(LaneSelector s) => [.. LaneSelector.Priorities.Select(p => s.LaneScore("default", p))];

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-03")]
    public void Weighted_lanes_divide_admissions_six_three_one()
    {
        var s = Selector(LaneSelectionMode.Weighted);
        var ready = Ready(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
        var picks = Enumerable.Range(0, 100).Select(_ => Admit(s, ready)).ToList();
        Assert.All(picks, c => Assert.False(c.Aging));
        Assert.Equal(60, picks.Count(c => c.Priority == "high"));
        Assert.Equal(30, picks.Count(c => c.Priority == "medium"));
        Assert.Equal(10, picks.Count(c => c.Priority == "low"));
    }

    [Theory]
    [InlineData(59_999, false)]
    [InlineData(60_000, true)]
    [InlineData(60_001, true)]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-03")]
    public void Aging_after_eight_admissions_at_threshold_equality(int lowAgeMs, bool aged)
    {
        var s = Selector(LaneSelectionMode.StrictWithAging);
        var young = Ready(high: TimeSpan.Zero, low: TimeSpan.FromMilliseconds(59_999));
        for (var i = 0; i < 20; i++)
        {
            var c = Admit(s, young);
            Assert.Equal(("high", false), (c.Priority, c.Aging));
            Assert.Equal(Math.Min(i + 1, WorkerSettings.AgingEvery), s.AgingCount("default"));
        }

        // The counter saturates at 8; age 60 000 ms is aged (07 C-2), 59 999 ms is not.
        var choice = s.Choose(Ready(high: TimeSpan.Zero, low: TimeSpan.FromMilliseconds(lowAgeMs)), TimeSpan.Zero)!;
        Assert.Equal(aged ? ("low", true) : ("high", false), (choice.Priority, choice.Aging));
        s.OnApplied(choice);
        Assert.Equal(aged ? 0 : 8, s.AgingCount("default"));
        if (aged)
        {
            Assert.Equal([0L, 0L, 0L], Scores(s));
        }
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-03")]
    public void Aging_success_resets_scores_and_failed_aging_rotates_the_pointer()
    {
        var s = Selector(LaneSelectionMode.Weighted);
        var ready = Ready(TimeSpan.Zero, TimeSpan.FromSeconds(90), TimeSpan.FromSeconds(90));
        while (s.AgingCount("default") < WorkerSettings.AgingEvery)
        {
            Assert.False(Admit(s, ready).Aging);
        }

        // Failed aging attempts (e.g. CapacityBlocked) leave scores unchanged and rotate the lower-lane pointer.
        var before = Scores(s);
        var first = s.Choose(ready, TimeSpan.Zero)!;
        Assert.Equal(("medium", true), (first.Priority, first.Aging));
        s.OnNotApplied(first);
        Assert.Equal(before, Scores(s));
        var second = s.Choose(ready, TimeSpan.Zero)!;
        Assert.Equal(("low", true), (second.Priority, second.Aging));
        s.OnNotApplied(second);
        Assert.Equal(before, Scores(s));
        Assert.Equal(8, s.AgingCount("default"));
        var third = s.Choose(ready, TimeSpan.Zero)!;
        Assert.Equal(("medium", true), (third.Priority, third.Aging));

        // An aging success resets the counter and the lane scores.
        s.OnApplied(third);
        Assert.Equal(0, s.AgingCount("default"));
        Assert.Equal([0L, 0L, 0L], Scores(s));
    }

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "SP-009")]
    [Trait("Case", "CAP-03")]
    public void Strict_starves_the_low_lane_and_strict_with_aging_selects_it_periodically()
    {
        var ready = Ready(high: TimeSpan.Zero, low: TimeSpan.FromMinutes(10));

        // Strict (documented): a continuously claimable high lane starves an aged low lane.
        var strict = Selector(LaneSelectionMode.Strict);
        Assert.All(Enumerable.Range(0, 100).Select(_ => Admit(strict, ready)), c => Assert.Equal(("high", false), (c.Priority, c.Aging)));

        // Strict with aging: every ninth admission is the aged low lane.
        var aging = Selector(LaneSelectionMode.StrictWithAging);
        var picks = Enumerable.Range(0, 90).Select(_ => Admit(aging, ready)).ToList();
        Assert.Equal(10, picks.Count(c => c.Priority == "low"));
        Assert.All(picks.Where((_, i) => i % 9 == 8), c => Assert.Equal(("low", true), (c.Priority, c.Aging)));
    }
}
