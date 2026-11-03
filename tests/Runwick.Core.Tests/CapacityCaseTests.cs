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

    // ------------------------------------------------------------------ CAP-02 (batched claim)

    [Fact]
    [Trait("Category", "Pure")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "CAP-02")]
    public async Task A_queue_hidden_by_a_stale_hint_returns_at_zero_with_its_exact_share_while_batches_run()
    {
        // Queues 1:3 in one pool of four slots, over the batched claim. Only "other" has work at first, so every free
        // slot is claimed in one ReserveAndBegin. Then "default" gets work behind the worker's back, as a queue resumed
        // by another process does (no wake: 05 Changes 2026-11-02, idle caps). Until the hint expires (2 s) the snapshot
        // still hides "default", so every admission goes to "other": that is §8.3 over the snapshot, not a share error.
        await using var e = EngineFixture.Create(concurrency: 4, batch: true, configure: r => r.Queue("other"), pool: p => p.Queue("other", 3));

        // Each handler takes 200 ms of fake time, so four slots admit about 20 jobs a second and neither queue runs dry.
        e.Script.Default = (_, _, _) => Task.Delay(TimeSpan.FromMilliseconds(200), e.Time, CancellationToken.None);
        for (var i = 0; i < 400; i++)
        {
            e.AddTo("other", "o" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Admissions.Count >= 20, Long, "20 admissions");
        Assert.True(e.Engine.Pools.Single().BatchClaims > 0, "the single-queue phase claims in batches");

        // Right after a fresh probe, so the hint that hides "default" has its full 2 s to live.
        var probes = Interlocked.Read(ref e.Engine.Readiness.Probes);
        await e.RunUntilAsync(() => Interlocked.Read(ref e.Engine.Readiness.Probes) > probes, Long, "a fresh probe", TimeSpan.FromMilliseconds(5));
        await EngineFixture.SettleAsync();
        var mark = e.Store.Admissions.Count;
        for (var i = 0; i < 60; i++)
        {
            e.AddTo("default", "d" + i);
        }

        await e.RunUntilAsync(() => e.Store.Admissions.Skip(mark).Contains("default"), Long, "first admission of the returning queue");
        var firstD = mark + e.Store.Admissions.Skip(mark).TakeWhile(q => q == "other").Count();
        await e.RunUntilAsync(() => e.Store.Admissions.Count >= firstD + 48, Long, "48 admissions after the return");
        var order = e.Store.Admissions.Select(q => q[0]).ToList();

        // The stale hint admitted more than the 8 that CAP-02 (real store) used to skip: measured from the change, the old
        // window under-counts "default" exactly as the real-store failure did.
        Assert.True(firstD - mark > 8, $"{firstD - mark} admissions under the stale hint");
        Assert.InRange(order.Skip(mark + 8).Take(40).Count(k => k == 'd'), 0, 8);

        // Measured from the first admission a fresh snapshot allowed: the returning queue starts at zero and gets exactly
        // one of every four admissions, never two in a row (no banked credit).
        var after = order.Skip(firstD).Take(40).ToList();
        Assert.Equal(10, after.Count(k => k == 'd'));
        Assert.DoesNotContain(after.Zip(after.Skip(1)), p => p.First == 'd' && p.Second == 'd');
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
