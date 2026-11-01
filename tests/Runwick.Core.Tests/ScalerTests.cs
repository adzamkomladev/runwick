using Runwick.Worker.Scaling;

namespace Runwick.Core.Tests;

/// <summary>
/// SP-014 controller rules (11 §3, capacity-defaults §4): the SCL-01 traces, band and boundary waiver, evidence,
/// cooldown, bounded steps, bootstrap, overrides (CAP-07), the service estimate (CAP-06) and occupancy integration.
/// The controller is pure: time and demand are supplied by the test.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-014")]
public sealed class ScalerTests
{
    static readonly TimeSpan Tick = ScalerRules.SampleInterval;

    /// <summary>Queued work that alone asks for <paramref name="tickets"/> tickets at 80 % over the 30 s horizon.</summary>
    static double WorkFor(int tickets) => tickets * ScalerRules.TargetUtilization * ScalerRules.DrainHorizon.TotalSeconds - 0.001;

    static ScalerSample S(TimeSpan now, double occupied = 0, double? work = 0, bool backlog = false, string? freeze = null, int? over = null,
        long membership = 1) => new(now, occupied, work, backlog, freeze, over, membership);

    /// <summary>Runs samples every 5 s and returns the sequence of applied targets.</summary>
    static List<int> Drive(PoolScaler scaler, Func<TimeSpan, ScalerSample> sample, TimeSpan duration)
    {
        var targets = new List<int> { scaler.Current };
        for (var now = Tick; now <= duration; now += Tick)
        {
            if (scaler.Sample(sample(now)).NewTarget is { } t)
            {
                targets.Add(t);
            }
        }

        return targets;
    }

    [Fact]
    [Trait("Case", "SCL-01")]
    public void Sustained_demand_grows_by_bounded_steps_to_the_maximum()
    {
        var scaler = new PoolScaler(4, 16);
        var targets = Drive(scaler, now => S(now, work: WorkFor(100), backlog: true), TimeSpan.FromMinutes(5));
        Assert.Equal([4, 5, 7, 9, 12, 15, 16], targets);
    }

    [Fact]
    [Trait("Case", "SCL-01")]
    public void Sustained_idleness_shrinks_by_bounded_steps_to_the_minimum()
    {
        var scaler = new PoolScaler(4, 16);
        Drive(scaler, now => S(now, work: WorkFor(100), backlog: true), TimeSpan.FromMinutes(5));
        Assert.Equal(16, scaler.Current);

        var start = TimeSpan.FromMinutes(5);
        var targets = new List<int> { scaler.Current };
        for (var now = start + Tick; now <= start + TimeSpan.FromMinutes(5); now += Tick)
        {
            if (scaler.Sample(S(now)).NewTarget is { } t)
            {
                targets.Add(t);
            }
        }

        Assert.Equal([16, 12, 9, 6, 4], targets);
    }

    [Fact]
    [Trait("Case", "SCL-01")]
    public void Changes_are_at_least_one_cooldown_apart_and_need_consecutive_evidence()
    {
        var scaler = new PoolScaler(1, 64);
        var changes = new List<TimeSpan>();
        for (var now = Tick; now <= TimeSpan.FromMinutes(3); now += Tick)
        {
            var d = scaler.Sample(S(now, work: WorkFor(64), backlog: true));
            if (d.NewTarget is not null)
            {
                changes.Add(now);
            }
        }

        Assert.Equal(TimeSpan.FromSeconds(15), changes[0]);
        Assert.All(changes.Zip(changes.Skip(1)), p => Assert.True(p.Second - p.First >= ScalerRules.ChangeCooldown));
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void Demand_inside_the_band_holds_and_interrupted_evidence_restarts()
    {
        // 1→2→3→4→5→7→9; a candidate of 10 is then inside the 20 % band of 9 (2) and away from max: it holds.
        var scaler = new PoolScaler(1, 64);
        Drive(scaler, now => S(now, work: WorkFor(10), backlog: true), TimeSpan.FromMinutes(4));
        var current = scaler.Current;
        Assert.Equal(9, current);

        var at = TimeSpan.FromMinutes(10);
        Assert.Equal("within_band", scaler.Sample(S(at, work: WorkFor(10), backlog: true)).Reason);

        // Two growth samples, one in-band sample, two more: no change because evidence restarted.
        var reasons = new[] { 20, 20, 10, 20, 20 }.Select((w, i) => scaler.Sample(S(at + Tick * (i + 1), work: WorkFor(w), backlog: true)))
            .ToList();
        Assert.All(reasons, d => Assert.Null(d.NewTarget));
        Assert.Equal("grow_evidence", reasons[^1].Reason);
        Assert.Equal(current, scaler.Current);
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void The_boundary_waiver_reaches_max_and_min_inside_the_band()
    {
        // From 8 the band is 2: candidate 9 (= max) is inside it, and only the waiver lets it apply.
        var up = new PoolScaler(8, 9);
        var applied = Enumerable.Range(1, 12).Select(i => up.Sample(S(Tick * i, work: WorkFor(9), backlog: true)))
            .Where(d => d.NewTarget is not null).ToList();
        Assert.Equal(9, Assert.Single(applied).NewTarget);
        Assert.Equal("grow_boundary", applied[0].Reason);

        var down = new PoolScaler(8, 9);
        Drive(down, now => S(now, work: WorkFor(9), backlog: true), TimeSpan.FromMinutes(1));
        Assert.Equal([9, 8], Drive(down, now => S(now + TimeSpan.FromMinutes(1)), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void Growth_needs_due_backlog_and_freezes_on_pressure_signals()
    {
        var scaler = new PoolScaler(2, 32);

        // Occupancy alone never grows a pool once its backlog drained.
        var d = scaler.Sample(S(Tick, occupied: 20, work: 0, backlog: false));
        Assert.Equal("no_due_backlog", d.Reason);

        foreach (var freeze in new[] { "pressure", "admission_closed", "presence_failing", "blocked_ratio", "unresolved_command" })
        {
            var f = new PoolScaler(2, 32);
            var targets = Drive(f, now => S(now, work: WorkFor(32), backlog: true, freeze: freeze), TimeSpan.FromMinutes(2));
            Assert.Equal([2], targets);
        }

        // A frozen pool still shrinks.
        var s = new PoolScaler(2, 32);
        Drive(s, now => S(now, work: WorkFor(32), backlog: true), TimeSpan.FromMinutes(2));
        var before = s.Current;
        var shrunk = Drive(s, now => S(now + TimeSpan.FromMinutes(2), freeze: "pressure"), TimeSpan.FromMinutes(2));
        Assert.True(shrunk[^1] < before);
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void Unknown_demand_holds_and_is_never_read_as_zero()
    {
        var scaler = new PoolScaler(0, 8);
        Drive(scaler, now => S(now, work: WorkFor(8), backlog: true), TimeSpan.FromMinutes(3));
        var current = scaler.Current;
        Assert.True(current > 1);

        var targets = Drive(scaler, now => S(now + TimeSpan.FromMinutes(3), work: null), TimeSpan.FromMinutes(5));
        Assert.Equal([current], targets);
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void Minimum_zero_bootstraps_one_ticket_until_trusted_idle_evidence()
    {
        var scaler = new PoolScaler(0, 8);
        Assert.Equal(1, scaler.Current);
        Assert.True(scaler.Bootstrapping);

        // Unknown demand keeps the bootstrap ticket.
        Assert.Null(scaler.Sample(S(Tick, work: null)).NewTarget);
        Assert.True(scaler.Bootstrapping);

        var idle = scaler.Sample(S(Tick * 2));
        Assert.Equal(ScalerAction.Bootstrap, idle.Action);
        Assert.Equal(0, idle.NewTarget);
        Assert.False(scaler.Bootstrapping);

        // Demand later grows it from zero by normal rules.
        var grown = Drive(scaler, now => S(now + Tick * 2, work: WorkFor(8), backlog: true), TimeSpan.FromMinutes(1));
        Assert.Equal(1, grown[1]);
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void An_override_applies_at_once_stops_automatic_changes_and_is_clamped_to_max()
    {
        var scaler = new PoolScaler(2, 10);
        var set = scaler.Sample(S(Tick, work: WorkFor(10), backlog: true, over: 7));
        Assert.Equal(ScalerAction.Override, set.Action);
        Assert.Equal(7, set.NewTarget);

        var held = Drive(scaler, now => S(now + Tick, work: WorkFor(10), backlog: true, over: 7), TimeSpan.FromMinutes(2));
        Assert.Equal([7], held);

        var clamped = scaler.Sample(S(TimeSpan.FromMinutes(3), over: 50));
        Assert.Equal(10, clamped.NewTarget);
        Assert.Equal("override_clamped_to_max", clamped.Reason);

        // Zero is a valid override below the minimum: an operator may stop a pool's refill.
        Assert.Equal(0, scaler.Sample(S(TimeSpan.FromMinutes(4), over: 0)).NewTarget);

        // Expiry or removal resumes inside [min, max] from the current target.
        var removed = scaler.Sample(S(TimeSpan.FromMinutes(5)));
        Assert.Equal("override_removed", removed.Reason);
        Assert.Equal(2, removed.NewTarget);
    }

    [Fact]
    [Trait("Case", "CAP-07")]
    public void A_membership_change_restarts_evidence()
    {
        var scaler = new PoolScaler(2, 32);
        scaler.Sample(S(Tick, work: WorkFor(32), backlog: true));
        scaler.Sample(S(Tick * 2, work: WorkFor(32), backlog: true));
        Assert.Equal(2, scaler.GrowEvidence);

        Assert.Equal("membership_changed", scaler.Sample(S(Tick * 3, work: WorkFor(32), backlog: true, membership: 2)).Reason);
        Assert.Equal(0, scaler.GrowEvidence);
    }

    [Fact]
    [Trait("Case", "CAP-06")]
    public void The_service_estimate_is_cold_then_an_ewma_with_a_live_lower_bound()
    {
        var e = new ServiceEstimator();
        Assert.Equal((1.0, false), e.Estimate("t", null));
        Assert.Equal(4.0, e.Estimate("t", TimeSpan.FromSeconds(4)).Seconds);

        e.Completed("t", TimeSpan.FromSeconds(2));
        Assert.Equal(2.0, e.Estimate("t", null).Seconds, 6);
        e.Completed("t", TimeSpan.FromSeconds(12));
        Assert.Equal(4.0, e.Estimate("t", null).Seconds, 6);

        for (var i = 0; i < 18; i++)
        {
            e.Completed("t", TimeSpan.FromSeconds(4));
        }

        Assert.True(e.Estimate("t", null).Confident);
        Assert.Equal((1.0, false), e.Estimate("other", null));
    }

    [Fact]
    [Trait("Case", "CAP-06")]
    public void Occupancy_is_a_time_weighted_mean_over_the_window()
    {
        var o = new OccupancyIntegrator();
        var w = ScalerRules.OccupancyWindow;
        Assert.Equal(0, o.Mean(TimeSpan.FromSeconds(100), w));

        o.Record(TimeSpan.FromSeconds(100), 4);
        o.Record(TimeSpan.FromSeconds(115), 2);
        Assert.Equal(3.0, o.Mean(TimeSpan.FromSeconds(130), w), 6);

        // Changes older than the window collapse into the level entering it.
        Assert.Equal(2.0, o.Mean(TimeSpan.FromSeconds(200), w), 6);
    }

    [Fact]
    [Trait("Case", "CAP-08")]
    public void The_blocked_ratio_needs_twenty_cycles_and_three_quarters_blocked()
    {
        var a = new AdmissionOutcomes();
        for (var i = 0; i < 19; i++)
        {
            a.Record(TimeSpan.FromSeconds(i), blocked: true);
        }

        Assert.False(a.MostlyBlocked(TimeSpan.FromSeconds(19)));
        a.Record(TimeSpan.FromSeconds(19), blocked: false);
        Assert.True(a.MostlyBlocked(TimeSpan.FromSeconds(20)));

        for (var i = 0; i < 10; i++)
        {
            a.Record(TimeSpan.FromSeconds(20), blocked: false);
        }

        Assert.False(a.MostlyBlocked(TimeSpan.FromSeconds(20)));
        Assert.False(a.MostlyBlocked(TimeSpan.FromSeconds(100)));
    }

    [Theory]
    [Trait("Case", "CAP-07")]
    [InlineData(0, 0, 0)]
    [InlineData(4, 0, 5)]
    [InlineData(0, 24, 1)]
    [InlineData(8, 240, 20)]
    public void Desired_is_occupancy_plus_drain_rate_over_utilization(double occupied, double work, int expected) =>
        Assert.Equal(expected, PoolScaler.Desired(occupied, work));

    [Fact]
    [Trait("Case", "SCL-03")]
    public void An_oscillating_backlog_and_long_jobs_never_move_the_target()
    {
        // capacity-control §6 "oscillating backlog and long jobs": demand that flips every sample never builds three
        // consecutive growth or six shrink samples; long jobs alone (occupancy, no due backlog) never grow.
        var scaler = new PoolScaler(4, 16);
        var flips = Drive(scaler, now => (int)(now / Tick) % 2 == 0 ? S(now, work: WorkFor(16), backlog: true) : S(now, occupied: 4),
            TimeSpan.FromMinutes(10));
        Assert.Equal([4], flips);

        // Two-on, one-off: still never three in a row.
        var bursts = Drive(new PoolScaler(4, 16), now => (int)(now / Tick) % 3 == 2 ? S(now, occupied: 4) : S(now, work: WorkFor(16), backlog: true),
            TimeSpan.FromMinutes(10));
        Assert.Equal([4], bursts);

        // Long jobs hold every ticket for minutes with nothing due: no growth, and the occupied tickets keep the target.
        var busy = new PoolScaler(4, 16);
        Assert.Equal([4], Drive(busy, now => S(now, occupied: 4, work: 0, backlog: false), TimeSpan.FromMinutes(10)));
        Assert.Equal("no_due_backlog", busy.Sample(S(TimeSpan.FromMinutes(11), occupied: 16, work: 0, backlog: false)).Reason);
    }

    [Fact]
    [Trait("Case", "SCL-05")]
    public void A_zero_minimum_keeps_discovery_through_missing_metrics_and_recovers_from_zero()
    {
        // capacity-control §6 "zero minimum and missing metrics": the bootstrap ticket survives any run of unknown
        // samples, trusted idleness drops it to zero, and due work grows it back by normal rules.
        var scaler = new PoolScaler(0, 4);
        Assert.Equal([1], Drive(scaler, now => S(now, work: null), TimeSpan.FromMinutes(10)));
        Assert.True(scaler.Bootstrapping);

        var start = TimeSpan.FromMinutes(10);
        Assert.Equal(0, scaler.Sample(S(start + Tick)).NewTarget);

        // At zero, unknown demand holds zero (never invents work); due backlog grows it after three samples.
        Assert.Null(scaler.Sample(S(start + Tick * 2, work: null)).NewTarget);
        var grown = new List<(TimeSpan At, int Target)>();
        for (var now = start + Tick * 3; now <= start + TimeSpan.FromMinutes(3); now += Tick)
        {
            if (scaler.Sample(S(now, work: WorkFor(4), backlog: true)).NewTarget is { } t)
            {
                grown.Add((now, t));
            }
        }

        // Evidence is complete at +25 s, but the drop to zero at +5 s starts a cooldown: the first growth is at +35 s.
        Assert.Equal(start + Tick + ScalerRules.ChangeCooldown, grown[0].At);
        Assert.Equal([1, 2, 3, 4], grown.Select(g => g.Target));
    }
}
