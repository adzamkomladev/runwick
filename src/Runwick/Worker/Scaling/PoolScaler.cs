namespace Runwick.Worker.Scaling;

/// <summary>Controller constants (11 §3, capacity-defaults §4). Benchmark inputs, not tuned release values.</summary>
internal static class ScalerRules
{
    public static readonly TimeSpan SampleInterval = TimeSpan.FromSeconds(5);
    public const double SampleJitter = 0.10;
    public static readonly TimeSpan SnapshotMaxAge = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan OccupancyWindow = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan DrainHorizon = TimeSpan.FromSeconds(30);
    public const double TargetUtilization = 0.80;
    public const double BandFraction = 0.20;
    public const double StepFraction = 0.25;
    public const int GrowSamples = 3;
    public const int ShrinkSamples = 6;
    public static readonly TimeSpan ChangeCooldown = TimeSpan.FromSeconds(30);
    public const double EwmaAlpha = 0.20;
    public static readonly TimeSpan ColdEstimate = TimeSpan.FromSeconds(1);
    public const int ConfidentSamples = 20;
    public static readonly TimeSpan EstimateDeadline = TimeSpan.FromSeconds(1);
    public const int BlockedRatioMinimumCycles = 20;
    public const double BlockedRatioFreeze = 0.75;
    public static readonly TimeSpan BlockedRatioWindow = TimeSpan.FromSeconds(30);
}

/// <summary>
/// One sample's inputs for a pool (11 §3.1). <see cref="QueuedWorkSeconds"/> is null when demand is unknown
/// (stale, truncated coverage, failed or late estimate) — unknown is never zero.
/// </summary>
internal sealed record ScalerSample(
    TimeSpan Now,
    double MeanOccupiedTickets,
    double? QueuedWorkSeconds,
    bool PositiveBacklog,
    string? FreezeGrowth,
    int? OverrideTarget,
    long MembershipRevision);

internal enum ScalerAction
{
    Hold,
    Grow,
    Shrink,
    Override,
    Bootstrap,
}

/// <summary>A sample's decision. <see cref="NewTarget"/> is set only when the target changes.</summary>
internal sealed record ScalerDecision(ScalerAction Action, int Current, int? NewTarget, int? Desired, int? Candidate, string Reason);

/// <summary>
/// The per-pool automatic controller (11 §3): occupancy plus allocated due backlog, hysteresis band with the R4
/// boundary waiver, consecutive evidence, change cooldown and bounded steps. Pure: the caller supplies time and
/// inputs; the result is applied by stopping or allowing refill, never by revoking tickets.
/// </summary>
internal sealed class PoolScaler
{
    int _grow;
    int _shrink;
    TimeSpan? _lastChange;
    long _membership = -1;
    int? _override;

    public PoolScaler(int min, int max)
    {
        if (min < 0 || max < 1 || min > max)
        {
            throw new ArgumentOutOfRangeException(nameof(min), $"AutoConcurrency({min}, {max}) needs 0 ≤ min ≤ max, max ≥ 1.");
        }

        Min = min;
        Max = max;

        // Min zero starts at one bootstrap ticket until trusted idle evidence (capacity-defaults §4).
        Current = min == 0 ? 1 : min;
        Bootstrapping = min == 0;
    }

    public int Min { get; }

    public int Max { get; }

    public int Current { get; private set; }

    public bool Bootstrapping { get; private set; }

    public int GrowEvidence => _grow;

    public int ShrinkEvidence => _shrink;

    public static int Desired(double meanOccupied, double queuedWorkSeconds) =>
        (int)Math.Min(int.MaxValue, Math.Ceiling((meanOccupied + queuedWorkSeconds / ScalerRules.DrainHorizon.TotalSeconds) / ScalerRules.TargetUtilization));

    static int Band(int current) => Math.Max(1, (int)Math.Ceiling(current * ScalerRules.BandFraction));

    static int Step(int current) => Math.Max(1, (int)Math.Ceiling(current * ScalerRules.StepFraction));

    public ScalerDecision Sample(ScalerSample s)
    {
        // Override transitions take effect at once and reset evidence; while one is active automatic changes stop.
        var overrideTarget = s.OverrideTarget is { } o ? Math.Clamp(o, 0, Max) : (int?)null;
        if (overrideTarget != _override)
        {
            _override = overrideTarget;
            Reset();
            if (overrideTarget is { } ot)
            {
                Bootstrapping = false;
                return Apply(s.Now, ScalerAction.Override, ot, null, null, s.OverrideTarget > Max ? "override_clamped_to_max" : "override");
            }

            // Removal: resume from the current target inside [Min, Max].
            var resumed = Math.Clamp(Current, Min, Max);
            return resumed == Current
                ? new ScalerDecision(ScalerAction.Hold, Current, null, null, null, "override_removed")
                : Apply(s.Now, ScalerAction.Override, resumed, null, null, "override_removed");
        }

        if (_override is not null)
        {
            return Hold("override_active");
        }

        if (s.MembershipRevision != _membership)
        {
            // Membership or configuration change: evidence restarts (capacity-defaults §4).
            var first = _membership == -1;
            _membership = s.MembershipRevision;
            Reset();
            if (!first)
            {
                return Hold("membership_changed");
            }
        }

        if (s.QueuedWorkSeconds is not { } work)
        {
            Reset();
            return Hold("demand_unknown");
        }

        var desired = Desired(s.MeanOccupiedTickets, work);
        var candidate = Math.Clamp(desired, Min, Max);
        if (Bootstrapping)
        {
            // Trusted demand ends the bootstrap: idle evidence drops to Min (zero), demand continues by normal rules.
            Bootstrapping = false;
            if (candidate == 0 && !s.PositiveBacklog && s.MeanOccupiedTickets <= 0)
            {
                return Apply(s.Now, ScalerAction.Bootstrap, 0, desired, candidate, "bootstrap_idle");
            }
        }

        var growBoundary = candidate == Max && Current < Max;
        var shrinkBoundary = candidate == Min && Current > Min;
        var grows = (candidate >= Current + Band(Current) || growBoundary) && candidate > Current;
        var shrinks = (candidate <= Current - Band(Current) || shrinkBoundary) && candidate < Current;

        if (grows && !s.PositiveBacklog)
        {
            // Occupancy alone never grows a pool after its backlog drained.
            Reset();
            return new ScalerDecision(ScalerAction.Hold, Current, null, desired, candidate, "no_due_backlog");
        }

        if (grows && s.FreezeGrowth is { } freeze)
        {
            Reset();
            return new ScalerDecision(ScalerAction.Hold, Current, null, desired, candidate, "frozen:" + freeze);
        }

        if (grows)
        {
            _grow++;
            _shrink = 0;
        }
        else if (shrinks)
        {
            _shrink++;
            _grow = 0;
        }
        else
        {
            Reset();
            return new ScalerDecision(ScalerAction.Hold, Current, null, desired, candidate, "within_band");
        }

        var needed = grows ? ScalerRules.GrowSamples : ScalerRules.ShrinkSamples;
        if ((grows ? _grow : _shrink) < needed)
        {
            return new ScalerDecision(ScalerAction.Hold, Current, null, desired, candidate, grows ? "grow_evidence" : "shrink_evidence");
        }

        if (_lastChange is { } last && s.Now - last < ScalerRules.ChangeCooldown)
        {
            // Evidence keeps accumulating during the cooldown; no change before it expires.
            return new ScalerDecision(ScalerAction.Hold, Current, null, desired, candidate, "cooldown");
        }

        var step = Step(Current);
        var next = grows ? Math.Min(candidate, Current + step) : Math.Max(candidate, Current - step);
        return Apply(s.Now, grows ? ScalerAction.Grow : ScalerAction.Shrink, next, desired, candidate,
            grows ? (growBoundary && candidate < Current + Band(Current) ? "grow_boundary" : "grow") : (shrinkBoundary && candidate > Current - Band(Current) ? "shrink_boundary" : "shrink"));
    }

    ScalerDecision Hold(string reason) => new(ScalerAction.Hold, Current, null, null, null, reason);

    ScalerDecision Apply(TimeSpan now, ScalerAction action, int target, int? desired, int? candidate, string reason)
    {
        var old = Current;
        Current = target;
        _lastChange = now;
        Reset();
        return new ScalerDecision(action, old, target == old ? null : target, desired, candidate, reason);
    }

    void Reset()
    {
        _grow = 0;
        _shrink = 0;
    }
}

/// <summary>
/// Time-weighted mean of outstanding tickets over the last 30 s (capacity-defaults §4), integrated from changes.
/// Thread-safe; fed by the pool's capacity gate. Before the first change the level is zero.
/// </summary>
internal sealed class OccupancyIntegrator
{
    readonly Lock _lock = new();
    readonly List<(TimeSpan At, int Level)> _changes = [];
    int _initial;

    public void Record(TimeSpan now, int outstanding)
    {
        lock (_lock)
        {
            _changes.Add((now, outstanding));
            Trim(now);
        }
    }

    public double Mean(TimeSpan now, TimeSpan window)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);

        lock (_lock)
        {
            Trim(now);
            var from = now - window;
            var level = _initial;
            var at = from;
            double area = 0;
            foreach (var (when, l) in _changes)
            {
                if (when <= from)
                {
                    level = l;
                    continue;
                }

                area += level * (when - at).TotalSeconds;
                at = when;
                level = l;
            }

            area += level * (now - at).TotalSeconds;
            return area / window.TotalSeconds;
        }
    }

    void Trim(TimeSpan now)
    {
        // Changes before the window collapse into the level entering it.
        var from = now - ScalerRules.OccupancyWindow;
        var drop = 0;
        while (drop < _changes.Count && _changes[drop].At <= from)
        {
            _initial = _changes[drop].Level;
            drop++;
        }

        _changes.RemoveRange(0, drop);
    }
}

/// <summary>
/// Per job type service estimate (capacity-defaults §4): EWMA (α 0.20) of healthy completed ticket durations, a
/// cold 1 s estimate with low confidence until 20 samples, and the longest live healthy ticket as a lower bound.
/// </summary>
internal sealed class ServiceEstimator
{
    readonly Lock _lock = new();
    readonly Dictionary<string, (double Seconds, int Samples)> _types = new(StringComparer.Ordinal);

    public void Completed(string jobType, TimeSpan ticketDuration)
    {
        lock (_lock)
        {
            var seconds = Math.Max(0, ticketDuration.TotalSeconds);
            _types[jobType] = _types.TryGetValue(jobType, out var e)
                ? (e.Seconds + ScalerRules.EwmaAlpha * (seconds - e.Seconds), e.Samples + 1)
                : (seconds, 1);
        }
    }

    public (double Seconds, bool Confident) Estimate(string jobType, TimeSpan? longestActive)
    {
        lock (_lock)
        {
            var (seconds, samples) = _types.TryGetValue(jobType, out var e) ? e : (ScalerRules.ColdEstimate.TotalSeconds, 0);
            if (longestActive is { } active && active.TotalSeconds > seconds)
            {
                seconds = active.TotalSeconds;
            }

            return (seconds, samples >= ScalerRules.ConfidentSamples);
        }
    }
}

/// <summary>Admission outcomes over the last 30 s for the blocked-ratio growth freeze (capacity-defaults §5).</summary>
internal sealed class AdmissionOutcomes
{
    readonly Lock _lock = new();
    readonly Queue<(TimeSpan At, bool Blocked)> _cycles = new();

    public void Record(TimeSpan now, bool blocked)
    {
        lock (_lock)
        {
            _cycles.Enqueue((now, blocked));
            while (_cycles.Count > 0 && now - _cycles.Peek().At > ScalerRules.BlockedRatioWindow)
            {
                _cycles.Dequeue();
            }
        }
    }

    /// <summary>True when ≥ 20 cycles in the window and ≥ 75 % ended blocked or deferred.</summary>
    public bool MostlyBlocked(TimeSpan now)
    {
        lock (_lock)
        {
            var recent = _cycles.Where(c => now - c.At <= ScalerRules.BlockedRatioWindow).ToList();
            return recent.Count >= ScalerRules.BlockedRatioMinimumCycles
                && recent.Count(c => c.Blocked) >= ScalerRules.BlockedRatioFreeze * recent.Count;
        }
    }
}
