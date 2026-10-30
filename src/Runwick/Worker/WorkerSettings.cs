using Runwick.Registration;

namespace Runwick.Worker;

/// <summary>One validated pool: subscriptions with weights, fixed target, lane mode and weights (05 §4, §8).</summary>
internal sealed record PoolSettings(
    string Name,
    IReadOnlyList<(string Queue, int Weight)> Queues,
    int Target,
    LaneSelectionMode Mode,
    (int High, int Medium, int Low) LaneWeights);

/// <summary>
/// The validated worker configuration: timing (06 §3 V-01…V-13) plus pools (V-15, V-20). Validation never
/// adjusts a value; every violation is reported with the rule it breaks.
/// </summary>
internal sealed class WorkerSettings
{
    public const int GroupMaximum = 16;
    public const int DefaultPoolTarget = 4;
    public const int AgingEvery = 8;
    public const int BlockedCandidatesPerLaneVisit = 8;
    public const int ReservesPerRound = 32;
    public const int RecoveryBatch = 32;
    public const int SettlementLane = 2;
    public static readonly TimeSpan AgingThreshold = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan HintMaxAge = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan IdleBackoffBase = TimeSpan.FromMilliseconds(100);
    public static readonly TimeSpan IdleBackoffCap = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan CooldownBase = TimeSpan.FromMilliseconds(250);
    public static readonly TimeSpan CooldownCap = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan PresenceHeartbeat = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan PresenceExpiry = TimeSpan.FromSeconds(20);
    public const double JitterFraction = 0.20;

    WorkerSettings(WorkerOptions options, IReadOnlyList<PoolSettings> pools, int groupMaximum)
    {
        Options = options;
        Pools = pools;
        GroupMax = groupMaximum;
    }

    public WorkerOptions Options { get; }

    public IReadOnlyList<PoolSettings> Pools { get; }

    public int GroupMax { get; }

    public TimeSpan RenewalIntervalMin => Options.RenewalInterval - Options.RenewalJitter;

    public TimeSpan RenewalIntervalMax => Options.RenewalInterval + Options.RenewalJitter;

    public TimeSpan UsableAuthority => Options.LeaseDuration - Options.LeaseSafetyMargin;

    /// <summary>05 §15.1: usableAuthority − RenewalIntervalMax − ControlRequestTimeout − SchedulerAllowance.</summary>
    public TimeSpan StopMargin => UsableAuthority - RenewalIntervalMax - Options.ControlRequestTimeout - Options.SchedulerAllowance;

    public TimeSpan ResumeMargin => StopMargin + Options.ControlRequestTimeout;

    /// <summary>Latest renewal due relative to d: 2·timeout + backoff max + allowance (05 §11.2).</summary>
    public TimeSpan RenewalLead => 2 * Options.ControlRequestTimeout + Options.ControlRetryMaxBackoff + Options.SchedulerAllowance;

    public static WorkerSettings Validate(WorkerBuilder builder, IReadOnlyCollection<string> knownQueues)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new WorkerOptions();
        foreach (var action in builder.OptionActions)
        {
            action(options);
        }

        var errors = new List<ConfigurationError>();
        ValidateTiming(options, errors);
        var groupMax = builder.MaxOutstanding ?? GroupMaximum;
        if (groupMax < 1)
        {
            errors.Add(new(RunwickErrorCodes.InvalidPool, "MaxOutstandingAttempts", "V-15: the worker/store group maximum must be at least 1."));
        }

        var pools = ValidatePools(builder, knownQueues, groupMax, errors);
        return errors.Count > 0 ? throw new RunwickConfigurationException(errors) : new WorkerSettings(options, pools, groupMax);
    }

    /// <summary>06 §3 V-01 … V-13 on the effective options, in whole milliseconds with checked arithmetic.</summary>
    internal static void ValidateTiming(WorkerOptions o, List<ConfigurationError> errors)
    {
        void Fail(string rule, string subject, string message) => errors.Add(new(RunwickErrorCodes.InvalidTiming, subject, $"{rule}: {message}"));

        (string Name, TimeSpan Value, bool ZeroAllowed)[] durations =
        [
            (nameof(o.LeaseDuration), o.LeaseDuration, false),
            (nameof(o.LeaseSafetyMargin), o.LeaseSafetyMargin, false),
            (nameof(o.RenewalInterval), o.RenewalInterval, false),
            (nameof(o.RenewalJitter), o.RenewalJitter, true),
            (nameof(o.LeaseWatchdogInterval), o.LeaseWatchdogInterval, false),
            (nameof(o.SchedulerAllowance), o.SchedulerAllowance, false),
            (nameof(o.ControlRequestTimeout), o.ControlRequestTimeout, false),
            (nameof(o.ControlRetryInitialBackoff), o.ControlRetryInitialBackoff, false),
            (nameof(o.ControlRetryMaxBackoff), o.ControlRetryMaxBackoff, false),
            (nameof(o.MinimumInvocationMargin), o.MinimumInvocationMargin, false),
            (nameof(o.ClaimResolutionBudget), o.ClaimResolutionBudget, false),
            (nameof(o.BeginResolutionBudget), o.BeginResolutionBudget, false),
            (nameof(o.CompletionResolutionBudget), o.CompletionResolutionBudget, false),
            (nameof(o.CancellationGrace), o.CancellationGrace, false),
            (nameof(o.CleanupBudget), o.CleanupBudget, false),
            (nameof(o.OwnershipCommandLifetime), o.OwnershipCommandLifetime, false),
            (nameof(o.RecoveryScanInterval), o.RecoveryScanInterval, false),
            (nameof(o.RecoveryScanJitter), o.RecoveryScanJitter, true),
            (nameof(o.ShutdownBudget), o.ShutdownBudget, false),
            (nameof(o.ShutdownDrain), o.ShutdownDrain, false),
            (nameof(o.ShutdownCancellation), o.ShutdownCancellation, false),
            (nameof(o.ShutdownFinalization), o.ShutdownFinalization, false),
        ];

        var bad = false;
        foreach (var (name, value, zero) in durations)
        {
            if (value < TimeSpan.Zero || (!zero && value == TimeSpan.Zero) || value.Ticks % TimeSpan.TicksPerMillisecond != 0
                || value == TimeSpan.MaxValue || value.TotalMilliseconds > long.MaxValue / 4)
            {
                Fail("V-01", name, $"{value} must be {(zero ? "non-negative" : "positive")}, finite and a whole number of milliseconds.");
                bad = true;
            }
        }

        if (bad)
        {
            return;
        }

        static long Ms(TimeSpan t) => (long)t.TotalMilliseconds;

        try
        {
            checked
            {
                var lease = Ms(o.LeaseDuration);
                var safety = Ms(o.LeaseSafetyMargin);
                var renewMin = Ms(o.RenewalInterval) - Ms(o.RenewalJitter);
                var renewMax = Ms(o.RenewalInterval) + Ms(o.RenewalJitter);
                var timeout = Ms(o.ControlRequestTimeout);
                var backoffMax = Ms(o.ControlRetryMaxBackoff);
                var allowance = Ms(o.SchedulerAllowance);
                var usable = lease - safety;
                if (!(safety > 0 && safety < lease))
                {
                    Fail("V-02", nameof(o.LeaseSafetyMargin), $"0 < LeaseSafetyMargin ({safety} ms) < LeaseDuration ({lease} ms) is required.");
                }

                if (!(renewMin > 0 && renewMin <= renewMax))
                {
                    Fail("V-03", nameof(o.RenewalJitter), $"0 < RenewalIntervalMin ({renewMin} ms) ≤ RenewalIntervalMax ({renewMax} ms) is required.");
                }

                var renewalPath = renewMax + 2 * timeout + backoffMax + allowance;
                if (!(renewalPath < usable))
                {
                    Fail("V-04", nameof(o.LeaseDuration),
                        $"RenewalIntervalMax + 2·ControlRequestTimeout + ControlRetryMaxBackoff + SchedulerAllowance ({renewalPath} ms) must be < LeaseDuration − LeaseSafetyMargin ({usable} ms).");
                }

                var lead = 2 * timeout + backoffMax + allowance;
                if (!(Ms(o.MinimumInvocationMargin) >= lead))
                {
                    Fail("V-05", nameof(o.MinimumInvocationMargin),
                        $"MinimumInvocationMargin ({Ms(o.MinimumInvocationMargin)} ms) must be ≥ 2·ControlRequestTimeout + ControlRetryMaxBackoff + SchedulerAllowance ({lead} ms).");
                }

                var stop = usable - renewMax - timeout - allowance;
                if (!(stop > 0))
                {
                    Fail("V-06", "StopMargin", $"StopMargin = (LeaseDuration − LeaseSafetyMargin) − RenewalIntervalMax − ControlRequestTimeout − SchedulerAllowance = {stop} ms must be > 0.");
                }

                if (!(o.LeaseWatchdogInterval <= TimeSpan.FromSeconds(1)))
                {
                    Fail("V-07", nameof(o.LeaseWatchdogInterval), $"0 < LeaseWatchdogInterval ({Ms(o.LeaseWatchdogInterval)} ms) ≤ 1 s is required.");
                }

                if (!(o.ControlRetryInitialBackoff <= o.ControlRetryMaxBackoff))
                {
                    Fail("V-08", nameof(o.ControlRetryInitialBackoff), "0 < ControlRetryInitialBackoff ≤ ControlRetryMaxBackoff is required.");
                }

                if (!(o.ControlRequestTimeout < o.OwnershipCommandLifetime))
                {
                    Fail("V-09", nameof(o.ControlRequestTimeout), "ControlRequestTimeout must be < OwnershipCommandLifetime.");
                }

                var slices = Ms(o.ShutdownDrain) + Ms(o.ShutdownCancellation) + Ms(o.ShutdownFinalization);
                if (!(slices <= Ms(o.ShutdownBudget)))
                {
                    Fail("V-10", nameof(o.ShutdownBudget),
                        $"ShutdownDrain + ShutdownCancellation + ShutdownFinalization ({slices} ms) must be ≤ ShutdownBudget ({Ms(o.ShutdownBudget)} ms).");
                }

                foreach (var (name, budget) in new[]
                {
                    (nameof(o.ClaimResolutionBudget), o.ClaimResolutionBudget),
                    (nameof(o.BeginResolutionBudget), o.BeginResolutionBudget),
                    (nameof(o.CompletionResolutionBudget), o.CompletionResolutionBudget),
                })
                {
                    if (budget < o.ControlRequestTimeout)
                    {
                        Fail("V-12", name, $"{name} ({Ms(budget)} ms) must be ≥ ControlRequestTimeout ({timeout} ms).");
                    }
                }

                if (!(o.RecoveryScanJitter < o.RecoveryScanInterval))
                {
                    Fail("V-13", nameof(o.RecoveryScanJitter), "0 < RecoveryScanMin ≤ RecoveryScanNominal ≤ RecoveryScanMax is required.");
                }
            }
        }
        catch (OverflowException)
        {
            Fail("V-01", nameof(WorkerOptions), "The timing relationships overflow signed 64-bit milliseconds.");
        }
    }

    static List<PoolSettings> ValidatePools(WorkerBuilder builder, IReadOnlyCollection<string> knownQueues, int groupMax, List<ConfigurationError> errors)
    {
        var pools = new List<PoolSettings>();
        if (builder.Pools.Count == 0)
        {
            errors.Add(new(RunwickErrorCodes.WorkerWithoutPool, "worker", "A worker needs at least one Pool(...)."));
        }

        foreach (var dup in builder.Pools.GroupBy(p => p.Name, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            errors.Add(new(RunwickErrorCodes.DuplicatePool, dup.Key, "Pool names must be unique."));
        }

        foreach (var p in builder.Pools)
        {
            if (p.Auto is not null)
            {
                errors.Add(new(RunwickErrorCodes.FeatureNotAvailable, p.Name, "V-15: AutoConcurrency is not available in this release; use FixedConcurrency(n)."));
            }

            if (p.Subscriptions.Count == 0)
            {
                errors.Add(new(RunwickErrorCodes.PoolWithoutQueue, p.Name, "V-15: a pool subscribes at least one queue."));
            }

            foreach (var (queue, weight) in p.Subscriptions)
            {
                if (weight is < 1 or > 1000)
                {
                    errors.Add(new(RunwickErrorCodes.InvalidPool, p.Name, $"V-15: queue '{queue}' weight {weight} must be an integer in [1, 1000]."));
                }

                if (!knownQueues.Contains(queue))
                {
                    errors.Add(new(RunwickErrorCodes.InvalidPool, p.Name, $"Queue '{queue}' is not registered by any job or Queue(...) call."));
                }
            }

            foreach (var dup in p.Subscriptions.GroupBy(s => s.Queue, StringComparer.Ordinal).Where(g => g.Count() > 1))
            {
                errors.Add(new(RunwickErrorCodes.InvalidPool, p.Name, $"Queue '{dup.Key}' is subscribed twice."));
            }

            var target = p.Fixed ?? DefaultPoolTarget;
            if (target < 1)
            {
                errors.Add(new(RunwickErrorCodes.InvalidPool, p.Name, $"V-15: FixedConcurrency({target}) must be at least 1."));
            }

            if (p.Weights is not { High: > 0, Medium: > 0, Low: > 0 })
            {
                errors.Add(new(RunwickErrorCodes.InvalidPool, p.Name, "V-20: lane weights must be positive integers."));
            }

            pools.Add(new PoolSettings(p.Name, [.. p.Subscriptions], target, p.Mode, p.Weights));
        }

        var sum = pools.Sum(p => (long)p.Target);
        if (sum > groupMax)
        {
            errors.Add(new(RunwickErrorCodes.PoolBudgetExceeded, "worker", $"V-15: the sum of pool maxima ({sum}) exceeds the group maximum ({groupMax})."));
        }

        return pools;
    }
}
