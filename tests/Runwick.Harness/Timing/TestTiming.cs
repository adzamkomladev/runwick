using Runwick;

namespace Runwick.Harness.Timing;

/// <summary>Test timing profiles (07 §2.7).</summary>
public static class TestTiming
{
    /// <summary>Every worker/control default divided by 10.</summary>
    public static void Fast(WorkerOptions o) => Scaled(o, 10);

    /// <summary>
    /// The divisor real-store worker tests use: <c>RUNWICK_TEST_TIMING_DIVISOR</c>, default 4. Fast (÷10) budgets a
    /// 500 ms control timeout, below the multi-second store stalls of a shared, loaded host (07 §2.7 note, 2026-10-30).
    /// </summary>
    public static int RealStoreDivisor =>
        int.TryParse(Environment.GetEnvironmentVariable("RUNWICK_TEST_TIMING_DIVISOR"), out var d) && d is 1 or 2 or 4 or 5 or 10 ? d : 4;

    /// <summary>
    /// Every worker/control default divided by <paramref name="divisor"/> (1, 2, 4, 5 or 10: the divisors that keep every
    /// default a whole number of milliseconds); all 06 §3 relationships scale linearly.
    /// </summary>
    public static void Scaled(WorkerOptions o, int divisor)
    {
        ArgumentNullException.ThrowIfNull(o);
        if (divisor is not (1 or 2 or 4 or 5 or 10))
        {
            throw new ArgumentOutOfRangeException(nameof(divisor), divisor, "Use 1, 2, 4, 5 or 10.");
        }

        var d = new WorkerOptions();
        o.LeaseDuration = d.LeaseDuration / divisor;
        o.LeaseSafetyMargin = d.LeaseSafetyMargin / divisor;
        o.RenewalInterval = d.RenewalInterval / divisor;
        o.RenewalJitter = d.RenewalJitter / divisor;
        o.LeaseWatchdogInterval = d.LeaseWatchdogInterval / divisor;
        o.SchedulerAllowance = d.SchedulerAllowance / divisor;
        o.ControlRequestTimeout = d.ControlRequestTimeout / divisor;
        o.ControlRetryInitialBackoff = d.ControlRetryInitialBackoff / divisor;
        o.ControlRetryMaxBackoff = d.ControlRetryMaxBackoff / divisor;
        o.MinimumInvocationMargin = d.MinimumInvocationMargin / divisor;
        o.ClaimResolutionBudget = d.ClaimResolutionBudget / divisor;
        o.BeginResolutionBudget = d.BeginResolutionBudget / divisor;
        o.CompletionResolutionBudget = d.CompletionResolutionBudget / divisor;
        o.CancellationGrace = d.CancellationGrace / divisor;
        o.CleanupBudget = d.CleanupBudget / divisor;
        o.OwnershipCommandLifetime = d.OwnershipCommandLifetime / divisor;
        o.RecoveryScanInterval = d.RecoveryScanInterval / divisor;
        o.RecoveryScanJitter = d.RecoveryScanJitter / divisor;
        o.ShutdownBudget = d.ShutdownBudget / divisor;
        o.ShutdownDrain = d.ShutdownDrain / divisor;
        o.ShutdownCancellation = d.ShutdownCancellation / divisor;
        o.ShutdownFinalization = d.ShutdownFinalization / divisor;
    }
}
