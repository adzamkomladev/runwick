using Runwick;

namespace Runwick.Harness.Timing;

/// <summary>Test timing profiles (07 §2.7).</summary>
public static class TestTiming
{
    /// <summary>Every worker/control default divided by 10.</summary>
    public static void Fast(WorkerOptions o)
    {
        ArgumentNullException.ThrowIfNull(o);
        var d = new WorkerOptions();
        o.LeaseDuration = d.LeaseDuration / 10;
        o.LeaseSafetyMargin = d.LeaseSafetyMargin / 10;
        o.RenewalInterval = d.RenewalInterval / 10;
        o.RenewalJitter = d.RenewalJitter / 10;
        o.LeaseWatchdogInterval = d.LeaseWatchdogInterval / 10;
        o.SchedulerAllowance = d.SchedulerAllowance / 10;
        o.ControlRequestTimeout = d.ControlRequestTimeout / 10;
        o.ControlRetryInitialBackoff = d.ControlRetryInitialBackoff / 10;
        o.ControlRetryMaxBackoff = d.ControlRetryMaxBackoff / 10;
        o.MinimumInvocationMargin = d.MinimumInvocationMargin / 10;
        o.ClaimResolutionBudget = d.ClaimResolutionBudget / 10;
        o.BeginResolutionBudget = d.BeginResolutionBudget / 10;
        o.CompletionResolutionBudget = d.CompletionResolutionBudget / 10;
        o.CancellationGrace = d.CancellationGrace / 10;
        o.CleanupBudget = d.CleanupBudget / 10;
        o.OwnershipCommandLifetime = d.OwnershipCommandLifetime / 10;
        o.RecoveryScanInterval = d.RecoveryScanInterval / 10;
        o.RecoveryScanJitter = d.RecoveryScanJitter / 10;
        o.ShutdownBudget = d.ShutdownBudget / 10;
        o.ShutdownDrain = d.ShutdownDrain / 10;
        o.ShutdownCancellation = d.ShutdownCancellation / 10;
        o.ShutdownFinalization = d.ShutdownFinalization / 10;
    }
}
