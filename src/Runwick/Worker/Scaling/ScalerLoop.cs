using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Worker.Scaling;

namespace Runwick.Worker;

internal sealed partial class WorkerEngine
{
    /// <summary>Healthy completed ticket durations per job type (11 §3.2).</summary>
    public ServiceEstimator Service { get; } = new();

    public long ScalerSamples;

    public long ScalerEstimateFailures;

    /// <summary>
    /// The local scaler (11 §3–§4): every 5 s ± 10 %, one shared bounded demand estimate (1 s deadline, never
    /// overlapping), then one decision per automatic pool. Unknown demand holds every target. Not critical: a
    /// failing scaler freezes targets, it never touches ownership or admission safety.
    /// </summary>
    async Task ScalerLoopAsync()
    {
        var auto = Pools.Where(p => p.Scaler is not null).ToList();
        if (auto.Count == 0)
        {
            return;
        }

        var scaling = Store as IScalingStore;
        var queues = Pools.SelectMany(p => p.Settings.Queues.Select(q => q.Queue)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        while (!ControlStop.IsCancellationRequested && !Draining)
        {
            await Clock.Delay(EngineClock.Uniform(ScalerRules.SampleInterval * (1 - ScalerRules.SampleJitter),
                ScalerRules.SampleInterval * (1 + ScalerRules.SampleJitter)), ControlStop).ConfigureAwait(false);
            if (Draining)
            {
                break;
            }

            DemandEstimate? estimate = null;
            if (scaling is not null)
            {
                try
                {
                    estimate = await CallAsync(BackgroundIo, 0, ScalerRules.EstimateDeadline,
                        t => scaling.EstimateDemandAsync(Application, WorkerInstanceId, queues, Contracts, t), ControlStop).ConfigureAwait(false);
                }
                catch (Exception ex) when (!ControlStop.IsCancellationRequested)
                {
                    // A late or failed estimate is unknown demand, never zero (capacity-defaults §4).
                    Interlocked.Increment(ref ScalerEstimateFailures);
                    Logger.LogDebug(ex, "Runwick demand estimate failed; automatic targets hold.");
                }
            }

            var saturated = Readiness.SaturatedQueues();
            foreach (var pool in auto)
            {
                var decision = pool.Scaler!.Sample(Sample(pool, estimate, saturated));
                pool.LastDecision = decision;
                Interlocked.Increment(ref ScalerSamples);
                if (decision.NewTarget is { } target)
                {
                    pool.Gate.SetTarget(target);
                    pool.Wake.Set();
                    if (scaling is not null)
                    {
                        _ = RecordScalingAsync(scaling, pool, decision, estimate);
                    }
                }
            }
        }
    }

    /// <summary>
    /// The pool's sample inputs (11 §4.2). Each (queue, tuple) backlog is divided among the live workers that both
    /// advertise the tuple and subscribe to the queue, then among this worker's pools on that queue by queue weight.
    /// Queues known to be saturated contribute no demand.
    /// </summary>
    ScalerSample Sample(PoolCoordinator pool, DemandEstimate? estimate, HashSet<string> saturated)
    {
        var now = Clock.Now;
        var freeze = pool.Suspended ? "unresolved_command"
            : PressureStopped ? "pressure"
            : !AdmissionOpen ? "admission_closed"
            : Presence.Failing ? "presence_failing"
            : pool.Outcomes.MostlyBlocked(now) ? "blocked_ratio"
            : null;
        var occupancy = pool.Occupancy.Mean(now, ScalerRules.OccupancyWindow);
        if (estimate is null)
        {
            return new ScalerSample(now, occupancy, null, false, freeze, null, 0);
        }

        double work = 0;
        var positive = false;
        foreach (var (queue, weight) in pool.Settings.Queues)
        {
            if (saturated.Contains(queue))
            {
                continue;
            }

            var siblings = Pools.SelectMany(p => p.Settings.Queues.Where(s => s.Queue == queue)).Sum(s => (double)s.Weight);
            var poolShare = siblings > 0 ? weight / siblings : 0;
            foreach (var row in estimate.Backlog.Where(r => r.Queue == queue && r.Due > 0))
            {
                var (seconds, _) = Service.Estimate(row.Contract.Name, LongestActive(row.Contract.Name));
                work += row.Due * poolShare / Math.Max(1, row.CapableInstances) * seconds;
                positive = true;
            }
        }

        int? overrideTarget = estimate.Overrides.TryGetValue(pool.Settings.Name, out var o) ? o : null;
        return new ScalerSample(now, occupancy, work, positive, freeze, overrideTarget, estimate.MembershipDigest);
    }

    /// <summary>The longest healthy active ticket of a job type: a lower bound on its estimate (capacity-defaults §4).</summary>
    TimeSpan? LongestActive(string jobType)
    {
        var now = Clock.Now;
        TimeSpan? longest = null;
        foreach (var e in _entries.Values)
        {
            if (e.Reserved.Contract.Name == jobType && e.Begun is not null && e.Loss is null && e.Phase != AttemptPhase.Retired)
            {
                var held = now - e.AdmittedAt;
                if (longest is null || held > longest)
                {
                    longest = held;
                }
            }
        }

        return longest;
    }

    async Task RecordScalingAsync(IScalingStore scaling, PoolCoordinator pool, ScalerDecision decision, DemandEstimate? estimate)
    {
        try
        {
            await CallAsync(BackgroundIo, 0, Options.ControlRequestTimeout, t => scaling.RecordScalingEventAsync(Application,
                new ScalingEventRecord(WorkerInstanceId, pool.Settings.Name, decision.Current, decision.NewTarget!.Value, decision.Desired,
                    decision.Candidate, decision.Reason, estimate?.ObservedAt, estimate?.MembershipDigest), t), ControlStop).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ControlStop.IsCancellationRequested)
        {
            // History is diagnostic: a lost row never changes the applied target.
            Logger.LogDebug(ex, "Runwick scaling event for pool {Pool} was not recorded.", pool.Settings.Name);
        }
    }
}
