using Runwick.Registration;

namespace Runwick.Harness.Jobs;

/// <summary>Two rate scopes: job type (<see cref="RateJobs.ReplicaLimit"/> per <see cref="RateJobs.ReplicaWindow"/>) and one generous resource scope.</summary>
public sealed record RatedReplicaJob(string Id, string Resource) : IQueueJob;

/// <summary>Two per-resource rate scopes of limit 1, declared in the order (First, Second).</summary>
public sealed record RatedOrderJob(string Id, string First, string Second) : IQueueJob;

/// <summary>A job-type rate of 1 per <see cref="RateJobs.CrashWindow"/>, with retries.</summary>
public sealed record RatedCrashJob(string Id) : IQueueJob;

/// <summary>A per-resource permit of capacity 1 and a job-type rate of 1 per minute.</summary>
public sealed record RatedPermitJob(string Id, string Resource) : IQueueJob;

/// <summary>Rated test contracts for peer and in-process clients (SP-013 owed cases). Requires <c>rate_admission</c> to dispatch.</summary>
public static class RateJobs
{
    public const int ReplicaLimit = 3;

    public static readonly TimeSpan ReplicaWindow = TimeSpan.FromSeconds(5);

    public static readonly TimeSpan OrderWindow = TimeSpan.FromMinutes(1);

    public static readonly TimeSpan CrashWindow = TimeSpan.FromSeconds(8);

    public static void Register(RunwickBuilder r)
    {
        r.AddJob<RatedReplicaJob>().Named("test.rate.replica").ToQueue("default")
            .RollingWindow(ReplicaLimit, ReplicaWindow).RollingWindowPerResource(j => j.Resource, 1_000, ReplicaWindow);
        r.AddJob<RatedOrderJob>().Named("test.rate.order").ToQueue("default")
            .RollingWindowPerResource(j => j.First, 1, OrderWindow).RollingWindowPerResource(j => j.Second, 1, OrderWindow);
        r.AddJob<RatedCrashJob>().Named("test.rate.crash").ToQueue("default").RollingWindow(1, CrashWindow)
            .WithMaxExecutions(3).WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        r.AddJob<RatedPermitJob>().Named("test.rate.permit").ToQueue("default")
            .PerResource(j => j.Resource, 1).RollingWindow(1, TimeSpan.FromMinutes(1));
    }
}
