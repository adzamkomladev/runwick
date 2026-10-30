using Runwick.Registration;

namespace Runwick.Harness.Jobs;

public sealed record PingJob(string Id) : IQueueJob;

public sealed record KeyedJob(string Id, string Key) : IQueueJob;

public sealed record ResourceJob(string Id, string Resource) : IQueueJob;

/// <summary>Type scope with capacity 1 (<c>MaxConcurrent(1)</c>).</summary>
public sealed record CappedJob(string Id) : IQueueJob;

/// <summary>Two resource scopes of capacity 1, declared in the order (First, Second).</summary>
public sealed record PairJob(string Id, string First, string Second) : IQueueJob;

/// <summary>One resource scope of capacity 2: two running jobs share it.</summary>
public sealed record SharedJob(string Id, string Resource) : IQueueJob;

public static class TestJobs
{
    /// <summary>Registers the shared test contracts on queue <c>default</c>.</summary>
    public static void Register(RunwickBuilder r, int maxExecutions = 3)
    {
        r.AddJob<PingJob>().Named("test.ping").ToQueue("default").WithMaxExecutions(maxExecutions)
            .WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        r.AddJob<KeyedJob>().Named("test.keyed").ToQueue("default").UniqueWhileActive(j => j.Key);
        r.AddJob<ResourceJob>().Named("test.resource").ToQueue("default").PerResource(j => j.Resource, 1);
        r.AddJob<CappedJob>().Named("test.capped").ToQueue("default").MaxConcurrent(1);
        r.AddJob<SharedJob>().Named("test.shared").ToQueue("default").PerResource(j => j.Resource, 2);
        r.AddJob<PairJob>().Named("test.pair").ToQueue("default").PerResource(j => j.First, 1).PerResource(j => j.Second, 1);
    }
}
