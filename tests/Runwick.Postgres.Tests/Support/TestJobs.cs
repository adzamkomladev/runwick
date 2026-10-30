using Runwick.Registration;

namespace Runwick.Postgres.Tests.Support;

public sealed record PingJob(string Id) : IQueueJob;

public sealed record KeyedJob(string Id, string Key) : IQueueJob;

public sealed record ResourceJob(string Id, string Resource) : IQueueJob;

internal static class TestJobs
{
    /// <summary>Registers the shared test contracts on queue <c>default</c>.</summary>
    public static void Register(RunwickBuilder r, int maxExecutions = 3)
    {
        r.AddJob<PingJob>().Named("test.ping").ToQueue("default").WithMaxExecutions(maxExecutions)
            .WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        r.AddJob<KeyedJob>().Named("test.keyed").ToQueue("default").UniqueWhileActive(j => j.Key);
        r.AddJob<ResourceJob>().Named("test.resource").ToQueue("default").PerResource(j => j.Resource, 1);
    }
}
