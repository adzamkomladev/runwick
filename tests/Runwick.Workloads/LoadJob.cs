namespace Runwick.Workloads;

/// <summary>Workload job: <see cref="Pad"/> sizes the payload (≈ 1 KiB for P-01, 07 §14.3).</summary>
public sealed record LoadJob(string Id, string Pad) : IQueueJob
{
    public static LoadJob Create(long sequence, int payloadBytes) =>
        new(sequence.ToString(System.Globalization.CultureInfo.InvariantCulture), new string('x', Math.Max(0, payloadBytes - 32)));
}

/// <summary>Handler duration for this worker process (0 = no-op).</summary>
public sealed record LoadBehaviour(TimeSpan Delay);

internal sealed class LoadHandler(LoadBehaviour behaviour) : IJobHandler<LoadJob>
{
    public Task HandleAsync(LoadJob job, CancellationToken cancellationToken) =>
        behaviour.Delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(behaviour.Delay, cancellationToken);
}

internal static class LoadRegistration
{
    public const string Queue = "load";
    public const string JobName = "wl.load";

    public static void Register(RunwickBuilder r, int? typeCeiling)
    {
        var job = r.AddJob<LoadJob>().Named(JobName).ToQueue(Queue).WithMaxExecutions(3);
        if (typeCeiling is { } ceiling)
        {
            job.MaxConcurrent(ceiling);
        }
    }
}
