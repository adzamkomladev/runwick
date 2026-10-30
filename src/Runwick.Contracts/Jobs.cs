namespace Runwick;

/// <summary>Marker for a durable job contract. Payloads are data; they carry no behaviour.</summary>
public interface IQueueJob;

/// <summary>Executes one job contract. One handler per stable job name per worker.</summary>
public interface IJobHandler<in TJob> where TJob : class, IQueueJob
{
    Task HandleAsync(TJob job, CancellationToken cancellationToken);
}
