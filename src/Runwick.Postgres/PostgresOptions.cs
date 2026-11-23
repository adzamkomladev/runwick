namespace Runwick.Postgres;

/// <summary>PostgreSQL store options.</summary>
public sealed class PostgresOptions
{
    /// <summary>Schema holding Runwick's tables. Only <c>runwick</c> is supported in M2 (04 P-1).</summary>
    public string Schema { get; set; } = "runwick";

    /// <summary>
    /// Grouped producer admission (on by default; set to <c>false</c> to send every call alone). When every producer slot (<c>ProducerOptions.MaxConcurrentCalls</c>) is
    /// busy, plain <c>DispatchAsync</c> calls that would wait for a slot anyway are sent together: the next free slot carries up
    /// to 8 of them (at most 2 MiB) in one transaction. A caller that finds a slot free is sent at once, as without grouping,
    /// and nobody waits to collect company. Every call keeps its own operation ID, receipt, deadline and result. Calls with a
    /// uniqueness key, concurrency or rate scopes, batches and calls on your own transaction are never grouped.
    /// </summary>
    public bool GroupProducerAdmission { get; set; } = true;
}
