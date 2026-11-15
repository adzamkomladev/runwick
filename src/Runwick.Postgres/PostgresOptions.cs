namespace Runwick.Postgres;

/// <summary>PostgreSQL store options.</summary>
public sealed class PostgresOptions
{
    /// <summary>Schema holding Runwick's tables. Only <c>runwick</c> is supported in M2 (04 P-1).</summary>
    public string Schema { get; set; } = "runwick";

    /// <summary>
    /// Opt-in producer grouping (off by default). While one dispatch is in flight, concurrent plain
    /// <c>DispatchAsync</c> calls on this process wait in a bounded queue (at most 8 commands and 2 MiB) and then commit
    /// together in one transaction. A lone caller is sent at once and never waits for company. Every command keeps its own
    /// operation id, receipt, deadline and result, and a command that is not plain (uniqueness, concurrency or rate scopes,
    /// schedule) takes the ordinary path. Calls on a caller's own transaction are never grouped.
    /// </summary>
    public bool GroupProducerAdmission { get; set; }

    /// <summary>Groups in flight at once when <see cref="GroupProducerAdmission"/> is on. Measured, not public.</summary>
    internal int GroupMaxInFlight { get; set; } = 1;
}
