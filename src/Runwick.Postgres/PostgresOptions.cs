namespace Runwick.Postgres;

/// <summary>PostgreSQL store options.</summary>
public sealed class PostgresOptions
{
    /// <summary>Schema holding Runwick's tables. Only <c>runwick</c> is supported in M2 (04 P-1).</summary>
    public string Schema { get; set; } = "runwick";
}
