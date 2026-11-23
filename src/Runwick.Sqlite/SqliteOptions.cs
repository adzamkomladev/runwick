namespace Runwick;

/// <summary>SQLite store options (20 §2.1, §10).</summary>
public sealed class SqliteOptions
{
    /// <summary>How long a write waits for another process's write lock, per transaction (100 ms – 30 s).</summary>
    public TimeSpan WriterBusyTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Reader threads, each with its own connection (1 – 8).</summary>
    public int ReaderThreads { get; set; } = 2;

    /// <summary>
    /// Lets writes of the same call class that are already queued share one <c>BEGIN IMMEDIATE</c> … <c>COMMIT</c> (one WAL sync),
    /// each in its own savepoint, with replies after the shared commit (20 §5.1). Off: only <c>Dispatch/1</c> and
    /// <c>ReceiveOutbox/1</c> calls share a commit, at most 8.
    /// </summary>
    public bool GroupCommit { get; set; }
}
