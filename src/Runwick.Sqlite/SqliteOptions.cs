namespace Runwick;

/// <summary>SQLite store options (20 §2.1, §10).</summary>
public sealed class SqliteOptions
{
    /// <summary>How long a write waits for another process's write lock, per transaction (100 ms – 30 s).</summary>
    public TimeSpan WriterBusyTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Reader threads, each with its own connection (1 – 8).</summary>
    public int ReaderThreads { get; set; } = 2;
}
