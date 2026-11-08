using Microsoft.Data.Sqlite;

namespace Runwick.Sqlite.Store;

/// <summary>
/// The store's own threads (20 §5.1): one writer thread with one long-lived connection, taking queued writes highest class
/// first (renewal → settlement → admission → producer → background, FIFO within a class), and <c>ReaderThreads</c> reader
/// threads with one connection each. Microsoft.Data.Sqlite blocks on every call, so no caller or engine loop ever waits
/// inside SQLite; callers get a TaskCompletionSource completed asynchronously.
/// </summary>
internal sealed class SqExecutor : IDisposable
{
    readonly string _connectionString;
    readonly SqliteOptions _options;
    readonly object _gate = new();
    readonly PriorityQueue<Work, (int, long)> _writes = new();
    readonly Queue<Work> _reads = new();
    readonly Thread _writer;
    readonly Thread[] _readers;
    readonly Dictionary<SqLane, SemaphoreSlim> _lanes;
    long _sequence;
    bool _stopped;

    public SqExecutor(string connectionString, SqliteOptions options, int producerCalls)
    {
        _connectionString = connectionString;
        _options = options;
        _lanes = new()
        {
            [SqLane.Renewal] = new SemaphoreSlim(4, 4),
            [SqLane.Settlement] = new SemaphoreSlim(4, 4),
            [SqLane.Admission] = new SemaphoreSlim(4, 4),
            [SqLane.Producer] = new SemaphoreSlim(producerCalls, producerCalls),
            [SqLane.Background] = new SemaphoreSlim(2, 2),
        };
        _writer = new Thread(WriterLoop) { IsBackground = true, Name = "runwick-sqlite-writer" };
        _readers = [.. Enumerable.Range(0, options.ReaderThreads).Select(i => new Thread(ReaderLoop) { IsBackground = true, Name = $"runwick-sqlite-reader-{i}" })];
        _writer.Start();
        foreach (var r in _readers)
        {
            r.Start();
        }
    }

    public string ConnectionString => _connectionString;

    /// <summary>Writer connections opened so far (SQL-06: settings are read back after each replacement).</summary>
    public int WriterConnectionsOpened { get; private set; }

    public SemaphoreSlim Gate(SqLane lane) => _lanes[lane];

    /// <summary>Runs <paramref name="work"/> on the writer thread with the writer connection.</summary>
    public Task<T> WriteAsync<T>(SqLane lane, Func<SqliteConnection, T> work, CancellationToken ct) => Enqueue((int)lane, work, write: true, ct);

    /// <summary>Runs <paramref name="work"/> on a reader thread with that reader's connection.</summary>
    public Task<T> ReadAsync<T>(Func<SqliteConnection, T> work, CancellationToken ct) => Enqueue(0, work, write: false, ct);

    Task<T> Enqueue<T>(int priority, Func<SqliteConnection, T> work, bool write, CancellationToken ct)
    {
        var item = new Work<T>(work, ct);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_stopped, this);
            if (write)
            {
                _writes.Enqueue(item, (priority, _sequence++));
            }
            else
            {
                _reads.Enqueue(item);
            }

            Monitor.PulseAll(_gate);
        }

        // A call cancelled while queued is removed: not applied (20 §5.2). Once started it runs to its end.
        if (ct.CanBeCanceled)
        {
            item.Registration = ct.Register(() => item.TryCancelQueued());
        }

        return item.Task;
    }

    void WriterLoop()
    {
        SqliteConnection? conn = null;
        while (Next(write: true) is { } work)
        {
            if (!work.TryStart())
            {
                continue;
            }

            try
            {
                if (conn is null)
                {
                    conn = SqFile.Open(_connectionString, (int)_options.WriterBusyTimeout.TotalMilliseconds);
                    WriterConnectionsOpened++;
                }

                work.Run(conn);
            }
            catch (Exception ex)
            {
                work.Fail(ex);
            }

            // A connection left inside a transaction (a failed ROLLBACK) is replaced (20 §4.3).
            if (conn is not null && (work.Broken || !conn.State.HasFlag(System.Data.ConnectionState.Open) || InTransaction(conn)))
            {
                conn.Dispose();
                conn = null;
            }
        }

        conn?.Dispose();
    }

    void ReaderLoop()
    {
        SqliteConnection? conn = null;
        while (Next(write: false) is { } work)
        {
            if (!work.TryStart())
            {
                continue;
            }

            try
            {
                conn ??= SqFile.Open(_connectionString, 1000);
                work.Run(conn);
            }
            catch (Exception ex)
            {
                work.Fail(ex);
                conn?.Dispose();
                conn = null;
            }

            if (conn is not null && InTransaction(conn))
            {
                conn.Dispose();
                conn = null;
            }
        }

        conn?.Dispose();
    }

    /// <summary>Whether the connection is inside a transaction (autocommit off).</summary>
    internal static bool InTransaction(SqliteConnection conn) =>
        conn.Handle is { } h && SQLitePCL.raw.sqlite3_get_autocommit(h) == 0;

    Work? Next(bool write)
    {
        lock (_gate)
        {
            while (true)
            {
                if (write ? _writes.TryDequeue(out var w, out _) : _reads.TryDequeue(out w))
                {
                    return w;
                }

                if (_stopped)
                {
                    return null;
                }

                Monitor.Wait(_gate);
            }
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _stopped = true;
            Monitor.PulseAll(_gate);
        }

        _writer.Join(TimeSpan.FromSeconds(10));
        foreach (var r in _readers)
        {
            r.Join(TimeSpan.FromSeconds(10));
        }

        foreach (var g in _lanes.Values)
        {
            g.Dispose();
        }
    }

    abstract class Work
    {
        int _state; // 0 queued, 1 running, 2 cancelled

        public CancellationTokenRegistration Registration { get; set; }

        /// <summary>The work asks for its connection to be replaced.</summary>
        public bool Broken { get; set; }

        public bool TryStart() => Interlocked.CompareExchange(ref _state, 1, 0) == 0;

        public void TryCancelQueued()
        {
            if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
            {
                Cancel();
            }
        }

        public abstract void Run(SqliteConnection conn);

        public abstract void Fail(Exception ex);

        protected abstract void Cancel();
    }

    sealed class Work<T>(Func<SqliteConnection, T> work, CancellationToken ct) : Work
    {
        readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<T> Task => _tcs.Task;

        public override void Run(SqliteConnection conn)
        {
            Registration.Dispose();
            try
            {
                _tcs.TrySetResult(work(conn));
            }
            catch (SqBrokenConnectionException ex)
            {
                Broken = true;
                _tcs.TrySetException(ex.InnerException ?? ex);
            }
        }

        public override void Fail(Exception ex) => _tcs.TrySetException(ex);

        protected override void Cancel() => _tcs.TrySetCanceled(ct);
    }
}

/// <summary>Thrown by work whose connection must be replaced; the inner exception reaches the caller.</summary>
internal sealed class SqBrokenConnectionException(Exception inner) : Exception("connection broken", inner);
