using Microsoft.Data.Sqlite;
using Runwick.Internal;

namespace Runwick.Sqlite.Store;

/// <summary>
/// The store's own threads (20 §5.1): one writer thread with one long-lived connection, taking queued writes highest class
/// first (renewal → settlement → admission → producer → background, FIFO within a class), and <c>ReaderThreads</c> reader
/// threads with one connection each. Microsoft.Data.Sqlite blocks on every call, so no caller or engine loop ever waits
/// inside SQLite; callers get a TaskCompletionSource completed asynchronously.
/// </summary>
internal sealed class SqExecutor : IDisposable
{
    const int MaxDispatchGroupCount = 8;
    const long MaxDispatchGroupBytes = 1024 * 1024;

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
    public Task<T> WriteAsync<T>(SqLane lane, Func<SqliteConnection, T> work, CancellationToken ct, string? operation = null) =>
        Enqueue((int)lane, work, write: true, ct, operation);

    /// <summary>Runs <paramref name="work"/> on a reader thread with that reader's connection.</summary>
    public Task<T> ReadAsync<T>(Func<SqliteConnection, T> work, CancellationToken ct) => Enqueue(0, work, write: false, ct);

    Task<T> Enqueue<T>(int priority, Func<SqliteConnection, T> work, bool write, CancellationToken ct, string? operation = null)
    {
        var item = new Work<T>(work, ct)
        {
            Operation = operation,
            QueuedAt = operation is null ? 0 : PhaseTimings.Start(),
        };
        Enqueue(item, priority, write, ct);
        return item.Task;
    }

    public Task<SqRow> DispatchAsync(SqRunner runner, SqOp op, Func<SqTx, SqRow> body, long bytes, CancellationToken ct)
    {
        var item = new DispatchWork(runner, op, body, bytes, ct) { Operation = op.Operation, QueuedAt = PhaseTimings.Start() };
        Enqueue(item, (int)SqLane.Producer, write: true, ct);
        return item.Task;
    }

    void Enqueue(Work item, int priority, bool write, CancellationToken ct)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_stopped, this);
            if (ct.CanBeCanceled)
            {
                item.Registration = ct.Register(() => CancelQueued(item, write));
            }

            if (!item.IsQueued)
            {
                return;
            }

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
    }

    void CancelQueued(Work item, bool write)
    {
        lock (_gate)
        {
            if (item.TryCancelQueued() && write)
            {
                // A cancelled call releases its lane slot. Remove it so new callers cannot grow a backlog
                // of cancelled entries while the writer is held in a transaction.
                _writes.Remove(item, out _, out _);
            }
        }
    }

    List<DispatchWork>? CollectDispatchGroup(Work first)
    {
        if (first is not DispatchWork head || head.Bytes > MaxDispatchGroupBytes)
        {
            return null;
        }

        List<DispatchWork>? group = null;
        var bytes = head.Bytes;
        lock (_gate)
        {
            // The next queue entry must be compatible. Higher priorities and other producer calls keep their
            // position. There is no coalescing timer or wait for another command.
            while ((group?.Count ?? 1) < MaxDispatchGroupCount
                   && _writes.TryPeek(out var next, out _)
                   && next is DispatchWork candidate
                   && ReferenceEquals(head.Runner, candidate.Runner)
                   && head.Op.Application == candidate.Op.Application && head.Op.ExpectedStoreId == candidate.Op.ExpectedStoreId
                   && candidate.Bytes <= MaxDispatchGroupBytes - bytes)
            {
                _writes.Dequeue();
                candidate.TryStart(); // The queue lock excludes cancellation of this entry.
                (group ??= [head]).Add(candidate);
                bytes += candidate.Bytes;
            }
        }

        return group;
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

            var group = CollectDispatchGroup(work);
            try
            {
                if (work.Operation is { } operation)
                {
                    PhaseTimings.Lap(work.QueuedAt, operation, "writerQueue");
                }

                if (conn is null)
                {
                    conn = SqFile.Open(_connectionString, (int)_options.WriterBusyTimeout.TotalMilliseconds);
                    WriterConnectionsOpened++;
                }

                if (group is null)
                {
                    work.Run(conn);
                }
                else
                {
                    for (var i = 1; i < group.Count; i++)
                    {
                        PhaseTimings.Lap(group[i].QueuedAt, group[i].Op.Operation, "writerQueue");
                    }

                    group[0].Runner.RunDispatchGroup(conn, group);
                }
            }
            catch (Exception ex)
            {
                if (group is null)
                {
                    work.Fail(ex);
                }
                else
                {
                    foreach (var item in group)
                    {
                        item.Fail(ex);
                    }
                }
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

    internal abstract class Work
    {
        int _state; // 0 queued, 1 running, 2 cancelled

        public CancellationTokenRegistration Registration { get; set; }

        public string? Operation { get; init; }

        public long QueuedAt { get; init; }

        /// <summary>The work asks for its connection to be replaced.</summary>
        public bool Broken { get; set; }

        public bool IsQueued => Volatile.Read(ref _state) == 0;

        public bool TryStart() => Interlocked.CompareExchange(ref _state, 1, 0) == 0;

        public bool TryCancelQueued()
        {
            if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
            {
                Cancel();
                return true;
            }

            return false;
        }

        public abstract void Run(SqliteConnection conn);

        public abstract void Fail(Exception ex);

        protected abstract void Cancel();
    }

    internal class Work<T>(Func<SqliteConnection, T> work, CancellationToken ct) : Work
    {
        readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<T> Task => _tcs.Task;

        public override void Run(SqliteConnection conn)
        {
            Registration.Dispose();
            Complete(work(conn));
        }

        public void Complete(T result) => _tcs.TrySetResult(result);

        public override void Fail(Exception ex)
        {
            Registration.Dispose();
            if (ex is SqBrokenConnectionException broken)
            {
                Broken = true;
                ex = broken.InnerException ?? broken;
            }

            _tcs.TrySetException(ex);
        }

        protected override void Cancel() => _tcs.TrySetCanceled(ct);
    }

    internal sealed class DispatchWork(SqRunner runner, SqOp op, Func<SqTx, SqRow> body, long bytes, CancellationToken ct)
        : Work<SqRow>(conn => runner.RunDispatchOnce(conn, op, body, ct), ct)
    {
        public SqRunner Runner { get; } = runner;
        public SqOp Op { get; } = op;
        public Func<SqTx, SqRow> Body { get; } = body;
        public long Bytes { get; } = bytes;
        public CancellationToken Ct { get; } = ct;
    }
}

/// <summary>Thrown by work whose connection must be replaced; the inner exception reaches the caller.</summary>
internal sealed class SqBrokenConnectionException(Exception inner) : Exception("connection broken", inner);
