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
    const int MaxGroupCount = 32;
    const long MaxGroupBytes = 1024 * 1024;
    static readonly long GroupBudgetTicks = System.Diagnostics.Stopwatch.Frequency / 40; // 25 ms of statements per shared commit

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

    /// <summary>Writes waiting in the writer queue (tests wait on it).</summary>
    internal int QueuedWrites
    {
        get
        {
            lock (_gate)
            {
                return _writes.Count;
            }
        }
    }

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

    /// <summary>A runner-built write that may share one transaction with compatible queued writes (20 §5.1 group commit).</summary>
    public Task<T> GroupableAsync<T>(GroupWork<T> item)
    {
        Enqueue(item, (int)item.Op.Lane, write: true, item.Ct);
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
                item.Key = (priority, _sequence++);
                _writes.Enqueue(item, item.Key);
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

    /// <summary>
    /// The queued writes that may share <paramref name="first"/>'s transaction, in queue order. They leave the queue but are
    /// not started, so a cancellation still wins until the group reaches each one. Collection waits zero time and stops at the
    /// first entry that does not fit; nothing is skipped over.
    /// </summary>
    List<GroupWork>? CollectGroup(Work first)
    {
        var all = _options.GroupCommit;
        if (first is not GroupWork head || head.Bytes > MaxGroupBytes || (!all && !head.IsDispatch))
        {
            return null;
        }

        List<GroupWork>? group = null;
        var bytes = head.Bytes;
        lock (_gate)
        {
            // Off: producer calls of one application and store identity. On: any runner write of the head's own call class,
            // so a lower class never joins a higher class's commit and a higher class is never queued behind one.
            while ((group?.Count ?? 1) < (all ? MaxGroupCount : MaxDispatchGroupCount)
                   && _writes.TryPeek(out var next, out var key)
                   && next is GroupWork candidate
                   && ReferenceEquals(head.Runner, candidate.Runner)
                   && (all
                       ? key.Item1 == head.Key.Item1
                       : candidate.IsDispatch && head.Op.Application == candidate.Op.Application
                         && head.Op.ExpectedStoreId == candidate.Op.ExpectedStoreId)
                   && candidate.Bytes <= MaxGroupBytes - bytes)
            {
                _writes.Dequeue();
                (group ??= [head]).Add(candidate);
                bytes += candidate.Bytes;
            }
        }

        return group;
    }

    /// <summary>
    /// Hands the group's writes to the runner one at a time. It stops early when a higher call class is waiting or the group
    /// has used its time budget, and puts the writes it did not start back in their old queue places, so a renewal waits for
    /// at most the write in progress and the shared commit.
    /// </summary>
    void RunGroup(SqliteConnection conn, List<GroupWork> group)
    {
        var started = new List<GroupWork>(group.Count);
        var position = 0;
        var since = System.Diagnostics.Stopwatch.GetTimestamp();
        GroupWork? Take()
        {
            while (position < group.Count)
            {
                if (position > 0 && StopGroup(group[0].Key.Item1, since))
                {
                    return null;
                }

                var item = group[position++];
                if (position == 1 || item.TryStart())
                {
                    if (item.Operation is { } operation)
                    {
                        PhaseTimings.Lap(item.QueuedAt, operation, "writerQueue");
                    }

                    started.Add(item);
                    return item;
                }
            }

            return null;
        }

        try
        {
            group[0].Runner.RunGroup(conn, Take);
        }
        catch (Exception ex)
        {
            foreach (var item in started)
            {
                item.Fail(ex);
            }
        }
        finally
        {
            Requeue(group.Skip(position));
        }

        if (started.Any(item => item.Broken))
        {
            group[0].Broken = true;
        }
    }

    void Requeue(IEnumerable<GroupWork> items)
    {
        lock (_gate)
        {
            foreach (var item in items.Where(item => item.IsQueued))
            {
                _writes.Enqueue(item, item.Key);
            }
        }
    }

    bool StopGroup(int priority, long since)
    {
        if (System.Diagnostics.Stopwatch.GetTimestamp() - since > GroupBudgetTicks)
        {
            return true;
        }

        lock (_gate)
        {
            return _writes.TryPeek(out _, out var key) && key.Item1 < priority;
        }
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

            var group = CollectGroup(work);
            try
            {
                if (conn is null)
                {
                    conn = SqFile.Open(_connectionString, (int)_options.WriterBusyTimeout.TotalMilliseconds);
                    WriterConnectionsOpened++;
                }

                if (group is null)
                {
                    if (work.Operation is { } operation)
                    {
                        PhaseTimings.Lap(work.QueuedAt, operation, "writerQueue");
                    }

                    work.Run(conn);
                }
                else
                {
                    RunGroup(conn, group);
                }
            }
            catch (Exception ex)
            {
                // Only opening the connection or a lone write gets here; the rest of a group is still unstarted.
                work.Fail(ex);
                if (group is not null)
                {
                    Requeue(group.Skip(1));
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

        /// <summary>The queue position (call class, arrival), kept so a group can put back the writes it did not start.</summary>
        public (int, long) Key { get; set; }

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

    /// <summary>A runner-built write: alone it is one transaction; in a group it runs under its own savepoint (20 §5.1).</summary>
    internal abstract class GroupWork(SqRunner runner, SqOp op, long bytes, bool dispatch, CancellationToken ct) : Work
    {
        public SqRunner Runner { get; } = runner;
        public SqOp Op { get; } = op;
        public long Bytes { get; } = bytes;
        public bool IsDispatch { get; } = dispatch;
        public CancellationToken Ct { get; } = ct;

        /// <summary>Its savepoint was released into the shared transaction.</summary>
        public abstract bool Committed { get; }

        /// <summary>Runs the write under its own savepoint of the open shared transaction and keeps the result.</summary>
        public abstract void Step(SqliteConnection conn);

        /// <summary>After the shared COMMIT, or the rollback of a group with nothing to commit: delivers the kept result.</summary>
        public abstract void Reply();

        /// <summary>The shared transaction was refused before any write ran (20 §4.3 mapping).</summary>
        public abstract void Reject(Protocol.StoreStatus status, string code);
    }

    internal sealed class GroupWork<T>(SqRunner runner, SqOp op, Func<SqTx, (T Result, bool Commits)> step,
        Func<Protocol.StoreStatus, string, T> noEffect, long bytes, bool dispatch, CancellationToken ct)
        : GroupWork(runner, op, bytes, dispatch, ct)
    {
        readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        T? _result;
        SqTx? _ready;
        Exception? _error;

        public Task<T> Task => _tcs.Task;

        public override bool Committed => _ready is not null;

        public override void Run(SqliteConnection conn)
        {
            Registration.Dispose();
            _tcs.TrySetResult(Runner.RunSingle(conn, Op, step, noEffect, Ct));
        }

        public override void Step(SqliteConnection conn) => (_result, _ready, _error) = Runner.RunSavepoint(conn, Op, step, noEffect, Ct);

        public override void Reply()
        {
            Registration.Dispose();
            if (_error is not null)
            {
                _tcs.TrySetException(_error);
                return;
            }

            try
            {
                if (_ready is { } tx)
                {
                    foreach (var after in tx.AfterCommit)
                    {
                        after();
                    }

                    tx.Fault(Protocol.FaultPoint.AfterCommitBeforeReply);
                }

                _tcs.TrySetResult(_result!);
            }
            catch (Exception ex)
            {
                _tcs.TrySetException(new Runwick.Protocol.StoreOutcomeUnknownException("commit_outcome_unknown", Op.CommandId, Op.Operation, ex));
            }
        }

        public override void Reject(Protocol.StoreStatus status, string code)
        {
            Registration.Dispose();
            _tcs.TrySetResult(noEffect(status, code));
        }

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

        protected override void Cancel() => _tcs.TrySetCanceled(Ct);
    }
}

/// <summary>Thrown by work whose connection must be replaced; the inner exception reaches the caller.</summary>
internal sealed class SqBrokenConnectionException(Exception inner) : Exception("connection broken", inner);
