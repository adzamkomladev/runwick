using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

/// <summary>
/// Opt-in producer grouping for plain Dispatch/1 (PF2-B1, <see cref="PostgresOptions.GroupProducerAdmission"/>). It is a
/// group commit: a caller that finds nothing in flight sends its own command at once, and callers that arrive while a
/// send is in flight wait in a bounded queue and leave together as one transaction when a slot frees. No timer holds
/// anyone back, so a lone caller pays nothing and the 2 ms collection allowance of the design is never spent.
/// Every command keeps its own receipt, deadline and result. A caller that stops waiting leaves its queue entry (nothing
/// was sent) or abandons a send that carries on and settles its own receipt, so the caller can resolve the operation id.
/// </summary>
internal sealed class PgDispatchGroup(int maxInFlight) : IAsyncDisposable
{
    internal const int MaxItems = 8;

    /// <summary>Retained bytes the queue may hold: the 2 MiB prepared envelope of the design.</summary>
    internal const long MaxBytes = 2 * 1024 * 1024;

    /// <summary>Per-command allowance for headers, fingerprints, policy and transport copies beyond the payload.</summary>
    const int Overhead = 2048;

    readonly object _gate = new();
    readonly List<Entry> _queue = [];
    readonly CancellationTokenSource _shutdown = new();
    readonly HashSet<Task> _running = [];
    long _queuedBytes;
    int _inFlight;
    bool _disposed;

    /// <summary>Groups formed and commands carried by them (diagnostics and tests).</summary>
    long _groups;
    long _grouped;
    long _fallbacks;

    public long Groups => Interlocked.Read(ref _groups);

    public long GroupedCommands => Interlocked.Read(ref _grouped);

    public long Fallbacks => Interlocked.Read(ref _fallbacks);

    /// <summary>Commands waiting for a slot.</summary>
    public int Queued
    {
        get
        {
            lock (_gate)
            {
                return _queue.Count;
            }
        }
    }

    /// <summary>A command is plain when it names no uniqueness key, scope, rate or schedule: the group function's only shape.</summary>
    internal static bool IsPlain(StoreCommand<DispatchRequest> command)
    {
        var item = command.Body.Item;
        var intent = item.Intent;
        return command.Body.CommandFingerprint is { Length: > 0 }
            && item.Scopes.Count == 0 && item.Rates.Count == 0 && item.Schedule is null
            && intent.ActiveKey is null && intent.WindowKey is null && intent.RateScopes.Count == 0
            && Cost(command) <= MaxBytes;
    }

    static long Cost(StoreCommand<DispatchRequest> command) => command.Body.Item.Intent.PayloadUtf8.Length + Overhead;

    public async Task<StoreReply<DispatchResult>> DispatchAsync(PgStore store, StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var entry = new Entry(command, Cost(command)) { Owner = this };
        var lone = false;
        var overflow = false;
        lock (_gate)
        {
            if (_disposed)
            {
                throw new StoreNotAppliedException("store_unavailable");
            }

            if (_inFlight < maxInFlight && _queue.Count == 0)
            {
                _inFlight++;
                lone = true;
            }
            else if (_queue.Count < MaxItems && _queuedBytes + entry.Bytes <= MaxBytes)
            {
                _queue.Add(entry);
                _queuedBytes += entry.Bytes;
            }
            else
            {
                overflow = true; // never an unbounded queue: the ordinary path waits on the producer lane instead
            }
        }

        if (overflow)
        {
            return await store.DispatchSingleAsync(command, ct).ConfigureAwait(false);
        }

        if (lone)
        {
            try
            {
                return await store.DispatchSingleAsync(command, ct).ConfigureAwait(false);
            }
            finally
            {
                SlotFree(store);
            }
        }

        return await WaitAsync(entry, ct).ConfigureAwait(false);
    }

    static async Task<StoreReply<DispatchResult>> WaitAsync(Entry entry, CancellationToken ct)
    {
        // The send may settle after its caller left; its failure is then nobody's to observe.
        _ = entry.Completion.Task.ContinueWith(static t => t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
        using var registration = ct.Register(static state => ((Entry)state!).Abandon(), entry);
        try
        {
            return await entry.Completion.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested && !entry.Submitted)
        {
            throw new OperationCanceledException("Cancelled before the command was sent; nothing was applied.", ct);
        }
        catch (EntryAbandonedException)
        {
            // The command was already in a transaction: the outcome is unknown to this caller and stays recoverable by
            // its operation id (the dispatcher turns this into DispatchCanceledAfterSendException).
            throw new StoreOutcomeUnknownException("commit_outcome_unknown", entry.Command.CommandId, Operations.Dispatch);
        }
    }

    /// <summary>A slot freed: send whatever queued up as one group (or one command).</summary>
    void SlotFree(PgStore store)
    {
        List<Entry>? batch = null;
        lock (_gate)
        {
            _inFlight--;
            if (!_disposed)
            {
                batch = Take();
                if (batch is not null)
                {
                    _inFlight++;
                    var task = Task.Run(() => RunAsync(store, batch));
                    _running.Add(task);
                    _ = task.ContinueWith(t => { lock (_gate) { _running.Remove(t); } }, CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                }
            }
        }
    }

    /// <summary>Takes the oldest entry and up to seven compatible ones: same application and store, distinct command and job ids.</summary>
    List<Entry>? Take()
    {
        if (_queue.Count == 0)
        {
            return null;
        }

        var first = _queue[0].Command;
        var batch = new List<Entry>(MaxItems);
        var commands = new HashSet<Guid>();
        var jobs = new HashSet<Guid>();
        for (var i = 0; i < _queue.Count && batch.Count < MaxItems; i++)
        {
            var e = _queue[i];
            var c = e.Command;
            if (c.Application == first.Application && c.Body.StoreId == first.Body.StoreId
                && commands.Add(c.CommandId) && jobs.Add(c.Body.Item.Intent.ProposedJobId))
            {
                batch.Add(e);
            }
        }

        foreach (var e in batch)
        {
            _queue.Remove(e);
            _queuedBytes -= e.Bytes;
            e.Submitted = true;
        }

        return batch;
    }

    async Task RunAsync(PgStore store, List<Entry> batch)
    {
        try
        {
            if (batch.Count == 1)
            {
                await RunSingleAsync(store, batch[0]).ConfigureAwait(false);
                return;
            }

            Interlocked.Increment(ref _groups);
            Interlocked.Add(ref _grouped, batch.Count);
            var result = await store.DispatchGroupAsync([.. batch.Select(e => e.Command)], _shutdown.Token).ConfigureAwait(false);
            if (result.Rows is { } rows)
            {
                for (var i = 0; i < batch.Count; i++)
                {
                    var reply = PgStore.MapGroupRow(rows[i]);
                    store.WakeIfDue(batch[i].Command, reply);
                    batch[i].Completion.TrySetResult(reply);
                }
            }
            else if (result.CommitUnknown)
            {
                foreach (var e in batch)
                {
                    e.Completion.TrySetException(new StoreOutcomeUnknownException("commit_outcome_unknown", e.Command.CommandId, Operations.Dispatch, result.Failure));
                }
            }
            else
            {
                // Nothing committed: each command goes alone, with its own identity and the ordinary retries and receipts.
                Interlocked.Add(ref _fallbacks, batch.Count);
                await Task.WhenAll(batch.Select(e => RunSingleAsync(store, e))).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            foreach (var e in batch)
            {
                e.Completion.TrySetException(ex);
            }
        }
        finally
        {
            SlotFree(store);
        }
    }

    async Task RunSingleAsync(PgStore store, Entry entry)
    {
        try
        {
            entry.Completion.TrySetResult(await store.DispatchSingleAsync(entry.Command, _shutdown.Token).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            entry.Completion.TrySetException(ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Task[] running;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            foreach (var e in _queue)
            {
                e.Completion.TrySetException(new StoreNotAppliedException("store_unavailable"));
            }

            _queue.Clear();
            _queuedBytes = 0;
            running = [.. _running];
        }

        // Sent work drains inside its own statement budget; past 10 s it is cancelled and its commands resolve by id.
        var drained = Task.WhenAll(running);
        if (await Task.WhenAny(drained, Task.Delay(TimeSpan.FromSeconds(10))).ConfigureAwait(false) != drained)
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
            await Task.WhenAny(drained, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
        }

        _shutdown.Dispose();
    }

    sealed class EntryAbandonedException : Exception;

    sealed class Entry(StoreCommand<DispatchRequest> command, long bytes)
    {
        public StoreCommand<DispatchRequest> Command => command;

        public long Bytes => bytes;

        /// <summary>True once the entry left the queue for a transaction; written under the group lock.</summary>
        public volatile bool Submitted;

        public TaskCompletionSource<StoreReply<DispatchResult>> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PgDispatchGroup? Owner { get; set; }

        /// <summary>The caller stopped waiting: out of the queue if it never left, otherwise abandon the send.</summary>
        public void Abandon()
        {
            if (Owner is { } owner && owner.Unqueue(this))
            {
                Completion.TrySetCanceled();
            }
            else
            {
                Completion.TrySetException(new EntryAbandonedException());
            }
        }
    }

    bool Unqueue(Entry entry)
    {
        lock (_gate)
        {
            if (!_queue.Remove(entry))
            {
                return false;
            }

            _queuedBytes -= entry.Bytes;
            return true;
        }
    }
}
