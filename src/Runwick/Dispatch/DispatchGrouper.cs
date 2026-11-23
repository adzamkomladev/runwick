using Runwick.Protocol;

namespace Runwick.Dispatching;

/// <summary>
/// Grouped producer admission (PF2-B1, opt-in per store). Only callers that would wait for a local admission slot anyway are
/// grouped. A caller that finds a slot free sends at once, taking along anyone already queued (normally no one), so a lone
/// caller, or a producer with fewer callers than slots, runs exactly as without grouping and no collection wait is ever
/// added. A caller that finds every slot busy queues and waits for a slot as before, under the same waiting bound and wait
/// time. Whichever caller gets a slot next takes up to <see cref="IGroupedDispatchStore.MaxGroupItems"/> queued commands
/// with it in one transaction, and those callers stop waiting for a slot. Each slot carries at most one transaction at a
/// time, as before, and stays taken until that transaction ends, even when the caller that took it stops waiting.
/// </summary>
internal sealed class DispatchGrouper(LocalAdmission admission, IGroupedDispatchStore store)
{
    readonly object _gate = new();
    readonly LinkedList<Entry> _queue = [];

    public bool CanGroup(StoreCommand<DispatchRequest> command) => store.CanGroup(command);

    /// <summary>The command's reply, or null when local admission refused it (nothing was sent).</summary>
    public async Task<StoreReply<DispatchResult>?> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        if (admission.TryEnterNow() is { } free)
        {
            return await LeadAsync(new Entry(command), free, ct).ConfigureAwait(false);
        }

        var entry = new Entry(command);
        lock (_gate)
        {
            entry.Node = _queue.AddLast(entry);
        }

        IDisposable? lease;
        try
        {
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct, entry.Taken.Token);
            lease = await admission.TryEnterAsync(wait.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            if (Unqueue(entry))
            {
                throw new OperationCanceledException("Cancelled before the command was sent; nothing was applied.", ct);
            }

            return await WaitAsync(entry, ct).ConfigureAwait(false); // another caller's group carries it
        }

        if (lease is null)
        {
            // Refused: the waiting queue was full or the admission wait elapsed, unless a group took it meanwhile.
            return Unqueue(entry) ? null : await WaitAsync(entry, ct).ConfigureAwait(false);
        }

        return await LeadAsync(entry, lease, ct).ConfigureAwait(false);
    }

    /// <summary>The caller holds a slot: send its command alone, or with the queued commands it takes along.</summary>
    async Task<StoreReply<DispatchResult>> LeadAsync(Entry entry, IDisposable lease, CancellationToken ct)
    {
        var batch = Take(entry);
        if (batch is null)
        {
            lease.Dispose(); // a group took this command while it waited; that group's slot carries it
            return await WaitAsync(entry, ct).ConfigureAwait(false);
        }

        if (batch.Count == 1)
        {
            using (lease)
            {
                return await store.DispatchAsync(entry.Command, ct).ConfigureAwait(false);
            }
        }

        // The transaction starts first; the members' slot waits end after (their entries are already out of the queue).
        _ = RunAsync(batch, lease);
        for (var i = 1; i < batch.Count; i++)
        {
            await batch[i].Taken.CancelAsync().ConfigureAwait(false);
        }

        return await WaitAsync(entry, ct).ConfigureAwait(false);
    }

    /// <summary>The group's transaction runs to its end whoever stops waiting; then the slot frees.</summary>
    async Task RunAsync(List<Entry> batch, IDisposable lease)
    {
        try
        {
            var replies = await store.DispatchGroupAsync([.. batch.Select(e => e.Command)], CancellationToken.None).ConfigureAwait(false);
            for (var i = 0; i < batch.Count; i++)
            {
                if (replies[i].Reply is { } reply)
                {
                    batch[i].Completion.TrySetResult(reply);
                }
                else
                {
                    batch[i].Completion.TrySetException(replies[i].Failure ?? new InvalidOperationException("The group returned no outcome."));
                }
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
            lease.Dispose();
        }
    }

    /// <summary>
    /// The leader and the oldest queued commands that fit: same application and store, distinct command and job IDs, within
    /// the count and byte bounds. Null when the leader itself was already taken by another group.
    /// </summary>
    List<Entry>? Take(Entry leader)
    {
        lock (_gate)
        {
            if (leader.Submitted)
            {
                return null;
            }

            Remove(leader);
            var batch = new List<Entry>(Math.Min(IGroupedDispatchStore.MaxGroupItems, _queue.Count + 1)) { leader };
            if (_queue.Count == 0)
            {
                return batch;
            }

            var first = leader.Command;
            var bytes = Cost(first);
            var commands = new HashSet<Guid> { first.CommandId };
            var jobs = new HashSet<Guid> { first.Body.Item.Intent.ProposedJobId };
            for (var node = _queue.First; node is not null && batch.Count < IGroupedDispatchStore.MaxGroupItems;)
            {
                var next = node.Next;
                var c = node.Value.Command;
                if (bytes + Cost(c) <= IGroupedDispatchStore.MaxGroupBytes && c.Application == first.Application
                    && c.Body.StoreId == first.Body.StoreId && !commands.Contains(c.CommandId) && !jobs.Contains(c.Body.Item.Intent.ProposedJobId))
                {
                    bytes += Cost(c);
                    commands.Add(c.CommandId);
                    jobs.Add(c.Body.Item.Intent.ProposedJobId);
                    Remove(node.Value);
                    batch.Add(node.Value);
                }

                node = next;
            }

            return batch;
        }
    }

    static long Cost(StoreCommand<DispatchRequest> c) => c.Body.Item.Intent.PayloadUtf8.Length + IGroupedDispatchStore.ItemOverhead;

    /// <summary>Takes the entry out of the queue for good (under the lock).</summary>
    void Remove(Entry entry)
    {
        entry.Submitted = true;
        if (entry.Node is { } node)
        {
            _queue.Remove(node);
            entry.Node = null;
        }
    }

    /// <summary>The caller leaves before any group took its command: true, and nothing will be sent for it.</summary>
    bool Unqueue(Entry entry)
    {
        lock (_gate)
        {
            if (entry.Submitted)
            {
                return false;
            }

            Remove(entry);
            return true;
        }
    }

    /// <summary>
    /// Waits for a command another caller's group carries. A caller that stops waiting gets outcome unknown under its own
    /// operation ID (the dispatcher reports it as cancelled after send); the transaction still settles its receipt.
    /// </summary>
    static async Task<StoreReply<DispatchResult>> WaitAsync(Entry entry, CancellationToken ct)
    {
        try
        {
            return await entry.Completion.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested && !entry.Completion.Task.IsCompleted)
        {
            _ = entry.Completion.Task.ContinueWith(static t => t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
            throw new StoreOutcomeUnknownException("commit_outcome_unknown", entry.Command.CommandId, Operations.Dispatch);
        }
    }

    sealed class Entry(StoreCommand<DispatchRequest> command)
    {
        public StoreCommand<DispatchRequest> Command => command;

        /// <summary>Set under the grouper lock once the entry left the queue, for a group or for good.</summary>
        public bool Submitted;

        public LinkedListNode<Entry>? Node;

        /// <summary>Cancelled when another caller's group takes this command, ending its wait for a slot.</summary>
        public CancellationTokenSource Taken { get; } = new();

        public TaskCompletionSource<StoreReply<DispatchResult>> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
