using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>
/// Opt-in producer grouping (perf plan F): plain dispatches that arrive while sends are in flight go to Redis in one
/// <c>dispatch_group</c> script call. Every member keeps its own command ID, fingerprint, deadline, receipt and event.
/// </summary>
internal sealed partial class RedisStore
{
    internal const string DispatchGroupOperation = "DispatchGroup";

    /// <summary>The group call's own time budget. Item deadlines are checked per item inside the script.</summary>
    static readonly TimeSpan GroupBudget = TimeSpan.FromSeconds(10);

    readonly ConcurrentDictionary<string, DispatchGrouper> _groupers = new(StringComparer.Ordinal);

    internal DispatchGrouper Grouper(string application) =>
        _groupers.GetOrAdd(application, a => new DispatchGrouper(this, a, options.MaxGroupItems, options.MaxGroupBytes, options.MaxGroupsInFlight));

    /// <summary>Counters for tests and the benchmark: grouped calls, their members, and calls the script declined.</summary>
    public long GroupedCalls => _groupers.Values.Sum(g => Interlocked.Read(ref g.Calls));

    public long GroupedMembers => _groupers.Values.Sum(g => Interlocked.Read(ref g.Members));

    public long GroupFallbacks => _groupers.Values.Sum(g => Interlocked.Read(ref g.Fallbacks));

    internal async Task<RxReply> DispatchGroupCallAsync(string application, IReadOnlyList<GroupEntry> batch, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var args = new List<RedisValue>(1 + batch.Count * 5) { batch.Count };
        foreach (var entry in batch)
        {
            var c = entry.Command;
            args.Add(Hex(c.CommandId));
            args.Add(Convert.ToHexStringLower(c.Body.CommandFingerprint));
            args.Add(Ms(c.NotAfter));
            args.Add(entry.Json ??= Json(ItemJson(c.Body.Item)));
            args.Add(c.Body.Item.Intent.PayloadUtf8);
        }

        // The group has an identity of its own (its recoverable intent) and no receipt.
        var op = Op(application, Guid.CreateVersion7(), DispatchGroupOperation, time.GetUtcNow() + GroupBudget, null, RxLane.Producer, 'c',
            batch[0].Command.Body.StoreId);
        return await Write(op, "dispatch_group", ct, [.. args]).ConfigureAwait(false);
    }
}

internal sealed class GroupEntry(StoreCommand<DispatchRequest> command, int bytes, string? json)
{
    public StoreCommand<DispatchRequest> Command { get; } = command;

    public int Bytes { get; } = bytes;

    public string? Json { get; set; } = json;

    public TaskCompletionSource<StoreReply<DispatchResult>> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// One queue per application. A caller that finds the queue empty and a send slot free sends alone at once. Others wait in a
/// bounded queue and the next free slot sends them together (at most <c>maxItems</c> and <c>maxBytes</c>). A caller that finds
/// the queue full, or whose dispatch is larger than a group may carry, goes alone through the ordinary path. Nothing waits on a timer.
/// </summary>
internal sealed class DispatchGrouper(RedisStore store, string application, int maxItems, int maxBytes, int slots)
{
    const int PerItemOverhead = 1024;
    readonly object _gate = new();
    readonly List<GroupEntry> _queue = [];
    readonly int _queueLimit = maxItems * 4;
    int _queuedBytes;
    int _busy;
    bool _closed;
    public long Calls;
    public long Members;
    public long Fallbacks;

    /// <summary>Plain dispatches (delayed ones included) without keys, scopes or schedules: the script declines everything else, so do not even ask.</summary>
    public static bool Eligible(DispatchItem item)
    {
        var i = item.Intent;
        return i.ActiveKey is null && i.WindowKey is null && i.WindowMs is null
            && i.ConcurrencyScopes.Count == 0 && i.RateScopes.Count == 0 && item.Scopes.Count == 0 && item.Rates.Count == 0 && item.Schedule is null;
    }

    public async Task<StoreReply<DispatchResult>> SubmitAsync(StoreCommand<DispatchRequest> command, CancellationToken ct)
    {
        var item = command.Body.Item;
        var bytes = item.Intent.PayloadUtf8.Length + PerItemOverhead;
        // Serialize before taking the lock when a queue is likely: the callers do this work in parallel, not the sender.
        var json = Volatile.Read(ref _busy) >= slots && bytes <= maxBytes ? RedisStore.Json(RedisStore.ItemJson(item)) : null;
        GroupEntry? entry = null;
        var direct = false;
        lock (_gate)
        {
            if (_closed)
            {
                throw new StoreNotAppliedException("store_unavailable");
            }

            if (_queue.Count == 0 && _busy < slots)
            {
                _busy++;
                direct = true;
            }
            else if (bytes <= maxBytes && _queue.Count < _queueLimit && _queuedBytes + bytes <= maxBytes * 4)
            {
                entry = new GroupEntry(command, bytes, json);
                _queue.Add(entry);
                _queuedBytes += bytes;
            }
        }

        if (direct)
        {
            try
            {
                return await store.DispatchSingleAsync(command, ct).ConfigureAwait(false);
            }
            finally
            {
                Release();
            }
        }

        if (entry is null)
        {
            return await store.DispatchSingleAsync(command, ct).ConfigureAwait(false);
        }

        try
        {
            return await entry.Done.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!entry.Done.Task.IsCompleted)
        {
            lock (_gate)
            {
                if (_queue.Remove(entry))
                {
                    _queuedBytes -= entry.Bytes;
                    throw;
                }
            }

            // Already taken for a send: like a single send past its lane, it runs to its outcome and the caller gets that.
            return await entry.Done.Task.ConfigureAwait(false);
        }
    }

    /// <summary>Stops accepting; queued entries that were never sent fail as not applied (safe to retry).</summary>
    public void Close()
    {
        List<GroupEntry> dropped;
        lock (_gate)
        {
            _closed = true;
            dropped = [.. _queue];
            _queue.Clear();
            _queuedBytes = 0;
        }

        foreach (var e in dropped)
        {
            e.Done.TrySetException(new StoreNotAppliedException("store_unavailable"));
        }
    }

    List<GroupEntry>? TakeLocked()
    {
        if (_queue.Count == 0)
        {
            return null;
        }

        var n = 0;
        var total = 0;
        while (n < _queue.Count && n < maxItems && (n == 0 || total + _queue[n].Bytes <= maxBytes))
        {
            total += _queue[n++].Bytes;
        }

        var batch = _queue.GetRange(0, n);
        _queue.RemoveRange(0, n);
        _queuedBytes -= total;
        return batch;
    }

    void Release()
    {
        List<GroupEntry>? batch;
        lock (_gate)
        {
            batch = TakeLocked();
            if (batch is null)
            {
                _busy--;
            }
        }

        if (batch is not null)
        {
            _ = Task.Run(() => PumpAsync(batch));
        }
    }

    async Task PumpAsync(List<GroupEntry>? batch)
    {
        while (batch is not null)
        {
            await SendAsync(batch).ConfigureAwait(false);
            lock (_gate)
            {
                batch = TakeLocked();
                if (batch is null)
                {
                    _busy--;
                }
            }
        }
    }

    async Task SendAsync(List<GroupEntry> batch)
    {
        try
        {
            if (batch.Count == 1)
            {
                await SingleAsync(batch[0]).ConfigureAwait(false);
                return;
            }

            RxReply group;
            try
            {
                group = await store.DispatchGroupCallAsync(application, batch, CancellationToken.None).ConfigureAwait(false);
            }
            catch (StoreOutcomeUnknownException ex)
            {
                // The group's reply is lost: each caller gets an unknown outcome for its own command, to resolve or resend as usual.
                foreach (var e in batch)
                {
                    e.Done.TrySetException(new StoreOutcomeUnknownException(ex.Code, e.Command.CommandId, Operations.Dispatch, ex.InnerException));
                }

                return;
            }

            if (group.Code == "group_fallback")
            {
                // Declined before any write: send each command alone with its original identity.
                Interlocked.Increment(ref Fallbacks);
                await Task.WhenAll(batch.Select(SingleAsync)).ConfigureAwait(false);
                return;
            }

            if (!group.HasValue || group.Result?["items"] is not JsonArray rows || rows.Count != batch.Count)
            {
                // A refusal of the whole call (a gate, for example) is the answer every single dispatch would have got.
                foreach (var e in batch)
                {
                    e.Done.TrySetResult(group.Reject<DispatchResult>());
                }

                return;
            }

            Interlocked.Increment(ref Calls);
            Interlocked.Add(ref Members, batch.Count);
            for (var i = 0; i < batch.Count; i++)
            {
                var r = RxReply.FromRow(rows[i]!.AsArray());
                batch[i].Done.TrySetResult(r.HasValue ? r.Applied(ReceiptCodec.ReadDispatch(r.Result!)) : r.Reject<DispatchResult>());
            }
        }
        catch (Exception ex)
        {
            foreach (var e in batch)
            {
                e.Done.TrySetException(ex);
            }
        }
    }

    async Task SingleAsync(GroupEntry e)
    {
        try
        {
            e.Done.TrySetResult(await store.DispatchSingleAsync(e.Command, CancellationToken.None).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            e.Done.TrySetException(ex);
        }
    }
}
