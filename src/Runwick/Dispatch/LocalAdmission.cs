namespace Runwick.Dispatching;

/// <summary>Bounded local admission for producer/admin mutations (spec 01 §10.5, 06 defaults 4/64/1 s).</summary>
internal sealed class LocalAdmission(int concurrency, int maxWaiting, TimeSpan wait, TimeProvider time) : IDisposable
{
    readonly SemaphoreSlim _slots = new(concurrency, concurrency);
    int _waiting;

    /// <summary>Operations currently waiting for a slot (bounded by MaxWaitingOperations).</summary>
    public int Waiting => Volatile.Read(ref _waiting);

    /// <summary>Returns a lease, or null when the gate refused (full queue or wait elapsed).</summary>
    public async Task<IDisposable?> TryEnterAsync(CancellationToken ct)
    {
        if (_slots.Wait(0, CancellationToken.None))
        {
            return new Lease(_slots);
        }

        if (Interlocked.Increment(ref _waiting) > maxWaiting)
        {
            Interlocked.Decrement(ref _waiting);
            return null;
        }

        try
        {
            using var timeout = new CancellationTokenSource(wait, time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
            try
            {
                await _slots.WaitAsync(linked.Token).ConfigureAwait(false);
                return new Lease(_slots);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null;
            }
        }
        finally
        {
            Interlocked.Decrement(ref _waiting);
        }
    }

    public void Dispose() => _slots.Dispose();

    sealed class Lease(SemaphoreSlim slots) : IDisposable
    {
        int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                slots.Release();
            }
        }
    }
}
