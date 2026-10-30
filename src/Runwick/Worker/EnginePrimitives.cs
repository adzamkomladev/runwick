namespace Runwick.Worker;

/// <summary>Local monotonic time (05 §11.1): every engine deadline is a span since engine start, never wall time.</summary>
internal sealed class EngineClock(TimeProvider time)
{
    readonly long _origin = time.GetTimestamp();

    public TimeProvider Time => time;

    public TimeSpan Now => time.GetElapsedTime(_origin);

    public DateTimeOffset WallNow => time.GetUtcNow();

    /// <summary>Wall NotAfter for a command: whole milliseconds, as receipts compare them.</summary>
    public DateTimeOffset NotAfter(TimeSpan lifetime) =>
        DateTimeOffset.FromUnixTimeMilliseconds((time.GetUtcNow() + lifetime).ToUnixTimeMilliseconds());

    public Task Delay(TimeSpan delay, CancellationToken ct) =>
        delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, time, ct);

    /// <summary>A token cancelled after <paramref name="delay"/> on this clock.</summary>
    public CancellationTokenSource After(TimeSpan delay) => new(delay > TimeSpan.Zero ? delay : TimeSpan.Zero, time);

    /// <summary>value × (1 + u), u uniform in [−fraction, +fraction] (idle backoff, cooldowns).</summary>
    public static TimeSpan Jitter(TimeSpan value, double fraction = WorkerSettings.JitterFraction) =>
        TimeSpan.FromTicks(Math.Max(1, (long)(value.Ticks * (1 + ((Random.Shared.NextDouble() * 2) - 1) * fraction))));

    /// <summary>Uniform in [min, max].</summary>
    public static TimeSpan Uniform(TimeSpan min, TimeSpan max) =>
        max <= min ? min : min + TimeSpan.FromTicks((long)(Random.Shared.NextDouble() * (max - min).Ticks));

    /// <summary>Control retry backoff: nominal initial·2^attempt capped at max, equal jitter (06 F-7).</summary>
    public static TimeSpan ControlBackoff(int attempt, TimeSpan initial, TimeSpan max)
    {
        var nominal = attempt >= 30 ? max : TimeSpan.FromTicks(Math.Min(max.Ticks, initial.Ticks << Math.Max(0, attempt)));
        return Uniform(nominal / 2, nominal);
    }
}

/// <summary>
/// One pool's local capacity (05 §4): a locked counter of outstanding idempotent tickets and a target. A ticket is
/// acquired before a Reserve is created and released exactly once when its holder retires.
/// </summary>
internal sealed class LocalCapacityGate(int target)
{
    readonly Lock _lock = new();
    int _outstanding;
    bool _open = true;

    public int Target { get; private set; } = target;

    public int Outstanding
    {
        get
        {
            lock (_lock)
            {
                return _outstanding;
            }
        }
    }

    /// <summary>Raised after a ticket is released (refill wake).</summary>
    public event Action? Released;

    public CapacityTicket? TryAcquire()
    {
        lock (_lock)
        {
            if (!_open || _outstanding >= Target)
            {
                return null;
            }

            _outstanding++;
            return new CapacityTicket(this);
        }
    }

    /// <summary>Closes acquisition (drain); outstanding tickets stay held until their holders retire.</summary>
    public void Close()
    {
        lock (_lock)
        {
            _open = false;
        }
    }

    internal void Release()
    {
        lock (_lock)
        {
            _outstanding--;
        }

        Released?.Invoke();
    }
}

/// <summary>An idempotent ticket: a second release is a no-op.</summary>
internal sealed class CapacityTicket(LocalCapacityGate gate)
{
    int _released;

    public bool IsReleased => Volatile.Read(ref _released) == 1;

    public void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
        {
            gate.Release();
        }
    }
}

/// <summary>Arbiter priorities (05 §6 rule 3): resolution, Finish/Release, Begin, Renew. Lower is served first.</summary>
internal static class ControlPriority
{
    public const int Resolution = 0;
    public const int Settlement = 1;
    public const int Begin = 2;
    public const int Renew = 3;
}

/// <summary>
/// A counted gate whose waiters are served by priority, then arrival. With capacity 1 it is the per-attempt control
/// arbiter (05 §6); with larger capacities it bounds an I/O class (05 §18). A slot is held until disposed, i.e.
/// until the provider task terminated; it is never freed early because a wait timed out.
/// </summary>
internal sealed class PriorityGate(int capacity)
{
    readonly Lock _lock = new();
    readonly List<(int Priority, long Sequence, TaskCompletionSource<Slot> Waiter)> _waiters = [];
    long _sequence;
    int _held;

    public int Held
    {
        get
        {
            lock (_lock)
            {
                return _held;
            }
        }
    }

    public int Waiting
    {
        get
        {
            lock (_lock)
            {
                return _waiters.Count;
            }
        }
    }

    /// <summary>When the oldest current holder acquired its slot (arbiter-blocked detection).</summary>
    public long HeldSinceTimestamp { get; private set; }

    public Task<Slot> AcquireAsync(int priority, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_held < capacity && _waiters.Count == 0)
            {
                _held++;
                HeldSinceTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
                return Task.FromResult(new Slot(this));
            }

            var tcs = new TaskCompletionSource<Slot>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((priority, _sequence++, tcs));
            if (!ct.CanBeCanceled)
            {
                return tcs.Task;
            }

            // The registration is disposed once the wait ends: long-lived stop tokens must not accumulate callbacks.
            var registration = ct.Register(() =>
            {
                bool removed;
                lock (_lock)
                {
                    removed = _waiters.RemoveAll(w => w.Waiter == tcs) > 0;
                }

                if (removed)
                {
                    tcs.TrySetCanceled(ct);
                }
            });
            return WaitAsync(tcs.Task, registration);
        }
    }

    static async Task<Slot> WaitAsync(Task<Slot> wait, CancellationTokenRegistration registration)
    {
        try
        {
            return await wait.ConfigureAwait(false);
        }
        finally
        {
            await registration.DisposeAsync().ConfigureAwait(false);
        }
    }

    void Free()
    {
        TaskCompletionSource<Slot>? next = null;
        lock (_lock)
        {
            if (_waiters.Count == 0)
            {
                _held--;
                return;
            }

            var best = 0;
            for (var i = 1; i < _waiters.Count; i++)
            {
                if (_waiters[i].Priority < _waiters[best].Priority
                    || (_waiters[i].Priority == _waiters[best].Priority && _waiters[i].Sequence < _waiters[best].Sequence))
                {
                    best = i;
                }
            }

            next = _waiters[best].Waiter;
            _waiters.RemoveAt(best);
            HeldSinceTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
        }

        // The slot passes directly to the chosen waiter (held count unchanged).
        next.TrySetResult(new Slot(this));
    }

    internal sealed class Slot(PriorityGate owner) : IDisposable
    {
        int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                owner.Free();
            }
        }
    }
}

/// <summary>A capacity-1 coalescing wake signal (05 §7.2): extra signals merge, nothing is allocated per signal.</summary>
internal sealed class WakeSignal
{
    readonly Lock _lock = new();
    TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set()
    {
        lock (_lock)
        {
            _tcs.TrySetResult();
        }
    }

    /// <summary>Waits for a signal or the delay, then consumes any pending signal.</summary>
    public async Task WaitAsync(EngineClock clock, TimeSpan timeout, CancellationToken ct)
    {
        Task signal;
        lock (_lock)
        {
            signal = _tcs.Task;
        }

        if (!signal.IsCompleted)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var delay = clock.Delay(timeout, cts.Token);
            await Task.WhenAny(signal, delay).ConfigureAwait(false);
            await cts.CancelAsync().ConfigureAwait(false);
        }

        lock (_lock)
        {
            if (_tcs.Task.IsCompleted)
            {
                _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        ct.ThrowIfCancellationRequested();
    }
}
