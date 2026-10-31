namespace Runwick.Scheduling.Evaluation;

/// <summary>
/// The single evaluator task of one scheduler/store group (spec 12 §8.3). Work runs synchronously inside the slot, so
/// an elapsed budget or a cancelled caller never starts a replacement while a library call is still running: the slot
/// is released only when the work actually returns.
/// </summary>
internal sealed class EvaluatorSlot : IDisposable
{
    readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Runs <paramref name="work"/> once the slot is free. Cancellation only abandons the wait, never running work.</summary>
    public async ValueTask<T> RunAsync<T>(Func<T> work, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return work();
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
