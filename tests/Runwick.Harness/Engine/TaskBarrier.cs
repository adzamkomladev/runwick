namespace Runwick.Harness.Engine;

/// <summary>
/// A one-shot rendezvous: code under test calls <see cref="ArriveAndWaitAsync"/>; the test
/// observes <see cref="Reached"/> and later calls <see cref="Release"/>.
/// </summary>
public sealed class TaskBarrier(string name)
{
    readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public string Name => name;

    public Task Reached => _reached.Task;

    public bool IsReleased => _released.Task.IsCompleted;

    public async Task ArriveAndWaitAsync(CancellationToken cancellationToken = default)
    {
        _reached.TrySetResult();
        await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Release() => _released.TrySetResult();

    public void Fail(Exception exception) => _released.TrySetException(exception);

    public async Task WaitReachedAsync(TimeSpan? timeout = null) =>
        await _reached.Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(30)).ConfigureAwait(false);

    public override string ToString() => $"barrier:{name}";
}
