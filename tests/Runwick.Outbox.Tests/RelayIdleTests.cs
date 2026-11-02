using Runwick.Harness.Jobs;

namespace Runwick.Outbox.Tests;

/// <summary>
/// The relay's idle poll (14 Changes 2026-11-02, idle caps): IdlePoll without a healthy wake listener, 5 × IdlePoll
/// with one, and IdlePoll again the moment the session is lost. The listener is scripted around the real SQLite adapter
/// (which has no wake channel of its own); claims and delivery are real.
/// </summary>
[Trait("Item", "PF-07")]
public sealed class RelayIdleTests
{
    /// <summary>The real adapter plus a wake channel the test drives.</summary>
    sealed class ListeningSource(IOutboxSource inner) : IOutboxSource, IOutboxWakeSource
    {
        Action? _onWake;
        Action? _onSubscribed;
        Action? _onLost;
        readonly TaskCompletionSource _running = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Running => _running.Task;

        public async Task ListenAsync(Action onWake, Action onSubscribed, Action onLost, CancellationToken ct)
        {
            (_onWake, _onSubscribed, _onLost) = (onWake, onSubscribed, onLost);
            _running.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
        }

        public void Subscribe() => _onSubscribed!();

        public void Lose() => _onLost!();

        public void Notify() => _onWake!();

        public Guid SourceStoreId => inner.SourceStoreId;

        public Task InitializeAsync(CancellationToken ct) => inner.InitializeAsync(ct);

        public Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct) =>
            inner.ClaimAsync(owner, max, leaseDuration, ct);

        public Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct) => inner.RenewAsync(lease, leaseDuration, ct);

        public Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct) =>
            inner.RecordTransportAsync(lease, commandId, notAfter, ct);

        public Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct) => inner.MarkDeliveredAsync(lease, delivered, ct);

        public Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct) => inner.RetryLaterAsync(lease, delay, error, ct);

        public Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct) => inner.BlockAsync(lease, reason, ct);
    }

    static async Task<long> ClaimsOver(OutboxRelay relay, TimeSpan window)
    {
        var before = Interlocked.Read(ref relay.Claims);
        await Task.Delay(window);
        return Interlocked.Read(ref relay.Claims) - before;
    }

    static async Task WaitUntil(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            await Task.Delay(10);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "WAKE-03")]
    public async Task A_healthy_wake_listener_lengthens_the_idle_poll_and_a_lost_one_restores_it_at_once()
    {
        await using var o = await OutboxFixture.StartAsync("relayidle", SourceKind.Sqlite);
        ListeningSource? source = null;
        var host = await o.OpenHost(relay: r => r.IdlePoll = TimeSpan.FromMilliseconds(200), wrapSource: s => source = new ListeningSource(s));
        var relay = host.Relay;
        Assert.NotNull(source);
        await source.Running.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitUntil(() => Interlocked.Read(ref relay.Claims) >= 1, "the first claim");

        // Not subscribed: a claim per 200 ms, ~20 in 4 s (fewer only by claim time on this host).
        Assert.False(relay.ListenerHealthy);
        Assert.Equal(TimeSpan.FromMilliseconds(200), relay.IdlePollNow);
        Assert.InRange(await ClaimsOver(relay, TimeSpan.FromSeconds(4)), 10, 22);

        // Healthy: one per 1 s, ~4 in 4 s (a 200 ms poll would be ~20).
        source.Subscribe();
        Assert.True(relay.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(1), relay.IdlePollNow);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.InRange(await ClaimsOver(relay, TimeSpan.FromSeconds(4)), 2, 7);

        // A stage plus its notification is delivered without waiting out the long poll.
        var staged = await o.StageAsync(host, new PingJob("notified"));
        source.Notify();
        await WaitUntil(() => o.StateAsync(staged.MessageId).Result == "delivered", "the notified message delivered");

        // Lost: the current wait ends at once, then a claim per 200 ms again.
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var before = Interlocked.Read(ref relay.Claims);
        source.Lose();
        Assert.False(relay.ListenerHealthy);
        Assert.Equal(1, Interlocked.Read(ref relay.ListenerLosses));
        Assert.Equal(TimeSpan.FromMilliseconds(200), relay.IdlePollNow);
        await WaitUntil(() => Interlocked.Read(ref relay.Claims) > before, "a claim right after the loss");
        Assert.InRange(await ClaimsOver(relay, TimeSpan.FromSeconds(4)), 10, 22);

        // Resubscribed: the long poll again.
        source.Subscribe();
        Assert.Equal(TimeSpan.FromSeconds(1), relay.IdlePollNow);
    }
}
