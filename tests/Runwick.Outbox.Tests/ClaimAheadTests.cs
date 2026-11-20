using Runwick.Harness.Jobs;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Outbox.Tests;

/// <summary>
/// Outbox D (14 Changes 2026-11-20): the relay claims ahead, so claimed messages wait (leased, renewed, holding no slot) until a
/// slot frees, and a freed slot starts its next message without a claim round trip. Slots still bound concurrent sends, leases
/// stay within twice the slots, and every safety rule of single delivery holds for the messages that waited.
/// </summary>
public sealed class ClaimAheadTests
{
    const string Receipts = "SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND operation = 'ReceiveOutbox'";

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    public async Task Claimed_messages_wait_for_a_slot_and_leases_stay_within_twice_the_slots(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ahead1", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 30; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("a" + i)));
        }

        SlowDestination? slow = null;
        await using var relay = await o.OpenHost(relay: r => r.MaxInFlight = 2,
            wrapDestination: s => slow = new SlowDestination(s) { Delay = TimeSpan.FromMilliseconds(30) });
        var maxLeases = 0;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (relay.Relay.Delivered < 30)
        {
            Assert.True(DateTime.UtcNow < deadline, $"only {relay.Relay.Delivered} of 30 delivered");
            maxLeases = Math.Max(maxLeases, relay.Relay.InFlightCount);
            await Task.Delay(5);
        }

        Assert.InRange(slow!.MaxConcurrent, 1, 2);
        Assert.InRange(maxLeases, 1, 4);
        Assert.True(maxLeases > 2, "no claimed message ever waited beside the two busy slots");
        foreach (var message in staged)
        {
            Assert.Equal("delivered", await o.StateAsync(message.MessageId));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
        }

        Assert.Equal(30, await o.JobsAsync());
        Assert.Equal(30, await o.MappingsAsync());
        await WaitAsync(() => relay.Relay.InFlightCount == 0, "the last marks to be counted back");
        Assert.Equal(2, relay.Relay.FreeSlots());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    public async Task Messages_waiting_for_a_slot_when_the_relay_dies_are_reclaimed_and_delivered_once(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ahead2", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 6; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("c" + i)));
        }

        // Two slots hang in the destination, two claimed messages wait behind them, two stay pending.
        var hung = new HungDestination();
        var first = await o.OpenHost(identity: "relay1", relay: r => r.MaxInFlight = 2, wrapDestination: s => hung.Wrap(s));
        await WaitAsync(() => first.Relay.InFlightCount == 4 && hung.Calls == 2, "two sends hung and two claimed messages waiting");
        Assert.Equal(4, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'leased'"));

        // The relay dies: the host deadline passes with the sends still hung, so nothing it held is marked or sent again.
        await first.StopRelayAsync(TimeSpan.FromMilliseconds(200));
        await first.DisposeAsync();
        hung.Fail();
        Assert.Equal(2, hung.Calls);
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'"));

        // A new relay takes the pending two at once and the four leased ones after their leases expire, each exactly once.
        await using var second = await o.OpenHost(identity: "relay2");
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(6, await o.JobsAsync());
        Assert.Equal(4, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE attempts >= 2"));
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    public async Task Mixed_outcomes_lost_replies_and_lost_marks_in_one_claim_leave_one_job_and_one_receipt_per_message(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ahead3", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 8; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("m" + i)));
        }

        // One message of the claim cannot be imported and blocks; the rest are delivered.
        var malformed = staged[5];
        await o.SourceExecAsync(o.Sql("UPDATE {messages} SET intent = " + (kind == SourceKind.Postgres ? "'\\x00'::bytea" : "X'00'") + " WHERE message_id = {m}"),
            malformed.MessageId);

        // The first two destination replies are lost after the destination applied them (the same command is resent and replayed);
        // one mark fails before the source write and one is written but its reply is lost (both repaired by the mapping).
        FaultySource? source = null;
        LossyDestination? lossy = null;
        await using var relay = await o.OpenHost(relay: r => r.MaxInFlight = 4,
            wrapSource: s => source = new FaultySource(s), wrapDestination: s => lossy = new LossyDestination(s) { LoseRepliesAfterApplying = 2 });
        source!.LoseMarksBefore(1);
        source.LoseMarksAfter(1);

        await o.WaitStateAsync(malformed.MessageId, "blocked", TimeSpan.FromSeconds(60));
        foreach (var message in staged.Where(m => m != malformed))
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(2, lossy!.LostReplies);
        Assert.Equal(0, await o.MappingsFor(malformed.MessageId));
        Assert.Equal(7, await o.JobsAsync());
        // Resends and reclaims reused each message's transport command, so each delivered message has exactly one receipt.
        Assert.Equal(7, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task With_grouping_a_slot_carries_one_destination_group_and_slots_bound_the_concurrent_calls()
    {
        await using var o = await OutboxFixture.StartAsync("ahead4", SourceKind.Postgres);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 24; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("g" + i)));
        }

        CountingGroupDestination? counting = null;
        await using var relay = await o.OpenHost(relay: r =>
        {
            r.MaxInFlight = 2;
            r.DeliveryGroupSize = 4;
        }, wrapDestination: s => counting = new CountingGroupDestination(s));
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
        }

        Assert.InRange(counting!.MaxConcurrent, 1, 2);
        Assert.True(counting.LargestGroup >= 3, $"largest group {counting.LargestGroup}: waiting messages were not grouped into a free slot");
        Assert.Equal(24, await o.JobsAsync());
        Assert.Equal(24, await o.MappingsAsync());
        await WaitAsync(() => relay.Relay.InFlightCount == 0, "the last marks to be counted back");
        Assert.Equal(2, relay.Relay.FreeSlots());
    }

    /// <summary>Counts destination calls in progress at once (single or grouped) and the largest group.</summary>
    sealed class CountingGroupDestination(PgStore inner) : IOutboxDestinationStore, IOutboxGroupDestinationStore
    {
        int _now;
        int _max;
        int _largest;

        public int MaxConcurrent => Volatile.Read(ref _max);

        public int LargestGroup => Volatile.Read(ref _largest);

        static void Raise(ref int field, int value)
        {
            int seen;
            while ((seen = Volatile.Read(ref field)) < value && Interlocked.CompareExchange(ref field, value, seen) != seen)
            {
            }
        }

        async Task<T> Counted<T>(Func<Task<T>> call)
        {
            Raise(ref _max, Interlocked.Increment(ref _now));
            try
            {
                await Task.Delay(20);
                return await call();
            }
            finally
            {
                Interlocked.Decrement(ref _now);
            }
        }

        public Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct) =>
            Counted(() => inner.ReceiveOutboxAsync(command, ct));

        public Task<StoreReply<OutboxGroupDelivery>> ReceiveOutboxGroupAsync(IReadOnlyList<StoreCommand<ReceiveOutboxRequest>> commands, CancellationToken ct)
        {
            Raise(ref _largest, commands.Count);
            return Counted(() => inner.ReceiveOutboxGroupAsync(commands, ct));
        }

        public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
            inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);
    }

    static async Task WaitAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting for " + what);
            await Task.Delay(10);
        }
    }

    /// <summary>Delays every send and records the most sends in progress at once.</summary>
    sealed class SlowDestination(PgStore inner) : IOutboxDestinationStore
    {
        int _now;
        int _max;

        public TimeSpan Delay { get; init; }

        public int MaxConcurrent => Volatile.Read(ref _max);

        public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
        {
            var now = Interlocked.Increment(ref _now);
            int seen;
            while ((seen = Volatile.Read(ref _max)) < now && Interlocked.CompareExchange(ref _max, now, seen) != seen)
            {
            }

            try
            {
                await Task.Delay(Delay, ct);
                return await inner.ReceiveOutboxAsync(command, ct);
            }
            finally
            {
                Interlocked.Decrement(ref _now);
            }
        }

        public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
            inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);
    }

    /// <summary>Every send hangs (ignoring cancellation, like a stuck process) until <see cref="Fail"/>; nothing reaches the store.</summary>
    sealed class HungDestination
    {
        readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public void Fail() => _gate.TrySetResult();

        public IOutboxDestinationStore Wrap(PgStore inner) => new Hung(this, inner);

        sealed class Hung(HungDestination owner, PgStore inner) : IOutboxDestinationStore
        {
            public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
            {
                Interlocked.Increment(ref owner._calls);
                await owner._gate.Task;
                throw new IOException("Injected crash of a hung send.");
            }

            public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
                inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);
        }
    }

    /// <summary>Loses the reply of the first <see cref="LoseRepliesAfterApplying"/> sends after the destination committed them.</summary>
    sealed class LossyDestination(PgStore inner) : IOutboxDestinationStore
    {
        int _calls;
        int _lost;

        public int LoseRepliesAfterApplying { get; init; }

        public int LostReplies => Volatile.Read(ref _lost);

        public async Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct)
        {
            var reply = await inner.ReceiveOutboxAsync(command, ct);
            if (Interlocked.Increment(ref _calls) <= LoseRepliesAfterApplying)
            {
                Interlocked.Increment(ref _lost);
                throw new StoreOutcomeUnknownException("commit_outcome_unknown", command.CommandId, OutboxOperations.ReceiveOutbox,
                    new IOException("Injected loss of the destination reply."));
            }

            return reply;
        }

        public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
            inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);
    }
}
