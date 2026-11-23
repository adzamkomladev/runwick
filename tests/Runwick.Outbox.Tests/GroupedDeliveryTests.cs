using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Outbox.Tests;

/// <summary>
/// D4 grouped destination delivery: the grouped ReceiveOutbox transaction keeps each command's receipt, mapping and reply,
/// the relay settles each message under its own lease token, and any group failure is answered by sending the same
/// commands alone.
/// </summary>
public sealed class GroupedDeliveryTests
{
    const string Receipts = "SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND operation = 'ReceiveOutbox'";

    static Task<long> ReceiptsFor(OutboxFixture o, Guid commandId) =>
        o.Destination.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    [Fact]
    [Trait("Category", "Contract")]
    public async Task A_group_gives_each_command_its_own_reply_receipt_and_mapping_and_refused_items_leave_nothing()
    {
        await using var o = await OutboxFixture.StartAsync("ogroup1", SourceKind.Postgres);
        var host = await o.OpenHost(identity: "g", start: false);
        var acceptUntil = Transport.Ms(DateTimeOffset.UtcNow.AddHours(1));
        PreparedDispatch Prepare(string name) => host.Prepared.Prepare(new PingJob(name));
        OutboxSourceIdentity NewIdentity() => Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var ct = CancellationToken.None;

        // Earlier singleton deliveries: one to replay inside the group, one whose identity a group item then conflicts with.
        var replayPrepared = Prepare("replay");
        var replay = Transport.Command(replayPrepared, NewIdentity(), acceptUntil);
        Assert.Equal(StoreStatus.Applied, (await host.Store.ReceiveOutboxAsync(replay, ct)).Status);
        var conflictPrepared = Prepare("conflict");
        var conflictIdentity = NewIdentity();
        Assert.Equal(StoreStatus.Applied, (await host.Store.ReceiveOutboxAsync(Transport.Command(conflictPrepared, conflictIdentity, acceptUntil), ct)).Status);

        var fresh1 = Transport.Command(Prepare("fresh1"), NewIdentity(), acceptUntil);
        var fresh2 = Transport.Command(Prepare("fresh2"), NewIdentity(), acceptUntil);
        var conflict = Transport.Command(conflictPrepared, conflictIdentity, acceptUntil.AddMinutes(1));
        var expired = Transport.Command(Prepare("expired"), NewIdentity(), acceptUntil, notAfter: DateTimeOffset.UtcNow.AddMilliseconds(400));
        await Task.Delay(600, ct);

        var reply = await host.Store.ReceiveOutboxGroupAsync([fresh1, replay, conflict, expired, fresh2], ct);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var outcomes = reply.Value!.Outcomes;
        Assert.Equal(5, outcomes.Count);

        Assert.Equal(StoreStatus.Applied, outcomes[0].Status);
        Assert.True(outcomes[0].Value!.FirstDelivery);
        Assert.Equal(fresh1.Body.Item.Intent.ProposedJobId, outcomes[0].Value!.JobId);

        Assert.Equal(StoreStatus.Applied, outcomes[1].Status);
        Assert.True(outcomes[1].Replayed);
        Assert.Equal(replayPrepared.Intent.ProposedJobId, outcomes[1].Value!.JobId);

        Assert.Equal(StoreStatus.CommandConflict, outcomes[2].Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, outcomes[2].Effect);
        Assert.Equal(StoreStatus.CommandExpired, outcomes[3].Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, outcomes[3].Effect);

        Assert.Equal(StoreStatus.Applied, outcomes[4].Status);
        Assert.Equal(fresh2.Body.Item.Intent.ProposedJobId, outcomes[4].Value!.JobId);

        // Two earlier deliveries plus two fresh ones: four jobs and four mappings, each with exactly one receipt.
        Assert.Equal(4, await o.JobsAsync());
        Assert.Equal(4, await o.MappingsAsync());
        Assert.Equal(4, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
        Assert.Equal(1, await ReceiptsFor(o, fresh1.CommandId));
        Assert.Equal(1, await ReceiptsFor(o, fresh2.CommandId));
        Assert.Equal(0, await ReceiptsFor(o, conflict.CommandId));
        Assert.Equal(0, await ReceiptsFor(o, expired.CommandId));

        // Each grouped command answers a later singleton resend with the reply the group recorded.
        var resend = await host.Store.ReceiveOutboxAsync(fresh1, ct);
        Assert.True(resend.Replayed);
        Assert.Equal(outcomes[0].DecidedAt, resend.DecidedAt);
        Assert.Equal(4, await o.JobsAsync());
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task Groups_the_store_cannot_take_are_refused_before_any_effect()
    {
        await using var o = await OutboxFixture.StartAsync("ogroup2", SourceKind.Postgres);
        var host = await o.OpenHost(identity: "g", start: false);
        var acceptUntil = Transport.Ms(DateTimeOffset.UtcNow.AddHours(1));
        var a = Transport.Command(host.Prepared.Prepare(new PingJob("a")), Transport.Identity(o.SourceStoreId, Guid.CreateVersion7()), acceptUntil);
        var b = Transport.Command(host.Prepared.Prepare(new PingJob("b")), Transport.Identity(o.SourceStoreId, Guid.CreateVersion7()), acceptUntil);

        // One command, a repeated command ID and a repeated message identity are not groups; nothing is written.
        var sameMessage = Transport.Command(host.Prepared.Prepare(new PingJob("b")), a.Body.Source, acceptUntil);
        var notGroups = new List<IReadOnlyList<StoreCommand<ReceiveOutboxRequest>>> { new[] { a }, new[] { a, a }, new[] { a, sameMessage } };
        foreach (var commands in notGroups)
        {
            var reply = await host.Store.ReceiveOutboxGroupAsync(commands, CancellationToken.None);
            Assert.NotEqual(StoreStatus.Applied, reply.Status);
        }

        Assert.Equal(0, await o.JobsAsync());
        Assert.Equal(0, await o.MappingsAsync());
        Assert.Equal(0, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
        Assert.Equal(StoreStatus.Applied, (await host.Store.ReceiveOutboxGroupAsync([a, b], CancellationToken.None)).Status);
        Assert.Equal(2, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    public async Task The_relay_delivers_claimed_messages_as_groups_and_each_message_maps_to_one_job(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup3", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 12; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("m" + i)));
        }

        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 8;
            r.DeliveryGroupSize = 8;
        });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(12, await o.JobsAsync());
        Assert.Equal(12, await o.MappingsAsync());
        Assert.Equal(12, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
        foreach (var message in staged)
        {
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(12, relay.Relay.Delivered);
        Assert.Equal(0, relay.Relay.Blocked);
        Assert.True(relay.Relay.GroupSends > 0 && relay.Relay.GroupedCommands >= 2, "claimed messages were never sent as a group");
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    public async Task A_lost_group_reply_sends_the_same_commands_alone_and_creates_no_second_job(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup4", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 6; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("lost" + i)));
        }

        LossyGroupDestination? lossy = null;
        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 6;
            r.DeliveryGroupSize = 6;
        }, wrapDestination: inner => lossy = new LossyGroupDestination(inner) { LoseRepliesAfterApplying = 100 });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.True(lossy!.GroupCalls > 0);
        Assert.True(relay.Relay.GroupFallbacks > 0);
        Assert.Equal(6, await o.JobsAsync());
        Assert.Equal(6, await o.MappingsAsync());
        // The group applied before its reply was lost, so the alone sends were all receipt replays: one receipt per command.
        Assert.Equal(6, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
        foreach (var message in staged)
        {
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
        }
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    public async Task Lost_delivered_marks_after_a_group_are_repaired_by_the_mappings_without_a_second_job(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup7", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 6; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("mark" + i)));
        }

        // Three marks are written and their replies lost, two never reach the source: the destination already holds all six mappings.
        FaultySource? source = null;
        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 6;
            r.DeliveryGroupSize = 6;
        }, wrapSource: inner =>
        {
            source = new FaultySource(inner);
            source.LoseMarksAfter(3);
            source.LoseMarksBefore(2);
            return source;
        });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(6, await o.JobsAsync());
        Assert.Equal(6, await o.MappingsAsync());
        foreach (var message in staged)
        {
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    public async Task A_message_that_cannot_be_delivered_blocks_alone_and_its_group_mates_are_confirmed(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup5", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var good = new List<StagedOutboxMessage>();
        for (var i = 0; i < 3; i++)
        {
            good.Add(await o.StageAsync(stager, new PingJob("good" + i)));
        }

        // A source row whose intent is malformed blocks at the relay before any destination call; its claim mates go as a group.
        var bad = await o.StageAsync(stager, new PingJob("bad"));
        await o.SourceExecAsync(o.Sql("UPDATE {messages} SET intent = " + (kind == SourceKind.Postgres ? "'\\x00'::bytea" : "X'00'") + " WHERE message_id = {m}"),
            bad.MessageId);

        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.DeliveryGroupSize = 4;
        });
        await o.WaitStateAsync(bad.MessageId, "blocked", TimeSpan.FromSeconds(60));
        foreach (var message in good)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(3, await o.JobsAsync());
        Assert.Equal(3, await o.MappingsAsync());
        Assert.Equal(0, await o.MappingsFor(bad.MessageId));
        Assert.Equal(3, relay.Relay.Delivered);
        Assert.Equal(1, relay.Relay.Blocked);
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task Groups_do_not_form_when_the_option_is_one_or_the_destination_cannot_group()
    {
        await using var o = await OutboxFixture.StartAsync("ogroup6", SourceKind.Postgres, store: StoreKind.Sqlite);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 4; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("alone" + i)));
        }

        // A SQLite destination shares commits through its producer group, not through the relay, so no relay group forms.
        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.DeliveryGroupSize = 4;
        });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(0, relay.Relay.GroupSends);
        Assert.Equal(4, relay.Relay.Delivered);
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Case", "OUTBOX-08")]
    public async Task One_group_with_fresh_replayed_and_conflicting_messages_settles_each_message_on_its_own(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup8", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var fresh1 = await o.StageAsync(stager, new PingJob("fresh1"));
        var replayed = await o.StageAsync(stager, new PingJob("replayed"));
        var conflicting = await o.StageAsync(stager, new PingJob("conflicting"));
        var fresh2 = await o.StageAsync(stager, new PingJob("fresh2"));

        // Before the relay runs, the destination already holds the exact delivery of one message (its reply was lost earlier) and a
        // different delivery under another message's identity (a conflict the relay must not overwrite).
        var replayPrepared = stager.Prepared.ImportPrepared(await o.EnvelopeAsync(replayed.MessageId));
        Assert.Equal(StoreStatus.Applied, (await stager.Store.ReceiveOutboxAsync(
            Transport.Command(replayPrepared, Transport.Identity(o.SourceStoreId, replayed.MessageId), replayed.AcceptUntil), CancellationToken.None)).Status);
        var other = stager.Prepared.Prepare(new PingJob("other"));
        Assert.Equal(StoreStatus.Applied, (await stager.Store.ReceiveOutboxAsync(
            Transport.Command(other, Transport.Identity(o.SourceStoreId, conflicting.MessageId), conflicting.AcceptUntil), CancellationToken.None)).Status);

        // One slot of four: the single claim of all four messages goes to the destination as one group.
        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 1;
            r.DeliveryGroupSize = 4;
        });
        await o.WaitStateAsync(conflicting.MessageId, "blocked", TimeSpan.FromSeconds(60));
        foreach (var message in new[] { fresh1, replayed, fresh2 })
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(1, relay.Relay.GroupSends);
        Assert.Equal(4, relay.Relay.GroupedCommands);
        Assert.Equal(0, relay.Relay.GroupFallbacks);
        Assert.Equal(3, relay.Relay.Delivered);
        Assert.Equal(1, relay.Relay.Blocked);
        Assert.Equal("delivery_conflict", await o.BlockedReasonAsync(conflicting.MessageId));
        Assert.Equal(1, await o.MappingsFor(conflicting.MessageId));

        // Two fresh jobs, the replayed one (created once, before the relay) and the conflicting identity's other job.
        Assert.Equal(4, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Case", "OUTBOX-09")]
    public async Task A_stalled_relay_whose_group_answers_after_a_takeover_cannot_confirm_with_its_stale_tokens(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup9", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 4; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("stall" + i)));
        }

        // Relay 1 sends one group that hangs, and its renewals silently stop reaching the source, so its leases expire while it
        // still believes it owns them.
        var hung = new HungGroupDestination(stager.Store);
        FaultySource? stalledSource = null;
        await using var stalled = await o.OpenHost(identity: "relay1", relay: r =>
        {
            r.MaxInFlight = 1;
            r.DeliveryGroupSize = 4;
        }, wrapSource: s => stalledSource = new FaultySource(s) { SkipRenewals = true }, wrapDestination: _ => hung);
        await WaitAsync(() => hung.GroupCalls == 1, "relay 1's group to reach the destination");

        // Relay 2 takes the expired leases over and delivers every message.
        await using var takeover = await o.OpenHost(identity: "relay2");
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        // Relay 1's group now answers (the receipts or mappings replay relay 2's deliveries); every mark it tries carries a stale token.
        hung.Release();
        await hung.Applied;
        await WaitAsync(() => Interlocked.Read(ref stalled.Relay.LeaseLosses) >= 4, "relay 1's stale marks to be refused", seconds: 90); // the full suite on a loaded host needed more than 30 s
        Assert.Equal(0, stalled.Relay.Delivered);
        Assert.Equal(4, takeover.Relay.Delivered);
        foreach (var message in staged)
        {
            Assert.Equal("delivered", await o.StateAsync(message.MessageId));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(4, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Case", "OUTBOX-10")]
    public async Task A_group_that_applies_after_its_relay_crashed_is_reclaimed_and_confirmed_without_a_second_job(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("ogroup10", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 4; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("crash" + i)));
        }

        var hung = new HungGroupDestination(stager.Store);
        var crashed = await o.OpenHost(identity: "relay1", relay: r =>
        {
            r.MaxInFlight = 1;
            r.DeliveryGroupSize = 4;
        }, wrapDestination: _ => hung);
        await WaitAsync(() => hung.GroupCalls == 1, "relay 1's group to reach the destination");

        // The relay dies with its group in flight; the group commits afterwards and its reply goes nowhere.
        await crashed.StopRelayAsync(TimeSpan.FromMilliseconds(200));
        await crashed.DisposeAsync();
        hung.Release();
        Assert.Equal(StoreStatus.Applied, (await hung.Applied).Status);
        Assert.Equal(4, await o.JobsAsync());
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'"));

        // A new relay reclaims the leases after they expire; the destination answers from the late group's receipts or mappings.
        await using var second = await o.OpenHost(identity: "relay2");
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
            Assert.Equal(1, await o.MappingsFor(message.MessageId));
        }

        Assert.Equal(4, await o.JobsAsync());
        Assert.Equal(4, second.Relay.Delivered);
    }

    static async Task WaitAsync(Func<bool> condition, string what, int seconds = 30)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(seconds);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting for " + what);
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Holds grouped calls until released, then applies them on a store that outlives the relay (ignoring the relay's cancellation,
    /// as a database keeps running a statement whose client went away). Single sends pass straight through.
    /// </summary>
    sealed class HungGroupDestination(PgStore inner) : IOutboxDestinationStore, IOutboxGroupDestinationStore
    {
        readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource<StoreReply<OutboxGroupDelivery>> _applied = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _groupCalls;

        public int GroupCalls => Volatile.Read(ref _groupCalls);

        /// <summary>The first held group's reply, once it has applied.</summary>
        public Task<StoreReply<OutboxGroupDelivery>> Applied => _applied.Task;

        public void Release() => _release.TrySetResult();

        public Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct) =>
            inner.ReceiveOutboxAsync(command, ct);

        public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
            inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);

        public async Task<StoreReply<OutboxGroupDelivery>> ReceiveOutboxGroupAsync(IReadOnlyList<StoreCommand<ReceiveOutboxRequest>> commands, CancellationToken ct)
        {
            Interlocked.Increment(ref _groupCalls);
            await _release.Task;
            try
            {
                var reply = await inner.ReceiveOutboxGroupAsync(commands, CancellationToken.None);
                _applied.TrySetResult(reply);
                return reply;
            }
            catch (Exception ex)
            {
                _applied.TrySetException(ex);
                throw;
            }
        }
    }

    /// <summary>A real destination store whose group replies are lost after the group applied (the relay must send alone, not resend a group).</summary>
    sealed class LossyGroupDestination(PgStore inner) : IOutboxDestinationStore, IOutboxGroupDestinationStore
    {
        int _groupCalls;

        public int LoseRepliesAfterApplying { get; init; }

        public int GroupCalls => Volatile.Read(ref _groupCalls);

        public Task<StoreReply<OutboxDelivery>> ReceiveOutboxAsync(StoreCommand<ReceiveOutboxRequest> command, CancellationToken ct) =>
            inner.ReceiveOutboxAsync(command, ct);

        public Task<OutboxLookup> FindDeliveryAsync(string application, OutboxSourceIdentity source, byte[] deliveryFingerprint, CancellationToken ct) =>
            inner.FindDeliveryAsync(application, source, deliveryFingerprint, ct);

        public async Task<StoreReply<OutboxGroupDelivery>> ReceiveOutboxGroupAsync(IReadOnlyList<StoreCommand<ReceiveOutboxRequest>> commands, CancellationToken ct)
        {
            var reply = await inner.ReceiveOutboxGroupAsync(commands, ct);
            if (Interlocked.Increment(ref _groupCalls) <= LoseRepliesAfterApplying)
            {
                throw new StoreOutcomeUnknownException("commit_outcome_unknown", commands[0].CommandId, OutboxOperations.ReceiveOutbox, new IOException("Injected loss of the group reply."));
            }

            return reply;
        }
    }
}
