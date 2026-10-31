using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Outbox.Tests;

/// <summary>CMD-01 … CMD-05 on <c>ReceiveOutbox/1</c> and ISO-01 for delivery identities (14 §9.4). Destination protocol only.</summary>
public sealed class ReceiveOutboxProtocolTests
{
    static Task<long> ReceiptsFor(OutboxFixture o, Guid commandId) =>
        o.Destination.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    static async Task<OutboxFixture> StartAsync(string name) => await OutboxFixture.StartAsync(name, SourceKind.Postgres);

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "CMD-01")]
    public async Task Same_command_concurrent_and_sequential_replay_applies_once()
    {
        await using var o = await StartAsync("ocmd01");
        var h1 = await o.OpenHost(identity: "p1", start: false);
        var h2 = await o.OpenHost(identity: "p2", start: false);
        var prepared = h1.Prepared.Prepare(new PingJob("x"));
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var acceptUntil = DateTimeOffset.UtcNow.AddHours(1);
        var command = Transport.Command(prepared, identity, acceptUntil);
        var other = Transport.Command(prepared, identity, acceptUntil);

        // (a) Two connections race the same command; a third, different command for the same delivery identity
        // serializes on the delivery identity lock and resolves to the same mapping.
        var hold = o.Destination.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(command.CommandId));
        var first = h1.Store.ReceiveOutboxAsync(command, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(hold);
        var second = h2.Store.ReceiveOutboxAsync(command, CancellationToken.None);
        var sibling = h2.Store.ReceiveOutboxAsync(other, CancellationToken.None);
        await Task.Delay(300);
        Assert.False(second.IsCompleted);
        Assert.False(sibling.IsCompleted);
        DriverFixture.ReleaseBarrier(hold);
        var replies = new List<StoreReply<OutboxDelivery>> { await first, await second };

        // (b) Sequential replays.
        for (var i = 0; i < 3; i++)
        {
            replies.Add(await h1.Store.ReceiveOutboxAsync(command, CancellationToken.None));
        }

        Assert.All(replies, r =>
        {
            Assert.Equal(StoreStatus.Applied, r.Status);
            Assert.Equal(StoreEffect.Applied, r.Effect);
            Assert.Equal(prepared.Intent.ProposedJobId, r.Value!.JobId);
            Assert.True(r.Value.FirstDelivery);
            Assert.Equal(replies[0].DecidedAt, r.DecidedAt);
        });
        Assert.Single(replies, r => !r.Replayed);

        var siblingReply = await sibling;
        Assert.Equal(StoreStatus.Applied, siblingReply.Status);
        Assert.False(siblingReply.Value!.FirstDelivery);
        Assert.Equal(prepared.Intent.ProposedJobId, siblingReply.Value.JobId);

        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsAsync());
        Assert.Equal(1, await ReceiptsFor(o, command.CommandId));
        Assert.Equal(1, await o.Destination.CountAsync("SELECT count(*) FROM runwick.job_generations"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "CMD-02")]
    public async Task Same_command_id_with_changed_body_deadline_operation_or_identity_conflicts()
    {
        await using var o = await StartAsync("ocmd02");
        var a = await o.OpenHost(identity: "a", start: false);
        var prepared = a.Prepared.Prepare(new PingJob("x"));
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var acceptUntil = Transport.Ms(DateTimeOffset.UtcNow.AddHours(1));
        var command = Transport.Command(prepared, identity, acceptUntil);
        Assert.Equal(StoreStatus.Applied, (await a.Store.ReceiveOutboxAsync(command, CancellationToken.None)).Status);
        var before = await o.Destination.ReadCommittedSnapshot();

        var changed = new[]
        {
            // (i) changed cutoff (a different delivery fingerprint)
            Transport.Command(prepared, identity, acceptUntil.AddMinutes(1), command.CommandId, command.NotAfter),
            // (ii) changed NotAfter only
            Transport.With(command, notAfter: command.NotAfter.AddSeconds(-1)),
            // (iii) a different source message under the same command ID
            Transport.Command(prepared, Transport.Identity(o.SourceStoreId, Guid.CreateVersion7()), acceptUntil, command.CommandId, command.NotAfter),
        };
        foreach (var c in changed)
        {
            var reply = await a.Store.ReceiveOutboxAsync(c, CancellationToken.None);
            Assert.Equal(StoreStatus.CommandConflict, reply.Status);
            Assert.Equal(StoreEffect.ObservedNoEffect, reply.Effect);
        }

        // (iv) a different operation (ordinary Dispatch) under the same command ID
        var dispatch = await a.Store.DispatchAsync(new StoreCommand<DispatchRequest>(command.Application, command.CommandId, command.NotAfter,
            new DispatchRequest(new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), prepared.StoreId, prepared.Digests.Command)),
            CancellationToken.None);
        Assert.Equal(StoreStatus.CommandConflict, dispatch.Status);
        Assert.Empty(before.DiffTables(await o.Destination.ReadCommittedSnapshot()));

        // The same command ID in application b applies independently and never sees a's decision.
        var b = await o.OpenHost(identity: "b", application: o.Destination.Application + "_b", start: false);
        var pb = b.Prepared.Prepare(new PingJob("x"));
        var replyB = await b.Store.ReceiveOutboxAsync(Transport.Command(pb, identity, acceptUntil, command.CommandId), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, replyB.Status);
        Assert.Equal(pb.Intent.ProposedJobId, replyB.Value!.JobId);
        Assert.True(replyB.Value.FirstDelivery);
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.JobsAsync(b.Runtime.Registry.Application));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "CMD-03")]
    public async Task Committed_receive_with_a_dropped_reply_replays_and_the_relay_resends_the_same_command()
    {
        await using var o = await StartAsync("ocmd03");
        var host = await o.OpenHost(start: false);
        var prepared = host.Prepared.Prepare(new PingJob("x"));
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var command = Transport.Command(prepared, identity, DateTimeOffset.UtcNow.AddHours(1));

        var drop = o.Destination.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(command.CommandId));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => host.Store.ReceiveOutboxAsync(command, CancellationToken.None));
        Assert.Equal(1, drop.Fired);
        var retried = await host.Store.ReceiveOutboxAsync(command, CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, retried.Status);
        Assert.True(retried.Replayed);
        Assert.True(retried.Value!.FirstDelivery);
        Assert.Equal(prepared.Intent.ProposedJobId, retried.Value.JobId);
        Assert.Equal(1, await ReceiptsFor(o, command.CommandId));

        // Relay variant: the first reply of its transport command is lost; it resends the same command ID.
        var relayDrop = o.Destination.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(OutboxOperations.ReceiveOutbox));
        var staged = await o.StageAsync(host, new PingJob("relay"));
        await host.StartRelayAsync();
        await o.WaitStateAsync(staged.MessageId, "delivered");
        Assert.Equal(1, relayDrop.Fired);
        Assert.True(host.Relay.Sends >= 2);
        Assert.Equal(2, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsFor(staged.MessageId));
        Assert.Equal(2, await o.Destination.CountAsync(
            "SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND operation = 'ReceiveOutbox'", ("a", o.Destination.Application)));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "CMD-04")]
    public async Task Deadline_expiring_while_waiting_for_the_delivery_identity_lock_applies_nothing()
    {
        await using var o = await StartAsync("ocmd04");
        var h1 = await o.OpenHost(identity: "p1", start: false);
        var h2 = await o.OpenHost(identity: "p2", start: false);
        var prepared = h1.Prepared.Prepare(new PingJob("x"));
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var acceptUntil = DateTimeOffset.UtcNow.AddHours(1);
        var now = await o.Destination.StoreNow();
        var a = Transport.Command(prepared, identity, acceptUntil, notAfter: now.AddSeconds(2));
        var b = Transport.Command(prepared, identity, acceptUntil, notAfter: now.AddSeconds(1));

        var hold = o.Destination.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(a.CommandId));
        var first = h1.Store.ReceiveOutboxAsync(a, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(hold);
        var waiting = h2.Store.ReceiveOutboxAsync(b, CancellationToken.None);
        while (await o.Destination.StoreNow() <= b.NotAfter)
        {
            await Task.Delay(50);
        }

        DriverFixture.ReleaseBarrier(hold);
        var applied = await first;
        var expired = await waiting;
        Assert.Equal(StoreStatus.Applied, applied.Status);
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, expired.Effect);
        Assert.True(expired.DecidedAt >= b.NotAfter);
        Assert.Equal(0, await ReceiptsFor(o, b.CommandId));

        // (c) After A's own NotAfter, replaying A returns its recorded receipt (an existing result is read first).
        while (await o.Destination.StoreNow() <= a.NotAfter)
        {
            await Task.Delay(50);
        }

        var replay = await h1.Store.ReceiveOutboxAsync(a, CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Equal(applied.DecidedAt, replay.DecidedAt);
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsAsync());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "CMD-05")]
    public async Task Pruned_transport_receipt_never_reapplies_and_a_new_command_resolves_through_the_mapping()
    {
        await using var o = await StartAsync("ocmd05");
        var host = await o.OpenHost(start: false);
        var prepared = host.Prepared.Prepare(new PingJob("x"));
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var acceptUntil = DateTimeOffset.UtcNow.AddHours(1);
        var p = Transport.Command(prepared, identity, acceptUntil, notAfter: (await o.Destination.StoreNow()).AddSeconds(2));
        var applied = await host.Store.ReceiveOutboxAsync(p, CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, applied.Status);
        while (await o.Destination.StoreNow() <= p.NotAfter)
        {
            await Task.Delay(50);
        }

        Assert.True(await o.Destination.PruneReceipts("durable", DateTimeOffset.UtcNow.AddDays(30)) >= 1);
        var replay = await host.Store.ReceiveOutboxAsync(p, CancellationToken.None);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, replay.Effect);

        // The narrow outbox rule (14 §6.2): a new command under the same delivery identity and cutoff is permitted
        // and returns the retained mapping, never a second job.
        var q = await host.Store.ReceiveOutboxAsync(Transport.Command(prepared, identity, acceptUntil), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, q.Status);
        Assert.False(q.Value!.FirstDelivery);
        Assert.Equal(applied.Value!.JobId, q.Value.JobId);
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsAsync());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "ISO-01")]
    public async Task Delivery_identities_are_isolated_per_application()
    {
        await using var o = await StartAsync("oiso01");
        var a = await o.OpenHost(identity: "a", start: false);
        var b = await o.OpenHost(identity: "b", application: o.Destination.Application + "_b", start: false);
        var appB = b.Runtime.Registry.Application;
        var identity = Transport.Identity(o.SourceStoreId, Guid.CreateVersion7());
        var acceptUntil = Transport.Ms(DateTimeOffset.UtcNow.AddHours(1));
        var commandId = Guid.CreateVersion7();
        var pa = a.Prepared.Prepare(new PingJob("x"));
        var pb = b.Prepared.Prepare(new PingJob("x"));

        var ra = await a.Store.ReceiveOutboxAsync(Transport.Command(pa, identity, acceptUntil, commandId), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, ra.Status);

        // b has never seen this (source, message): lookups and conflicts never cross namespaces.
        var lookup = await b.Store.FindDeliveryAsync(appB, identity, Transport.Fingerprint(pb, identity, acceptUntil), CancellationToken.None);
        Assert.Null(lookup.Delivery);
        Assert.False(lookup.Conflict);
        var rb = await b.Store.ReceiveOutboxAsync(Transport.Command(pb, identity, acceptUntil, commandId), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, rb.Status);
        Assert.True(rb.Value!.FirstDelivery);
        Assert.NotEqual(ra.Value!.JobId, rb.Value.JobId);

        // A conflicting delivery (changed cutoff) in b is rejected in b only; a's mapping is untouched.
        var conflict = await b.Store.ReceiveOutboxAsync(Transport.Command(pb, identity, acceptUntil.AddMinutes(5)), CancellationToken.None);
        Assert.Equal(StoreStatus.CommandConflict, conflict.Status);
        Assert.Equal(rb.Value.JobId, conflict.Error?.ConflictingJobId);
        var stillA = await a.Store.FindDeliveryAsync(a.Runtime.Registry.Application, identity, Transport.Fingerprint(pa, identity, acceptUntil), CancellationToken.None);
        Assert.Equal(ra.Value.JobId, stillA.Delivery!.JobId);
        Assert.Equal(1, await o.MappingsAsync());
        Assert.Equal(1, await o.MappingsAsync(appB));
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.JobsAsync(appB));
    }
}
