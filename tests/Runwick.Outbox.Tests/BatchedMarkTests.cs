using Runwick.Harness.Jobs;
using Runwick.Outbox.Postgres;

namespace Runwick.Outbox.Tests;

/// <summary>
/// D2: delivered marks that queue behind the control lane share one statement (PostgreSQL source), each row still guarded by
/// its own lease token; the relay can claim in batches and free a slot when the destination answers.
/// </summary>
public sealed class BatchedMarkTests
{
    static async Task<PostgresOutboxSource> OpenSourceAsync(OutboxFixture o)
    {
        var source = new PostgresOutboxSource(o.RelayConnectionString) { BatchMarks = true };
        await source.InitializeAsync(CancellationToken.None);
        return source;
    }

    static OutboxDelivered Delivery() => new(Guid.CreateVersion7(), "Created", Protocol.OutboxOperations.PostgresProfile, DateTimeOffset.UtcNow);

    [Fact]
    [Trait("Category", "Contract")]
    public async Task Queued_marks_share_statements_and_each_keeps_its_own_lease_token()
    {
        await using var o = await OutboxFixture.StartAsync("obatch1", SourceKind.Postgres);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 12; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("m" + i)));
        }

        await using var source = await OpenSourceAsync(o);
        var leases = await source.ClaimAsync(Guid.CreateVersion7(), 12, TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.Equal(12, leases.Count);
        var stale = leases[3] with { Token = Guid.CreateVersion7() }; // a replaced owner's token: the row must not move
        var deliveries = leases.Select(_ => Delivery()).ToList();

        var results = await Task.WhenAll(leases.Select((l, i) =>
            Task.Run(() => source.MarkDeliveredAsync(i == 3 ? stale : l, deliveries[i], CancellationToken.None))));

        Assert.Equal(11, results.Count(r => r));
        Assert.False(results[3]);
        Assert.True(source.MarkBatches >= 1 && source.BatchedMarks >= 2, "queued marks were never written together");
        for (var i = 0; i < leases.Count; i++)
        {
            if (i == 3)
            {
                Assert.Equal("leased", await o.StateAsync(leases[i].MessageId));
            }
            else
            {
                Assert.Equal("delivered", await o.StateAsync(leases[i].MessageId));
                Assert.Equal(deliveries[i].JobId, await o.DeliveredJobAsync(leases[i].MessageId));
            }
        }

        // The stale owner's mark is still refused afterwards, and the real owner can still settle that row.
        Assert.False(await source.MarkDeliveredAsync(stale, Delivery(), CancellationToken.None));
        Assert.True(await source.MarkDeliveredAsync(leases[3], deliveries[3], CancellationToken.None));
        Assert.Equal(deliveries[3].JobId, await o.DeliveredJobAsync(leases[3].MessageId));
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task A_lone_mark_uses_the_single_row_statement_and_a_cancelled_caller_writes_nothing()
    {
        await using var o = await OutboxFixture.StartAsync("obatch2", SourceKind.Postgres);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var first = await o.StageAsync(stager, new PingJob("one"));
        var second = await o.StageAsync(stager, new PingJob("two"));
        await using var source = await OpenSourceAsync(o);
        var leases = await source.ClaimAsync(Guid.CreateVersion7(), 2, TimeSpan.FromSeconds(30), CancellationToken.None);
        var byId = leases.ToDictionary(l => l.MessageId);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => source.MarkDeliveredAsync(byId[first.MessageId], Delivery(), cancelled.Token));
        Assert.Equal("leased", await o.StateAsync(first.MessageId));

        // The next mark takes its turn on the lane and skips the cancelled one: only its own row is written, and no batch statement runs.
        Assert.True(await source.MarkDeliveredAsync(byId[second.MessageId], Delivery(), CancellationToken.None));
        Assert.Equal("delivered", await o.StateAsync(second.MessageId));
        Assert.Equal("leased", await o.StateAsync(first.MessageId));
        Assert.Equal(0, source.MarkBatches);
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task A_very_short_claim_wait_never_ends_the_claim_loop()
    {
        // A wait of 1 ms expires between the loop test and the wait call; a negative timeout there once threw and stopped claiming for good.
        await using var o = await OutboxFixture.StartAsync("obatch4", SourceKind.Postgres);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 40; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("w" + i)));
        }

        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.ClaimBatchMin = 4;
            r.ClaimBatchWait = TimeSpan.FromMilliseconds(1);
        });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(40, relay.Relay.Delivered);
    }

    [Fact]
    [Trait("Category", "Contract")]
    public async Task A_relay_that_claims_in_batches_and_frees_slots_on_reply_delivers_each_message_once()
    {
        await using var o = await OutboxFixture.StartAsync("obatch3", SourceKind.Postgres);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 24; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("r" + i)));
        }

        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.DeliveryGroupSize = 4;
            r.ClaimBatchMin = 2;
            r.ReleaseSlotAfterReply = true;
            r.BatchDeliveredMarks = true;
        });
        foreach (var message in staged)
        {
            await o.WaitStateAsync(message.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        Assert.Equal(24, await o.JobsAsync());
        Assert.Equal(24, await o.MappingsAsync());
        foreach (var message in staged)
        {
            Assert.Equal(message.ProposedJobId.Value, await o.DeliveredJobAsync(message.MessageId));
        }

        Assert.Equal(24, relay.Relay.Delivered);
        Assert.Equal(0, relay.Relay.InFlightCount);
        Assert.Equal(4, relay.Relay.FreeSlots()); // every slot given up on reply was counted back exactly once
    }
}
