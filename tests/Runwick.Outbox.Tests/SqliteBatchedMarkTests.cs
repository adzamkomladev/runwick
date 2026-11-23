using Runwick.Harness.Jobs;
using Runwick.Outbox.Sqlite;

namespace Runwick.Outbox.Tests;

/// <summary>
/// Delivered marks that queue behind the control lane share one statement and one commit on the SQLite source too, each row
/// still guarded by its own lease token (the PostgreSQL cases are in <see cref="BatchedMarkTests"/>).
/// </summary>
public sealed class SqliteBatchedMarkTests
{
    static async Task<SqliteOutboxSource> OpenSourceAsync(OutboxFixture o)
    {
        var source = new SqliteOutboxSource(o.RelayConnectionString) { BatchMarks = true };
        await source.InitializeAsync(CancellationToken.None);
        return source;
    }

    static OutboxDelivered Delivery() => new(Guid.CreateVersion7(), "Created", Protocol.OutboxOperations.PostgresProfile, DateTimeOffset.UtcNow);

    [Fact]
    [Trait("Category", "Contract")]
    public async Task Queued_marks_share_statements_and_a_stale_token_leaves_its_row_alone()
    {
        await using var o = await OutboxFixture.StartAsync("slbatch1", SourceKind.Sqlite);
        var stager = await o.OpenHost(identity: "stager", start: false);
        for (var i = 0; i < 12; i++)
        {
            await o.StageAsync(stager, new PingJob("m" + i));
        }

        using var source = await OpenSourceAsync(o);
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
        await using var o = await OutboxFixture.StartAsync("slbatch2", SourceKind.Sqlite);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var first = await o.StageAsync(stager, new PingJob("one"));
        var second = await o.StageAsync(stager, new PingJob("two"));
        using var source = await OpenSourceAsync(o);
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
    public async Task A_relay_with_batched_marks_delivers_each_message_once()
    {
        await using var o = await OutboxFixture.StartAsync("slbatch3", SourceKind.Sqlite);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 24; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("r" + i)));
        }

        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.ClaimBatchMin = 2;
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
    }

    /// <summary>
    /// Drain timing, not a contract: set RUNWICK_OUTBOX_SQLITE_BENCH to a message count. Stages that many messages on a SQLite
    /// source, starts a four-slot relay and reports delivered per second until the backlog is empty, with marks batched or not.
    /// </summary>
    [Theory]
    [Trait("Category", "Bench")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Bench_sqlite_source_drain_delivered_per_second(bool batchMarks)
    {
        if (!int.TryParse(Environment.GetEnvironmentVariable("RUNWICK_OUTBOX_SQLITE_BENCH"), out var count) || count <= 0)
        {
            Assert.Skip("Set RUNWICK_OUTBOX_SQLITE_BENCH to a message count to run this timing.");
        }

        await using var o = await OutboxFixture.StartAsync("slbench", SourceKind.Sqlite);
        var stager = await o.OpenHost(identity: "stager", start: false);
        for (var i = 0; i < count; i++)
        {
            await o.StageAsync(stager, new PingJob("b" + i));
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        await using var relay = await o.OpenHost(identity: "relay", relay: r =>
        {
            r.MaxInFlight = 4;
            r.BatchDeliveredMarks = batchMarks;
        });
        while (await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'") < count)
        {
            Assert.True(started.Elapsed < TimeSpan.FromMinutes(10), "the backlog did not drain in 10 minutes");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        var seconds = started.Elapsed.TotalSeconds;
        TestContext.Current.TestOutputHelper?.WriteLine(FormattableString.Invariant(
            $"sqlite-source drain: batchMarks={batchMarks} messages={count} seconds={seconds:F2} delivered/s={count / seconds:F1}"));
        Assert.Equal(count, await o.JobsAsync());
    }
}
