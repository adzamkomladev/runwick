using Microsoft.Data.Sqlite;
using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;

namespace Runwick.Outbox.Tests;

/// <summary>OUTBOX-01 and the TX-01 … TX-04 analogues for outbox Stage on both sources (14 §9.2).</summary>
public sealed class StagingTests
{
    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-01")]
    public async Task Rollback_removes_business_and_outbox_writes_and_Stage_never_contacts_the_destination(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox01", kind, StoreProfile.DirectPrimaryViaProxy);
        var ready = await o.OpenHost(start: false);
        var storeId = ready.Runtime.StoreId;
        var before = await o.Destination.ReadCommittedSnapshot();

        // The destination is unreachable for the whole staging phase; Stage still succeeds with a configured store ID.
        o.Destination.Proxy!.DropEverything = true;
        var offline = await o.OpenHost(identity: "stager", destination: new OutboxDestination { StoreId = storeId }, ensureReady: false, start: false);
        var rolledBack = await o.StageAsync(offline, new PingJob("rollback"), commit: false);
        var committed = await o.StageAsync(offline, new PingJob("commit"));

        Assert.Equal(o.SourceStoreId, committed.SourceStoreId);
        Assert.Equal(storeId, committed.DestinationStoreId);
        Assert.False(committed.Existing);
        Assert.Null(await o.StateAsync(rolledBack.MessageId));
        Assert.Equal("pending", await o.StateAsync(committed.MessageId));
        Assert.Equal(1, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));

        // No job, receipt, mapping or any other destination row was written by Stage.
        o.Destination.Proxy.DropEverything = false;
        Assert.Empty(before.DiffTables(await o.Destination.ReadCommittedSnapshot()));

        // Without a configured store ID and before the first handshake, Stage fails before any write.
        await using var business = await o.BeginBusinessAsync();
        var rows = await business.CountRowsAsync();
        var unknown = await o.OpenHost(identity: "cold", ensureReady: false, start: false);
        var ex = await Assert.ThrowsAsync<OutboxStagingException>(() => unknown.StageAsync(new PingJob("cold"), business));
        Assert.Equal("outbox_destination_unknown", ex.Code);
        Assert.Equal(rows, await business.CountRowsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-01")]
    public async Task Business_row_and_staged_message_roll_back_together(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("otx01", kind);
        var host = await o.OpenHost();
        var messageId = Guid.CreateVersion7();
        var options = new OutboxStageOptions
        {
            MessageId = messageId,
            Dispatch = new DispatchOptions { Uniqueness = new DispatchUniqueness("k", "w", TimeSpan.FromHours(1)) },
        };

        await o.StageAsync(host, new KeyedJob("a", "k"), options, commit: false, businessRow: "row-1");
        Assert.Null(await o.StateAsync(messageId));
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));
        await Task.Delay(500);
        Assert.Equal(0, await o.JobsAsync());
        Assert.Equal(0, await o.MappingsAsync());

        // The rolled-back ID was never staged: staging it again is fresh, and is then delivered once.
        var again = await o.StageAsync(host, new KeyedJob("a", "k"), options, businessRow: "row-1");
        Assert.False(again.Existing);
        await o.WaitStateAsync(messageId, "delivered");
        Assert.Equal(1, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-02")]
    public async Task Stage_leaves_the_caller_transaction_open_and_commits_nothing_itself(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("otx02", kind);
        var host = await o.OpenHost(start: false);
        var receipts = await o.Destination.CountAsync("SELECT count(*) FROM runwick.storage_commands");
        await using var business = await o.BeginBusinessAsync();
        await business.InsertRowAsync("row-1");
        var staged = await host.StageAsync(new PingJob("x"), business);

        // The caller can keep working on the same transaction; nothing is visible elsewhere until it commits.
        await business.InsertRowAsync("row-2");
        Assert.Equal(2, await business.CountRowsAsync());
        if (kind == SourceKind.Postgres)
        {
            Assert.Null(await o.StateAsync(staged.MessageId));
            Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));
        }

        await business.Transaction.CommitAsync();
        Assert.Equal("pending", await o.StateAsync(staged.MessageId));
        Assert.Equal(2, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));

        // StagedOutboxMessage carries source/message identity and the absolute cutoff only; no queue receipt exists.
        Assert.Equal(staged.StagedAt + TimeSpan.FromDays(7), staged.AcceptUntil);
        Assert.Equal(0, await o.JobsAsync());
        Assert.Equal(receipts, await o.Destination.CountAsync("SELECT count(*) FROM runwick.storage_commands"));

        // Restaging the same ID with the same intent is idempotent; changed intent is a conflict that leaves the transaction usable.
        await using var retry = await o.BeginBusinessAsync();
        var same = await host.StageAsync(new PingJob("x"), retry, new OutboxStageOptions { MessageId = staged.MessageId });
        Assert.True(same.Existing);
        Assert.Equal(staged.ProposedJobId, same.ProposedJobId);
        Assert.Equal(staged.AcceptUntil, same.AcceptUntil);
        await Assert.ThrowsAsync<OutboxMessageConflictException>(() =>
            host.StageAsync(new PingJob("changed"), retry, new OutboxStageOptions { MessageId = staged.MessageId }));
        await retry.InsertRowAsync("row-3");
        await retry.Transaction.CommitAsync();
        Assert.Equal(3, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));
        Assert.Equal(1, await o.SourceCountAsync("SELECT count(*) FROM {messages}"));
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-03")]
    public async Task Caller_transaction_failure_surfaces_unchanged_and_a_whole_retry_stages_once(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("otx03", kind);
        var host = await o.OpenHost();
        var messageId = Guid.CreateVersion7();
        var options = new OutboxStageOptions { MessageId = messageId };

        if (kind == SourceKind.Postgres)
        {
            // Two SERIALIZABLE callers with a read/write conflict on business rows, each staging the same message ID.
            await using var c1 = new NpgsqlConnection(o.SourceConnectionString);
            await using var c2 = new NpgsqlConnection(o.SourceConnectionString);
            await c1.OpenAsync();
            await c2.OpenAsync();
            await using var t1 = await c1.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);
            await using var t2 = await c2.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);
            var b1 = new BusinessTransaction(c1, t1);
            var b2 = new BusinessTransaction(c2, t2);
            Assert.Equal(0, await b1.CountRowsAsync());
            Assert.Equal(0, await b2.CountRowsAsync());
            await b1.InsertRowAsync("r1");
            await b2.InsertRowAsync("r2");
            await host.StageAsync(new PingJob("tx03"), b1, options);
            await t1.CommitAsync();

            // The loser blocks on the winner's message key, then fails with the provider's own error: no internal retry.
            var ex = await Record.ExceptionAsync(async () =>
            {
                await host.StageAsync(new PingJob("tx03"), b2, options);
                await t2.CommitAsync();
            });
            var pg = Assert.IsType<PostgresException>(ex is OutboxStagingException { InnerException: PostgresException inner } ? inner : ex);
            Assert.Equal("40001", pg.SqlState);
        }
        else
        {
            // SQLite: a business constraint failure after Stage; the caller rolls back and retries the whole unit.
            await using (var b = await o.BeginBusinessAsync())
            {
                await b.InsertRowAsync("dup");
                await host.StageAsync(new PingJob("tx03"), b, options);
                var ex = await Assert.ThrowsAsync<SqliteException>(() => b.InsertRowAsync("dup"));
                Assert.Equal(19, ex.SqliteErrorCode);
                await b.Transaction.RollbackAsync();
            }

            Assert.Null(await o.StateAsync(messageId));
            var retried = await o.StageAsync(host, new PingJob("tx03"), options, businessRow: "dup");
            Assert.False(retried.Existing);
        }

        await o.WaitStateAsync(messageId, "delivered");
        Assert.Equal(1, await o.SourceCountAsync("SELECT count(*) FROM {messages}"));
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsFor(messageId));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-04")]
    public async Task Lost_caller_commit_reply_makes_no_success_claim_and_the_relay_resolves_by_message_identity()
    {
        // PostgreSQL source only: SQLite is same-host and has no lost-COMMIT-reply mode (14 §9.2, not-applicable).
        await using var o = await OutboxFixture.StartAsync("otx04", SourceKind.Postgres, StoreProfile.DirectPrimaryViaProxy);
        var proxy = o.Destination.Proxy!;
        var host = await o.OpenHost();
        var businessCs = proxy.Rewrite(o.SourceConnectionString);

        // (a) COMMIT applied, reply dropped: the caller sees an error; the message exists and is delivered once.
        var committedId = Guid.CreateVersion7();
        await using (var c = new NpgsqlConnection(businessCs))
        {
            await c.OpenAsync();
            await using var t = await c.BeginTransactionAsync();
            var b = new BusinessTransaction(c, t);
            await b.InsertRowAsync("a");
            await host.StageAsync(new PingJob("a"), b, new OutboxStageOptions { MessageId = committedId });
            _ = proxy.ArmCommitReplyDrop(1, PgFaultProxy.Role("business", "outbox"));
            await Assert.ThrowsAnyAsync<Exception>(() => t.CommitAsync());
        }

        Assert.Contains("COMMIT", proxy.DroppedCommitTags);
        await o.WaitStateAsync(committedId, "delivered");
        Assert.Equal(1, await o.MappingsFor(committedId));

        // (b) The connection is severed before COMMIT is sent: nothing is visible and nothing is ever delivered.
        var severedId = Guid.CreateVersion7();
        await using (var c = new NpgsqlConnection(businessCs))
        {
            await c.OpenAsync();
            await using var t = await c.BeginTransactionAsync();
            var b = new BusinessTransaction(c, t);
            await b.InsertRowAsync("b");
            await host.StageAsync(new PingJob("b"), b, new OutboxStageOptions { MessageId = severedId });
            proxy.Sever(PgFaultProxy.Role("business", "outbox"));
            await Assert.ThrowsAnyAsync<Exception>(() => t.CommitAsync());
        }

        await Task.Delay(1000);
        Assert.Null(await o.StateAsync(severedId));
        Assert.Equal(0, await o.MappingsFor(severedId));
        Assert.Equal(1, await o.JobsAsync());
    }
}
