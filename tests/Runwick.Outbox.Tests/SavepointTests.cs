using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Outbox.Postgres;
using Runwick.Outbox.Sqlite;

namespace Runwick.Outbox.Tests;

/// <summary>
/// Stage's savepoint on the caller's transaction (14 §2.1 steps 2 and 5), from the SP-017 review of the SQLite
/// source's async savepoint use: a Stage that is cancelled after its INSERT, or that fails inside the savepoint, leaves
/// the caller's transaction exactly as it was and still usable, and the provider's own error surfaces unchanged.
/// </summary>
public sealed class SavepointTests
{
    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-03")]
    public async Task A_stage_cancelled_after_its_insert_leaves_nothing_staged_in_the_caller_transaction(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("osave", kind);
        var host = await o.OpenHost(start: false);
        await using var business = await o.BeginBusinessAsync();
        await business.InsertRowAsync("before");
        using var cts = new CancellationTokenSource();
        var messageId = Guid.CreateVersion7();

        // Cancel once the message row is inserted: SQLite through a temp trigger calling back into the test (the
        // remaining awaits then see a cancelled token); PostgreSQL by a trigger that sleeps while the token fires.
        if (kind == SourceKind.Sqlite)
        {
            var connection = (SqliteConnection)business.Connection;
            connection.CreateFunction("runwick_test_cancel", () =>
            {
                cts.Cancel();
                return 0;
            });
            await using var trigger = connection.CreateCommand();
            trigger.Transaction = (SqliteTransaction)business.Transaction;
            trigger.CommandText = """
                CREATE TEMP TRIGGER runwick_test_cancel_after_stage AFTER INSERT ON main.runwick_outbox_messages
                BEGIN SELECT runwick_test_cancel(); END
                """;
            await trigger.ExecuteNonQueryAsync();
        }
        else
        {
            await o.SourceExecAsync("""
                CREATE FUNCTION runwick_outbox.test_sleep() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_sleep(5); RETURN NEW; END $$;
                CREATE TRIGGER test_sleep AFTER INSERT ON runwick_outbox.outbox_messages FOR EACH ROW EXECUTE FUNCTION runwick_outbox.test_sleep();
                """);
            cts.CancelAfter(TimeSpan.FromMilliseconds(500));
        }

        var options = new OutboxStageOptions { MessageId = messageId };
        Task Stage() => kind == SourceKind.Sqlite
            ? host.Services.GetRequiredService<ISqliteOutbox>().StageAsync(new PingJob("cancelled"), (SqliteTransaction)business.Transaction, options, cts.Token)
            : host.Services.GetRequiredService<IPostgresOutbox>().StageAsync(new PingJob("cancelled"), (NpgsqlTransaction)business.Transaction, options, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(Stage);

        // The caller's transaction is as it was: usable, holding its own row and no outbox row.
        await business.InsertRowAsync("after");
        Assert.Equal(2, await business.CountRowsAsync());
        await business.Transaction.CommitAsync();
        Assert.Equal(2, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));
        Assert.Null(await o.StateAsync(messageId));
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages}"));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "TX-03")]
    public async Task A_sqlite_error_inside_the_savepoint_surfaces_unchanged_and_the_caller_keeps_its_transaction()
    {
        await using var o = await OutboxFixture.StartAsync("osavefull", SourceKind.Sqlite);
        var host = await o.OpenHost(start: false);
        await using var business = await o.BeginBusinessAsync();
        await business.InsertRowAsync("before");

        // A message-row constraint the adapter does not map: SQLITE_CONSTRAINT (19) raised inside the savepoint.
        var connection = (SqliteConnection)business.Connection;
        await using (var trigger = connection.CreateCommand())
        {
            trigger.Transaction = (SqliteTransaction)business.Transaction;
            trigger.CommandText = """
                CREATE TEMP TRIGGER runwick_test_reject BEFORE INSERT ON main.runwick_outbox_messages
                BEGIN SELECT RAISE(ABORT, 'rejected by test'); END
                """;
            await trigger.ExecuteNonQueryAsync();
        }

        var error = await Assert.ThrowsAsync<SqliteException>(() =>
            host.Services.GetRequiredService<ISqliteOutbox>().StageAsync(new PingJob("rejected"), (SqliteTransaction)business.Transaction));
        Assert.Equal(19, error.SqliteErrorCode);
        Assert.Contains("rejected by test", error.Message, StringComparison.Ordinal);

        await business.InsertRowAsync("after");
        await business.Transaction.CommitAsync();
        Assert.Equal(2, await o.SourceCountAsync("SELECT count(*) FROM business_rows"));
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages}"));
    }
}
