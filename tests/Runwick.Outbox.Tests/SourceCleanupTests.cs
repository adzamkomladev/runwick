using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Outbox.Postgres;
using Runwick.Outbox.Sqlite;

namespace Runwick.Outbox.Tests;

/// <summary>
/// Source-outbox retention on the relay side (17 §6) on both sources: delivered rows past both bounds are deleted in
/// bounded batches; pending, blocked and younger delivered rows stay. Rows are aged with admin SQL (17 §8 "seed").
/// </summary>
public sealed class SourceCleanupTests
{
    /// <summary>Marks a staged row delivered <paramref name="deliveredHoursAgo"/> with AcceptUntil <paramref name="acceptHoursAgo"/> in the past.</summary>
    static Task AgeAsync(OutboxFixture o, Guid messageId, int deliveredHoursAgo, int acceptHoursAgo, string state = "delivered")
    {
        var delivered = state == "delivered";
        var sql = o.Kind == SourceKind.Postgres
            ? $"""
              UPDATE {"{messages}"} SET state = '{state}',
                  delivered_job_id = {(delivered ? "proposed_job_id" : "NULL")}, delivered_disposition = {(delivered ? "'Created'" : "NULL")},
                  confirmed_profile = {(delivered ? "'test'" : "NULL")}, delivered_at = {(delivered ? $"clock_timestamp() - interval '{deliveredHoursAgo} hours'" : "NULL")},
                  blocked_reason = {(delivered ? "NULL" : "'test'")},
                  staged_at = clock_timestamp() - interval '{acceptHoursAgo + 24} hours', accept_until = clock_timestamp() - interval '{acceptHoursAgo} hours'
              WHERE message_id = {"{m}"}
              """
            : $"""
              UPDATE {"{messages}"} SET state = '{state}',
                  delivered_job_id = {(delivered ? "proposed_job_id" : "NULL")}, delivered_disposition = {(delivered ? "'Created'" : "NULL")},
                  confirmed_profile = {(delivered ? "'test'" : "NULL")},
                  delivered_at = {(delivered ? $"CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER) - {deliveredHoursAgo} * 3600000" : "NULL")},
                  blocked_reason = {(delivered ? "NULL" : "'test'")},
                  staged_at = CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER) - {acceptHoursAgo + 24} * 3600000,
                  accept_until = CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER) - {acceptHoursAgo} * 3600000
              WHERE message_id = {"{m}"}
              """;
        return o.SourceExecAsync(o.Sql(sql), messageId);
    }

    static Task<long> RowsAsync(OutboxFixture o) => o.SourceCountAsync("SELECT count(*) FROM {messages}");

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "OPS-04")]
    public async Task Delivered_rows_past_both_bounds_go_in_bounded_batches_and_nothing_else_does(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("srcclean", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var old = new List<Guid>();
        for (var i = 0; i < 130; i++)
        {
            var m = (await o.StageAsync(stager, new PingJob("old" + i))).MessageId;
            await AgeAsync(o, m, deliveredHoursAgo: 8 * 24, acceptHoursAgo: 25);
            old.Add(m);
        }

        var youngDelivery = (await o.StageAsync(stager, new PingJob("young"))).MessageId;
        await AgeAsync(o, youngDelivery, deliveredHoursAgo: 6 * 24, acceptHoursAgo: 25);
        var openHorizon = (await o.StageAsync(stager, new PingJob("horizon"))).MessageId;
        await AgeAsync(o, openHorizon, deliveredHoursAgo: 8 * 24, acceptHoursAgo: 23);
        var blocked = (await o.StageAsync(stager, new PingJob("blocked"))).MessageId;
        await AgeAsync(o, blocked, 0, acceptHoursAgo: 30 * 24, state: "blocked");
        var pending = (await o.StageAsync(stager, new PingJob("pending"))).MessageId;
        await using var source = await o.NewSourceAsync();

        if (kind == SourceKind.Postgres)
        {
            // A row another session holds is skipped, never waited for.
            await using var holder = new NpgsqlConnection(o.SourceConnectionString);
            await holder.OpenAsync();
            await using var tx = await holder.BeginTransactionAsync();
            await using (var lockAll = new NpgsqlCommand("SELECT 1 FROM runwick_outbox.outbox_messages FOR UPDATE", holder, tx))
            {
                await lockAll.ExecuteNonQueryAsync();
            }

            Assert.Equal(0, await source.PurgeDeliveredAsync(100, 1 << 20, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));
            await tx.RollbackAsync();
        }

        // The byte budget stops a batch after its first row; then 100, then the remaining 29, then nothing.
        Assert.Equal(1, await source.PurgeDeliveredAsync(100, 1, CancellationToken.None));
        Assert.Equal(100, await source.PurgeDeliveredAsync(OutboxRelay.CleanupRows, OutboxRelay.CleanupBytes, CancellationToken.None));
        Assert.Equal(29, await source.PurgeDeliveredAsync(OutboxRelay.CleanupRows, OutboxRelay.CleanupBytes, CancellationToken.None));
        Assert.Equal(0, await source.PurgeDeliveredAsync(OutboxRelay.CleanupRows, OutboxRelay.CleanupBytes, CancellationToken.None));

        Assert.Equal(4, await RowsAsync(o));
        foreach (var m in old)
        {
            Assert.Null(await o.StateAsync(m));
        }

        Assert.Equal(("delivered", "delivered", "blocked", "pending"),
            (await o.StateAsync(youngDelivery), await o.StateAsync(openHorizon), await o.StateAsync(blocked), await o.StateAsync(pending)));
    }

    [Theory]
    [MemberData(nameof(OutboxData.SourcesAndStores), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-021")]
    [Trait("Case", "CLN-05")]
    public async Task The_relay_runs_cleanup_on_its_own_and_version_1_sources_upgrade_in_place(SourceKind kind, StoreKind store)
    {
        await using var o = await OutboxFixture.StartAsync("srcloop", kind, store: store);

        // Schema version 2 (the delivered index); a version 1 source is still readable and InstallAsync upgrades it.
        Assert.Equal(2, await o.SourceCountAsync("SELECT schema_version FROM {metadata}"));
        await o.SourceExecAsync(o.Sql("UPDATE {metadata} SET schema_version = 1"));
        await using (var v1 = await o.NewSourceAsync())
        {
            Assert.Equal(o.SourceStoreId, v1.SourceStoreId);
        }

        var again = kind == SourceKind.Postgres
            ? await PostgresOutboxSchema.InstallAsync(o.SourceConnectionString)
            : await SqliteOutboxSchema.InstallAsync(o.SourceConnectionString);
        Assert.Equal(o.SourceStoreId, again);
        Assert.Equal(2, await o.SourceCountAsync("SELECT schema_version FROM {metadata}"));
        Assert.Equal(1, await o.SourceCountAsync(kind == SourceKind.Postgres
            ? "SELECT count(*) FROM pg_indexes WHERE schemaname = 'runwick_outbox' AND indexname = 'outbox_messages_delivered'"
            : "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'runwick_outbox_messages_delivered'"));

        var stager = await o.OpenHost(identity: "stager", start: false);
        var gone = (await o.StageAsync(stager, new PingJob("gone"))).MessageId;
        await AgeAsync(o, gone, deliveredHoursAgo: 8 * 24, acceptHoursAgo: 25);
        var kept = (await o.StageAsync(stager, new PingJob("kept"))).MessageId;
        await AgeAsync(o, kept, deliveredHoursAgo: 1, acceptHoursAgo: 25);

        // A started relay runs its first cleanup batch at once.
        var relay = await o.OpenHost(identity: "relay");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (await o.StateAsync(gone) is not null || Interlocked.Read(ref relay.Relay.Purged) == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "the relay never deleted the old delivered row");
            await Task.Delay(50);
        }

        Assert.Equal(1, Interlocked.Read(ref relay.Relay.Purged));
        Assert.Equal("delivered", await o.StateAsync(kept));
    }
}
