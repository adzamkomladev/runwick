using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Postgres.Tests;

/// <summary>The commit-safe event cursor (08 §7): EVT-01, EVT-02.</summary>
public sealed class EventCursorTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<List<JobEvent>> DrainAsync(TestClient c, EventCursor? from, int pageSize)
    {
        var all = new List<JobEvent>();
        var cursor = from;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            var page = await c.Store.EventsSinceAsync(c.Application, cursor, pageSize, CancellationToken.None);
            Assert.True(page.Events.Count <= pageSize);
            all.AddRange(page.Events);
            if (page.Events.Count == 0)
            {
                // The horizon is cluster-wide: another database's open transaction can hold it briefly (08 §7.2).
                if (page.Partial && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(50);
                    continue;
                }

                return all;
            }

            cursor = page.Next;
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "EVT-01")]
    public async Task Rereading_from_a_cursor_returns_the_same_ordered_rows()
    {
        await using var f = await DriverFixture.StartAsync(Ns("evt01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 10; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i));
        }

        var first = await DrainAsync(c, null, 3);
        var second = await DrainAsync(c, null, 4);
        Assert.Equal(10, first.Count(e => e.Kind == "dispatched"));
        Assert.Equal(first.Select(e => e.Cursor), second.Select(e => e.Cursor));
        Assert.Equal(first.OrderBy(e => e.TxId).ThenBy(e => e.EventId).Select(e => e.Cursor), first.Select(e => e.Cursor));
        Assert.All(first, e => Assert.Equal(1, e.SchemaVersion));

        var tail = await DrainAsync(c, first[4].Cursor, 2);
        Assert.Equal(first.Skip(5).Select(e => e.EventId), tail.Select(e => e.EventId));

        // 15 §12.2 rule 4: every committed job history is accepted by the ReferenceModel.
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "EVT-02")]
    public async Task A_slow_writer_is_never_skipped_and_pages_stay_bounded()
    {
        await using var f = await DriverFixture.StartAsync(Ns("evt02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("before"));
        var start = (await DrainAsync(c, null, 100)).Last().Cursor;

        // Writer A takes its transaction ID first and stays open; writer B commits after it.
        await using var slow = new NpgsqlConnection(f.AdminConnectionString);
        await slow.OpenAsync();
        await using var tx = await slow.BeginTransactionAsync();
        using (var insert = new NpgsqlCommand("""
            INSERT INTO runwick.queue_events (application, event_kind, occurred_at, details)
            VALUES (@a, 'queue_paused', clock_timestamp(), '{"slow":true}')
            """, slow, tx))
        {
            insert.Parameters.AddWithValue("a", c.Application);
            await insert.ExecuteNonQueryAsync();
        }

        await c.Dispatcher.DispatchAsync(new PingJob("after"));

        // B is committed but beyond the horizon set by A: not returned yet, reported partial.
        var page = await c.Store.EventsSinceAsync(c.Application, start, 100, CancellationToken.None);
        Assert.Empty(page.Events);
        Assert.True(page.Partial);
        Assert.Equal(start, page.Next);

        await tx.CommitAsync();
        var events = await DrainAsync(c, start, 1);
        Assert.Equal(["queue_paused", "dispatched"], events.Select(e => e.Kind));

        // 15 §12.2 rule 4: every committed job history is accepted by the ReferenceModel.
        Assert.Empty(await f.HistoryViolations());
    }
}
