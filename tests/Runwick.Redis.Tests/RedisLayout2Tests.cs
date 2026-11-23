using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Migrations;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Stored-data cuts (docs/plans/redis-stored-data-design.md, 22 §3.1–§3.2, REDIS-23): finish receipts kept per call, and layout
/// revision 2 (attempt records packed at close, events packed per call with one entry per claimed attempt, no null fields). What
/// replies, management reads and the event feed show is unchanged; data written under layout 1 stays readable next to the new forms.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisLayout2Tests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C, IDatabase Db, string Prefix)> OpenAsync(string name, int jobs = 8)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < jobs; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        return (f, c, (await RedisServer.AdminAsync()).GetDatabase(), RxClient.KeyPrefix(f.Application));
    }

    static IManagementQueryStore Reads(TestClient c) => c.Services.GetRequiredService<IManagementQueryStore>();

    static IJobControlStore Controls(TestClient c) => c.Services.GetRequiredService<IJobControlStore>();

    static async Task<List<LeaseHandle>> ClaimAsync(TestClient c, int n) =>
        [.. (await c.ReserveAndBeginAsync(n)).Value!.Outcomes.Select(o => o.Begun!.Lease.Handle)];

    /// <summary>The raw rows EventsSince returns (what GET /v1/events is built from), paged by <paramref name="limit"/>.</summary>
    static async Task<List<string>> FeedAsync(TestClient c, int limit)
    {
        var store = c.Services.GetRequiredService<RedisStore>();
        var rows = new List<string>();
        long after = 0;
        while (true)
        {
            var a = await store.Runner.ReadAsync(c.Application, RxScript.Read("events"), RxLane.Background, [after, limit], Ct);
            var page = a.Skip(3).Select(x => (string)x!).ToList();
            Assert.True(page.Count <= limit);
            if (page.Count == 0)
            {
                return rows;
            }

            rows.AddRange(page);
            after = JsonNode.Parse(page[^1])!["eventId"]!.GetValue<long>();
        }
    }

    // ------------------------------------------------------------------ (a) finish receipts per call

    [Fact]
    [Trait("Case", "L2-01")]
    public async Task A_finish_batch_keeps_one_receipt_per_call_and_every_resend_by_the_sender_replays_the_same_answer()
    {
        var (f, c, db, prefix) = await OpenAsync("l201");
        await using var _ = f;
        var leases = await ClaimAsync(c, 4);
        var items = leases.Take(3).Select((l, i) => c.FinishCommand(l, i == 2 ? AttemptOutcome.RetryableFailure : AttemptOutcome.Succeeded)).ToList();

        // The reply of the call is lost.
        var call = Guid.CreateVersion7();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(call));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishManyAsync(items, call));
        Assert.Equal(1, drop.Fired);

        // One receipt key for the call (field = item command id, "1|fp|NotAfter|decided|result") and one expiry entry; none per item.
        var key = prefix + "rcpt:" + RxClient.Hex(call) + ":f";
        var stored = (await db.HashGetAllAsync(key)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
        Assert.Equal(items.Select(i => RxClient.Hex(i.CommandId)).Order(), stored.Keys.Order());
        Assert.All(stored.Values, v => Assert.Matches(@"^1\|[0-9a-f]{64}\|\d+\|\d+\|\{""~"":1,", v));
        Assert.All(items, i => Assert.False(db.KeyExists(prefix + "rcpt:" + RxClient.Hex(i.CommandId))));
        Assert.Equal(items.Max(i => i.NotAfter).ToUnixTimeMilliseconds() + 86_400_000,
            (long)(await db.SortedSetScoreAsync(prefix + "rcpt:exp:durable", RxClient.Hex(call) + ":f"))!);

        // The engine resolves each item alone (05 §12.5): each resend names the call and replays the answer; the marker is written.
        var alone = new List<StoreReply<FinishedAttempt>>();
        foreach (var item in items)
        {
            var r = await c.FinishAsync(item);
            Assert.Equal((StoreStatus.Applied, true), (r.Status, r.Replayed));
            Assert.True(await db.HashExistsAsync(key, "c:" + RxClient.Hex(item.CommandId)));
            alone.Add(r);
        }

        Assert.Equal(["succeeded", "succeeded", "retrying"], alone.Select(r => r.Value!.Disposition));

        // Resent inside another batch, and the same call resent whole: the same answers, byte for byte as the snapshot shows them.
        foreach (var again in new[] { await c.FinishManyAsync(items), await c.FinishManyAsync(items, call) })
        {
            Assert.All(again.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, true), (o.Status, o.Replayed)));
            Assert.Equivalent(alone.Select(r => r.Value), again.Value.Outcomes.Select(o => o.Value), strict: true);
        }

        // A changed body under the same command id is a conflict, not a replay.
        var changed = items[0] with { Body = items[0].Body with { Outcome = AttemptOutcome.PermanentFailure } };
        Assert.Equal(StoreStatus.CommandConflict, (await c.FinishAsync(changed)).Status);

        // Another process never resends these finishes (it holds none of their leases); if it did, nothing is applied twice.
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));
        var stranger = await other.FinishAsync(items[0]);
        Assert.False(stranger.Status == StoreStatus.Applied && !stranger.Replayed);
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Equal(3, model.Attempts.Count(a => items.Any(i => i.Body.Lease.AttemptId == a.AttemptId) && a.ClosedAt is not null));
        Assert.Equal(3, model.Receipts.Count(r => items.Any(i => i.CommandId == r.CommandId)));

        // A first send of a single Finish keeps its own receipt; a FinishAndClaim's finishes go in that call's key.
        var single = c.FinishCommand(leases[3], AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(single)).Status);
        Assert.True(await db.KeyExistsAsync(prefix + "rcpt:" + RxClient.Hex(single.CommandId)));
    }

    // ------------------------------------------------------------------ (b), (e) events packed per call

    [Fact]
    [Trait("Case", "L2-02")]
    public async Task Events_are_stored_one_pack_per_call_and_the_feed_is_the_same_rows_whatever_the_page_size()
    {
        var (f, c, db, prefix) = await OpenAsync("l202");
        await using var _ = f;
        var leases = await ClaimAsync(c, 5);
        await c.FinishManyAsync([.. leases.Take(3).Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded))]);
        await c.FinishAsync(leases[3], AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromSeconds(30));
        await c.FinishAsync(leases[4], AttemptOutcome.UserCancelled);

        // 8 dispatches (8 calls, one event each), one claim of 5 (10 events, 5 entries), a FinishMany of 3 (3), two Finishes (2).
        var stored = (await db.HashGetAllAsync(prefix + "events:data")).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
        Assert.Equal(8 + 1 + 1 + 2, stored.Count);
        Assert.All(stored.Values, v => Assert.StartsWith("~1|", v, StringComparison.Ordinal));
        Assert.DoesNotContain(stored.Values, v => v.Contains("null", StringComparison.Ordinal));
        var claim = Assert.Single(stored.Values, v => v.Contains("\n~rs|", StringComparison.Ordinal));
        Assert.Equal(5, claim.Split('\n').Length - 1);
        var members = (await db.SortedSetRangeByScoreWithScoresAsync(prefix + "events")).ToList();
        Assert.Equal(stored.Keys.Order(), members.Select(m => (string)m.Element!).Order());
        Assert.All(members, m => Assert.Equal((double)long.Parse((string)m.Element!), m.Score));

        // Expanded, the rows are the layout-1 rows: ids 1..23 in order, as the harness's own expansion gives them.
        var feed = await FeedAsync(c, 1000);
        Assert.Equal(Enumerable.Range(1, 23).Select(i => (long)i), feed.Select(r => JsonNode.Parse(r)!["eventId"]!.GetValue<long>()));
        Assert.Equal(RedisEventRows.All(stored), feed);
        Assert.Equal(["attempt_reserved", "attempt_started"], feed.Skip(8).Take(2).Select(r => JsonNode.Parse(r)!["kind"]!.GetValue<string>()));
        Assert.All(feed, r => Assert.StartsWith("{\"actorId\":null,\"attemptId\":", r, StringComparison.Ordinal));
        for (var limit = 1; limit <= 12; limit++)
        {
            Assert.Equal(feed, await FeedAsync(c, limit));
        }

        // Through the store API (GET /v1/events): every event once, in order, from a cursor in the middle of a pack.
        var page = await Controls(c).EventsSinceAsync(c.Application, new EventCursor(0, 9), 4, Ct);
        Assert.Equal([10L, 11, 12, 13], page.Events.Select(e => e.EventId));
        Assert.Equal(["attempt_started", "attempt_reserved", "attempt_started", "attempt_reserved"], page.Events.Select(e => e.Kind));
    }

    // ------------------------------------------------------------------ (c) attempt records, and data from layout 1

    [Fact]
    [Trait("Case", "L2-03")]
    public async Task Attempts_and_events_written_under_layout_1_read_next_to_the_packed_forms()
    {
        var (f, c, db, prefix) = await OpenAsync("l203", jobs: 3);
        await using var _ = f;
        var leases = await ClaimAsync(c, 3);
        var job = leases[0].JobId;
        var jobKey = prefix + "job:" + RxClient.Hex(job);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(leases[0], AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(1),
            error: new AttemptError("first", "System.Exception", "m", null))).Status);

        // The closed attempt is one field; the open attempt of another job has no a: fields at all.
        var fields = (await db.HashGetAllAsync(jobKey)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
        var record = JsonNode.Parse(fields["a:" + RxClient.Hex(leases[0].AttemptId)])!.AsArray();
        Assert.Equal(13, record.Count);
        Assert.Equal(["retryable_failure", "retrying"], new[] { record[7]!.GetValue<string>(), record[8]!.GetValue<string>() });
        Assert.DoesNotContain(fields.Keys, k => k.EndsWith(":token", StringComparison.Ordinal) || k.EndsWith(":closedby", StringComparison.Ordinal));
        Assert.True(fields.ContainsKey("a:" + RxClient.Hex(leases[0].AttemptId) + ":error"));
        var open = (await db.HashGetAllAsync(prefix + "job:" + RxClient.Hex(leases[1].JobId))).Select(e => (string)e.Name!).ToList();
        Assert.Contains("ares", open);
        Assert.Contains("aauth", open);
        Assert.DoesNotContain(open, k => k.StartsWith("a:", StringComparison.Ordinal));

        // A layout-1 attempt of the same job (closed before the migration) and a layout-1 event row, as the previous version wrote them.
        var oldAttempt = Guid.CreateVersion7();
        var a = "a:" + RxClient.Hex(oldAttempt) + ":";
        var t = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();
        await db.HashSetAsync(jobKey,
        [
            new(a + "gen", "1"), new(a + "res", "0"), new(a + "worker", RxClient.Hex(c.WorkerInstanceId)), new(a + "token", "t"), new(a + "reserved", t),
            new(a + "exec", "0"), new(a + "auth", t + 1), new(a + "fin", t + 2), new(a + "outcome", "interrupted"), new(a + "disp", "retrying"),
            new(a + "elapsed", "1"), new(a + "error", "{\"code\":\"old\"}"), new(a + "closedby", "x"),
        ]);
        var next = long.Parse((string)(await db.HashGetAsync(prefix + "meta", "nextevent"))!);
        var row = "{\"actorId\":\"ops\",\"attemptId\":null,\"commandId\":\"" + RxClient.Hex(Guid.NewGuid()) + "\",\"details\":{\"x\":1},\"eventId\":" + next
            + ",\"jobId\":null,\"kind\":\"queue_paused\",\"occurredAt\":" + t + "}";
        await db.HashSetAsync(prefix + "events:data", next.ToString(), row);
        await db.SortedSetAddAsync(prefix + "events", next.ToString(), next);
        await db.HashSetAsync(prefix + "meta", "nextevent", next + 1);

        // GetJob: both attempts in reservation order, each with its values; the open-attempt reader shows the running job's attempt.
        var detail = (await Reads(c).GetJobAsync(c.Application, job, Ct)).Value!;
        Assert.Equal([oldAttempt, leases[0].AttemptId], detail.Attempts.Select(x => x.AttemptId));
        Assert.Equal(("interrupted", "old", 0, 0L), (detail.Attempts[0].Outcome, detail.Attempts[0].ErrorCode, detail.Attempts[0].ExecutionNumber ?? -1,
            detail.Attempts[0].ReservationNumber));
        Assert.Equal(("retryable_failure", "retrying", "first", 1), (detail.Attempts[1].Outcome, detail.Attempts[1].Disposition, detail.Attempts[1].ErrorCode,
            detail.Attempts[1].ExecutionNumber ?? -1));
        Assert.NotNull(detail.Attempts[1].FinishedAt);
        var running = (await Reads(c).GetJobAsync(c.Application, leases[1].JobId, Ct)).Value!.Attempts.Single();
        Assert.Equal((leases[1].AttemptId, 1, (DateTimeOffset?)null), (running.AttemptId, running.ExecutionNumber ?? -1, running.FinishedAt));
        Assert.NotNull(running.AuthorizedAt);

        // The error of either form is revealed the same way.
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Equal(4, model.Attempts.Count);

        // The feed: the layout-1 row in its place among the packs, then new packs after it.
        await c.FinishAsync(leases[1], AttemptOutcome.Succeeded);
        var feed = await FeedAsync(c, 3);
        Assert.Equal(row, feed.Single(r => r.Contains("queue_paused", StringComparison.Ordinal)));
        Assert.Equal(Enumerable.Range(1, feed.Count).Select(i => (long)i), feed.Select(r => JsonNode.Parse(r)!["eventId"]!.GetValue<long>()));
        Assert.Equal("attempt_finished", JsonNode.Parse(feed[^1])!["kind"]!.GetValue<string>());
    }

    [Fact]
    [Trait("Case", "L2-04")]
    public async Task An_attempt_reserved_under_layout_1_closes_in_its_own_form()
    {
        var (f, c, db, prefix) = await OpenAsync("l204", jobs: 1);
        await using var _ = f;
        var lease = (await ClaimAsync(c, 1)).Single();
        var jobKey = prefix + "job:" + RxClient.Hex(lease.JobId);

        // Rewrite the open attempt as layout 1 held it (a store migrated without draining).
        var j = (await db.HashGetAllAsync(jobKey)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
        var a = "a:" + RxClient.Hex(lease.AttemptId) + ":";
        await db.HashSetAsync(jobKey,
        [
            new(a + "gen", j["gen"]), new(a + "res", j["rcount"]), new(a + "worker", j["worker"]), new(a + "token", j["token"]), new(a + "reserved", j["ares"]),
            new(a + "exec", j["g:1:started"]), new(a + "auth", j["aauth"]),
        ]);
        await db.HashDeleteAsync(jobKey, ["ares", "aauth"]);

        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        var after = (await db.HashGetAllAsync(jobKey)).Select(e => (string)e.Name!).ToList();
        Assert.Contains(a + "fin", after);
        Assert.DoesNotContain("a:" + RxClient.Hex(lease.AttemptId), after);
        var attempt = (await Reads(c).GetJobAsync(c.Application, lease.JobId, Ct)).Value!.Attempts.Single();
        Assert.Equal(("succeeded", "succeeded", 1), (attempt.Outcome, attempt.Disposition, attempt.ExecutionNumber ?? -1));
    }

    // ------------------------------------------------------------------ the upgrade (23 §4)

    const int Database = 13;

    [Fact]
    [Trait("Case", "L2-05")]
    public async Task Migrate_moves_a_layout_1_store_to_2_after_every_application_and_refuses_while_an_intent_is_pending()
    {
        var config = RedisServer.Configuration + ",defaultDatabase=" + Database;
        var mux = await RedisServer.AdminAsync();
        await mux.GetServer(mux.GetEndPoints()[0]).FlushDatabaseAsync(Database);
        var db = mux.GetDatabase(Database);
        await RunwickRedisMigrations.MigrateAsync(config, Ct);
        var app = "l205_" + Guid.NewGuid().ToString("N")[..8];
        await using (var c = await TestClient.CreateAsync(RedisServer.Prefix + config, app, new HarnessFaultInjector(), r => TestJobs.Register(r)))
        {
            await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        }

        // The store as the previous version left it: layout 1 in rw:store (with that manifest) and in the application's meta copy.
        var meta = RxClient.Meta(app);
        var (manifest1, digest1) = RedisMigrator.Manifest(1);
        await db.HashSetAsync(RedisMigrator.StoreKey, [new("layout", 1), new("manifest", manifest1.ToJsonString()), new("digest", Convert.ToHexStringLower(digest1))]);
        await db.HashSetAsync(meta, "layout", "1");
        await Assert.ThrowsAsync<StoreIncompatibleException>(async () =>
        {
            await using var refused = await TestClient.CreateAsync(RedisServer.Prefix + config, app, new HarnessFaultInjector(), r => TestJobs.Register(r));
            await refused.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);
        });

        // A pending intent: refused, and rw:store stays at 1.
        await db.HashSetAsync(meta, "intent", "pending");
        Assert.Equal("intent_pending", (await Assert.ThrowsAsync<RedisMigrationException>(() => RunwickRedisMigrations.MigrateAsync(config, Ct))).Code);
        Assert.Equal("1", (string?)await db.HashGetAsync(RedisMigrator.StoreKey, "layout"));
        await db.HashSetAsync(meta, "intent", "");

        var result = await RunwickRedisMigrations.MigrateAsync(config, Ct);
        Assert.Equal(2L, result.LayoutRevision);
        Assert.Equal(["layout-2"], result.AppliedPhases);
        Assert.Equal("2", (string?)await db.HashGetAsync(RedisMigrator.StoreKey, "layout"));
        Assert.Equal(Convert.ToHexStringLower(RedisMigrator.Manifest().Digest), (string?)await db.HashGetAsync(RedisMigrator.StoreKey, "digest"));
        Assert.Equal("2", (string?)await db.HashGetAsync(meta, "layout"));
        Assert.Empty((await RunwickRedisMigrations.MigrateAsync(config, Ct)).AppliedPhases);

        await using var upgraded = await TestClient.CreateAsync(RedisServer.Prefix + config, app, new HarnessFaultInjector(), r => TestJobs.Register(r));
        await upgraded.Dispatcher.DispatchAsync(new PingJob("c"), cancellationToken: Ct);
        Assert.Equal(StoreStatus.Applied, (await upgraded.ReserveAndBeginAsync(2)).Status);
    }
}
