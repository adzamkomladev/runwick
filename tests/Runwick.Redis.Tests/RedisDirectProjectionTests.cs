using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Direct projection (22 §4.1): a command that publishes in its first script call writes its after-images without storing its intent
/// first, and stores the intent only when a write fails partway. Receipts, replays and recovery are unchanged.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisDirectProjectionTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C, IDatabase Db)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)), (await RedisServer.AdminAsync()).GetDatabase());
    }

    static async Task PendingAsync(TestClient c, int n)
    {
        for (var i = 0; i < n; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }
    }

    static async Task<StoreSnapshot> ModelAsync(DriverFixture f)
    {
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Empty(SnapshotInvariants.Violations(model));
        return model;
    }

    static async Task NoIntentAsync(IDatabase db, DriverFixture f)
    {
        var meta = RxClient.Meta(f.Application);
        Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
        Assert.False(await db.HashExistsAsync(meta, "intent")); // never stored, not merely cleared
    }

    static async Task LoseReplyAsync<T>(DriverFixture f, Guid id, Func<Task<T>> call)
    {
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(call);
        Assert.Equal(1, drop.Fired);
    }

    [Fact]
    [Trait("Case", "DP-01")]
    public async Task Worker_commands_store_no_intent_and_each_lost_reply_replays_the_same_answer()
    {
        var (f, c, db) = await OpenAsync("dp01");
        await using var _ = f;
        await PendingAsync(c, 6);
        var rev0 = (long)await db.HashGetAsync(RxClient.Meta(f.Application), "rev");

        // ReserveAndBegin
        var claimId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        await LoseReplyAsync(f, claimId, () => c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter));
        await NoIntentAsync(db, f);
        var claimed = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true), (claimed.Status, claimed.Replayed));
        var leases = claimed.Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
        var model = await ModelAsync(f);
        Assert.Equal(leases.Select(l => l.AttemptId).Order(), model.Attempts.Select(a => a.AttemptId).Order());

        // FinishMany
        var items = leases.Take(2).Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var manyId = Guid.CreateVersion7();
        await LoseReplyAsync(f, manyId, () => c.FinishManyAsync(items, manyId));
        await NoIntentAsync(db, f);
        var many = await c.FinishManyAsync(items);
        Assert.All(many.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, true), (o.Status, o.Replayed)));

        // Finish
        var single = c.FinishCommand(leases[2], AttemptOutcome.RetryableFailure, error: new AttemptError("boom", "System.Exception", "boom", null));
        await LoseReplyAsync(f, single.CommandId, () => c.FinishAsync(single));
        await NoIntentAsync(db, f);
        var finished = await c.FinishAsync(single);
        Assert.Equal((StoreStatus.Applied, true, "retrying"), (finished.Status, finished.Replayed, finished.Value!.Disposition));

        // FinishAndClaim: the claim's receipt replays the claim, each finish its own receipt.
        var running = (await c.ReserveAndBeginAsync(1)).Value!.Outcomes.Single().Begun!.Lease.Handle;
        var finish = c.FinishCommand(running, AttemptOutcome.Succeeded);
        var claim = c.ClaimCommand(2);
        await LoseReplyAsync(f, claim.CommandId, () => c.FinishAndClaimAsync([finish], claim));
        await NoIntentAsync(db, f);
        var again = await c.FinishAndClaimAsync([finish], claim);
        Assert.Equal((StoreStatus.Applied, true), (again.Value!.Claim.Status, again.Value.Claim.Replayed));
        var finishReplay = await c.FinishAsync(finish); // a replayed claim carries no finishes; each finish answers from its own receipt
        Assert.Equal((StoreStatus.Applied, true), (finishReplay.Status, finishReplay.Replayed));

        model = await ModelAsync(f);
        Assert.Equal(3, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(2, model.Jobs.Count(j => j.State == "running"));
        Assert.True((long)await db.HashGetAsync(RxClient.Meta(f.Application), "rev") > rev0);
        await NoIntentAsync(db, f);
    }

    [Fact]
    [Trait("Case", "DP-02")]
    public async Task A_claim_whose_write_fails_partway_stores_its_intent_and_the_resend_publishes_and_replays_it()
    {
        var (f, c, db) = await OpenAsync("dp02");
        await using var _ = f;
        await PendingAsync(c, 3);
        var meta = RxClient.Meta(f.Application);
        var leasesKey = RxClient.KeyPrefix(f.Application) + "leases";
        await db.StringSetAsync(leasesKey, "not-a-zset"); // a foreign writer; the claim's job hash writes come before its lease index write

        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter));
        Assert.Equal(RxClient.Hex(id), (string?)await db.HashGetAsync(meta, "guard"));
        Assert.NotEqual("", (string?)await db.HashGetAsync(meta, "intentsha") ?? "");
        await Assert.ThrowsAnyAsync<Exception>(() => c.ReserveAndBeginAsync(1)); // Recovering: nothing proceeds past the stored intent

        await db.KeyDeleteAsync(leasesKey); // the operator repairs the key, never the intent
        var replayed = await c.ReserveAndBeginAsync(3, commandId: id, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true, 3), (replayed.Status, replayed.Replayed, replayed.Value!.Outcomes.Count));
        Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
        var model = await ModelAsync(f);
        Assert.Equal(3, model.Jobs.Count(j => j.State == "running"));
        Assert.Equal(3, (await db.SortedSetLengthAsync(leasesKey)));
    }

    [Fact]
    [Trait("Case", "DP-03")]
    public async Task A_finish_batch_whose_write_fails_partway_stores_its_intent_and_each_finish_applies_once()
    {
        var (f, c, db) = await OpenAsync("dp03");
        await using var _ = f;
        await PendingAsync(c, 2);
        var leases = (await c.ReserveAndBeginAsync(2)).Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
        var terminal = RxClient.KeyPrefix(f.Application) + "idx:terminal";
        await db.KeyDeleteAsync(terminal);
        await db.StringSetAsync(terminal, "not-a-zset");

        var items = leases.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishManyAsync(items));
        Assert.NotEqual("", (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");

        await db.KeyDeleteAsync(terminal);
        var many = await c.FinishManyAsync(items);
        Assert.All(many.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, true), (o.Status, o.Replayed)));
        var model = await ModelAsync(f);
        Assert.Equal(2, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(2, model.Receipts.Count(r => r.Operation == "Finish"));
    }

    [Fact]
    [Trait("Case", "DP-04")]
    public async Task The_size_bound_that_lets_an_intent_skip_packing_is_never_below_the_packed_size()
    {
        // Each worker script and dispatch run with a check that packs every intent and fails if the cheap bound is smaller.
        const string Line = "local bound, bytes = packed_bound(intent), nil";
        var ctor = typeof(RxScript).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(string), typeof(string)])!;
        var cache = (ConcurrentDictionary<string, RxScript>)typeof(RxScript).GetField("Cache", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        string[] ops = ["dispatch", "reserve_and_begin", "finish_many", "finish", "finish_and_claim"];
        var originals = ops.ToDictionary(o => o, RxScript.Write);
        var (f, c, _) = await OpenAsync("dp04");
        await using var _f = f;
        try
        {
            foreach (var (op, s) in originals)
            {
                Assert.Contains(Line, s.Text, StringComparison.Ordinal);
                cache["w:" + op] = (RxScript)ctor.Invoke([op, s.Text.Replace(Line, "local bound, bytes = packed_bound(intent), cmsgpack.pack(intent)\n"
                    + "  if bound < #bytes then error('packed_bound ' .. bound .. ' < ' .. #bytes) end", StringComparison.Ordinal)]);
            }

            await PendingAsync(c, 5);
            var leases = (await c.ReserveAndBeginAsync(4)).Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
            var many = await c.FinishManyAsync([c.FinishCommand(leases[0], AttemptOutcome.Succeeded),
                c.FinishCommand(leases[1], AttemptOutcome.RetryableFailure, error: new AttemptError("e", "T", new string('é', 300), "at x"))]);
            Assert.All(many.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(leases[2], AttemptOutcome.PermanentFailure)).Status);
            var both = await c.FinishAndClaimAsync([c.FinishCommand(leases[3], AttemptOutcome.Succeeded)], c.ClaimCommand(1));
            Assert.Equal(StoreStatus.Applied, both.Value!.Claim.Status);
        }
        finally
        {
            foreach (var (op, s) in originals)
            {
                cache["w:" + op] = s;
            }
        }
    }
}
