using System.Collections.Concurrent;
using System.Reflection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Compact receipt results (22 §3.2, REDIS-20): claim and finish receipts store their result in a lossless compact form that expands to
/// the replied text. A lost reply replays the same answer, receipts written in the plain form (by the previous version) still replay,
/// and a result codec this binary does not know is never reported as success.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisCompactReceiptTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly string[] WorkerScripts = ["reserve_and_begin", "finish_many", "finish", "finish_and_claim"];

    static async Task<(DriverFixture F, TestClient C, IDatabase Db)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 8; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        return (f, c, (await RedisServer.AdminAsync()).GetDatabase());
    }

    static Task<RedisValue> ResultAsync(IDatabase db, DriverFixture f, Guid command) =>
        db.HashGetAsync(RxClient.KeyPrefix(f.Application) + "rcpt:" + RxClient.Hex(command), "result");

    /// <summary>Runs the worker scripts as the previous version did: plain receipt results (the seam the codec switch leaves).</summary>
    static async Task WithPlainReceiptsAsync(Func<Task> body)
    {
        var ctor = typeof(RxScript).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(string), typeof(string)])!;
        var cache = (ConcurrentDictionary<string, RxScript>)typeof(RxScript).GetField("Cache", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        var originals = WorkerScripts.ToDictionary(o => o, RxScript.Write);
        try
        {
            foreach (var (op, s) in originals)
            {
                Assert.Contains("U.COMPACT = true", s.Text, StringComparison.Ordinal);
                cache["w:" + op] = (RxScript)ctor.Invoke([op, s.Text.Replace("U.COMPACT = true", "U.COMPACT = false", StringComparison.Ordinal)]);
            }

            await body();
        }
        finally
        {
            foreach (var (op, s) in originals)
            {
                cache["w:" + op] = s;
            }
        }
    }

    [Fact]
    [Trait("Case", "RC-01")]
    public async Task The_compact_form_rebuilds_exactly_the_replied_text_and_keeps_the_plain_text_when_it_cannot()
    {
        // The codec in Lua: a compact receipt rebuilt by the result builders must be the text those builders give when the decision
        // is made, for any texts (quotes, backslashes, control bytes, non-ASCII, JSON, absent values).
        const string Body = """
            local fails = {}
            local function same(name, compact, full)
              if not compact then fails[#fails + 1] = name .. ': no compact form' return end
              if string.sub(compact, 1, 7) ~= '{"~":1,' then fails[#fails + 1] = name .. ': prefix' end
              if #compact >= #full then fails[#fails + 1] = name .. ': not shorter' end
              local back = U.cdec(compact)
              if back ~= full then fails[#fails + 1] = name .. ': ' .. tostring(back) .. ' ~= ' .. full end
            end
            local id, id2 = string.rep('ab', 16), string.rep('0f', 16)
            local function rv(job, odd)
              return { job = job, gen = '1', rrev = '9007199254740991', att = id2, worker = 'w', token = id2, lease = '1795103260713', rnum = '1',
                name = odd and 'quote " backslash \\ tab \t nul \0 del \127 e9 \195\169' or 'test', pver = '1', profile = 'json', profver = '1',
                queue = odd and '~q,"x":1' or 'default', priority = 'medium', elig = '1795103250000', timeout = odd and '30000' or nil,
                maxun = '3', max = '3', retry = '{"initialDelay":100,"maximumDelay":1000}', uretry = odd and '{"a":"\\""}' or '{}' }
            end
            local b = { brev = '3', xnum = '1', txnum = '12', auth = '1795103260700' }
            local v1, v2, v3, v4 = rv(id, false), rv(id2, true), rv(id2, true), rv(id, false)
            v3.timeout = nil -- absent after present: not taken from the item before
            local b2 = { brev = '4', xnum = '1', txnum = '13', auth = '1795103260700' }
            same('a claim', U.cenc({ packed = { { 0, v1, b }, { 1, v2, b }, { 2, v3, b2 }, { 3, v4, b2 } } }),
              U.enc({ items = U.arr({ U.claim_item(0, v1, b), U.claim_item(1, v2, b), U.claim_item(2, v3, b2), U.claim_item(3, v4, b2) }) }))
            local f1 = { att = id, outcome = 'succeeded', state = 'succeeded', gen = '1', job = id2, rev = '4', reason = 'succeeded' }
            same('a finish', U.cenc({ fv = f1 }), U.enc(U.finish_result(f1)))
            local f2 = { att = id, outcome = 'retryable_failure', state = 'retrying', elig = '1795103260713', gen = '2', job = id2, delay = '75', rev = '5' }
            same('a retried finish', U.cenc({ fv = f2 }), U.enc(U.finish_result(f2)))
            local extra = rv(id, false)
            extra.surprise = 'x'
            if U.cenc({ packed = { { 0, extra, b } } }) ~= nil then fails[#fails + 1] = 'a value outside the key list keeps the plain text' end
            if U.cenc({ fv = { att = id, rev = 4 } }) ~= nil then fails[#fails + 1] = 'a value that is not text keeps the plain text' end
            if U.cenc({ result = { items = {} } }) ~= nil then fails[#fails + 1] = 'a decision without lists keeps the plain text' end
            if U.cdec('{"~":2,"f":[]}') ~= nil then fails[#fails + 1] = 'an unknown codec is not rebuilt' end
            if U.cdec('{"items":[]}') ~= '{"items":[]}' then fails[#fails + 1] = 'plain text is returned as stored' end
            return fails
            """;
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var script = Resource("Runwick.Redis.Lua.prelude.lua") + "\n" + Resource("Runwick.Redis.Lua.jobs.lua") + "\n" + Body;
        var fails = (RedisResult[])(await db.ScriptEvaluateAsync(script, [RxClient.Meta("rc01")], [.. Enumerable.Repeat((RedisValue)"", 11)]))!;
        Assert.Empty(fails.Select(x => (string)x!));
    }

    [Fact]
    [Trait("Case", "RC-02")]
    public async Task Claim_and_finish_receipts_are_compact_and_a_lost_reply_replays_the_same_answer()
    {
        var (f, c, db) = await OpenAsync("rc02");
        await using var _ = f;

        // ReserveAndBegin: the reply seen first and the replay of a resend are the same.
        var claimId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var first = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
        Assert.StartsWith("{\"~\":1,", (string?)await ResultAsync(db, f, claimId), StringComparison.Ordinal);
        var again = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true, first.DecidedAt), (again.Status, again.Replayed, again.DecidedAt));
        Assert.Equivalent(first.Value, again.Value, strict: true);
        var leases = first.Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();

        // A lost reply: the resend replays from the compact receipt.
        var lostId = Guid.CreateVersion7();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(lostId));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(2, commandId: lostId, notAfter: notAfter));
        Assert.Equal(1, drop.Fired);
        var replayed = await c.ReserveAndBeginAsync(2, commandId: lostId, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true, 2), (replayed.Status, replayed.Replayed, replayed.Value!.Outcomes.Count));
        Assert.All(replayed.Value.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));

        // Finish and FinishMany items: compact receipts, replays equal to the first answers, and resolution expands them.
        var single = c.FinishCommand(leases[0], AttemptOutcome.RetryableFailure, error: new AttemptError("boom", "System.Exception", "bo\"om é", null));
        var finished = await c.FinishAsync(single);
        Assert.StartsWith("{\"~\":1,", (string?)await ResultAsync(db, f, single.CommandId), StringComparison.Ordinal);
        var finishedAgain = await c.FinishAsync(single);
        Assert.True(finishedAgain.Replayed);
        Assert.Equivalent(finished.Value, finishedAgain.Value, strict: true);

        var items = leases.Skip(1).Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var many = await c.FinishManyAsync(items);
        var manyAgain = await c.FinishManyAsync(items);
        Assert.All(manyAgain.Value!.Outcomes, o => Assert.True(o.Replayed));
        Assert.Equivalent(many.Value!.Outcomes.Select(o => o.Value), manyAgain.Value.Outcomes.Select(o => o.Value), strict: true);

        var resolved = Assert.IsType<ReceiptResolution.Recorded>(await c.Driver.ResolveOperationAsync(f.Application, single.CommandId, single.NotAfter, Ct));
        Assert.Equal("retrying", resolved.Result.GetProperty("state").GetString());
        Assert.Equal(single.Body.Lease.AttemptId.ToString("D"), resolved.Result.GetProperty("attemptId").GetString());
        var claimResolved = Assert.IsType<ReceiptResolution.Recorded>(await c.Driver.ResolveOperationAsync(f.Application, claimId, notAfter, Ct));
        Assert.Equal(3, claimResolved.Result.GetProperty("items").GetArrayLength());
    }

    [Fact]
    [Trait("Case", "RC-03")]
    public async Task Receipts_written_in_the_plain_form_by_the_previous_version_still_replay()
    {
        var (f, c, db) = await OpenAsync("rc03");
        await using var _ = f;
        var claimId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        StoreReply<ClaimedJobs> first = null!;
        StoreReply<FinishedAttempt> finished = null!;
        StoreReply<FinishedAndClaimed> combined = null!;
        StoreCommand<FinishRequest> single = null!, combinedFinish = null!;
        StoreCommand<ReserveAndBeginRequest> claim = null!;
        await WithPlainReceiptsAsync(async () =>
        {
            first = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
            var leases = first.Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
            single = c.FinishCommand(leases[0], AttemptOutcome.Succeeded);
            finished = await c.FinishAsync(single);
            combinedFinish = c.FinishCommand(leases[1], AttemptOutcome.PermanentFailure);
            claim = c.ClaimCommand(2);
            combined = await c.FinishAndClaimAsync([combinedFinish], claim);
        });
        Assert.StartsWith("{\"items\":", (string?)await ResultAsync(db, f, claimId), StringComparison.Ordinal);
        Assert.StartsWith("{\"attemptId\":", (string?)await ResultAsync(db, f, single.CommandId), StringComparison.Ordinal);
        Assert.StartsWith("{\"items\":", (string?)await ResultAsync(db, f, claim.CommandId), StringComparison.Ordinal);

        // This version replays each of them, byte for byte as they were stored.
        var again = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true), (again.Status, again.Replayed));
        Assert.Equivalent(first.Value, again.Value, strict: true);
        var finishedAgain = await c.FinishAsync(single);
        Assert.True(finishedAgain.Replayed);
        Assert.Equivalent(finished.Value, finishedAgain.Value, strict: true);
        var many = await c.FinishManyAsync([single, combinedFinish]); // both from receipts the previous version wrote
        Assert.All(many.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, true), (o.Status, o.Replayed)));
        var combinedAgain = await c.FinishAndClaimAsync([combinedFinish], claim);
        Assert.Equal((StoreStatus.Applied, true), (combinedAgain.Value!.Claim.Status, combinedAgain.Value.Claim.Replayed));
        Assert.Equivalent(combined.Value!.Claim.Value, combinedAgain.Value.Claim.Value, strict: true);

        // New and old receipts side by side in one store: a new claim writes the compact form next to the plain ones.
        var newer = await c.ReserveAndBeginAsync(1);
        Assert.Equal(StoreStatus.Applied, newer.Status);
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Equal(4, model.Jobs.Count(j => j.State == "running"));
    }

    [Fact]
    [Trait("Case", "RC-04")]
    public async Task A_receipt_result_in_an_unknown_codec_is_never_reported_as_success()
    {
        var (f, c, db) = await OpenAsync("rc04");
        await using var _ = f;
        var claimId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var lease = (await c.ReserveAndBeginAsync(1, commandId: claimId, notAfter: notAfter)).Value!.Outcomes.Single().Begun!.Lease.Handle;
        var finish = c.FinishCommand(lease, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(finish)).Status);
        var prefix = RxClient.KeyPrefix(f.Application) + "rcpt:";
        await db.HashSetAsync(prefix + RxClient.Hex(claimId), "result", "{\"~\":2,\"items\":[]}");
        await db.HashSetAsync(prefix + RxClient.Hex(finish.CommandId), "result", "{\"~\":2,\"jobId\":1}");

        var claim = await c.ReserveAndBeginAsync(1, commandId: claimId, notAfter: notAfter);
        Assert.Equal((StoreStatus.UnsupportedVersion, "receipt_codec_unsupported"), (claim.Status, claim.Error?.Code));
        var finishAgain = await c.FinishAsync(finish);
        Assert.Equal(StoreStatus.UnsupportedVersion, finishAgain.Status);
        var many = await c.FinishManyAsync([finish]);
        Assert.Equal(StoreStatus.UnsupportedVersion, many.Value!.Outcomes.Single().Status);
        await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.Driver.ResolveOperationAsync(f.Application, finish.CommandId, finish.NotAfter, Ct));
        Assert.Equal("succeeded", (await f.ReadCommittedSnapshot()).Model.Jobs.Single(j => j.JobId == lease.JobId).State);
    }

    static string Resource(string name)
    {
        using var stream = typeof(RedisStore).Assembly.GetManifestResourceStream(name) ?? throw new InvalidOperationException("missing resource " + name);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
