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
    public async Task The_compact_form_expands_to_exactly_the_encoded_text_and_refuses_what_it_cannot_write()
    {
        // The codec in Lua: every case is encoded with U.enc and with U.cenc, and the compact text must expand to the same bytes.
        const string Body = """
            local fails = {}
            local function check(name, v)
              local c = U.cenc(v)
              if not c then fails[#fails + 1] = name .. ': no compact form' return end
              if string.sub(c, 1, 12) ~= '{"#c":1,"v":' then fails[#fails + 1] = name .. ': prefix' end
              local full, back = U.enc(v), U.cdec(c)
              if back ~= full then fails[#fails + 1] = name .. ': ' .. tostring(back) .. ' ~= ' .. full end
            end
            local id = string.rep('ab', 16)
            local lease = { jobId = id, attemptId = id, generation = U.raw('1'), leaseExpiresAt = 1795103260713, ownershipToken = 'tok',
              revision = U.raw('9007199254740991'), state = 'running', workerInstanceId = 'w', authorizedAt = 1795103260713,
              executionNumber = U.raw('007'), totalExecutionNumber = U.raw('-1') }
            check('fixed shapes and refs', { items = U.arr({ { ordinal = 0, jobId = id, status = 'Applied', code = U.NULL, begin = lease,
              reserve = { fences = U.arr({ { scopeId = id, value = U.raw('12') } }), policy = { executionTimeout = U.NULL, fingerprintVersion = 1,
                maxConsecutiveUnstartedAbandonments = U.raw('3'), maxExecutions = U.raw('3'), retry = U.raw('{"initialDelay":100}'),
                unstartedRetry = U.raw('{"initialDelay":100}') }, cursor = { eligibleAt = U.raw('1795103260713'), jobId = id } } },
              { ordinal = 1, jobId = id, status = 'CapacityBlocked', code = 'capacity_blocked', blockedScopeId = id,
                cursor = { eligibleAt = 1795103260713, jobId = id } } }) })
            check('shapes outside the list, used twice', U.arr({ { zeta = 1, alpha = 'x' }, { alpha = 'y', zeta = 2 }, {}, {} }))
            check('awkward text', { a = 'quote " backslash \\ tab \t nul \0 del \127 e9 \195\169 ' .. string.rep('x', 300), b = '', c = '-1',
              d = U.raw('"text"'), e = U.raw(''), f = U.raw('{"#c":1}'), g = true, h = false, i = cjson.null, ['#c'] = 2,
              ['k,l'] = 1, k = 1, l = 1 })
            check('nested arrays and holes', U.arr({ U.arr(), U.arr({ 1, 2, U.arr({ 'x', 'x' }) }), setmetatable({ n = 3, [1] = 'a', [3] = 'c' },
              getmetatable(U.arr())) }))
            check('numbers', U.arr({ 0, 999999999, 1000000000, 1000000000, 9007199254740991, U.raw('1000000000'), U.raw('123456789012345'),
              U.raw('1234567890123456') }))
            check('a finish result', { attemptId = id, attemptOutcome = 'succeeded', disposition = 'succeeded', eligibleAt = U.NULL,
              generation = U.raw('1'), jobId = id, retryDelay = U.NULL, revision = U.raw('4'), state = 'succeeded', terminalReason = 'succeeded' })
            if U.cenc({ a = -1 }) ~= nil then fails[#fails + 1] = 'a negative number has no compact form' end
            if U.cenc({ a = 1.5 }) ~= nil then fails[#fails + 1] = 'a fraction has no compact form' end
            if U.cdec('{"#c":2,"v":[1]}') ~= nil then fails[#fails + 1] = 'an unknown codec is not expanded' end
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
        Assert.StartsWith("{\"#c\":1,", (string?)await ResultAsync(db, f, claimId), StringComparison.Ordinal);
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
        Assert.StartsWith("{\"#c\":1,", (string?)await ResultAsync(db, f, single.CommandId), StringComparison.Ordinal);
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
        await db.HashSetAsync(prefix + RxClient.Hex(claimId), "result", "{\"#c\":2,\"v\":[1]}");
        await db.HashSetAsync(prefix + RxClient.Hex(finish.CommandId), "result", "{\"#c\":2,\"v\":[10]}");

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
