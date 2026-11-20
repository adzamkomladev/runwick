using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Worker script cuts (22 §4.1, REDIS-21): the claim, finish and event texts the worker scripts now write directly are the texts the
/// generic JSON writer gives, for awkward values too, and fall back to it for values of other types; a FinishMany batch that mixes
/// replayed and new items replays the old answers and writes the confirmation markers; event rows stay canonical JSON.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisScriptCutTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<RedisResult> LuaAsync(IDatabase db, RedisKey meta, string body)
    {
        var script = Resource("Runwick.Redis.Lua.prelude.lua") + "\n" + Resource("Runwick.Redis.Lua.jobs.lua") + "\n" + body;
        return await db.ScriptEvaluateAsync(script, [meta], [.. Enumerable.Repeat((RedisValue)"", 11)]);
    }

    [Fact]
    [Trait("Case", "SC-01")]
    public async Task The_texts_written_directly_are_the_texts_the_generic_writer_gives()
    {
        const string Body = """
            local fails = {}
            local function same(name, got, want)
              if got ~= want then fails[#fails + 1] = name .. ': ' .. tostring(got) .. ' ~= ' .. tostring(want) end
            end
            local id, id2 = string.rep('ab', 16), string.rep('0f', 16)
            local function rv(odd)
              return { job = id, gen = '1', rrev = '9007199254740991', att = id2, worker = 'w', token = id2, lease = '1795103260713', rnum = '1',
                name = odd and 'quote " backslash \\ tab \t nul \0 del \127 e9 \195\169' or 'test', pver = '1', profile = 'json', profver = '1',
                queue = odd and '~q,"x":1' or 'default', priority = 'medium', elig = '1795103250000', timeout = odd and '30000' or nil,
                maxun = '3', max = '3', retry = '{"initialDelay":100,"maximumDelay":1000}', uretry = odd and '{"a":"\\""}' or '{}' }
            end
            local b = { brev = '3', xnum = '1', txnum = '12', auth = '1795103260700' }
            local cases = { rv(false), rv(true) }
            local absent = rv(false)
            absent.gen, absent.elig, absent.rnum = nil, nil, nil -- absent raw texts: the generic writer gives {}
            cases[#cases + 1] = absent
            local number = rv(false)
            number.token = 12 -- not text: written the generic way
            cases[#cases + 1] = number
            local noname = rv(true)
            noname.name = nil
            cases[#cases + 1] = noname
            for i, v in ipairs(cases) do
              for _, ord in ipairs({ 0, 7, 255 }) do
                same('claim ' .. i .. ' at ' .. ord, U.claim_text(ord, v, b), U.enc(U.claim_item(ord, v, b)))
              end
            end
            local sparse = { brev = '3' }
            same('claim with absent begin texts', U.claim_text(1, rv(false), sparse), U.enc(U.claim_item(1, rv(false), sparse)))

            local finishes = {
              { att = id, outcome = 'succeeded', state = 'succeeded', gen = '1', job = id2, rev = '4', reason = 'succeeded' },
              { att = id, outcome = 'retryable_failure', state = 'retrying', elig = '1795103260713', gen = '2', job = id2, delay = '75', rev = '5' },
              { att = id, outcome = 'odd " \\ \n \0', state = 'failed', job = id2, reason = 'r"r' },
              { att = id, outcome = 3, state = 'failed', gen = '1', job = id2, rev = '2' },
              { att = id, outcome = 'permanent_failure', state = 'failed', gen = '1', job = id2, rev = '2', reason = false },
            }
            for i, v in ipairs(finishes) do same('finish ' .. i, U.finish_text(v), U.enc(U.finish_result(v))) end

            -- A FinishMany item reply as the table it was before (U.finish_item_json now returns the text when it can).
            local function table_of(cmd, r, replayed)
              return { commandId = cmd, status = r.status, effect = r.effect == 'A' and 'Applied' or 'ObservedNoEffect', replayed = replayed,
                code = r.code or U.NULL, decidedAt = r.decided and r.decided ~= '' and U.raw(r.decided) or U.NULL,
                result = r.result and (type(r.result) == 'table' and r.result or U.raw(r.result)) or U.NULL }
            end
            local items = {
              { id, { status = 'Applied', effect = 'A', decided = '1795103260713', result = U.raw('{"a":1}') }, false },
              { id, { status = 'StaleLease', effect = 'N', code = 'stale_lease', decided = '1795103260713' }, false },
              { id, { status = 'Applied', effect = 'A', decided = '', result = '{"b":2}' }, true },
              { 'c "q" \\', { status = 'CommandExpired', effect = 'N', code = 'command_expired' }, false },
              { id, { status = 'Applied', effect = 'A', decided = '1', result = { x = 1, y = U.arr({ 'z' }) } }, false },
              { id, { status = false, effect = 'A' }, true },
              { id, { status = 'Applied', effect = 'A', code = false, decided = false, result = false }, false },
            }
            for i, c in ipairs(items) do
              same('finish item ' .. i, U.enc(U.finish_item_json(c[1], c[2], c[3])), U.enc(table_of(c[1], c[2], c[3])))
            end

            -- Event details: the closing event's, with and without eligibleAt and terminalReason, and the reserve and begin ones, against
            -- the tables written before.
            local closes = {
              { nil, 1, 'succeeded', 'succeeded', 'succeeded', nil },
              { nil, 2, 'retryable_failure', 'retrying', nil, 1795103260713 },
              { nil, 3, 'odd " \\ \0', 'failed', 'r"r', 1795103260713 },
              { nil, nil, 'succeeded', 'succeeded', nil, nil },
              { nil, 1, 7, 'failed', nil, nil },
              { nil, 1, 'x', 'failed', nil, '1795103260713' },
              { { blockingScopes = U.arr({ id }), rateTime = 5 }, 1, 'admission_deferred', 'pending', nil, 1795103260713 },
            }
            for i, c in ipairs(closes) do
              local function plain()
                local d = c[1] and { blockingScopes = c[1].blockingScopes, rateTime = c[1].rateTime } or {}
                d.generation, d.outcome, d.disposition = c[2], c[3], c[4]
                if c[5] then d.terminalReason = c[5] end
                if c[6] then d.eligibleAt = c[6] end
                return d
              end
              same('close details ' .. i, U.enc(U.close_details(c[1], c[2], c[3], c[4], c[5], c[6])), U.enc(plain()))
            end
            for _, g in ipairs({ 1, 12, false }) do
              local gv = g or nil
              same('reserved details', U.enc(U.reserved_details(gv, '40')), U.enc({ generation = gv, reservationNumber = U.raw('40') }))
              same('started details', U.enc(U.started_details(gv, 3)), U.enc({ generation = gv, executionNumber = 3 }))
              same('started details, no number', U.enc(U.started_details(gv, nil)), U.enc({ generation = gv }))
            end
            return fails
            """;
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var fails = (RedisResult[])(await LuaAsync(db, RxClient.Meta("sc01"), Body))!;
        Assert.Empty(fails.Select(x => (string)x!));
    }

    [Fact]
    [Trait("Case", "SC-02")]
    public async Task A_finish_batch_mixing_replayed_and_new_items_replays_the_old_answers_and_confirms_their_receipts()
    {
        var f = await DriverFixture.StartAsync("sc02_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 4; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        var claimed = await c.ReserveAndBeginAsync(4);
        var leases = claimed.Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
        Assert.Equal(4, leases.Count);
        var a = c.FinishCommand(leases[0], AttemptOutcome.Succeeded);
        var b = c.FinishCommand(leases[1], AttemptOutcome.RetryableFailure, error: new AttemptError("x", "System.Exception", "m", null));
        var first = await c.FinishManyAsync([a, b]);
        Assert.All(first.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, false), (o.Status, o.Replayed)));

        // The resend of a and b rides in a batch with two new finishes, the replayed ones in the middle and out of command-id order.
        var d = c.FinishCommand(leases[3], AttemptOutcome.PermanentFailure);
        var e = c.FinishCommand(leases[2], AttemptOutcome.Succeeded);
        var mixed = await c.FinishManyAsync([e, b, a, d]);
        var outcomes = mixed.Value!.Outcomes;
        Assert.Equal([false, true, true, false], outcomes.Select(o => o.Replayed));
        Assert.All(outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        Assert.Equivalent(first.Value.Outcomes[1].Value, outcomes[1].Value, strict: true);
        Assert.Equivalent(first.Value.Outcomes[0].Value, outcomes[2].Value, strict: true);

        // Replayed items carry the 22 §4.5 confirmation marker; newly decided ones do not.
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var prefix = RxClient.KeyPrefix(f.Application) + "rcpt:";
        foreach (var (cmd, confirmed) in new[] { (a, true), (b, true), (d, false), (e, false) })
        {
            Assert.Equal(confirmed, await db.HashExistsAsync(prefix + RxClient.Hex(cmd.CommandId), "confirmed"));
        }

        var jobs = (await f.ReadCommittedSnapshot()).Model.Jobs;
        Assert.Equal(["failed", "retrying", "succeeded", "succeeded"], jobs.Select(j => j.State).Order());
    }

    [Fact]
    [Trait("Case", "SC-03")]
    public async Task Event_rows_written_by_the_worker_scripts_hold_canonical_details_and_a_lost_claim_reply_replays_its_payloads()
    {
        var f = await DriverFixture.StartAsync("sc03_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 4; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("j" + i), cancellationToken: Ct);
        }

        // A claim whose reply is lost replays the same jobs with the same payload rows (read again from Redis on the replay; taken
        // from the job as the claim read it on the first answer).
        var claimId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(claimId));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter));
        Assert.Equal(1, drop.Fired);
        var replayed = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: notAfter);
        Assert.True(replayed.Replayed);
        var freshId = Guid.CreateVersion7();
        var fresh = await c.ReserveAndBeginAsync(1, commandId: freshId, notAfter: notAfter);
        Assert.False(fresh.Replayed);
        var begun = replayed.Value!.Outcomes.Concat(fresh.Value!.Outcomes).Select(o => o.Begun!).ToList();
        Assert.Equal(4, begun.Count);
        var again = await c.ReserveAndBeginAsync(1, commandId: freshId, notAfter: notAfter);
        Assert.True(again.Replayed);
        Assert.Equivalent(fresh.Value, again.Value, strict: true);

        var leases = begun.Select(x => x.Lease.Handle).ToList();
        await c.FinishManyAsync([c.FinishCommand(leases[0], AttemptOutcome.Succeeded), c.FinishCommand(leases[1], AttemptOutcome.PermanentFailure)]);
        await c.FinishAsync(leases[2], AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromSeconds(30));
        await c.FinishAsync(leases[3], AttemptOutcome.UserCancelled);

        // Every event row's details are what the generic writer gives for the same values (canonical: keys in byte order), with the
        // keys each kind has always had.
        const string Check = """
            local bad, n = {}, 0
            local keys = { dispatched = 'generation,ordinal,priority,queue', attempt_reserved = 'generation,reservationNumber',
              attempt_started = 'executionNumber,generation', attempt_finished = 'disposition,generation,outcome' }
            local rows = redis.call('HGETALL', P .. 'events:data')
            for i = 2, #rows, 2 do
              local d = string.match(rows[i], ',"details":(.*),"eventId":')
              n = n + 1
              local t = d and cjson.decode(d)
              if not t or U.enc(t) ~= d then bad[#bad + 1] = rows[i] else
                t.eligibleAt, t.terminalReason = nil, nil -- optional on attempt_finished
                if table.concat(U.sorted(t), ',') ~= keys[cjson.decode(rows[i]).kind] then bad[#bad + 1] = 'keys: ' .. rows[i] end
              end
            end
            bad[#bad + 1] = 'rows ' .. n
            return bad
            """;
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var result = ((RedisResult[])(await LuaAsync(db, RxClient.KeyPrefix(f.Application) + "meta", Check))!).Select(x => (string)x!).ToList();
        Assert.Equal("rows 16", result[^1]); // 4 dispatched, 4 reserved, 4 started, 4 finished
        Assert.Single(result);
    }

    static string Resource(string name)
    {
        using var stream = typeof(RedisStore).Assembly.GetManifestResourceStream(name) ?? throw new InvalidOperationException("missing resource " + name);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
