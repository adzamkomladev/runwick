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
/// Lean worker paths (22 §4.1, REDIS-22): claim, finish, FinishMany and renew are decided by hand-written Lua for their usual case and
/// hand over to the generic frame (<c>GENERIC()</c>, defined in the same script) for every other case before any write. The lean texts
/// are the generic writer's texts; the lean path runs for the usual commands and the generic one for replays and scoped jobs; a
/// lean command interrupted after any of its writes stores its intent, and the resend applies it exactly once. Byte-for-byte equality
/// of the stored data and the replies with the generic path is checked by eng/bench-compare/redis-group/diffcheck.py.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisLeanScriptTests
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

    static string Resource(string name)
    {
        using var stream = typeof(RedisStore).Assembly.GetManifestResourceStream(name) ?? throw new InvalidOperationException("missing resource " + name);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [Fact]
    [Trait("Case", "LS-01")]
    public async Task The_lean_texts_and_helpers_are_the_generic_ones()
    {
        // The lean frame and the generic chunk in one script (GENERIC is never called: the key is valid), then each pair compared.
        const string Body = """
            L.libs()
            local fails = {}
            local function same(name, got, want)
              if got ~= want then fails[#fails + 1] = name .. ': ' .. tostring(got) .. ' ~= ' .. tostring(want) end
            end
            local id, id2 = string.rep('ab', 16), string.rep('0f', 16)
            local odd = 'quote " backslash \\ tab \t nul \0 del \127 e9 \195\169 nl \n cr \r'
            local function rv(o)
              return { job = id, gen = '1', rrev = '9007199254740991', att = id2, worker = o and odd or 'w', token = id2, lease = '1795103260713',
                rnum = '1', name = o and odd or 'test', pver = '1', profile = 'json', profver = '1', queue = o and '~q,"x":1' or 'default',
                priority = 'medium', elig = '1795103250000', timeout = o and '30000' or nil, maxun = '3', max = '3',
                retry = '{"initialDelay":100,"maximumDelay":1000}', uretry = o and '{"a":"\\""}' or '{}' }
            end
            local b = { brev = '3', xnum = '1', txnum = '12', auth = '1795103260700' }
            local absent = rv(false)
            absent.gen, absent.elig, absent.rnum = nil, nil, nil
            for i, v in ipairs({ rv(false), rv(true), absent }) do
              for _, ord in ipairs({ 0, 7, 255 }) do same('claim ' .. i .. ' at ' .. ord, L.claim_text(ord, v, b), U.claim_text(ord, v, b)) end
            end
            -- A claim's compact receipt: the lean list texts against U.cenc on the same items.
            local v1, v2 = rv(false), rv(true)
            local b2 = { brev = '4', xnum = '1', txnum = '13', auth = '1795103260700' }
            same('claim receipt', '{"~":1,"c":[[0,' .. L.texts(v1, L.RKEYS) .. ',' .. L.texts(b, L.BKEYS) .. '],[1,' .. L.texts(v2, L.RKEYS, v1) .. ','
              .. L.texts(b2, L.BKEYS, b) .. ']]}', U.cenc({ packed = { { 0, v1, b }, { 1, v2, b2 } } }))
            local finishes = {
              { att = id, outcome = 'succeeded', state = 'succeeded', gen = '1', job = id2, rev = '4', reason = 'succeeded' },
              { att = id, outcome = 'retryable_failure', state = 'retrying', elig = '1795103260713', gen = '2', job = id2, delay = '75', rev = '5' },
              { att = id, outcome = odd, state = 'failed', job = id2, reason = 'r"r' },
            }
            for i, v in ipairs(finishes) do
              same('finish ' .. i, L.finish_text(v), U.finish_text(v))
              same('finish receipt ' .. i, L.finish_compact(v), U.cenc({ fv = v }))
            end
            for _, s in ipairs({ '', 'a', odd, id, string.rep('"', 40) }) do same('str ' .. s, L.str(s), U.str(s)) end
            local words = { '', 'a', 'ab', 'abc', 'b', 'ba', '\0', 'a\0', '\255', id, id2, id .. 'x', string.rep('a', 17), string.rep('a', 16) .. 'b' }
            for _, x in ipairs(words) do
              for _, y in ipairs(words) do same('blt ' .. x .. ' ' .. y, L.blt(x, y), U.blt(x, y)) end
            end
            for _, s in ipairs({ '0', '9', '19', '99', '9223372036854775806', '9223372036854775807' }) do same('inc ' .. s, L.inc(s), U.inc(s)) end
            return fails
            """;
        var script = "local function GENERIC() return nil end\n" + Resource("Runwick.Redis.Lua.lean.frame.lua") + "\n"
            + Resource("Runwick.Redis.Lua.prelude.lua") + "\n" + Resource("Runwick.Redis.Lua.jobs.lua") + "\n" + Body;
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var fails = (RedisResult[])(await db.ScriptEvaluateAsync(script, [RxClient.Meta("ls01")], [.. Enumerable.Repeat((RedisValue)"", 11)]))!;
        Assert.Empty(fails.Select(x => (string)x!));
    }

    /// <summary>Swaps the cached lean scripts for copies that count their GENERIC() calls in <c>&lt;prefix&gt;generic</c>, by operation.</summary>
    static async Task CountingGenericAsync(Func<Task> body)
    {
        var ctor = typeof(RxScript).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(string), typeof(string)])!;
        var cache = (ConcurrentDictionary<string, RxScript>)typeof(RxScript).GetField("Cache", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        var originals = RxScript.Lean.ToDictionary(o => o, RxScript.Write);
        try
        {
            foreach (var (op, s) in originals)
            {
                Assert.Contains("local function GENERIC()\n", s.Text, StringComparison.Ordinal);
                cache["w:" + op] = (RxScript)ctor.Invoke([op, s.Text.Replace("local function GENERIC()\n",
                    "local function GENERIC()\nredis.call('HINCRBY', string.sub(KEYS[1], 1, #KEYS[1] - 4) .. 'generic', ARGV[3], 1)\n", StringComparison.Ordinal)]);
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
    [Trait("Case", "LS-02")]
    public async Task The_usual_worker_commands_take_the_lean_path_and_replays_and_scoped_jobs_the_generic_one()
    {
        var (f, c, db) = await OpenAsync("ls02");
        await using var _ = f;
        var counts = (RedisKey)(RxClient.KeyPrefix(f.Application) + "generic");
        await CountingGenericAsync(async () =>
        {
            await PendingAsync(c, 5);
            var claimId = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            var claimed = await c.ReserveAndBeginAsync(4, commandId: claimId, notAfter: notAfter);
            var leases = claimed.Value!.Outcomes.Select(o => o.Begun!.Lease.Handle).ToList();
            Assert.Equal(4, leases.Count);
            Assert.All((await c.RenewManyAsync(leases)).Value!.Outcomes, i => Assert.Equal(StoreStatus.Applied, i.Status));
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(leases[0])).Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(leases[0], AttemptOutcome.Succeeded)).Status);
            var many = await c.FinishManyAsync([c.FinishCommand(leases[1], AttemptOutcome.Succeeded),
                c.FinishCommand(leases[2], AttemptOutcome.RetryableFailure, error: new AttemptError("e", "T", "m \" é", "at x"))]);
            Assert.All(many.Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, false), (o.Status, o.Replayed)));
            Assert.Equal(StoreStatus.NoWork, (await c.ReserveAndBeginAsync(4, priority: "high")).Status);
            // A stale renewal (the finished lease) is answered by the lean path too.
            Assert.NotEqual(StoreStatus.Applied, (await c.RenewAsync(leases[0])).Status);
            Assert.Empty(await db.HashGetAllAsync(counts));

            // A resent claim replays from its receipt: the generic frame answers it.
            var replay = await c.ReserveAndBeginAsync(4, commandId: claimId, notAfter: notAfter);
            Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
            Assert.Equal("1", (string?)await db.HashGetAsync(counts, "ReserveAndBegin"));
        });

        var model = await ModelAsync(f);
        Assert.Equal(["pending", "retrying", "running", "succeeded", "succeeded"], model.Jobs.Select(j => j.State).Order());
    }

    [Fact]
    [Trait("Case", "LS-03")]
    public async Task Every_write_prefix_of_each_lean_command_stores_its_intent_and_the_resend_applies_it_once()
    {
        var (f, c, db) = await OpenAsync("ls03");
        await using var _ = f;
        var store = c.Services.GetRequiredService<RedisStore>();
        var meta = RxClient.Meta(f.Application);

        // Runs call with FailAfter = 1, 2, ... until it completes; each interrupted call must leave a stored intent or a lost reply,
        // and resend (a call that recovers first) must then answer as the first call would have and leave no job running.
        async Task<int> SweepAsync(Func<Task> prepare, Func<Task> call, Func<Task> resend)
        {
            var interrupted = 0;
            for (var n = 1; n < 200; n++)
            {
                await prepare();
                store.Runner.Client.FailAfter = n;
                var completed = false;
                try
                {
                    await call();
                    completed = true;
                }
                catch (StoreOutcomeUnknownException)
                {
                    interrupted++;
                }
                finally
                {
                    store.Runner.Client.FailAfter = 0;
                }

                if (completed)
                {
                    await resend(); // a plain resend of a completed call: the same answer (and the jobs it left running are finished)
                    return interrupted;
                }

                await resend();
                Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
                await ModelAsync(f);
            }

            throw new InvalidOperationException("the call never completed");
        }

        // Claim: the resend replays the same three jobs, begun once.
        Guid claimId = default;
        DateTimeOffset claimNotAfter = default;
        var claims = await SweepAsync(
            async () =>
            {
                await PendingAsync(c, 3);
                (claimId, claimNotAfter) = (Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddSeconds(30));
            },
            () => c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: claimNotAfter),
            async () =>
            {
                var r = await c.ReserveAndBeginAsync(3, commandId: claimId, notAfter: claimNotAfter);
                Assert.Equal((StoreStatus.Applied, 3), (r.Status, r.Value!.Outcomes.Count));
                foreach (var o in r.Value.Outcomes)
                {
                    Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(o.Begun!.Lease.Handle, AttemptOutcome.Succeeded)).Status);
                }
            });
        Assert.True(claims > 5, $"only {claims} interrupted claims");

        // Renew, RenewMany, Finish and FinishMany on running jobs.
        List<LeaseHandle> running = [];
        async Task RunningAsync(int n)
        {
            await PendingAsync(c, n);
            running = [.. (await c.ReserveAndBeginAsync(n)).Value!.Outcomes.Select(o => o.Begun!.Lease.Handle)];
        }

        async Task FinishAllAsync()
        {
            foreach (var l in running)
            {
                await c.FinishAsync(l, AttemptOutcome.Succeeded);
            }
        }

        var renews = await SweepAsync(() => RunningAsync(2), () => c.RenewManyAsync(running), async () =>
        {
            Assert.All((await c.RenewManyAsync(running)).Value!.Outcomes, i => Assert.Equal(StoreStatus.Applied, i.Status));
            await FinishAllAsync();
        });
        Assert.True(renews >= 2, $"only {renews} interrupted renewals");

        StoreCommand<FinishRequest>? single = null;
        var finishes = await SweepAsync(
            async () =>
            {
                await RunningAsync(1);
                single = c.FinishCommand(running[0], AttemptOutcome.RetryableFailure);
            },
            () => c.FinishAsync(single!),
            async () => Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(single!)).Status));
        Assert.True(finishes > 5, $"only {finishes} interrupted finishes");

        List<StoreCommand<FinishRequest>> items = [];
        var batches = await SweepAsync(
            async () =>
            {
                await RunningAsync(2);
                items = [c.FinishCommand(running[0], AttemptOutcome.Succeeded), c.FinishCommand(running[1], AttemptOutcome.PermanentFailure)];
            },
            () => c.FinishManyAsync(items),
            async () => Assert.All((await c.FinishManyAsync(items)).Value!.Outcomes, o => Assert.Equal((StoreStatus.Applied, true), (o.Status, o.Replayed))));
        Assert.True(batches > 5, $"only {batches} interrupted batches");

        var model = await ModelAsync(f);
        Assert.DoesNotContain(model.Jobs, j => j.State is "running" or "reserved");
        Assert.Equal(model.Attempts.Count, model.Attempts.Count(a => a.Outcome is not null));
    }
}
