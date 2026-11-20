using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>Perf plan F: the opt-in grouped Redis dispatch. Members keep their own identity, receipt and event; every failure maps per command.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisGroupedDispatchTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    sealed record Host(ServiceProvider Provider, string Application, HarnessFaultInjector Faults, string Configuration) : IAsyncDisposable
    {
        public RedisStore Store => Provider.GetRequiredService<RedisStore>();

        public IPreparedJobDispatcher Prepared => Provider.GetRequiredService<IPreparedJobDispatcher>();

        public async ValueTask DisposeAsync()
        {
            await Provider.DisposeAsync();
            await RedisServer.DropApplicationAsync(Application);
        }

        public StoreCommand<DispatchRequest> Command(string id, DispatchOptions? options = null)
        {
            var p = Prepared.Prepare(new PingJob(id), options);
            var preparer = Provider.GetRequiredService<Dispatching.DispatchPreparer>();
            var item = new DispatchItem(p.Intent, p.Scopes, p.Digests) { Rates = preparer.RatesFor(p.Intent) };
            return new StoreCommand<DispatchRequest>(p.Application, p.OperationId.Value, p.NotAfter, new DispatchRequest(item, p.StoreId, p.Digests.Command));
        }

        public async Task<(HarnessSnapshot Snapshot, ConnectionMultiplexer Mux)> SnapshotAsync()
        {
            var mux = await ConnectionMultiplexer.ConnectAsync(Configuration + ",allowAdmin=true");
            return (await RedisSnapshotReader.ReadAsync(mux, Application), mux);
        }

        public async Task<int> JobCountAsync()
        {
            var (snapshot, mux) = await SnapshotAsync();
            await mux.DisposeAsync();
            Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
            return snapshot.Model.Jobs.Count;
        }

        /// <summary>Event rows by command ID: each dispatch writes exactly one, attributed to its own command.</summary>
        public async Task<List<(string CommandId, int Ordinal)>> DispatchEventsAsync()
        {
            await using var mux = await ConnectionMultiplexer.ConnectAsync(Configuration);
            var hash = await mux.GetDatabase().HashGetAllAsync(RedisServer.Key(Application, "events:data"));
            var rows = RedisEventRows.All(hash.Select(e => KeyValuePair.Create((string)e.Name!, (string)e.Value!)));
            return [.. rows.Select(r => JsonNode.Parse(r)!.AsObject())
                .Where(o => o["kind"]!.GetValue<string>() == "dispatched")
                .Select(o => (o["commandId"]!.GetValue<string>(), o["details"]!["ordinal"]!.GetValue<int>()))];
        }
    }

    static async Task<Host> OpenAsync(string name, Action<RedisOptions>? configure = null, string? configuration = null)
    {
        configuration ??= RedisServer.Configuration;
        if (configuration == RedisServer.Configuration)
        {
            await RedisServer.EnsureMigratedAsync();
        }

        var application = name + "_" + Guid.NewGuid().ToString("N")[..8];
        var faults = new HarnessFaultInjector();
        var services = new ServiceCollection();
        services.AddSingleton<IFaultInjector>(faults);
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(configuration, o =>
            {
                o.GroupDispatches = true;
                o.MaxGroupsInFlight = 1;
                configure?.Invoke(o);
            });
        });
        var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct);
        return new Host(provider, application, faults, configuration);
    }

    // ------------------------------------------------------------------ grouping, identity, replay

    [Fact]
    public async Task Concurrent_callers_are_grouped_and_every_member_has_its_own_receipt_and_event_and_replays_alone()
    {
        await using var host = await OpenAsync("fgroup");
        var prepared = Enumerable.Range(0, 240).Select(i => host.Prepared.Prepare(new PingJob("g" + i))).ToList();
        var receipts = new DispatchReceipt[prepared.Count];
        await Parallel.ForEachAsync(Enumerable.Range(0, prepared.Count), new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = Ct },
            async (i, ct) => receipts[i] = await host.Prepared.DispatchAsync(prepared[i], ct));

        Assert.True(host.Store.GroupedCalls > 0, "no call was grouped");
        Assert.True(host.Store.GroupedMembers >= 2 * host.Store.GroupedCalls);
        Assert.Equal(0, host.Store.GroupFallbacks);
        Assert.Equal(prepared.Count, receipts.Select(r => r.JobId).Distinct().Count());
        Assert.Equal(240, await host.JobCountAsync());

        // One event per dispatch, attributed to the member's own command, ordinal zero.
        var events = await host.DispatchEventsAsync();
        Assert.Equal(240, events.Count);
        Assert.Equal(prepared.Select(p => p.OperationId.Value.ToString("N")).Order(), events.Select(e => e.CommandId).Order());
        Assert.All(events, e => Assert.Equal(0, e.Ordinal));

        // Every command resolves and resending it alone returns its original job, whichever way it was first sent.
        foreach (var (p, receipt) in prepared.Zip(receipts))
        {
            var resolved = await host.Prepared.ResolveAsync(p.OperationId, Ct);
            Assert.Equal(DispatchResolutionStatus.Confirmed, resolved.Status);
            Assert.Equal(receipt.JobId, (await host.Prepared.DispatchAsync(p, Ct)).JobId);
        }

        Assert.Equal(240, await host.JobCountAsync());
    }

    [Fact]
    public async Task A_lone_caller_goes_out_alone_and_never_waits_for_company()
    {
        await using var host = await OpenAsync("flone");
        for (var i = 0; i < 25; i++)
        {
            await host.Provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new PingJob("one" + i), cancellationToken: Ct);
        }

        Assert.Equal(0, host.Store.GroupedCalls);
        Assert.Equal(25, await host.JobCountAsync());
    }

    [Fact]
    public async Task Delayed_dispatches_group_with_immediate_ones_and_keep_their_own_eligibility()
    {
        await using var host = await OpenAsync("fdelay");
        var later = DateTimeOffset.UtcNow.AddMinutes(30);
        var prepared = Enumerable.Range(0, 120).Select(i =>
            host.Prepared.Prepare(new PingJob("d" + i), i % 2 == 0 ? new DispatchOptions { NotBefore = later } : null)).ToList();
        await Parallel.ForEachAsync(prepared, new ParallelOptions { MaxDegreeOfParallelism = 12, CancellationToken = Ct },
            async (p, ct) => await host.Prepared.DispatchAsync(p, ct));

        Assert.True(host.Store.GroupedCalls > 0);
        var (snapshot, mux) = await host.SnapshotAsync();
        await mux.DisposeAsync();
        Assert.Equal(120, snapshot.Model.Jobs.Count);
        // Only the 60 immediate jobs are claimable now.
        var claimed = 0;
        var client = await TestClient.CreateAsync(RedisServer.Prefix + host.Configuration, host.Application, new HarnessFaultInjector(), r => TestJobs.Register(r));
        await using var _ = client;
        for (; claimed < 120; claimed++)
        {
            if ((await client.ReserveAsync()).Status != StoreStatus.Applied)
            {
                break;
            }
        }

        Assert.Equal(60, claimed);
    }

    // ------------------------------------------------------------------ cancellation

    [Fact]
    public async Task A_caller_cancelled_while_queued_leaves_no_trace_and_its_neighbours_are_unharmed()
    {
        await using var host = await OpenAsync("fcancelq");
        var hold = host.Faults.Arm(FaultPoint.AfterCommitBeforeReply, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
        var first = host.Prepared.Prepare(new PingJob("first"));
        var firstTask = host.Prepared.DispatchAsync(first, Ct);
        await hold.Barrier.WaitReachedAsync();

        // The only slot is busy: these three queue.
        var b = host.Prepared.Prepare(new PingJob("b"));
        var c = host.Prepared.Prepare(new PingJob("c"));
        var d = host.Prepared.Prepare(new PingJob("d"));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var bTask = host.Prepared.DispatchAsync(b, Ct);
        var cTask = host.Prepared.DispatchAsync(c, cancel.Token);
        var dTask = host.Prepared.DispatchAsync(d, Ct);
        await Task.Delay(300, Ct);
        await cancel.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cTask);

        hold.Barrier.Release();
        await firstTask;
        await Task.WhenAll(bTask, dTask);
        Assert.Equal(DispatchResolutionStatus.NotObserved, (await host.Prepared.ResolveAsync(c.OperationId, Ct)).Status);
        Assert.Equal(3, await host.JobCountAsync());
        Assert.True(host.Store.GroupedCalls >= 1);
    }

    [Fact]
    public async Task A_caller_cancelled_after_its_group_was_sent_gets_the_real_outcome()
    {
        await using var host = await OpenAsync("fcancelg");
        var holdFirst = host.Faults.Arm(FaultPoint.AfterCommitBeforeReply, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
        var holdGroup = host.Faults.Arm(FaultPoint.AfterCommitBeforeReply, FaultSpec.Barrier, FaultTarget.Op(RedisStore.DispatchGroupOperation));
        var firstTask = host.Prepared.DispatchAsync(host.Prepared.Prepare(new PingJob("first")), Ct);
        await holdFirst.Barrier.WaitReachedAsync();

        var b = host.Prepared.Prepare(new PingJob("b"));
        var c = host.Prepared.Prepare(new PingJob("c"));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var bTask = host.Prepared.DispatchAsync(b, cancel.Token);
        var cTask = host.Prepared.DispatchAsync(c, Ct);
        await Task.Delay(300, Ct);
        holdFirst.Barrier.Release();
        await firstTask;
        await holdGroup.Barrier.WaitReachedAsync();

        // The group is committed and its reply is held: cancelling now cannot undo it, and the caller still gets its own receipt.
        await cancel.CancelAsync();
        holdGroup.Barrier.Release();
        var receipt = await bTask;
        await cTask;
        Assert.Equal((await host.Prepared.DispatchAsync(b, Ct)).JobId, receipt.JobId);
        Assert.Equal(3, await host.JobCountAsync());
    }

    // ------------------------------------------------------------------ faults

    [Fact]
    public async Task A_lost_group_reply_gives_each_member_an_unknown_outcome_for_its_own_command_and_each_resolves_once()
    {
        await using var host = await OpenAsync("flost");
        var hold = host.Faults.Arm(FaultPoint.AfterCommitBeforeReply, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
        host.Faults.Arm(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(RedisStore.DispatchGroupOperation));
        var firstTask = host.Prepared.DispatchAsync(host.Prepared.Prepare(new PingJob("first")), Ct);
        await hold.Barrier.WaitReachedAsync();

        var members = Enumerable.Range(0, 3).Select(i => host.Prepared.Prepare(new PingJob("m" + i))).ToList();
        var tasks = members.Select(m => host.Prepared.DispatchAsync(m, Ct)).ToList();
        await Task.Delay(300, Ct);
        hold.Barrier.Release();
        await firstTask;

        foreach (var task in tasks)
        {
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => task);
            Assert.Contains("outcome", ex.GetType().Name + ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // The group committed before the reply was lost: every member resolves, and resending creates nothing new.
        foreach (var m in members)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await host.Prepared.ResolveAsync(m.OperationId, Ct)).Status);
            await host.Prepared.DispatchAsync(m, Ct);
        }

        Assert.Equal(4, await host.JobCountAsync());
    }

    [Fact]
    public async Task Every_write_prefix_of_a_group_recovers_and_resending_each_command_creates_one_job_per_identity()
    {
        await using var host = await OpenAsync("fprefix");
        var store = host.Store;
        var expected = 0;
        var sawUnknown = 0;
        for (var n = 1; n <= 80; n++)
        {
            var commands = Enumerable.Range(0, 3).Select(i => host.Command($"p{n}-{i}")).ToList();
            var entries = commands.Select(c => new GroupEntry(c, 1024, null)).ToList();
            store.Runner.Client.FailAfter = n;
            var completed = false;
            try
            {
                var reply = await store.DispatchGroupCallAsync(host.Application, entries, Ct);
                completed = reply.HasValue && reply.Code != "group_fallback";
            }
            catch (StoreOutcomeUnknownException)
            {
                sawUnknown++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            // Whatever the failure left behind is recovered by the next call; each original command then resolves exactly once.
            foreach (var command in commands)
            {
                var reply = await store.DispatchSingleAsync(command, Ct);
                Assert.Equal(StoreStatus.Applied, reply.Status);
            }

            expected += 3;
            Assert.Equal(expected, await host.JobCountAsync());
            if (completed)
            {
                Assert.True(n > 20, "the sweep never reached an injected write");
                break;
            }
        }

        Assert.True(sawUnknown > 10, "too few injected failures: " + sawUnknown);
        var events = await host.DispatchEventsAsync();
        Assert.Equal(expected, events.Count);
        Assert.Equal(expected, events.Select(e => e.CommandId).Distinct().Count());
    }

    [Fact]
    public async Task A_group_the_script_declines_writes_nothing_and_the_commands_go_alone_with_their_identities()
    {
        await using var host = await OpenAsync("fdecline");
        var one = host.Command("dup");
        var reply = await host.Store.DispatchGroupCallAsync(host.Application, [new GroupEntry(one, 1024, null), new GroupEntry(one, 1024, null)], Ct);
        Assert.False(reply.HasValue);
        Assert.Equal("group_fallback", reply.Code);
        Assert.Equal(0, await host.JobCountAsync());

        // A dispatch with a uniqueness key never enters a group at all.
        var keyed = host.Prepared.Prepare(new PingJob("keyed"), new DispatchOptions { Uniqueness = new DispatchUniqueness("only-one", null, null) });
        Assert.False(DispatchGrouper.Eligible(new DispatchItem(keyed.Intent, keyed.Scopes, keyed.Digests)));
        Assert.Equal(StoreStatus.Applied, (await host.Store.DispatchAsync(host.Command("after"), Ct)).Status);
    }

    [Fact]
    public async Task A_group_with_a_member_past_its_deadline_or_already_sent_answers_each_member_on_its_own_terms()
    {
        await using var host = await OpenAsync("fmixed");
        var sent = host.Command("sent");
        Assert.Equal(StoreStatus.Applied, (await host.Store.DispatchSingleAsync(sent, Ct)).Status);
        var fresh = host.Command("fresh");
        var expired = host.Command("expired") with { NotAfter = DateTimeOffset.UtcNow.AddSeconds(-5) };
        var reply = await host.Store.DispatchGroupCallAsync(host.Application,
            [new GroupEntry(sent, 1024, null), new GroupEntry(fresh, 1024, null), new GroupEntry(expired, 1024, null)], Ct);
        var rows = reply.Result!["items"]!.AsArray().Select(r => RxReply.FromRow(r!.AsArray())).ToList();
        Assert.Equal(("Applied", true), (rows[0].Status, rows[0].Replayed));
        Assert.Equal(("Applied", false), (rows[1].Status, rows[1].Replayed));
        Assert.Equal("CommandExpired", rows[2].Status);
        Assert.Equal(2, await host.JobCountAsync());
    }

    // ------------------------------------------------------------------ AOF restart

    [Fact]
    public async Task Grouped_dispatches_confirmed_before_a_kill_and_restart_of_Redis_are_all_there_afterwards()
    {
        await using var node = await RedisNode.StartAsync();
        await RunwickRedisMigrations.MigrateAsync(node.Configuration, Ct);
        await using var host = await OpenAsync("faof", configuration: node.Configuration);
        var prepared = Enumerable.Range(0, 160).Select(i => host.Prepared.Prepare(new PingJob("k" + i))).ToList();
        await Parallel.ForEachAsync(prepared, new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = Ct },
            async (p, ct) => await host.Prepared.DispatchAsync(p, ct));
        Assert.True(host.Store.GroupedCalls > 0);

        await node.CrashAndRestartAsync();
        var confirmed = 0;
        foreach (var p in prepared)
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    confirmed += (await host.Prepared.ResolveAsync(p.OperationId, Ct)).Status == DispatchResolutionStatus.Confirmed ? 1 : 0;
                    break;
                }
                catch (Exception) when (attempt < 60)
                {
                    await Task.Delay(250, Ct);
                }
            }
        }

        Assert.Equal(160, confirmed);
        Assert.Equal(160, await host.JobCountAsync());
    }
}
