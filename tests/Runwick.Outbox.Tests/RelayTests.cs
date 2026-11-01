using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Outbox.Tests;

/// <summary>OUTBOX-02 … OUTBOX-07, OUT-01 and OUT-02: the relay against real source adapters and a real destination (14 §9.3).</summary>
public sealed class RelayTests
{
    const string Receipts = "SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND operation = 'ReceiveOutbox'";

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-02")]
    public async Task Lost_delivered_mark_maps_the_retry_to_the_original_job(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox02", kind);

        // (a) The persisted transport command is still valid at re-claim: the same command is resent and replayed.
        FaultySource? first = null;
        var h1 = await o.OpenHost(identity: "relay1", wrapSource: s => first = new FaultySource(s));
        first!.LoseMarksBefore(1);
        var a = await o.StageAsync(h1, new PingJob("a"));
        await o.WaitStateAsync(a.MessageId, "delivered", TimeSpan.FromSeconds(40));
        Assert.Equal(1, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));
        await h1.DisposeAsync();

        // (b) The transport command expired before re-claim: a new command under the same delivery identity and cutoff
        // returns the stored mapping, not a second job.
        FaultySource? second = null;
        var h2 = await o.OpenHost(identity: "relay2", relay: r => r.TransportLifetime = TimeSpan.FromSeconds(2),
            wrapSource: s => second = new FaultySource(s));
        second!.LoseMarksBefore(1);
        var b = await o.StageAsync(h2, new PingJob("b"));
        await o.WaitStateAsync(b.MessageId, "delivered", TimeSpan.FromSeconds(40));
        Assert.Equal(3, await o.Destination.CountAsync(Receipts, ("a", o.Destination.Application)));

        foreach (var staged in new[] { a, b })
        {
            var job = await o.DeliveredJobAsync(staged.MessageId);
            Assert.Equal(staged.ProposedJobId.Value, job);
            Assert.Equal(1, await o.MappingsFor(staged.MessageId));
            Assert.Equal(1, await o.Destination.CountAsync("SELECT count(*) FROM runwick.outbox_deliveries WHERE job_id = @j", ("j", job!.Value)));
        }

        Assert.Equal(2, await o.JobsAsync());
        // Each lost mark forced a second attempt; a loaded host may expire a relay lease and add another, still one mapping.
        Assert.Equal(2, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE attempts >= 2"));
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-03")]
    public async Task Two_owners_a_stale_token_and_an_expired_transport_request_leave_one_mapping(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox03", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = await o.StageAsync(stager, new PingJob("x"));

        // Owner A claims with a 1 s lease and stalls; owner B (a running relay) takes over after expiry and delivers.
        await using var stale = await o.NewSourceAsync();
        var leaseA = Assert.Single(await stale.ClaimAsync(Guid.CreateVersion7(), 4, TimeSpan.FromSeconds(1), CancellationToken.None));
        Assert.True(await stale.RecordTransportAsync(leaseA, Guid.CreateVersion7(), Transport.Ms(DateTimeOffset.UtcNow.AddSeconds(1)), CancellationToken.None));
        await Task.Delay(1500);
        await using var relayB = await o.OpenHost(identity: "relayB");
        await o.WaitStateAsync(staged.MessageId, "delivered");
        var jobB = await o.DeliveredJobAsync(staged.MessageId);

        // A's token is stale: renew, transport, retry, block and delivered marks all write nothing.
        var fake = new OutboxDelivered(Guid.CreateVersion7(), "Created", OutboxOperations.PostgresProfile, DateTimeOffset.UtcNow);
        Assert.False(await stale.RenewAsync(leaseA, TimeSpan.FromSeconds(30), CancellationToken.None));
        Assert.False(await stale.RecordTransportAsync(leaseA, Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddSeconds(5), CancellationToken.None));
        Assert.False(await stale.MarkDeliveredAsync(leaseA, fake, CancellationToken.None));
        Assert.False(await stale.RetryLaterAsync(leaseA, TimeSpan.Zero, "stale", CancellationToken.None));
        Assert.False(await stale.BlockAsync(leaseA, "stale", CancellationToken.None));
        Assert.Equal("delivered", await o.StateAsync(staged.MessageId));
        Assert.Equal(jobB, await o.DeliveredJobAsync(staged.MessageId));

        // A still reaches the destination: its expired request applies nothing; a fresh request under the same delivery
        // identity returns B's mapping.
        var prepared = stager.Prepared.ImportPrepared(leaseA.Envelope);
        var identity = Transport.Identity(o.SourceStoreId, staged.MessageId);
        var expired = await stager.Store.ReceiveOutboxAsync(
            Transport.Command(prepared, identity, staged.AcceptUntil, notAfter: DateTimeOffset.UtcNow.AddSeconds(-1)), CancellationToken.None);
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, expired.Effect);
        var fresh = await stager.Store.ReceiveOutboxAsync(Transport.Command(prepared, identity, staged.AcceptUntil), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, fresh.Status);
        Assert.False(fresh.Value!.FirstDelivery);
        Assert.Equal(jobB, fresh.Value.JobId);
        Assert.Equal(1, await o.MappingsFor(staged.MessageId));
        Assert.Equal(1, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-04")]
    public async Task Nothing_is_newly_delivered_at_or_after_the_absolute_cutoff(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox04", kind);
        var host = await o.OpenHost(start: false);
        var horizon = new OutboxStageOptions { AcceptanceHorizon = TimeSpan.FromSeconds(3) };

        // (a) Cutoff equality: a request sent before AcceptUntil whose decision lands at or after it applies nothing.
        var edge = await o.StageAsync(host, new PingJob("edge"), horizon);
        var prepared = host.Prepared.ImportPrepared(await o.EnvelopeAsync(edge.MessageId));
        var identity = Transport.Identity(o.SourceStoreId, edge.MessageId);
        var command = Transport.Command(prepared, identity, edge.AcceptUntil, notAfter: edge.AcceptUntil);
        var hold = o.Destination.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Command(command.CommandId));
        var pending = host.Store.ReceiveOutboxAsync(command, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(hold);
        while (await o.Destination.StoreNow() < edge.AcceptUntil)
        {
            await Task.Delay(50);
        }

        DriverFixture.ReleaseBarrier(hold);
        var late = await pending;
        Assert.Equal(StoreStatus.CommandExpired, late.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, late.Effect);
        Assert.Equal(0, await o.MappingsFor(edge.MessageId));

        // (b) Delivered in time, then the mapping is cleaned up after its retention: the delayed original command and
        // a fresh one under the same identity both apply nothing.
        var kept = await o.StageAsync(host, new PingJob("kept"), horizon);
        var keptPrepared = host.Prepared.ImportPrepared(await o.EnvelopeAsync(kept.MessageId));
        var keptIdentity = Transport.Identity(o.SourceStoreId, kept.MessageId);
        var original = Transport.Command(keptPrepared, keptIdentity, kept.AcceptUntil);
        Assert.Equal(StoreStatus.Applied, (await host.Store.ReceiveOutboxAsync(original, CancellationToken.None)).Status);
        while (await o.Destination.StoreNow() < kept.AcceptUntil)
        {
            await Task.Delay(50);
        }

        await o.Destination.ExecAsync("DELETE FROM runwick.outbox_deliveries WHERE application = @a AND message_id = @m",
            ("a", o.Destination.Application), ("m", kept.MessageId));
        await o.Destination.PruneReceipts("durable", DateTimeOffset.UtcNow.AddDays(30));
        Assert.Equal(StoreStatus.CommandExpired, (await host.Store.ReceiveOutboxAsync(original, CancellationToken.None)).Status);
        var fresh = await host.Store.ReceiveOutboxAsync(Transport.Command(keptPrepared, keptIdentity, kept.AcceptUntil,
            notAfter: DateTimeOffset.UtcNow.AddSeconds(10)), CancellationToken.None);
        Assert.NotEqual(StoreStatus.Applied, fresh.Status);
        Assert.Equal(1, await o.JobsAsync());

        // The relay resolves both after the horizon: no mapping → blocked for reconciliation, never a new job.
        await host.StartRelayAsync();
        await o.WaitStateAsync(edge.MessageId, "blocked");
        await o.WaitStateAsync(kept.MessageId, "blocked");
        Assert.Equal("horizon_passed_undelivered", await o.BlockedReasonAsync(edge.MessageId));
        Assert.Equal(1, await o.JobsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-05")]
    public async Task Unavailable_destination_policy_drift_and_malformed_intent_are_visible_without_false_success(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox05", kind, StoreProfile.DirectPrimaryViaProxy);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var storeId = stager.Runtime.StoreId;

        // (a) Destination unreachable after the relay's handshake: messages stay undelivered with a bounded error and are
        // delivered once it returns.
        await using (var relay = await o.OpenHost(identity: "relay", destination: new OutboxDestination { StoreId = storeId }))
        {
            o.Destination.Proxy!.DropEverything = true;
            var down = await o.StageAsync(stager, new PingJob("down"));
            await Task.Delay(TimeSpan.FromSeconds(4));
            Assert.Equal(0, relay.Relay.Delivered);
            Assert.NotEqual("delivered", await o.StateAsync(down.MessageId));
            o.Destination.Proxy.DropEverything = false;
            await o.WaitStateAsync(down.MessageId, "delivered", TimeSpan.FromSeconds(60));
        }

        // (b) Contract drift: the relay host no longer registers the frozen contract (test.ping); the message blocks, never rewritten.
        var drift = await o.StageAsync(stager, new PingJob("drift"));
        // (c) Malformed intent bytes in the source row: blocked, never rewritten.
        var malformed = await o.StageAsync(stager, new PingJob("malformed"));
        await o.SourceExecAsync(o.Sql("UPDATE {messages} SET intent = " + (kind == SourceKind.Postgres ? "'\\x00'::bytea" : "X'00'") + " WHERE message_id = {m}"),
            malformed.MessageId);
        await using (var drifted = await o.OpenHost(identity: "drifted", configure: r => r.AddJob<KeyedJob>().Named("test.keyed").ToQueue("default").UniqueWhileActive(j => j.Key)))
        {
            await o.WaitStateAsync(drift.MessageId, "blocked");
            await o.WaitStateAsync(malformed.MessageId, "blocked");
        }

        Assert.Equal("contract_not_registered", await o.BlockedReasonAsync(drift.MessageId));
        Assert.False(string.IsNullOrEmpty(await o.BlockedReasonAsync(malformed.MessageId)));
        Assert.Null(await o.DeliveredJobAsync(drift.MessageId));
        Assert.Equal(0, await o.MappingsFor(drift.MessageId));
        Assert.Equal(0, await o.MappingsFor(malformed.MessageId));
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'"));
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-06")]
    public async Task Restores_show_the_finite_horizon_and_the_history_loss_limit(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("outbox06", kind);
        var storeId = await StoreIdAsync(o);

        // Stage without touching the destination, then take a destination backup that predates the delivery.
        var stager = await o.OpenHost(identity: "stager", destination: new OutboxDestination { StoreId = storeId }, ensureReady: false, start: false);
        var staged = await o.StageAsync(stager, new PingJob("x"));
        var backup = await BackupDestinationAsync(o);

        var relay = await o.OpenHost(identity: "relay1");
        await o.WaitStateAsync(staged.MessageId, "delivered");
        var original = await o.DeliveredJobAsync(staged.MessageId);
        await relay.DisposeAsync();

        // (a) Source-only restore within the retained destination history: redelivery (by a new relay host; a stopped
        // hosted service is not restarted) resolves through the mapping.
        await o.SourceExecAsync(o.Sql(RestoreToPending), staged.MessageId);
        await using (var redeliver = await o.OpenHost(identity: "relay1b"))
        {
            await o.WaitStateAsync(staged.MessageId, "delivered");
        }

        Assert.Equal(original, await o.DeliveredJobAsync(staged.MessageId));
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.MappingsFor(staged.MessageId));

        // (b) Both restored from backups that predate the delivery: the destination has no memory of it, so the same
        // message is delivered as new again (a second handler run) — the documented limit (14 §8), asserted so the suite
        // cannot be read as claiming otherwise.
        var restoredAt = await o.Destination.StoreNow();
        await RestoreDestinationAsync(o, backup);
        await o.SourceExecAsync(o.Sql(RestoreToPending), staged.MessageId);
        await using var after = await o.OpenHost(identity: "relay2");
        await o.WaitStateAsync(staged.MessageId, "delivered");
        Assert.Equal(original, await o.DeliveredJobAsync(staged.MessageId));
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal(1, await o.Destination.CountAsync(
            "SELECT count(*) FROM runwick.outbox_deliveries WHERE message_id = @m AND delivered_at >= @t", ("m", staged.MessageId), ("t", restoredAt.UtcDateTime)));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUTBOX-07")]
    public async Task Connection_task_retry_and_shutdown_bounds_hold_through_a_long_outage()
    {
        // PostgreSQL source: the relay's sessions are countable in pg_stat_activity. The SQLite adapter has the same
        // lanes and is covered for task/retry/shutdown bounds by the same assertions below minus the session count.
        await using var o = await OutboxFixture.StartAsync("outbox07", SourceKind.Postgres, StoreProfile.DirectPrimaryViaProxy);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var storeId = stager.Runtime.StoreId;

        // The relay completes its handshake, then the destination goes away for the whole observation window.
        FaultySource? source = null;
        var relay = await o.OpenHost(identity: "relay", destination: new OutboxDestination { StoreId = storeId },
            wrapSource: s => source = new FaultySource(s));
        o.Destination.Proxy!.DropEverything = true;
        for (var i = 0; i < 40; i++)
        {
            await o.StageAsync(stager, new PingJob("m" + i));
        }

        var maxSessions = 0L;
        var idleInTx = 0L;
        var maxInFlight = 0;
        var until = DateTime.UtcNow + TimeSpan.FromSeconds(12);
        while (DateTime.UtcNow < until)
        {
            maxInFlight = Math.Max(maxInFlight, relay.Relay.InFlightCount);
            maxSessions = Math.Max(maxSessions, await o.Destination.CountAsync(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = @d AND application_name = 'rw:outbox:relay'", ("d", SourceDatabase(o))));
            idleInTx += await o.Destination.CountAsync(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = @d AND application_name = 'rw:outbox:relay' AND state LIKE 'idle in transaction%'",
                ("d", SourceDatabase(o)));
            await Task.Delay(100);
        }

        Assert.InRange(maxSessions, 1, 2);
        Assert.Equal(0, idleInTx);
        Assert.InRange(maxInFlight, 0, 4);
        Assert.InRange(source!.MaxClaimBatch, 1, 4);
        Assert.Equal(0, relay.Relay.Delivered);
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'"));

        // Retry spacing never exceeds RetryMax (1 s in tests) plus the lease; nothing is past its cutoff.
        Assert.Equal(0, await o.SourceCountAsync(
            "SELECT count(*) FROM {messages} WHERE state = 'pending' AND next_attempt_at > clock_timestamp() + interval '2 seconds'"));

        // Shutdown during the outage: claims stop first, the host deadline bounds the wait, nothing is marked delivered.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await relay.StopRelayAsync(TimeSpan.FromSeconds(2));
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(6), $"Shutdown took {stopwatch.Elapsed}.");
        var claims = source.Claims;
        await Task.Delay(500);
        Assert.Equal(claims, source.Claims);
        Assert.Equal(0, await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'"));
        Assert.Equal(0, relay.Relay.InFlightCount);

        // After the outage a new relay drains everything exactly once.
        o.Destination.Proxy.DropEverything = false;
        await using var after = await o.OpenHost(identity: "after");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'") < 40 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }

        Assert.Equal(40, await o.JobsAsync());
        Assert.Equal(40, await o.MappingsAsync());
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUT-01")]
    public async Task Concurrent_relays_with_lost_marks_deduplicate_by_message_identity_within_the_horizon(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("out01", kind);
        var stager = await o.OpenHost(identity: "stager", start: false);
        var staged = new List<StagedOutboxMessage>();
        for (var i = 0; i < 60; i++)
        {
            staged.Add(await o.StageAsync(stager, new PingJob("m" + i)));
        }

        var sources = new List<FaultySource>();
        for (var r = 0; r < 3; r++)
        {
            await o.OpenHost(identity: "relay" + r, wrapSource: s =>
            {
                var f = new FaultySource(s) { MarkLossRate = 0.2 };
                sources.Add(f);
                return f;
            });
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(120);
        while (await o.SourceCountAsync("SELECT count(*) FROM {messages} WHERE state = 'delivered'") < staged.Count && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
        }

        Assert.Equal(staged.Count, await o.JobsAsync());
        Assert.Equal(staged.Count, await o.MappingsAsync());
        foreach (var m in staged)
        {
            Assert.Equal(m.ProposedJobId.Value, await o.DeliveredJobAsync(m.MessageId));
        }

        // Every recorded mark names the mapping's job; marks lost before the write were repaired by the mapping.
        Assert.All(sources.SelectMany(s => s.Marks), mark => Assert.Contains(staged, m => m.MessageId == mark.MessageId && m.ProposedJobId.Value == mark.JobId));
    }

    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "OUT-02")]
    public async Task Replay_outside_the_retained_history_is_an_explicit_limit_not_a_silent_dispatch(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("out02", kind);
        var host = await o.OpenHost(start: false);
        var staged = await o.StageAsync(host, new PingJob("x"), new OutboxStageOptions { AcceptanceHorizon = TimeSpan.FromSeconds(2) });
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Never delivered within the horizon: after it, the relay blocks for operator reconciliation and dispatches nothing.
        await host.StartRelayAsync();
        await o.WaitStateAsync(staged.MessageId, "blocked");
        Assert.Equal("horizon_passed_undelivered", await o.BlockedReasonAsync(staged.MessageId));
        Assert.Equal(0, await o.JobsAsync());

        // The cutoff cannot be extended under the same message ID: restaging with a longer horizon is a conflict only if
        // intent changed, otherwise it returns the stored row with its original cutoff.
        await using (var business = await o.BeginBusinessAsync())
        {
            var again = await host.StageAsync(new PingJob("x"), business,
                new OutboxStageOptions { MessageId = staged.MessageId, AcceptanceHorizon = TimeSpan.FromDays(7) });
            Assert.True(again.Existing);
            Assert.Equal(staged.AcceptUntil, again.AcceptUntil);
            await business.Transaction.CommitAsync();
        }

        // Rerunning the work needs an explicit new message identity (and acknowledges possible duplicate effects).
        var rerun = await o.StageAsync(host, new PingJob("x"));
        await o.WaitStateAsync(rerun.MessageId, "delivered");
        Assert.Equal(1, await o.JobsAsync());
        Assert.Equal("blocked", await o.StateAsync(staged.MessageId));
    }

    const string RestoreToPending = """
        UPDATE {messages} SET state = 'pending', next_attempt_at = staged_at, lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL, transport_command_id = NULL, transport_not_after = NULL, delivered_job_id = NULL,
            delivered_disposition = NULL, confirmed_profile = NULL, delivered_at = NULL
        WHERE message_id = {m}
        """;

    static async Task<Guid> StoreIdAsync(OutboxFixture o)
    {
        await using var c = new Npgsql.NpgsqlConnection(o.Destination.AdminConnectionString);
        await c.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand("SELECT store_id FROM runwick.store_metadata WHERE singleton_id = 1", c);
        return (Guid)(await cmd.ExecuteScalarAsync())!;
    }

    /// <summary>A file-level copy of the destination database (CREATE DATABASE … TEMPLATE): a backup at this instant.</summary>
    static async Task<string> BackupDestinationAsync(OutboxFixture o)
    {
        var backup = o.Destination.DatabaseName + "_bk";
        Npgsql.NpgsqlConnection.ClearAllPools();
        var admin = o.Destination.Server.ConnectionStringFor("postgres", pooling: false);
        await StoreServer.ExecAsync(admin, $"DROP DATABASE IF EXISTS {backup} WITH (FORCE)");
        await StoreServer.ExecAsync(admin, $"CREATE DATABASE {backup} TEMPLATE {o.Destination.DatabaseName}");
        return backup;
    }

    /// <summary>Replaces the destination with the backup (all destination clients must be closed).</summary>
    static async Task RestoreDestinationAsync(OutboxFixture o, string backup)
    {
        Npgsql.NpgsqlConnection.ClearAllPools();
        var admin = o.Destination.Server.ConnectionStringFor("postgres", pooling: false);
        await StoreServer.ExecAsync(admin, $"DROP DATABASE {o.Destination.DatabaseName} WITH (FORCE)");
        await StoreServer.ExecAsync(admin, $"ALTER DATABASE {backup} RENAME TO {o.Destination.DatabaseName}");
    }

    static string SourceDatabase(OutboxFixture o) => new Npgsql.NpgsqlConnectionStringBuilder(o.SourceConnectionString).Database!;
}
