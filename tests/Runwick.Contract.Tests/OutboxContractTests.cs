using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// The outbox destination (14: OUT-01, OUT-02) on every store, ported from Runwick.Outbox.Tests.RelayTests.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
public sealed class OutboxContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    /// <summary>Rows of a table in the store's own form: SQL tables, or the Redis key that holds them.</summary>
    static async Task<long> Rows(DriverFixture f, string table)
    {
        if (f.Kind != StoreKind.Redis)
        {
            return await f.CountAsync($"SELECT count(*) FROM {(f.Kind == StoreKind.Sqlite ? "runwick_" : "runwick.")}{table}");
        }

        var tables = (await f.ReadCommittedSnapshot()).Tables;
        return table == "jobs"
            ? tables.Keys.Count(k => k.StartsWith("job:", StringComparison.Ordinal))
            : tables.GetValueOrDefault("outbox", []).Count(r => r.Contains('{', StringComparison.Ordinal));
    }

    static string Profile(StoreKind kind) => kind switch
    {
        StoreKind.Sqlite => AcknowledgementProfiles.SqliteWalFull,
        StoreKind.Redis => "redis.aof-always.v1",
        _ => OutboxOperations.PostgresProfile,
    };

    // ------------------------------------------------------------------ OUT-01, OUT-02 (store level)

    static StoreCommand<ReceiveOutboxRequest> Receive(TestClient c, PreparedDispatch prepared, OutboxSourceIdentity identity, DateTimeOffset acceptUntil)
    {
        var cutoff = DateTimeOffset.FromUnixTimeMilliseconds(acceptUntil.ToUnixTimeMilliseconds());
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(20);
        deadline = DateTimeOffset.FromUnixTimeMilliseconds((deadline < cutoff ? deadline : cutoff).ToUnixTimeMilliseconds());
        var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, cutoff.ToUnixTimeMilliseconds(), prepared.Intent,
            prepared.Digests);
        return new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(), deadline,
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), cutoff, fingerprint, prepared.StoreId));
    }

    static PreparedDispatch Prepare(TestClient c, IQueueJob job) =>
        c.Services.GetRequiredService<DispatchPreparer>().Prepare(c.Runtime.StoreId, job, null);

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OUT-01")]
    public async Task Out01_concurrent_transport_commands_deduplicate_by_message_identity(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "out01");
        await using var _ = f;
        var destination = (IOutboxDestinationStore)c.Driver;
        var prepared = Prepare(c, new PingJob("m"));
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var acceptUntil = DateTimeOffset.UtcNow.AddDays(1);

        // Several relays send their own transport commands for one message: one job, one mapping, one first delivery.
        var commands = Enumerable.Range(0, 8).Select(_ => Receive(c, prepared, identity, acceptUntil)).ToList();
        var replies = await Task.WhenAll(commands.Select(cmd => destination.ReceiveOutboxAsync(cmd, Ct)));
        Assert.All(replies, r => Assert.Equal((StoreStatus.Applied, prepared.Intent.ProposedJobId), (r.Status, r.Value!.JobId)));
        Assert.Single(replies, r => r.Value!.FirstDelivery);
        var profile = Profile(kind);
        Assert.All(replies, r => Assert.Equal(profile, r.Value!.ConfirmedProfile));
        Assert.Equal(1, await Rows(f, "jobs"));
        Assert.Equal(1, await Rows(f, "outbox_deliveries"));

        // A resent command replays its receipt; different intent under the same identity is a conflict, not a dispatch.
        var resent = await destination.ReceiveOutboxAsync(commands[0], Ct);
        Assert.Equal((StoreStatus.Applied, true), (resent.Status, resent.Replayed));
        var other = Prepare(c, new PingJob("other"));
        var conflict = await destination.ReceiveOutboxAsync(Receive(c, other, identity, acceptUntil), Ct);
        Assert.Equal((StoreStatus.CommandConflict, "delivery_conflict", prepared.Intent.ProposedJobId),
            (conflict.Status, conflict.Error!.Code, conflict.Error.ConflictingJobId));

        var found = await destination.FindDeliveryAsync(c.Application, identity, commands[0].Body.DeliveryFingerprint, Ct);
        Assert.Equal((prepared.Intent.ProposedJobId, false), (found.Delivery!.JobId, found.Conflict));
        var mismatch = await destination.FindDeliveryAsync(c.Application, identity, new byte[32], Ct);
        Assert.Equal((null, true), (mismatch.Delivery, mismatch.Conflict));
        Assert.Equal(1, await Rows(f, "jobs"));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OUT-02")]
    public async Task Out02_a_message_outside_its_horizon_is_an_explicit_limit_not_a_silent_dispatch(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "out02");
        await using var _ = f;
        var destination = (IOutboxDestinationStore)c.Driver;
        var prepared = Prepare(c, new PingJob("x"));
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var late = Receive(c, prepared, identity, DateTimeOffset.UtcNow.AddMilliseconds(1500));
        await Task.Delay(2000, Ct);

        // Past AcceptUntil no command can apply (its NotAfter is never later), no mapping exists, and nothing was dispatched.
        Assert.Equal(StoreStatus.CommandExpired, (await destination.ReceiveOutboxAsync(late, Ct)).Status);
        var lookup = await destination.FindDeliveryAsync(c.Application, identity, late.Body.DeliveryFingerprint, Ct);
        Assert.Equal((null, false), (lookup.Delivery, lookup.Conflict));
        Assert.Equal(0, await Rows(f, "jobs"));

        // A NotAfter beyond AcceptUntil is refused before the store is touched.
        var beyond = Receive(c, prepared, new OutboxSourceIdentity(identity.SourceStoreId, Guid.CreateVersion7()), DateTimeOffset.UtcNow.AddDays(1));
        var invalid = await destination.ReceiveOutboxAsync(beyond with { NotAfter = beyond.Body.AcceptUntil.AddSeconds(1) }, Ct);
        Assert.Equal(StoreStatus.InvalidRequest, invalid.Status);

        // A delivered mapping keeps answering after the horizon, (cleanup after retain_until is a retention case).
        var delivered = Receive(c, prepared, new OutboxSourceIdentity(identity.SourceStoreId, Guid.CreateVersion7()), DateTimeOffset.UtcNow.AddMilliseconds(1500));
        Assert.Equal(StoreStatus.Applied, (await destination.ReceiveOutboxAsync(delivered, Ct)).Status);
        await Task.Delay(2000, Ct);
        var after = await destination.FindDeliveryAsync(c.Application, delivered.Body.Source, delivered.Body.DeliveryFingerprint, Ct);
        Assert.Equal(prepared.Intent.ProposedJobId, after.Delivery!.JobId);
    }
}
