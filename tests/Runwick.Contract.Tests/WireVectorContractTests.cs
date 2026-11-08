using Microsoft.Data.Sqlite;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Contract.Tests;

/// <summary>
/// The store-side wire vectors of 02 §13 on every store: WV-09 (stored fingerprint version, retention), WV-10 (active aliasing,
/// composition) and WV-12 (batches). The vectors that need only the codec (WV-01…WV-08, WV-11) run in Runwick.Core.Tests.
/// The rolled-back staged dispatch of WV-10 needs a caller transaction (PostgreSQL only): Runwick.Postgres.Tests.WireVectorTests.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class WireVectorContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    static DispatchOptions Window(string key, string? active = null) => new() { Uniqueness = new DispatchUniqueness(active, key, TimeSpan.FromHours(1)) };

    static DispatchOptions Active(string key) => new() { Uniqueness = new DispatchUniqueness(key, null, null) };

    static async Task<int> JobCount(DriverFixture f) => (await f.ReadCommittedSnapshot()).Model.Jobs.Count;

    // ------------------------------------------------------------------ admin edits (setup only)

    /// <summary>Sets the stored fingerprint version of every window binding (the SQL stores' column, the Redis record's field).</summary>
    static async Task SetBindingVersion(DriverFixture f, int version)
    {
        switch (f.Kind)
        {
            case StoreKind.Postgres:
                await f.ExecAsync("UPDATE runwick.dispatch_keys SET fingerprint_version = @v WHERE key_kind = 'window'", ("v", version));
                break;
            case StoreKind.Sqlite:
                using (var conn = new SqliteConnection(f.AdminConnectionString))
                {
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "UPDATE runwick_dispatch_keys SET fingerprint_version = @v WHERE key_kind = 'window'";
                    cmd.Parameters.AddWithValue("@v", version);
                    Assert.Equal(1, cmd.ExecuteNonQuery());
                }

                break;
            default:
                var db = (await RedisServer.AdminAsync()).GetDatabase();
                var hash = RedisServer.Key(f.Application, "keys:window");
                var entry = Assert.Single(await db.HashGetAllAsync(hash));
                var json = System.Text.Json.Nodes.JsonNode.Parse((string)entry.Value!)!.AsObject();
                json["ver"] = version;
                await db.HashSetAsync(hash, entry.Name, json.ToJsonString());
                break;
        }
    }

    /// <summary>Removes a job and its history (what a retention pass does once nothing holds it), leaving every other row.</summary>
    static async Task PurgeJob(DriverFixture f, Guid job)
    {
        switch (f.Kind)
        {
            case StoreKind.Postgres:
                await f.ExecAsync("DELETE FROM runwick.jobs WHERE job_id = @j", ("j", job));
                break;
            case StoreKind.Sqlite:
                using (var conn = new SqliteConnection(f.AdminConnectionString))
                {
                    conn.Open();
                    using var pragma = conn.CreateCommand();
                    pragma.CommandText = "PRAGMA foreign_keys = ON";
                    pragma.ExecuteNonQuery();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "DELETE FROM runwick_jobs WHERE job_id = @j";
                    cmd.Parameters.AddWithValue("@j", job.ToByteArray(bigEndian: true));
                    Assert.Equal(1, cmd.ExecuteNonQuery());
                }

                break;
            default:
                var mux = await RedisServer.AdminAsync();
                var db = mux.GetDatabase();
                var prefix = RedisServer.Key(f.Application, "job:" + job.ToString("N"));
                await foreach (var k in mux.GetServer(mux.GetEndPoints()[0]).KeysAsync(db.Database, prefix + "*", 100))
                {
                    await db.KeyDeleteAsync(k);
                }

                break;
        }
    }

    // ------------------------------------------------------------------ WV-09

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "WV-09")]
    public async Task An_unknown_stored_fingerprint_version_is_UnsupportedVersion_with_nothing_written_and_version_1_survives_a_purged_job(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "wv09");
        await using var _ = f;
        var first = await c.Dispatcher.DispatchAsync(new PingJob("one"), Window("w"), Ct);
        Assert.Equal(DispatchDisposition.Created, first.Disposition);

        // Unknown version: the binding is not interpreted; the dispatch is refused and writes nothing.
        await SetBindingVersion(f, 2);
        var before = await f.ReadCommittedSnapshot();
        var refused = await Assert.ThrowsAnyAsync<RunwickException>(() => c.Dispatcher.DispatchAsync(new PingJob("one"), Window("w"), Ct));
        Assert.Contains("fingerprint_version_unsupported", refused.ToString(), StringComparison.Ordinal);
        Assert.Empty(before.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Single(before.Model.Jobs);

        // Version 1 after the bound job's history was purged: no foreign key, so the binding still compares and answers.
        await SetBindingVersion(f, 1);
        await PurgeJob(f, first.JobId.Value);
        var again = await c.Dispatcher.DispatchAsync(new PingJob("one"), Window("w"), Ct);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, first.JobId), (again.Disposition, again.DuplicateMatch, again.JobId));
        Assert.Equal(0, await JobCount(f));
    }

    // ------------------------------------------------------------------ WV-10

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "WV-10")]
    public async Task Active_aliasing_reuses_the_job_and_stores_the_incoming_digests_and_a_composition_conflict_rewrites_nothing(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "wv10");
        await using var _ = f;

        // Active key user:42 is bound to J1. A request with another payload, the same active key and a new window key W reuses J1.
        var j1 = await c.Dispatcher.DispatchAsync(new PingJob("p1"), Active("user:42"), Ct);
        var reused = await c.Dispatcher.DispatchAsync(new PingJob("p2"), Window("w", active: "user:42"), Ct);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Active, j1.JobId), (reused.Disposition, reused.DuplicateMatch, reused.JobId));
        Assert.Equal(1, await JobCount(f));

        // W stores the incoming request's digests (p2 with its active key), not J1's: the p2 request matches both keys, the p1 request
        // (which created J1) now conflicts on W; and J1's payload is still p1's.
        var viaBoth = await c.Dispatcher.DispatchAsync(new PingJob("p2"), Window("w", active: "user:42"), Ct);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.ActiveAndWindow, j1.JobId), (viaBoth.Disposition, viaBoth.DuplicateMatch, viaBoth.JobId));
        await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchAsync(new PingJob("p1"), Window("w", active: "user:42"), Ct));
        var reserved = await c.ReserveAsync();
        Assert.Equal(j1.JobId.Value, reserved.Value!.Position.JobId);
        Assert.Contains("p1", System.Text.Encoding.UTF8.GetString(reserved.Value.PayloadUtf8), StringComparison.Ordinal);
        Assert.DoesNotContain("p2", System.Text.Encoding.UTF8.GetString(reserved.Value.PayloadUtf8), StringComparison.Ordinal);

        // Composition: active key bound to J3, window key bound to J4 (J3 ≠ J4). A request naming both is a conflict; nothing is rewritten.
        var j3 = await c.Dispatcher.DispatchAsync(new PingJob("j3"), Active("user:43"), Ct);
        var j4 = await c.Dispatcher.DispatchAsync(new PingJob("j4"), Window("w2"), Ct);
        Assert.NotEqual(j3.JobId, j4.JobId);
        var before = await f.ReadCommittedSnapshot();
        await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchAsync(new PingJob("j4"), Window("w2", active: "user:43"), Ct));
        Assert.Empty(before.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(3, before.Model.Jobs.Count);
    }

    // ------------------------------------------------------------------ WV-12

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "WV-12")]
    public async Task A_batch_with_a_window_conflict_fails_whole_or_commits_the_others_with_one_receipt_and_101_items_are_refused(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "wv12");
        await using var _ = f;
        var orig = await c.Dispatcher.DispatchAsync(new PingJob("orig"), Window("w"), Ct);
        List<BatchDispatchItem> items =
        [
            new(new PingJob("n0")),
            new(new PingJob("changed"), Window("w")), // item 1 conflicts with the live window binding
            new(new PingJob("n2")),
        ];
        var pre = await f.ReadCommittedSnapshot();

        // Default: the whole batch fails and nothing is created.
        var conflict = await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchManyAsync(items, cancellationToken: Ct));
        var failure = Assert.Single(conflict.ItemFailures);
        Assert.Equal((1, orig.JobId), (failure.Ordinal, failure.ConflictingJobId));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // ContinueOnConflict: item 1 is reported as a failure, items 0 and 2 commit together, under one receipt with ordinals 0-2.
        var prepared = c.Prepared.PrepareMany(items, new BatchDispatchOptions { ContinueOnConflict = true });
        var receipt = await c.Prepared.DispatchAsync(prepared, Ct);
        Assert.Equal([0, 1, 2], receipt.Items.Select(i => i.Ordinal));
        Assert.Equal(DispatchDisposition.Created, receipt.Items[0].Receipt!.Disposition);
        Assert.Null(receipt.Items[1].Receipt);
        Assert.Equal((1, orig.JobId), (receipt.Items[1].Failure!.Ordinal, receipt.Items[1].Failure!.ConflictingJobId));
        Assert.Equal(DispatchDisposition.Created, receipt.Items[2].Receipt!.Disposition);
        Assert.Equal(3, await JobCount(f));

        // Replaying the same operation returns the recorded per-ordinal results and creates nothing.
        var replay = await c.Prepared.DispatchAsync(prepared, Ct);
        Assert.Equal(receipt.Items, replay.Items);
        var resolution = await c.Prepared.ResolveAsync(prepared.OperationId, Ct);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal(receipt.Items, resolution.BatchReceipt!.Items);
        Assert.Equal(3, await JobCount(f));

        // 101 items: batch_too_large, known not applied.
        var tooMany = Enumerable.Range(0, 101).Select(i => new BatchDispatchItem(new PingJob("x" + i))).ToList();
        var large = await Assert.ThrowsAsync<DispatchValidationException>(() => c.Dispatcher.DispatchManyAsync(tooMany, cancellationToken: Ct));
        Assert.Equal((RunwickErrorCodes.BatchTooLarge, ErrorDisposition.KnownNotApplied), (large.Code, large.Disposition));
        Assert.Equal(3, await JobCount(f));
    }
}
