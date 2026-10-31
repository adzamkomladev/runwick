using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Internal;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Serialization;

namespace Runwick.Postgres.Tests;

public sealed record JsonPayloadJob(string Id, string Text, JsonElement Data) : IQueueJob;

/// <summary>ENC-01 (SP-005) and SER-06/SER-07 (SP-007) on the real store (07 §3, §SER).</summary>
public sealed class EncodingCaseTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(120);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<DateTimeOffset> InstantAsync(string connectionString, string sql, Guid job)
    {
        var conn = new NpgsqlConnection(connectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync();
            using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("j", job);
            return new DateTimeOffset(DateTime.SpecifyKind((DateTime)(await cmd.ExecuteScalarAsync())!, DateTimeKind.Utc));
        }
    }

    // ------------------------------------------------------------------ ENC-01

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "ENC-01")]
    [Trait("RequiresDocker", "true")]
    public async Task Sub_millisecond_instants_round_and_an_expiry_instant_is_expired()
    {
        // (a) The store clock is frozen with libfaketime (07 I-9) so decisions land exactly on an expiry instant.
        await using var db = await TestDatabase.StartFakeTimeAsync();
        await RunwickPostgresMigrations.MigrateAsync(db.For("rw:harness:migrate", pooling: false));
        var admin = db.For("rw:harness:admin");
        var t = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 2);
        await db.FreezeClockAsync(t);
        try
        {
            await using var c = await TestClient.CreateAsync(db.For("rw:enc01"), "enc01", new HarnessFaultInjector(), r => TestJobs.Register(r));
            var ms = TimeSpan.FromMilliseconds(1);

            // NotBefore rounds up to the millisecond.
            var later = await c.Dispatcher.DispatchAsync(new PingJob("later"), new DispatchOptions { NotBefore = t + TimeSpan.FromSeconds(10) + TimeSpan.FromTicks(4) });
            Assert.Equal(t + TimeSpan.FromSeconds(10) + ms, await InstantAsync(admin, "SELECT eligible_at FROM runwick.jobs WHERE job_id = @j", later.JobId.Value));

            // A lease expiry landing on a sub-millisecond instant rounds down.
            var now = await c.Dispatcher.DispatchAsync(new PingJob("now"));
            var reserved = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(90) + TimeSpan.FromTicks(6_000))).Value!;
            Assert.Equal(now.JobId.Value, reserved.Position.JobId);
            Assert.Equal(t + TimeSpan.FromSeconds(90), reserved.Lease.ExpiresAt);
            Assert.Equal(t + TimeSpan.FromSeconds(90), await InstantAsync(admin, "SELECT lease_expires_at FROM runwick.jobs WHERE job_id = @j", now.JobId.Value));
            var lease = reserved.Lease.Handle;

            // A command NotAfter rounds down; deciding at exactly NotAfter is expired, one millisecond earlier is not.
            await db.FreezeClockAsync(t + TimeSpan.FromSeconds(45));
            var expired = await c.BeginAsync(lease, notAfter: t + TimeSpan.FromSeconds(45) + TimeSpan.FromTicks(5_000));
            Assert.Equal(StoreStatus.CommandExpired, expired.Reply.Status);
            var begun = await c.BeginAsync(lease, notAfter: t + TimeSpan.FromSeconds(45) + ms + TimeSpan.FromTicks(5_000));
            Assert.Equal(StoreStatus.Applied, begun.Reply.Status);

            // The lease is live one second before its expiry and expired at exactly the expiry instant.
            await db.FreezeClockAsync(t + TimeSpan.FromSeconds(89));
            Assert.Empty(await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None));
            await db.FreezeClockAsync(t + TimeSpan.FromSeconds(90));
            Assert.Equal(now.JobId.Value, Assert.Single(await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).JobId);
            var renew = await c.RenewAsync(lease, notAfter: t + TimeSpan.FromSeconds(120));
            Assert.NotEqual(StoreStatus.Applied, renew.Status);
            Assert.NotEqual(StoreStatus.CommandExpired, renew.Status);
            Assert.Equal(t + TimeSpan.FromSeconds(90), await InstantAsync(admin, "SELECT lease_expires_at FROM runwick.jobs WHERE job_id = @j", now.JobId.Value));
        }
        finally
        {
            await db.StepClockAsync(TimeSpan.Zero);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "ENC-01")]
    public async Task Uuid_order_of_the_driver_comparer_matches_server_order_by()
    {
        // (b) v7, v4 and byte patterns on which the little-endian Guid.ToByteArray() order and big-endian order differ.
        var random = new Random(20261031);
        var ids = new List<Guid>();
        for (var i = 0; i < 4_000; i++)
        {
            ids.Add(Guid.CreateVersion7(DateTimeOffset.UnixEpoch + TimeSpan.FromMilliseconds(random.NextInt64(0, 1L << 47))));
            ids.Add(Guid.NewGuid());
        }

        for (var i = 0; i < 2_000; i++)
        {
            var bytes = new byte[16];
            bytes[random.Next(16)] = (byte)random.Next(256);
            bytes[random.Next(16)] = (byte)random.Next(256);
            ids.Add(new Guid(bytes, bigEndian: true));
        }

        Assert.Equal(10_000, ids.Count);
        var expected = ids.OrderBy(g => g, UuidOrder.Instance).ToList();
        Assert.NotEqual(expected, ids.OrderBy(g => g.ToByteArray(), Comparer<byte[]>.Create((x, y) => x.AsSpan().SequenceCompareTo(y))).ToList());

        await using var f = await DriverFixture.StartAsync(Ns("enc01b"));
        var conn = new NpgsqlConnection(f.AdminConnectionString);
        await using (conn)
        {
            await conn.OpenAsync();
            using var cmd = new NpgsqlCommand("SELECT id FROM unnest(@ids) AS t(id) ORDER BY id", conn);
            cmd.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = ids.ToArray() });
            var server = new List<Guid>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                server.Add(reader.GetGuid(0));
            }

            Assert.Equal(expected, server);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "ENC-01")]
    public async Task Fence_at_the_top_of_its_range_is_exact_and_overflow_commits_nothing()
    {
        // (c) A fenced resource scope whose counter starts at long.MaxValue − 1.
        await using var f = await DriverFixture.StartAsync(Ns("enc01c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("x", "r1"))).JobId.Value;
        await f.ExecAsync("UPDATE runwick.concurrency_scopes SET fencing_enabled = true, fencing_counter = @v WHERE scope_kind = 'resource'", ("v", long.MaxValue - 1));

        var first = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal(long.MaxValue, Assert.Single(first.Value!.Fences).Value);
        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(first.Value.Lease.Handle)).Status);
        var before = await f.ReadCommittedSnapshot();

        // The second increment overflows: an explicit error, and the transaction commits nothing.
        var second = await Record.ExceptionAsync(async () =>
        {
            var reply = await c.ReserveAsync();
            Assert.NotEqual(StoreStatus.Applied, reply.Status);
            throw new InvalidOperationException($"{reply.Status}: {reply.Error?.Code}");
        });
        Assert.NotNull(second);
        Assert.Matches("overflow|out of range|fence", second.ToString());
        Assert.Equal(long.MaxValue, await f.CountAsync("SELECT fencing_counter FROM runwick.concurrency_scopes WHERE scope_kind = 'resource'"));
        Assert.Equal("pending", (await f.ReadCommittedSnapshot()).Model.Jobs.Single(j => j.JobId == job).State);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.Equal(before.Model.Jobs.Single().Revision, (await f.ReadCommittedSnapshot()).Model.Jobs.Single().Revision);
    }

    // ------------------------------------------------------------------ SER-06 / SER-07

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "SER-06")]
    public async Task Reserved_payload_bytes_and_profile_are_identical_to_the_dispatched_ones()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ser06"));
        var c = await f.OpenClient(configure: r => r.AddJob<JsonPayloadJob>().Named("test.json").ToQueue("default"));
        string[] data = ["1", "1.0", "1e0", "-0", "123456789012345678901234567890", "\"\\u00e9\\\"\\\\\"", "\"é\\n\"", """{"b":1.0,"a":[1e0,-0]}"""];
        string[] texts = ["plain", "é \" \\ / \u0001 \n"];
        var sent = new Dictionary<Guid, byte[]>();
        foreach (var d in data)
        {
            foreach (var text in texts)
            {
                using var doc = JsonDocument.Parse(d);
                var prepared = c.Prepared.Prepare(new JsonPayloadJob(d, text, doc.RootElement.Clone()));
                var receipt = await c.Prepared.DispatchAsync(prepared);
                sent[receipt.JobId.Value] = prepared.Intent.PayloadUtf8.ToArray();
            }
        }

        var reserved = new List<ReservedJob>();
        while (await c.ReserveAsync() is { Status: StoreStatus.Applied, Value: { } r })
        {
            reserved.Add(r);
        }

        Assert.Equal(sent.Count, reserved.Count);
        foreach (var r in reserved)
        {
            Assert.Equal(sent[r.Position.JobId], r.PayloadUtf8);
            Assert.Equal((RunwickJsonProfile.ProfileId, RunwickJsonProfile.ProfileVersion), (r.Contract.SerializerProfileId, r.Contract.SerializerProfileVersion));
        }

        Assert.Equal(0, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs AS j JOIN runwick.job_payloads AS p USING (application, job_id)
            WHERE j.serializer_profile_id <> @id OR p.serializer_profile_id <> @id
               OR j.serializer_profile_version <> @v OR p.serializer_profile_version <> @v
            """, ("id", RunwickJsonProfile.ProfileId), ("v", RunwickJsonProfile.ProfileVersion)));
        foreach (var (job, bytes) in sent)
        {
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_payloads WHERE job_id = @j AND payload_utf8 = @b", ("j", job), ("b", bytes)));
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "SER-07")]
    public async Task Unsupported_profile_is_never_reserved_and_malformed_data_fails_permanently_once()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ser07"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 2);
        var unsupported = (await w.DispatchAsync("unsupported")).JobId.Value;
        var malformed = (await w.DispatchAsync("malformed")).JobId.Value;

        // Same job name and payload version under a profile this worker does not support; one statement keeps the
        // jobs → job_payloads profile reference consistent.
        await f.ExecAsync("""
            WITH p AS (UPDATE runwick.job_payloads SET serializer_profile_id = 'runwick.json-unsupported' WHERE job_id = @j RETURNING job_id)
            UPDATE runwick.jobs SET serializer_profile_id = 'runwick.json-unsupported' WHERE job_id = (SELECT job_id FROM p)
            """, ("j", unsupported));
        await f.ExecAsync("UPDATE runwick.job_payloads SET payload_utf8 = @b WHERE job_id = @j", ("j", malformed), ("b", Encoding.UTF8.GetBytes("""{"id":""")));

        await w.StartAsync();
        var ok = (await w.DispatchAsync("ok")).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("ok") == 1, Wait, "the supported job");
        var deadline = DateTime.UtcNow + Wait;
        while (await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'failed'", ("j", malformed)) == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "malformed job failed");
            await Task.Delay(250);
        }
        await Task.Delay(TimeSpan.FromSeconds(3));
        await w.StopAsync();

        Assert.Equal(0, w.Script.InvocationsOf("malformed"));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", malformed)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j AND outcome = 'permanent_failure' AND authorized_at IS NOT NULL", ("j", malformed)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", malformed)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'pending'", ("j", unsupported)));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", unsupported)));
        Assert.Equal(0, w.Script.InvocationsOf("unsupported"));
        Assert.NotEqual(Guid.Empty, ok);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
