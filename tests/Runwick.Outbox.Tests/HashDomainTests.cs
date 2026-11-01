using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Npgsql;
using Runwick.Harness.Jobs;

namespace Runwick.Outbox.Tests;

/// <summary>
/// SER-11 for the outbox hash domains on both sources (14 §7, §10 item 2): what each source adapter stores as
/// <c>delivery_fingerprint</c>, what the destination keeps in <c>outbox_deliveries</c>, and the ReceiveOutbox/1 receipt
/// fingerprint of the persisted transport command are recomputed from the stored row by the independent Node encoder
/// (tests/vectors/tools/canonical.mjs, which also generates the frozen tests/vectors/wire/outbox.json).
/// </summary>
public sealed class HashDomainTests
{
    [Theory]
    [MemberData(nameof(OutboxData.Sources), MemberType = typeof(OutboxData))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-017")]
    [Trait("Case", "SER-11")]
    public async Task Stored_fingerprints_match_the_independent_encoder_on_both_sources(SourceKind kind)
    {
        await using var o = await OutboxFixture.StartAsync("oser11", kind);
        var host = await o.OpenHost();
        var messageId = Guid.CreateVersion7();
        await o.StageAsync(host, new PingJob("vector"), new OutboxStageOptions { MessageId = messageId });
        await o.WaitStateAsync(messageId, "delivered");

        var row = await ReadRowAsync(o, messageId);
        var envelope = JsonNode.Parse(row.Intent)!.AsObject();
        Assert.Equal(Convert.ToHexStringLower(row.BusinessDigest), envelope["businessDigest"]!.GetValue<string>());
        Assert.Equal(Convert.ToHexStringLower(row.PolicyDigest), envelope["policyDigest"]!.GetValue<string>());

        var input = new JsonObject
        {
            ["delivery"] = new JsonObject
            {
                ["sourceStoreId"] = Text(o.SourceStoreId),
                ["messageId"] = Text(messageId),
                ["destinationStoreId"] = Text(row.DestinationStoreId),
                ["application"] = row.Application,
                ["businessDigest"] = Convert.ToHexStringLower(row.BusinessDigest),
                ["policyDigest"] = Convert.ToHexStringLower(row.PolicyDigest),
                ["diagnosticMetadata"] = envelope["diagnosticMetadata"]!.DeepClone(),
                ["proposedJobId"] = Text(row.ProposedJobId),
                ["acceptUntil"] = row.AcceptUntilMs,
            },
            ["transport"] = new JsonObject { ["commandId"] = Text(row.TransportCommandId), ["notAfter"] = row.TransportNotAfterMs },
        };
        using var expected = JsonDocument.Parse(RunEncoder(input.ToJsonString()));
        var delivery = expected.RootElement.GetProperty("delivery").GetString();
        var command = expected.RootElement.GetProperty("command").GetString();

        Assert.Equal(delivery, Convert.ToHexStringLower(row.DeliveryFingerprint));
        Assert.Equal([delivery!], await HexAsync(o, """
            SELECT encode(delivery_fingerprint, 'hex') FROM runwick.outbox_deliveries
            WHERE application = @a AND source_store_id = @s AND message_id = @m
            """, ("a", o.Destination.Application), ("s", o.SourceStoreId), ("m", messageId)));
        Assert.Equal([command!], await HexAsync(o, """
            SELECT encode(fingerprint, 'hex') FROM runwick.storage_commands
            WHERE application = @a AND operation = 'ReceiveOutbox' AND command_id = @c
            """, ("a", o.Destination.Application), ("c", row.TransportCommandId)));
    }

    sealed record SourceRow(Guid DestinationStoreId, string Application, byte[] Intent, byte[] BusinessDigest, byte[] PolicyDigest,
        byte[] DeliveryFingerprint, Guid ProposedJobId, long AcceptUntilMs, Guid TransportCommandId, long TransportNotAfterMs);

    static string Text(Guid value) => value.ToString("D", CultureInfo.InvariantCulture);

    static async Task<SourceRow> ReadRowAsync(OutboxFixture o, Guid messageId)
    {
        const string Columns = """
            destination_store_id, destination_application, intent, business_digest, policy_digest, delivery_fingerprint,
            proposed_job_id, accept_until, transport_command_id, transport_not_after
            """;
        if (o.Kind == SourceKind.Postgres)
        {
            await using var conn = new NpgsqlConnection(o.SourceConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand($"SELECT {Columns} FROM runwick_outbox.outbox_messages WHERE message_id = @m", conn);
            cmd.Parameters.AddWithValue("m", messageId);
            await using var r = await cmd.ExecuteReaderAsync();
            Assert.True(await r.ReadAsync());
            static long Ms(DateTime t) => new DateTimeOffset(DateTime.SpecifyKind(t, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
            return new SourceRow(r.GetGuid(0), r.GetString(1), (byte[])r[2], (byte[])r[3], (byte[])r[4], (byte[])r[5], r.GetGuid(6),
                Ms(r.GetDateTime(7)), r.GetGuid(8), Ms(r.GetDateTime(9)));
        }

        await using var sqlite = new SqliteConnection(o.SourceConnectionString);
        await sqlite.OpenAsync();
        await using var read = new SqliteCommand($"SELECT {Columns} FROM runwick_outbox_messages WHERE message_id = $m", sqlite);
        read.Parameters.AddWithValue("$m", Text(messageId));
        await using var s = await read.ExecuteReaderAsync();
        Assert.True(await s.ReadAsync());
        static Guid G(string v) => Guid.Parse(v, CultureInfo.InvariantCulture);
        return new SourceRow(G(s.GetString(0)), s.GetString(1), (byte[])s[2], (byte[])s[3], (byte[])s[4], (byte[])s[5], G(s.GetString(6)),
            s.GetInt64(7), G(s.GetString(8)), s.GetInt64(9));
    }

    static async Task<List<string>> HexAsync(OutboxFixture o, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new NpgsqlConnection(o.Destination.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            list.Add(r.GetString(0));
        }

        return list;
    }

    /// <summary>Runs the independent encoder. SER-11 requires it (02 §13), so a missing <c>node</c> fails rather than skips.</summary>
    static string RunEncoder(string input)
    {
        var root = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(root, "Runwick.slnx")))
        {
            root = Path.GetDirectoryName(root) ?? throw new InvalidOperationException("Runwick.slnx not found");
        }

        ProcessStartInfo start = new("node", ["--no-warnings", Path.Combine(root, "tests", "vectors", "tools", "canonical.mjs"), "--outbox-digests", input])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = root,
        };
        using var process = Process.Start(start) ?? throw new InvalidOperationException("node did not start");
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0, error);
        return output;
    }
}
