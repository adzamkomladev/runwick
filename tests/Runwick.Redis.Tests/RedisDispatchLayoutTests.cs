using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>
/// Perf plan F, dispatch script diet: the script writes the fields of a dispatch in bytewise order without sorting them at run time.
/// A small hash keeps its insertion order, so reading it back shows the order the script wrote. These tests pin it to the order the
/// old sorting gave, and pin the field sets, so a field that is dropped from the pre-ordered lists fails here. Layout 2 (2026-11-20)
/// changed the stored event from a row to a pack; the row EventsSince serves from it is pinned unchanged.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
[Collection(nameof(RedisServerState))]
public sealed class RedisDispatchLayoutTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly string[] JobFields =
    [
        "akey", "created", "eligible", "g:1:created", "g:1:max", "g:1:maxun", "g:1:reason", "g:1:retry", "g:1:started", "g:1:timeout",
        "g:1:unstarted", "g:1:uretry", "gen", "headers", "name", "penc", "priority", "profile", "profver", "pver", "queue", "rates", "rcount",
        "rev", "scopes", "state", "updated", "xcount",
    ];

    static readonly string[] ReceiptFields = ["cls", "code", "decided", "detail", "fp", "notafter", "op", "result", "retain", "status", "ver"];

    static string[] Bytewise(IEnumerable<string> names) => [.. names.OrderBy(n => n, StringComparer.Ordinal)];

    [Fact]
    public void The_pre_ordered_field_lists_are_bytewise_sorted_and_match_the_script_text()
    {
        Assert.Equal(Bytewise(JobFields), JobFields);
        Assert.Equal(Bytewise(ReceiptFields), ReceiptFields);
        Assert.Equal(JobFields.Length, JobFields.Distinct().Count());
        // The lists in the Lua sources are these lists: a name added to one place and not the other fails here.
        var jobs = ReadResource("Runwick.Redis.Lua.jobs.lua");
        var prelude = ReadResource("Runwick.Redis.Lua.prelude.lua");
        Assert.Equal(JobFields, ListIn(jobs, "U.JOB_FIELDS = {"));
        Assert.Equal(ReceiptFields, ListIn(prelude, "local RECEIPT_FIELDS = {"));
    }

    [Fact]
    public async Task A_dispatch_writes_the_job_receipt_and_event_in_bytewise_field_order()
    {
        await RedisServer.EnsureMigratedAsync();
        var application = "layout_" + Guid.NewGuid().ToString("N")[..8];
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application(application);
            TestJobs.Register(r);
            r.UseRedis(RedisServer.Configuration);
        });
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct);
        try
        {
            var prepared = provider.GetRequiredService<IPreparedJobDispatcher>().Prepare(new PingJob("layout"));
            var receipt = await provider.GetRequiredService<IPreparedJobDispatcher>().DispatchAsync(prepared, Ct);
            var command = prepared.OperationId.Value.ToString("N");
            var job = receipt.JobId.Value.ToString("N");

            await using var mux = await ConnectionMultiplexer.ConnectAsync(RedisServer.Configuration);
            var db = mux.GetDatabase();

            // Job hash: every field of a new job is present (akey and a timeout only when set) and nothing else. Redis keeps a hash with a
            // long value unordered, so the field order itself is pinned by the list test above, not read back here.
            var jobFields = (await db.HashKeysAsync(RedisServer.Key(application, "job:" + job))).Select(f => (string)f!).Order(StringComparer.Ordinal).ToArray();
            Assert.Equal(JobFields.Where(f => f is not ("akey" or "g:1:timeout")), jobFields.Where(f => f is not ("akey" or "g:1:timeout")));
            Assert.All(jobFields, f => Assert.Contains(f, JobFields));

            // Receipt hash: all eleven fields.
            var receiptFields = (await db.HashKeysAsync(RedisServer.Key(application, "rcpt:" + command))).Select(f => (string)f!).Order(StringComparer.Ordinal).ToArray();
            Assert.Equal(ReceiptFields, receiptFields);

            // Event (layout 2, 22 §3.2): the call's events are stored as one pack keyed by its first event id, with no null actor or
            // attempt written out; the dispatch's pack holds one entry. Expanded, as EventsSince serves it, it is the layout-1 row: keys in
            // bytewise order, one dispatched event attributed to the command.
            var stored = (await db.HashGetAllAsync(RedisServer.Key(application, "events:data"))).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
            var pack = Assert.Single(stored.Values, v => v.Contains("\ndispatched|", StringComparison.Ordinal));
            Assert.Matches(@"^~1\|\d+\|" + command + @"\ndispatched\|" + job + @"\|\|\|0\|\{""generation"":", pack);
            var rows = RedisEventRows.All(stored);
            var row = Assert.Single(rows.Select(r => JsonNode.Parse(r)!.AsObject()), o => o["kind"]!.GetValue<string>() == "dispatched");
            var keys = row.Select(p => p.Key).ToArray();
            Assert.Equal(Bytewise(keys), keys);
            Assert.Equal(["actorId", "attemptId", "commandId", "details", "eventId", "jobId", "kind", "occurredAt"], keys);
            Assert.Equal(command, row["commandId"]!.GetValue<string>());
            Assert.Equal(job, row["jobId"]!.GetValue<string>());
            var details = row["details"]!.AsObject().Select(p => p.Key).ToArray();
            Assert.Equal(Bytewise(details), details);

            // A resend replays the receipt and the replayed result is the same job.
            Assert.Equal(receipt.JobId, (await provider.GetRequiredService<IPreparedJobDispatcher>().DispatchAsync(prepared, Ct)).JobId);
        }
        finally
        {
            await RedisServer.DropApplicationAsync(application);
        }
    }

    static string ReadResource(string name)
    {
        using var stream = typeof(Runwick.Redis.Store.RedisStore).Assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException("missing resource " + name);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <summary>The quoted names of the Lua table literal that starts at <paramref name="start"/>, in order.</summary>
    static string[] ListIn(string source, string start)
    {
        var begin = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(begin >= 0, start);
        var end = source.IndexOf('}', begin);
        var body = source[(begin + start.Length)..end];
        return [.. System.Text.RegularExpressions.Regex.Matches(body, "'([^']+)'").Select(m => m.Groups[1].Value)];
    }
}
