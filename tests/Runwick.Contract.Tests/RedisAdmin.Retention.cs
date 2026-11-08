using System.Globalization;
using System.Text.Json.Nodes;
using StackExchange.Redis;

namespace Runwick.Contract.Tests;

internal static partial class RedisAdmin
{
    // ------------------------------------------------------------------ retention setup and assertions (22 §7.3)

    static async Task<List<string>> JobKeys(string application) => [.. (await Keys(application, "job:*"))];

    public static async Task AgeJobs(string application, long ms)
    {
        var db = await Db();
        foreach (var k in await JobKeys(application))
        {
            if ((string?)await db.HashGetAsync(P(application) + k, "terminalat") is { Length: > 0 } t)
            {
                await db.HashSetAsync(P(application) + k, "terminalat", long.Parse(t, CultureInfo.InvariantCulture) - ms);
            }
        }
    }

    public static async Task AgeCreated(string application, Guid job, long ms)
    {
        var db = await Db();
        var key = P(application) + "job:" + job.ToString("N");
        await db.HashSetAsync(key, "created", long.Parse((string)(await db.HashGetAsync(key, "created"))!, CultureInfo.InvariantCulture) - ms);
    }

    public static async Task<long> JobCount(string application) => (await JobKeys(application)).Count;

    public static async Task<string?> JobField(string application, Guid job, string field) =>
        (string?)await (await Db()).HashGetAsync(P(application) + "job:" + job.ToString("N"), field);

    /// <summary>Moves a receipt's end (and its deadline with it) to <paramref name="end"/> ms, as the SQL stores' UPDATE does.</summary>
    public static async Task EndReceipt(string application, Guid command, long end)
    {
        var db = await Db();
        var key = P(application) + "rcpt:" + command.ToString("N");
        var retain = long.Parse((string)(await db.HashGetAsync(key, "retain"))!, CultureInfo.InvariantCulture);
        var notAfter = long.Parse((string)(await db.HashGetAsync(key, "notafter"))!, CultureInfo.InvariantCulture);
        var cls = (string)(await db.HashGetAsync(key, "cls"))!;
        await db.HashSetAsync(key, [new HashEntry("notafter", notAfter - (retain - end)), new HashEntry("retain", end)]);
        await db.SortedSetAddAsync(P(application) + "rcpt:exp:" + cls, command.ToString("N"), end);
    }

    public static async Task AgeAllReceipts(string application, long ms)
    {
        var db = await Db();
        foreach (var k in await Keys(application, "rcpt:*"))
        {
            if (k.StartsWith("rcpt:exp:", StringComparison.Ordinal))
            {
                continue;
            }

            var h = await Hash(application, k);
            await db.HashSetAsync(P(application) + k, [new HashEntry("notafter", long.Parse(h["notafter"], CultureInfo.InvariantCulture) - ms),
                new HashEntry("retain", long.Parse(h["retain"], CultureInfo.InvariantCulture) - ms)]);
            await db.SortedSetAddAsync(P(application) + "rcpt:exp:" + h["cls"], k["rcpt:".Length..], long.Parse(h["retain"], CultureInfo.InvariantCulture) - ms);
        }
    }

    public static async Task<long> Receipts(string application, Guid command) => await Exists(application, "rcpt:" + command.ToString("N")) ? 1 : 0;

    static async Task<List<(string Field, JsonObject Row)>> Windows(string application, string? key = null) =>
        [.. (await Hash(application, "keys:window")).Where(p => key is null || p.Key.EndsWith("\0" + key, StringComparison.Ordinal))
            .Select(p => (p.Key, JsonNode.Parse(p.Value)!.AsObject()))];

    public static async Task ExpireWindows(string application, long at, string? key = null)
    {
        var db = await Db();
        foreach (var (field, row) in await Windows(application, key))
        {
            row["exp"] = at;
            await db.HashSetAsync(P(application) + "keys:window", field, row.ToJsonString());
            await db.SortedSetAddAsync(P(application) + "keys:window:exp", field, at);
        }
    }

    public static async Task<long> WindowRows(string application, string key, Guid? boundTo = null) =>
        (await Windows(application, key)).Count(w => boundTo is null || w.Row["job"]!.GetValue<string>() == boundTo.Value.ToString("N"));

    public static async Task<long> DispatchKeys(string application) =>
        (await Hash(application, "keys:window")).Count + (await Hash(application, "keys:active")).Count;

    /// <summary>Adds a live permit for each scope of the job (the SQL stores insert permit rows from the job's requirements).</summary>
    public static async Task AddPermits(string application, Guid job)
    {
        var db = await Db();
        var scopes = await JobField(application, job, "scopes") ?? "";
        foreach (var sid in scopes.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            await db.HashSetAsync(P(application) + "permits", sid + ":" + job.ToString("N"), "{}");
        }
    }

    public static async Task ClearPermits(string application) => await (await Db()).KeyDeleteAsync(P(application) + "permits");

    /// <summary>Puts every progress row's summary day one day back, with the given counts (CLN-02).</summary>
    public static async Task BackdateProgress(string application, long rows, int batches, int failures)
    {
        var db = await Db();
        foreach (var (field, json) in await Hash(application, "retention"))
        {
            if (!field.StartsWith("p:", StringComparison.Ordinal))
            {
                continue;
            }

            var p = JsonNode.Parse(json)!.AsObject();
            p["day"] = p["day"]!.GetValue<long>() - 1;
            p["dr"] = rows;
            p["db"] = batches;
            p["df"] = failures;
            await db.HashSetAsync(P(application) + "retention", field, p.ToJsonString());
        }
    }

    static Guid G(string? hex) => Guid.Parse(hex!);

    /// <summary>The retention cases' count statements (PostgreSQL form) answered from the keys; null when the statement is not one of them.</summary>
    static async Task<long?> RetentionCount(string app, string s, Func<string, string?> param)
    {
        if (s == "SELECT count(*) FROM runwick.jobs")
        {
            return await JobCount(app);
        }

        if (s == "SELECT count(*) FROM runwick.jobs WHERE job_id = @j")
        {
            return await Exists(app, "job:" + param("j")) ? 1 : 0;
        }

        if (s == "SELECT count(*) FROM runwick.job_payloads WHERE job_id = @j")
        {
            return await Exists(app, "pay:" + param("j")) ? 1 : 0;
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND detail_expired_at", StringComparison.Ordinal))
        {
            return await JobField(app, G(param("j")), "detailexp") is not null && !await Exists(app, "pay:" + param("j")) ? 1 : 0;
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", StringComparison.Ordinal))
        {
            return await JobField(app, G(param("j")), "state") == "succeeded" ? 1 : 0;
        }

        if (s == "SELECT count(*) FROM runwick.dispatch_keys")
        {
            return await DispatchKeys(app);
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.dispatch_keys WHERE key_kind = 'window' AND key = @k", StringComparison.Ordinal))
        {
            return await WindowRows(app, param("k")!, param("j") is { } j ? G(j) : null);
        }

        return null;
    }

    static async Task<bool> RetentionExec(string app, string s, (string Name, object Value)[] p)
    {
        T Param<T>(string name) => (T)p.First(x => x.Name == name).Value;
        if (s.StartsWith("UPDATE runwick.jobs SET terminal_at = terminal_at - @by", StringComparison.Ordinal))
        {
            await AgeJobs(app, (long)Param<TimeSpan>("by").TotalMilliseconds);
        }
        else if (s.StartsWith("UPDATE runwick.jobs SET created_at = created_at - interval '200 days'", StringComparison.Ordinal))
        {
            await AgeCreated(app, Param<Guid>("j"), (long)TimeSpan.FromDays(200).TotalMilliseconds);
        }
        else if (s.StartsWith("UPDATE runwick.storage_commands SET not_after = not_after - (retain_until - @t)", StringComparison.Ordinal))
        {
            await EndReceipt(app, Param<Guid>("gone"), Param<DateTimeOffset>("t").ToUnixTimeMilliseconds());
        }
        else if (s.StartsWith("UPDATE runwick.storage_commands SET not_after = not_after - interval '400 days'", StringComparison.Ordinal))
        {
            await AgeAllReceipts(app, (long)TimeSpan.FromDays(400).TotalMilliseconds);
        }
        else if (s.StartsWith("UPDATE runwick.dispatch_keys SET expires_at = @t WHERE key_kind = 'window'", StringComparison.Ordinal))
        {
            await ExpireWindows(app, Param<DateTimeOffset>("t").ToUnixTimeMilliseconds(), p.Any(x => x.Name == "k") ? Param<string>("k") : null);
        }
        else if (s.StartsWith("INSERT INTO runwick.concurrency_permits", StringComparison.Ordinal))
        {
            await AddPermits(app, Param<Guid>("j"));
        }
        else if (s == "DELETE FROM runwick.concurrency_permits")
        {
            await ClearPermits(app);
        }
        else if (s.StartsWith("UPDATE runwick.cleanup_progress SET summary_day", StringComparison.Ordinal))
        {
            await BackdateProgress(app, 5, 2, 1);
        }
        else
        {
            return false;
        }

        return true;
    }
}
