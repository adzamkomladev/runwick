using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Runwick.Harness.Store;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Contract.Tests;

/// <summary>
/// Admin access to one Redis application's keys (22 §3.1) for the shared contract cases: counts for assertions and the setup
/// writes the SQL stores do with admin SQL. The shared cases keep their PostgreSQL-form SQL text; this class answers the same
/// questions from the keys, matching the (fixed) statement texts. Setup only; never used to fake protocol effects under test.
/// </summary>
internal static partial class RedisAdmin
{
    static async Task<IDatabase> Db() => (await RedisServer.AdminAsync()).GetDatabase();

    static string P(string application) => RxClient.KeyPrefix(application);

    static string Hex(object g) => g is Guid id ? id.ToString("N") : (string)g;

    static async Task<List<string>> Keys(string application, string pattern)
    {
        var mux = await RedisServer.AdminAsync();
        var list = new List<string>();
        await foreach (var k in mux.GetServer(mux.GetEndPoints()[0]).KeysAsync(mux.GetDatabase().Database, P(application) + pattern, 1000))
        {
            list.Add(((string)k!)[P(application).Length..]);
        }

        return list;
    }

    public static async Task<Dictionary<string, string>> Hash(string application, string key) =>
        (await (await Db()).HashGetAllAsync(P(application) + key)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);

    static async Task<List<JsonObject>> Json(string application, string key) =>
        [.. (await Hash(application, key)).Values.Select(v => JsonNode.Parse(v)!.AsObject())];

    public static async Task<bool> Exists(string application, string key) => await (await Db()).KeyExistsAsync(P(application) + key);

    /// <summary>Adds a required capability to the application gate (setup; the SQL stores update application_controls).</summary>
    public static async Task Activate(string application, string capability)
    {
        var db = await Db();
        var meta = P(application) + "meta";
        var caps = JsonNode.Parse((string?)await db.HashGetAsync(meta, "caps") ?? "{}")!.AsObject();
        caps[capability] = 1;
        var rev = long.Parse((string?)await db.HashGetAsync(meta, "gaterev") ?? "1", CultureInfo.InvariantCulture) + 1;
        await db.HashSetAsync(meta, [new HashEntry("caps", caps.ToJsonString()), new HashEntry("gaterev", rev)]);
    }

    // ------------------------------------------------------------------ schedules

    static async Task<List<(string Id, JsonObject Row)>> Schedules(string application) =>
        [.. (await Hash(application, "sched")).Where(p => p.Key.Length == 32).Select(p => (p.Key, JsonNode.Parse(p.Value)!.AsObject()))];

    static async Task SaveSchedule(string application, string id, JsonObject row)
    {
        var db = await Db();
        await db.HashSetAsync(P(application) + "sched", id, row.ToJsonString());
        var due = row["enabled"]!.GetValue<bool>() && !row["paused"]!.GetValue<bool>() && !row["review"]!.GetValue<bool>() && row["next"] is not null;
        if (due)
        {
            await db.SortedSetAddAsync(P(application) + "sched:due", id, row["scan"]!.GetValue<long>());
        }
        else
        {
            await db.SortedSetRemoveAsync(P(application) + "sched:due", id);
        }
    }

    public static async Task Rewind(string application, string key, long through, long next, long scan)
    {
        var id = (string)(await (await Db()).HashGetAsync(P(application) + "sched", "k:" + key))!;
        var row = JsonNode.Parse((await Hash(application, "sched"))[id])!.AsObject();
        row["through"] = through;
        row["next"] = next;
        row["scan"] = scan;
        await SaveSchedule(application, id, row);
    }

    public static async Task SetEpisode(string application, Guid episode, long cutoff)
    {
        foreach (var (id, row) in await Schedules(application))
        {
            row["ep"] = new JsonObject { ["id"] = Hex(episode), ["cutoff"] = cutoff, ["emitted"] = 0, ["limit"] = 10 };
            await SaveSchedule(application, id, row);
        }
    }

    public static async Task SetMaxEpisodeJobs(string application, long limit)
    {
        var db = await Db();
        foreach (var (id, row) in await Schedules(application))
        {
            var field = "d:" + id + ":" + row["drev"]!.GetValue<long>();
            var def = JsonNode.Parse((await Hash(application, "sched"))[field])!.AsObject();
            def["maxEpisodeJobs"] = limit;
            await db.HashSetAsync(P(application) + "sched", field, def.ToJsonString());
        }
    }

    public static async Task DeleteOccurrences(string application)
    {
        var db = await Db();
        foreach (var k in await Keys(application, "occ:*"))
        {
            await db.KeyDeleteAsync(P(application) + k);
        }
    }

    static async Task<List<(string Field, JsonObject Row)>> Occurrences(string application) =>
        [.. (await Task.WhenAll((await Keys(application, "occ:*")).Where(k => k != "occ:idx").Select(async k => (await Hash(application, k)).Select(p => (p.Key, JsonNode.Parse(p.Value)!.AsObject())))))
            .SelectMany(x => x)];

    static async Task<List<JsonObject>> Rows(string application, string key) => await Json(application, key);

    public static async Task<long> CountAsync(DriverFixture f, string sql, (string Name, object Value)[] p)
    {
        var app = f.Application;
        string? Param(string name) => p.Where(x => x.Name == name).Select(x => x.Value switch { Guid g => g.ToString("N"), var v => Convert.ToString(v, CultureInfo.InvariantCulture) }).FirstOrDefault();
        var s = Regex.Replace(sql, @"\s+", " ").Trim();
        if (await RetentionCount(app, s, Param) is { } retention)
        {
            return retention;
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.jobs WHERE application = @a", StringComparison.Ordinal))
        {
            return await (await Db()).SortedSetLengthAsync(P(Param("a")!) + "idx:created");
        }

        if (s == "SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_kind = @kind")
        {
            return (await Occurrences(app)).Count(o => o.Field.StartsWith(Param("kind") + ":", StringComparison.Ordinal));
        }

        if (s == "SELECT count(*) FROM runwick.schedules")
        {
            return (await Schedules(app)).Count;
        }

        if (s == "SELECT count(*) FROM runwick.schedule_definitions")
        {
            return (await Hash(app, "sched")).Keys.Count(k => k.StartsWith("d:", StringComparison.Ordinal));
        }

        if (Regex.Match(s, @"FROM runwick.queue_events WHERE event_kind = '(\w+)'") is { Success: true } ev)
        {
            return (await Json(app, "events:data")).Count(e => e["kind"]!.GetValue<string>() == ev.Groups[1].Value);
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", StringComparison.Ordinal))
        {
            return await Exists(app, "rcpt:" + Param("c")) ? 1 : 0;
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_key = @k", StringComparison.Ordinal))
        {
            return (await Occurrences(app)).Count(o => o.Field == "scheduled:" + Param("k") && o.Row["through"]?.GetValue<long>() == long.Parse(Param("t")!, CultureInfo.InvariantCulture));
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.schedule_audit_ranges", StringComparison.Ordinal))
        {
            var reason = Regex.Match(s, @"reason = '(\w+)'");
            return (await Hash(app, "sched")).Where(x => x.Key.StartsWith("ar:", StringComparison.Ordinal))
                .Count(x => !reason.Success || JsonNode.Parse(x.Value)!["reason"]!.GetValue<string>() == reason.Groups[1].Value);
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.management_audit WHERE operation", StringComparison.Ordinal))
        {
            var ops = Regex.Matches(s, @"'(\w+)'").Select(m => m.Groups[1].Value).ToHashSet();
            var target = Regex.Match(s, @"target_id = '(\w+)'");
            var resulting = Regex.Match(s, @"resulting_revision = (\d+)");
            return (await Json(app, "audit:data")).Count(a => ops.Contains(a["action"]!.GetValue<string>())
                && (!target.Success || a["targetId"]?.GetValue<string>() == target.Groups[1].Value)
                && (!resulting.Success || a["resultingRevision"]?.GetValue<long>() == long.Parse(resulting.Groups[1].Value, CultureInfo.InvariantCulture)));
        }

        if (s.Contains("s.scope_kind = 'schedule'", StringComparison.Ordinal))
        {
            var sid = (string?)await (await Db()).HashGetAsync(P(app) + "scopes", "k:schedule\0" + Param("id"));
            if (sid is null || JsonNode.Parse((await Hash(app, "scopes"))[sid])!["capacity"]!.GetValue<int>() != 1)
            {
                return 0;
            }

            var n = 0;
            foreach (var k in await Keys(app, "job:*"))
            {
                n += (string?)await (await Db()).HashGetAsync(P(app) + k, "scopes") is { } list && list.Split(',').Contains(sid) ? 1 : 0;
            }

            return n;
        }

        if (s.Contains("j.job_name, j.payload_version", StringComparison.Ordinal))
        {
            var bad = 0;
            var schedules = await Schedules(app);
            var sched = await Hash(app, "sched");
            foreach (var (field, row) in await Occurrences(app))
            {
                var j = await Hash(app, "job:" + row["job"]!.GetValue<string>());
                var owner = schedules.First(x => x.Row["drev"]!.GetValue<long>() >= 1);
                var def = JsonNode.Parse(sched["d:" + owner.Id + ":" + owner.Row["drev"]!.GetValue<long>()])!["template"]!;
                if (j["name"] != def["jobName"]!.GetValue<string>() || j["pver"] != def["payloadVersion"]!.ToString() || j["profile"] != def["profileId"]!.GetValue<string>()
                    || j["profver"] != def["profileVersion"]!.ToString())
                {
                    bad++;
                }
            }

            return bad;
        }

        if (s.StartsWith("SELECT count(*) FROM runwick.job_generations WHERE created_reason = 'scheduled'", StringComparison.Ordinal))
        {
            var n = 0;
            foreach (var k in await Keys(app, "job:*"))
            {
                n += (string?)await (await Db()).HashGetAsync(P(app) + k, "g:1:reason") == "scheduled" ? 1 : 0;
            }

            return n;
        }

        throw new NotSupportedException("No Redis form for: " + s);
    }

    public static async Task ExecAsync(DriverFixture f, string sql, (string Name, object Value)[] p)
    {
        var app = f.Application;
        T Param<T>(string name) => (T)p.First(x => x.Name == name).Value;
        var s = Regex.Replace(sql, @"\s+", " ").Trim();
        if (await RetentionExec(app, s, p))
        {
            return;
        }

        if (s.StartsWith("UPDATE runwick.schedules SET evaluated_through", StringComparison.Ordinal))
        {
            await Rewind(app, Param<string>("key"), Param<long>("through"), Param<long>("next"), Param<long>("scan"));
        }
        else if (s == "DELETE FROM runwick.schedule_occurrences")
        {
            await DeleteOccurrences(app);
        }
        else if (s.StartsWith("UPDATE runwick.schedule_definitions SET max_episode_jobs", StringComparison.Ordinal))
        {
            await SetMaxEpisodeJobs(app, Convert.ToInt64(Param<object>("limit"), CultureInfo.InvariantCulture));
        }
        else if (s.StartsWith("UPDATE runwick.schedules SET episode_id", StringComparison.Ordinal))
        {
            await SetEpisode(app, Param<Guid>("e"), Param<long>("t"));
        }
        else
        {
            throw new NotSupportedException("No Redis form for: " + s);
        }
    }
}
