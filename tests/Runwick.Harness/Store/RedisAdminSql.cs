using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Harness.Store;

/// <summary>
/// The Redis form of the contract tests' admin SQL (<see cref="DriverFixture.CountAsync"/> / <see cref="DriverFixture.ExecAsync"/>):
/// the PostgreSQL-shaped statements the shared cases use, answered from the application's keys. Only the shapes the cases use
/// are understood: <c>SELECT count(*)|column FROM runwick.table [WHERE a = b AND c IS NULL AND d IN (...) AND details->>'k' = v]</c>,
/// plus the three setup statements that stand in for retention (a detail removal). Unknown shapes throw, so a new case cannot
/// silently pass.
/// </summary>
internal static partial class RedisAdminSql
{
    static string Hex(string g) => Guid.Parse(g).ToString("D");

    public static async Task<long> CountAsync(string application, string sql, (string Name, object Value)[] parameters)
    {
        var m = SelectShape().Match(Normalize(sql));
        if (!m.Success)
        {
            throw new NotSupportedException("Redis admin SQL not supported: " + sql);
        }

        var rows = await RowsAsync(application, m.Groups[2].Value).ConfigureAwait(false);
        var kept = rows.Where(r => Conditions(m.Groups[3].Value).All(c => c(r, parameters))).ToList();
        if (m.Groups[1].Value == "count(*)")
        {
            return kept.Count;
        }

        return long.Parse(kept.Single()[m.Groups[1].Value] ?? throw new InvalidOperationException("NULL column"), CultureInfo.InvariantCulture);
    }

    public static async Task ExecAsync(string application, string sql, (string Name, object Value)[] parameters)
    {
        var mux = await RedisServer.AdminAsync().ConfigureAwait(false);
        var db = mux.GetDatabase();
        var prefix = RxClient.KeyPrefix(application);
        var job = Guid.Parse(parameters.Single(p => p.Name == "j").Value.ToString()!).ToString("N");
        switch (Normalize(sql))
        {
            case "UPDATE runwick.jobs SET payload_retained = false WHERE job_id = @j":
                break; // the payload row's removal below is the retained flag
            case "DELETE FROM runwick.job_payloads WHERE job_id = @j":
                await db.KeyDeleteAsync(prefix + "pay:" + job).ConfigureAwait(false);
                break;
            case "UPDATE runwick.jobs SET detail_expired_at = terminal_at WHERE job_id = @j":
                var at = await db.HashGetAsync(prefix + "job:" + job, "terminalat").ConfigureAwait(false);
                await db.HashSetAsync(prefix + "job:" + job, "detailexp", at).ConfigureAwait(false);
                await db.KeyDeleteAsync(prefix + "pay:" + job).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException("Redis admin SQL not supported: " + sql);
        }
    }

    static string Normalize(string sql) => Spaces().Replace(sql.Trim(), " ");

    [GeneratedRegex(@"^SELECT (count\(\*\)|\w+) FROM runwick\.(\w+)(?: WHERE (.*))?$")]
    private static partial Regex SelectShape();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();

    static IEnumerable<Func<Dictionary<string, string?>, (string Name, object Value)[], bool>> Conditions(string where)
    {
        if (where.Length == 0)
        {
            yield break;
        }

        foreach (var part in where.Split(" AND "))
        {
            var c = part.Trim();
            var detail = Regex.Match(c, @"^details->>'(\w+)' = (.+)$");
            var isNull = Regex.Match(c, @"^(\w+) IS NULL$");
            var not = Regex.Match(c, @"^NOT (\w+)$");
            var inList = Regex.Match(c, @"^(\w+) IN \((.*)\)$");
            var eq = Regex.Match(c, @"^(\w+) = (.+)$");
            if (detail.Success)
            {
                yield return (r, p) => r["details"] is { } d && JsonNode.Parse(d)![detail.Groups[1].Value]?.ToString() == Literal(detail.Groups[2].Value, p);
            }
            else if (isNull.Success)
            {
                yield return (r, _) => r.GetValueOrDefault(isNull.Groups[1].Value) is null;
            }
            else if (not.Success)
            {
                yield return (r, _) => !string.Equals(r.GetValueOrDefault(not.Groups[1].Value), "true", StringComparison.OrdinalIgnoreCase);
            }
            else if (inList.Success)
            {
                var set = inList.Groups[2].Value.Split(',').Select(x => x.Trim().Trim('\'')).ToHashSet(StringComparer.Ordinal);
                yield return (r, _) => r.GetValueOrDefault(inList.Groups[1].Value) is { } v && set.Contains(v);
            }
            else if (eq.Success)
            {
                yield return (r, p) => string.Equals(r.GetValueOrDefault(eq.Groups[1].Value), Literal(eq.Groups[2].Value, p), StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                throw new NotSupportedException("Redis admin SQL condition not supported: " + c);
            }
        }
    }

    static string Literal(string text, (string Name, object Value)[] parameters)
    {
        if (text.StartsWith('@'))
        {
            var v = parameters.Single(p => p.Name == text[1..]).Value;
            return v is Guid g ? g.ToString("D") : Convert.ToString(v, CultureInfo.InvariantCulture)!;
        }

        return text.Trim('\'');
    }

    static async Task<List<Dictionary<string, string?>>> RowsAsync(string application, string table)
    {
        var mux = await RedisServer.AdminAsync().ConfigureAwait(false);
        var db = mux.GetDatabase();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var prefix = RxClient.KeyPrefix(application);
        async Task<List<string>> Keys(string pattern)
        {
            var keys = new List<string>();
            await foreach (var k in server.KeysAsync(db.Database, prefix + pattern, 1000).ConfigureAwait(false))
            {
                keys.Add(((string)k!)[prefix.Length..]);
            }

            return keys;
        }

        async Task<Dictionary<string, string>> Hash(string key) =>
            (await db.HashGetAllAsync(prefix + key).ConfigureAwait(false)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);

        async Task<List<Dictionary<string, string>>> Jobs()
        {
            var jobs = new List<Dictionary<string, string>>();
            foreach (var k in await Keys("job:*").ConfigureAwait(false))
            {
                var h = await Hash(k).ConfigureAwait(false);
                h["id"] = k[4..];
                jobs.Add(h);
            }

            return jobs;
        }

        var rows = new List<Dictionary<string, string?>>();
        switch (table)
        {
            case "jobs":
                foreach (var j in await Jobs().ConfigureAwait(false))
                {
                    rows.Add(new() { ["job_id"] = Hex(j["id"]), ["cancel_requested_at"] = j.GetValueOrDefault("cancelat"), ["execution_count"] = j.GetValueOrDefault("xcount"),
                        ["state"] = j["state"], ["queue"] = j["queue"] });
                }

                break;
            case "job_generations":
                foreach (var j in await Jobs().ConfigureAwait(false))
                {
                    foreach (var f in j.Keys.Where(f => f.StartsWith("g:", StringComparison.Ordinal) && f.EndsWith(":created", StringComparison.Ordinal)))
                    {
                        var n = f.Split(':')[1];
                        rows.Add(new() { ["job_id"] = Hex(j["id"]), ["generation"] = n, ["created_reason"] = j.GetValueOrDefault($"g:{n}:reason"),
                            ["started_count"] = j.GetValueOrDefault($"g:{n}:started") });
                    }
                }

                break;
            case "job_concurrency_requirements":
                foreach (var j in await Jobs().ConfigureAwait(false))
                {
                    foreach (var s in (j.GetValueOrDefault("scopes") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries))
                    {
                        rows.Add(new() { ["job_id"] = Hex(j["id"]), ["scope_id"] = s });
                    }
                }

                break;
            case "dispatch_keys":
                var active = await Hash("keys:active").ConfigureAwait(false);
                foreach (var j in (await Jobs().ConfigureAwait(false)).Where(j => j.ContainsKey("akey")))
                {
                    rows.Add(new() { ["key"] = j["akey"], ["bound_job_id"] = active.TryGetValue(j["name"] + "\0" + j["akey"], out var b) ? Hex(b) : null });
                }

                rows = [.. rows.DistinctBy(r => r["key"])];
                break;
            case "concurrency_permits":
                foreach (var f in (await Hash("permits").ConfigureAwait(false)).Keys)
                {
                    rows.Add(new() { ["scope_id"] = f.Split(':')[0], ["job_id"] = Hex(f.Split(':')[1]) });
                }

                break;
            case "queue_controls":
                foreach (var (q, json) in await Hash("queues").ConfigureAwait(false))
                {
                    var o = JsonNode.Parse(json)!;
                    rows.Add(new() { ["queue"] = q, ["revision"] = o["revision"]!.ToString(), ["updated_by"] = o["updatedBy"]?.ToString(), ["paused"] = o["paused"]?.ToString() });
                }

                break;
            case "concurrency_scopes":
                var scopes = await Hash("scopes").ConfigureAwait(false);
                foreach (var (id, json) in scopes.Where(p => !p.Key.StartsWith("k:", StringComparison.Ordinal)))
                {
                    var o = JsonNode.Parse(json)!;
                    rows.Add(new() { ["scope_id"] = id, ["scope_kind"] = o["kind"]!.ToString(), ["scope_key"] = o["key"]!.ToString(),
                        ["policy_revision"] = o["prev"]!.ToString(), ["capacity"] = o["capacity"]!.ToString() });
                }

                break;
            case "storage_commands":
                foreach (var k in (await Keys("rcpt:*").ConfigureAwait(false)).Where(k => !k.StartsWith("rcpt:exp:", StringComparison.Ordinal)))
                {
                    var h = await Hash(k).ConfigureAwait(false);
                    rows.Add(new() { ["command_id"] = Hex(k[5..]), ["operation"] = h.GetValueOrDefault("op") });
                }

                break;
            case "management_audit":
                foreach (var json in (await Hash("audit:data").ConfigureAwait(false)).Values)
                {
                    var o = JsonNode.Parse(json)!;
                    rows.Add(new() { ["command_id"] = Hex(o["commandId"]!.ToString()), ["operation"] = o["action"]!.ToString(), ["target_kind"] = o["targetKind"]!.ToString(),
                        ["target_id"] = o["targetId"]!.ToString(), ["reason"] = o["reason"]?.ToString(), ["details"] = o["details"]!.ToJsonString(),
                        ["actor_subject"] = o["subject"]?.ToString(), ["actor_scheme"] = o["scheme"]?.ToString() });
                }

                break;
            case "worker_instances":
                foreach (var id in (await Hash("workers").ConfigureAwait(false)).Keys.Where(k => k.Length == 32))
                {
                    rows.Add(new() { ["instance_id"] = Hex(id) });
                }

                break;
            case "queue_events":
                foreach (var json in (await Hash("events:data").ConfigureAwait(false)).Values)
                {
                    var o = JsonNode.Parse(json)!;
                    rows.Add(new() { ["event_kind"] = o["kind"]!.ToString(), ["actor_id"] = o["actorId"]?.ToString(), ["command_id"] = o["commandId"] is { } c ? Hex(c.ToString()) : null,
                        ["job_id"] = o["jobId"] is { } jb ? Hex(jb.ToString()) : null });
                }

                break;
            default:
                throw new NotSupportedException("Redis admin SQL table not supported: " + table);
        }

        return rows;
    }
}
