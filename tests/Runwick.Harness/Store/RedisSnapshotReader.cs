using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Harness.Store;

/// <summary>
/// The Redis form of <see cref="SnapshotReader"/> (22 §11): every key of one application, normalized into one "table" per key
/// (sorted <c>field=value</c> rows), plus the ReferenceModel view built from the job, attempt, permit, key and receipt records.
/// Reads are not one atomic step; callers snapshot a quiescent store.
/// </summary>
internal static class RedisSnapshotReader
{
    static Guid G(string hex) => Guid.Parse(hex);

    static Guid? Gn(string? hex) => string.IsNullOrEmpty(hex) ? null : Guid.Parse(hex);

    static DateTimeOffset T(string ms) => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(ms, CultureInfo.InvariantCulture));

    static DateTimeOffset? Tn(string? ms) => string.IsNullOrEmpty(ms) ? null : T(ms);

    static int I(string? s) => int.Parse(s ?? "0", CultureInfo.InvariantCulture);

    public static async Task<HarnessSnapshot> ReadAsync(IConnectionMultiplexer mux, string application)
    {
        var db = mux.GetDatabase();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var prefix = RxClient.KeyPrefix(application);
        var readAt = DateTimeOffset.UtcNow;
        var keys = new List<string>();
        await foreach (var k in server.KeysAsync(db.Database, prefix + "*", 1000))
        {
            keys.Add(((string)k!)[prefix.Length..]);
        }

        keys.Sort(StringComparer.Ordinal);
        var tables = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        var hashes = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            var full = prefix + key;
            switch (await db.KeyTypeAsync(full))
            {
                case RedisType.Hash:
                    var h = (await db.HashGetAllAsync(full)).ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);
                    hashes[key] = h;
                    tables[key] = [.. h.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Key + "=" + p.Value)];
                    break;
                case RedisType.SortedSet:
                    tables[key] = [.. (await db.SortedSetRangeByRankWithScoresAsync(full)).Select(e => (string)e.Element! + "=" + e.Score.ToString(CultureInfo.InvariantCulture))];
                    break;
                case RedisType.String:
                    var bytes = (byte[])(await db.StringGetAsync(full))!;
                    tables[key] = [$"len:{bytes.Length},sha256:{Convert.ToHexStringLower(SHA256.HashData(bytes))}"];
                    break;
            }
        }

        var jobs = new List<JobRow>();
        var attempts = new List<AttemptRow>();
        foreach (var (key, j) in hashes.Where(p => p.Key.StartsWith("job:", StringComparison.Ordinal)).OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var id = G(key[4..]);
            var gen = j["gen"];
            jobs.Add(new JobRow(id, j["state"], I(gen), long.Parse(j["rev"], CultureInfo.InvariantCulture), I(j.GetValueOrDefault($"g:{gen}:started")),
                long.Parse(j.GetValueOrDefault("xcount", "0"), CultureInfo.InvariantCulture), I(j.GetValueOrDefault($"g:{gen}:unstarted")),
                I(j[$"g:{gen}:max"]), I(j[$"g:{gen}:maxun"]), Gn(j.GetValueOrDefault("attempt")), Gn(j.GetValueOrDefault("token")),
                Gn(j.GetValueOrDefault("worker")), Tn(j.GetValueOrDefault("lease")), !string.IsNullOrEmpty(j.GetValueOrDefault("cancelat")), T(j["eligible"]),
                j["queue"], j.GetValueOrDefault("reason") is { Length: > 0 } r ? r : null));
            foreach (var a in j.Keys.Where(f => f.StartsWith("a:", StringComparison.Ordinal) && f.EndsWith(":res", StringComparison.Ordinal)))
            {
                var aid = a[2..^4];
                string? F(string f) => j.GetValueOrDefault($"a:{aid}:{f}") is { Length: > 0 } v ? v : null;
                attempts.Add(new AttemptRow(G(aid), id, I(F("gen")), long.Parse(F("res")!, CultureInfo.InvariantCulture),
                    F("exec") is { } e ? I(e) : null, F("outcome"), T(F("reserved")!), Tn(F("auth")), Tn(F("fin"))));
            }
        }

        attempts = [.. attempts.OrderBy(a => a.JobId).ThenBy(a => a.ReservationNumber)];
        var permits = hashes.GetValueOrDefault("permits", []).Select(p =>
        {
            var o = JsonNode.Parse(p.Value)!;
            var parts = p.Key.Split(':');
            return new PermitRow(G(parts[0]), G(parts[1]), G(o["attempt"]!.GetValue<string>()), DateTimeOffset.FromUnixTimeMilliseconds(o["lease"]!.GetValue<long>()));
        }).OrderBy(p => p.ScopeId).ThenBy(p => p.AttemptId).ToList();
        var dispatchKeys = new List<DispatchKeyRow>();
        foreach (var (f, job) in hashes.GetValueOrDefault("keys:active", []))
        {
            dispatchKeys.Add(new DispatchKeyRow("active", f[(f.IndexOf('\0', StringComparison.Ordinal) + 1)..], G(job), true, null));
        }

        foreach (var (f, json) in hashes.GetValueOrDefault("keys:window", []))
        {
            var o = JsonNode.Parse(json)!;
            dispatchKeys.Add(new DispatchKeyRow("window", f[(f.IndexOf('\0', StringComparison.Ordinal) + 1)..], G(o["job"]!.GetValue<string>()), false,
                DateTimeOffset.FromUnixTimeMilliseconds(o["exp"]!.GetValue<long>())));
        }

        var receipts = hashes.Where(p => p.Key.StartsWith("rcpt:", StringComparison.Ordinal) && !p.Key.StartsWith("rcpt:exp:", StringComparison.Ordinal))
            .Select(p => new ReceiptRow(G(p.Key[5..]), p.Value.GetValueOrDefault("op", ""), p.Value.GetValueOrDefault("cls", ""),
                p.Value.GetValueOrDefault("result") is { Length: > 0 }))
            .OrderBy(r => r.RetentionClass, StringComparer.Ordinal).ThenBy(r => r.CommandId).ToList();
        return new HarnessSnapshot(new StoreSnapshot(readAt, jobs, attempts, permits, dispatchKeys, receipts), tables);
    }
}
