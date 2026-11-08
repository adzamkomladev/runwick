using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using StackExchange.Redis;

namespace Runwick.Redis.Prototype;

/// <summary>A store command with a stable ID, fingerprinted arguments and an absolute NotAfter (epoch ms).</summary>
public sealed record StoreCommand(string Op, Guid Id, long NotAfter, string[] Args)
{
    public string Fingerprint => Convert.ToHexStringLower(
        SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\u001f', [Op, .. Args]))));

    public static StoreCommand Create(string op, TimeSpan lifetime, params string[] args) =>
        new(op, Guid.CreateVersion7(), DateTimeOffset.UtcNow.Add(lifetime).ToUnixTimeMilliseconds(), args);
}

/// <summary>A decided reply. Kind is APPLIED, REPLAYED or NOEFFECT.</summary>
public sealed record StoreReply(string Kind, string Status, string Code, string Result, string DecidedAt)
{
    public bool Replayed => Kind == "REPLAYED";

    public JsonElement Json => JsonDocument.Parse(Result).RootElement;
}

public sealed record ReadResult(bool Recovering, string Revision, IReadOnlyDictionary<string, string> Fields);

public sealed record Summary(
    bool Recovering, string Revision, IReadOnlyDictionary<string, string> Counters,
    long Pending, long Leases, long Outbox, long Audit)
{
    public long Counter(string name) => Counters.TryGetValue(name, out var v) ? long.Parse(v, CultureInfo.InvariantCulture) : 0;
}

/// <summary>No effect can have been written.</summary>
public sealed class StoreNotAppliedException(string code, Exception? inner = null) : Exception(code, inner)
{
    public string Code { get; } = code;
}

/// <summary>The command may or may not have been applied; resolve by resubmitting the same command.</summary>
public sealed class StoreOutcomeUnknownException(string code, Guid commandId, Exception? inner = null) : Exception(code, inner)
{
    public string Code { get; } = code;

    public Guid CommandId { get; } = commandId;
}

/// <summary>A pending intent cannot be completed right now (memory, wrong key type, outage). Nothing partial is visible.</summary>
public sealed class StoreRecoveringException(string guard, Exception? inner = null) : Exception("recovering: " + guard, inner)
{
    public string Guard { get; } = guard;
}

/// <summary>SP-018 prototype of the Redis store: application-scoped recoverable intent, guarded reads.</summary>
public sealed class RedisPrototypeStore(IDatabase db, string storeId, string application)
{
    private static readonly string Common = Load("common.lua");
    private static readonly string MutateScript = "#!lua\n" + Common + Load("mutate.lua");
    private static readonly string RecoverScript = "#!lua\n" + Common + Load("recover.lua");
    private static readonly string ConfirmScript = "#!lua\n" + Common + Load("confirm.lua");
    private static readonly string ReadScript = "#!lua flags=no-writes\n" + Load("read.lua");

    public RedisKey Meta { get; } = $"rw:{{{application}}}:meta";

    public string Prefix => $"rw:{{{application}}}:";

    /// <summary>Test hook: fixed decision time (epoch ms) instead of server TIME.</summary>
    public long? Clock { get; set; }

    public int RecoveryBatch { get; init; } = 100;

    public Task InitAsync() => db.ScriptEvaluateAsync(
        "redis.call('HSETNX', KEYS[1], 'store', ARGV[1]); redis.call('HSETNX', KEYS[1], 'rev', '0'); return 1",
        [Meta], [storeId]);

    /// <param name="failAfter">Test hook: the mutation script fails after this many writes.</param>
    /// <param name="recoverFailAfter">Test hook: the first projection batch of a large intent fails after this many writes.</param>
    public async Task<StoreReply> ExecuteAsync(StoreCommand c, int failAfter = -1, int recoverFailAfter = -1)
    {
        for (var attempt = 0; ; attempt++)
        {
            RedisValue[] args =
            [
                Clock?.ToString(CultureInfo.InvariantCulture) ?? "", failAfter, storeId, c.Op, c.Id.ToString(),
                c.Fingerprint, c.NotAfter, .. c.Args.Select(a => (RedisValue)a),
            ];
            var a = (RedisResult[])(await EvalCommandAsync(MutateScript, args, c))!;
            failAfter = -1;
            switch ((string)a[0]!)
            {
                case "RECOVERING":
                    if (attempt >= 5) throw new StoreRecoveringException((string)a[1]!);
                    await RecoverAsync();
                    continue;
                case "ACCEPTED":
                    var published = await RecoverAsync((string)a[1]!, recoverFailAfter, c);
                    if (published is not null) return published;
                    continue; // someone else published it; resubmitting replays the receipt
                default:
                    return ToReply(a);
            }
        }
    }

    /// <summary>Completes the pending intent (if any). Returns the intent's reply when it published <paramref name="expected"/>.</summary>
    public async Task<StoreReply?> RecoverAsync(string expected = "", int failAfter = -1, StoreCommand? owner = null)
    {
        var from = 1;
        while (true)
        {
            RedisResult raw;
            try
            {
                raw = await db.ScriptEvaluateAsync(RecoverScript, [Meta], [failAfter, expected, from, RecoveryBatch]);
            }
            catch (RedisServerException e)
            {
                if (owner is not null && e.Message.StartsWith("INJECTED", StringComparison.Ordinal))
                    throw new StoreOutcomeUnknownException("commit_outcome_unknown", owner.Id, e);
                throw new StoreRecoveringException(expected, e);
            }
            catch (Exception e) when (e is RedisConnectionException or RedisTimeoutException)
            {
                if (owner is not null) throw new StoreOutcomeUnknownException("commit_outcome_unknown", owner.Id, e);
                throw new StoreRecoveringException(expected, e);
            }

            failAfter = -1;
            var a = (RedisResult[])raw!;
            switch ((string)a[0]!)
            {
                case "CLEAN":
                case "OTHER":
                    return null;
                case "MORE":
                    from = (int)a[2];
                    continue;
                default: // PUBLISHED
                    return expected == "" || (string)a[1]! == expected ? ToReply(a[2..]) : null;
            }
        }
    }

    public async Task<ReadResult> ReadJobAsync(string jobId)
    {
        var a = await ReadAsync("job", jobId);
        if ((string)a[0]! == "RECOVERING") return new(true, "", new Dictionary<string, string>());
        return new(false, (string)a[1]!, Pairs((RedisResult[])a[2]!));
    }

    public async Task<Summary> ReadSummaryAsync()
    {
        var a = await ReadAsync("summary");
        if ((string)a[0]! == "RECOVERING") return new(true, "", new Dictionary<string, string>(), 0, 0, 0, 0);
        return new(false, (string)a[1]!, Pairs((RedisResult[])a[2]!), (long)a[3], (long)a[4], (long)a[5], (long)a[6]);
    }

    /// <summary>Observational receipt resolution (spec 03 §8.6). Returns RECOVERING while an intent is pending.</summary>
    public async Task<string> ResolveAsync(StoreCommand c, long classFloorMs)
    {
        var a = await ReadAsync("resolve", c.Id.ToString(), c.Op, c.Fingerprint,
            c.NotAfter.ToString(CultureInfo.InvariantCulture), classFloorMs.ToString(CultureInfo.InvariantCulture));
        return (string)a[0]!;
    }

    /// <summary>Writes the receipt-confirmation marker: a fresh fsynced write/reply after reconnect.</summary>
    public async Task<string> ConfirmAsync(Guid commandId)
    {
        var a = (RedisResult[])(await db.ScriptEvaluateAsync(ConfirmScript, [Meta], [storeId, commandId.ToString()]))!;
        return (string)a[0]!;
    }

    /// <summary>History continuity: same store identity and no lost publications compared with what we saw.</summary>
    public async Task<bool> HistoryIntactAsync(BigInteger minRevision)
    {
        var m = await db.HashGetAsync(Meta, ["store", "rev"]);
        return m[0] == storeId && !m[1].IsNull && BigInteger.Parse((string)m[1]!, CultureInfo.InvariantCulture) >= minRevision;
    }

    private async Task<RedisResult[]> ReadAsync(params string[] args)
    {
        try
        {
            return (RedisResult[])(await db.ScriptEvaluateAsync(ReadScript, [Meta], [storeId, .. args.Select(a => (RedisValue)a)]))!;
        }
        catch (Exception e) when (e is RedisConnectionException or RedisTimeoutException)
        {
            throw new StoreNotAppliedException("store_unavailable", e);
        }
    }

    private async Task<RedisResult> EvalCommandAsync(string script, RedisValue[] args, StoreCommand c)
    {
        try
        {
            return await db.ScriptEvaluateAsync(script, [Meta], args);
        }
        catch (RedisServerException e) when (IsRefusedBeforeRun(e.Message))
        {
            throw new StoreNotAppliedException(e.Message.Split(' ')[0].ToLowerInvariant(), e);
        }
        catch (RedisServerException e)
        {
            // The script may have written a prefix before failing: only the guard can say what is visible.
            throw new StoreOutcomeUnknownException("commit_outcome_unknown", c.Id, e);
        }
        catch (RedisConnectionException e) when (e.FailureType == ConnectionFailureType.UnableToResolvePhysicalConnection)
        {
            throw new StoreNotAppliedException("store_unavailable", e); // FailFast: never sent, never queued
        }
        catch (Exception e) when (e is RedisConnectionException or RedisTimeoutException)
        {
            throw new StoreOutcomeUnknownException("commit_outcome_unknown", c.Id, e);
        }
    }

    // Shebang scripts are checked for OOM/MISCONF/READONLY before they run, so these never leave a prefix.
    private static bool IsRefusedBeforeRun(string message) =>
        message.StartsWith("OOM", StringComparison.Ordinal) || message.StartsWith("MISCONF", StringComparison.Ordinal)
        || message.StartsWith("READONLY", StringComparison.Ordinal);

    private static StoreReply ToReply(RedisResult[] a) =>
        new((string)a[0]!, (string)a[1]!, (string)a[2]!, (string)a[3]!, (string)a[4]!);

    private static Dictionary<string, string> Pairs(RedisResult[] r)
    {
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < r.Length; i += 2) d[(string)r[i]!] = (string)r[i + 1]!;
        return d;
    }

    private static string Load(string name)
    {
        using var s = Assembly.GetExecutingAssembly().GetManifestResourceStream("Runwick.Redis.Prototype.Scripts." + name)
            ?? throw new InvalidOperationException(name);
        using var r = new StreamReader(s);
        return r.ReadToEnd();
    }
}

/// <summary>Checks the selected persistent profile <c>redis-aof-always-single-primary-v1</c> on a live server.</summary>
public static class ProfileValidator
{
    private static readonly (string Name, string Value)[] Required =
    [
        ("appendonly", "yes"), ("appendfsync", "always"), ("no-appendfsync-on-rewrite", "no"),
        ("maxmemory-policy", "noeviction"), ("aof-load-truncated", "no"),
    ];

    public static async Task<IReadOnlyList<string>> ValidateAsync(IDatabase db)
    {
        var violations = new List<string>();
        foreach (var (name, value) in Required)
        {
            try
            {
                var r = (RedisResult[])(await db.ExecuteAsync("CONFIG", "GET", name))!;
                var actual = r.Length == 2 ? (string)r[1]! : "<missing>";
                if (actual != value) violations.Add($"{name}={actual}");
            }
            catch (RedisServerException)
            {
                violations.Add($"{name}:unverifiable");
            }
        }

        var info = await InfoAsync(db);
        Check(info, "role", "master", violations);
        Check(info, "redis_mode", "standalone", violations);
        Check(info, "cluster_enabled", "0", violations);
        Check(info, "aof_enabled", "1", violations);
        Check(info, "aof_last_write_status", "ok", violations);
        return violations;
    }

    public static async Task<Dictionary<string, string>> InfoAsync(IDatabase db)
    {
        var text = (string)(await db.ExecuteAsync("INFO", "everything"))!;
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in text.Split('\n'))
        {
            var i = line.IndexOf(':', StringComparison.Ordinal);
            if (i > 0 && line[0] != '#') d[line[..i]] = line[(i + 1)..].Trim();
        }

        return d;
    }

    private static void Check(Dictionary<string, string> info, string key, string expected, List<string> violations)
    {
        var actual = info.GetValueOrDefault(key, "<missing>");
        if (actual != expected) violations.Add($"{key}={actual}");
    }
}
