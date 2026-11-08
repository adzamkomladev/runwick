using System.Globalization;
using System.Text.Json.Nodes;
using StackExchange.Redis;

namespace Runwick.Contract.Tests;

/// <summary>Redis setup writes for the ported ADM/OWN/UNI/ENC cases (the SQL stores use an admin UPDATE). Setup only.</summary>
internal static partial class RedisAdmin
{
    /// <summary>Moves one pending job's eligibility back by <paramref name="ms"/>: the job hash field and its ready-index score.</summary>
    public static async Task MakeEligibleEarlier(string application, Guid job, long ms)
    {
        var db = await Db();
        var key = P(application) + "job:" + job.ToString("N");
        var fields = await db.HashGetAsync(key, ["eligible", "queue", "priority"]);
        var eligible = long.Parse((string)fields[0]!, CultureInfo.InvariantCulture) - ms;
        await db.HashSetAsync(key, "eligible", eligible);
        await db.SortedSetAddAsync(P(application) + "ready:" + (string)fields[1]! + ":" + (string)fields[2]!, job.ToString("N"), eligible);
    }

    /// <summary>Removes the permit of <paramref name="attempt"/> with the lowest scope and returns it so it can be put back.</summary>
    public static async Task<(string Field, string Value)> TakePermit(string application, Guid attempt)
    {
        var db = await Db();
        var permits = await Hash(application, "permits");
        var (field, value) = permits.Where(p => JsonNode.Parse(p.Value)!["attempt"]!.GetValue<string>() == attempt.ToString("N"))
            .OrderBy(p => p.Key, StringComparer.Ordinal).First();
        await db.HashDeleteAsync(P(application) + "permits", field);
        return (field, value);
    }

    /// <summary>Puts a taken permit back with its lease expiry moved by <paramref name="ms"/>.</summary>
    public static async Task PutPermitShifted(string application, (string Field, string Value) permit, long ms)
    {
        var row = JsonNode.Parse(permit.Value)!.AsObject();
        row["lease"] = row["lease"]!.GetValue<long>() + ms;
        await (await Db()).HashSetAsync(P(application) + "permits", permit.Field, row.ToJsonString());
    }

    /// <summary>Records a cancellation request on a job (the job hash field Cancel/1 sets), for cases that need the request without the engine.</summary>
    public static async Task SetCancelRequested(string application, Guid job) =>
        await (await Db()).HashSetAsync(P(application) + "job:" + job.ToString("N"), "cancelat", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    /// <summary>Gives a waiting job a spent execution budget: its generation's started count reaches the maximum.</summary>
    public static async Task SpendBudget(string application, Guid job)
    {
        var db = await Db();
        var key = P(application) + "job:" + job.ToString("N");
        await db.HashSetAsync(key, "g:1:started", await db.HashGetAsync(key, "g:1:max"));
    }

    /// <summary>Scopes holding more live permits (lease after <paramref name="nowMs"/>) than their capacity.</summary>
    public static async Task<long> OverCapacityScopes(string application, long nowMs)
    {
        var permits = (await Hash(application, "permits")).GroupBy(p => p.Key.Split(':')[0])
            .ToDictionary(g => g.Key, g => g.Count(p => JsonNode.Parse(p.Value)!["lease"]!.GetValue<long>() > nowMs));
        var scopes = (await Hash(application, "scopes")).Where(p => !p.Key.StartsWith("k:", StringComparison.Ordinal));
        return scopes.Count(s => permits.GetValueOrDefault(s.Key) > JsonNode.Parse(s.Value)!["capacity"]!.GetValue<int>());
    }
}

