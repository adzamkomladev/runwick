using System.Globalization;
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
}
