using System.Globalization;

namespace Runwick.Harness.Store;

/// <summary>
/// The layout-1 event rows of one stored <c>events:data</c> entry (22 §3.2): a row as it is, or a layout-2 pack expanded as
/// <c>U.event_rows</c> (prelude.lua) does for <c>EventsSince</c>. For test helpers that read the hash directly.
/// </summary>
public static class RedisEventRows
{
    public static IEnumerable<string> Expand(string member, string value)
    {
        if (!value.StartsWith("~1|", StringComparison.Ordinal))
        {
            yield return value;
            yield break;
        }

        var lines = value.Split('\n');
        var head = lines[0].Split('|', 3);
        var (at, cmd) = (head[1], head[2]);
        var id = long.Parse(member, CultureInfo.InvariantCulture);
        static string Q(string x) => "\"" + x + "\"";
        foreach (var line in lines.Skip(1))
        {
            var f = line.Split('|', 6);
            var (kind, job, att, c) = (f[0], f[1], f[2], f[3]);
            var n = int.Parse(f[4], CultureInfo.InvariantCulture);
            var rest = f[5];
            var start = "{\"actorId\":" + (n > 0 ? rest[..n] : "null") + ",\"attemptId\":" + (att.Length > 0 ? Q(att) : "null")
                + ",\"commandId\":" + Q(c.Length > 0 ? c : cmd) + ",\"details\":";
            var tail = ",\"jobId\":" + (job.Length > 0 ? Q(job) : "null") + ",\"kind\":";
            if (kind == "~rs")
            {
                var g = rest.Split(',');
                yield return start + "{\"generation\":" + g[0] + ",\"reservationNumber\":" + g[1] + "},\"eventId\":" + id + tail
                    + "\"attempt_reserved\",\"occurredAt\":" + at + "}";
                yield return start + "{\"executionNumber\":" + g[2] + ",\"generation\":" + g[0] + "},\"eventId\":" + (id + 1) + tail
                    + "\"attempt_started\",\"occurredAt\":" + at + "}";
                id += 2;
            }
            else
            {
                yield return start + rest[n..] + ",\"eventId\":" + id + tail + Q(kind) + ",\"occurredAt\":" + at + "}";
                id++;
            }
        }
    }

    /// <summary>Every row of an <c>events:data</c> hash (member to stored entry), in event id order.</summary>
    public static List<string> All(IEnumerable<KeyValuePair<string, string>> hash) =>
        [.. hash.OrderBy(p => long.Parse(p.Key, CultureInfo.InvariantCulture)).SelectMany(p => Expand(p.Key, p.Value))];
}
