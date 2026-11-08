using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Redis.Store;

internal sealed partial class RedisStore : ITelemetrySampleStore
{
    /// <summary>
    /// The collector sample (18 §4) as PgStore.Telemetry, in one read script: per queue a capped due count and the oldest due
    /// instant from the ready sets, plus the schedule review count and oldest overdue instant. An unregistered application samples empty.
    /// </summary>
    public async Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("sample"), RxLane.Background,
            [Json(new JsonArray([.. queues.Select(q => (JsonNode?)q)])), ITelemetrySampleStore.MaxDue], ct).ConfigureAwait(false);
        switch ((string)a[0]!)
        {
            case "OK":
                break;
            case "NONE":
                return new ApplicationSample(time.GetUtcNow(), [.. queues.Select(q => new QueueSample(q, 0, null))], 0, null);
            default:
                throw new StoreUnavailableException("The store is not accepting work (maintenance); the sample is unknown.");
        }

        Seen(application, (string)a[1]!);
        var samples = JsonNode.Parse((string)a[3]!)!.AsArray().Select(n => new QueueSample(n!["queue"]!.GetValue<string>(), n["due"]!.GetValue<long>(),
            n["oldest"] is { } o ? At(o) : null)).ToList();
        var overdue = (string?)a[5] is { Length: > 0 } s ? RxReply.T(s) : (DateTimeOffset?)null;
        return new ApplicationSample(At(a[2]), samples, long.Parse((string)a[4]!, System.Globalization.CultureInfo.InvariantCulture), overdue);
    }
}
