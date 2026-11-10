using System.Globalization;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

internal sealed partial class RedisStore : IReadinessDiagnostics
{
    // ReadinessProbe/2, ListUnsupportedBacklog/1, ListExpiredLeases/1 as read-only scripts (22 §4.6); PublishPresence/1 as a
    // receipt-less write.

    static DateTimeOffset At(RedisResult r) => RxReply.T((string)r!);

    static DateTimeOffset At(JsonNode? n) => DateTimeOffset.FromUnixTimeMilliseconds(n!.GetValue<long>());

    public async Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct)
    {
        var contracts = supportedContracts.Distinct().ToArray();
        if (lanes.Count is < 1 or > 384 || contracts.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(lanes), "Probe takes 1–384 lanes and 1–512 contracts.");
        }

        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var laneJson = new JsonArray([.. lanes.Select(l => (JsonNode?)new JsonArray(l.Queue, l.Priority))]);
        var a = await runner.ReadAsync(application, RxScript.Read("probe"), RxLane.Background, [Json(laneJson), Json(Contracts(contracts))], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK")
        {
            return new ReadySnapshot(time.GetUtcNow(), []);
        }

        Seen(application, (string)a[1]!);
        var observed = At(a[2]);
        var queues = JsonNode.Parse((string)a[3]!)!.AsArray().Select(n => new QueueReadiness(n!["queue"]!.GetValue<string>(), n["paused"]!.GetValue<bool>(),
            n["revision"]!.GetValue<long>(), n["scopeId"] is { } s ? Guid.Parse(s.GetValue<string>()) : null,
            n["capacity"]?.GetValue<int>(), n["live"]?.GetValue<int>())).ToList();
        var ready = JsonNode.Parse((string)a[4]!)!.AsArray()
            .Select(n => new ReadyLane(new LaneKey(n!["queue"]!.GetValue<string>(), n["priority"]!.GetValue<string>()), At(n["at"]))).ToList();
        var unsupported = JsonNode.Parse((string)a[5]!)!.AsArray()
            .Select(n => new LaneKey(n![0]!.GetValue<string>(), n[1]!.GetValue<string>())).ToList();
        return new ReadySnapshot(observed, ready) { Queues = queues, UnsupportedLanes = unsupported };
    }

    public async Task<UnsupportedBacklog> ListUnsupportedBacklogAsync(string application, int samplePerLane, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(samplePerLane, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(samplePerLane, 1_000);
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("backlog"), RxLane.Background, [samplePerLane], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK")
        {
            return new UnsupportedBacklog(time.GetUtcNow(), 0, samplePerLane, []);
        }

        var entries = JsonNode.Parse((string)a[4]!)!.AsArray().Select(n => new UnsupportedBacklogEntry(n!["queue"]!.GetValue<string>(),
            n["priority"]!.GetValue<string>(),
            new JobContract(n["name"]!.GetValue<string>(), n["pver"]!.GetValue<int>(), n["profile"]!.GetValue<string>(), n["profver"]!.GetValue<int>()),
            At(n["oldest"]), n["n"]!.GetValue<int>(), n["truncated"]!.GetValue<bool>(), n["reason"]!.GetValue<string>())).ToList();
        return new UnsupportedBacklog(At(a[2]), int.Parse((string)a[3]!, CultureInfo.InvariantCulture), samplePerLane, entries);
    }

    public async Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 100);
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("expired"), RxLane.Background, [limit], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK")
        {
            return [];
        }

        var list = new List<ExpiredLease>();
        for (var i = 2; i + 4 < a.Length; i += 5)
        {
            list.Add(new ExpiredLease(Guid.Parse((string)a[i]!), Guid.Parse((string)a[i + 1]!), Guid.Parse((string)a[i + 2]!), (string)a[i + 3]!, At(a[i + 4])));
        }

        return list;
    }

    public async Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct)
    {
        var capabilities = presence.Capabilities.Distinct().ToArray();
        if (capabilities.Length > 512 || presence.SubscribedQueues.Count > 128 || presence.Expiry <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(presence));
        }

        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var pools = new JsonObject();
        foreach (var key in presence.PoolTargets.Keys.Order(StringComparer.Ordinal))
        {
            pools[key] = presence.PoolTargets[key];
        }

        var record = new JsonObject
        {
            ["seq"] = presence.PresenceSequence, ["status"] = presence.Status, ["caprev"] = presence.CapabilityRevision, ["pid"] = presence.ProcessId,
            ["host"] = presence.HostName, ["build"] = presence.BuildId,
            ["subscriptions"] = new JsonArray([.. presence.SubscribedQueues.Distinct().Order(StringComparer.Ordinal).Select(q => (JsonNode?)q)]),
            ["pools"] = pools, ["caps"] = Contracts(capabilities),
        };
        if (presence.DetailText() is { } detail)
        {
            record["detail"] = detail;
        }

        var r = await Write(Op(application, Guid.CreateVersion7(), "PublishPresence", time.GetUtcNow() + TimeSpan.FromSeconds(10), null, RxLane.Background, 'd'),
            "presence", ct, Hex(presence.InstanceId), Json(record), LeaseMs(presence.Expiry)).ConfigureAwait(false);
        if (r.HasValue)
        {
            return new PresenceAck(At(r.Result!["expiresAt"]), true);
        }

        return r.Code == "presence_sequence_stale" ? new PresenceAck(r.Decided, false) : throw new StoreUnavailableException($"Presence publication was not applied: {r}");
    }
}
