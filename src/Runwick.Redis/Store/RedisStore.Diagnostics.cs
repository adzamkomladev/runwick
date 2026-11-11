using System.Globalization;
using Runwick.Management;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

internal sealed partial class RedisStore : IRedisStoreDiagnosticsSource
{
    async Task<RedisStoreDiagnostics> IRedisStoreDiagnosticsSource.ObserveDiagnosticsAsync(CancellationToken ct)
    {
        try
        {
            return ParseDiagnostics(await runner.Client.DiagnosticsInfoAsync(ct).ConfigureAwait(false));
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("NOPERM", StringComparison.Ordinal))
        {
            return MissingDiagnostics(StoreSnapshotStatus.PermissionDenied);
        }
    }

    internal static RedisStoreDiagnostics ParseDiagnostics(string? info)
    {
        if (string.IsNullOrEmpty(info) || info.Length > 128 * 1024)
        {
            return MissingDiagnostics(StoreSnapshotStatus.Unavailable);
        }

        // Match whole field names. Ignore per-replica records, addresses, IDs, and all other INFO output.
        var fields = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["used_memory"] = null, ["maxmemory"] = null, ["mem_fragmentation_ratio"] = null,
            ["connected_clients"] = null, ["instantaneous_ops_per_sec"] = null, ["evicted_keys"] = null,
            ["aof_enabled"] = null, ["aof_current_size"] = null, ["aof_last_write_status"] = null,
            ["aof_rewrite_in_progress"] = null, ["aof_rewrite_scheduled"] = null, ["aof_last_bgrewrite_status"] = null,
            ["role"] = null, ["connected_slaves"] = null, ["master_repl_offset"] = null,
            ["slave_repl_offset"] = null, ["master_link_status"] = null,
        };
        using var lines = new StringReader(info);
        while (lines.ReadLine() is { } line)
        {
            var split = line.IndexOf(':', StringComparison.Ordinal);
            if (split is > 0 and <= 32 && fields.ContainsKey(line[..split]))
            {
                fields[line[..split]] = line[(split + 1)..];
            }
        }

        long? Number(string key) => long.TryParse(fields[key], NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : null;
        bool? Flag(string key) => fields[key] switch { "1" => true, "0" => false, _ => null };
        RedisAofStatus? AofStatus(string key) => fields[key] switch { "ok" => RedisAofStatus.Ok, "err" => RedisAofStatus.Error, _ => null };
        var ratio = double.TryParse(fields["mem_fragmentation_ratio"], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var parsed)
            && double.IsFinite(parsed) && parsed >= 0 ? parsed : (double?)null;
        var memory = new RedisMemoryStats(Number("used_memory"), Number("maxmemory"), ratio);
        var activity = new RedisActivityStats(Number("connected_clients"), Number("instantaneous_ops_per_sec"), Number("evicted_keys"));
        var enabled = Flag("aof_enabled");
        var aof = enabled != false
            ? new RedisAofStats(enabled, Number("aof_current_size"), AofStatus("aof_last_write_status"),
                Flag("aof_rewrite_in_progress"), Flag("aof_rewrite_scheduled"), AofStatus("aof_last_bgrewrite_status"))
            : new RedisAofStats(enabled, null, null, null, null, null);
        RedisNodeRole? role = fields["role"] switch { "master" => RedisNodeRole.Primary, "slave" => RedisNodeRole.Replica, _ => null };
        var replication = new RedisReplicationStats(role, Number("connected_slaves"), Number("master_repl_offset"),
            role == RedisNodeRole.Replica ? Number("slave_repl_offset") : null,
            role == RedisNodeRole.Replica ? fields["master_link_status"] switch { "up" => true, "down" => false, _ => null } : null);

        return new(
            Fact(memory is { UsedBytes: null, LimitBytes: null, FragmentationRatio: null } ? null : memory),
            Fact(activity is { ConnectedClients: null, CommandsPerSecond: null, EvictedKeys: null } ? null : activity),
            Fact(aof is { Enabled: null, CurrentBytes: null, LastWrite: null, RewriteInProgress: null, RewriteScheduled: null, LastRewrite: null } ? null : aof),
            Fact(replication is { Role: null, ConnectedReplicas: null, ReplicationOffset: null, ReplicaAppliedOffset: null, PrimaryLinkUp: null } ? null : replication));
    }

    static StoreSnapshotFact<T> Fact<T>(T? value, StoreSnapshotStatus missing = StoreSnapshotStatus.Unavailable) where T : class =>
        new(StoreSnapshotScope.ProviderNode, value is null ? missing : StoreSnapshotStatus.Available, value);

    static RedisStoreDiagnostics MissingDiagnostics(StoreSnapshotStatus status) =>
        new(Fact<RedisMemoryStats>(null, status), Fact<RedisActivityStats>(null, status), Fact<RedisAofStats>(null, status),
            Fact<RedisReplicationStats>(null, status));
}
