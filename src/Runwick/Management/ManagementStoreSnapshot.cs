using System.Text.Json.Serialization;

namespace Runwick.Management;

[JsonConverter(typeof(JsonStringEnumConverter<StoreSnapshotStatus>))]
public enum StoreSnapshotStatus
{
    Available,
    Stale,
    Unavailable,
    Unsupported,
    PermissionDenied,
}

[JsonConverter(typeof(JsonStringEnumConverter<StoreSnapshotScope>))]
public enum StoreSnapshotScope
{
    Process,
    Application,
    Store,
    ProviderNode,
}

[JsonConverter(typeof(JsonStringEnumConverter<StoreReachability>))]
public enum StoreReachability
{
    Reachable,
    Unavailable,
    Unknown,
}

public sealed record StoreSnapshotFact<T>(StoreSnapshotScope Scope, StoreSnapshotStatus Status, T? Value) where T : class;

public sealed record StoreConnectionObservation(StoreSnapshotScope Scope, StoreReachability Status, string Reason);

public sealed record StoreRoleObservation(StoreSnapshotScope Scope, RunwickRoleHealth Readiness);

public sealed record StoreApplicationObservation(string AdmissionMode, int RequiredProtocolEpoch, bool SupportsRequirements);

public sealed record StoreIdentityObservation(Guid StoreId, long SchemaRevision, int ProtocolEpoch, string MigrationMode);

public sealed record StoreProfileObservation(string Profile, StoreSnapshotStatus Verification);

public sealed record StoreByteCount(long Bytes);

public sealed record SqliteWriterCounters(long Commits, long BusyConflicts);

public sealed record SqliteStoreDiagnostics(
    StoreSnapshotFact<StoreByteCount> DatabaseFile,
    StoreSnapshotFact<StoreByteCount> WalFile,
    StoreSnapshotFact<SqliteWriterCounters> Writer);

internal interface ISqliteStoreDiagnosticsSource
{
    SqliteStoreDiagnostics ObserveDiagnostics();
}

public sealed record PostgresSessionCount(int ConnectedSessions);

public sealed record PostgresLockWaitCount(long SessionsWaiting);

public sealed record PostgresJobTableStats(long TotalBytes, long EstimatedDeadTuples, long AutovacuumCount);

public sealed record PostgresWalStats(decimal BytesGenerated, DateTimeOffset? ResetAt);

public sealed record PostgresRecoveryState(bool InRecovery);

public sealed record PostgresReplicationStats(long ConnectedSenders, decimal? LargestReplayBacklogBytes);

public sealed record PostgresStoreDiagnostics(
    StoreSnapshotFact<PostgresSessionCount> Sessions,
    StoreSnapshotFact<PostgresLockWaitCount> LockWaits,
    StoreSnapshotFact<StoreByteCount> DatabaseSize,
    StoreSnapshotFact<PostgresJobTableStats> JobsTable,
    StoreSnapshotFact<PostgresWalStats> Wal,
    StoreSnapshotFact<PostgresRecoveryState> Recovery,
    StoreSnapshotFact<PostgresReplicationStats> Replication)
{
    public DateTimeOffset? SampledAt { get; init; }
}

internal interface IPostgresStoreDiagnosticsSource
{
    Task<PostgresStoreDiagnostics> ObserveDiagnosticsAsync(CancellationToken ct);
}

public sealed record RedisMemoryStats(long? UsedBytes, long? LimitBytes, double? FragmentationRatio);

public sealed record RedisActivityStats(long? ConnectedClients, long? CommandsPerSecond, long? EvictedKeys);

[JsonConverter(typeof(JsonStringEnumConverter<RedisAofStatus>))]
public enum RedisAofStatus { Ok, Error }

public sealed record RedisAofStats(bool? Enabled, long? CurrentBytes, RedisAofStatus? LastWrite,
    bool? RewriteInProgress, bool? RewriteScheduled, RedisAofStatus? LastRewrite);

[JsonConverter(typeof(JsonStringEnumConverter<RedisNodeRole>))]
public enum RedisNodeRole { Primary, Replica }

public sealed record RedisReplicationStats(RedisNodeRole? Role, long? ConnectedReplicas, long? ReplicationOffset,
    long? ReplicaAppliedOffset, bool? PrimaryLinkUp);

public sealed record RedisStoreDiagnostics(
    StoreSnapshotFact<RedisMemoryStats> Memory,
    StoreSnapshotFact<RedisActivityStats> Activity,
    StoreSnapshotFact<RedisAofStats> Aof,
    StoreSnapshotFact<RedisReplicationStats> Replication)
{
    public DateTimeOffset? SampledAt { get; init; }
}

internal interface IRedisStoreDiagnosticsSource
{
    Task<RedisStoreDiagnostics> ObserveDiagnosticsAsync(CancellationToken ct);
}

[JsonConverter(typeof(JsonStringEnumConverter<StoreProviderCapability>))]
public enum StoreProviderCapability
{
    BatchClaim,
    BatchFinish,
    BatchRenewal,
    RateAdmission,
    Scheduling,
    Retention,
    WakeNotifications,
    OutboxDestination,
}

/// <summary>Host-observed store state. Facts can be independently unavailable, stale or hidden.</summary>
public sealed record ManagementStoreSnapshot(
    string Application,
    string Provider,
    DateTimeOffset ObservedAt,
    DateTimeOffset? LastSuccessfulAt,
    double? LastSuccessfulAgeSeconds,
    StoreConnectionObservation Connection,
    IReadOnlyList<StoreRoleObservation> Roles,
    StoreSnapshotFact<StoreApplicationObservation> Admission,
    StoreSnapshotFact<StoreIdentityObservation> Identity,
    StoreSnapshotFact<StoreProfileObservation> Persistence,
    StoreSnapshotFact<IReadOnlyList<StoreProviderCapability>> Capabilities,
    StoreSnapshotFact<string> ServerVersion,
    StoreSnapshotFact<string> Recovery,
    StoreSnapshotFact<string> Topology)
{
    public SqliteStoreDiagnostics? Sqlite { get; init; }

    public PostgresStoreDiagnostics? Postgres { get; init; }

    public RedisStoreDiagnostics? Redis { get; init; }

    internal static ManagementStoreSnapshot Unobserved(DateTimeOffset now) => new("", "unknown", now, null, null,
        new(StoreSnapshotScope.Process, StoreReachability.Unknown, "not_observed"), [],
        new(StoreSnapshotScope.Application, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.Store, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.Process, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.Process, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.ProviderNode, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.Store, StoreSnapshotStatus.Unavailable, null),
        new(StoreSnapshotScope.ProviderNode, StoreSnapshotStatus.Unavailable, null));
}
