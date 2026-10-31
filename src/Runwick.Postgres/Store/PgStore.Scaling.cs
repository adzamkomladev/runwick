using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Scaler support (11 §4–§6): the shared bounded demand estimate, revisioned pool overrides (SetPoolOverride/1) and
/// the scaling history.
/// </summary>
internal sealed partial class PgStore : IScalingStore
{
    /// <summary>Due rows counted per lane before the count is reported as a lower bound (06 §1.13).</summary>
    internal const int DemandCountCap = 2_000;

    // ------------------------------------------------------------------ EstimateDemand/1 (11 §4)

    /// <summary>
    /// One read, no locks: per subscribed lane, up to <see cref="DemandCountCap"/> due rows in <c>jobs_ready</c> order,
    /// grouped by supported tuple, with the live workers that advertise the tuple and subscribe to the queue; the
    /// instance's unexpired overrides; and a digest of the live membership. The caller bounds it with a 1 s deadline.
    /// </summary>
    public Task<DemandEstimate> EstimateDemandAsync(string application, Guid instanceId, IReadOnlyList<string> queues, IReadOnlyList<JobContract> contracts,
        CancellationToken ct)
    {
        var tuples = contracts.Distinct().ToArray();
        if (queues.Count is < 1 or > 128 || tuples.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(queues), "Estimate takes 1–128 queues and 1–512 contracts.");
        }

        return ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand("""
                WITH gate AS (
                    SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, clock_timestamp() AS observed_at
                    FROM runwick.store_metadata AS s
                    WHERE s.singleton_id = 1
                    FOR SHARE
                ),
                live AS (
                    SELECT w.instance_id, w.capability_revision
                    FROM runwick.worker_instances AS w, gate AS g
                    WHERE w.application = @application AND w.expires_at > g.observed_at
                      AND w.status IN ('starting', 'ready', 'degraded')
                ),
                tuples AS (
                    SELECT t.name, t.version, t.profile_id, t.profile_version
                    FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
                         AS t(name, version, profile_id, profile_version)
                ),
                sample AS (
                    SELECT q.queue, p.priority, j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
                    FROM unnest(@queues::text[]) AS q(queue)
                    CROSS JOIN gate AS g
                    CROSS JOIN unnest(ARRAY['high', 'medium', 'low']::text[]) AS p(priority)
                    CROSS JOIN LATERAL (
                        SELECT j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
                        FROM runwick.jobs AS j
                        WHERE j.application = @application AND j.queue = q.queue AND j.priority = p.priority
                          AND j.state IN ('pending', 'retrying') AND j.eligible_at <= g.observed_at
                        ORDER BY j.eligible_at, j.job_id
                        LIMIT @cap
                    ) AS j
                ),
                truncated AS (
                    SELECT coalesce(bool_or(n = @cap), false) AS any_full
                    FROM (SELECT count(*) AS n FROM sample GROUP BY queue, priority) AS c
                ),
                backlog AS (
                    SELECT s.queue, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version, count(*) AS due
                    FROM sample AS s
                    JOIN tuples AS t
                      ON t.name = s.job_name AND t.version = s.payload_version
                     AND t.profile_id = s.serializer_profile_id AND t.profile_version = s.serializer_profile_version
                    GROUP BY s.queue, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.observed_at,
                       (SELECT any_full FROM truncated),
                       (SELECT count(*)::integer FROM live),
                       (SELECT coalesce(hashtextextended(string_agg(l.instance_id::text, ',' ORDER BY l.instance_id), 0), 0) FROM live AS l),
                       (SELECT coalesce(jsonb_object_agg(pc.pool_name, pc.target), '{}'::jsonb)::text
                        FROM runwick.pool_controls AS pc
                        WHERE pc.application = @application AND pc.instance_id = @instance_id AND pc.target IS NOT NULL
                          AND (pc.expires_at IS NULL OR pc.expires_at > g.observed_at)),
                       b.queue, b.job_name, b.payload_version, b.serializer_profile_id, b.serializer_profile_version, b.due,
                       (SELECT count(*)::integer
                        FROM live AS l
                        JOIN runwick.worker_capabilities AS c
                          ON c.application = @application AND c.instance_id = l.instance_id AND c.capability_revision = l.capability_revision
                        JOIN runwick.worker_subscriptions AS ws
                          ON ws.application = @application AND ws.instance_id = l.instance_id AND ws.queue = b.queue
                        WHERE c.job_name = b.job_name AND c.payload_version = b.payload_version
                          AND c.serializer_profile_id = b.serializer_profile_id
                          AND c.serializer_profile_version = b.serializer_profile_version)
                FROM gate AS g
                LEFT JOIN backlog AS b ON true
                """, conn);
            cmd.P("application", application).P("instance_id", instanceId).P("cap", DemandCountCap)
                .Arr("queues", [.. queues], NpgsqlDbType.Text)
                .Arr("names", [.. tuples.Select(x => x.Name)], NpgsqlDbType.Text)
                .Arr("versions", [.. tuples.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                .Arr("profile_ids", [.. tuples.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                .Arr("profile_versions", [.. tuples.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer);
            var rows = new List<DemandRow>();
            DateTimeOffset observed = default;
            var truncated = false;
            var live = 0;
            long digest = 0;
            IReadOnlyDictionary<string, int> overrides = new Dictionary<string, int>();
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    observed = PgTime.Floor(PgTime.Read(reader, 4));
                    truncated = reader.GetBoolean(5);
                    live = reader.GetInt32(6);
                    digest = reader.GetInt64(7);
                    overrides = JsonNode.Parse(reader.GetString(8))!.AsObject()
                        .ToDictionary(p => p.Key, p => p.Value!.GetValue<int>(), StringComparer.Ordinal);
                    if (await reader.IsDBNullAsync(9, ct).ConfigureAwait(false))
                    {
                        continue;
                    }

                    rows.Add(new DemandRow(reader.GetString(9),
                        new JobContract(reader.GetString(10), reader.GetInt32(11), reader.GetString(12), reader.GetInt32(13)),
                        reader.GetInt64(14), reader.GetInt32(15)));
                }
            }

            // A store in maintenance yields no demand rows; the estimate is still "known empty" only when the gate is open.
            return open
                ? new DemandEstimate(observed, rows, truncated, live, digest, overrides)
                : throw new StoreUnavailableException("The store is not accepting work (maintenance); demand is unknown.");
        }, ct);
    }

    // ------------------------------------------------------------------ RecordScalingEvent/1 (11 §6)

    public async Task<bool> RecordScalingEventAsync(string application, ScalingEventRecord record, CancellationToken ct)
    {
        if (record.OldTarget == record.NewTarget || record.OldTarget < 0 || record.NewTarget < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(record), "A scaling event records an actual target change.");
        }

        var notAfter = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var reply = await runner.ExecuteAsync<ScalingBox>(
            Op(application, Guid.CreateVersion7(), "RecordScalingEvent", notAfter, null, PgLane.Background),
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<ScalingBox>.Rollback(tx.Reject<ScalingBox>(StoreStatus.CommandExpired, "command_expired"));
                }

                using var insert = tx.Sql("""
                    INSERT INTO runwick.scaling_events (
                        application, event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason,
                        snapshot_at, membership_digest, created_at)
                    VALUES (@application, @event_id, @instance_id, @pool, @old, @new, @desired, @candidate, @reason,
                            @snapshot_at, @digest, @now)
                    """).P("event_id", Guid.CreateVersion7()).P("instance_id", record.InstanceId).P("pool", record.Pool)
                        .P("old", record.OldTarget).P("new", record.NewTarget).P("desired", record.Desired, NpgsqlDbType.Integer)
                        .P("candidate", record.Candidate, NpgsqlDbType.Integer).P("reason", record.Reason)
                        .P("snapshot_at", record.SnapshotAt).P("digest", record.MembershipDigest, NpgsqlDbType.Bigint).P("now", tx.Now);
                Expect(await tx.ExecAsync(insert).ConfigureAwait(false), 1);
                return new PgOutcome<ScalingBox>.Commit(StoreReply<ScalingBox>.Applied(new ScalingBox(), tx.Now), null);
            },
            null,
            ct).ConfigureAwait(false);
        return reply.Status == StoreStatus.Applied;
    }

    sealed record ScalingBox;

    // ------------------------------------------------------------------ SetPoolOverride/1 (11 §5)

    public async Task<StoreReply<PoolOverride>> SetPoolOverrideAsync(StoreCommand<PoolOverrideRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var now = time.GetUtcNow();
        var invalid = ValidateActor(r.Actor)
            ?? (string.IsNullOrEmpty(r.Pool) || System.Text.Encoding.UTF8.GetByteCount(r.Pool) > 64 ? "pool" : null)
            ?? (r.Target is < 0 or > 10_000 ? "target" : null)
            ?? (r.Target is null && r.ExpiresAt is not null ? "expiresAt" : null)
            ?? (r.ExpiresAt is { } e && (e <= now || e > now + MaxOverrideLifetime) ? "expiresAt" : null)
            ?? (r.ExpectedRevision < 0 ? "expectedRevision" : null);
        if (invalid is not null)
        {
            return StoreReply<PoolOverride>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), now);
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.SetPoolOverride, command.CommandId, command.NotAfter,
            CommandEnvelopes.PoolOverride(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.SetPoolOverride, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                using var q = tx.Sql("""
                    SELECT target, revision, expires_at FROM runwick.pool_controls
                    WHERE application = @application AND instance_id = @instance_id AND pool_name = @pool
                    FOR UPDATE
                    """).P("instance_id", r.InstanceId).P("pool", r.Pool);
                var rows = await tx.QueryAsync(q, x => (Target: x.IsDBNull(0) ? (int?)null : x.GetInt32(0), Revision: x.GetInt64(1),
                    ExpiresAt: PgTime.ReadNullable(x, 2))).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<PoolOverride>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                var actual = rows.Count == 0 ? 0 : rows[0].Revision;
                if (actual != r.ExpectedRevision)
                {
                    return new PgOutcome<PoolOverride>.Rollback(StoreReply<PoolOverride>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: actual, Detail: $"actual={actual}"), tx.Now));
                }

                // An expired override reads as absent: setting the same target again renews it rather than no_change.
                var current = rows.Count == 0 || rows[0].ExpiresAt is { } x0 && x0 <= tx.Now ? (Target: (int?)null, ExpiresAt: (DateTimeOffset?)null)
                    : (rows[0].Target, rows[0].ExpiresAt);
                var expires = r.ExpiresAt is { } ex ? PgTime.Floor(ex) : (DateTimeOffset?)null;
                if (current.Target == r.Target && current.ExpiresAt == expires)
                {
                    return Rollback<PoolOverride>(tx, StoreStatus.InvalidState, "no_change");
                }

                var revision = actual + 1;
                using (var u = tx.Sql("""
                    INSERT INTO runwick.pool_controls AS pc (
                        application, instance_id, pool_name, target, revision, expires_at, reason, updated_at, updated_by)
                    VALUES (@application, @instance_id, @pool, @target, @revision, @expires_at, @reason, @now, @subject)
                    ON CONFLICT (application, instance_id, pool_name) DO UPDATE
                    SET target = EXCLUDED.target, revision = EXCLUDED.revision, expires_at = EXCLUDED.expires_at,
                        reason = EXCLUDED.reason, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
                    """).P("instance_id", r.InstanceId).P("pool", r.Pool).P("target", r.Target, NpgsqlDbType.Integer)
                        .P("revision", revision).P("expires_at", expires).P("reason", r.Actor.Reason).P("now", tx.Now)
                        .P("subject", r.Actor.Subject))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var result = new PoolOverride(r.InstanceId, r.Pool, r.Target, revision, expires);
                var details = new JsonObject
                {
                    ["instanceId"] = r.InstanceId.ToString("D"), ["pool"] = r.Pool, ["target"] = r.Target,
                    ["expiresAt"] = expires is { } x1 ? PgTime.Ms(x1) : null, ["revision"] = revision,
                };
                await ControlEventAsync(tx, null, null, "pool_override_set", r.Actor, details).ConfigureAwait(false);
                await AuditAsync(tx, "pool", r.InstanceId.ToString("D") + "/" + r.Pool, r.Actor, actual, revision,
                    (JsonObject)details.DeepClone()).ConfigureAwait(false);
                return new PgOutcome<PoolOverride>.Commit(StoreReply<PoolOverride>.Applied(result, tx.Now), ReceiptCodec.Override(result));
            },
            (tx, row) => Task.FromResult(StoreReply<PoolOverride>.Applied(ReceiptCodec.ReadOverride(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    /// <summary>Longest override lifetime (06 §1.13): overrides are operational, not configuration.</summary>
    static readonly TimeSpan MaxOverrideLifetime = TimeSpan.FromDays(7);
}
