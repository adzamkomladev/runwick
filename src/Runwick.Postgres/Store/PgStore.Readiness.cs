using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    // ------------------------------------------------------------------ ReadinessProbe/1 (04 §7.4)

    public Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct)
    {
        var contracts = supportedContracts.Distinct().ToArray();
        if (lanes.Count is < 1 or > 384 || contracts.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(lanes), "Probe takes 1–384 lanes and 1–512 contracts.");
        }

        return ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand("""
                WITH gate AS (
                    SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, clock_timestamp() AS observed_at
                    FROM runwick.store_metadata AS s
                    WHERE s.singleton_id = 1
                    FOR SHARE
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.observed_at,
                       r.queue, r.priority, r.oldest_eligible_at
                FROM gate AS g
                LEFT JOIN LATERAL (
                    SELECT l.queue, l.priority, c.eligible_at AS oldest_eligible_at
                    FROM unnest(@lane_queues::text[], @lane_priorities::text[]) AS l(queue, priority)
                    JOIN runwick.queue_controls AS q
                      ON q.application = @application AND q.queue = l.queue AND NOT q.paused
                    CROSS JOIN LATERAL (
                        SELECT j.eligible_at
                        FROM runwick.jobs AS j
                        WHERE j.application = @application
                          AND j.queue = l.queue
                          AND j.priority = l.priority
                          AND j.state IN ('pending', 'retrying')
                          AND j.eligible_at <= g.observed_at
                          AND EXISTS (
                              SELECT 1
                              FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
                                   AS t(name, version, profile_id, profile_version)
                              WHERE t.name = j.job_name AND t.version = j.payload_version
                                AND t.profile_id = j.serializer_profile_id
                                AND t.profile_version = j.serializer_profile_version)
                        ORDER BY j.eligible_at, j.job_id
                        LIMIT 1
                    ) AS c
                ) AS r ON true
                """, conn);
            cmd.P("application", application)
                .Arr("lane_queues", [.. lanes.Select(l => l.Queue)], NpgsqlDbType.Text)
                .Arr("lane_priorities", [.. lanes.Select(l => l.Priority)], NpgsqlDbType.Text)
                .Arr("names", [.. contracts.Select(x => x.Name)], NpgsqlDbType.Text)
                .Arr("versions", [.. contracts.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                .Arr("profile_ids", [.. contracts.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                .Arr("profile_versions", [.. contracts.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer);
            var ready = new List<ReadyLane>();
            DateTimeOffset observed = default;
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    observed = PgTime.Floor(PgTime.Read(reader, 4));
                    if (!await reader.IsDBNullAsync(5, ct).ConfigureAwait(false))
                    {
                        ready.Add(new ReadyLane(new LaneKey(reader.GetString(5), reader.GetString(6)), PgTime.Read(reader, 7)));
                    }
                }
            }

            return new ReadySnapshot(observed, open ? ready : []);
        }, ct);
    }

    /// <summary>Read-only gate validation (03 §10.1): identity and schema mismatches throw; maintenance yields no work.</summary>
    bool CheckGate(Guid storeId, long revision, int epoch, string mode)
    {
        if (runner.StoreId is { } expected && expected != storeId)
        {
            throw new StoreIncompatibleException("store_identity_mismatch");
        }

        if (mode != "normal" || revision == 0)
        {
            return false;
        }

        return revision == 1 && epoch == 1 ? true : throw new StoreIncompatibleException("schema_incompatible");
    }

    async Task<T> ReadOnlyAsync<T>(PgLane lane, Func<NpgsqlConnection, Task<T>> body, CancellationToken ct)
    {
        var gate = sources.Gate(lane);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var conn = await sources.For(lane).OpenConnectionAsync(ct).ConfigureAwait(false);
            await using (conn.ConfigureAwait(false))
            {
                return await body(conn).ConfigureAwait(false);
            }
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01" or "42703" or "3F000")
        {
            throw new StoreIncompatibleException("The Runwick schema is not installed or is incompatible.", ex);
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException)
        {
            throw new StoreUnavailableException("Could not reach the PostgreSQL store.", ex);
        }
        finally
        {
            gate.Release();
        }
    }

    // ------------------------------------------------------------------ ListExpiredLeases/1 (04 §7.11)

    public Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 100);
        return ReadOnlyAsync<IReadOnlyList<ExpiredLease>>(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand("""
                WITH gate AS (
                    SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, clock_timestamp() AS observed_at
                    FROM runwick.store_metadata AS s
                    WHERE s.singleton_id = 1
                    FOR SHARE
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode,
                       e.job_id, e.current_attempt_id, e.ownership_token, e.state, e.lease_expires_at
                FROM gate AS g
                LEFT JOIN LATERAL (
                    SELECT j.job_id, j.current_attempt_id, j.ownership_token, j.state, j.lease_expires_at
                    FROM runwick.jobs AS j
                    WHERE j.application = @application
                      AND j.state IN ('reserved', 'running')
                      AND j.lease_expires_at <= g.observed_at
                    ORDER BY j.lease_expires_at, j.job_id
                    LIMIT @limit
                ) AS e ON true
                """, conn);
            cmd.P("application", application).P("limit", limit);
            var list = new List<ExpiredLease>();
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    if (!await reader.IsDBNullAsync(4, ct).ConfigureAwait(false))
                    {
                        list.Add(new ExpiredLease(reader.GetGuid(4), reader.GetGuid(5), reader.GetGuid(6), reader.GetString(7), PgTime.Read(reader, 8)));
                    }
                }
            }

            return open ? list : [];
        }, ct);
    }

    // ------------------------------------------------------------------ PublishPresence/1 (04 §7.12)

    public async Task<PresenceAck> PublishPresenceAsync(string application, PresenceRecord presence, CancellationToken ct)
    {
        var capabilities = presence.Capabilities.Distinct().ToArray();
        if (capabilities.Length > 512 || presence.SubscribedQueues.Count > 128 || presence.Expiry <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(presence));
        }

        PresenceAck? ack = null;
        var notAfter = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var reply = await runner.ExecuteAsync<PresenceBox>(
            Op(application, Guid.CreateVersion7(), "PublishPresence", notAfter, null, PgLane.Background),
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<PresenceBox>.Rollback(tx.Reject<PresenceBox>(StoreStatus.CommandExpired, "command_expired"));
                }

                var subscriptions = new System.Text.Json.Nodes.JsonArray([.. presence.SubscribedQueues.Order(StringComparer.Ordinal).Select(q => (System.Text.Json.Nodes.JsonNode?)q)]);
                var pools = new System.Text.Json.Nodes.JsonObject();
                foreach (var key in presence.PoolTargets.Keys.Order(StringComparer.Ordinal))
                {
                    pools[key] = presence.PoolTargets[key];
                }

                using var w = tx.Sql("""
                    INSERT INTO runwick.worker_instances AS w (
                        application, instance_id, process_id, host_name, build_id, status,
                        capability_revision, presence_sequence, subscriptions, pool_targets, started_at, heartbeat_at, expires_at)
                    VALUES (@application, @instance_id, @process_id, @host_name, @build_id, @status,
                            @capability_revision, @presence_sequence, @subscriptions, @pool_targets, @now, @now, @expires)
                    ON CONFLICT (application, instance_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        capability_revision = EXCLUDED.capability_revision,
                        presence_sequence = EXCLUDED.presence_sequence,
                        subscriptions = EXCLUDED.subscriptions,
                        pool_targets = EXCLUDED.pool_targets,
                        heartbeat_at = EXCLUDED.heartbeat_at,
                        expires_at = EXCLUDED.expires_at
                    WHERE w.presence_sequence < EXCLUDED.presence_sequence
                    RETURNING w.expires_at
                    """).P("instance_id", presence.InstanceId).P("process_id", presence.ProcessId).P("host_name", presence.HostName)
                        .P("build_id", presence.BuildId).P("status", presence.Status).P("capability_revision", presence.CapabilityRevision)
                        .P("presence_sequence", presence.PresenceSequence).Json("subscriptions", subscriptions).Json("pool_targets", pools)
                        .P("now", tx.Now).P("expires", PgTime.Floor(tx.Now + presence.Expiry));
                var rows = await tx.QueryAsync(w, r => PgTime.Read(r, 0), mutation: true).ConfigureAwait(false);
                if (rows.Count == 0)
                {
                    ack = new PresenceAck(tx.Now, false);
                    return new PgOutcome<PresenceBox>.Rollback(tx.Reject<PresenceBox>(StoreStatus.VersionConflict, "presence_sequence_stale"));
                }

                if (capabilities.Length > 0)
                {
                    using var c = tx.Sql("""
                        INSERT INTO runwick.worker_capabilities (
                            application, instance_id, capability_revision, job_name, payload_version, serializer_profile_id, serializer_profile_version)
                        SELECT @application, @instance_id, @capability_revision, c.name, c.version, c.profile_id, c.profile_version
                        FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
                             AS c(name, version, profile_id, profile_version)
                        ON CONFLICT DO NOTHING
                        """).P("instance_id", presence.InstanceId).P("capability_revision", presence.CapabilityRevision)
                            .Arr("names", [.. capabilities.Select(x => x.Name)], NpgsqlDbType.Text)
                            .Arr("versions", [.. capabilities.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                            .Arr("profile_ids", [.. capabilities.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                            .Arr("profile_versions", [.. capabilities.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer);
                    await tx.ExecAsync(c).ConfigureAwait(false);
                }

                ack = new PresenceAck(rows[0], true);
                return new PgOutcome<PresenceBox>.Commit(StoreReply<PresenceBox>.Applied(new PresenceBox(), tx.Now), null);
            },
            null,
            ct).ConfigureAwait(false);
        return ack is not null && (reply.Status == StoreStatus.Applied || !ack.Accepted)
            ? ack
            : throw new StoreUnavailableException($"Presence publication was not applied: {reply}");
    }

    sealed record PresenceBox;
}
