using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IReadinessDiagnostics
{
    // ------------------------------------------------------------------ ReadinessProbe/2 (09 §2, 04 §7.4)

    /// <summary>
    /// One round trip, no locks, no payload, no unbounded counting. Row kinds: <c>lane</c> (oldest due supported job of
    /// an unpaused registered lane), <c>unsupported</c> (the lane's oldest due job has a tuple outside the request) and
    /// <c>queue</c> (control revision, pause flag, queue-scope live permits counted up to the capacity).
    /// </summary>
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
                ),
                lanes AS (
                    SELECT l.queue, l.priority
                    FROM unnest(@lane_queues::text[], @lane_priorities::text[]) AS l(queue, priority)
                    JOIN runwick.queue_controls AS q
                      ON q.application = @application AND q.queue = l.queue AND NOT q.paused
                ),
                tuples AS (
                    SELECT t.name, t.version, t.profile_id, t.profile_version
                    FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
                         AS t(name, version, profile_id, profile_version)
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.observed_at,
                       r.kind, r.queue, r.priority, r.at, r.revision, r.paused, r.scope_id, r.capacity, r.live
                FROM gate AS g
                LEFT JOIN LATERAL (
                    SELECT 'lane'::text AS kind, l.queue, l.priority, c.eligible_at AS at,
                           NULL::bigint AS revision, NULL::boolean AS paused, NULL::uuid AS scope_id, NULL::integer AS capacity,
                           NULL::integer AS live
                    FROM lanes AS l
                    CROSS JOIN LATERAL (
                        SELECT j.eligible_at
                        FROM runwick.jobs AS j
                        WHERE j.application = @application
                          AND j.queue = l.queue
                          AND j.priority = l.priority
                          AND j.state IN ('pending', 'retrying')
                          AND j.eligible_at <= g.observed_at
                          AND EXISTS (
                              SELECT 1 FROM tuples AS t
                              WHERE t.name = j.job_name AND t.version = j.payload_version
                                AND t.profile_id = j.serializer_profile_id
                                AND t.profile_version = j.serializer_profile_version)
                        ORDER BY j.eligible_at, j.job_id
                        LIMIT 1
                    ) AS c
                    UNION ALL
                    SELECT 'unsupported', l.queue, l.priority, h.eligible_at, NULL, NULL, NULL, NULL, NULL
                    FROM lanes AS l
                    CROSS JOIN LATERAL (
                        SELECT j.eligible_at, j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
                        FROM runwick.jobs AS j
                        WHERE j.application = @application
                          AND j.queue = l.queue
                          AND j.priority = l.priority
                          AND j.state IN ('pending', 'retrying')
                          AND j.eligible_at <= g.observed_at
                        ORDER BY j.eligible_at, j.job_id
                        LIMIT 1
                    ) AS h
                    WHERE NOT EXISTS (
                        SELECT 1 FROM tuples AS t
                        WHERE t.name = h.job_name AND t.version = h.payload_version
                          AND t.profile_id = h.serializer_profile_id
                          AND t.profile_version = h.serializer_profile_version)
                    UNION ALL
                    SELECT 'queue', q.queue, NULL, NULL, q.revision, q.paused, qs.scope_id, qs.capacity, qs.live
                    FROM (SELECT DISTINCT u.queue FROM unnest(@lane_queues::text[]) AS u(queue)) AS d
                    JOIN runwick.queue_controls AS q ON q.application = @application AND q.queue = d.queue
                    LEFT JOIN LATERAL (
                        SELECT s.scope_id, s.capacity,
                               (SELECT count(*)::integer
                                FROM (SELECT 1 FROM runwick.concurrency_permits AS p
                                      WHERE p.application = @application AND p.scope_id = s.scope_id
                                        AND p.lease_expires_at > g.observed_at
                                      LIMIT s.capacity) AS x) AS live
                        FROM runwick.concurrency_scopes AS s
                        WHERE s.application = @application AND s.scope_kind = 'queue' AND s.scope_key = q.queue
                    ) AS qs ON true
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
            var unsupported = new List<LaneKey>();
            var queues = new List<QueueReadiness>();
            DateTimeOffset observed = default;
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    observed = PgTime.Floor(PgTime.Read(reader, 4));
                    if (await reader.IsDBNullAsync(5, ct).ConfigureAwait(false))
                    {
                        continue;
                    }

                    switch (reader.GetString(5))
                    {
                        case "lane":
                            ready.Add(new ReadyLane(new LaneKey(reader.GetString(6), reader.GetString(7)), PgTime.Read(reader, 8)));
                            break;
                        case "unsupported":
                            unsupported.Add(new LaneKey(reader.GetString(6), reader.GetString(7)));
                            break;
                        default:
                            queues.Add(new QueueReadiness(reader.GetString(6), reader.GetBoolean(10), reader.GetInt64(9),
                                await reader.IsDBNullAsync(11, ct).ConfigureAwait(false) ? null : reader.GetGuid(11),
                                await reader.IsDBNullAsync(12, ct).ConfigureAwait(false) ? null : reader.GetInt32(12),
                                await reader.IsDBNullAsync(13, ct).ConfigureAwait(false) ? null : reader.GetInt32(13)));
                            break;
                    }
                }
            }

            return open
                ? new ReadySnapshot(observed, ready) { Queues = queues, UnsupportedLanes = unsupported }
                : new ReadySnapshot(observed, []);
        }, ct);
    }

    // ------------------------------------------------------------------ ListUnsupportedBacklog/1 (09 §5)

    /// <summary>
    /// Samples up to <paramref name="samplePerLane"/> due jobs per registered lane (index <c>jobs_ready</c>, no locks) and
    /// reports tuples that no unexpired, non-stopping worker both advertises at its active capability revision and
    /// subscribes to on that queue.
    /// </summary>
    public Task<UnsupportedBacklog> ListUnsupportedBacklogAsync(string application, int samplePerLane, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(samplePerLane, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(samplePerLane, 1_000);
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
                sample AS (
                    SELECT q.queue, p.priority, j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version,
                           j.eligible_at
                    FROM runwick.queue_controls AS q
                    CROSS JOIN gate AS g
                    CROSS JOIN unnest(ARRAY['high', 'medium', 'low']::text[]) AS p(priority)
                    CROSS JOIN LATERAL (
                        SELECT j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version, j.eligible_at
                        FROM runwick.jobs AS j
                        WHERE j.application = @application AND j.queue = q.queue AND j.priority = p.priority
                          AND j.state IN ('pending', 'retrying') AND j.eligible_at <= g.observed_at
                        ORDER BY j.eligible_at, j.job_id
                        LIMIT @per_lane
                    ) AS j
                    WHERE q.application = @application
                ),
                grouped AS (
                    SELECT s.queue, s.priority, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version,
                           min(s.eligible_at) AS oldest, count(*)::integer AS sampled,
                           EXISTS (
                               SELECT 1 FROM live AS l
                               JOIN runwick.worker_capabilities AS c
                                 ON c.application = @application AND c.instance_id = l.instance_id
                                AND c.capability_revision = l.capability_revision
                               WHERE c.job_name = s.job_name AND c.payload_version = s.payload_version
                                 AND c.serializer_profile_id = s.serializer_profile_id
                                 AND c.serializer_profile_version = s.serializer_profile_version) AS advertised,
                           EXISTS (
                               SELECT 1 FROM live AS l
                               JOIN runwick.worker_capabilities AS c
                                 ON c.application = @application AND c.instance_id = l.instance_id
                                AND c.capability_revision = l.capability_revision
                               JOIN runwick.worker_subscriptions AS ws
                                 ON ws.application = @application AND ws.instance_id = l.instance_id AND ws.queue = s.queue
                               WHERE c.job_name = s.job_name AND c.payload_version = s.payload_version
                                 AND c.serializer_profile_id = s.serializer_profile_id
                                 AND c.serializer_profile_version = s.serializer_profile_version) AS capable
                    FROM sample AS s
                    GROUP BY s.queue, s.priority, s.job_name, s.payload_version, s.serializer_profile_id, s.serializer_profile_version
                ),
                lane_counts AS (
                    SELECT queue, priority, count(*)::integer AS lane_sampled FROM sample GROUP BY queue, priority
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.observed_at,
                       (SELECT count(*)::integer FROM live) AS live_workers,
                       x.queue, x.priority, x.job_name, x.payload_version, x.serializer_profile_id, x.serializer_profile_version,
                       x.oldest, x.sampled, x.truncated, x.advertised
                FROM gate AS g
                LEFT JOIN LATERAL (
                    SELECT gr.queue, gr.priority, gr.job_name, gr.payload_version, gr.serializer_profile_id,
                           gr.serializer_profile_version, gr.oldest, gr.sampled, gr.advertised,
                           lc.lane_sampled = @per_lane AS truncated
                    FROM grouped AS gr
                    JOIN lane_counts AS lc ON lc.queue = gr.queue AND lc.priority = gr.priority
                    WHERE NOT gr.capable
                    ORDER BY gr.queue, gr.priority, gr.oldest
                ) AS x ON true
                """, conn);
            cmd.P("application", application).P("per_lane", samplePerLane);
            var entries = new List<UnsupportedBacklogEntry>();
            DateTimeOffset observed = default;
            var liveWorkers = 0;
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    observed = PgTime.Floor(PgTime.Read(reader, 4));
                    liveWorkers = reader.GetInt32(5);
                    if (await reader.IsDBNullAsync(6, ct).ConfigureAwait(false))
                    {
                        continue;
                    }

                    entries.Add(new UnsupportedBacklogEntry(reader.GetString(6), reader.GetString(7),
                        new JobContract(reader.GetString(8), reader.GetInt32(9), reader.GetString(10), reader.GetInt32(11)),
                        PgTime.Read(reader, 12), reader.GetInt32(13), reader.GetBoolean(14),
                        reader.GetBoolean(15) ? "advertised_but_not_subscribed" : "no_live_worker_advertises_tuple"));
                }
            }

            return new UnsupportedBacklog(observed, liveWorkers, samplePerLane, open ? entries : []);
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

        return revision == Migrations.PgMigrator.CurrentRevision && epoch == 1 ? true : throw new StoreIncompatibleException("schema_incompatible");
    }

    async Task<T> ReadOnlyAsync<T>(PgLane lane, Func<NpgsqlConnection, Task<T>> body, CancellationToken ct)
    {
        var gate = sources.Gate(lane);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                var conn = await sources.For(lane).OpenConnectionAsync(ct).ConfigureAwait(false);
                await using (conn.ConfigureAwait(false))
                {
                    try
                    {
                        return await body(conn).ConfigureAwait(false);
                    }
                    catch (PostgresException ex) when (attempt == 1 && PgRunner.IsStalePlan(ex))
                    {
                        // 0A000 after a migration: a read-only call applied nothing, so it is retried once on a
                        // cleared pool (04 §9.2, PF-03).
                        sources.For(lane).Clear();
                    }
                }
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
        var subscriptions = new System.Text.Json.Nodes.JsonArray([.. presence.SubscribedQueues.Order(StringComparer.Ordinal).Select(q => (System.Text.Json.Nodes.JsonNode?)q)]);
        var pools = new System.Text.Json.Nodes.JsonObject();
        foreach (var key in presence.PoolTargets.Keys.Order(StringComparer.Ordinal))
        {
            pools[key] = presence.PoolTargets[key];
        }

        // The clock, the instance upsert (sequence-fenced), the capability rows and the subscription rows run in
        // runwick.publish_presence_v1 (04 §7.12, 09 §3).
        var reply = await runner.CallAsync(
            Op(application, Guid.CreateVersion7(), "PublishPresence", notAfter, null, PgLane.Background),
            new PgCall("publish_presence_v1").P("p_instance_id", presence.InstanceId).P("p_process_id", presence.ProcessId)
                .P("p_host_name", presence.HostName).P("p_build_id", presence.BuildId).P("p_status", presence.Status)
                .P("p_capability_revision", presence.CapabilityRevision).P("p_presence_sequence", presence.PresenceSequence)
                .Json("p_subscriptions", subscriptions).Json("p_pool_targets", pools).P("p_expiry", presence.Expiry)
                .Arr("p_names", [.. capabilities.Select(x => x.Name)], NpgsqlDbType.Text)
                .Arr("p_versions", [.. capabilities.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                .Arr("p_profile_ids", [.. capabilities.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                .Arr("p_profile_versions", [.. capabilities.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer)
                .Arr("p_queues", [.. presence.SubscribedQueues.Distinct().Order(StringComparer.Ordinal)], NpgsqlDbType.Text),
            row =>
            {
                if (row.HasValue)
                {
                    ack = new PresenceAck(PgTime.FromMs(row.Result!["expiresAt"]!.GetValue<long>()), true);
                    return row.Applied(new PresenceBox());
                }

                ack = row.Code == "presence_sequence_stale" ? new PresenceAck(row.Decided, false) : null;
                return row.Reject<PresenceBox>();
            },
            ct).ConfigureAwait(false);
        return ack is not null && (reply.Status == StoreStatus.Applied || !ack.Accepted)
            ? ack
            : throw new StoreUnavailableException($"Presence publication was not applied: {reply}");
    }

    sealed record PresenceBox;
}
