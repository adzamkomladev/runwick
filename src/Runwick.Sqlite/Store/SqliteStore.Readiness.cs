using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Internal;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore : IReadinessDiagnostics
{
    // ReadinessProbe/2, ListUnsupportedBacklog/1 and ListExpiredLeases/1 on the read path (20 §4.2: one snapshot, no write
    // lock); PublishPresence/1 on the write path. The statements are PgStore.Readiness's, split per lane.

    /// <summary>Read-only gate (03 §10.1): identity and schema mismatches throw; maintenance yields no work.</summary>
    bool CheckGate(SqliteConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT store_id, schema_revision, protocol_epoch, migration_mode FROM runwick_store_metadata WHERE singleton_id = 1";
        using var r = cmd.ExecuteReader();
        if (!r.Read())
        {
            throw new StoreIncompatibleException("schema_incompatible");
        }

        if (runner.StoreId is { } expected && expected != r.Uuid(0))
        {
            throw new StoreIncompatibleException("store_identity_mismatch");
        }

        if (r.GetString(3) != "normal" || r.GetInt64(1) == 0)
        {
            return false;
        }

        return r.GetInt64(1) == Migrations.SqliteMigrator.CurrentRevision && r.GetInt32(2) == 1 ? true : throw new StoreIncompatibleException("schema_incompatible");
    }

    static SqliteCommand Cmd(SqliteConnection conn, string application, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@application", application);
        return cmd;
    }

    static List<T> Rows<T>(SqliteCommand cmd, Func<SqliteDataReader, T> map)
    {
        using (cmd)
        {
            using var r = cmd.ExecuteReader();
            var list = new List<T>();
            while (r.Read())
            {
                list.Add(map(r));
            }

            return list;
        }
    }

    static JsonArray Tuples(IEnumerable<JobContract> contracts) =>
        new([.. contracts.Select(c => (JsonNode)new JsonArray(c.Name, c.PayloadVersion, c.SerializerProfileId, c.SerializerProfileVersion))]);

    static readonly string[] Priorities = ["high", "medium", "low"];

    const string TupleMatch = """
        EXISTS (SELECT 1 FROM json_each(@tuples) AS c
                WHERE c.value ->> 0 = j.job_name AND c.value ->> 1 = j.payload_version
                  AND c.value ->> 2 = j.serializer_profile_id AND c.value ->> 3 = j.serializer_profile_version)
        """;

    public Task<ReadySnapshot> ProbeAsync(string application, IReadOnlyList<LaneKey> lanes, IReadOnlyList<JobContract> supportedContracts, CancellationToken ct)
    {
        var contracts = supportedContracts.Distinct().ToArray();
        if (lanes.Count is < 1 or > 384 || contracts.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(lanes), "Probe takes 1–384 lanes and 1–512 contracts.");
        }

        var tuples = Tuples(contracts).ToJsonString();
        return runner.ReadAsync(conn =>
        {
            var open = CheckGate(conn);
            var observed = Now();
            if (!open)
            {
                return new ReadySnapshot(observed, []);
            }

            var ready = new List<ReadyLane>();
            var unsupported = new List<LaneKey>();
            var queues = new List<QueueReadiness>();
            var controls = new Dictionary<string, (bool Paused, long Revision)>(StringComparer.Ordinal);
            foreach (var queue in lanes.Select(l => l.Queue).Distinct(StringComparer.Ordinal))
            {
                var q = Rows(Cmd(conn, application, "SELECT paused, revision FROM runwick_queue_controls WHERE application = @application AND queue = @queue")
                    .P("queue", queue), r => (r.Bool(0), r.GetInt64(1)));
                if (q.Count == 0)
                {
                    continue;
                }

                controls[queue] = q[0];
                var scope = Rows(Cmd(conn, application, """
                    SELECT s.scope_id, s.capacity,
                           min(s.capacity, (SELECT count(*) FROM runwick_concurrency_permits AS p
                                                  WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.lease_expires_at > @now
                                            ))
                    FROM runwick_concurrency_scopes AS s
                    WHERE s.application = @application AND s.scope_kind = 'queue' AND s.scope_key = @queue
                    """).P("queue", queue).P("now", observed), r => (Id: (Guid?)r.Uuid(0), Capacity: (int?)r.GetInt32(1), Live: (int?)r.GetInt32(2)));
                var (id, capacity, live) = scope.Count == 1 ? scope[0] : (null, null, null);
                queues.Add(new QueueReadiness(queue, q[0].Item1, q[0].Item2, id, capacity, live));
            }

            foreach (var lane in lanes)
            {
                if (!controls.TryGetValue(lane.Queue, out var c) || c.Paused)
                {
                    continue;
                }

                var head = Rows(Cmd(conn, application, $"""
                    SELECT j.eligible_at, {TupleMatch} FROM runwick_jobs AS j
                    WHERE j.application = @application AND j.queue = @queue AND j.priority = @priority AND j.state IN ('pending', 'retrying')
                      AND j.eligible_at <= @now
                    ORDER BY j.eligible_at, j.job_id LIMIT 1
                    """).P("queue", lane.Queue).P("priority", lane.Priority).P("now", observed).P("tuples", tuples), r => r.Bool(1));
                if (head.Count == 1 && !head[0])
                {
                    unsupported.Add(lane);
                }

                var due = Rows(Cmd(conn, application, $"""
                    SELECT j.eligible_at FROM runwick_jobs AS j
                    WHERE j.application = @application AND j.queue = @queue AND j.priority = @priority AND j.state IN ('pending', 'retrying')
                      AND j.eligible_at <= @now AND {TupleMatch}
                    ORDER BY j.eligible_at, j.job_id LIMIT 1
                    """).P("queue", lane.Queue).P("priority", lane.Priority).P("now", observed).P("tuples", tuples), r => r.Time(0));
                if (due.Count == 1)
                {
                    ready.Add(new ReadyLane(lane, due[0]));
                }
            }

            return new ReadySnapshot(observed, ready) { Queues = queues, UnsupportedLanes = unsupported };
        }, ct);
    }

    public Task<UnsupportedBacklog> ListUnsupportedBacklogAsync(string application, int samplePerLane, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(samplePerLane, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(samplePerLane, 1_000);
        return runner.ReadAsync(conn =>
        {
            var open = CheckGate(conn);
            var observed = Now();
            const string Live = """
                SELECT w.instance_id, w.capability_revision FROM runwick_worker_instances AS w
                WHERE w.application = @application AND w.expires_at > @now AND w.status IN ('starting', 'ready', 'degraded')
                """;
            var liveWorkers = Rows(Cmd(conn, application, $"SELECT count(*) FROM ({Live})").P("now", observed), r => r.GetInt32(0))[0];
            var entries = new List<UnsupportedBacklogEntry>();
            if (!open)
            {
                return new UnsupportedBacklog(observed, liveWorkers, samplePerLane, entries);
            }

            var queues = Rows(Cmd(conn, application, "SELECT queue FROM runwick_queue_controls WHERE application = @application"), r => r.GetString(0));
            foreach (var queue in queues.Order(Utf8Order.Instance))
            {
                foreach (var priority in Priorities)
                {
                    var sample = Rows(Cmd(conn, application, """
                        SELECT j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version, j.eligible_at
                        FROM runwick_jobs AS j
                        WHERE j.application = @application AND j.queue = @queue AND j.priority = @priority AND j.state IN ('pending', 'retrying')
                          AND j.eligible_at <= @now
                        ORDER BY j.eligible_at, j.job_id LIMIT @per_lane
                        """).P("queue", queue).P("priority", priority).P("now", observed).P("per_lane", samplePerLane),
                        r => (Contract: new JobContract(r.GetString(0), r.GetInt32(1), r.GetString(2), r.GetInt32(3)), At: r.Time(4)));
                    var truncated = sample.Count == samplePerLane;
                    var groups = sample.GroupBy(s => s.Contract).Select(g => (Contract: g.Key, Oldest: g.Min(x => x.At), Count: g.Count()));
                    foreach (var g in groups.OrderBy(g => g.Oldest))
                    {
                        bool Exists(bool subscribed) => Rows(Cmd(conn, application, $"""
                            SELECT EXISTS (SELECT 1 FROM ({Live}) AS l
                                JOIN runwick_worker_capabilities AS c
                                  ON c.application = @application AND c.instance_id = l.instance_id AND c.capability_revision = l.capability_revision
                                WHERE c.job_name = @name AND c.payload_version = @version AND c.serializer_profile_id = @profile_id
                                  AND c.serializer_profile_version = @profile_version
                                  AND (@subscribed = 0 OR EXISTS (SELECT 1 FROM runwick_worker_subscriptions AS ws
                                       WHERE ws.application = @application AND ws.instance_id = l.instance_id AND ws.queue = @queue)))
                            """).P("now", observed).P("name", g.Contract.Name).P("version", g.Contract.PayloadVersion)
                            .P("profile_id", g.Contract.SerializerProfileId).P("profile_version", g.Contract.SerializerProfileVersion)
                            .P("subscribed", subscribed).P("queue", queue), r => r.Bool(0))[0];
                        if (!Exists(subscribed: true))
                        {
                            entries.Add(new UnsupportedBacklogEntry(queue, priority, g.Contract, g.Oldest, g.Count, truncated,
                                Exists(subscribed: false) ? "advertised_but_not_subscribed" : "no_live_worker_advertises_tuple"));
                        }
                    }
                }
            }

            return new UnsupportedBacklog(observed, liveWorkers, samplePerLane, entries);
        }, ct);
    }

    public Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(string application, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 100);
        return runner.ReadAsync<IReadOnlyList<ExpiredLease>>(conn => !CheckGate(conn) ? [] : Rows(Cmd(conn, application, """
            SELECT job_id, current_attempt_id, ownership_token, state, lease_expires_at FROM runwick_jobs
            WHERE application = @application AND state IN ('reserved', 'running') AND lease_expires_at <= @now
            ORDER BY lease_expires_at, job_id LIMIT @limit
            """).P("now", time.GetUtcNow()).P("limit", limit), r => new ExpiredLease(r.Uuid(0), r.Uuid(1), r.Uuid(2), r.GetString(3), r.Time(4))), ct);
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
        var subscriptions = new JsonArray([.. presence.SubscribedQueues.Order(StringComparer.Ordinal).Select(q => (JsonNode?)q)]);
        var pools = new JsonObject();
        foreach (var key in presence.PoolTargets.Keys.Order(StringComparer.Ordinal))
        {
            pools[key] = presence.PoolTargets[key];
        }

        var queues = presence.SubscribedQueues.Distinct().Order(StringComparer.Ordinal).ToArray();
        var reply = await runner.CallAsync(
            Op(application, Guid.CreateVersion7(), "PublishPresence", time.GetUtcNow() + TimeSpan.FromSeconds(10), null, SqLane.Background),
            tx =>
            {
                var expires = tx.Query(tx.Sql("""
                    INSERT INTO runwick_worker_instances AS w (application, instance_id, process_id, host_name, build_id, status,
                        capability_revision, presence_sequence, subscriptions, pool_targets, started_at, heartbeat_at, expires_at, detail)
                    VALUES (@application, @id, @pid, @host, @build, @status, @revision, @sequence, @subscriptions, @pools, @now, @now, @expires, @detail)
                    ON CONFLICT (application, instance_id) DO UPDATE
                    SET status = excluded.status, capability_revision = excluded.capability_revision, presence_sequence = excluded.presence_sequence,
                        subscriptions = excluded.subscriptions, pool_targets = excluded.pool_targets, heartbeat_at = excluded.heartbeat_at,
                        expires_at = excluded.expires_at, detail = excluded.detail
                    WHERE w.presence_sequence < excluded.presence_sequence
                    RETURNING expires_at
                    """).P("id", presence.InstanceId).P("pid", presence.ProcessId).P("host", presence.HostName).P("build", presence.BuildId)
                    .P("status", presence.Status).P("revision", presence.CapabilityRevision).P("sequence", presence.PresenceSequence)
                    .P("subscriptions", subscriptions).P("pools", pools).P("now", tx.Now).P("expires", SqTime.Floor(tx.Now + presence.Expiry))
                    .P("detail", presence.DetailText()),
                    r => r.Time(0), mutation: true);
                if (expires.Count == 0)
                {
                    return SqRow.NoEffect("VersionConflict", "presence_sequence_stale", tx.Now);
                }

                foreach (var c in capabilities)
                {
                    tx.Exec(tx.Sql("""
                        INSERT INTO runwick_worker_capabilities (application, instance_id, capability_revision, job_name, payload_version,
                            serializer_profile_id, serializer_profile_version)
                        VALUES (@application, @id, @revision, @name, @version, @profile_id, @profile_version) ON CONFLICT DO NOTHING
                        """).P("id", presence.InstanceId).P("revision", presence.CapabilityRevision).P("name", c.Name).P("version", c.PayloadVersion)
                        .P("profile_id", c.SerializerProfileId).P("profile_version", c.SerializerProfileVersion));
                }

                foreach (var q in queues)
                {
                    tx.Exec(tx.Sql("INSERT INTO runwick_worker_subscriptions (application, instance_id, queue) VALUES (@application, @id, @queue) ON CONFLICT DO NOTHING")
                        .P("id", presence.InstanceId).P("queue", q));
                }

                return SqRow.Applied("Applied", null, tx.Now, new JsonObject { ["expiresAt"] = SqTime.Ms(expires[0]) });
            },
            row =>
            {
                if (row.HasValue)
                {
                    ack = new PresenceAck(SqTime.FromMs(row.Result!["expiresAt"]!.GetValue<long>()), true);
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
