using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Scaler support (11 §4–§6) as PgStore.Scaling: EstimateDemand on the read path (one snapshot, 20 §4.2), SetPoolOverride/1 and
/// RecordScalingEvent on the write lock.
/// </summary>
internal sealed partial class SqliteStore : IScalingStore
{
    /// <summary>Due rows counted per lane before the count is reported as a lower bound (06 §1.13).</summary>
    const int DemandCountCap = 2_000;

    /// <summary>Longest override lifetime (06 §1.13).</summary>
    static readonly TimeSpan MaxOverrideLifetime = TimeSpan.FromDays(7);

    // ------------------------------------------------------------------ EstimateDemand/1 (11 §4)

    public Task<DemandEstimate> EstimateDemandAsync(string application, Guid instanceId, IReadOnlyList<string> queues, IReadOnlyList<JobContract> contracts,
        CancellationToken ct)
    {
        var tuples = contracts.Distinct().ToArray();
        if (queues.Count is < 1 or > 128 || tuples.Length is < 1 or > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(queues), "Estimate takes 1–128 queues and 1–512 contracts.");
        }

        var supported = tuples.ToHashSet();
        return runner.ReadAsync(conn =>
        {
            if (!CheckGate(conn))
            {
                throw new StoreUnavailableException("The store is not accepting work (maintenance); demand is unknown.");
            }

            var observed = Now();
            var live = Rows(Cmd(conn, application, """
                SELECT instance_id FROM runwick_worker_instances
                WHERE application = @application AND expires_at > @now AND status IN ('starting', 'ready', 'degraded')
                ORDER BY instance_id
                """).P("now", observed), r => r.Uuid(0));

            // Per lane, up to the cap of due rows in jobs_ready order, grouped by supported tuple.
            var truncated = false;
            var due = new Dictionary<(string Queue, JobContract Contract), long>();
            foreach (var queue in queues.Distinct(StringComparer.Ordinal))
            {
                foreach (var priority in Priorities)
                {
                    var sample = Rows(Cmd(conn, application, """
                        SELECT job_name, payload_version, serializer_profile_id, serializer_profile_version
                        FROM runwick_jobs
                        WHERE application = @application AND queue = @queue AND priority = @priority
                          AND state IN ('pending', 'retrying') AND eligible_at <= @now
                        ORDER BY eligible_at, job_id
                        LIMIT @cap
                        """).P("queue", queue).P("priority", priority).P("now", observed).P("cap", DemandCountCap),
                        r => new JobContract(r.GetString(0), r.GetInt32(1), r.GetString(2), r.GetInt32(3)));
                    truncated |= sample.Count == DemandCountCap;
                    foreach (var c in sample.Where(supported.Contains))
                    {
                        due[(queue, c)] = due.GetValueOrDefault((queue, c)) + 1;
                    }
                }
            }

            var rows = due.Select(d => new DemandRow(d.Key.Queue, d.Key.Contract, d.Value, (int)Rows(Cmd(conn, application, """
                SELECT count(*)
                FROM runwick_worker_instances AS w
                JOIN runwick_worker_capabilities AS c
                  ON c.application = w.application AND c.instance_id = w.instance_id AND c.capability_revision = w.capability_revision
                JOIN runwick_worker_subscriptions AS s
                  ON s.application = w.application AND s.instance_id = w.instance_id AND s.queue = @queue
                WHERE w.application = @application AND w.expires_at > @now AND w.status IN ('starting', 'ready', 'degraded')
                  AND c.job_name = @name AND c.payload_version = @version
                  AND c.serializer_profile_id = @profile_id AND c.serializer_profile_version = @profile_version
                """).P("queue", d.Key.Queue).P("now", observed).P("name", d.Key.Contract.Name).P("version", d.Key.Contract.PayloadVersion)
                .P("profile_id", d.Key.Contract.SerializerProfileId).P("profile_version", d.Key.Contract.SerializerProfileVersion),
                r => r.GetInt64(0))[0])).ToList();

            var overrides = Rows(Cmd(conn, application, """
                SELECT pool_name, target FROM runwick_pool_controls
                WHERE application = @application AND instance_id = @instance_id AND target IS NOT NULL
                  AND (expires_at IS NULL OR expires_at > @now)
                """).P("instance_id", instanceId).P("now", observed), r => (r.GetString(0), r.GetInt32(1)))
                .ToDictionary(o => o.Item1, o => o.Item2, StringComparer.Ordinal);

            return new DemandEstimate(observed, rows, truncated, live.Count, MembershipDigest(live), overrides);
        }, ct);
    }

    /// <summary>
    /// A digest of the live membership: 0 when there is none. Compared only with earlier digests of the same store, so it
    /// need not equal PostgreSQL's hashtextextended value.
    /// </summary>
    static long MembershipDigest(List<Guid> live)
    {
        if (live.Count == 0)
        {
            return 0;
        }

        var hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(string.Join(',', live.Select(g => g.ToString("D")))));
        return BitConverter.ToInt64(hash, 0) is var d && d != 0 ? d : 1;
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
            Op(application, Guid.CreateVersion7(), "RecordScalingEvent", notAfter, null, SqLane.Background),
            tx =>
            {
                if (!tx.Decide())
                {
                    return Rollback<ScalingBox>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                Expect(tx.Exec(tx.Sql("""
                    INSERT INTO runwick_scaling_events (
                        application, event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason,
                        snapshot_at, membership_digest, created_at)
                    VALUES (@application, @event_id, @instance_id, @pool, @old, @new, @desired, @candidate, @reason,
                            @snapshot_at, @digest, @now)
                    """).P("event_id", Guid.CreateVersion7()).P("instance_id", record.InstanceId).P("pool", record.Pool)
                    .P("old", record.OldTarget).P("new", record.NewTarget).P("desired", record.Desired).P("candidate", record.Candidate)
                    .P("reason", record.Reason).P("snapshot_at", record.SnapshotAt is { } at ? SqTime.Floor(at) : null)
                    .P("digest", record.MembershipDigest).P("now", tx.Now)) == 1);
                return new SqOutcome<ScalingBox>.Commit(StoreReply<ScalingBox>.Applied(new ScalingBox(), tx.Now), null);
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.SetPoolOverride, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var rows = tx.Query(tx.Sql("""
                    SELECT target, revision, expires_at FROM runwick_pool_controls
                    WHERE application = @application AND instance_id = @instance_id AND pool_name = @pool
                    """).P("instance_id", r.InstanceId).P("pool", r.Pool), x => (Target: x.IntN(0), Revision: x.GetInt64(1), ExpiresAt: x.TimeN(2)));
                if (!tx.Decide())
                {
                    return Rollback<PoolOverride>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                var actual = rows.Count == 0 ? 0 : rows[0].Revision;
                if (actual != r.ExpectedRevision)
                {
                    return RevisionConflict<PoolOverride>(tx, actual);
                }

                // An expired override reads as absent: setting the same target again renews it rather than no_change.
                var current = rows.Count == 0 || rows[0].ExpiresAt is { } x0 && x0 <= tx.Now ? (Target: (int?)null, ExpiresAt: (DateTimeOffset?)null)
                    : (rows[0].Target, rows[0].ExpiresAt);
                var expires = r.ExpiresAt is { } ex ? SqTime.Floor(ex) : (DateTimeOffset?)null;
                if (current.Target == r.Target && current.ExpiresAt == expires)
                {
                    return Rollback<PoolOverride>(tx, StoreStatus.InvalidState, "no_change");
                }

                var revision = actual + 1;
                Expect(tx.Exec(tx.Sql("""
                    INSERT INTO runwick_pool_controls (
                        application, instance_id, pool_name, target, revision, expires_at, reason, updated_at, updated_by)
                    VALUES (@application, @instance_id, @pool, @target, @revision, @expires_at, @reason, @now, @subject)
                    ON CONFLICT (application, instance_id, pool_name) DO UPDATE
                    SET target = excluded.target, revision = excluded.revision, expires_at = excluded.expires_at,
                        reason = excluded.reason, updated_at = excluded.updated_at, updated_by = excluded.updated_by
                    """).P("instance_id", r.InstanceId).P("pool", r.Pool).P("target", r.Target).P("revision", revision)
                    .P("expires_at", expires).P("reason", r.Actor.Reason).P("now", tx.Now).P("subject", r.Actor.Subject)) == 1);

                var result = new PoolOverride(r.InstanceId, r.Pool, r.Target, revision, expires);
                var details = new JsonObject
                {
                    ["instanceId"] = r.InstanceId.ToString("D"), ["pool"] = r.Pool, ["target"] = r.Target,
                    ["expiresAt"] = expires is { } x1 ? SqTime.Ms(x1) : null, ["revision"] = revision,
                };
                ControlEvent(tx, null, null, "pool_override_set", r.Actor, details);
                Audit(tx, "pool", r.InstanceId.ToString("D") + "/" + r.Pool, r.Actor, actual, revision, (JsonObject)details.DeepClone());
                return new SqOutcome<PoolOverride>.Commit(StoreReply<PoolOverride>.Applied(result, tx.Now), ReceiptCodec.Override(result));
            },
            (tx, row) => StoreReply<PoolOverride>.Applied(ReceiptCodec.ReadOverride(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }
}
