using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    const string WindowKind = "window";
    const string ActiveKind = "active";

    public Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        runner.ExecuteAsync(DispatchOp(command), DispatchBody(command), DispatchReplay, ct);

    public Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        runner.ExecuteAsync(DispatchManyOp(command), DispatchManyBody(command), DispatchManyReplay, ct);

    /// <summary>Dispatch/1 on a caller-owned transaction (01 §15, 04 §6.1).</summary>
    public Task<StoreReply<DispatchResult>> StageDispatchAsync(NpgsqlTransaction transaction, StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        runner.StageAsync(transaction, DispatchOp(command), DispatchBody(command), DispatchReplay, ct);

    /// <summary>DispatchMany/1 on a caller-owned transaction.</summary>
    public Task<StoreReply<DispatchBatchResult>> StageDispatchManyAsync(NpgsqlTransaction transaction, StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        runner.StageAsync(transaction, DispatchManyOp(command), DispatchManyBody(command), DispatchManyReplay, ct);

    PgOp DispatchOp(StoreCommand<DispatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.Dispatch, command.NotAfter, command.Body.CommandFingerprint, PgLane.Producer, command.Body.StoreId);

    PgOp DispatchManyOp(StoreCommand<DispatchBatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.DispatchMany, command.NotAfter, command.Body.CommandFingerprint, PgLane.Producer, command.Body.StoreId);

    Func<PgTx, Task<PgOutcome<DispatchResult>>> DispatchBody(StoreCommand<DispatchRequest> command) =>
        async tx =>
        {
            var outcome = await DispatchCoreAsync(tx, [command.Body.Item], continueOnConflict: false).ConfigureAwait(false);
            if (outcome.Rejection is { } rejection)
            {
                return new PgOutcome<DispatchResult>.Rollback(StoreReply<DispatchResult>.NoEffect(rejection.Status, rejection.Error, tx.Now));
            }

            var result = outcome.Items[0].Result!;
            return new PgOutcome<DispatchResult>.Commit(StoreReply<DispatchResult>.Applied(result, tx.Now), ReceiptCodec.Dispatch(result));
        };

    Func<PgTx, Task<PgOutcome<DispatchBatchResult>>> DispatchManyBody(StoreCommand<DispatchBatchRequest> command) =>
        async tx =>
        {
            var outcome = await DispatchCoreAsync(tx, command.Body.Items, command.Body.ContinueOnConflict).ConfigureAwait(false);
            if (outcome.Rejection is { } rejection)
            {
                return new PgOutcome<DispatchBatchResult>.Rollback(StoreReply<DispatchBatchResult>.NoEffect(rejection.Status, rejection.Error, tx.Now));
            }

            var result = new DispatchBatchResult(outcome.Items);
            return new PgOutcome<DispatchBatchResult>.Commit(StoreReply<DispatchBatchResult>.Applied(result, tx.Now), ReceiptCodec.DispatchMany(result));
        };

    static Task<StoreReply<DispatchResult>> DispatchReplay(PgTx tx, ReceiptRow row) =>
        Task.FromResult(StoreReply<DispatchResult>.Applied(ReceiptCodec.ReadDispatch(row.Result!), row.DecidedAt!.Value, replayed: true));

    static Task<StoreReply<DispatchBatchResult>> DispatchManyReplay(PgTx tx, ReceiptRow row) =>
        Task.FromResult(StoreReply<DispatchBatchResult>.Applied(ReceiptCodec.ReadDispatchMany(row.Result!), row.DecidedAt!.Value, replayed: true));

    sealed class KeyState(Guid? bound, byte[]? business, byte[]? policy, int? version, DateTimeOffset? expires)
    {
        public Guid? Bound { get; set; } = bound;
        public byte[]? Business { get; set; } = business;
        public byte[]? Policy { get; set; } = policy;
        public int? Version { get; set; } = version;
        public DateTimeOffset? Expires { get; set; } = expires;
        public bool Dirty { get; set; }
    }

    sealed record DispatchOutcome(IReadOnlyList<DispatchBatchItemResult> Items, (StoreStatus Status, StoreError Error)? Rejection);

    /// <summary>Shared Dispatch/1 and DispatchMany/1 body (04 §7.2 steps 2–10).</summary>
    async Task<DispatchOutcome> DispatchCoreAsync(PgTx tx, IReadOnlyList<DispatchItem> items, bool continueOnConflict,
        string createdReason = "initial")
    {
        static DispatchOutcome Reject(StoreStatus status, string code, Guid? conflicting = null, string? detail = null) =>
            new([], (status, new StoreError(code, ConflictingJobId: conflicting, Detail: detail)));

        // Validation bounds (03 I-6) before any write.
        foreach (var item in items)
        {
            var p = item.Intent.Policy;
            if (p.MaxExecutions is < 1 or > 100 || p.MaxConsecutiveUnstartedAbandonments is < 1 or > 100
                || p.ExecutionTimeoutMs is < 1 or > 2_592_000_000 || item.Scopes.Count > 8 || item.Intent.RateScopes.Count > 8
                || item.Rates.Count != item.Intent.RateScopes.Count
                || item.Rates.Any(r => r.Limit is < 1 or > 100_000 || r.WindowMs is < 1_000 or > 86_400_000)
                || !ValidRetry(p.Retry) || !ValidRetry(p.UnstartedRetry))
            {
                return Reject(StoreStatus.InvalidRequest, "invalid_request", detail: "policy or scope bounds");
            }
        }

        // Rate scopes are refused until the application activated rate_admission, so no binary without the Begin rate
        // step can ever own a rate-limited job (10 §6.1).
        if (items.Any(i => i.Rates.Count > 0) && !await RateAdmissionActiveAsync(tx).ConfigureAwait(false))
        {
            return Reject(StoreStatus.UnsupportedVersion, "rate_admission_not_active");
        }

        // 2. Queue registration (plain read).
        var queues = items.Select(i => i.Intent.Queue).Distinct().ToArray();
        using (var q = tx.Sql("SELECT queue FROM runwick.queue_controls WHERE application = @application AND queue = ANY (@queues::text[])")
            .Arr("queues", queues, NpgsqlDbType.Text))
        {
            var registered = (await tx.QueryAsync(q, r => r.GetString(0)).ConfigureAwait(false)).ToHashSet(StringComparer.Ordinal);
            if (queues.Any(x => !registered.Contains(x)))
            {
                return Reject(StoreStatus.InvalidRequest, "queue_not_registered");
            }
        }

        // 3. Key rows, sorted once over the whole batch.
        var keyIds = items.SelectMany(i => new[]
            {
                i.Intent.ActiveKey is { } a ? (i.Intent.JobName, ActiveKind, a) : default,
                i.Intent.WindowKey is { } w ? (i.Intent.JobName, WindowKind, w) : default,
            })
            .Where(k => k.JobName is not null).Distinct()
            .OrderBy(k => k.JobName, Internal.Utf8Order.Instance).ThenBy(k => k.Item2, Internal.Utf8Order.Instance).ThenBy(k => k.Item3, Internal.Utf8Order.Instance)
            .ToList();
        var keys = new Dictionary<(string, string, string), KeyState>();
        if (keyIds.Count > 0)
        {
            var names = keyIds.Select(k => k.JobName).ToArray();
            var kinds = keyIds.Select(k => k.Item2).ToArray();
            var values = keyIds.Select(k => k.Item3).ToArray();
            using (var insert = tx.Sql("""
                INSERT INTO runwick.dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
                SELECT @application, k.job_name, k.key_kind, k.key, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
                FROM unnest(@names::text[], @kinds::text[], @keys::text[]) AS k(job_name, key_kind, key)
                ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
                ON CONFLICT DO NOTHING
                """).Arr("names", names, NpgsqlDbType.Text).Arr("kinds", kinds, NpgsqlDbType.Text).Arr("keys", values, NpgsqlDbType.Text))
            {
                await tx.ExecAsync(insert).ConfigureAwait(false);
            }

            using var lockKeys = tx.Sql("""
                SELECT d.job_name, d.key_kind, d.key, d.bound_job_id, d.business_fingerprint, d.policy_fingerprint,
                       d.fingerprint_version, d.expires_at
                FROM runwick.dispatch_keys AS d
                JOIN unnest(@names::text[], @kinds::text[], @keys::text[]) AS k(job_name, key_kind, key)
                  ON d.job_name = k.job_name AND d.key_kind = k.key_kind AND d.key = k.key
                WHERE d.application = @application
                ORDER BY d.job_name, d.key_kind, d.key
                FOR UPDATE OF d
                """).Arr("names", names, NpgsqlDbType.Text).Arr("kinds", kinds, NpgsqlDbType.Text).Arr("keys", values, NpgsqlDbType.Text);
            foreach (var row in await tx.QueryAsync(lockKeys, r => (
                Id: (r.GetString(0), r.GetString(1), r.GetString(2)),
                State: new KeyState(r.IsDBNull(3) ? null : r.GetGuid(3), r.IsDBNull(4) ? null : (byte[])r[4], r.IsDBNull(5) ? null : (byte[])r[5],
                    r.IsDBNull(6) ? null : r.GetInt32(6), PgTime.ReadNullable(r, 7)))).ConfigureAwait(false))
            {
                keys[row.Id] = row.State;
            }
        }

        // 4. Decide.
        if (!await tx.DecideAsync().ConfigureAwait(false))
        {
            return Reject(StoreStatus.CommandExpired, "command_expired");
        }

        var now = tx.Now;

        // 5. Resolve items in ordinal order against the in-memory key state.
        var results = new List<DispatchBatchItemResult>(items.Count);
        var created = new List<(int Ordinal, DispatchItem Item)>();
        var drift = new List<(int Ordinal, Guid JobId, DuplicateMatch Match)>();
        var conflicts = new List<(int Ordinal, string Code, Guid? Job)>();
        for (var ordinal = 0; ordinal < items.Count; ordinal++)
        {
            var item = items[ordinal];
            var intent = item.Intent;
            KeyState? active = intent.ActiveKey is { } ak ? keys[(intent.JobName, ActiveKind, ak)] : null;
            KeyState? window = intent.WindowKey is { } wk ? keys[(intent.JobName, WindowKind, wk)] : null;
            var windowLive = window is { Bound: not null } && window.Expires > now;
            if (windowLive)
            {
                if (window!.Version != 1)
                {
                    return Reject(StoreStatus.UnsupportedVersion, "fingerprint_version_unsupported");
                }

                if (!window.Business.AsSpan().SequenceEqual(item.Digests.Business))
                {
                    conflicts.Add((ordinal, "window_fingerprint_mismatch", window.Bound));
                    results.Add(new(ordinal, null, new StoreError("uniqueness_conflict", ConflictingJobId: window.Bound)));
                    continue;
                }

                if (active is { Bound: { } ab } && ab != window.Bound)
                {
                    conflicts.Add((ordinal, "uniqueness_composition_conflict", ab));
                    results.Add(new(ordinal, null, new StoreError("uniqueness_composition_conflict", ConflictingJobId: ab)));
                    continue;
                }

                var match = active is { Bound: { } same } && same == window.Bound ? DuplicateMatch.ActiveAndWindow : DuplicateMatch.Window;
                var policyDrift = !window.Policy.AsSpan().SequenceEqual(item.Digests.Policy);
                results.Add(new(ordinal, new DispatchResult(window.Bound!.Value, DispatchDisposition.Reused, match, policyDrift), null));
                if (policyDrift)
                {
                    drift.Add((ordinal, window.Bound!.Value, match));
                }

                continue;
            }

            if (active is { Bound: { } existing })
            {
                if (window is not null)
                {
                    Bind(window, existing, item, now);
                }

                results.Add(new(ordinal, new DispatchResult(existing, DispatchDisposition.Reused, DuplicateMatch.Active, false), null));
                continue;
            }

            var jobId = intent.ProposedJobId;
            if (active is not null)
            {
                active.Bound = jobId;
                active.Dirty = true;
            }

            if (window is not null)
            {
                Bind(window, jobId, item, now);
            }

            created.Add((ordinal, item));
            results.Add(new(ordinal, new DispatchResult(jobId, DispatchDisposition.Created, DuplicateMatch.None, false), null));
        }

        if (conflicts.Count > 0 && !continueOnConflict)
        {
            var first = conflicts[0];
            var detail = items.Count > 1
                ? new JsonArray([.. conflicts.Select(c => (JsonNode)new JsonObject
                {
                    ["ordinal"] = c.Ordinal,
                    ["code"] = c.Code == "window_fingerprint_mismatch" ? "uniqueness_conflict" : c.Code,
                    ["conflictingJobId"] = c.Job?.ToString("D"),
                })]).ToJsonString()
                : null;
            return Reject(StoreStatus.UniquenessConflict, first.Code, first.Job, detail);
        }

        // 6. Key writes (rows already locked), in sorted order.
        foreach (var id in keyIds)
        {
            var state = keys[id];
            if (!state.Dirty)
            {
                continue;
            }

            using var update = tx.Sql("""
                UPDATE runwick.dispatch_keys
                SET bound_job_id = @bound, business_fingerprint = @business, policy_fingerprint = @policy,
                    fingerprint_version = @version, expires_at = @expires, updated_at = @now
                WHERE application = @application AND job_name = @job_name AND key_kind = @kind AND key = @key
                """).P("bound", state.Bound, NpgsqlDbType.Uuid).P("business", state.Business, NpgsqlDbType.Bytea)
                    .P("policy", state.Policy, NpgsqlDbType.Bytea).P("version", state.Version, NpgsqlDbType.Integer)
                    .P("expires", state.Expires).P("now", now).P("job_name", id.JobName).P("kind", id.Item2).P("key", id.Item3);
            await tx.ExecAsync(update).ConfigureAwait(false);
        }

        // 7. New jobs, payloads, generations in ascending job_id.
        if (created.Count > 0)
        {
            var ordered = created.OrderBy(c => c.Item.Intent.ProposedJobId, Internal.UuidOrder.Instance).ToList();
            var jobIds = ordered.Select(c => c.Item.Intent.ProposedJobId).ToArray();
            var intents = ordered.Select(c => c.Item.Intent).ToList();
            using (var jobs = tx.Sql("""
                INSERT INTO runwick.jobs (
                    application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
                    queue, priority, state, eligible_at, created_at, updated_at, active_unique_key)
                SELECT @application, n.job_id, n.job_name, n.payload_version, n.profile_id, n.profile_version,
                       n.queue, n.priority, 'pending', runwick.ms_ceil(GREATEST(COALESCE(n.not_before, @now), @now)),
                       @now, @now, n.active_key
                FROM unnest(@job_ids::uuid[], @job_names::text[], @payload_versions::integer[], @profile_ids::text[],
                            @profile_versions::integer[], @job_queues::text[], @priorities::text[],
                            @not_befores::timestamptz[], @active_keys::text[])
                     AS n(job_id, job_name, payload_version, profile_id, profile_version, queue, priority, not_before, active_key)
                ORDER BY n.job_id
                """).P("now", now).Arr("job_ids", jobIds, NpgsqlDbType.Uuid)
                    .Arr("job_names", intents.Select(i => i.JobName).ToArray(), NpgsqlDbType.Text)
                    .Arr("payload_versions", intents.Select(i => i.PayloadVersion).ToArray(), NpgsqlDbType.Integer)
                    .Arr("profile_ids", intents.Select(i => i.SerializerProfileId).ToArray(), NpgsqlDbType.Text)
                    .Arr("profile_versions", intents.Select(i => i.SerializerProfileVersion).ToArray(), NpgsqlDbType.Integer)
                    .Arr("job_queues", intents.Select(i => i.Queue).ToArray(), NpgsqlDbType.Text)
                    .Arr("priorities", intents.Select(i => i.Priority).ToArray(), NpgsqlDbType.Text)
                    .Arr("not_befores", intents.Select(i => i.NotBeforeMs is { } nb ? (DateTime?)PgTime.FromMs(nb).UtcDateTime : null).ToArray(), NpgsqlDbType.TimestampTz)
                    .Arr("active_keys", intents.Select(i => i.ActiveKey).ToArray(), NpgsqlDbType.Text))
            {
                await tx.ExecAsync(jobs).ConfigureAwait(false);
            }

            using (var payloads = tx.Sql("""
                INSERT INTO runwick.job_payloads (
                    application, job_id, payload_utf8, payload_encoding_version, serializer_profile_id, serializer_profile_version, headers)
                SELECT @application, p.job_id, p.payload, 1, p.profile_id, p.profile_version, p.headers
                FROM unnest(@job_ids::uuid[], @payloads::bytea[], @profile_ids::text[], @profile_versions::integer[], @headers::jsonb[])
                     AS p(job_id, payload, profile_id, profile_version, headers)
                ORDER BY p.job_id
                """).Arr("job_ids", jobIds, NpgsqlDbType.Uuid)
                    .Arr("payloads", intents.Select(i => i.PayloadUtf8).ToArray(), NpgsqlDbType.Bytea)
                    .Arr("profile_ids", intents.Select(i => i.SerializerProfileId).ToArray(), NpgsqlDbType.Text)
                    .Arr("profile_versions", intents.Select(i => i.SerializerProfileVersion).ToArray(), NpgsqlDbType.Integer)
                    .Arr("headers", ordered.Select(c => Headers(c.Item, tx.Now)).ToArray(), NpgsqlDbType.Jsonb))
            {
                await tx.ExecAsync(payloads).ConfigureAwait(false);
            }

            using (var generations = tx.Sql("""
                INSERT INTO runwick.job_generations (
                    application, job_id, generation, created_at, created_reason,
                    max_executions, max_unstarted_abandonments, execution_timeout_ms, retry_policy, unstarted_retry_policy)
                SELECT @application, g.job_id, 1, @now, @created_reason, g.max_executions, g.max_unstarted, g.timeout_ms, g.retry, g.unstarted_retry
                FROM unnest(@job_ids::uuid[], @max_executions::integer[], @max_unstarted::integer[], @timeouts::bigint[],
                            @retry::jsonb[], @unstarted::jsonb[])
                     AS g(job_id, max_executions, max_unstarted, timeout_ms, retry, unstarted_retry)
                ORDER BY g.job_id
                """).P("now", now).P("created_reason", createdReason).Arr("job_ids", jobIds, NpgsqlDbType.Uuid)
                    .Arr("max_executions", intents.Select(i => i.Policy.MaxExecutions).ToArray(), NpgsqlDbType.Integer)
                    .Arr("max_unstarted", intents.Select(i => i.Policy.MaxConsecutiveUnstartedAbandonments).ToArray(), NpgsqlDbType.Integer)
                    .Arr("timeouts", intents.Select(i => i.Policy.ExecutionTimeoutMs).ToArray(), NpgsqlDbType.Bigint)
                    .Arr("retry", intents.Select(i => PolicyJson(i.Policy.Retry).ToJsonString()).ToArray(), NpgsqlDbType.Jsonb)
                    .Arr("unstarted", intents.Select(i => PolicyJson(i.Policy.UnstartedRetry).ToJsonString()).ToArray(), NpgsqlDbType.Jsonb))
            {
                await tx.ExecAsync(generations).ConfigureAwait(false);
            }

            // 8. Scope provisioning, then requirements in ascending scope_id.
            var scopes = await ProvisionScopesAsync(tx, [.. ordered.SelectMany(c => c.Item.Scopes)]).ConfigureAwait(false);
            var requirements = ordered
                .SelectMany(c => c.Item.Scopes.Select(s => (Job: c.Item.Intent.ProposedJobId, Scope: scopes[(s.Kind, s.Key)].ScopeId)))
                .Distinct()
                .OrderBy(r => r.Scope, Internal.UuidOrder.Instance).ThenBy(r => r.Job, Internal.UuidOrder.Instance)
                .ToList();
            if (requirements.Count > 0)
            {
                using var req = tx.Sql("""
                    INSERT INTO runwick.job_concurrency_requirements (application, job_id, scope_id)
                    SELECT @application, r.job_id, r.scope_id
                    FROM unnest(@job_ids::uuid[], @scope_ids::uuid[]) AS r(job_id, scope_id)
                    ORDER BY r.scope_id, r.job_id
                    """).Arr("job_ids", requirements.Select(r => r.Job).ToArray(), NpgsqlDbType.Uuid)
                        .Arr("scope_ids", requirements.Select(r => r.Scope).ToArray(), NpgsqlDbType.Uuid);
                await tx.ExecAsync(req).ConfigureAwait(false);
            }

            await ProvisionRatesAsync(tx, [.. ordered.SelectMany(c => c.Item.Rates.Select(r => (c.Item.Intent.ProposedJobId, r)))]).ConfigureAwait(false);
        }

        // 9. Events.
        var events = created.Select(c => (Job: c.Item.Intent.ProposedJobId, Kind: "dispatched", Details: new JsonObject
            {
                ["generation"] = 1, ["queue"] = c.Item.Intent.Queue, ["priority"] = c.Item.Intent.Priority, ["ordinal"] = c.Ordinal,
            }))
            .Concat(drift.Select(d => (Job: d.JobId, Kind: "policy_drift", Details: new JsonObject { ["ordinal"] = d.Ordinal, ["match"] = d.Match.ToString() })))
            .ToList();
        if (events.Count > 0)
        {
            using var ev = tx.Sql("""
                INSERT INTO runwick.queue_events (application, job_id, command_id, event_kind, occurred_at, details)
                SELECT @application, e.job_id, @command_id, e.kind, @now, e.details
                FROM unnest(@job_ids::uuid[], @kinds::text[], @details::jsonb[]) AS e(job_id, kind, details)
                """).P("command_id", tx.Op.CommandId).P("now", now)
                    .Arr("job_ids", events.Select(e => e.Job).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("kinds", events.Select(e => e.Kind).ToArray(), NpgsqlDbType.Text)
                    .Arr("details", events.Select(e => e.Details.ToJsonString()).ToArray(), NpgsqlDbType.Jsonb);
            await tx.ExecAsync(ev).ConfigureAwait(false);
        }

        // 10. Final deadline guard: provisioning may have waited after the decision.
        if (await tx.ClockAsync().ConfigureAwait(false) >= tx.Op.NotAfter)
        {
            return Reject(StoreStatus.CommandExpired, "command_expired");
        }

        return new DispatchOutcome(results, null);
    }

    static bool ValidRetry(RetryPolicy r) =>
        r.InitialDelayMs is >= 1 and <= 86_400_000 && r.MaximumDelayMs >= r.InitialDelayMs && r.MaximumDelayMs <= 86_400_000;

    static void Bind(KeyState window, Guid jobId, DispatchItem item, DateTimeOffset now)
    {
        window.Bound = jobId;
        window.Business = item.Digests.Business;
        window.Policy = item.Digests.Policy;
        window.Version = Fingerprints.Version;
        window.Expires = PgTime.Floor(now + TimeSpan.FromMilliseconds(item.Intent.WindowMs!.Value));
        window.Dirty = true;
    }

    static string Headers(DispatchItem item, DateTimeOffset now)
    {
        var intent = item.Intent;
        static JsonObject Map(IReadOnlyDictionary<string, string> m)
        {
            var o = new JsonObject();
            foreach (var key in m.Keys.Order(Internal.Utf8Order.Instance))
            {
                o[key] = m[key];
            }

            return o;
        }

        var headers = new JsonObject { ["business"] = Map(intent.BusinessMetadata), ["diagnostic"] = Map(intent.DiagnosticMetadata) };
        if (item.Schedule is { } s)
        {
            if (s.ScheduledAt == default)
            {
                // A manual trigger: its occurrence is the decision time of the command that creates the job.
                s = new JobScheduleInfo(s.ScheduleName, now, now, now);
            }

            // The occurrence of a store-built scheduled job (13 §3.2): a third member beside the two metadata maps.
            headers["schedule"] = new JsonObject
            {
                ["key"] = s.ScheduleName, ["scheduledAt"] = PgTime.Ms(s.ScheduledAt),
                ["coveredFrom"] = PgTime.Ms(s.CoveredFrom), ["coveredUntil"] = PgTime.Ms(s.CoveredUntil),
            };
        }

        return headers.ToJsonString();
    }

    // ------------------------------------------------------------------ ResolveCommand/1

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) =>
        ResolveCoreAsync(command.Application, command.CommandId, command.Operation, command.Fingerprint, command.NotAfter, ct);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        ResolveCoreAsync(application, commandId, null, null, notAfterBound, ct);

    /// <summary>Joins the receipt row's lock queue, classifies (03 §8.6) and always rolls back.</summary>
    async Task<ReceiptResolution> ResolveCoreAsync(string application, Guid commandId, string? operation, byte[]? fingerprint, DateTimeOffset notAfter, CancellationToken ct)
    {
        ReceiptResolution? resolution = null;
        var op = new PgOp(application, commandId, "ResolveCommand", DateTimeOffset.MaxValue, null, PgLane.Producer, null);
        var reply = await runner.ExecuteAsync<ResolutionBox>(op, async tx =>
        {
            await tx.Fault(FaultPoint.DuringReceiptLookup).ConfigureAwait(false);
            ReceiptRow? found = null;
            foreach (var retentionClass in operation is null ? BothClasses : [Operations.RetentionClass(operation)])
            {
                found = await PgRunner.ReadReceiptAsync(tx, retentionClass).ConfigureAwait(false);
                if (found is not null)
                {
                    break;
                }
            }

            var now = await tx.ClockAsync().ConfigureAwait(false);
            if (found is not null)
            {
                resolution = (operation is not null && (found.Operation != operation || !found.Fingerprint.AsSpan().SequenceEqual(fingerprint)))
                    ? new ReceiptResolution.Conflict()
                    : found.Result is null
                        ? new ReceiptResolution.NotObserved()
                        : new ReceiptResolution.Recorded(found.Operation, found.OperationVersion, found.Status!, found.DecidedAt!.Value,
                            System.Text.Json.JsonDocument.Parse(found.Result.ToJsonString()).RootElement.Clone());
            }
            else
            {
                var floor = operation is null ? TimeSpan.FromHours(24) : Operations.RetentionFloor(operation);
                resolution = now < notAfter ? new ReceiptResolution.NotObserved()
                    : now < notAfter + floor ? new ReceiptResolution.NotApplied()
                    : new ReceiptResolution.ExpiredUnknown();
            }

            return new PgOutcome<ResolutionBox>.Rollback(StoreReply<ResolutionBox>.NoEffect(StoreStatus.NoWork, new StoreError("resolved"), now));
        }, null, ct).ConfigureAwait(false);

        return resolution ?? throw new StoreIncompatibleException($"Receipt resolution failed: {reply}");
    }

    static readonly string[] BothClasses = [Operations.DurableClass, Operations.OwnershipClass];

    sealed record ResolutionBox;
}
