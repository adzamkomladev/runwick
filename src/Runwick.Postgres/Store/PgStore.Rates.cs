using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Rate admission (10): scope provisioning at Dispatch, the all-or-none rate step of Begin/1 with committed deferral
/// (T20), EditRatePolicy/1 with the edit freeze, and ActivateCapabilities/1.
/// </summary>
internal sealed partial class PgStore : IRateControlStore
{
    /// <summary>Minimum defer interval and jitter bound (06 §1.11, capacity-defaults §3).</summary>
    static readonly TimeSpan MinimumDefer = TimeSpan.FromMilliseconds(250);

    const int MaxDeferJitterMs = 100;

    internal sealed record RateScopeRow(Guid Id, int Limit, long WindowMs, long Revision, DateTimeOffset? LastDecisionAt, DateTimeOffset? NotBefore)
    {
        public TimeSpan Window => TimeSpan.FromMilliseconds(WindowMs);
    }

    // ------------------------------------------------------------------ Dispatch (10 §3.1)

    /// <summary>True when the application has activated <c>rate_admission</c> (plain read; the gate row is share-locked by the runner).</summary>
    static async Task<bool> RateAdmissionActiveAsync(PgTx tx)
    {
        using var q = tx.Sql("""
            SELECT required_capabilities ? 'rate_admission' FROM runwick.application_controls WHERE application = @application
            """);
        var rows = await tx.QueryAsync(q, r => r.GetBoolean(0)).ConfigureAwait(false);
        return rows.Count == 1 && rows[0];
    }

    /// <summary>
    /// Seed-if-absent in (kind, key) order, then the requirement rows in ascending scope ID. A seed never changes an
    /// existing scope's policy (10 §3.1).
    /// </summary>
    static async Task ProvisionRatesAsync(PgTx tx, IReadOnlyList<(Guid JobId, RateRequirement Rate)> requirements)
    {
        if (requirements.Count == 0)
        {
            return;
        }

        var distinct = requirements.Select(r => r.Rate)
            .GroupBy(r => (r.Kind, r.Key)).Select(g => g.First())
            .OrderBy(r => r.Kind, Internal.Utf8Order.Instance).ThenBy(r => r.Key, Internal.Utf8Order.Instance).ToList();
        var kinds = distinct.Select(r => r.Kind).ToArray();
        var keys = distinct.Select(r => r.Key).ToArray();
        using (var insert = tx.Sql("""
            INSERT INTO runwick.rate_scopes (
                application, rate_scope_id, scope_kind, scope_key, limit_count, window_ms, created_at, updated_at)
            SELECT @application, s.id, s.kind, s.key, s.limit_count, s.window_ms,
                   runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
            FROM unnest(@ids::uuid[], @kinds::text[], @keys::text[], @limits::integer[], @windows::bigint[])
                 AS s(id, kind, key, limit_count, window_ms)
            ORDER BY s.kind COLLATE "C", s.key COLLATE "C"
            ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
            """).Arr("ids", distinct.Select(_ => Guid.CreateVersion7()).ToArray(), NpgsqlDbType.Uuid)
                .Arr("kinds", kinds, NpgsqlDbType.Text).Arr("keys", keys, NpgsqlDbType.Text)
                .Arr("limits", distinct.Select(r => r.Limit).ToArray(), NpgsqlDbType.Integer)
                .Arr("windows", distinct.Select(r => r.WindowMs).ToArray(), NpgsqlDbType.Bigint))
        {
            await tx.ExecAsync(insert).ConfigureAwait(false);
        }

        var ids = new Dictionary<(string, string), Guid>();
        using (var read = tx.Sql("""
            SELECT r.rate_scope_id, r.scope_kind, r.scope_key
            FROM runwick.rate_scopes AS r
            JOIN unnest(@kinds::text[], @keys::text[]) AS s(kind, key) ON r.scope_kind = s.kind AND r.scope_key = s.key
            WHERE r.application = @application
            """).Arr("kinds", kinds, NpgsqlDbType.Text).Arr("keys", keys, NpgsqlDbType.Text))
        {
            foreach (var (id, kind, key) in await tx.QueryAsync(read, r => (r.GetGuid(0), r.GetString(1), r.GetString(2))).ConfigureAwait(false))
            {
                ids[(kind, key)] = id;
            }
        }

        var rows = requirements
            .Select(r => (r.JobId, Scope: ids[(r.Rate.Kind, r.Rate.Key)]))
            .Distinct()
            .OrderBy(r => r.Scope, Internal.UuidOrder.Instance).ThenBy(r => r.JobId, Internal.UuidOrder.Instance)
            .ToList();
        using var req = tx.Sql("""
            INSERT INTO runwick.job_rate_requirements (application, job_id, rate_scope_id)
            SELECT @application, r.job_id, r.scope_id
            FROM unnest(@job_ids::uuid[], @scope_ids::uuid[]) AS r(job_id, scope_id)
            ORDER BY r.scope_id, r.job_id
            """).Arr("job_ids", rows.Select(r => r.JobId).ToArray(), NpgsqlDbType.Uuid)
                .Arr("scope_ids", rows.Select(r => r.Scope).ToArray(), NpgsqlDbType.Uuid);
        await tx.ExecAsync(req).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ Begin rate step (10 §4)

    // The rate step of Begin/1 (10 §4: R, logical time t, the window counts, the charge or the committed deferral with
    // its hint) runs in runwick.begin_v1 (PF-05).

    // ------------------------------------------------------------------ EditRatePolicy/1 (10 §6.2)

    public async Task<StoreReply<RatePolicy>> EditRatePolicyAsync(StoreCommand<RatePolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (r.ScopeKind is not ("queue" or "job-type" or "resource") ? "scopeKind" : null)
            ?? (string.IsNullOrEmpty(r.ScopeKey) || System.Text.Encoding.UTF8.GetByteCount(r.ScopeKey) > 512 ? "scopeKey" : null)
            ?? (r.Limit is < 1 or > 100_000 ? "limit" : null)
            ?? (r.WindowMs is < 1_000 or > 86_400_000 ? "windowMs" : null);
        if (invalid is not null)
        {
            return StoreReply<RatePolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditRatePolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.RatePolicy(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditRatePolicy, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                using var q = tx.Sql("""
                    SELECT rate_scope_id, limit_count, window_ms, policy_revision, last_decision_at, not_before
                    FROM runwick.rate_scopes
                    WHERE application = @application AND scope_kind = @kind AND scope_key = @key
                    FOR UPDATE
                    """).P("kind", r.ScopeKind).P("key", r.ScopeKey);
                var rows = await tx.QueryAsync(q, x => new RateScopeRow(x.GetGuid(0), x.GetInt32(1), x.GetInt64(2), x.GetInt64(3),
                    PgTime.ReadNullable(x, 4), PgTime.ReadNullable(x, 5))).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<RatePolicy>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (rows.Count == 0)
                {
                    return Rollback<RatePolicy>(tx, StoreStatus.NotFound, "scope_not_registered");
                }

                var s = rows[0];
                if (s.Revision != r.ExpectedPolicyRevision)
                {
                    return new PgOutcome<RatePolicy>.Rollback(StoreReply<RatePolicy>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: s.Revision, Detail: $"actual={s.Revision}"), tx.Now));
                }

                if (s.Limit == r.Limit && s.WindowMs == r.WindowMs)
                {
                    // An identical policy never restarts the freeze (capacity-defaults §3).
                    return Rollback<RatePolicy>(tx, StoreStatus.InvalidState, "no_change");
                }

                // Freeze new authorizations for one full new window in the scope's logical time (10 §6.3).
                var t = s.LastDecisionAt is { } last && last > tx.Now ? last : tx.Now;
                var notBefore = t + TimeSpan.FromMilliseconds(r.WindowMs);
                using (var u = tx.Sql("""
                    UPDATE runwick.rate_scopes
                    SET limit_count = @limit, window_ms = @window_ms, policy_revision = policy_revision + 1,
                        last_decision_at = @t, not_before = @not_before, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND rate_scope_id = @id
                    """).P("limit", r.Limit).P("window_ms", r.WindowMs).P("t", t).P("not_before", notBefore).P("now", tx.Now)
                        .P("subject", r.Actor.Subject).P("id", s.Id))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                int retained;
                using (var c = tx.Sql("""
                    SELECT count(*)::int FROM runwick.rate_grants
                    WHERE application = @application AND rate_scope_id = @id AND authorized_at > @t - @window_ms * interval '1 millisecond'
                    """).P("id", s.Id).P("t", t).P("window_ms", r.WindowMs))
                {
                    retained = (await tx.QueryAsync(c, x => x.GetInt32(0)).ConfigureAwait(false))[0];
                }

                var policy = new RatePolicy(s.Id, r.ScopeKind, r.ScopeKey, r.Limit, r.WindowMs, s.Revision + 1, notBefore, retained);
                await ControlEventAsync(tx, null, null, "rate_policy_edited", r.Actor, new JsonObject
                {
                    ["scopeKind"] = r.ScopeKind, ["scopeKey"] = r.ScopeKey, ["limit"] = r.Limit, ["windowMs"] = r.WindowMs,
                    ["policyRevision"] = policy.PolicyRevision, ["notBefore"] = PgTime.Ms(notBefore),
                }).ConfigureAwait(false);
                await AuditAsync(tx, "rate_scope", r.ScopeKind + "/" + r.ScopeKey, r.Actor, s.Revision, policy.PolicyRevision, new JsonObject
                {
                    ["limit"] = r.Limit, ["windowMs"] = r.WindowMs, ["previousLimit"] = s.Limit, ["previousWindowMs"] = s.WindowMs,
                }).ConfigureAwait(false);
                return new PgOutcome<RatePolicy>.Commit(StoreReply<RatePolicy>.Applied(policy, tx.Now), ReceiptCodec.Rate(policy));
            },
            (tx, row) => Task.FromResult(StoreReply<RatePolicy>.Applied(ReceiptCodec.ReadRate(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ ActivateCapabilities/1 (10 §6.1, 04 §8.6)

    public async Task<StoreReply<ActivatedCapabilities>> ActivateCapabilitiesAsync(StoreCommand<ActivateCapabilitiesRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (r.Capabilities.Count is 0 or > 16 || r.Capabilities.Any(c => !global::Runwick.Hosting.RunwickRuntime.KnownCapabilities.TryGetValue(c.Key, out var v) || c.Value < 1 || c.Value > v)
                ? "capabilities" : null);
        if (invalid is not null)
        {
            return StoreReply<ActivatedCapabilities>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ActivateCapabilities, command.CommandId, command.NotAfter,
            CommandEnvelopes.ActivateCapabilities(r));
        return await runner.ExecuteAsync(
            Op(command.Application, command.CommandId, Operations.ActivateCapabilities, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                using var q = tx.Sql("""
                    SELECT revision, required_capabilities::text FROM runwick.application_controls
                    WHERE application = @application
                    FOR UPDATE
                    """);
                var rows = await tx.QueryAsync(q, x => (Revision: x.GetInt64(0), Required: ParseCapabilities(x.GetString(1)))).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ActivatedCapabilities>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (rows.Count == 0)
                {
                    return Rollback<ActivatedCapabilities>(tx, StoreStatus.NotFound, "application_not_registered");
                }

                var (revision, required) = rows[0];
                var merged = new Dictionary<string, int>(required, StringComparer.Ordinal);
                foreach (var (name, version) in r.Capabilities)
                {
                    merged[name] = Math.Max(version, merged.GetValueOrDefault(name));
                }

                if (merged.Count == required.Count && merged.All(m => required.TryGetValue(m.Key, out var v) && v == m.Value))
                {
                    return Rollback<ActivatedCapabilities>(tx, StoreStatus.InvalidState, "no_change");
                }

                var json = new JsonObject();
                foreach (var (name, version) in merged.OrderBy(m => m.Key, StringComparer.Ordinal))
                {
                    json[name] = version;
                }

                using (var u = tx.Sql("""
                    UPDATE runwick.application_controls
                    SET required_capabilities = @required, revision = revision + 1, updated_at = @now, actor_subject = @subject
                    WHERE application = @application
                    """).Json("required", json).P("now", tx.Now).P("subject", r.Actor.Subject))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var activated = new ActivatedCapabilities(revision + 1, merged);
                await ControlEventAsync(tx, null, null, "capabilities_activated", r.Actor,
                    new JsonObject { ["requiredCapabilities"] = json.DeepClone(), ["revision"] = activated.Revision }).ConfigureAwait(false);
                await AuditAsync(tx, "application", command.Application, r.Actor, revision, activated.Revision,
                    new JsonObject { ["requiredCapabilities"] = json.DeepClone() }).ConfigureAwait(false);
                return new PgOutcome<ActivatedCapabilities>.Commit(StoreReply<ActivatedCapabilities>.Applied(activated, tx.Now), ReceiptCodec.Activated(activated));
            },
            (tx, row) => Task.FromResult(StoreReply<ActivatedCapabilities>.Applied(ReceiptCodec.ReadActivated(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }
}
