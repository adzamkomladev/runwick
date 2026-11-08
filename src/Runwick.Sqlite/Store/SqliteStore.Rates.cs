using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// EditRatePolicy/1 with the edit freeze and ActivateCapabilities/1 (10 §6), PgStore.Rates's steps on the write lock. The rate
/// step of Begin/1 and its T20 deferral are in SqliteStore.Ownership (RateStep).
/// </summary>
internal sealed partial class SqliteStore : IRateControlStore
{
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditRatePolicy, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var rows = tx.Query(tx.Sql("""
                    SELECT rate_scope_id, limit_count, window_ms, policy_revision, last_decision_at
                    FROM runwick_rate_scopes
                    WHERE application = @application AND scope_kind = @kind AND scope_key = @key
                    """).P("kind", r.ScopeKind).P("key", r.ScopeKey),
                    x => (Id: x.Uuid(0), Limit: x.GetInt32(1), WindowMs: x.GetInt64(2), Revision: x.GetInt64(3), Last: x.TimeN(4)));
                if (!tx.Decide())
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
                    return RevisionConflict<RatePolicy>(tx, s.Revision);
                }

                if (s.Limit == r.Limit && s.WindowMs == r.WindowMs)
                {
                    // An identical policy never restarts the freeze (capacity-defaults §3).
                    return Rollback<RatePolicy>(tx, StoreStatus.InvalidState, "no_change");
                }

                // Freeze new authorizations for one full new window in the scope's logical time (10 §6.3).
                var t = s.Last is { } last && last > tx.Now ? last : tx.Now;
                var notBefore = t + TimeSpan.FromMilliseconds(r.WindowMs);
                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_rate_scopes
                    SET limit_count = @limit, window_ms = @window_ms, policy_revision = policy_revision + 1,
                        last_decision_at = @t, not_before = @not_before, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND rate_scope_id = @id
                    """).P("limit", r.Limit).P("window_ms", r.WindowMs).P("t", t).P("not_before", notBefore).P("now", tx.Now)
                    .P("subject", r.Actor.Subject).P("id", s.Id)) == 1);

                var retained = (int)tx.Count(tx.Sql("""
                    SELECT count(*) FROM runwick_rate_grants
                    WHERE application = @application AND rate_scope_id = @id AND authorized_at > @since
                    """).P("id", s.Id).P("since", SqTime.Ms(t) - r.WindowMs));

                var policy = new RatePolicy(s.Id, r.ScopeKind, r.ScopeKey, r.Limit, r.WindowMs, s.Revision + 1, notBefore, retained);
                ControlEvent(tx, null, null, "rate_policy_edited", r.Actor, new JsonObject
                {
                    ["scopeKind"] = r.ScopeKind, ["scopeKey"] = r.ScopeKey, ["limit"] = r.Limit, ["windowMs"] = r.WindowMs,
                    ["policyRevision"] = policy.PolicyRevision, ["notBefore"] = SqTime.Ms(notBefore),
                });
                Audit(tx, "rate_scope", r.ScopeKind + "/" + r.ScopeKey, r.Actor, s.Revision, policy.PolicyRevision, new JsonObject
                {
                    ["limit"] = r.Limit, ["windowMs"] = r.WindowMs, ["previousLimit"] = s.Limit, ["previousWindowMs"] = s.WindowMs,
                });
                return new SqOutcome<RatePolicy>.Commit(StoreReply<RatePolicy>.Applied(policy, tx.Now), ReceiptCodec.Rate(policy));
            },
            (tx, row) => StoreReply<RatePolicy>.Applied(ReceiptCodec.ReadRate(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }

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
            Op(command.Application, command.CommandId, Operations.ActivateCapabilities, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var rows = tx.Query(tx.Sql("SELECT revision, required_capabilities FROM runwick_application_controls WHERE application = @application"),
                    x => (Revision: x.GetInt64(0), Required: ParseCapabilities(x.GetString(1))));
                if (!tx.Decide())
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

                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_application_controls
                    SET required_capabilities = @required, revision = revision + 1, updated_at = @now, actor_subject = @subject
                    WHERE application = @application
                    """).P("required", json).P("now", tx.Now).P("subject", r.Actor.Subject)) == 1);

                var activated = new ActivatedCapabilities(revision + 1, merged);
                ControlEvent(tx, null, null, "capabilities_activated", r.Actor,
                    new JsonObject { ["requiredCapabilities"] = json.DeepClone(), ["revision"] = activated.Revision });
                Audit(tx, "application", command.Application, r.Actor, revision, activated.Revision,
                    new JsonObject { ["requiredCapabilities"] = json.DeepClone() });
                return new SqOutcome<ActivatedCapabilities>.Commit(StoreReply<ActivatedCapabilities>.Applied(activated, tx.Now), ReceiptCodec.Activated(activated));
            },
            (tx, row) => StoreReply<ActivatedCapabilities>.Applied(ReceiptCodec.ReadActivated(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }
}
