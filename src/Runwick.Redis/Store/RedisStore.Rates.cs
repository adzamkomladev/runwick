using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Redis.Store;

/// <summary>
/// ActivateCapabilities/1 and EditRatePolicy/1 with the edit freeze (10 §6), as PgStore.Rates: shape checks here, the decision in
/// <c>activate_capabilities.lua</c> and <c>edit_rate_policy.lua</c>. The rate step of Begin/1 is in jobs.lua (rate_step).
/// </summary>
internal sealed partial class RedisStore : IRateControlStore
{
    public async Task<StoreReply<ActivatedCapabilities>> ActivateCapabilitiesAsync(StoreCommand<ActivateCapabilitiesRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = RateValidateActor(r.Actor)
            ?? (r.Capabilities.Count is 0 or > 16 || r.Capabilities.Any(c => !Hosting.RunwickRuntime.KnownCapabilities.TryGetValue(c.Key, out var v) || c.Value < 1 || c.Value > v)
                ? "capabilities" : null);
        if (invalid is not null)
        {
            return StoreReply<ActivatedCapabilities>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ActivateCapabilities, command.CommandId, command.NotAfter,
            CommandEnvelopes.ActivateCapabilities(r));
        var caps = new JsonObject();
        foreach (var (name, version) in r.Capabilities.OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            caps[name] = version;
        }

        var reply = await Write(Op(command.Application, command.CommandId, Operations.ActivateCapabilities, command.NotAfter, fp, RxLane.Producer, 'd'),
            "activate_capabilities", ct, Json(caps), Json(RateActorJson(r.Actor)), command.Application).ConfigureAwait(false);
        return reply.HasValue ? reply.Applied(ReceiptCodec.ReadActivated(reply.Result!)) : RateReject<ActivatedCapabilities>(reply);
    }

    public async Task<StoreReply<RatePolicy>> EditRatePolicyAsync(StoreCommand<RatePolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = RateValidateActor(r.Actor)
            ?? (r.ScopeKind is not ("queue" or "job-type" or "resource") ? "scopeKind" : null)
            ?? (string.IsNullOrEmpty(r.ScopeKey) || Encoding.UTF8.GetByteCount(r.ScopeKey) > 512 ? "scopeKey" : null)
            ?? (r.Limit is < 1 or > 100_000 ? "limit" : null)
            ?? (r.WindowMs is < 1_000 or > 86_400_000 ? "windowMs" : null);
        if (invalid is not null)
        {
            return StoreReply<RatePolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditRatePolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.RatePolicy(r));
        var reply = await Write(Op(command.Application, command.CommandId, Operations.EditRatePolicy, command.NotAfter, fp, RxLane.Producer, 'd'),
            "edit_rate_policy", ct, r.ScopeKind, r.ScopeKey, r.Limit, r.WindowMs, r.ExpectedPolicyRevision, Json(RateActorJson(r.Actor))).ConfigureAwait(false);
        return reply.HasValue ? reply.Applied(ReceiptCodec.ReadRate(reply.Result!)) : RateReject<RatePolicy>(reply);
    }

    /// <summary>08 §2 actor bounds (PgStore.ValidateActor); returns the offending field or null.</summary>
    static string? RateValidateActor(ControlActor actor)
    {
        static bool Bytes(string? s, int min, int max) => s is null ? min == 0 : Encoding.UTF8.GetByteCount(s) is var n && n >= min && n <= max;

        if (actor is null || !Bytes(actor.Subject, 1, 256) || actor.Subject.Any(char.IsControl))
        {
            return "actor.subject";
        }

        if (!Bytes(actor.Display, 0, 256))
        {
            return "actor.display";
        }

        if (string.IsNullOrEmpty(actor.Scheme) || actor.Scheme.Length > 64 || !char.IsAsciiLetterOrDigit(actor.Scheme[0])
            || actor.Scheme.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-')))
        {
            return "actor.scheme";
        }

        return Bytes(actor.Reason, 0, 512) ? null : "reason";
    }

    /// <summary>The actor as U.audit reads it.</summary>
    static JsonObject RateActorJson(ControlActor a) => new() { ["subject"] = a.Subject, ["display"] = a.Display, ["scheme"] = a.Scheme, ["reason"] = a.Reason };

    /// <summary>A no-effect reply; a revision conflict carries the actual revision the script put in its detail (<c>actual=n</c>).</summary>
    static StoreReply<T> RateReject<T>(RxReply r) where T : class =>
        r.Code == "revision_conflict" && r.Detail is { } d && d.StartsWith("actual=", StringComparison.Ordinal)
            ? StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
                new StoreError("revision_conflict", ActualRevision: long.Parse(d["actual=".Length..], CultureInfo.InvariantCulture), Detail: d), r.Decided)
            : r.Reject<T>();
}
