using System.Globalization;
using System.Text.Json.Nodes;
using Runwick.Management;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>
/// Retention (17, 22 §7.3): CleanupBatch/1 is one intent per batch that selects from the retention indexes and deletes with explicit
/// after-images; the guard serializes it with live work, so there is nothing to skip and a batch never reports <c>skipped_locked</c>.
/// </summary>
internal sealed partial class RedisStore : IRetentionStore
{
    /// <summary>The seven hours (null = forever) as the JSON the scripts keep; the same text for the same policy.</summary>
    static string RetHours(RetentionPolicy p) => new JsonArray([.. p.Hours().Select(h => (JsonNode?)(h is { } v ? JsonValue.Create(v) : null))]).ToJsonString();

    internal static readonly string RetDefaultHours = RetHours(RetentionPolicy.Default);

    public async Task<CleanupBatchResult> RunCleanupBatchAsync(string application, string recordClass, CleanupLimits limits, CancellationToken ct)
    {
        if (!RecordClasses.All.Contains(recordClass, StringComparer.Ordinal))
        {
            throw new ArgumentOutOfRangeException(nameof(recordClass), recordClass, "Unknown record class.");
        }

        string code;
        try
        {
            var r = await RetCallAsync(application, limits.StatementTimeout, ct, recordClass, "batch", limits.Rows, limits.DetailBytes, "", RetDefaultHours).ConfigureAwait(false);
            if (r.HasValue)
            {
                var o = r.Result!;
                return new CleanupBatchResult("done", o["rows"]!.GetValue<int>(), o["complete"]!.GetValue<bool>(), o["caughtUp"]!.GetValue<bool>(), null);
            }

            code = r.Code ?? r.Status;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && ex is StoreNotAppliedException or StoreOutcomeUnknownException or RedisException)
        {
            code = ex switch
            {
                StoreNotAppliedException n => n.Code,
                StoreOutcomeUnknownException => "outcome_unknown",
                _ => "store_unavailable",
            };
        }

        await RecordCleanupFailureAsync(application, recordClass, code, ct).ConfigureAwait(false);
        return new CleanupBatchResult("failed", 0, false, false, code);
    }

    Task<RxReply> RetCallAsync(string application, TimeSpan budget, CancellationToken ct, params RedisValue[] args) =>
        Write(Op(application, Guid.CreateVersion7(), Operations.CleanupBatch, time.GetUtcNow() + budget + TimeSpan.FromSeconds(5), null, RxLane.Background, 'n'),
            "cleanup", ct, args);

    /// <summary>A failed batch changed nothing; its error is recorded in a separate small intent, best effort.</summary>
    async Task RecordCleanupFailureAsync(string application, string recordClass, string code, CancellationToken ct)
    {
        try
        {
            await RetCallAsync(application, TimeSpan.Zero, ct, recordClass, "fail", 0, 0, code, RetDefaultHours).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException or RedisException)
        {
            // Progress is observational; the next batch runs regardless.
        }
    }

    public async Task<StoreRead<RetentionState>> GetRetentionAsync(string application, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("retention_get"), RxLane.Background, [], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK" || a.Length < 4)
        {
            throw new StoreNotAppliedException("store_unavailable");
        }

        Seen(application, (string)a[1]!);
        var now = At(a[2]);
        var policy = (string)a[3]! is { Length: > 0 } json
            ? RetPolicy(JsonNode.Parse(json)!.AsObject())
            : new StoredRetentionPolicy(RetentionPolicy.Default, 0);
        var today = now.ToUnixTimeMilliseconds() / 86_400_000;
        var rows = new Dictionary<string, RetentionClassProgress>(StringComparer.Ordinal);
        for (var i = 4; i + 1 < a.Length; i += 2)
        {
            var c = (string)a[i]!;
            var p = JsonNode.Parse((string)a[i + 1]!)!.AsObject();
            var current = p["day"]?.GetValue<long>() == today;
            var caughtUp = p["cu"] is { } cu ? DateTimeOffset.FromUnixTimeMilliseconds(cu.GetValue<long>()) : (DateTimeOffset?)null;
            rows[c] = new RetentionClassProgress(c, RecordClasses.IsPolicy(c) ? "policy" : "fixed",
                p["lb"] is { } lb ? DateTimeOffset.FromUnixTimeMilliseconds(lb.GetValue<long>()) : null, caughtUp, p["tr"]?.GetValue<long>() ?? 0,
                current ? p["dr"]?.GetValue<long>() ?? 0 : 0, current ? p["db"]?.GetValue<int>() ?? 0 : 0, current ? p["df"]?.GetValue<int>() ?? 0 : 0,
                p["err"]?.GetValue<string>(), p["erra"] is { } ea ? DateTimeOffset.FromUnixTimeMilliseconds(ea.GetValue<long>()) : null,
                caughtUp is not { } t || t < now - TimeSpan.FromHours(24));
        }

        IReadOnlyList<RetentionClassProgress> classes = [.. RecordClasses.All.Select(c => rows.TryGetValue(c, out var p) ? p
            : new RetentionClassProgress(c, RecordClasses.IsPolicy(c) ? "policy" : "fixed", null, null, 0, 0, 0, 0, null, null, true))];
        return new StoreRead<RetentionState>(new RetentionState(policy, classes), now);
    }

    static StoredRetentionPolicy RetPolicy(JsonObject row) =>
        new(RetentionPolicy.FromHours([.. JsonNode.Parse(row["hs"]!.GetValue<string>())!.AsArray().Select(n => n?.GetValue<int>())]), row["rev"]!.GetValue<long>());

    public async Task<StoreReply<StoredRetentionPolicy>> EditRetentionPolicyAsync(StoreCommand<RetentionPolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ActorProblem(r.Actor) is { } invalidActor)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalidActor), time.GetUtcNow());
        }

        if (r.Policy.Problem() is { } problem)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_retention_policy", Detail: problem), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditRetentionPolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.RetentionPolicy(r));
        var reply = await Write(Op(command.Application, command.CommandId, Operations.EditRetentionPolicy, command.NotAfter, fp, RxLane.Background,
            GateOf(Operations.EditRetentionPolicy)), "edit_retention", ct, r.ExpectedRevision, RetHours(r.Policy), Json(SchActorJson(r.Actor)),
            RetDefaultHours, command.Application).ConfigureAwait(false);
        if (reply.HasValue)
        {
            return reply.Applied(new StoredRetentionPolicy(
                RetentionPolicy.FromHours([.. reply.Result!["policy"]!.AsArray().Select(n => n?.GetValue<int>())]), reply.Result["revision"]!.GetValue<long>()));
        }

        var rejected = reply.Reject<StoredRetentionPolicy>();
        return reply.Code == "revision_conflict" && reply.Detail is { } d && d.StartsWith("actual=", StringComparison.Ordinal)
            ? rejected with { Error = rejected.Error! with { ActualRevision = long.Parse(d["actual=".Length..], CultureInfo.InvariantCulture) } }
            : rejected;
    }
}
