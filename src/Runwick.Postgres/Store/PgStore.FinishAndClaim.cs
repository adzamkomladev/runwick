using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IFinishAndClaimStore
{
    // ------------------------------------------------------------------ FinishAndClaim (opt-in, 05 §12.7)

    /// <summary>
    /// The finishes as FinishMany/1 and then one claim as ReserveAndBegin/1, in one transaction and one round trip
    /// (runwick.finish_and_claim_v1, which calls finish_many_v3 and reserve_and_begin_v3). The call runs as the claim command:
    /// a resend of a decided claim replays the claim alone, and each finish keeps its own receipt.
    /// </summary>
    public Task<StoreReply<FinishedAndClaimed>> FinishAndClaimAsync(FinishAndClaimRequest request, CancellationToken ct)
    {
        var items = request.Finishes;
        var claim = request.Claim;
        if (items.Count is 0 or > IFinishAndClaimStore.MaxFinishes || !ValidClaim(claim.Body) || !ValidFinishes(items, claim.Application)
            || items.Any(i => i.CommandId == claim.CommandId))
        {
            return Task.FromResult(StoreReply<FinishedAndClaimed>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        var call = ClaimArgs(new PgCall("finish_and_claim_v1"), claim.Body)
            .P("p_finish_class", Operations.RetentionClass(Operations.FinishMany))
            .P("p_finish_deadline_cap", Operations.DeadlineCap(Operations.FinishMany));
        return runner.CallAsync(ClaimOp(claim, PgLane.Settlement), FinishArgs(call, items, "p_finish_tokens"), ReadFinishedAndClaimed, ct);
    }

    static StoreReply<FinishedAndClaimed> ReadFinishedAndClaimed(PgRow row)
    {
        if (!row.HasValue)
        {
            return row.Reject<FinishedAndClaimed>();
        }

        var result = row.Result!;
        if (result["finished"] is not JsonArray finished)
        {
            // A replayed claim holds only the claim: its finishes are resolved from their own receipts.
            return row.Applied(new FinishedAndClaimed(null, row.Applied(ReadClaimed(row))));
        }

        StoreReply<ClaimedJobs> claimed;
        var replayed = result["claimReplayed"]?.GetValue<bool>() ?? false;
        if (result["claim"] is JsonObject c)
        {
            var cursor = c["resumeAt"] is { } at && c["resumeJob"] is { } job
                ? new CandidateCursor(PgTime.FromMs(at.GetValue<long>()), Guid.Parse(job.GetValue<string>()))
                : null;
            var blocked = c["blocked"] is { } scope ? Guid.Parse(scope.GetValue<string>()) : (Guid?)null;
            var decided = c["decidedAt"] is { } ms ? PgTime.FromMs(ms.GetValue<long>()) : row.Decided;
            claimed = StoreReply<ClaimedJobs>.NoEffect(Enum.Parse<StoreStatus>(c["status"]!.GetValue<string>()),
                new StoreError(c["code"]?.GetValue<string>() ?? "invariant_fault", null, null, blocked, cursor), decided);
        }
        else
        {
            claimed = new StoreReply<ClaimedJobs>(StoreStatus.Applied, StoreEffect.Applied, replayed, ReadClaimed(row), null, row.Decided);
        }

        return new StoreReply<FinishedAndClaimed>(StoreStatus.Applied, StoreEffect.Applied, false,
            new FinishedAndClaimed(ReadFinishItems(finished), claimed), null, row.Decided);
    }
}
