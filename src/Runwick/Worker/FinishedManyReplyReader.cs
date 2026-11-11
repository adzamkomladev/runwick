using System.Text.Json;
using System.Text.Json.Serialization;
using Runwick.Protocol;

namespace Runwick.Worker;

/// <summary>Reads the v1 worker batch settlement reply without materializing a JSON node tree.</summary>
internal static class FinishedManyReplyReader
{
    static readonly FinishedManyJsonContext Codec = new(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowDuplicateProperties = false,
    });

    // The caller retains its original decoder for noncanonical shapes and its original error behavior.
    internal static FinishedMany? TryRead(ReadOnlySpan<byte> utf8)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize(utf8, Codec.FinishedManyWireReply);
            if (envelope?.Items is not { } items)
            {
                return null;
            }

            var outcomes = new List<StoreReply<FinishedAttempt>>(items.Length);
            foreach (var item in items)
            {
                if (item?.Status is not { } statusText || item.Effect is null || !Enum.TryParse<StoreStatus>(statusText, out var status))
                {
                    return null;
                }

                var decided = item.DecidedAt is { } ms ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : default;
                if (item.Effect != nameof(StoreEffect.Applied))
                {
                    outcomes.Add(StoreReply<FinishedAttempt>.NoEffect(status, new StoreError(item.Code ?? "invariant_fault"), decided));
                    continue;
                }

                if (item.Replayed is not { } replayed || item.Result is not { JobId: { } id, Generation: { } generation, Revision: { } revision,
                        State: { } state, AttemptOutcome: { } attempt, Disposition: { } disposition } result || !Guid.TryParse(id, out var jobId))
                {
                    return null;
                }

                var eligible = result.EligibleAt is { } at ? DateTimeOffset.FromUnixTimeMilliseconds(at) : (DateTimeOffset?)null;
                var finished = new FinishedAttempt(new JobPosition(jobId, generation, revision, state), attempt, disposition, result.TerminalReason, eligible);
                outcomes.Add(new StoreReply<FinishedAttempt>(status, StoreEffect.Applied, replayed, finished,
                    item.Code is { } code ? new StoreError(code) : null, decided));
            }

            return new FinishedMany(outcomes);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}

internal sealed record FinishedManyWireReply(FinishedManyWireItem?[]? Items);

internal sealed record FinishedManyWireItem(string? Status, string? Effect, bool? Replayed, string? Code, long? DecidedAt,
    FinishedManyWireResult? Result);

internal sealed record FinishedManyWireResult(string? JobId, int? Generation, long? Revision, string? State,
    string? AttemptOutcome, string? Disposition, string? TerminalReason, long? EligibleAt);

[JsonSerializable(typeof(FinishedManyWireReply))]
internal sealed partial class FinishedManyJsonContext : JsonSerializerContext;
