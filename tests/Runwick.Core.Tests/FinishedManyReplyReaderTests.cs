using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Core.Tests;

public sealed class FinishedManyReplyReaderTests
{
    [Fact]
    public void Forward_reader_preserves_applied_and_no_effect_items()
    {
        var reply = FinishedManyReplyReader.TryRead("""
            {"items":[
              {"effect":"Applied","status":"Applied","replayed":true,"decidedAt":42,"result":{
                "jobId":"00112233445566778899aabbccddeeff","generation":2,"revision":9223372036854775807,
                "state":"retrying","attemptOutcome":"transient_failure","disposition":"retry_scheduled","eligibleAt":999
              }},
              {"status":"StaleLease","effect":"ObservedNoEffect","code":"stale_lease","decidedAt":null}
            ]}
            """u8);

        Assert.NotNull(reply);
        Assert.Collection(reply.Outcomes,
            first =>
            {
                Assert.True(first.Replayed);
                Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(42), first.DecidedAt);
                Assert.NotNull(first.Value);
                Assert.Equal(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), first.Value.Position.JobId);
                Assert.Equal(long.MaxValue, first.Value.Position.Revision);
                Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(999), first.Value.EligibleAt);
                Assert.Null(first.Value.TerminalReason);
            },
            second =>
            {
                Assert.Equal(StoreEffect.ObservedNoEffect, second.Effect);
                Assert.Equal(StoreStatus.StaleLease, second.Status);
                Assert.Equal("stale_lease", second.Error?.Code);
                Assert.Null(second.Value);
            });
    }

    [Fact]
    public void Unrecognized_or_invalid_shapes_defer_to_the_legacy_decoder()
    {
        Assert.Null(FinishedManyReplyReader.TryRead("""{"items":[{"status":"Applied","status":"Applied","effect":"Applied"}]}"""u8));
        Assert.Null(FinishedManyReplyReader.TryRead("""{"items":[{"status":"Applied","effect":"Applied","replayed":"false"}]}"""u8));
        Assert.Null(FinishedManyReplyReader.TryRead("""{"items":[{"status":"Applied","effect":"Applied","replayed":false,"result":{}}]}"""u8));
        Assert.Null(FinishedManyReplyReader.TryRead("""{"items":[]} trailing"""u8));
    }
}
