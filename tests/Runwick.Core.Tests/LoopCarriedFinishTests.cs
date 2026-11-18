using Runwick.Harness.Engine;
using Runwick.Protocol;

namespace Runwick.Core.Tests;

/// <summary>
/// The pool's claim carries the waiting finishes (05 §12.7, loop direction): with combined finish and claim and buffered finish on, the
/// pool's claim loop takes the Finishes that wait to be sent into its own claim call. Fake time over the scripted store.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "PF2-F2")]
public sealed class LoopCarriedFinishTests
{
    static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    const string Call = "FinishAndClaim";

    static EngineFixture Create(int concurrency, TimeSpan? flush = null) =>
        EngineFixture.Create(concurrency: concurrency, options: o => o.UseBufferedFinish(flush ?? TimeSpan.FromMilliseconds(100)).UseCombinedFinishClaim(),
            finishMany: true);

    static ScriptedFinishManyStore StoreOf(EngineFixture e) => (ScriptedFinishManyStore)e.Store;

    /// <summary>Every Finish command ID the worker sent, once per send, across Finish, FinishMany and FinishAndClaim.</summary>
    static List<Guid> FinishSends(EngineFixture e) =>
    [
        .. e.Store.Calls.SelectMany(c => c.Body switch
        {
            FinishRequest => [c.CommandId],
            FinishManyRequest m => m.Items.Select(i => i.CommandId),
            FinishAndClaimRequest f => f.Finishes.Select(i => i.CommandId),
            _ => [],
        }),
    ];

    static void AssertEachJobFinishedOnce(EngineFixture e, int jobs)
    {
        Assert.Equal(jobs, e.Store.Jobs.Count);
        Assert.All(e.Store.Jobs, j =>
        {
            Assert.Equal(JobStates.Succeeded, j.State);
            Assert.Single(j.Finishes);
        });
        Assert.Equal(jobs, e.Script.Invocations.Count);
    }

    [Fact]
    public async Task The_loop_claim_carries_the_finishes_that_wait()
    {
        await using var e = Create(concurrency: 4);
        for (var i = 0; i < 40; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        AssertEachJobFinishedOnce(e, 40);

        var c = e.Engine.Completion;
        Assert.True(c.LoopCarriedCalls > 0, "the loop never carried a finish");
        Assert.True(c.LoopCarriedFinishes >= c.LoopCarriedCalls);
        Assert.Equal(e.Store.Count(Call), (int)c.CombinedCalls);

        // Every carried call also claimed: the loop sends it only as its own claim.
        Assert.All(e.Store.Calls.Where(x => x.Operation == Call), x => Assert.True(((FinishAndClaimRequest)x.Body).Claim.Body.MaxJobs >= 1));
        Assert.Equal(0, c.Buffered);
    }

    [Fact]
    public async Task A_finish_is_sent_once_while_the_loop_and_the_completion_lane_race_for_it()
    {
        // A one-millisecond interval makes the collector flush about as often as the loop claims, so both take from the same waiting set.
        await using var e = Create(concurrency: 8, TimeSpan.FromMilliseconds(1));
        e.Step = TimeSpan.FromMilliseconds(1);
        for (var i = 0; i < 300; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        AssertEachJobFinishedOnce(e, 300);

        // No failures were scripted, so every Finish command was sent exactly once, by whichever side took it.
        var sends = FinishSends(e);
        Assert.Equal(300, sends.Count);
        Assert.Equal(300, sends.Distinct().Count());
        Assert.True(e.Engine.Completion.LoopCarriedFinishes > 0);
        Assert.True(e.Store.Count(Operations.FinishMany) + e.Store.Count(Operations.Finish) > 0, "the completion lane never sent");
        await e.RunUntilAsync(() => e.Engine.Entries.Count == 0, Long, "every attempt retired");
        Assert.Equal(0, e.Engine.Completion.Buffered);
    }

    [Fact]
    public async Task A_drain_with_nothing_left_to_claim_still_sends_the_waiting_finishes()
    {
        // A one-second interval would hold these finishes for a second, and no claim is coming to carry them: the stop sends them.
        await using var e = Create(concurrency: 4, TimeSpan.FromSeconds(1));
        e.Step = TimeSpan.FromMilliseconds(10);
        for (var i = 0; i < 4; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.Invocations.Count == 4 && e.Engine.Completion.Buffered > 0, Long, "handlers done, finishes buffered");
        await e.StopAsync();
        AssertEachJobFinishedOnce(e, 4);
        Assert.Equal(0, e.Engine.Completion.Buffered);
        Assert.Equal(4, FinishSends(e).Distinct().Count());
    }

    [Fact]
    public async Task A_refused_call_turns_the_option_off_and_everything_finishes_with_separate_calls()
    {
        await using var e = Create(concurrency: 4);
        StoreOf(e).RefuseFinishAndClaim = true;
        for (var i = 0; i < 30; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        AssertEachJobFinishedOnce(e, 30);
        Assert.Equal(1, e.Store.Count(Call));
        Assert.Equal(1, e.Engine.Completion.CombinedFallbacks);
        Assert.Contains(e.Engine.Health().Diagnostics, d => d.Contains("FinishAndClaim returned UnsupportedVersion", StringComparison.Ordinal));

        // The refused call's claim was then sent alone with the same command ID.
        var refused = (FinishAndClaimRequest)e.Store.Calls.Single(c => c.Operation == Call).Body;
        Assert.Contains(e.Store.Calls, c => c.Operation == Operations.ReserveAndBegin && c.CommandId == refused.Claim.CommandId);
    }

    [Fact]
    public async Task A_lost_reply_resolves_each_finish_and_the_claim_by_its_own_command()
    {
        await using var e = Create(concurrency: 4);
        var dropped = 0;
        e.Store.DropReply = c => c.Operation == Call && Interlocked.Exchange(ref dropped, 1) == 0;
        for (var i = 0; i < 30; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        await e.RunUntilAsync(() => e.Engine.Entries.Count == 0, Long, "every attempt retired");
        AssertEachJobFinishedOnce(e, 30);

        // The lost call committed. Its claim was resent alone with the same ID (a replay), each of its finishes by its own Finish command.
        var lost = (FinishAndClaimRequest)e.Store.Calls.First(c => c.Operation == Call).Body;
        Assert.Contains(e.Store.Calls, c => c.Operation == Operations.ReserveAndBegin && c.CommandId == lost.Claim.CommandId);
        Assert.All(lost.Finishes, f => Assert.Contains(e.Store.Calls, c => c.Operation == Operations.Finish && c.CommandId == f.CommandId));
        Assert.True(e.Engine.Completion.CombinedFallbacks >= 1);
        Assert.DoesNotContain(e.Engine.Health().Diagnostics, d => d.Contains("unconfirmed outcome", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Without_the_option_the_loop_never_sends_the_call()
    {
        await using var e = EngineFixture.Create(concurrency: 4, options: o => o.UseBufferedFinish(), finishMany: true);
        for (var i = 0; i < 20; i++)
        {
            e.Add("j" + i);
        }

        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), Long, "all succeeded");
        AssertEachJobFinishedOnce(e, 20);
        Assert.Equal(0, e.Store.Count(Call));
        Assert.Equal(0, e.Engine.Completion.LoopCarriedCalls);
    }
}
