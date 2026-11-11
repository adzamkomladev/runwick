using System.Globalization;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Store;
using Runwick.Protocol;
using StackExchange.Redis;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// END-04 (07 §8): a handler result that arrives after its execution timeout is a timeout, and one that arrives in time is a success even when the store
/// answers the Finish late. Ported from the PostgreSQL suite. The late acknowledgement is made on the store side: PostgreSQL delays the client before the
/// gate lock (the original fault), SQLite delays the Finish at its commit point while it holds the writer, and Redis pauses writes for three seconds
/// once the handler has started (Redis has no store fault points).
/// </summary>
[Trait("Category", "Fault")]
[Trait("Item", "SP-028")]
[Collection(nameof(RedisServerWideCollection))]
public sealed class WorkerEndContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly TimeSpan Patience = TimeSpan.FromSeconds(90);

    static WorkerHost Host(DriverFixture f, TimeSpan timeout) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 1, j => j.WithExecutionTimeout(timeout));

    static async Task WaitState(DriverFixture f, Guid job, string state) =>
        await WorkerHost.WaitUntil(() => Model(f).Result.Job(job).State == state, Patience, $"job {job} {state}");

    static async Task<List<string>> Outcomes(DriverFixture f, Guid job) =>
        [.. (await Model(f)).AttemptsOf(job).Select(a => a.Outcome ?? "<open>")];

    static async Task<long> ElapsedMs(DriverFixture f, Guid job) => f.Kind == StoreKind.Redis
        ? await RedisAdmin.AttemptElapsed(f.Application, job)
        : await f.CountAsync(Q(f, "SELECT execution_elapsed_ms FROM runwick.job_attempts WHERE job_id = @j"), ("j", job));

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "END-04")]
    public async Task Timeout_versus_delayed_acknowledgement(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("end04"), kind);

        // (a) Timeout 1 s; the handler ignores cancellation and returns success at 1.5 s: TimedOut.
        await using (var w = Host(f, TimeSpan.FromSeconds(1)))
        {
            w.Script.On("late", (_, _, _) => Task.Delay(1500, CancellationToken.None));
            var job = (await w.DispatchAsync("late")).JobId.Value;
            await w.StartAsync();
            await WaitState(f, job, JobStates.Retrying);
            Assert.Equal("timed_out", (await Outcomes(f, job))[0]);   // the retry may already have started its next attempt
        }

        // (b) Timeout 2 s; the handler returns at 0.5 s; Finish delayed 3 s in the store: Succeeded.
        await using (var w = Host(f, TimeSpan.FromSeconds(2)))
        {
            w.Script.On("quick", (_, _, _) => Task.Delay(500, CancellationToken.None));
            var job = (await w.DispatchAsync("quick")).JobId.Value;
            var late = TimeSpan.FromSeconds(3);
            if (kind == StoreKind.Postgres)
            {
                f.ArmFault(FaultPoint.BeforeGateLock, new FaultSpec(FaultAction.Delay, Delay: late), FaultTarget.Op(Operations.Finish));
            }
            else if (kind == StoreKind.Sqlite)
            {
                f.ArmFault(FaultPoint.BeforeCommit, new FaultSpec(FaultAction.Delay, Delay: late), FaultTarget.Op(Operations.Finish));
            }

            await w.StartAsync();
            if (kind == StoreKind.Redis)
            {
                await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("quick") == 1, Patience, "the handler started");
                var mux = await RedisServer.AdminAsync();
                await mux.GetServer(mux.GetEndPoints()[0]).ExecuteAsync("CLIENT", "PAUSE", (long)late.TotalMilliseconds, "WRITE");
            }

            await WaitState(f, job, JobStates.Succeeded);
            Assert.Equal(["succeeded"], await Outcomes(f, job));
            var elapsed = await ElapsedMs(f, job);
            Assert.InRange(elapsed, 450, 1999);
        }
    }
}
