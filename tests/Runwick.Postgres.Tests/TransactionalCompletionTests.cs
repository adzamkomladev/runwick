using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.ReferenceModel;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>Transactional completion (R6, 08 §8, 01 §16): TXC-01 … TXC-06 through the real worker engine.</summary>
public sealed class TransactionalCompletionTests
{
    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<(DriverFixture F, WorkerHost W)> StartAsync(string name, StoreProfile profile = StoreProfile.DirectPrimary)
    {
        var f = await DriverFixture.StartAsync(Ns(name), profile);
        await f.ExecAsync("CREATE TABLE public.ledger (job_id uuid NOT NULL, attempt_id uuid NOT NULL, note text)");
        var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 2);
        await w.StartAsync();
        return (f, w);
    }

    static IPostgresJobCompletion Completion(IJobContext context) =>
        ((IServiceProvider)context.Items[typeof(IServiceProvider)]!).GetRequiredService<IPostgresJobCompletion>();

    static async Task CreditAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IJobContext context, string note)
    {
        using var cmd = new NpgsqlCommand("INSERT INTO public.ledger VALUES (@j, @a, @n)", conn, tx);
        cmd.Parameters.AddWithValue("j", context.JobId.Value);
        cmd.Parameters.AddWithValue("a", context.AttemptId.Value);
        cmd.Parameters.AddWithValue("n", note);
        await cmd.ExecuteNonQueryAsync();
    }

    static Task<long> Ledger(DriverFixture f, Guid job) => f.CountAsync("SELECT count(*) FROM public.ledger WHERE job_id = @j", ("j", job));

    static async Task<string> State(DriverFixture f, Guid job) =>
        (await TextAsync(f, "SELECT state FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single();

    static Task WaitTerminal(DriverFixture f, Guid job) =>
        WorkerHost.WaitUntil(() => State(f, job).GetAwaiter().GetResult() is "succeeded" or "failed" or "cancelled", Long, $"job {job} terminal");

    /// <summary>Committed-snapshot invariants and the ReferenceModel's acceptance of every committed job history (15 §12.2 rule 4).</summary>
    static async Task AssertInvariants(DriverFixture f)
    {
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-02")]
    public async Task Caller_commit_finalizes_exactly_once_without_an_engine_finish()
    {
        var (f, w) = await StartAsync("txc02");
        await using (f)
        await using (w)
        {
            StagedCompletion? staged = null;
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                await using var conn = await OpenBusinessAsync(f);
                await using var tx = await conn.BeginTransactionAsync(ct);
                await CreditAsync(conn, tx, ctx, "credit");
                staged = await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                await Assert.ThrowsAsync<JobCompletionException>(() => Completion(ctx).CompleteInTransactionAsync(tx, ct));
                await tx.CommitAsync(ct);
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(1, await Ledger(f, job));
            Assert.Equal(job, staged!.JobId.Value);

            // The caller's commit makes the job terminal; the engine observes it by receipt shortly after.
            await WorkerHost.WaitUntil(() => w.EventCount("completionCommitted") == 1, Long, "the engine observes the committed staging");
            Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
            Assert.Equal(1, Interlocked.Read(ref w.Engine.StagedCompletionsObserved));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Finish' AND command_id = @c", ("c", staged.CommandId)));
            Assert.Equal(1, w.Script.InvocationsOf("pay"));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-01")]
    public async Task Caller_rollback_leaves_the_attempt_owned_and_the_normal_path_settles()
    {
        var (f, w) = await StartAsync("txc01");
        await using (f)
        await using (w)
        {
            string? stateAfterRollback = null;
            var calls = 0;
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                if (Interlocked.Increment(ref calls) > 1)
                {
                    return; // the retry succeeds through the normal path
                }

                await using var conn = await OpenBusinessAsync(f);
                await using var tx = await conn.BeginTransactionAsync(ct);
                await CreditAsync(conn, tx, ctx, "credit");
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                await tx.RollbackAsync(ct);
                stateAfterRollback = await State(f, ctx.JobId.Value);
                throw new InvalidOperationException("business rule failed after staging");
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("running", stateAfterRollback);
            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(0, await Ledger(f, job));
            Assert.Equal(["retryable_failure", "succeeded"],
                await TextAsync(f, "SELECT outcome FROM runwick.job_attempts WHERE job_id = @j ORDER BY reserved_at", ("j", job)));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-03")]
    public async Task A_lost_caller_commit_reply_resolves_by_receipt_without_rerunning_the_handler()
    {
        var (f, w) = await StartAsync("txc03", StoreProfile.DirectPrimaryViaProxy);
        await using (f)
        await using (w)
        {
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                await using var conn = await OpenBusinessAsync(f);
                await using var tx = await conn.BeginTransactionAsync(ct);
                await CreditAsync(conn, tx, ctx, "credit");
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                // Arm only: the returned task completes when the reply is dropped, after the commit below.
                var dropped = f.Proxy!.ArmCommitReplyDrop(1, PgFaultProxy.Role("business"));
                await tx.CommitAsync(ct); // the reply is lost: this throws after the server committed
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal(1, f.Proxy!.CommitRepliesDropped);
            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(1, await Ledger(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("pay"));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", job)));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-04")]
    public async Task A_stale_owner_gets_ownership_lost_and_must_roll_back()
    {
        var (f, w) = await StartAsync("txc04");
        await using (f)
        await using (w)
        {
            string? code = null;
            // Own calls ignore the attempt token: a renewal may observe the loss first and cancel it.
            w.Script.On("pay", async (_, ctx, _) =>
            {
                // Another owner's authority: the lease is already expired in store time.
                await f.ExecAsync("UPDATE runwick.jobs SET lease_expires_at = runwick.ms_floor(clock_timestamp()) - interval '1 second' WHERE job_id = @j",
                    ("j", ctx.JobId.Value));
                await using var conn = await OpenBusinessAsync(f);
                await using var tx = await conn.BeginTransactionAsync(CancellationToken.None);
                await CreditAsync(conn, tx, ctx, "credit");
                try
                {
                    await Completion(ctx).CompleteInTransactionAsync(tx, CancellationToken.None);
                }
                catch (JobOwnershipLostException ex)
                {
                    code = ex.Code;
                    await tx.RollbackAsync(CancellationToken.None);
                    return;
                }

                await tx.CommitAsync(CancellationToken.None);
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WorkerHost.WaitUntil(() => code is not null, Long, "ownership_lost observed");

            Assert.Equal(RunwickErrorCodes.OwnershipLost, code);
            Assert.Equal(0, await Ledger(f, job));
            Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j AND outcome = 'succeeded'", ("j", job)));
            await AssertInvariants(f);
            await w.StopAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-05")]
    public async Task A_staged_completion_that_never_commits_fails_the_attempt(bool leaveOpen)
    {
        var (f, w) = await StartAsync("txc05");
        await using (f)
        await using (w)
        {
            NpgsqlConnection? held = null;
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                var conn = await OpenBusinessAsync(f);
                var tx = await conn.BeginTransactionAsync(ct);
                await CreditAsync(conn, tx, ctx, "credit");
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                if (leaveOpen)
                {
                    held = conn; // returns with the transaction still open and holding the job row
                    return;
                }

                await tx.RollbackAsync(ct);
                await conn.DisposeAsync();
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            if (leaveOpen)
            {
                await WorkerHost.WaitUntil(() => held is not null, Long, "handler returned");
                await Task.Delay(TimeSpan.FromSeconds(1));
                Assert.Equal("running", await State(f, job));
                await held!.DisposeAsync(); // the caller's transaction ends without commit; the normal Finish proceeds
            }

            await WaitTerminal(f, job);
            Assert.Equal("failed", await State(f, job));
            Assert.Equal(0, await Ledger(f, job));
            Assert.Equal([RunwickErrorCodes.StagedCompletionNotCommitted],
                await TextAsync(f, "SELECT error->>'code' FROM runwick.job_attempts WHERE job_id = @j", ("j", job)));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-06")]
    public async Task A_handler_exception_after_a_committed_completion_leaves_the_commit_standing()
    {
        var (f, w) = await StartAsync("txc06");
        await using (f)
        await using (w)
        {
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                await using (var conn = await OpenBusinessAsync(f))
                await using (var tx = await conn.BeginTransactionAsync(ct))
                {
                    await CreditAsync(conn, tx, ctx, "credit");
                    await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                    await tx.CommitAsync(ct);
                }

                throw new InvalidOperationException("post-commit bookkeeping failed");
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(1, await Ledger(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("pay"));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", job)));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "TXC-02")]
    public async Task Outside_an_attempt_completion_is_no_active_attempt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("txcnone"));
        var c = await f.OpenClient(configure: r => Runwick.Harness.Jobs.TestJobs.Register(r));
        await using var scope = c.Services.CreateAsyncScope();
        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync();
        var ex = await Assert.ThrowsAsync<JobCompletionException>(() =>
            scope.ServiceProvider.GetRequiredService<IPostgresJobCompletion>().CompleteInTransactionAsync(tx));
        Assert.Equal(RunwickErrorCodes.NoActiveAttempt, ex.Code);
    }
}
