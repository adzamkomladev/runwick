using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.ReferenceModel;

namespace Runwick.Sqlite.Tests;

/// <summary>
/// Transactional completion on SQLite (15 TXC-01, 03–06; 20 §8) through the real worker engine, ported from
/// Runwick.Postgres.Tests.TransactionalCompletionTests. TXC-02 is in <see cref="SqliteStagingTests"/>.
/// </summary>
[Trait("Item", "SP-028")]
public sealed class SqliteCompletionTests
{
    static readonly TimeSpan Long = TimeSpan.FromSeconds(90);

    static async Task<(DriverFixture F, WorkerHost W)> StartAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        using (var conn = Business(f))
        {
            Exec(conn, null, "CREATE TABLE ledger (job_id TEXT NOT NULL, attempt_id TEXT NOT NULL, note TEXT)");
        }

        var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 2);
        await w.StartAsync();
        return (f, w);
    }

    /// <summary>A business connection with the store's profile (foreign_keys = 1, synchronous = FULL).</summary>
    static SqliteConnection Business(DriverFixture f)
    {
        var conn = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False;Foreign Keys=True;Default Timeout=5");
        conn.Open();
        Exec(conn, null, "PRAGMA synchronous = FULL");
        return conn;
    }

    static void Exec(SqliteConnection conn, SqliteTransaction? tx, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    static ISqliteJobCompletion Completion(IJobContext context) =>
        ((IServiceProvider)context.Items[typeof(IServiceProvider)]!).GetRequiredService<ISqliteJobCompletion>();

    static void Credit(SqliteConnection conn, SqliteTransaction tx, IJobContext context) =>
        Exec(conn, tx, $"INSERT INTO ledger VALUES ('{context.JobId.Value}', '{context.AttemptId.Value}', 'credit')");

    static Task<long> Ledger(DriverFixture f, Guid job) => f.CountAsync("SELECT count(*) FROM ledger WHERE job_id = @j", ("j", job.ToString()));

    static async Task<string> State(DriverFixture f, Guid job) => (await f.ReadCommittedSnapshot()).Model.Job(job).State;

    static Task WaitTerminal(DriverFixture f, Guid job) =>
        WorkerHost.WaitUntil(() => State(f, job).GetAwaiter().GetResult() is "succeeded" or "failed" or "cancelled", Long, $"job {job} terminal");

    static async Task AssertInvariants(DriverFixture f) => Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    [Fact]
    [Trait("Category", "Fault")]
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

                using var conn = Business(f);
                using var tx = conn.BeginTransaction(deferred: false);
                Credit(conn, tx, ctx);
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                tx.Rollback();
                stateAfterRollback = await State(f, ctx.JobId.Value);
                throw new InvalidOperationException("business rule failed after staging");
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("running", stateAfterRollback);
            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(0, await Ledger(f, job));
            Assert.Equal(["retryable_failure", "succeeded"], (await f.ReadCommittedSnapshot()).Model.AttemptsOf(job).Select(a => a.Outcome));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    /// <summary>
    /// SQLite has no wire between the caller and the file (20 §12, the reason TX-04 does not apply), so there is no COMMIT reply to
    /// drop. What the case protects is the engine's reaction to a caller whose commit happened but who then saw a failure: the engine
    /// resolves the staged command by its receipt, the job is succeeded once and the handler does not run again. The failure after
    /// the commit is thrown by the handler.
    /// </summary>
    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "TXC-03")]
    public async Task A_caller_failure_after_the_commit_resolves_by_receipt_without_rerunning_the_handler()
    {
        var (f, w) = await StartAsync("txc03");
        await using (f)
        await using (w)
        {
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                using var conn = Business(f);
                using var tx = conn.BeginTransaction(deferred: false);
                Credit(conn, tx, ctx);
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                tx.Commit();
                throw new IOException("the commit reply was lost");
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(1, await Ledger(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("pay"));
            Assert.Single((await f.ReadCommittedSnapshot()).Model.AttemptsOf(job));
            Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "TXC-04")]
    public async Task A_stale_owner_gets_ownership_lost_and_must_roll_back()
    {
        var (f, w) = await StartAsync("txc04");
        await using (f)
        await using (w)
        {
            string? code = null;
            w.Script.On("pay", async (_, ctx, _) =>
            {
                // Another owner's authority: the lease is already expired in store time.
                await f.ExecAsync("UPDATE runwick_jobs SET lease_expires_at = @t WHERE job_id = @j",
                    ("j", ctx.JobId.Value), ("t", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000));
                using var conn = Business(f);
                using var tx = conn.BeginTransaction(deferred: false);
                Credit(conn, tx, ctx);
                try
                {
                    await Completion(ctx).CompleteInTransactionAsync(tx, CancellationToken.None);
                }
                catch (JobOwnershipLostException ex)
                {
                    code = ex.Code;
                    tx.Rollback();
                    return;
                }

                tx.Commit();
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WorkerHost.WaitUntil(() => code is not null, Long, "ownership_lost observed");

            Assert.Equal(RunwickErrorCodes.OwnershipLost, code);
            Assert.Equal(0, await Ledger(f, job));
            Assert.DoesNotContain((await f.ReadCommittedSnapshot()).Model.AttemptsOf(job), a => a.Outcome == "succeeded");
            await AssertInvariants(f);
            await w.StopAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "Fault")]
    [Trait("Case", "TXC-05")]
    public async Task A_staged_completion_that_never_commits_fails_the_attempt(bool leaveOpen)
    {
        var (f, w) = await StartAsync("txc05");
        await using (f)
        await using (w)
        {
            SqliteConnection? held = null;
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                var conn = Business(f);
                var tx = conn.BeginTransaction(deferred: false);
                Credit(conn, tx, ctx);
                await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                if (leaveOpen)
                {
                    held = conn; // returns with the transaction still open
                    return;
                }

                tx.Rollback();
                conn.Dispose();
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            if (leaveOpen)
            {
                await WorkerHost.WaitUntil(() => held is not null, Long, "handler returned");
                await Task.Delay(TimeSpan.FromSeconds(1));
                Assert.Equal("running", await State(f, job));
                held!.Dispose(); // the caller's transaction ends without commit; the normal Finish proceeds
            }

            await WaitTerminal(f, job);
            Assert.Equal("failed", await State(f, job));
            Assert.Equal(0, await Ledger(f, job));
            var attempt = Assert.Single((await f.ReadCommittedSnapshot()).Model.AttemptsOf(job));
            Assert.Equal("permanent_failure", attempt.Outcome);
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick_job_attempts WHERE job_id = @j AND error LIKE '%' || @code || '%'",
                ("j", job), ("code", RunwickErrorCodes.StagedCompletionNotCommitted)));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "TXC-06")]
    public async Task A_handler_exception_after_a_committed_completion_leaves_the_commit_standing()
    {
        var (f, w) = await StartAsync("txc06");
        await using (f)
        await using (w)
        {
            w.Script.On("pay", async (_, ctx, ct) =>
            {
                using (var conn = Business(f))
                using (var tx = conn.BeginTransaction(deferred: false))
                {
                    Credit(conn, tx, ctx);
                    await Completion(ctx).CompleteInTransactionAsync(tx, ct);
                    tx.Commit();
                }

                throw new InvalidOperationException("post-commit bookkeeping failed");
            });

            var job = (await w.DispatchAsync("pay")).JobId.Value;
            await WaitTerminal(f, job);

            Assert.Equal("succeeded", await State(f, job));
            Assert.Equal(1, await Ledger(f, job));
            Assert.Equal(1, w.Script.InvocationsOf("pay"));
            Assert.Single((await f.ReadCommittedSnapshot()).Model.AttemptsOf(job));
            await w.StopAsync();
            await AssertInvariants(f);
        }
    }
}
