using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;

namespace Runwick.Sqlite.Tests;

/// <summary>Caller transactions on SQLite (20 §8): TX-01, TX-02, SQL-03 and transactional completion.</summary>
[Trait("Item", "SP-019")]
public sealed class SqliteStagingTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly DispatchOptions WindowW = new() { Uniqueness = new DispatchUniqueness(null, "w", TimeSpan.FromHours(1)) };

    static async Task<(DriverFixture F, TestClient C)> StartAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        using var conn = Business(f);
        Exec(conn, null, "CREATE TABLE biz (id INTEGER PRIMARY KEY, note TEXT); INSERT INTO biz VALUES (0, 'seed')");
        return (f, c);
    }

    /// <summary>A business connection with the store's profile (foreign_keys = 1, synchronous = FULL).</summary>
    static SqliteConnection Business(DriverFixture f, string synchronous = "FULL")
    {
        var conn = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False;Foreign Keys=True;Default Timeout=1");
        conn.Open();
        Exec(conn, null, $"PRAGMA synchronous = {synchronous}");
        return conn;
    }

    static void Exec(SqliteConnection conn, SqliteTransaction? tx, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    static ISqliteJobDispatcher Staging(TestClient c) => c.Services.GetRequiredService<ISqliteJobDispatcher>();

    static Task<long> Biz(DriverFixture f, int id) => f.CountAsync("SELECT count(*) FROM biz WHERE id = @id", ("id", id));

    static Task<long> Jobs(DriverFixture f) => f.CountAsync("SELECT count(*) FROM runwick_jobs");

    static Task<long> Receipts(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick_storage_commands WHERE command_id = @c", ("c", commandId));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "TX-01")]
    public async Task Rolled_back_business_transaction_leaves_nothing()
    {
        var (f, c) = await StartAsync("tx01");
        await using var _ = f;
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"), WindowW);

        using (var conn = Business(f))
        using (var tx = conn.BeginTransaction(deferred: false))
        {
            Exec(conn, tx, "INSERT INTO biz VALUES (1, 'row')");
            var staged = await Staging(c).StageAsync(p, tx, Ct);
            Assert.Equal(DispatchDisposition.Created, staged.Disposition);
            tx.Rollback();
        }

        Assert.Equal(0, await Biz(f, 1));
        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await Receipts(f, p.OperationId.Value));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick_dispatch_keys WHERE bound_job_id IS NOT NULL"));
        Assert.Equal(DispatchResolutionStatus.NotObserved, (await c.Prepared.ResolveAsync(p.OperationId, Ct)).Status);
        var later = await c.Dispatcher.DispatchAsync(new KeyedJob("t", "k"), WindowW, Ct);
        Assert.Equal(DispatchDisposition.Created, later.Disposition);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "TX-02")]
    public async Task Committed_business_transaction_makes_everything_visible_together()
    {
        var (f, c) = await StartAsync("tx02");
        await using var _ = f;
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"), WindowW);
        StagedDispatch staged;
        using (var conn = Business(f))
        using (var tx = conn.BeginTransaction(deferred: false))
        {
            Exec(conn, tx, "INSERT INTO biz VALUES (1, 'row')");
            staged = await Staging(c).StageAsync(p, tx, Ct);

            // Still the caller's open transaction: further SQL runs, nothing is visible outside yet.
            Assert.NotNull(tx.Connection);
            Exec(conn, tx, "UPDATE biz SET note = 'after-stage' WHERE id = 1");
            Assert.Equal(0, await Jobs(f));
            Assert.Equal(0, await Receipts(f, p.OperationId.Value));
            tx.Commit();
        }

        Assert.Equal((p.OperationId, DispatchDisposition.Created, DuplicateMatch.None, false),
            (staged.OperationId, staged.Disposition, staged.DuplicateMatch, staged.PolicyDrift));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM biz WHERE id = 1 AND note = 'after-stage'"));
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick_jobs WHERE job_id = @j", ("j", staged.JobId.Value)));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick_dispatch_keys WHERE bound_job_id = @j", ("j", staged.JobId.Value)));
        Assert.Equal(1, await Receipts(f, p.OperationId.Value));
        var resolution = await c.Prepared.ResolveAsync(p.OperationId, Ct);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal((staged.JobId, DispatchDisposition.Created), (resolution.Receipt!.JobId, resolution.Receipt.Disposition));

        // Rejections leave the caller's transaction usable (savepoint) and throw the dispatch exception; a batch stages too.
        using (var conn = Business(f))
        using (var tx = conn.BeginTransaction(deferred: false))
        {
            await Assert.ThrowsAsync<UniquenessConflictException>(() => Staging(c).StageAsync(new KeyedJob("other", "z"), tx, WindowW, Ct));
            Exec(conn, tx, "INSERT INTO biz VALUES (2, 'still usable')");
            var batch = await Staging(c).StageManyAsync([new BatchDispatchItem(new PingJob("b1")), new BatchDispatchItem(new PingJob("b2"))], tx,
                cancellationToken: Ct);
            Assert.All(batch.Items, i => Assert.NotNull(i.Staged));
            tx.Commit();
        }

        Assert.Equal(1, await Biz(f, 2));
        Assert.Equal(3, await Jobs(f));

        // A completed transaction and another database file are rejected before any write.
        using (var conn = Business(f))
        {
            var done = conn.BeginTransaction();
            done.Commit();
            var invalid = await Assert.ThrowsAsync<TransactionValidationException>(() => Staging(c).StageAsync(new PingJob("x"), done, cancellationToken: Ct));
            Assert.Equal(RunwickErrorCodes.TransactionInvalid, invalid.Code);
        }

        var other = Path.Combine(Path.GetDirectoryName(f.SqlitePath)!, "other.db");
        using (var conn = new SqliteConnection($"Data Source={other};Pooling=False;Foreign Keys=True"))
        {
            conn.Open();
            using var tx = conn.BeginTransaction(deferred: false);
            var mismatch = await Assert.ThrowsAsync<TransactionValidationException>(() => Staging(c).StageAsync(new PingJob("x"), tx, cancellationToken: Ct));
            Assert.Equal(RunwickErrorCodes.TransactionStoreMismatch, mismatch.Code);
        }

        Assert.Equal(3, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "SQL-03")]
    [Trait("Case", "TX-03")]
    public async Task Busy_snapshot_reaches_the_caller_unchanged_and_whole_unit_retry_stages_once()
    {
        var (f, c) = await StartAsync("sql03a");
        await using var _ = f;
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"), WindowW);

        using (var conn = Business(f))
        using (var tx = conn.BeginTransaction(deferred: true))
        {
            using (var read = conn.CreateCommand())
            {
                read.Transaction = tx;
                read.CommandText = "SELECT count(*) FROM biz"; // the deferred transaction now holds a read snapshot
                read.ExecuteScalar();
            }

            using (var writer = Business(f))
            {
                Exec(writer, null, "INSERT INTO biz VALUES (9, 'another connection')");
            }

            var ex = await Assert.ThrowsAsync<SqliteException>(() => Staging(c).StageAsync(p, tx, Ct));
            Assert.Equal(5, ex.SqliteErrorCode); // SQLITE_BUSY (extended: BUSY_SNAPSHOT)
            Assert.NotNull(tx.Connection); // the caller's transaction is still the caller's
            tx.Rollback();
        }

        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await Receipts(f, p.OperationId.Value));

        // The caller retries the whole unit with the same PreparedDispatch, twice: one job, the second a receipt replay.
        var jobs = new List<JobId>();
        for (var i = 0; i < 2; i++)
        {
            using var conn = Business(f);
            using var tx = conn.BeginTransaction(deferred: false);
            Exec(conn, tx, $"INSERT INTO biz VALUES ({10 + i}, 'retry')");
            var staged = await Staging(c).StageAsync(p, tx, Ct);
            Assert.Equal(DispatchDisposition.Created, staged.Disposition);
            jobs.Add(staged.JobId);
            tx.Commit();
        }

        Assert.Equal(jobs[0], jobs[1]);

        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await Receipts(f, p.OperationId.Value));
    }

    [Theory]
    [Trait("Category", "Contract")]
    [Trait("Case", "SQL-03")]
    [InlineData("NORMAL", true)]
    [InlineData("FULL", false)]
    public async Task A_weaker_caller_connection_is_transaction_profile_mismatch_before_any_write(string synchronous, bool foreignKeys)
    {
        var (f, c) = await StartAsync("sql03b");
        await using var _ = f;
        using (var conn = Business(f, synchronous))
        {
            Exec(conn, null, $"PRAGMA foreign_keys = {(foreignKeys ? 1 : 0)}");
            using var tx = conn.BeginTransaction(deferred: false);
            Exec(conn, tx, "INSERT INTO biz VALUES (1, 'row')");
            var ex = await Assert.ThrowsAsync<TransactionValidationException>(() => Staging(c).StageAsync(new PingJob("x"), tx, cancellationToken: Ct));
            Assert.Equal(RunwickErrorCodes.TransactionProfileMismatch, ex.Code);
            Exec(conn, tx, "INSERT INTO biz VALUES (2, 'still usable')");
            tx.Commit();
        }

        Assert.Equal(1, await Biz(f, 2));
        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick_storage_commands WHERE operation = 'Dispatch'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "TXC-02")]
    public async Task Transactional_completion_commits_the_business_row_and_the_finish_together()
    {
        await using var f = await DriverFixture.StartAsync("txc02_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        using (var conn = Business(f))
        {
            Exec(conn, null, "CREATE TABLE ledger (job_id TEXT NOT NULL)");
        }

        // Outside an attempt: no_active_attempt.
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await using (var scope = c.Services.CreateAsyncScope())
        using (var conn = Business(f))
        using (var tx = conn.BeginTransaction(deferred: false))
        {
            var none = await Assert.ThrowsAsync<JobCompletionException>(() =>
                scope.ServiceProvider.GetRequiredService<ISqliteJobCompletion>().CompleteInTransactionAsync(tx, Ct));
            Assert.Equal(RunwickErrorCodes.NoActiveAttempt, none.Code);
        }

        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 2);
        await w.StartAsync();
        StagedCompletion? staged = null;
        w.Script.On("pay", async (_, ctx, ct) =>
        {
            var completion = ((IServiceProvider)ctx.Items[typeof(IServiceProvider)]!).GetRequiredService<ISqliteJobCompletion>();
            using var conn = Business(f);
            using var tx = conn.BeginTransaction(deferred: false);
            Exec(conn, tx, $"INSERT INTO ledger VALUES ('{ctx.JobId.Value}')");
            staged = await completion.CompleteInTransactionAsync(tx, ct);
            tx.Commit();
        });

        var job = (await w.DispatchAsync("pay")).JobId.Value;
        await WorkerHost.WaitUntil(() => w.EventCount("completionCommitted") == 1, TimeSpan.FromSeconds(60), "the engine observes the committed staging");
        Assert.Equal(job, staged!.JobId.Value);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM ledger WHERE job_id = @j", ("j", job.ToString())));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick_jobs WHERE job_id = @j AND state = 'succeeded'", ("j", job)));
        Assert.Equal(0, Interlocked.Read(ref w.Engine.FinishesSent));
        await w.StopAsync();
    }
}
