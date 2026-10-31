using System.Transactions;
using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>TX-01..04 (07 §8) and DEP-02 (07 §13): caller-owned staging.</summary>
public sealed class StagingTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly DispatchOptions WindowW = new() { Uniqueness = new DispatchUniqueness(null, "w", TimeSpan.FromHours(1)) };

    static async Task<(DriverFixture F, TestClient C)> StartAsync(string name, StoreProfile profile = StoreProfile.DirectPrimary)
    {
        var f = await DriverFixture.StartAsync(Ns(name), profile);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await f.ExecAsync("CREATE TABLE public.biz (id integer PRIMARY KEY, note text)");
        await f.ExecAsync("INSERT INTO public.biz VALUES (0, 'seed')");
        return (f, c);
    }

    static Task<long> Biz(DriverFixture f, int id) => f.CountAsync("SELECT count(*) FROM public.biz WHERE id = @id", ("id", id));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "TX-01")]
    public async Task Rolled_back_business_transaction_leaves_nothing()
    {
        var (f, c) = await StartAsync("tx01");
        await using var _ = f;
        var pre = await f.ReadCommittedSnapshot();
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"), WindowW);

        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await ExecAsync(conn, tx, "INSERT INTO public.biz VALUES (1, 'row')");
            var staged = await Staging(c).StageAsync(p, tx);
            Assert.Equal(DispatchDisposition.Created, staged.Disposition);
            await tx.RollbackAsync();
        }

        Assert.Equal(0, await Biz(f, 1));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(DispatchResolutionStatus.NotObserved, (await c.Prepared.ResolveAsync(p.OperationId)).Status);
        var later = await c.Dispatcher.DispatchAsync(new KeyedJob("t", "k"), WindowW);
        Assert.Equal(DispatchDisposition.Created, later.Disposition);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "TX-02")]
    public async Task Committed_business_transaction_makes_everything_visible_together()
    {
        var (f, c) = await StartAsync("tx02");
        await using var _ = f;
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"), WindowW);
        StagedDispatch staged;
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await ExecAsync(conn, tx, "INSERT INTO public.biz VALUES (1, 'row')");
            staged = await Staging(c).StageAsync(p, tx);

            // Still the caller's open transaction: further SQL runs, nothing is visible outside yet.
            Assert.NotNull(tx.Connection);
            Assert.Equal(System.Data.ConnectionState.Open, conn.State);
            await ExecAsync(conn, tx, "UPDATE public.biz SET note = 'after-stage' WHERE id = 1");
            Assert.Equal(0, await Jobs(f));
            Assert.Equal(0, await Receipts(f, p.OperationId.Value));
            await tx.CommitAsync();
        }

        Assert.Equal((p.OperationId, DispatchDisposition.Created, DuplicateMatch.None, false),
            (staged.OperationId, staged.Disposition, staged.DuplicateMatch, staged.PolicyDrift));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM public.biz WHERE id = 1 AND note = 'after-stage'"));
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j", ("j", staged.JobId.Value)));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE bound_job_id = @j", ("j", staged.JobId.Value)));
        Assert.Equal(1, await Receipts(f, p.OperationId.Value));
        var resolution = await c.Prepared.ResolveAsync(p.OperationId);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal((staged.JobId, DispatchDisposition.Created), (resolution.Receipt!.JobId, resolution.Receipt.Disposition));

        // Rejections leave the caller's transaction usable (savepoint) and throw the dispatch exception.
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await Assert.ThrowsAsync<UniquenessConflictException>(() => Staging(c).StageAsync(new KeyedJob("other", "z"), tx, WindowW));
            await ExecAsync(conn, tx, "INSERT INTO public.biz VALUES (2, 'still usable')");
            await tx.CommitAsync();
        }

        Assert.Equal(1, await Biz(f, 2));

        // A completed transaction is rejected before any write.
        await using (var conn = await OpenBusinessAsync(f))
        {
            var done = await conn.BeginTransactionAsync();
            await done.CommitAsync();
            var invalid = await Assert.ThrowsAsync<TransactionValidationException>(() => Staging(c).StageAsync(new PingJob("x"), done));
            Assert.Equal(RunwickErrorCodes.TransactionInvalid, invalid.Code);
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "TX-03")]
    public async Task Caller_deadlock_surfaces_unchanged_and_whole_transaction_retry_creates_one_job()
    {
        var (f, c) = await StartAsync("tx03b");
        await using var _ = f;
        var p1 = c.Prepared.Prepare(new KeyedJob("one", "k"));
        var p2 = c.Prepared.Prepare(new KeyedJob("two", "k"));
        var deadlocksBefore = Deadlocks(c);

        await using var conn1 = await OpenBusinessAsync(f, "app1");
        await using var conn2 = await OpenBusinessAsync(f, "app2");
        var tx1 = await conn1.BeginTransactionAsync();
        var tx2 = await conn2.BeginTransactionAsync();
        await ExecAsync(conn1, tx1, "UPDATE public.biz SET note = 'tx1' WHERE id = 0");      // tx1 locks B
        await Staging(c).StageAsync(p2, tx2);                                                 // tx2 holds key k
        var t1 = Staging(c).StageAsync(p1, tx1);                                              // tx1 waits on k
        await Task.Delay(300);
        var t2 = ExecAsync(conn2, tx2, "UPDATE public.biz SET note = 'tx2' WHERE id = 0");   // tx2 waits on B → deadlock

        // The deadlock victim fails first; the survivor proceeds only once the victim's transaction is rolled back.
        var first = await Task.WhenAny(t1, t2).WaitAsync(Wait);
        var loserIsTx1 = first == t1;
        var pg = Assert.IsType<PostgresException>(await Capture(first));
        Assert.Equal("40P01", pg.SqlState);
        var (winnerTx, loserTx) = loserIsTx1 ? (tx2, tx1) : (tx1, tx2);
        await loserTx.RollbackAsync();
        Assert.Null(await Capture(loserIsTx1 ? t2 : t1).WaitAsync(Wait));
        await winnerTx.CommitAsync();
        await tx1.DisposeAsync();
        await tx2.DisposeAsync();
        Assert.Equal(deadlocksBefore, Deadlocks(c));

        // The caller retries its whole transaction with the same PreparedDispatch.
        var retryPrepared = loserIsTx1 ? p1 : p2;
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await ExecAsync(conn, tx, "UPDATE public.biz SET note = 'retry' WHERE id = 0");
            var staged = await Staging(c).StageAsync(retryPrepared, tx);
            Assert.Equal(DispatchDisposition.Reused, staged.Disposition);
            await tx.CommitAsync();
        }

        Assert.Equal(1, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "TX-03")]
    public async Task Caller_serialization_failure_surfaces_unchanged()
    {
        var (f, c) = await StartAsync("tx03a");
        await using var _ = f;
        var p1 = c.Prepared.Prepare(new PingJob("one"));
        var p2 = c.Prepared.Prepare(new PingJob("two"));
        await using var conn1 = await OpenBusinessAsync(f, "app1");
        await using var conn2 = await OpenBusinessAsync(f, "app2");
        var tx1 = await conn1.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);
        var tx2 = await conn2.BeginTransactionAsync(System.Data.IsolationLevel.Serializable);

        // Classic write skew: each reads the set and inserts based on it.
        await ExecAsync(conn1, tx1, "SELECT count(*) FROM public.biz");
        await ExecAsync(conn2, tx2, "SELECT count(*) FROM public.biz");
        await ExecAsync(conn1, tx1, "INSERT INTO public.biz SELECT max(id) + 1, 'tx1' FROM public.biz");
        await ExecAsync(conn2, tx2, "INSERT INTO public.biz SELECT max(id) + 100, 'tx2' FROM public.biz");
        var e1 = await Capture(Staging(c).StageAsync(p1, tx1));
        var e2 = await Capture(Staging(c).StageAsync(p2, tx2));
        var c1 = e1 ?? await Capture(tx1.CommitAsync());
        var c2 = e2 ?? await Capture(tx2.CommitAsync());

        var failure = Assert.Single(new[] { c1, c2 }, e => e is not null);
        Assert.Equal("40001", Assert.IsType<PostgresException>(failure).SqlState);
        await tx1.DisposeAsync();
        await tx2.DisposeAsync();

        var retry = c1 is not null ? p1 : p2;
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.Serializable))
        {
            await ExecAsync(conn, tx, "INSERT INTO public.biz SELECT max(id) + 1000, 'retry' FROM public.biz");
            Assert.Equal(DispatchDisposition.Created, (await Staging(c).StageAsync(retry, tx)).Disposition);
            await tx.CommitAsync();
        }

        Assert.Equal(2, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "TX-04")]
    public async Task Lost_caller_commit_reply_resolves_by_operation_id()
    {
        var (f, c) = await StartAsync("tx04", StoreProfile.DirectPrimaryViaProxy);
        await using var _ = f;

        // Reply lost after COMMIT reached the server.
        var p = c.Prepared.Prepare(new PingJob("lost-reply"));
        await using (var conn = await OpenBusinessAsync(f))
        {
            var tx = await conn.BeginTransactionAsync();
            await ExecAsync(conn, tx, "INSERT INTO public.biz VALUES (1, 'row')");
            var staged = await Staging(c).StageAsync(p, tx);
            Assert.Equal(DispatchDisposition.Created, staged.Disposition);
            var dropped = f.Proxy!.ArmCommitReplyDrop(1, PgFaultProxy.Role("business"));
            await Assert.ThrowsAnyAsync<Exception>(() => tx.CommitAsync());
            await dropped.WaitAsync(Wait);
        }

        Assert.Contains("COMMIT", f.Proxy.DroppedCommitTags);
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(p.OperationId)).Status);
        Assert.Equal(1, await Biz(f, 1));
        Assert.Equal(1, await Jobs(f));

        // Severed before COMMIT is sent: nothing visible.
        var q = c.Prepared.Prepare(new PingJob("severed"));
        await using (var conn = await OpenBusinessAsync(f))
        {
            var tx = await conn.BeginTransactionAsync();
            await ExecAsync(conn, tx, "INSERT INTO public.biz VALUES (2, 'row')");
            await Staging(c).StageAsync(q, tx);
            Assert.True(f.Proxy.Sever(PgFaultProxy.Role("business")) >= 1);
            await Assert.ThrowsAnyAsync<Exception>(() => tx.CommitAsync());
        }

        Assert.Equal(0, await Biz(f, 2));
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(DispatchResolutionStatus.NotObserved, (await c.Prepared.ResolveAsync(q.OperationId)).Status);
        var afterNotAfter = await c.Store.ResolveOperationAsync(c.Application, q.OperationId.Value,
            q.NotAfter - TimeSpan.FromHours(24) - TimeSpan.FromMinutes(6), CancellationToken.None);
        Assert.IsType<ReceiptResolution.ExpiredUnknown>(afterNotAfter);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "DEP-02")]
    public async Task Staging_writes_on_the_caller_backend_and_ambient_scopes_are_ignored()
    {
        var (f, c) = await StartAsync("dep02");
        await using var _ = f;
        const string DriverBackends = "SELECT pid::text FROM pg_stat_activity WHERE application_name IN ('rw:local:work', 'rw:local:control')";
        var poolBefore = await TextAsync(f, DriverBackends);

        // (a) The staged rows carry the caller transaction's xid.
        StagedDispatch staged;
        long callerXid;
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            using (var cmd = new NpgsqlCommand("SELECT pg_current_xact_id()::text::bigint", conn, tx))
            {
                callerXid = (long)(await cmd.ExecuteScalarAsync())!;
            }

            staged = await Staging(c).StageAsync(new PingJob("dep02"), tx);

            // Visible on the caller's backend inside its transaction (written in a subtransaction of callerXid),
            // invisible to every other session until the caller commits.
            using (var cmd = new NpgsqlCommand("""
                SELECT count(*) FROM runwick.jobs j JOIN runwick.storage_commands s ON s.command_id = @c
                WHERE j.job_id = @j AND j.xmin::text::bigint > @x AND s.xmin::text::bigint > @x
                """, conn, tx))
            {
                cmd.Parameters.AddWithValue("j", staged.JobId.Value);
                cmd.Parameters.AddWithValue("c", staged.OperationId.Value);
                cmd.Parameters.AddWithValue("x", callerXid % (1L << 32));
                Assert.Equal(1L, (long)(await cmd.ExecuteScalarAsync())!);
            }

            Assert.Equal(0, await Jobs(f));
            await tx.CommitAsync();
        }

        // No Runwick pool backend was opened for staging (idle pruning may close one; none may appear).
        Assert.Empty((await TextAsync(f, DriverBackends)).Except(poolBefore));

        // (b) Reserve and Finish inside an uncompleted TransactionScope still commit (Enlist=false).
        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            var lease = await RunAsync(c, staged.JobId.Value);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        }

        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", staged.JobId.Value)));
    }

    static async Task<Exception?> Capture(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
